//! Refresh orchestration: when a catalogue is imported, and what an import may
//! disturb.
//!
//! [`CatalogSource`] answers one question and [`CatalogStore`] retains one
//! answer; neither decides *when* to ask, what a slow upstream costs, or how a run of
//! failures paces itself. Left to a caller, those decisions are where silent
//! staleness comes from — a scheduler that forgot to count a refusal, a retry
//! loop that hammered a struggling mirror, a boot path that admitted a
//! half-stored import. [`CatalogRefresher`] is the one place they are made.
//!
//! Four properties hold whatever a caller does with it:
//!
//! - **Retention precedes admission.** An import is written to the store before
//!   it becomes active in this process, so a store that refuses leaves the
//!   previous catalogue active and counts a [`RefusalReason::NotRetained`]
//!   refusal. A replica cannot serve a catalogue it did not manage to keep.
//! - **Every outcome is counted.** Success, `304`, a refused parse, an outage, a
//!   store failure, and a timeout all pass through
//!   [`LastKnownGoodCatalog::record_refresh`], so
//!   [`CatalogReport::consecutive_refusals`] is a property of the catalogue
//!   rather than of a caller's diligence, and the store's copy of that count
//!   moves with it.
//! - **A refresh is bounded in time and paced by its failures.** One attempt is
//!   capped by [`RefreshSchedule::timeout`]; a refused one is retried on
//!   [`convergence backoff`](crate::convergence::backoff), which is deterministic
//!   and saturating, so a week-long outage settles at one attempt per ceiling
//!   instead of pinning a mirror.
//! - **Nothing an upstream says activates anything.** A refresh admits
//!   *observations*. An operator's enablements keep pointing at the snapshot
//!   they were approved against — a pin is a digest, and admitting new content
//!   does not move one — so a model that appears upstream is not usable and a
//!   price that changes upstream is not charged. What a new catalogue *would*
//!   mean for what operators enabled is reported by [`RefreshImpact`] and acted
//!   on by a human.
//!
//! # Manual and scheduled refreshes are the same import
//!
//! [`RefreshTrigger`] distinguishes them for reporting and for one behaviour
//! only: a scheduled refresh that is not due yet is skipped, and a manual one is
//! never skipped. An operator asking for a refresh during an incident is asking
//! now. Everything after that — the timeout, the ordering, the counting, the
//! backoff — is identical, so the path an operator exercises by hand is the path
//! that runs unattended.

use std::collections::BTreeSet;
use std::time::{Duration, SystemTime};

use super::catalog::{
    Admission, CatalogContent, CatalogError, CatalogRefresh, CatalogReport, CatalogSnapshot,
    CatalogSource, LastKnownGoodCatalog, RawPayload, Refreshed, Refusable, Refusal, RefusalReason,
    SourceValidators,
};
use super::catalog_store::{
    CatalogStore, CatalogStoreError, HydrationError, RetainedCatalog, Retention, hydrate,
};
use super::models_dev::{SEED_PAYLOAD, seed_snapshot};
use crate::convergence::backoff::{Backoff, BackoffPolicy, InvalidBackoff};
use crate::desired_state::models::{ModelEnablementBody, OfferingId};

/// How often a catalogue is refreshed, how long one attempt may take, and how a
/// refused one is paced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RefreshSchedule {
    /// The interval between refreshes that were not refused.
    pub interval: Duration,
    /// The ceiling on one refresh, fetch and retention together. A source that
    /// hangs must cost a refused refresh rather than a scheduler that never
    /// runs again.
    pub timeout: Duration,
    /// How a run of refused refreshes paces itself.
    pub backoff: BackoffPolicy,
}

impl Default for RefreshSchedule {
    /// A catalogue is a document that changes a few times a week, so six hours
    /// is frequent enough that a new model is days-fresh and rare enough that a
    /// deployment is not a load source. The retry ceiling stays under the
    /// interval: backoff exists to pace an outage, not to make a failing
    /// deployment refresh less often than a healthy one.
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(6 * 60 * 60),
            timeout: Duration::from_secs(60),
            backoff: BackoffPolicy {
                initial: Duration::from_secs(60),
                max: Duration::from_secs(30 * 60),
                multiplier: 2,
            },
        }
    }
}

/// Why a schedule cannot be used.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidSchedule {
    #[error("catalogue refresh interval must be greater than zero")]
    ZeroInterval,
    #[error("catalogue refresh timeout must be greater than zero")]
    ZeroTimeout,
    #[error("catalogue refresh timeout ({timeout:?}) must not exceed the interval ({interval:?})")]
    TimeoutExceedsInterval {
        timeout: Duration,
        interval: Duration,
    },
    #[error(
        "catalogue retry ceiling ({max:?}) must not exceed the refresh interval ({interval:?}): a \
         refusing deployment would refresh less often than a healthy one"
    )]
    BackoffExceedsInterval { max: Duration, interval: Duration },
    #[error(transparent)]
    Backoff(#[from] InvalidBackoff),
}

impl RefreshSchedule {
    pub fn validate(&self) -> Result<(), InvalidSchedule> {
        if self.interval.is_zero() {
            return Err(InvalidSchedule::ZeroInterval);
        }
        if self.timeout.is_zero() {
            return Err(InvalidSchedule::ZeroTimeout);
        }
        if self.timeout > self.interval {
            return Err(InvalidSchedule::TimeoutExceedsInterval {
                timeout: self.timeout,
                interval: self.interval,
            });
        }
        self.backoff.validate()?;
        if self.backoff.max > self.interval {
            return Err(InvalidSchedule::BackoffExceedsInterval {
                max: self.backoff.max,
                interval: self.interval,
            });
        }
        Ok(())
    }
}

/// What asked for an import.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshTrigger {
    /// The interval elapsed.
    Scheduled,
    /// An operator asked. Never skipped for not being due: during an incident,
    /// "refresh now" means now.
    Manual,
}

/// What a deployment may do when it boots holding no catalogue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bootstrap {
    /// Start empty and wait for the first refresh. The honest default for a
    /// deployment with egress: an empty catalogue reports as empty, and nothing
    /// pretends to know what models exist.
    Empty,
    /// Import the bundled seed, so an air-gapped deployment holds a real
    /// catalogue immediately.
    ///
    /// The seed's provenance is seed-local by construction
    /// ([`seed_snapshot`]), so no upstream can
    /// answer `304` against it and the first live refresh transfers the real
    /// document: seeding establishes content, never a claim that an upstream
    /// confirmed it.
    Seed,
}

/// What restoring found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Restored {
    /// A catalogue this deployment had already imported, with when it was last
    /// confirmed current.
    Stored {
        content_id: super::catalog::CatalogContentId,
        confirmed_at: SystemTime,
    },
    /// Nothing was stored, and the bundled seed was imported.
    Seeded {
        content_id: super::catalog::CatalogContentId,
    },
    /// Nothing was stored, and nothing was invented.
    Empty,
}

/// Why an import did not advance the catalogue.
///
/// One type over the three layers a refresh crosses, so
/// [`LastKnownGoodCatalog::record_refresh`] can count any of them by bounded
/// reason while the typed detail — a pointer into the payload, a `SQLSTATE`
/// message — stays available for the log line beside it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RefreshError {
    #[error(transparent)]
    Source(#[from] CatalogError),
    #[error(transparent)]
    Store(#[from] CatalogStoreError),
    #[error(transparent)]
    Stored(#[from] HydrationError),
    #[error("the catalogue refresh returned an inconsistent import: {message}")]
    InvalidImport { refusal: Refusal, message: String },
    #[error("the refresh did not finish within {timeout:?}")]
    TimedOut { timeout: Duration },
}

impl Refusable for RefreshError {
    fn refusal(&self) -> Refusal {
        match self {
            Self::Source(error) => error.refusal(),
            Self::Store(error) => error.refusal(),
            Self::Stored(error) => error.refusal(),
            Self::InvalidImport { refusal, .. } => refusal.clone(),
            // An upstream that did not answer in time is an upstream that did
            // not answer: the same reason a transport failure carries, because
            // an operator's next step is the same one.
            Self::TimedOut { .. } => Refusal::new(RefusalReason::Unreachable),
        }
    }
}

/// What one refresh did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefreshOutcome {
    /// The catalogue advanced: new content, or an unchanged answer that
    /// confirmed the content already held.
    Admitted {
        trigger: RefreshTrigger,
        admission: Admission,
        /// Whether the store had to write the import. `None` for an answer that
        /// carried no content.
        retention: Option<Retention>,
        /// When the next scheduled refresh is due.
        next_due: SystemTime,
    },
    /// The import was refused, and the previously active catalogue — if any —
    /// is still active.
    Refused {
        trigger: RefreshTrigger,
        refusal: Refusal,
        /// How long until the next attempt, from the deterministic backoff.
        retry_in: Duration,
        next_due: SystemTime,
    },
    /// A scheduled refresh that was not due. Never returned for
    /// [`RefreshTrigger::Manual`].
    NotDue { next_due: SystemTime },
}

/// What a newly imported catalogue would mean for what operators have enabled.
///
/// A report, and deliberately only a report. The catalogue is an observation and
/// an enablement is a decision; a refresh that withdrew an enablement because an
/// upstream stopped listing a model would be an upstream operating the
/// deployment. So this names what a human should look at, and changes nothing:
/// every enablement keeps its state, its approved price, and the snapshot it was
/// approved against.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RefreshImpact {
    /// Enablements still pinned to a catalogue that is no longer the active one.
    /// Expected, not a fault: a pin moves when an operator republishes an
    /// enablement against a newer snapshot.
    pub pins_unmoved: usize,
    /// Offerings an operator has enabled that the newly imported catalogue no
    /// longer publishes. The set worth waking someone for — and still not
    /// something a refresh may act on.
    pub withdrawn: BTreeSet<OfferingId>,
}

impl RefreshImpact {
    /// Compare `enablements` against newly imported `content`, which was parsed
    /// from the payload `raw_digest` identifies.
    ///
    /// `raw_digest` rather than the content id because an enablement pins the
    /// snapshot *blob* it was read from ([`CatalogOffering`]).
    ///
    /// [`CatalogOffering`]: crate::desired_state::models::CatalogOffering
    pub fn of<'a>(
        enablements: impl IntoIterator<Item = &'a ModelEnablementBody>,
        content: &CatalogContent,
        raw_digest: crate::desired_state::Checksum,
    ) -> Self {
        let published: BTreeSet<OfferingId> = content
            .models()
            .iter()
            .flat_map(|model| model.offerings.iter())
            .filter_map(|offering| {
                OfferingId::of(offering.provider.as_str(), offering.model.as_str()).ok()
            })
            .collect();
        let mut impact = Self::default();
        for enablement in enablements {
            let offering = enablement.offering();
            if !offering.is_pinned_to(raw_digest) {
                impact.pins_unmoved += 1;
            }
            if !published.contains(&offering.offering) {
                impact.withdrawn.insert(offering.offering);
            }
        }
        impact
    }
}

/// The scheduler: one source, one store, and the last-known-good catalogue they
/// agree on.
///
/// Owned by one background task and driven by `&mut self` rather than holding
/// locks: two concurrent refreshes of the same catalogue would race over the
/// active pointer to no purpose, and a caller that wants a manual refresh sends
/// the task a message.
pub struct CatalogRefresher<S, T> {
    source: S,
    store: T,
    schedule: RefreshSchedule,
    bootstrap: Bootstrap,
    catalogue: LastKnownGoodCatalog,
    backoff: Backoff,
    next_due: SystemTime,
}

impl<S: CatalogSource, T: CatalogStore> CatalogRefresher<S, T> {
    /// A refresher that has not looked at the store yet. Call
    /// [`restore`](Self::restore) before the first refresh.
    pub fn new(
        source: S,
        store: T,
        schedule: RefreshSchedule,
        bootstrap: Bootstrap,
        now: SystemTime,
    ) -> Result<Self, InvalidSchedule> {
        schedule.validate()?;
        Ok(Self {
            source,
            store,
            schedule,
            bootstrap,
            catalogue: LastKnownGoodCatalog::new(),
            backoff: Backoff::new(schedule.backoff),
            // Due immediately: a replica that just booted has no evidence its
            // stored catalogue is still current, and the first refresh is a
            // conditional request that usually transfers nothing.
            next_due: now,
        })
    }

    /// Adopt what the deployment has already imported, seeding if it has
    /// imported nothing and is configured to.
    ///
    /// A stored catalogue that no longer rehydrates — bytes that do not match
    /// their digest, a payload this build normalizes differently — is refused
    /// and counted rather than served: the content an operator approved a price
    /// book against is not the content that would be active, and a deployment
    /// reading as healthy on content nobody can reproduce is the failure this
    /// check exists for.
    pub async fn restore(&mut self, now: SystemTime) -> Result<Restored, RefreshError> {
        let stored = match self.store.load().await {
            Ok(stored) => stored,
            Err(error) => {
                self.count_refusal(error.refusal(), now).await;
                return Err(RefreshError::Store(error));
            }
        };
        if let Some(active) = stored.active {
            let confirmed_at = active.source.fetched_at;
            let snapshot = match hydrate(&active) {
                Ok(snapshot) => snapshot,
                Err(error) => {
                    // Adopt the run the store recorded *before* counting this
                    // refusal onto it. Counting onto a fresh holder would report
                    // one refusal for a deployment that has been refusing for a
                    // week, and an unreadable stored catalogue is precisely when
                    // the durable count is the only history there is.
                    self.catalogue = LastKnownGoodCatalog::restored(
                        None,
                        stored.consecutive_refusals,
                        stored.last_refusal.map(Refusal::new),
                    );
                    self.count_refusal(error.refusal(), now).await;
                    return Err(RefreshError::Stored(error));
                }
            };
            let content_id = snapshot.content.content_id();
            self.catalogue = LastKnownGoodCatalog::restored(
                Some(snapshot),
                stored.consecutive_refusals,
                stored.last_refusal.map(Refusal::new),
            );
            self.report_state(now);
            return Ok(Restored::Stored {
                content_id,
                confirmed_at,
            });
        }
        self.catalogue = LastKnownGoodCatalog::restored(
            None,
            stored.consecutive_refusals,
            stored.last_refusal.map(Refusal::new),
        );
        if self.bootstrap == Bootstrap::Empty {
            self.report_state(now);
            return Ok(Restored::Empty);
        }
        let snapshot = seed_snapshot();
        let import = RetainedCatalog {
            source: snapshot.source.clone(),
            payload: RawPayload::new(SEED_PAYLOAD.as_bytes()),
        };
        if let Err(error) = self.store.activate(&import, now).await {
            self.count_refusal(error.refusal(), now).await;
            return Err(RefreshError::Store(error));
        }
        // Aged to now, not to the day the excerpt was cut: age is how long ago
        // this process confirmed the content, and a seed imported this minute is
        // a minute old however old its fixture says it is.
        let admission = self.catalogue.admit_as_of(snapshot, now);
        self.report_state(now);
        Ok(Restored::Seeded {
            content_id: admission.content_id(),
        })
    }

    /// Attempt one import.
    pub async fn refresh(&mut self, trigger: RefreshTrigger, now: SystemTime) -> RefreshOutcome {
        if trigger == RefreshTrigger::Scheduled && now < self.next_due {
            return RefreshOutcome::NotDue {
                next_due: self.next_due,
            };
        }
        let asked_with = self.catalogue.validators().cloned();
        let answer = self.attempt(asked_with.as_ref(), now).await;
        let (answer, retention) = match answer {
            Ok((refresh, retention)) => (Ok(refresh), retention),
            Err(error) => (Err(error), None),
        };
        let recorded = self
            .catalogue
            .record_refresh(answer, asked_with.as_ref(), now);
        match recorded {
            Ok(Refreshed::Admitted(admission)) => {
                self.backoff.succeed();
                self.next_due = now + self.schedule.interval;
                self.report_state(now);
                RefreshOutcome::Admitted {
                    trigger,
                    admission,
                    retention,
                    next_due: self.next_due,
                }
            }
            // A `304` nobody asked for: nothing was admitted and nothing was
            // stored, so it is paced and counted exactly like any other refusal.
            Ok(Refreshed::Refused(refusal)) => self.refused(trigger, refusal, now).await,
            Err((error, _still_active)) => {
                let refusal = error.refusal();
                tracing::warn!(
                    reason = refusal.reason().as_str(),
                    pointer = refusal.pointer().map(|pointer| pointer.as_str()),
                    %error,
                    "catalogue refresh refused",
                );
                self.refused(trigger, refusal, now).await
            }
        }
    }

    /// Ask the source and retain what it answered, under the schedule's
    /// ceiling.
    ///
    /// One ceiling over both halves, because either can hang and the failure is
    /// the same one: a background task that never produces an outcome stops
    /// counting refusals and stops reporting staleness, which is worse than the
    /// refused refresh a bounded attempt costs. A store is free to impose its
    /// own ceiling — [`PostgresCatalogStore`] does — but the refresher does not
    /// depend on every implementation having remembered to.
    ///
    /// [`PostgresCatalogStore`]: super::catalog_store::postgres::PostgresCatalogStore
    async fn attempt(
        &mut self,
        asked_with: Option<&SourceValidators>,
        now: SystemTime,
    ) -> Result<(CatalogRefresh, Option<Retention>), RefreshError> {
        let timeout = self.schedule.timeout;
        tokio::time::timeout(timeout, async {
            let refresh = self
                .source
                .refresh(asked_with)
                .await
                .map_err(RefreshError::Source)?;
            self.retain(refresh, asked_with, now).await
        })
        .await
        .map_err(|_| RefreshError::TimedOut { timeout })?
    }

    /// Write what the source answered, before any of it becomes active.
    ///
    /// The ordering is the contract: a store that refuses here means the answer
    /// never reaches [`LastKnownGoodCatalog`], so the catalogue this process
    /// serves and the catalogue the deployment has retained cannot diverge.
    async fn retain(
        &mut self,
        refresh: CatalogRefresh,
        asked_with: Option<&SourceValidators>,
        now: SystemTime,
    ) -> Result<(CatalogRefresh, Option<Retention>), RefreshError> {
        match refresh {
            CatalogRefresh::Updated { snapshot, payload } => {
                if snapshot.source.content_id != snapshot.content.content_id() {
                    return Err(RefreshError::InvalidImport {
                        refusal: Refusal::new(RefusalReason::Content),
                        message: format!(
                            "snapshot metadata names {}, but normalized content is {}",
                            snapshot.source.content_id,
                            snapshot.content.content_id(),
                        ),
                    });
                }
                snapshot
                    .source
                    .raw
                    .verify(payload.as_bytes())
                    .map_err(|error| RefreshError::InvalidImport {
                        refusal: Refusal::new(RefusalReason::Content),
                        message: error.to_string(),
                    })?;
                let import = RetainedCatalog {
                    source: snapshot.source.clone(),
                    payload,
                };
                let retention = self.store.activate(&import, now).await?;
                Ok((
                    CatalogRefresh::Updated {
                        snapshot,
                        payload: import.payload,
                    },
                    Some(retention),
                ))
            }
            CatalogRefresh::Unchanged { validators } => {
                // Exactly the condition the holder applies, asked of the holder
                // rather than restated here: an answer `record_refresh` is about
                // to refuse as unsolicited must not first move the stored
                // confirmation time forward and clear the stored refusal run,
                // which would leave a restarted replica reading as freshly
                // checked while nothing had confirmed anything.
                let confirmable = self.catalogue.can_confirm_unchanged(asked_with);
                if let Some(active) = self.catalogue.active().filter(|_| confirmable) {
                    self.store
                        .confirm(active.content.content_id(), &validators, now)
                        .await?;
                }
                Ok((CatalogRefresh::Unchanged { validators }, None))
            }
        }
    }

    async fn refused(
        &mut self,
        trigger: RefreshTrigger,
        refusal: Refusal,
        now: SystemTime,
    ) -> RefreshOutcome {
        self.publish_refusal(refusal.reason(), now).await;
        let retry_in = self.backoff.fail();
        self.next_due = now + retry_in;
        RefreshOutcome::Refused {
            trigger,
            refusal,
            retry_in,
            next_due: self.next_due,
        }
    }

    /// Count a refusal everywhere it is counted: in the holder, in the metric
    /// an alert reads, in the store, and in the state gauges.
    ///
    /// Boot refusals go through this too. A deployment whose store is
    /// unreachable, or whose stored catalogue no longer rehydrates, is refusing
    /// exactly as a deployment whose upstream is down is refusing; a refusal
    /// that moved the counter without moving the metric would be invisible to
    /// an alert keyed on the metric, and precisely at boot, where the previous
    /// process's gauges are the ones still published.
    async fn count_refusal(&mut self, refusal: Refusal, now: SystemTime) {
        self.catalogue.record_refusal(refusal.clone());
        self.publish_refusal(refusal.reason(), now).await;
    }

    /// Everything a counted refusal owes the outside: the metric, the store,
    /// and the state gauges. Separate from the holder's count because a
    /// refreshed refusal is already counted by
    /// [`LastKnownGoodCatalog::record_refresh`], and counting it twice would
    /// make one outage read as two.
    async fn publish_refusal(&self, reason: RefusalReason, now: SystemTime) {
        crate::telemetry::metrics::record_catalog_refusal(reason);
        self.record_refusal_durably(reason, now).await;
        self.report_state(now);
    }

    /// Count a refusal in the store, best effort and bounded.
    ///
    /// A store that cannot record the refusal has already cost this refresh —
    /// the in-process count moved, and the report an operator reads is
    /// projected from it — so failing the refresh a second time over the
    /// bookkeeping would only replace one refusal with a less informative one.
    /// The same reasoning bounds it: bookkeeping that hangs must not be what
    /// stops a refresher from ever producing an outcome.
    async fn record_refusal_durably(&self, reason: RefusalReason, now: SystemTime) {
        match tokio::time::timeout(self.schedule.timeout, self.store.refuse(reason, now)).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::warn!(%error, "a refused catalogue refresh could not be counted durably");
            }
            Err(_) => {
                tracing::warn!(
                    timeout = ?self.schedule.timeout,
                    "a refused catalogue refresh could not be counted durably in time",
                );
            }
        }
    }

    /// Publish the catalogue's state to the metrics an alert reads.
    fn report_state(&self, now: SystemTime) {
        crate::telemetry::metrics::record_catalog_state(&self.catalogue.report(now));
    }

    /// When the next scheduled refresh is due: the interval after an admitted
    /// import, or the backoff delay after a refused one.
    pub const fn next_due(&self) -> SystemTime {
        self.next_due
    }

    pub fn active(&self) -> Option<&CatalogSnapshot> {
        self.catalogue.active()
    }

    pub fn report(&self, now: SystemTime) -> CatalogReport {
        self.catalogue.report(now)
    }

    pub const fn store(&self) -> &T {
        &self.store
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use async_trait::async_trait;

    use super::super::catalog::{
        CatalogChange, CatalogContentId, PERSISTENT_REFUSAL_THRESHOLD, SchemaVersion,
        source_snapshot,
    };
    use super::super::catalog_store::{InMemoryCatalogStore, StoredCatalogState};
    use super::super::models_dev::{ModelsDevAdapter, ModelsDevError};
    use super::super::{Capabilities, Capability};
    use super::*;

    const CATALOGUE: &str = include_str!("fixtures/models_dev/catalog.identity.json");

    fn at(seconds: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(seconds)
    }

    /// Parse a fixture the way the adapter does, so no test here restates the
    /// parser's contract.
    fn imported(payload: &str, validators: SourceValidators) -> (CatalogSnapshot, RawPayload) {
        let snapshot = ModelsDevAdapter::default()
            .parse(payload.as_bytes(), validators, at(0))
            .expect("the fixture parses");
        (snapshot, RawPayload::new(payload.as_bytes()))
    }

    fn updated(payload: &str, etag: &str) -> CatalogRefresh {
        let (snapshot, payload) = imported(payload, SourceValidators::etag(etag));
        CatalogRefresh::Updated {
            snapshot: Box::new(snapshot),
            payload,
        }
    }

    /// The identity fixture with one model's price changed: a second catalogue,
    /// still produced by the real parser.
    fn repriced() -> String {
        let payload = CATALOGUE.replacen("\"input\": 5,", "\"input\": 6,", 1);
        assert_ne!(payload, CATALOGUE, "the fixture must actually change");
        payload
    }

    /// A source that answers a script, so a test states the sequence of upstream
    /// answers it is about and nothing else.
    struct ScriptedSource {
        answers: Mutex<VecDeque<Result<CatalogRefresh, CatalogError>>>,
        asked_with: Mutex<Vec<Option<SourceValidators>>>,
        /// Answers only after this long, to be caught by the schedule's ceiling.
        latency: Duration,
    }

    impl ScriptedSource {
        fn new(answers: impl IntoIterator<Item = Result<CatalogRefresh, CatalogError>>) -> Self {
            Self {
                answers: Mutex::new(answers.into_iter().collect()),
                asked_with: Mutex::new(Vec::new()),
                latency: Duration::ZERO,
            }
        }

        fn slow(latency: Duration) -> Self {
            Self {
                answers: Mutex::new(VecDeque::new()),
                asked_with: Mutex::new(Vec::new()),
                latency,
            }
        }

        fn asked_with(&self) -> Vec<Option<SourceValidators>> {
            self.asked_with.lock().expect("lock").clone()
        }
    }

    #[async_trait]
    impl CatalogSource for ScriptedSource {
        fn name(&self) -> &'static str {
            "scripted"
        }

        fn capabilities(&self) -> Capabilities {
            Capabilities::new(&[Capability::IncrementalRefresh])
        }

        async fn refresh(
            &self,
            since: Option<&SourceValidators>,
        ) -> Result<CatalogRefresh, CatalogError> {
            self.asked_with.lock().expect("lock").push(since.cloned());
            if !self.latency.is_zero() {
                tokio::time::sleep(self.latency).await;
            }
            self.answers
                .lock()
                .expect("lock")
                .pop_front()
                .expect("the script has an answer for every refresh")
        }
    }

    /// A store that refuses whatever it is asked, to prove an import that cannot
    /// be retained does not become active.
    #[derive(Debug, Default)]
    struct RefusingStore {
        refusals: Mutex<Vec<RefusalReason>>,
    }

    #[async_trait]
    impl CatalogStore for RefusingStore {
        fn name(&self) -> &'static str {
            "refusing"
        }

        fn capabilities(&self) -> Capabilities {
            Capabilities::NONE
        }

        async fn load(&self) -> Result<StoredCatalogState, CatalogStoreError> {
            Ok(StoredCatalogState::default())
        }

        async fn retained(
            &self,
            _content_id: CatalogContentId,
        ) -> Result<Option<RetainedCatalog>, CatalogStoreError> {
            Ok(None)
        }

        async fn activate(
            &self,
            _import: &RetainedCatalog,
            _activated_at: SystemTime,
        ) -> Result<Retention, CatalogStoreError> {
            Err(CatalogStoreError::unavailable(
                "refusing",
                "the database is down",
            ))
        }

        async fn confirm(
            &self,
            _content_id: CatalogContentId,
            _validators: &SourceValidators,
            _confirmed_at: SystemTime,
        ) -> Result<bool, CatalogStoreError> {
            Err(CatalogStoreError::unavailable(
                "refusing",
                "the database is down",
            ))
        }

        async fn refuse(
            &self,
            reason: RefusalReason,
            _refused_at: SystemTime,
        ) -> Result<(), CatalogStoreError> {
            self.refusals.lock().expect("lock").push(reason);
            Ok(())
        }
    }

    /// A store whose writes never return, to prove the ceiling covers the half
    /// of an attempt the upstream is not responsible for.
    #[derive(Debug, Default)]
    struct HangingStore;

    #[async_trait]
    impl CatalogStore for HangingStore {
        fn name(&self) -> &'static str {
            "hanging"
        }

        fn capabilities(&self) -> Capabilities {
            Capabilities::NONE
        }

        async fn load(&self) -> Result<StoredCatalogState, CatalogStoreError> {
            Ok(StoredCatalogState::default())
        }

        async fn retained(
            &self,
            _content_id: CatalogContentId,
        ) -> Result<Option<RetainedCatalog>, CatalogStoreError> {
            Ok(None)
        }

        async fn activate(
            &self,
            _import: &RetainedCatalog,
            _activated_at: SystemTime,
        ) -> Result<Retention, CatalogStoreError> {
            std::future::pending().await
        }

        async fn confirm(
            &self,
            _content_id: CatalogContentId,
            _validators: &SourceValidators,
            _confirmed_at: SystemTime,
        ) -> Result<bool, CatalogStoreError> {
            std::future::pending().await
        }

        async fn refuse(
            &self,
            _reason: RefusalReason,
            _refused_at: SystemTime,
        ) -> Result<(), CatalogStoreError> {
            std::future::pending().await
        }
    }

    fn refresher<S: CatalogSource, T: CatalogStore>(source: S, store: T) -> CatalogRefresher<S, T> {
        CatalogRefresher::new(
            source,
            store,
            RefreshSchedule {
                interval: Duration::from_secs(3_600),
                timeout: Duration::from_secs(30),
                backoff: BackoffPolicy {
                    initial: Duration::from_secs(60),
                    max: Duration::from_secs(600),
                    multiplier: 2,
                },
            },
            Bootstrap::Empty,
            at(0),
        )
        .expect("a valid schedule")
    }

    #[tokio::test]
    async fn a_first_import_is_retained_and_becomes_active() {
        let source = ScriptedSource::new([Ok(updated(CATALOGUE, "\"one\""))]);
        let mut refresher = refresher(source, InMemoryCatalogStore::new());
        refresher.restore(at(10)).await.expect("restore");

        let outcome = refresher.refresh(RefreshTrigger::Scheduled, at(20)).await;
        let RefreshOutcome::Admitted {
            admission,
            retention,
            next_due,
            ..
        } = outcome
        else {
            panic!("a first import is admitted: {outcome:?}");
        };
        assert!(matches!(admission, Admission::Initial { .. }));
        assert_eq!(retention, Some(Retention::Retained));
        assert_eq!(next_due, at(20 + 3_600));

        // The deployment holds it, and holds it as content it can rebuild.
        let stored = refresher.store().load().await.expect("load");
        let active = stored.active.expect("an active catalogue");
        assert_eq!(
            hydrate(&active).expect("rehydrate").content.content_id(),
            refresher.active().expect("active").content.content_id()
        );
    }

    #[tokio::test]
    async fn an_import_with_a_payload_that_does_not_match_its_metadata_is_refused() {
        let (snapshot, _) = imported(CATALOGUE, SourceValidators::etag("\"one\""));
        let source = ScriptedSource::new([Ok(CatalogRefresh::Updated {
            snapshot: Box::new(snapshot),
            payload: RawPayload::new(&b"not-the-payload"[..]),
        })]);
        let store = InMemoryCatalogStore::new();
        let mut refresher = refresher(source, &store);
        refresher.restore(at(10)).await.expect("restore");

        let RefreshOutcome::Refused { refusal, .. } =
            refresher.refresh(RefreshTrigger::Manual, at(20)).await
        else {
            panic!("an inconsistent payload is refused");
        };
        assert_eq!(refusal.reason(), RefusalReason::Content);
        assert!(refresher.active().is_none());
        assert_eq!(store.retained_count(), 0);
    }

    #[tokio::test]
    async fn an_import_with_a_content_id_that_does_not_match_its_content_is_refused() {
        let (mut snapshot, payload) = imported(CATALOGUE, SourceValidators::etag("\"one\""));
        snapshot.source.content_id = super::super::catalog::CatalogContentId::from_checksum(
            crate::desired_state::Checksum::of(b"a different catalogue"),
        );
        let source = ScriptedSource::new([Ok(CatalogRefresh::Updated {
            snapshot: Box::new(snapshot),
            payload,
        })]);
        let store = InMemoryCatalogStore::new();
        let mut refresher = refresher(source, &store);
        refresher.restore(at(10)).await.expect("restore");

        let RefreshOutcome::Refused { refusal, .. } =
            refresher.refresh(RefreshTrigger::Manual, at(20)).await
        else {
            panic!("inconsistent snapshot metadata is refused");
        };
        assert_eq!(refusal.reason(), RefusalReason::Content);
        assert!(refresher.active().is_none());
        assert_eq!(store.retained_count(), 0);
    }

    /// The whole point of retention: a replica that restarts serves what it
    /// imported, at the age it was last confirmed, without asking the upstream.
    #[tokio::test]
    async fn a_restart_restores_the_catalogue_and_its_age() {
        let store = InMemoryCatalogStore::new();
        {
            let source = ScriptedSource::new([Ok(updated(CATALOGUE, "\"one\""))]);
            let mut refresher = refresher(source, &store);
            refresher.restore(at(10)).await.expect("restore");
            refresher.refresh(RefreshTrigger::Scheduled, at(100)).await;
        }

        let source = ScriptedSource::new([]);
        let mut restarted = refresher(source, &store);
        let restored = restarted.restore(at(1_000)).await.expect("restore");
        let Restored::Stored {
            content_id,
            confirmed_at,
        } = restored
        else {
            panic!("the stored catalogue is adopted: {restored:?}");
        };
        assert_eq!(confirmed_at, at(100));
        assert_eq!(
            restarted.active().expect("active").content.content_id(),
            content_id
        );
        let report = restarted.report(at(1_000));
        assert_eq!(
            report.active_age(),
            Some(Duration::from_secs(900)),
            "age is measured from the last confirmation, not from boot"
        );
    }

    #[tokio::test]
    async fn a_refusal_run_is_restored_so_a_restart_does_not_look_healthy() {
        let store = InMemoryCatalogStore::new();
        {
            let source = ScriptedSource::new([
                Err(CatalogError::Unavailable {
                    backend: super::super::BackendKind::ModelsDev.as_str(),
                    refusal: Refusal::new(RefusalReason::Unreachable),
                    message: "connection reset".to_owned(),
                }),
                Err(CatalogError::Unavailable {
                    backend: super::super::BackendKind::ModelsDev.as_str(),
                    refusal: Refusal::new(RefusalReason::Unreachable),
                    message: "connection reset".to_owned(),
                }),
            ]);
            let mut refresher = refresher(source, &store);
            refresher.restore(at(10)).await.expect("restore");
            refresher.refresh(RefreshTrigger::Manual, at(20)).await;
            refresher.refresh(RefreshTrigger::Manual, at(30)).await;
        }

        let mut restarted = refresher(ScriptedSource::new([]), &store);
        assert_eq!(
            restarted.restore(at(40)).await.expect("restore"),
            Restored::Empty
        );
        let report = restarted.report(at(40));
        assert_eq!(report.consecutive_refusals, 2);
        assert_eq!(report.last_refusal, Some(RefusalReason::Unreachable));
        assert!(
            report.persistent_refusal(),
            "the alert condition survives the process that observed it"
        );
        assert_eq!(PERSISTENT_REFUSAL_THRESHOLD, 2);
    }

    /// Idempotence, end to end: the same catalogue imported twice stores one
    /// snapshot and reports no change.
    #[tokio::test]
    async fn an_unchanged_catalogue_imported_again_stores_nothing_new() {
        let source = ScriptedSource::new([
            Ok(updated(CATALOGUE, "\"one\"")),
            Ok(updated(CATALOGUE, "\"one\"")),
        ]);
        let store = InMemoryCatalogStore::new();
        let mut refresher = refresher(source, &store);
        refresher.restore(at(10)).await.expect("restore");
        refresher.refresh(RefreshTrigger::Scheduled, at(20)).await;

        let outcome = refresher.refresh(RefreshTrigger::Manual, at(30)).await;
        let RefreshOutcome::Admitted {
            admission,
            retention,
            ..
        } = outcome
        else {
            panic!("identical content is still an admitted import: {outcome:?}");
        };
        assert!(matches!(admission, Admission::Unchanged { .. }));
        assert_eq!(retention, Some(Retention::AlreadyRetained));
        assert_eq!(store.retained_count(), 1);
    }

    /// The semantic diff a changed catalogue produces is what an operator reads,
    /// so it has to be exact rather than "something changed".
    #[tokio::test]
    async fn a_changed_catalogue_reports_the_change_it_made() {
        let repriced = repriced();
        let source = ScriptedSource::new([
            Ok(updated(CATALOGUE, "\"one\"")),
            Ok(updated(&repriced, "\"two\"")),
        ]);
        let store = InMemoryCatalogStore::new();
        let mut refresher = refresher(source, &store);
        refresher.restore(at(10)).await.expect("restore");
        refresher.refresh(RefreshTrigger::Scheduled, at(20)).await;

        let outcome = refresher
            .refresh(RefreshTrigger::Scheduled, at(4_000))
            .await;
        let RefreshOutcome::Admitted {
            admission,
            retention,
            ..
        } = outcome
        else {
            panic!("changed content is admitted: {outcome:?}");
        };
        assert_eq!(retention, Some(Retention::Retained));
        let Admission::Updated { diff, .. } = admission else {
            panic!("changed content is an update");
        };
        let changes = diff.changes();
        assert_eq!(changes.len(), 1, "one price moved: {changes:?}");
        assert!(
            matches!(changes[0], CatalogChange::PriceChanged { .. }),
            "the diff names the price, not merely a difference: {:?}",
            changes[0]
        );
        assert_eq!(
            refresher
                .report(at(4_000))
                .last_diff
                .expect("the refresh report keeps the classification")
                .prices_changed,
            1
        );
        assert_eq!(
            store.retained_count(),
            2,
            "the superseded catalogue is retained, so a pin still resolves"
        );
    }

    /// The rule the whole slice exists for: a refused import cannot replace a
    /// good one, in memory or in the store.
    #[tokio::test]
    async fn a_malformed_payload_leaves_the_active_catalogue_alone() {
        let source = ScriptedSource::new([
            Ok(updated(CATALOGUE, "\"one\"")),
            Err(CatalogError::Invalid {
                backend: super::super::BackendKind::ModelsDev.as_str(),
                refusal: ModelsDevError::NotJson {
                    message: "expected value".to_owned(),
                }
                .refusal(),
                message: "expected value".to_owned(),
            }),
        ]);
        let store = InMemoryCatalogStore::new();
        let mut refresher = refresher(source, &store);
        refresher.restore(at(10)).await.expect("restore");
        refresher.refresh(RefreshTrigger::Scheduled, at(20)).await;
        let good = refresher.active().expect("active").content.content_id();

        let outcome = refresher
            .refresh(RefreshTrigger::Scheduled, at(4_000))
            .await;
        let RefreshOutcome::Refused {
            refusal, retry_in, ..
        } = outcome
        else {
            panic!("a malformed payload is refused: {outcome:?}");
        };
        assert_eq!(refusal.reason(), RefusalReason::NotJson);
        assert_eq!(retry_in, Duration::from_secs(60));
        assert_eq!(
            refresher
                .active()
                .expect("still active")
                .content
                .content_id(),
            good
        );
        let stored = store.load().await.expect("load");
        assert_eq!(stored.active.expect("still active").content_id(), good);
        assert_eq!(stored.consecutive_refusals, 1);
        assert_eq!(stored.last_refusal, Some(RefusalReason::NotJson));
    }

    /// An import the deployment cannot keep is an import the deployment does not
    /// serve: otherwise a replica would answer from a catalogue that vanishes on
    /// restart.
    #[tokio::test]
    async fn an_import_that_cannot_be_retained_is_not_admitted() {
        let source = ScriptedSource::new([Ok(updated(CATALOGUE, "\"one\""))]);
        let mut refresher = refresher(source, RefusingStore::default());
        refresher.restore(at(10)).await.expect("restore");

        let outcome = refresher.refresh(RefreshTrigger::Manual, at(20)).await;
        let RefreshOutcome::Refused { refusal, .. } = outcome else {
            panic!("a store failure refuses the import: {outcome:?}");
        };
        assert_eq!(refusal.reason(), RefusalReason::NotRetained);
        assert!(
            refresher.active().is_none(),
            "nothing became active that the deployment did not retain"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_source_that_does_not_answer_costs_one_refused_refresh() {
        let source = ScriptedSource::slow(Duration::from_secs(300));
        let mut refresher = refresher(source, InMemoryCatalogStore::new());
        refresher.restore(at(10)).await.expect("restore");

        let outcome = refresher.refresh(RefreshTrigger::Scheduled, at(20)).await;
        let RefreshOutcome::Refused { refusal, .. } = outcome else {
            panic!("a hanging upstream is refused: {outcome:?}");
        };
        assert_eq!(
            refusal.reason(),
            RefusalReason::Unreachable,
            "an upstream that did not answer in time is an upstream that did not answer"
        );
    }

    /// The ceiling is documented as covering fetch and retention together, and
    /// a store that hangs is the more dangerous half: the refresher would stop
    /// producing outcomes altogether, so nothing would count a refusal or
    /// report the staleness that followed.
    #[tokio::test(start_paused = true)]
    async fn a_store_that_does_not_answer_costs_one_refused_refresh_too() {
        let source = ScriptedSource::new([Ok(updated(CATALOGUE, "\"one\""))]);
        let mut refresher = refresher(source, HangingStore);
        refresher.restore(at(10)).await.expect("restore");

        let outcome = refresher.refresh(RefreshTrigger::Scheduled, at(20)).await;
        let RefreshOutcome::Refused { refusal, .. } = outcome else {
            panic!("a hanging store is refused rather than waited on: {outcome:?}");
        };
        assert_eq!(refusal.reason(), RefusalReason::Unreachable);
        assert!(refresher.active().is_none());
        assert_eq!(refresher.report(at(20)).consecutive_refusals, 1);
    }

    /// The retry sequence is a pure function of the failure count, so it is
    /// asserted exactly rather than as a range.
    #[tokio::test]
    async fn refused_refreshes_back_off_deterministically_and_reset_on_success() {
        let outage = || {
            Err(CatalogError::Unavailable {
                backend: super::super::BackendKind::ModelsDev.as_str(),
                refusal: Refusal::new(RefusalReason::Unreachable),
                message: "connection reset".to_owned(),
            })
        };
        let source = ScriptedSource::new([
            outage(),
            outage(),
            outage(),
            outage(),
            Ok(updated(CATALOGUE, "\"one\"")),
        ]);
        let mut refresher = refresher(source, InMemoryCatalogStore::new());
        refresher.restore(at(10)).await.expect("restore");

        let mut delays = Vec::new();
        for attempt in 0..4 {
            let outcome = refresher
                .refresh(RefreshTrigger::Manual, at(1_000 + attempt))
                .await;
            let RefreshOutcome::Refused { retry_in, .. } = outcome else {
                panic!("the upstream is down: {outcome:?}");
            };
            delays.push(retry_in);
        }
        assert_eq!(
            delays,
            vec![
                Duration::from_secs(60),
                Duration::from_secs(120),
                Duration::from_secs(240),
                Duration::from_secs(480),
            ]
        );

        let outcome = refresher.refresh(RefreshTrigger::Manual, at(2_000)).await;
        assert!(matches!(outcome, RefreshOutcome::Admitted { .. }));
        assert_eq!(
            refresher.next_due(),
            at(2_000 + 3_600),
            "a successful import returns to the ordinary cadence"
        );
    }

    #[tokio::test]
    async fn the_retry_ceiling_bounds_a_long_outage() {
        let source = ScriptedSource::new(
            std::iter::repeat_with(|| {
                Err(CatalogError::Unavailable {
                    backend: super::super::BackendKind::ModelsDev.as_str(),
                    refusal: Refusal::new(RefusalReason::Unreachable),
                    message: "connection reset".to_owned(),
                })
            })
            .take(12),
        );
        let mut refresher = refresher(source, InMemoryCatalogStore::new());
        refresher.restore(at(10)).await.expect("restore");
        let mut last = Duration::ZERO;
        for attempt in 0..12 {
            let outcome = refresher
                .refresh(RefreshTrigger::Manual, at(1_000 + attempt))
                .await;
            let RefreshOutcome::Refused { retry_in, .. } = outcome else {
                panic!("the upstream is down");
            };
            last = retry_in;
        }
        assert_eq!(last, Duration::from_secs(600), "the ceiling holds");
    }

    /// A scheduled refresh waits for its interval; an operator does not.
    #[tokio::test]
    async fn a_manual_refresh_runs_when_a_scheduled_one_would_wait() {
        let source = ScriptedSource::new([
            Ok(updated(CATALOGUE, "\"one\"")),
            Ok(CatalogRefresh::Unchanged {
                validators: SourceValidators::etag("\"one\""),
            }),
        ]);
        let mut refresher = refresher(source, InMemoryCatalogStore::new());
        refresher.restore(at(10)).await.expect("restore");
        refresher.refresh(RefreshTrigger::Scheduled, at(20)).await;

        assert_eq!(
            refresher.refresh(RefreshTrigger::Scheduled, at(30)).await,
            RefreshOutcome::NotDue {
                next_due: at(20 + 3_600)
            }
        );
        let outcome = refresher.refresh(RefreshTrigger::Manual, at(40)).await;
        assert!(
            matches!(
                outcome,
                RefreshOutcome::Admitted {
                    trigger: RefreshTrigger::Manual,
                    ..
                }
            ),
            "a manual refresh is never skipped: {outcome:?}"
        );
    }

    /// A `304` moves provenance in both places at once. If only the process
    /// recorded it, a restart would ask with a stale validator and transfer the
    /// whole document; if only the store did, the two would disagree about age.
    #[tokio::test]
    async fn an_unchanged_answer_ages_the_catalogue_forward_in_the_store_too() {
        let source = ScriptedSource::new([
            Ok(updated(CATALOGUE, "\"one\"")),
            Ok(CatalogRefresh::Unchanged {
                validators: SourceValidators::etag("\"two\""),
            }),
        ]);
        let store = InMemoryCatalogStore::new();
        let mut refresher = refresher(source, &store);
        refresher.restore(at(10)).await.expect("restore");
        refresher.refresh(RefreshTrigger::Scheduled, at(20)).await;

        let outcome = refresher.refresh(RefreshTrigger::Manual, at(5_000)).await;
        assert!(matches!(
            outcome,
            RefreshOutcome::Admitted {
                admission: Admission::Unchanged { .. },
                retention: None,
                ..
            }
        ));
        let stored = store.load().await.expect("load").active.expect("active");
        assert_eq!(stored.source.fetched_at, at(5_000));
        assert_eq!(stored.source.validators, SourceValidators::etag("\"two\""));
        assert_eq!(
            refresher.report(at(5_000)).active_age(),
            Some(Duration::ZERO)
        );
    }

    /// The mirror image: an unchanged answer to a request that carried no
    /// validator proves nothing, so it must not move the store's confirmation
    /// time or clear the store's refusal run either. A durable state that read
    /// as freshly checked here would be the one surface on which a catalogue
    /// nobody is verifying looks healthy.
    #[tokio::test]
    async fn an_unchanged_answer_nobody_asked_for_does_not_confirm_the_stored_catalogue() {
        let (snapshot, payload) = imported(CATALOGUE, SourceValidators::default());
        let source = ScriptedSource::new([
            Ok(CatalogRefresh::Updated {
                snapshot: Box::new(snapshot),
                payload,
            }),
            Ok(CatalogRefresh::Unchanged {
                validators: SourceValidators::etag("\"upstream\""),
            }),
        ]);
        let store = InMemoryCatalogStore::new();
        let mut refresher = refresher(source, &store);
        refresher.restore(at(10)).await.expect("restore");
        refresher.refresh(RefreshTrigger::Scheduled, at(20)).await;

        let outcome = refresher.refresh(RefreshTrigger::Manual, at(5_000)).await;
        let RefreshOutcome::Refused { refusal, .. } = outcome else {
            panic!("nothing was asked, so nothing was confirmed: {outcome:?}");
        };
        assert_eq!(refusal.reason(), RefusalReason::UnsolicitedUnchanged);
        let state = store.load().await.expect("load");
        let active = state.active.expect("active");
        assert_eq!(
            active.source.fetched_at,
            at(20),
            "the stored catalogue was last confirmed when it was imported"
        );
        assert_eq!(
            active.source.validators,
            SourceValidators::default(),
            "the refused answer's validator has no provenance to record"
        );
        assert_eq!(state.consecutive_refusals, 1);
        assert_eq!(
            state.last_refusal,
            Some(RefusalReason::UnsolicitedUnchanged),
            "the durable run reflects the refusal rather than being cleared by it"
        );
    }

    #[tokio::test]
    async fn a_conditional_refresh_asks_with_what_it_holds() {
        let source = ScriptedSource::new([
            Ok(updated(CATALOGUE, "\"one\"")),
            Ok(CatalogRefresh::Unchanged {
                validators: SourceValidators::etag("\"one\""),
            }),
        ]);
        let mut refresher = refresher(source, InMemoryCatalogStore::new());
        refresher.restore(at(10)).await.expect("restore");
        refresher.refresh(RefreshTrigger::Scheduled, at(20)).await;
        refresher.refresh(RefreshTrigger::Manual, at(30)).await;
        // The source is moved into the refresher, so the script is read back
        // through it.
        assert_eq!(
            refresher.source.asked_with(),
            vec![None, Some(SourceValidators::etag("\"one\""))]
        );
    }

    #[tokio::test]
    async fn an_air_gapped_deployment_boots_on_the_seed_and_retains_it() {
        let store = InMemoryCatalogStore::new();
        let mut refresher = CatalogRefresher::new(
            ScriptedSource::new([]),
            &store,
            RefreshSchedule::default(),
            Bootstrap::Seed,
            at(0),
        )
        .expect("a valid schedule");

        let restored = refresher.restore(at(1_000)).await.expect("restore");
        let Restored::Seeded { content_id } = restored else {
            panic!("an empty store seeds: {restored:?}");
        };
        assert_eq!(content_id, seed_snapshot().content.content_id());
        assert_eq!(
            refresher.report(at(1_000)).active_age(),
            Some(Duration::ZERO),
            "a seed imported now is fresh, whatever day its fixture was cut"
        );
        let stored = store.load().await.expect("load").active.expect("active");
        assert_eq!(
            hydrate(&stored).expect("the retained seed rehydrates"),
            {
                let mut expected = seed_snapshot();
                expected.source.fetched_at = at(1_000);
                expected
            },
            "the seed is retained as an import like any other"
        );
    }

    /// The seed is content, not a claim that an upstream confirmed anything: an
    /// upstream answering `304` against a validator the seed never sent is
    /// refused rather than credited.
    #[tokio::test]
    async fn a_seeded_deployment_cannot_be_confirmed_by_an_upstream_304() {
        let mut refresher = CatalogRefresher::new(
            ScriptedSource::new([Ok(CatalogRefresh::Unchanged {
                validators: SourceValidators::etag("\"upstream\""),
            })]),
            InMemoryCatalogStore::new(),
            RefreshSchedule::default(),
            Bootstrap::Seed,
            at(0),
        )
        .expect("a valid schedule");
        refresher.restore(at(1_000)).await.expect("restore");
        let seeded = refresher.active().expect("seeded").content.content_id();

        let outcome = refresher.refresh(RefreshTrigger::Manual, at(2_000)).await;
        let RefreshOutcome::Admitted { admission, .. } = outcome else {
            panic!("the seed's own validator was sent, so this confirms it: {outcome:?}");
        };
        assert_eq!(admission.content_id(), seeded);
        assert_eq!(
            refresher.source.asked_with(),
            vec![Some(SourceValidators::etag(format!("W/\"seed-{seeded}\"")))],
            "the conditional request carries the seed's own tag, which no upstream can match"
        );
    }

    #[tokio::test]
    async fn a_stored_catalogue_that_no_longer_rehydrates_is_refused_rather_than_served() {
        let store = InMemoryCatalogStore::new();
        let (snapshot, _) = imported(CATALOGUE, SourceValidators::etag("\"one\""));
        store
            .activate(
                &RetainedCatalog {
                    source: snapshot.source,
                    // Bytes that are not the ones the record's digest names.
                    payload: RawPayload::new(&b"{\"models\":{},\"providers\":{}}"[..]),
                },
                at(10),
            )
            .await
            .expect("activate");

        let mut refresher = refresher(ScriptedSource::new([]), &store);
        let error = refresher
            .restore(at(20))
            .await
            .expect_err("a damaged record is not served");
        assert!(matches!(
            error,
            RefreshError::Stored(HydrationError::Payload { .. })
        ));
        assert!(refresher.active().is_none());
        assert_eq!(refresher.report(at(20)).consecutive_refusals, 1);
    }

    /// And it is counted *onto* the run the store recorded, not instead of it.
    /// A deployment that has been refusing for a week and then cannot read its
    /// own stored catalogue is at its least healthy; reporting one refusal
    /// would be the moment the alarm reset itself.
    #[tokio::test]
    async fn an_unreadable_stored_catalogue_is_counted_onto_the_run_the_store_recorded() {
        let store = InMemoryCatalogStore::new();
        let (snapshot, _) = imported(CATALOGUE, SourceValidators::etag("\"one\""));
        store
            .activate(
                &RetainedCatalog {
                    source: snapshot.source,
                    payload: RawPayload::new(&b"{\"models\":{},\"providers\":{}}"[..]),
                },
                at(10),
            )
            .await
            .expect("activate");
        for _ in 0..PERSISTENT_REFUSAL_THRESHOLD {
            store
                .refuse(RefusalReason::Unreachable, at(15))
                .await
                .expect("refuse");
        }

        let mut refresher = refresher(ScriptedSource::new([]), &store);
        refresher
            .restore(at(20))
            .await
            .expect_err("a damaged record is not served");

        let report = refresher.report(at(20));
        assert_eq!(
            report.consecutive_refusals,
            PERSISTENT_REFUSAL_THRESHOLD + 1
        );
        assert_eq!(report.last_refusal, Some(RefusalReason::NotRetained));
        assert!(report.persistent_refusal());
    }

    #[test]
    fn a_schedule_that_would_pace_a_failing_deployment_worse_than_a_healthy_one_is_refused() {
        let base = RefreshSchedule::default();
        assert_eq!(base.validate(), Ok(()));
        assert_eq!(
            RefreshSchedule {
                interval: Duration::ZERO,
                ..base
            }
            .validate(),
            Err(InvalidSchedule::ZeroInterval)
        );
        assert_eq!(
            RefreshSchedule {
                timeout: Duration::from_secs(7 * 60 * 60),
                ..base
            }
            .validate(),
            Err(InvalidSchedule::TimeoutExceedsInterval {
                timeout: Duration::from_secs(7 * 60 * 60),
                interval: base.interval,
            })
        );
        let far = BackoffPolicy {
            max: Duration::from_secs(24 * 60 * 60),
            ..base.backoff
        };
        assert_eq!(
            RefreshSchedule {
                backoff: far,
                ..base
            }
            .validate(),
            Err(InvalidSchedule::BackoffExceedsInterval {
                max: far.max,
                interval: base.interval,
            })
        );
    }

    /// An upstream cannot enable, disable, or reprice anything: it can only make
    /// a human's list of things to look at longer.
    #[tokio::test]
    async fn a_refresh_reports_what_it_would_mean_for_operators_and_changes_nothing() {
        use crate::desired_state::Checksum;
        use crate::desired_state::fixtures::{resource_id, tenant_id};
        use crate::desired_state::models::{
            CatalogOffering, ModelEnablementBody, ModelLifecycle, ModelOwner, WireFamily,
        };

        let (first, first_payload) = imported(CATALOGUE, SourceValidators::etag("\"one\""));
        let enabled = first.content.models()[0].offerings[0].clone();
        let pinned = Checksum::of(first_payload.as_bytes());
        let offering = CatalogOffering::new(
            OfferingId::of(enabled.provider.as_str(), enabled.model.as_str()).expect("an id"),
            pinned,
        );
        let enablement = ModelEnablementBody::new(
            resource_id(1),
            ModelOwner::tenant(tenant_id(1)),
            offering,
            WireFamily::OpenaiChat,
        );

        // The same catalogue, one price different: a new snapshot, a new blob.
        let repriced = repriced();
        let (second, second_payload) = imported(&repriced, SourceValidators::etag("\"two\""));
        let impact = RefreshImpact::of(
            [&enablement],
            &second.content,
            Checksum::of(second_payload.as_bytes()),
        );
        assert_eq!(
            impact,
            RefreshImpact {
                pins_unmoved: 1,
                withdrawn: BTreeSet::new(),
            },
            "a refresh does not move a pin, and the offering is still published"
        );
        assert_eq!(
            enablement.state(),
            ModelLifecycle::Enabled,
            "and it changed nothing about the operator's decision"
        );
        assert!(enablement.billable_price().is_none());
        assert!(enablement.offering().is_pinned_to(pinned));
    }

    #[tokio::test]
    async fn an_offering_the_upstream_withdrew_is_reported_and_not_acted_on() {
        use crate::desired_state::Checksum;
        use crate::desired_state::fixtures::{resource_id, tenant_id};
        use crate::desired_state::models::{
            CatalogOffering, ModelEnablementBody, ModelOwner, WireFamily,
        };

        let (snapshot, payload) = imported(CATALOGUE, SourceValidators::etag("\"one\""));
        let gone = OfferingId::of("openai", "a-model-that-was-withdrawn").expect("an id");
        let enablement = ModelEnablementBody::new(
            resource_id(2),
            ModelOwner::tenant(tenant_id(1)),
            CatalogOffering::new(gone, Checksum::of(payload.as_bytes())),
            WireFamily::OpenaiChat,
        );

        let impact = RefreshImpact::of(
            [&enablement],
            &snapshot.content,
            Checksum::of(payload.as_bytes()),
        );
        assert_eq!(impact.pins_unmoved, 0, "this enablement pins this snapshot");
        assert_eq!(
            impact.withdrawn,
            [gone].into_iter().collect::<BTreeSet<_>>()
        );
    }

    #[test]
    fn the_schema_version_a_refresher_stores_is_the_one_it_parsed() {
        let (snapshot, _) = imported(CATALOGUE, SourceValidators::default());
        assert_eq!(
            snapshot.source.schema_version,
            SchemaVersion::MODELS_DEV_CATALOG_V1
        );
        let rebuilt = source_snapshot(
            snapshot.source.source_url.clone(),
            snapshot.source.schema_version,
            CATALOGUE.as_bytes(),
            &snapshot.content,
            SourceValidators::default(),
            at(0),
        );
        assert_eq!(rebuilt.content_id, snapshot.source.content_id);
    }
}
