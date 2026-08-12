//! The convergence loop: observe, hydrate, compile, publish.
//!
//! One task per replica, off the request path, doing one thing: making the active
//! snapshot equal the control plane's desired revision, or reporting exactly why
//! it is not.
//!
//! # Why polling is the mechanism
//!
//! Postgres `LISTEN`/`NOTIFY` is fire-and-forget: a notification delivered while
//! a replica is reconnecting is simply gone, and a replica that treated
//! notifications as its trigger would sit on a stale snapshot indefinitely with
//! nothing to report. So the poll is the *correctness* mechanism and a
//! notification only shortens the wait ([`ChangeSignal`]). Turning notifications
//! off costs latency, never convergence — and that is what the missed-notification
//! test asserts.
//!
//! # Why a failed candidate cannot half-apply
//!
//! [`converge_once`](Reconciler::converge_once) never holds the running snapshot.
//! It hydrates a *complete* revision, compiles it into a whole
//! [`ConfigSnapshot`], and only then hands that snapshot to the sink in one
//! atomic store. Fetch, hydration, validation, compilation, and secret resolution
//! all fail *before* anything is published, so "the previous revision keeps
//! serving" is a consequence of the control flow rather than a cleanup path.
//!
//! # Convergence targets
//!
//! With the defaults in [`ConvergenceSettings`], a healthy replica publishes a
//! new revision within one poll interval plus its compile time — under a second
//! when notifications are delivered, within five seconds when they are not. A
//! replica that cannot reach the control plane retries on a bounded exponential
//! backoff up to 30 seconds and keeps serving, and its lag is reported the whole
//! time (see [`super::status`]).

use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::Notify;

use super::backoff::Backoff;
use super::compile::{CandidateCompiler, CompileError};
use super::lkg::{LastKnownGood, LastKnownGoodError};
use super::settings::ConvergenceSettings;
use super::status::{Clock, Rejection, RevisionReport, RevisionStatus, SnapshotSource};
use crate::backends::BackendFailure;
use crate::backends::control_plane::{ControlPlaneError, ControlPlaneStore};
use crate::desired_state::{LoadedRevision, RevisionId};
use crate::state::{AppState, ConfigSnapshot};
use crate::telemetry;

/// Where a published snapshot goes.
///
/// A trait rather than a direct [`AppState`] dependency so convergence is
/// testable without a process's worth of resources — and so the *only* thing the
/// reconciler can do to the running config is replace it wholesale.
pub trait SnapshotSink: Send + Sync {
    /// Replace the serving snapshot atomically. In-flight requests keep the
    /// snapshot they already hold.
    fn publish(&self, snapshot: ConfigSnapshot);

    /// The generation currently serving, which the next candidate increments.
    fn generation(&self) -> u64;
}

impl SnapshotSink for AppState {
    fn publish(&self, snapshot: ConfigSnapshot) {
        AppState::publish(self, snapshot);
    }

    fn generation(&self) -> u64 {
        self.config().generation
    }
}

/// A hint that desired state changed.
///
/// Optional by construction: nothing here carries the change itself, so a lost
/// signal costs at most one poll interval. A Postgres `LISTEN` task calls
/// [`ChangeSignal::notify`]; a deployment without notifications simply never
/// does.
#[derive(Debug, Default)]
pub struct ChangeSignal {
    notify: Notify,
}

impl ChangeSignal {
    pub fn new() -> Self {
        Self::default()
    }

    /// Wake the reconciler now instead of at its next poll.
    pub fn notify(&self) {
        self.notify.notify_one();
    }

    async fn notified(&self) {
        self.notify.notified().await;
    }
}

/// What one convergence attempt did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// A candidate was compiled and published; requests started after this see
    /// it.
    Published {
        revision: RevisionId,
        generation: u64,
        took: Duration,
    },
    /// Desired state already equals what is active. The common case, and
    /// deliberately not free of a control-plane read: that read is what detects a
    /// change *and* what proves the control plane is reachable.
    AlreadyConverged { revision: Option<RevisionId> },
    /// The control plane has published nothing yet.
    Empty,
    /// A candidate was refused. The previous revision keeps serving.
    Rejected {
        revision: Option<RevisionId>,
        reason: &'static str,
    },
}

impl Outcome {
    /// A stable label for metrics.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Published { .. } => "published",
            Self::AlreadyConverged { .. } => "converged",
            Self::Empty => "empty",
            Self::Rejected { .. } => "rejected",
        }
    }
}

/// Why one attempt did not produce a published snapshot.
#[derive(Debug, thiserror::Error)]
enum AttemptError {
    #[error(transparent)]
    Store(#[from] ControlPlaneError),
    #[error(transparent)]
    Compile(#[from] CompileError),
}

impl AttemptError {
    /// The low-cardinality reason label. Store failures are classified by the
    /// backend's own category, so "unavailable" (retry) and "corrupt" (page
    /// someone) never collapse into one bucket.
    fn reason(&self) -> &'static str {
        match self {
            Self::Store(error) => match error.category() {
                crate::backends::FailureCategory::Unavailable => "unavailable",
                crate::backends::FailureCategory::Conflict => "conflict",
                crate::backends::FailureCategory::NotFound => "not_found",
                crate::backends::FailureCategory::Invalid => "invalid",
                crate::backends::FailureCategory::Denied => "denied",
                crate::backends::FailureCategory::Corrupt => "corrupt",
            },
            Self::Compile(error) => error.reason(),
        }
    }

    fn revision(&self) -> Option<RevisionId> {
        match self {
            Self::Store(ControlPlaneError::Corrupt { revision, .. })
            | Self::Store(ControlPlaneError::RevisionNotFound(revision))
            | Self::Store(ControlPlaneError::TooLarge { revision, .. }) => Some(*revision),
            Self::Store(_) => None,
            Self::Compile(error) => Some(error.revision()),
        }
    }
}

/// Why a stateful replica could not reach a servable snapshot at boot.
#[derive(Debug, thiserror::Error)]
pub enum BootstrapError {
    /// Reachable, but nothing has ever been published. A stateful replica has no
    /// implicit empty configuration to fall back to: it would serve a gateway
    /// with no aliases and no keys while reporting itself healthy.
    #[error(
        "the control plane is reachable but has published no revision; a stateful replica \
         has nothing to serve until desired state exists"
    )]
    Empty,
    /// The control plane was unreachable and no signed cache was available.
    #[error(
        "the control plane is unreachable and no last-known-good snapshot is available: {source}"
    )]
    Unavailable {
        #[source]
        source: ControlPlaneError,
    },
    /// The control plane answered, but with something a retry cannot clear:
    /// unreadable storage, a refused read, a revision larger than this build
    /// hydrates. Never answered from cache — cached state would mask storage an
    /// operator has to repair.
    #[error("the control plane refused to yield desired state: {source}")]
    Store {
        #[source]
        source: ControlPlaneError,
    },
    /// The desired revision exists but does not compile. Fatal at boot on
    /// purpose: there is no previous revision to keep serving.
    #[error("the desired revision cannot be served: {source}")]
    Rejected {
        #[source]
        source: Box<CompileError>,
    },
    /// The cache was consulted and refused itself: unauthentic, corrupt, or
    /// unreadable. Never downgraded to "boot empty".
    #[error("the last-known-good snapshot could not be restored: {source}")]
    Cache {
        #[source]
        source: Box<LastKnownGoodError>,
    },
}

/// One replica's convergence loop.
pub struct Reconciler {
    store: Arc<dyn ControlPlaneStore>,
    compiler: Arc<dyn CandidateCompiler>,
    sink: Arc<dyn SnapshotSink>,
    status: Arc<RevisionStatus>,
    settings: ConvergenceSettings,
    cache: Option<LastKnownGood>,
    clock: Arc<dyn Clock>,
    /// The revision the sink is serving, as this reconciler last published it.
    /// Compared against desired to decide whether to hydrate at all.
    active: Mutex<Option<RevisionId>>,
    backoff: Mutex<Backoff>,
    /// Whether the last export failed, so a recovering disk is logged once
    /// rather than every attempt.
    export_failing: AtomicBool,
}

impl Reconciler {
    pub fn new(
        store: Arc<dyn ControlPlaneStore>,
        compiler: Arc<dyn CandidateCompiler>,
        sink: Arc<dyn SnapshotSink>,
        settings: ConvergenceSettings,
        cache: Option<LastKnownGood>,
        clock: Arc<dyn Clock>,
    ) -> Self {
        let status = Arc::new(RevisionStatus::new(Box::new(ArcClock(Arc::clone(&clock)))));
        Self {
            store,
            compiler,
            sink,
            status,
            backoff: Mutex::new(Backoff::new(settings.backoff)),
            settings,
            cache,
            clock,
            active: Mutex::new(None),
            export_failing: AtomicBool::new(false),
        }
    }

    /// What this replica reports about itself: desired, loaded, active, lag, and
    /// the last refusal.
    pub fn report(&self) -> RevisionReport {
        self.status.report()
    }

    /// Shared status, for the telemetry and administrative readers that observe
    /// convergence without driving it.
    pub fn status(&self) -> &Arc<RevisionStatus> {
        &self.status
    }

    /// Reach a first servable snapshot, or explain why the replica must not
    /// start.
    ///
    /// The cache is consulted for exactly one failure — the control plane being
    /// unreachable — because that is the only one where cached state is the
    /// better answer. A revision that does not compile is fatal here: booting
    /// from an older cached revision would silently serve state an operator
    /// already replaced.
    pub async fn bootstrap(&self) -> Result<RevisionId, BootstrapError> {
        let span = telemetry::revision_convergence_span(telemetry::CONVERGENCE_BOOT);
        let result = self.bootstrap_inner().await;
        let outcome = match &result {
            Ok(revision) => Outcome::Published {
                revision: *revision,
                generation: self.status.report().generation,
                took: self.status.report().last_convergence.unwrap_or_default(),
            },
            Err(BootstrapError::Empty) => Outcome::Empty,
            Err(_) => Outcome::Rejected {
                revision: None,
                reason: self
                    .status
                    .report()
                    .last_rejection
                    .map_or("boot", |rejection| rejection.reason),
            },
        };
        telemetry::finish_revision_convergence(
            &span,
            telemetry::CONVERGENCE_BOOT,
            &outcome,
            &self.status.report(),
        );
        result
    }

    async fn bootstrap_inner(&self) -> Result<RevisionId, BootstrapError> {
        match self.attempt().await {
            Ok(Some(published)) => Ok(published),
            Ok(None) => Err(BootstrapError::Empty),
            Err(error) => {
                self.record_failure(&error);
                match error {
                    AttemptError::Store(source) if source.retryable() => {
                        self.restore_from_cache(source).await
                    }
                    AttemptError::Store(source) => Err(BootstrapError::Store { source }),
                    AttemptError::Compile(source) => Err(BootstrapError::Rejected {
                        source: Box::new(source),
                    }),
                }
            }
        }
    }

    /// One full convergence step. Deterministic and independently callable, which
    /// is what the tests drive instead of racing the loop's timers.
    pub async fn converge_once(&self, trigger: &'static str) -> Outcome {
        let span = telemetry::revision_convergence_span(trigger);
        let outcome = match self.attempt().await {
            Ok(Some(revision)) => {
                let report = self.status.report();
                Outcome::Published {
                    revision,
                    generation: report.generation,
                    took: report.last_convergence.unwrap_or_default(),
                }
            }
            Ok(None) => match *self.active.lock().expect("not poisoned") {
                Some(revision) => Outcome::AlreadyConverged {
                    revision: Some(revision),
                },
                None => Outcome::Empty,
            },
            Err(error) => {
                let reason = self.record_failure(&error);
                Outcome::Rejected {
                    revision: error.revision(),
                    reason,
                }
            }
        };
        telemetry::finish_revision_convergence(&span, trigger, &outcome, &self.status.report());
        outcome
    }

    /// Poll, converge, and back off, until `shutdown` completes.
    ///
    /// The wait is a race between the poll interval, a change signal, and
    /// shutdown, so a notification shortens the wait without changing what the
    /// loop does when it wakes. A failing attempt replaces the poll interval with
    /// the backoff delay, which is why an outage cannot become a hot loop.
    pub async fn run(&self, signal: Arc<ChangeSignal>, shutdown: impl Future<Output = ()> + Send) {
        let shutdown = std::pin::pin!(shutdown);
        let mut shutdown = shutdown;
        loop {
            let delay = {
                let backoff = self.backoff.lock().expect("not poisoned");
                if backoff.failures() == 0 {
                    self.settings.poll_interval
                } else {
                    backoff.delay()
                }
            };
            tokio::select! {
                biased;
                () = &mut shutdown => {
                    tracing::debug!("revision convergence stopped");
                    return;
                }
                () = signal.notified() => {
                    self.converge_once(telemetry::CONVERGENCE_NOTIFIED).await;
                }
                () = tokio::time::sleep(delay) => {
                    self.converge_once(telemetry::CONVERGENCE_POLLED).await;
                }
            }
        }
    }

    /// The attempt itself: `Ok(None)` means there was nothing to do.
    async fn attempt(&self) -> Result<Option<RevisionId>, AttemptError> {
        let started = self.clock.now();
        // The cheap read first. It answers "is there anything to do?" without
        // hydrating bodies, and it is also this replica's liveness check against
        // the control plane, which is why it runs even when converged.
        let desired = self.store.desired_revision().await?;
        self.status.observe_desired(desired);
        let active = *self.active.lock().expect("not poisoned");
        if desired.is_none() || desired == active {
            self.backoff.lock().expect("not poisoned").succeed();
            return Ok(None);
        }

        // Hydration is a single consistent read of the *complete* revision
        // (#166), not a re-read of the id above: if a newer revision was
        // published between the two calls, converging straight to it is correct.
        let Some(revision) = self.store.load_desired_revision().await? else {
            self.backoff.lock().expect("not poisoned").succeed();
            return Ok(None);
        };
        self.status.observe_desired(Some(revision.id()));
        self.publish(revision, SnapshotSource::ControlPlane, started)
            .map(Some)
            .map_err(AttemptError::from)
    }

    /// Compile and publish a hydrated revision.
    ///
    /// Compilation happens before the sink is touched at all, so the failure path
    /// leaves the running snapshot exactly as it was.
    fn publish(
        &self,
        revision: LoadedRevision,
        source: SnapshotSource,
        started: std::time::Instant,
    ) -> Result<RevisionId, CompileError> {
        let id = revision.id();
        let generation = self.sink.generation().saturating_add(1);
        let snapshot = self.compiler.compile(&revision, generation)?;
        self.status.observe_loaded(id);

        self.sink.publish(snapshot);
        *self.active.lock().expect("not poisoned") = Some(id);
        self.backoff.lock().expect("not poisoned").succeed();
        let took = self.clock.now().saturating_duration_since(started);
        self.status.record_published(id, generation, source, took);
        tracing::info!(
            revision = %id,
            generation,
            source = source.as_str(),
            took_ms = took.as_millis(),
            "published desired revision"
        );
        self.export(&revision);
        Ok(id)
    }

    /// Write the revision this replica just published to its signed cache.
    ///
    /// A cache failure is logged and counted, never propagated: the replica is
    /// already serving the revision, and refusing to serve because a *cache* is
    /// unwritable would turn a full disk into an outage.
    fn export(&self, revision: &LoadedRevision) {
        let Some(cache) = &self.cache else {
            return;
        };
        match cache.export(revision) {
            Ok(()) => {
                if self.export_failing.swap(false, Ordering::Relaxed) {
                    tracing::info!(
                        path = %cache.path().display(),
                        "last-known-good snapshot is writable again"
                    );
                }
                telemetry::record_last_known_good("exported");
            }
            Err(error) => {
                telemetry::record_last_known_good("export_failed");
                if !self.export_failing.swap(true, Ordering::Relaxed) {
                    tracing::warn!(
                        path = %cache.path().display(),
                        error = %error,
                        "the last-known-good snapshot could not be written; the replica keeps \
                         serving, but a cold boot during a control-plane outage will have no \
                         cached state"
                    );
                }
            }
        }
    }

    /// Boot from the signed cache because the control plane is unreachable.
    async fn restore_from_cache(
        &self,
        source: ControlPlaneError,
    ) -> Result<RevisionId, BootstrapError> {
        let Some(cache) = &self.cache else {
            return Err(BootstrapError::Unavailable { source });
        };
        let restored = cache.load().map_err(|source| BootstrapError::Cache {
            source: Box::new(source),
        })?;
        let Some(revision) = restored else {
            return Err(BootstrapError::Unavailable { source });
        };
        let started = self.clock.now();
        let id = self
            .publish(revision, SnapshotSource::LastKnownGood, started)
            .map_err(|source| BootstrapError::Rejected {
                source: Box::new(source),
            })?;
        telemetry::record_last_known_good("restored");
        tracing::warn!(
            revision = %id,
            error = %source,
            "the control plane is unreachable; booted from the signed last-known-good snapshot, \
             which may be older than desired state"
        );
        Ok(id)
    }

    /// Record a refusal and its backoff, and return the reason label.
    fn record_failure(&self, error: &AttemptError) -> &'static str {
        let reason = error.reason();
        let (failures, delay) = {
            let mut backoff = self.backoff.lock().expect("not poisoned");
            let delay = backoff.fail();
            (backoff.failures(), delay)
        };
        self.status.record_rejection(
            Rejection {
                revision: error.revision(),
                reason,
                detail: error.to_string(),
            },
            failures,
        );
        telemetry::record_revision_rejection(reason);
        tracing::warn!(
            reason,
            failures,
            retry_in_ms = delay.as_millis(),
            error = %error,
            "desired revision was not applied; the active revision keeps serving"
        );
        reason
    }
}

/// A [`Clock`] that shares one implementation between the reconciler and the
/// status it reports through, so a test's clock governs both.
struct ArcClock(Arc<dyn Clock>);

impl Clock for ArcClock {
    fn now(&self) -> std::time::Instant {
        self.0.now()
    }
}
