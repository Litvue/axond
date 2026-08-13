//! The delivery worker: what turns a durable append into a written row.
//!
//! One task per gateway, claiming a bounded batch, writing it to the sinks the
//! journal delivers to, and acknowledging only what landed. Everything it does
//! is a retry of something it may already have done — a lease expires while a
//! write is in flight, a process dies between the write and the acknowledgement —
//! so the destinations must be idempotent on `request_id`. That is the contract
//! at-least-once delivery buys: no lost event, at the price of a duplicate a
//! constraint absorbs (`docs/usage-schema.md`).
//!
//! # Why a failed write is not retried here
//!
//! A batch the destination rejected is left claimed and unacknowledged rather
//! than retried in a tight loop. Its lease expires, the next claim hands it back
//! as a redelivery with the attempt number incremented, and the attempt budget in
//! [`Capacity::max_delivery_attempts`](super::Capacity::max_delivery_attempts)
//! eventually quarantines it. So the lease *is* the backoff, and a poison event
//! cannot block its ordering key forever.
//!
//! That budget is only spent on an event the destination refused *on its own
//! account*, which [`DeliveryWorker::deliver`] establishes by halving a refused
//! batch until the refusal is isolated. A destination that accepts nothing is an
//! outage, not a verdict, and an outage may not condemn anything however long it
//! lasts.
//!
//! # Shutdown
//!
//! [`WorkerHandle::drain`] gets a bound, like every other shutdown step
//! (ADR 0029). What it reports is deliberately not "records lost": an event still
//! in the outbox at exit is durable and will be delivered by the next process to
//! start. The [`DrainReport`] says how far behind delivery was left, because
//! *that* is what an operator has to know before they decommission the replica.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::{Claim, ConsumerId, Delivery, JournalError, JournalStats, PoisonReason, UsageJournal};
use crate::usage::{ObservedRecord, UsageSink};

/// How long past its budget a worker is waited for, to cover the batch it is
/// already writing.
const DRAIN_MARGIN: Duration = Duration::from_secs(1);

/// The bound on a backlog read taken after the delivery budget is spent. Kept
/// below [`DRAIN_MARGIN`] so a closing read can neither make a worker that
/// stopped correctly look abandoned nor push shutdown past the flush timeout.
const CLOSING_READ: Duration = Duration::from_millis(500);

/// How many refused writes one delivery pass may spend isolating a refusal
/// before a batch nothing has been accepted from is taken as the destination
/// being down.
///
/// The bisection has to reach single events to attribute a refusal, and in a
/// real outage every one of those probes fails, so without a bound a claim of
/// 256 would beat on a dead destination 512 times. Anything under this bound is
/// enough to isolate the handful of poison events a destination refuses while
/// it is healthy, which is what the budget exists for.
const PROBE_WRITES: usize = 32;

/// How the worker claims and how often.
#[derive(Debug, Clone)]
pub struct WorkerSettings {
    /// The consumer name delivery state is kept under. Stable across restarts:
    /// a renamed consumer starts from the beginning of the retained outbox.
    pub consumer: ConsumerId,
    pub claim_batch: usize,
    /// How long a claimed batch stays invisible to other claimants. Must exceed
    /// the slowest write the destinations do, or a live delivery is redelivered
    /// beside itself.
    pub lease: Duration,
    /// How long the worker waits after finding nothing to deliver.
    pub poll_interval: Duration,
    /// How often retention is applied. Independent of delivery: an append at
    /// capacity reclaims what it needs itself.
    pub maintain_interval: Duration,
}

impl Default for WorkerSettings {
    fn default() -> Self {
        Self {
            consumer: ConsumerId::parse("billing").expect("a static consumer name"),
            claim_batch: 256,
            lease: Duration::from_secs(30),
            poll_interval: Duration::from_millis(250),
            maintain_interval: Duration::from_secs(60),
        }
    }
}

/// What one drain achieved, and what it left behind.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct DrainReport {
    /// Events acknowledged by this worker over its whole life.
    pub delivered: u64,
    /// Batches the destinations refused. Each one is retried after its lease
    /// expires, so this is a rate to alert on rather than a loss.
    pub failed: u64,
    /// Events still waiting or leased when the worker stopped. Durable, not
    /// lost: the next process to start delivers them.
    pub undelivered: u64,
    /// Events set aside as poison, awaiting an operator.
    pub quarantined: u64,
    /// Whether delivery was caught up when the worker stopped.
    pub drained: bool,
    /// Whether the worker handed this report back itself. False when it had to
    /// be abandoned at its bound, in which case `delivered` and `failed` are
    /// unknown rather than zero — the difference an operator deciding whether a
    /// replica is finished has to see.
    pub reported: bool,
    /// Whether the backlog counters were read from the journal at all.
    pub counted: bool,
}

impl DrainReport {
    fn observe(&mut self, stats: JournalStats) {
        self.undelivered = stats.pending + stats.in_flight;
        self.quarantined = stats.quarantined;
        self.drained = stats.is_drained();
        self.counted = true;
    }

    pub fn log(&self) {
        if !self.reported {
            // Deliberately does not print `delivered`/`failed`: a report the
            // worker never handed back knows nothing about them, and a zero
            // there reads as "nothing was delivered".
            if self.counted {
                tracing::error!(
                    undelivered = self.undelivered,
                    quarantined = self.quarantined,
                    "usage journal worker did not stop within the shutdown bound; the backlog \
                     below is durable and will be delivered after restart, and this run's \
                     delivered count is unknown"
                );
            } else {
                tracing::error!(
                    "usage journal worker did not stop within the shutdown bound and its backlog \
                     could not be read; the events are durable and will be delivered after restart"
                );
            }
            return;
        }
        if self.drained {
            tracing::info!(
                delivered = self.delivered,
                quarantined = self.quarantined,
                "usage journal drained on shutdown"
            );
        } else {
            // Not an error: the events are durable. It is a warning because the
            // replica is being taken away from work it had not finished.
            tracing::warn!(
                delivered = self.delivered,
                undelivered = self.undelivered,
                quarantined = self.quarantined,
                failed = self.failed,
                "usage journal was not drained within the shutdown bound; the events are \
                 durable and will be delivered after restart"
            );
        }
    }
}

/// Drains a [`UsageJournal`] into the sinks it delivers to.
pub struct DeliveryWorker {
    journal: Arc<dyn UsageJournal>,
    /// The destinations an acknowledgement speaks for. Unbuffered on purpose: a
    /// batching sink would make the acknowledgement a lie, because it returns
    /// before the row exists.
    sinks: Arc<Vec<Box<dyn UsageSink>>>,
    settings: WorkerSettings,
}

impl DeliveryWorker {
    pub fn new(
        journal: Arc<dyn UsageJournal>,
        sinks: Arc<Vec<Box<dyn UsageSink>>>,
        settings: WorkerSettings,
    ) -> Self {
        Self {
            journal,
            sinks,
            settings,
        }
    }

    /// Start delivering, and hand back the handle shutdown drains through.
    pub fn spawn(self) -> WorkerHandle {
        let (stop, receiver) = watch::channel(None);
        let (journal, consumer) = (Arc::clone(&self.journal), self.settings.consumer.clone());
        WorkerHandle {
            stop,
            task: tokio::spawn(self.run(receiver)),
            journal,
            consumer,
        }
    }

    async fn run(self, mut stop: watch::Receiver<Option<Duration>>) -> DrainReport {
        let mut report = DrainReport {
            reported: true,
            ..DrainReport::default()
        };
        let mut next_maintain = Instant::now() + self.settings.maintain_interval;
        let budget = loop {
            self.pump_until_idle(&mut report, &stop, &mut next_maintain)
                .await;
            // Not once the stop signal is in: housekeeping is three journal
            // operations, each bounded only by the journal's own operation
            // timeout, and starting one here would spend the shutdown bound on
            // work the next process does anyway.
            if !stop.has_changed().unwrap_or(true) {
                self.maintain_if_due(&mut next_maintain).await;
            }
            tokio::select! {
                changed = stop.changed() => {
                    // A dropped sender is a process that is going away without a
                    // budget to give, so the drain phase is empty rather than
                    // unbounded.
                    break changed.ok().and_then(|()| *stop.borrow_and_update()).unwrap_or_default();
                }
                _ = tokio::time::sleep(self.settings.poll_interval) => {}
            }
        };

        let deadline = Instant::now() + budget;
        // Bounded inside the pass rather than only between passes: one pass is
        // a claim, a destination write, and an acknowledgement per event, each
        // carrying the journal's operation timeout, so a full batch against a
        // slow destination can outlast the whole budget on its own. Cutting one
        // off costs nothing — an unacknowledged delivery's lease expires and
        // the next process claims it again — whereas overrunning the budget
        // gets the worker abandoned and its counts reported as unknown.
        while let Some(remaining) = deadline.checked_duration_since(Instant::now()) {
            match tokio::time::timeout(remaining, self.pump(&mut report)).await {
                Ok(Ok(0)) | Ok(Err(_)) | Err(_) => break,
                Ok(Ok(_)) => {}
            }
        }
        // Bounded separately from the delivery budget, and by less than the
        // margin `WorkerHandle::drain` waits: a journal operation carries its
        // own timeout, which can be many times that margin, and a worker
        // abandoned for a slow closing read would be reported as never having
        // stopped when it had.
        if let Ok(Ok(stats)) =
            tokio::time::timeout(CLOSING_READ, self.journal.stats(&self.settings.consumer)).await
        {
            report.observe(stats);
        }
        report
    }

    /// Deliver until there is nothing claimable, a batch fails, the journal is
    /// unreachable, or shutdown asks for its budget — whichever comes first.
    ///
    /// The stop signal is checked between batches rather than only between
    /// polls, because a replica with a long backlog and a healthy destination
    /// would otherwise keep claiming past its shutdown bound and be abandoned
    /// mid-loop.
    ///
    /// Housekeeping is due on its own interval rather than once delivery has
    /// caught up: a replica that never catches up is exactly the one whose
    /// retention has to run, whose claim floor has to advance, and whose depth
    /// an operator is watching.
    async fn pump_until_idle(
        &self,
        report: &mut DrainReport,
        stop: &watch::Receiver<Option<Duration>>,
        next_maintain: &mut Instant,
    ) {
        loop {
            if stop.has_changed().unwrap_or(true) {
                return;
            }
            self.maintain_if_due(next_maintain).await;
            match self.pump(report).await {
                Ok(0) => return,
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(
                        journal = self.journal.name(),
                        error = %error,
                        "usage journal claim failed; retrying after the poll interval"
                    );
                    return;
                }
            }
        }
    }

    /// One claim, one write, one acknowledgement per event. Returns how many
    /// events were acknowledged.
    async fn pump(&self, report: &mut DrainReport) -> Result<usize, JournalError> {
        let claimed = self
            .journal
            .claim(
                &self.settings.consumer,
                Claim {
                    max_events: self.settings.claim_batch,
                    lease: self.settings.lease,
                    now: SystemTime::now(),
                },
            )
            .await?;
        if claimed.is_empty() {
            return Ok(0);
        }
        let redeliveries = claimed
            .iter()
            .filter(|delivery| delivery.id.is_redelivery())
            .count();
        if redeliveries > 0 {
            crate::telemetry::metrics::record_usage_journal_deliveries(
                self.journal.name(),
                self.settings.consumer.as_str(),
                "redelivered",
                redeliveries as u64,
            );
        }
        let outcome = self.deliver(&claimed).await;
        if outcome.landed.len() < claimed.len() {
            report.failed += 1;
            crate::telemetry::metrics::record_usage_journal_deliveries(
                self.journal.name(),
                self.settings.consumer.as_str(),
                "failed",
                (claimed.len() - outcome.landed.len()) as u64,
            );
            // Everything else is left claimed and unacknowledged: the lease is
            // the backoff. What cannot be left is an event the destination
            // refuses on its own account and that has used up its attempts,
            // because its ordering key would wait behind it forever.
            let attempts = self.journal.capacity().max_delivery_attempts;
            for index in &outcome.refused {
                let delivery = &claimed[*index];
                if delivery.id.attempt < attempts {
                    continue;
                }
                match self
                    .journal
                    .quarantine(&delivery.id, PoisonReason::Rejected)
                    .await
                {
                    // Counted here rather than in the journal, because this is
                    // the path that condemns a rejected event: the count is what
                    // an operator alerts on, and the gauge alone only moves on
                    // the next maintenance tick.
                    Ok(()) => crate::telemetry::metrics::record_usage_journal_quarantined(
                        self.journal.name(),
                        self.settings.consumer.as_str(),
                        PoisonReason::Rejected.as_str(),
                    ),
                    Err(error) => tracing::warn!(
                        delivery = %delivery.id,
                        error = %error,
                        "usage event exhausted its delivery attempts but could not be quarantined"
                    ),
                }
            }
            // An event nobody accepted and nobody attributed a refusal to gets
            // its attempt back, so however long a destination stays down it
            // cannot spend a budget that exists for poison. The lease is
            // untouched, so the retry still waits for it.
            for (index, delivery) in claimed.iter().enumerate() {
                if outcome.landed.contains(&index) || outcome.refused.contains(&index) {
                    continue;
                }
                if let Err(error) = self.journal.relinquish(&delivery.id).await {
                    tracing::warn!(
                        delivery = %delivery.id,
                        error = %error,
                        "usage event's delivery attempt could not be returned after an \
                         unattributable refusal"
                    );
                }
            }
        }
        if outcome.landed.is_empty() {
            return Ok(0);
        }

        let mut acknowledged = 0;
        for delivery in outcome.landed.iter().map(|index| &claimed[*index]) {
            match self.journal.ack(&delivery.id).await {
                Ok(()) => acknowledged += 1,
                // The write happened; the acknowledgement did not. A redelivery
                // is the correct outcome, and the destination's idempotency is
                // what makes it harmless.
                Err(error) => tracing::warn!(
                    delivery = %delivery.id,
                    error = %error,
                    "usage event was written but not acknowledged; it will be redelivered"
                ),
            }
        }
        report.delivered += acknowledged as u64;
        crate::telemetry::metrics::record_usage_journal_deliveries(
            self.journal.name(),
            self.settings.consumer.as_str(),
            "acknowledged",
            acknowledged as u64,
        );
        Ok(acknowledged)
    }

    /// Write the batch, and work out who a refusal belongs to.
    ///
    /// A destination that refuses a whole batch has said nothing about any
    /// particular event in it: `SinkFailure` carries a message, not a
    /// classification, so "this row is bad" and "the database is restarting" look
    /// the same. Charging the poison budget for either would mean an outage a few
    /// leases long condemns the head of every ordering key — permanent manual
    /// reconciliation, in the mode that exists so nothing needs reconciling.
    ///
    /// So a refusal is attributed rather than assumed. The batch is halved and
    /// rewritten until a refused range is a single event, and *only* an event
    /// refused while the same destination accepted its siblings counts as
    /// refused-on-its-own-account. If the whole bisection lands nothing, the
    /// destination is down: no attempt is charged to any event, and the lease
    /// hands the batch back to be retried whole.
    ///
    /// That verdict is taken at the *end* of the bisection rather than at the
    /// first level that lands nothing, because two events refused on their own
    /// account — one in each half — also land nothing at the first level. Ending
    /// there would deliver neither them nor their healthy siblings, spend no
    /// attempt, and re-claim the identical batch forever. Only the bound in
    /// [`PROBE_WRITES`] cuts the search short, and then only while nothing at
    /// all has been accepted.
    ///
    /// A batch of one is refused without a verdict for the same reason: with no
    /// sibling to judge it against, "the destination is down" and "this row is
    /// bad" are the same observation. Such an event is retried until traffic on
    /// another ordering key gives the destination a chance to accept something
    /// beside it, so a genuinely poisonous event stalls its own key — visible on
    /// `axond.usage.journal.oldest_pending_age` — rather than being set aside on a
    /// guess.
    ///
    /// Rewriting a range means a destination may see an event twice, which is the
    /// same duplicate the lease already produces and the same idempotency on
    /// `request_id` absorbs it.
    async fn deliver(&self, claimed: &[Delivery]) -> Delivered {
        let mut outcome = Delivered::default();
        if self.write(claimed).await {
            outcome.landed.extend(0..claimed.len());
            return outcome;
        }
        if claimed.len() == 1 {
            return outcome;
        }

        let mut orphans: Vec<usize> = Vec::new();
        let mut refusals = 1;
        let mut level = vec![halve(0..claimed.len())];
        while !level.is_empty() {
            let mut next = Vec::new();
            for (left, right) in level {
                for range in [left, right] {
                    if self.write(&claimed[range.clone()]).await {
                        outcome.landed.extend(range);
                        continue;
                    }
                    refusals += 1;
                    if range.len() == 1 {
                        orphans.push(range.start);
                    } else {
                        next.push(halve(range));
                    }
                    if outcome.landed.is_empty() && refusals >= PROBE_WRITES {
                        // Nothing has been accepted after a bounded search, so
                        // the destination is down rather than refusing anybody
                        // in particular: stop probing it and let the lease hand
                        // the batch back whole.
                        return Delivered::default();
                    }
                }
            }
            level = next;
        }
        if outcome.landed.is_empty() {
            // Every event, alone, was refused by a destination that accepted
            // nothing else either. That is an outage, and an outage condemns
            // no one.
            return outcome;
        }
        outcome.refused = orphans;
        outcome
    }

    /// Write the batch to every destination. All-or-nothing per destination: a
    /// sink that rejected the batch has not been written, so nothing in it is
    /// acknowledged and the whole batch is redelivered.
    ///
    /// A destination that accepted the batch is counted on
    /// `axond.usage.records_written` exactly as a batching sink counts it, so
    /// enabling the journal does not silence the per-sink write counter. Its twin
    /// is deliberately not emitted here: a refused batch stays journaled and is
    /// retried, so nothing was dropped, and the journal's delivery, loss, and
    /// quarantine counters are what a billing-grade deployment alerts on instead.
    async fn write(&self, claimed: &[Delivery]) -> bool {
        let batch: Vec<ObservedRecord> = claimed
            .iter()
            .map(|delivery| delivery.event.observed())
            .collect();
        for sink in self.sinks.iter() {
            if let Err(error) = sink.record_batch(&batch).await {
                tracing::warn!(
                    sink = sink.name(),
                    records = batch.len(),
                    error = %error,
                    "usage journal delivery failed; the events stay journaled"
                );
                return false;
            }
            crate::telemetry::metrics::record_usage_written(sink.name(), batch.len() as u64);
        }
        true
    }

    /// Publish the backlog gauges. Read once per maintenance tick rather than
    /// per claim: the query walks the outbox, and a gauge nobody samples faster
    /// than that gains nothing from being recomputed per batch.
    async fn publish_stats(&self) {
        match self.journal.stats(&self.settings.consumer).await {
            Ok(stats) => crate::telemetry::metrics::record_usage_journal_stats(
                self.journal.name(),
                self.settings.consumer.as_str(),
                &stats,
            ),
            Err(error) => tracing::warn!(
                journal = self.journal.name(),
                error = %error,
                "usage journal stats could not be read"
            ),
        }
    }

    /// Prune and publish if the interval has come round. Cheap to call between
    /// batches: it is a clock comparison until it is due.
    async fn maintain_if_due(&self, next_maintain: &mut Instant) {
        if Instant::now() < *next_maintain {
            return;
        }
        self.maintain().await;
        self.publish_stats().await;
        *next_maintain = Instant::now() + self.settings.maintain_interval;
    }

    async fn maintain(&self) {
        match self.journal.maintain(SystemTime::now()).await {
            Ok(pruned) if pruned > 0 => {
                tracing::debug!(
                    journal = self.journal.name(),
                    pruned,
                    "usage journal pruned"
                )
            }
            Ok(_) => {}
            Err(error) => tracing::warn!(
                journal = self.journal.name(),
                error = %error,
                "usage journal retention pass failed"
            ),
        }
        self.report_other_consumers().await;
    }

    /// Say so when the journal holds delivery state for a consumer this
    /// deployment is not running.
    ///
    /// Retention waits on every registered consumer, so a name that was retired —
    /// the predecessor of a renamed `consumer`, or a replay consumer somebody
    /// finished with — stops the outbox pruning at all, and a bounded outbox that
    /// cannot prune eventually refuses appends. Nothing here deletes the state:
    /// the same row is what a second fleet delivering from this outbox depends on,
    /// and only an operator knows which it is. Reported once a maintenance tick,
    /// alongside the depth gauge that says whether it is costing anything yet.
    async fn report_other_consumers(&self) {
        match self
            .journal
            .consumers_besides(&self.settings.consumer)
            .await
        {
            Ok(others) if !others.is_empty() => tracing::warn!(
                journal = self.journal.name(),
                consumer = %self.settings.consumer,
                others = others.join(","),
                "the usage journal is holding delivery state for consumers this deployment is \
                 not running, and retention waits on every one of them; if they are retired \
                 names, delete their rows, or the outbox grows to its limit and refuses appends"
            ),
            Ok(_) => {}
            Err(error) => tracing::debug!(
                journal = self.journal.name(),
                error = %error,
                "usage journal consumers could not be listed"
            ),
        }
    }
}

/// What one delivery pass established: which events the destinations accepted,
/// and which ones they refused on their own account.
#[derive(Default)]
struct Delivered {
    /// Indices into the claimed batch that reached every destination.
    landed: Vec<usize>,
    /// Indices the destinations refused while accepting their siblings, so the
    /// refusal is the event's own and may spend its attempt budget. Empty when
    /// the destination accepted nothing: an outage condemns no one.
    refused: Vec<usize>,
}

/// Split a range in two, left half first.
fn halve(range: std::ops::Range<usize>) -> (std::ops::Range<usize>, std::ops::Range<usize>) {
    let mid = range.start + range.len() / 2;
    (range.start..mid, mid..range.end)
}

/// The running worker, and the only way to stop it.
pub struct WorkerHandle {
    stop: watch::Sender<Option<Duration>>,
    task: JoinHandle<DrainReport>,
    /// Kept so a worker that had to be abandoned does not cost the operator the
    /// one number they are shutting down on: the backlog is read here instead.
    journal: Arc<dyn UsageJournal>,
    consumer: ConsumerId,
}

impl WorkerHandle {
    /// Stop claiming new work, spend up to `budget` finishing what is
    /// deliverable, and report what was left.
    ///
    /// Bounded, and honest about the bound: an outbox that cannot be drained in
    /// time is reported as undelivered rather than waited on, because the events
    /// are durable and the process is not.
    pub async fn drain(self, budget: Duration) -> DrainReport {
        let Self {
            stop,
            task,
            journal,
            consumer,
        } = self;
        let _ = stop.send(Some(budget));
        // The bound the worker was given plus a margin for the batch it is
        // already writing; a task that overran it is abandoned rather than
        // allowed to hold the process open.
        match tokio::time::timeout(budget + DRAIN_MARGIN, task).await {
            Ok(Ok(report)) => report,
            Ok(Err(error)) => {
                tracing::error!(error = %error, "usage journal worker panicked");
                Self::unreported(&journal, &consumer).await
            }
            Err(_) => {
                tracing::error!("usage journal worker did not stop inside its bound");
                Self::unreported(&journal, &consumer).await
            }
        }
    }

    /// What can still be said about a worker whose own report never arrived.
    ///
    /// Not zeros: a report with `undelivered: 0` says delivery finished, which is
    /// the opposite of what a drain that ran out of time means, and an operator
    /// reads it as licence to decommission the replica. The backlog is read
    /// straight from the journal instead, and the counters only this worker knew
    /// are marked unknown.
    async fn unreported(journal: &Arc<dyn UsageJournal>, consumer: &ConsumerId) -> DrainReport {
        let mut report = DrainReport::default();
        // Bounded too: this runs after the shutdown budget is already spent, so
        // an unreachable journal must not add its operation timeout to a
        // shutdown an orchestrator sized its grace period against.
        match tokio::time::timeout(CLOSING_READ, journal.stats(consumer)).await {
            Ok(Ok(stats)) => report.observe(stats),
            Err(_) => tracing::error!(
                journal = journal.name(),
                "usage journal backlog could not be read inside its bound after the drain was \
                 abandoned"
            ),
            Ok(Err(error)) => tracing::error!(
                journal = journal.name(),
                error = %error,
                "usage journal backlog could not be read after the drain was abandoned"
            ),
        }
        report.drained = false;
        report
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use async_trait::async_trait;

    use super::super::oracle::InMemoryUsageJournal;
    use super::super::tests::{consumer, event_for};
    use super::super::{Appended, Capacity, CapacityPolicy, DeliveryId, PoisonReason, UsageEvent};
    use super::*;
    use crate::usage::{SinkFailure, UsageRecord};

    /// A sink that remembers every `request_id` it was handed, in order, so a
    /// redelivery is visible as a repeat rather than inferred.
    #[derive(Default)]
    struct Recorder {
        written: Mutex<Vec<String>>,
        /// Whether the destination refuses the batch. A refusal is the failure
        /// the lease exists for.
        refuse: bool,
        /// The `request_id`s this destination refuses whenever one of them
        /// appears in a batch, accepting everything else: the bad rows the
        /// poison budget is for.
        poison: Vec<String>,
    }

    impl Recorder {
        fn written(&self) -> Vec<String> {
            self.written.lock().expect("not poisoned").clone()
        }
    }

    #[async_trait]
    impl UsageSink for Recorder {
        fn name(&self) -> &'static str {
            "recorder"
        }

        async fn record(&self, record: &UsageRecord) {
            self.written
                .lock()
                .expect("not poisoned")
                .push(record.request_id.clone());
        }

        async fn record_batch(&self, batch: &[ObservedRecord]) -> Result<(), SinkFailure> {
            if self.refuse {
                return Err(SinkFailure::new("the destination is refusing writes"));
            }
            if batch
                .iter()
                .any(|observed| self.poison.contains(&observed.record.request_id))
            {
                return Err(SinkFailure::new("the destination refuses this row"));
            }
            for observed in batch {
                self.record(&observed.record).await;
            }
            Ok(())
        }
    }

    /// A journal whose acknowledgements never land: what a worker that dies
    /// between the destination write and the acknowledgement leaves behind.
    struct LosesAcks(InMemoryUsageJournal);

    #[async_trait]
    impl UsageJournal for LosesAcks {
        fn name(&self) -> &'static str {
            "loses-acks"
        }

        fn capacity(&self) -> Capacity {
            self.0.capacity()
        }

        fn mode(&self) -> super::super::DeliveryMode {
            self.0.mode()
        }

        async fn append(&self, event: &UsageEvent) -> Result<Appended, JournalError> {
            self.0.append(event).await
        }

        async fn claim(
            &self,
            consumer: &ConsumerId,
            claim: Claim,
        ) -> Result<Vec<Delivery>, JournalError> {
            self.0.claim(consumer, claim).await
        }

        async fn ack(&self, delivery: &DeliveryId) -> Result<(), JournalError> {
            Err(JournalError::Backend(format!(
                "the acknowledgement of {delivery} never reached the journal"
            )))
        }

        async fn quarantine(
            &self,
            delivery: &DeliveryId,
            reason: PoisonReason,
        ) -> Result<(), JournalError> {
            self.0.quarantine(delivery, reason).await
        }

        async fn relinquish(&self, delivery: &DeliveryId) -> Result<(), JournalError> {
            self.0.relinquish(delivery).await
        }

        async fn stats(&self, consumer: &ConsumerId) -> Result<JournalStats, JournalError> {
            self.0.stats(consumer).await
        }
    }

    /// A journal whose backlog read takes as long as a journal operation
    /// timeout allows: what a large outbox on a busy database does to the read
    /// the worker takes on its way out.
    struct SlowStats(InMemoryUsageJournal, Duration);

    #[async_trait]
    impl UsageJournal for SlowStats {
        fn name(&self) -> &'static str {
            "slow-stats"
        }

        fn capacity(&self) -> Capacity {
            self.0.capacity()
        }

        fn mode(&self) -> super::super::DeliveryMode {
            self.0.mode()
        }

        async fn append(&self, event: &UsageEvent) -> Result<Appended, JournalError> {
            self.0.append(event).await
        }

        async fn claim(
            &self,
            consumer: &ConsumerId,
            claim: Claim,
        ) -> Result<Vec<Delivery>, JournalError> {
            self.0.claim(consumer, claim).await
        }

        async fn ack(&self, delivery: &DeliveryId) -> Result<(), JournalError> {
            self.0.ack(delivery).await
        }

        async fn quarantine(
            &self,
            delivery: &DeliveryId,
            reason: PoisonReason,
        ) -> Result<(), JournalError> {
            self.0.quarantine(delivery, reason).await
        }

        async fn relinquish(&self, delivery: &DeliveryId) -> Result<(), JournalError> {
            self.0.relinquish(delivery).await
        }

        async fn stats(&self, consumer: &ConsumerId) -> Result<JournalStats, JournalError> {
            tokio::time::sleep(self.1).await;
            self.0.stats(consumer).await
        }
    }

    /// The closing backlog read is bounded on its own, so a journal operation
    /// that outlasts the drain margin cannot turn a worker that stopped
    /// correctly into an abandoned one — nor hold shutdown open for the whole
    /// operation timeout twice over.
    #[tokio::test]
    async fn a_slow_closing_backlog_read_does_not_make_a_stopped_worker_look_abandoned() {
        let journal = Arc::new(SlowStats(
            InMemoryUsageJournal::new(),
            Duration::from_secs(5),
        ));
        let sinks: Vec<Box<dyn UsageSink>> = vec![];
        let handle = DeliveryWorker::new(
            Arc::clone(&journal) as Arc<dyn UsageJournal>,
            Arc::new(sinks),
            settings(Duration::from_secs(30)),
        )
        .spawn();

        let started = Instant::now();
        let report = handle.drain(Duration::from_millis(50)).await;
        assert!(
            report.reported,
            "a worker abandoned for its closing read: {report:?}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "shutdown waited on the journal's own timeout: {:?}",
            started.elapsed()
        );
        assert!(
            !report.counted,
            "the backlog read did not finish, so it is unknown rather than zero: {report:?}"
        );
    }

    /// A journal whose housekeeping pass takes as long as a journal operation
    /// timeout allows, from the moment the test says so.
    struct SlowMaintain(InMemoryUsageJournal, Arc<AtomicBool>, Duration);

    #[async_trait]
    impl UsageJournal for SlowMaintain {
        fn name(&self) -> &'static str {
            "slow-maintain"
        }

        fn capacity(&self) -> Capacity {
            self.0.capacity()
        }

        fn mode(&self) -> super::super::DeliveryMode {
            self.0.mode()
        }

        async fn append(&self, event: &UsageEvent) -> Result<Appended, JournalError> {
            self.0.append(event).await
        }

        async fn claim(
            &self,
            consumer: &ConsumerId,
            claim: Claim,
        ) -> Result<Vec<Delivery>, JournalError> {
            self.0.claim(consumer, claim).await
        }

        async fn ack(&self, delivery: &DeliveryId) -> Result<(), JournalError> {
            self.0.ack(delivery).await
        }

        async fn quarantine(
            &self,
            delivery: &DeliveryId,
            reason: PoisonReason,
        ) -> Result<(), JournalError> {
            self.0.quarantine(delivery, reason).await
        }

        async fn relinquish(&self, delivery: &DeliveryId) -> Result<(), JournalError> {
            self.0.relinquish(delivery).await
        }

        async fn stats(&self, consumer: &ConsumerId) -> Result<JournalStats, JournalError> {
            self.0.stats(consumer).await
        }

        async fn maintain(&self, now: SystemTime) -> Result<u64, JournalError> {
            if self.1.load(Ordering::SeqCst) {
                tokio::time::sleep(self.2).await;
            }
            self.0.maintain(now).await
        }
    }

    /// A destination that says when it has started writing, so a test can stop
    /// a worker that is provably mid-pass rather than idle.
    struct Announcing(watch::Sender<bool>, Duration);

    #[async_trait]
    impl UsageSink for Announcing {
        fn name(&self) -> &'static str {
            "announcing"
        }

        async fn record(&self, _record: &UsageRecord) {
            let _ = self.0.send(true);
            tokio::time::sleep(self.1).await;
        }

        async fn record_batch(&self, _batch: &[ObservedRecord]) -> Result<(), SinkFailure> {
            let _ = self.0.send(true);
            tokio::time::sleep(self.1).await;
            Ok(())
        }
    }

    /// Housekeeping is not started once shutdown has been asked for: it is
    /// nobody's dependency, the next process does it anyway, and a pass begun
    /// here would spend the whole shutdown bound and have the worker abandoned.
    #[tokio::test]
    async fn housekeeping_due_at_shutdown_does_not_cost_the_worker_its_report() {
        let slow = Arc::new(AtomicBool::new(false));
        let journal = Arc::new(SlowMaintain(
            InMemoryUsageJournal::new(),
            Arc::clone(&slow),
            Duration::from_secs(5),
        ));
        let (writing, mut written) = watch::channel(false);
        let sinks: Vec<Box<dyn UsageSink>> =
            vec![Box::new(Announcing(writing, Duration::from_millis(100)))];
        let handle = DeliveryWorker::new(
            Arc::clone(&journal) as Arc<dyn UsageJournal>,
            Arc::new(sinks),
            WorkerSettings {
                // Always due, so shutdown lands on a tick rather than by luck.
                maintain_interval: Duration::ZERO,
                ..settings(Duration::from_secs(30))
            },
        )
        .spawn();
        journal
            .append(&event_for("GW_INBOUND_ACME_KEY"))
            .await
            .expect("append");
        // Stopped while the worker is inside a write: the pass it could run
        // slowly is then unambiguously one begun after the stop signal.
        written.changed().await.expect("the worker started writing");
        slow.store(true, Ordering::SeqCst);
        let started = Instant::now();
        let report = handle.drain(Duration::from_millis(50)).await;

        assert!(
            report.reported,
            "the worker was abandoned inside a housekeeping pass it should not have started: \
             {report:?}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "shutdown waited on housekeeping: {:?}",
            started.elapsed()
        );
    }

    /// One delivery pass can outlast the whole shutdown bound on its own, so
    /// the bound is enforced inside the pass: the events it drops are still
    /// claimed, leased, and redelivered by the next process.
    #[tokio::test]
    async fn a_single_slow_delivery_pass_cannot_overrun_the_shutdown_bound() {
        let journal = Arc::new(InMemoryUsageJournal::new());
        let sinks: Vec<Box<dyn UsageSink>> = vec![Box::new(Slow(Duration::from_secs(5)))];
        let handle = DeliveryWorker::new(
            Arc::clone(&journal) as Arc<dyn UsageJournal>,
            Arc::new(sinks),
            WorkerSettings {
                // Nothing is claimable until the appends below, so the pass that
                // matters is the one the drain phase starts.
                poll_interval: Duration::from_secs(30),
                ..settings(Duration::from_secs(30))
            },
        )
        .spawn();
        tokio::time::sleep(Duration::from_millis(20)).await;
        for _ in 0..8 {
            journal
                .append(&event_for("GW_INBOUND_ACME_KEY"))
                .await
                .expect("append");
        }

        let started = Instant::now();
        let report = handle.drain(Duration::from_millis(50)).await;

        assert!(
            report.reported,
            "a pass slower than the bound left the worker abandoned: {report:?}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "shutdown waited on the destination: {:?}",
            started.elapsed()
        );
        // Cut off, not lost: the events are still in the outbox for the next
        // process to claim.
        assert_eq!(report.undelivered, 8, "{report:?}");
        assert!(!report.drained, "{report:?}");
    }

    /// Fast enough that a test does not wait on a poll, long enough that the
    /// worker is not spinning while the test appends.
    fn settings(lease: Duration) -> WorkerSettings {
        WorkerSettings {
            consumer: consumer("billing"),
            claim_batch: 8,
            lease,
            poll_interval: Duration::from_millis(5),
            maintain_interval: Duration::from_millis(20),
        }
    }

    fn worker(
        journal: Arc<dyn UsageJournal>,
        sink: Arc<Recorder>,
        settings: WorkerSettings,
    ) -> WorkerHandle {
        let sinks: Vec<Box<dyn UsageSink>> = vec![Box::new(SharedSink(Arc::clone(&sink)))];
        DeliveryWorker::new(journal, Arc::new(sinks), settings).spawn()
    }

    /// The recorder, kept by the test as well as by the worker.
    struct SharedSink(Arc<Recorder>);

    #[async_trait]
    impl UsageSink for SharedSink {
        fn name(&self) -> &'static str {
            self.0.name()
        }

        async fn record(&self, record: &UsageRecord) {
            self.0.record(record).await;
        }

        async fn record_batch(&self, batch: &[ObservedRecord]) -> Result<(), SinkFailure> {
            self.0.record_batch(batch).await
        }
    }

    /// Wait for `predicate`, or fail with what was actually observed. Bounded, so
    /// a broken worker fails the test instead of hanging the suite.
    async fn eventually(
        sink: &Recorder,
        what: &str,
        predicate: impl Fn(&[String]) -> bool,
    ) -> Vec<String> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let written = sink.written();
            if predicate(&written) {
                return written;
            }
            assert!(
                Instant::now() < deadline,
                "{what} did not happen; the sink saw {written:?}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    #[tokio::test]
    async fn an_appended_event_is_delivered_and_acknowledged() {
        let journal = Arc::new(InMemoryUsageJournal::new());
        let sink = Arc::new(Recorder::default());
        let handle = worker(
            Arc::clone(&journal) as Arc<dyn UsageJournal>,
            Arc::clone(&sink),
            settings(Duration::from_secs(30)),
        );
        let event = event_for("GW_INBOUND_ACME_KEY");
        journal.append(&event).await.expect("append");

        let written = eventually(&sink, "the event reached the sink", |written| {
            !written.is_empty()
        })
        .await;
        assert_eq!(written, vec![event.id().to_string()]);
        let report = handle.drain(Duration::from_secs(5)).await;
        assert_eq!(report.delivered, 1, "{report:?}");
        assert_eq!(report.undelivered, 0, "{report:?}");
        assert!(report.drained, "{report:?}");
    }

    #[tokio::test]
    async fn a_destination_that_refuses_the_batch_leaves_the_event_journaled() {
        let journal = Arc::new(InMemoryUsageJournal::new());
        let sink = Arc::new(Recorder {
            refuse: true,
            ..Recorder::default()
        });
        let handle = worker(
            Arc::clone(&journal) as Arc<dyn UsageJournal>,
            Arc::clone(&sink),
            // A lease long enough that the attempt budget is nowhere near spent:
            // this is the ordinary destination outage, not a poison event.
            settings(Duration::from_secs(30)),
        );
        journal
            .append(&event_for("GW_INBOUND_ACME_KEY"))
            .await
            .expect("append");
        tokio::time::sleep(Duration::from_millis(50)).await;

        let report = handle.drain(Duration::from_millis(50)).await;
        assert!(sink.written().is_empty(), "nothing was written");
        // Undelivered rather than lost, and reported as such: the event is still
        // in the journal for the next process to claim.
        assert_eq!(report.delivered, 0, "{report:?}");
        assert_eq!(report.undelivered, 1, "{report:?}");
        assert!(report.failed > 0, "{report:?}");
        assert!(!report.drained, "{report:?}");
        assert_eq!(journal.stored_events(), 1);
    }

    #[tokio::test]
    async fn a_destination_wide_outage_quarantines_nothing_however_long_it_lasts() {
        // One attempt each, so the old accounting — every event in a refused
        // batch spends an attempt — would condemn the whole backlog on the
        // second claim.
        let journal = Arc::new(InMemoryUsageJournal::with_capacity(Capacity {
            max_events: 8,
            max_delivery_attempts: 1,
            retain_acknowledged: Duration::from_secs(60),
            policy: CapacityPolicy::Refuse,
        }));
        let sink = Arc::new(Recorder {
            refuse: true,
            ..Recorder::default()
        });
        let handle = worker(
            Arc::clone(&journal) as Arc<dyn UsageJournal>,
            Arc::clone(&sink),
            settings(Duration::from_millis(5)),
        );
        for subject in ["one", "two", "three", "four"] {
            journal.append(&event_for(subject)).await.expect("append");
        }

        // Long enough for every event to be re-claimed several times over.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let report = handle.drain(Duration::from_millis(50)).await;
        assert!(report.failed > 1, "the batch was retried: {report:?}");
        // Undelivered and still deliverable, which is the whole promise: an
        // outage is not a verdict on anybody's event.
        assert_eq!(report.quarantined, 0, "{report:?}");
        assert_eq!(report.undelivered, 4, "{report:?}");
        assert_eq!(report.delivered, 0, "{report:?}");
    }

    #[tokio::test]
    async fn one_refused_event_is_isolated_and_its_siblings_are_delivered() {
        let journal = Arc::new(InMemoryUsageJournal::with_capacity(Capacity {
            max_events: 8,
            max_delivery_attempts: 1,
            retain_acknowledged: Duration::from_secs(60),
            policy: CapacityPolicy::Refuse,
        }));
        let poison = event_for("poison");
        let sink = Arc::new(Recorder {
            poison: vec![poison.record().request_id.clone()],
            ..Recorder::default()
        });
        let handle = worker(
            Arc::clone(&journal) as Arc<dyn UsageJournal>,
            Arc::clone(&sink),
            settings(Duration::from_millis(5)),
        );
        journal.append(&poison).await.expect("append");
        for subject in ["one", "two", "three"] {
            journal.append(&event_for(subject)).await.expect("append");
        }

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let stats = journal.stats(&consumer("billing")).await.expect("stats");
            if stats.quarantined == 1 && stats.pending == 0 && stats.in_flight == 0 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "the refused event was not isolated: {stats:?}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let written = sink.written();
        assert!(
            !written.contains(&poison.record().request_id),
            "the refused event was never written: {written:?}"
        );
        let report = handle.drain(Duration::from_millis(50)).await;
        assert_eq!(report.quarantined, 1, "{report:?}");
        assert_eq!(report.undelivered, 0, "{report:?}");
    }

    #[tokio::test]
    async fn two_refused_events_in_one_batch_are_both_isolated() {
        // One refused event in each half of the claim, so nothing lands at the
        // first split. Ending the search there — as it once did — delivered
        // neither them nor their healthy siblings, spent no attempt, and
        // re-claimed the identical batch forever.
        let journal = Arc::new(InMemoryUsageJournal::with_capacity(Capacity {
            max_events: 8,
            max_delivery_attempts: 1,
            retain_acknowledged: Duration::from_secs(60),
            policy: CapacityPolicy::Refuse,
        }));
        let (first, second) = (event_for("poison-one"), event_for("poison-two"));
        let (healthy, other) = (event_for("one"), event_for("two"));
        let sink = Arc::new(Recorder {
            poison: vec![
                first.record().request_id.clone(),
                second.record().request_id.clone(),
            ],
            ..Recorder::default()
        });
        let handle = worker(
            Arc::clone(&journal) as Arc<dyn UsageJournal>,
            Arc::clone(&sink),
            settings(Duration::from_millis(5)),
        );
        for event in [&first, &healthy, &second, &other] {
            journal.append(event).await.expect("append");
        }

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let stats = journal.stats(&consumer("billing")).await.expect("stats");
            if stats.quarantined == 2 && stats.pending == 0 && stats.in_flight == 0 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "the refused events were not isolated: {stats:?}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let written = sink.written();
        for delivered in [&healthy, &other] {
            assert!(
                written.contains(&delivered.record().request_id),
                "a sibling of the refused events was delivered: {written:?}"
            );
        }
        let report = handle.drain(Duration::from_millis(50)).await;
        assert_eq!(report.quarantined, 2, "{report:?}");
        assert_eq!(report.undelivered, 0, "{report:?}");
    }

    /// A journal that always has something claimable: every claim appends
    /// another event first, which is a replica whose arrivals outpace its
    /// deliveries.
    struct NeverCatchesUp {
        inner: InMemoryUsageJournal,
        maintained: AtomicUsize,
    }

    #[async_trait]
    impl UsageJournal for NeverCatchesUp {
        fn name(&self) -> &'static str {
            "never-catches-up"
        }

        fn capacity(&self) -> Capacity {
            self.inner.capacity()
        }

        fn mode(&self) -> super::super::DeliveryMode {
            self.inner.mode()
        }

        async fn append(&self, event: &UsageEvent) -> Result<Appended, JournalError> {
            self.inner.append(event).await
        }

        async fn claim(
            &self,
            consumer: &ConsumerId,
            claim: Claim,
        ) -> Result<Vec<Delivery>, JournalError> {
            // A round trip a real journal cannot answer synchronously, so the
            // busy worker still leaves the runtime to the rest of the test.
            tokio::time::sleep(Duration::from_millis(1)).await;
            self.inner.append(&event_for("arriving")).await?;
            self.inner.claim(consumer, claim).await
        }

        async fn ack(&self, delivery: &DeliveryId) -> Result<(), JournalError> {
            self.inner.ack(delivery).await
        }

        async fn quarantine(
            &self,
            delivery: &DeliveryId,
            reason: PoisonReason,
        ) -> Result<(), JournalError> {
            self.inner.quarantine(delivery, reason).await
        }

        async fn relinquish(&self, delivery: &DeliveryId) -> Result<(), JournalError> {
            self.inner.relinquish(delivery).await
        }

        async fn stats(&self, consumer: &ConsumerId) -> Result<JournalStats, JournalError> {
            self.inner.stats(consumer).await
        }

        async fn maintain(&self, now: SystemTime) -> Result<u64, JournalError> {
            self.maintained.fetch_add(1, Ordering::Relaxed);
            self.inner.maintain(now).await
        }
    }

    #[tokio::test]
    async fn housekeeping_runs_on_a_worker_that_never_catches_up() {
        // Retention, the claim floor, and the depth gauges used to wait for the
        // worker to find nothing left to deliver, so the replica under the most
        // pressure was the one that never pruned and never said how far behind
        // it was.
        let journal = Arc::new(NeverCatchesUp {
            inner: InMemoryUsageJournal::new(),
            maintained: AtomicUsize::new(0),
        });
        let handle = worker(
            Arc::clone(&journal) as Arc<dyn UsageJournal>,
            Arc::new(Recorder::default()),
            settings(Duration::from_secs(30)),
        );

        let deadline = Instant::now() + Duration::from_secs(5);
        while journal.maintained.load(Ordering::Relaxed) == 0 {
            assert!(
                Instant::now() < deadline,
                "a worker that never went idle never ran its maintenance tick"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        handle.drain(Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn a_write_whose_acknowledgement_was_lost_is_delivered_again() {
        let journal = Arc::new(LosesAcks(InMemoryUsageJournal::new()));
        let sink = Arc::new(Recorder::default());
        let handle = worker(
            Arc::clone(&journal) as Arc<dyn UsageJournal>,
            Arc::clone(&sink),
            // A lease this short is exactly the crash window: the write landed,
            // the acknowledgement did not, and the lease expires.
            settings(Duration::from_millis(5)),
        );
        let event = event_for("GW_INBOUND_ACME_KEY");
        journal.append(&event).await.expect("append");

        let written = eventually(&sink, "the event was delivered twice", |written| {
            written.len() >= 2
        })
        .await;
        // At-least-once, and the duplicate carries the *same* identity — which is
        // what makes the destination's deduplication possible.
        assert!(
            written.iter().all(|id| *id == event.id().to_string()),
            "{written:?}"
        );
        handle.drain(Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn one_callers_events_are_delivered_in_append_order() {
        let journal = Arc::new(InMemoryUsageJournal::new());
        let sink = Arc::new(Recorder::default());
        let handle = worker(
            Arc::clone(&journal) as Arc<dyn UsageJournal>,
            Arc::clone(&sink),
            settings(Duration::from_secs(30)),
        );
        let mut expected = Vec::new();
        for _ in 0..4 {
            let event = event_for("GW_INBOUND_ACME_KEY");
            expected.push(event.id().to_string());
            journal.append(&event).await.expect("append");
        }

        let written = eventually(&sink, "every event reached the sink", move |written| {
            written.len() >= 4
        })
        .await;
        assert_eq!(written, expected);
        handle.drain(Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn a_drain_that_runs_out_of_budget_reports_the_backlog_rather_than_waiting() {
        let journal = Arc::new(InMemoryUsageJournal::new());
        let sink = Arc::new(Recorder {
            refuse: true,
            ..Recorder::default()
        });
        let handle = worker(
            Arc::clone(&journal) as Arc<dyn UsageJournal>,
            Arc::clone(&sink),
            settings(Duration::from_secs(30)),
        );
        for _ in 0..3 {
            journal
                .append(&event_for("GW_INBOUND_ACME_KEY"))
                .await
                .expect("append");
        }

        let started = Instant::now();
        let report = handle.drain(Duration::from_millis(50)).await;
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "the drain waited past its bound: {:?}",
            started.elapsed()
        );
        assert_eq!(report.undelivered, 3, "{report:?}");
        assert!(!report.drained, "{report:?}");
        assert!(
            report.reported,
            "the worker reported for itself: {report:?}"
        );
    }

    /// A destination whose write never returns: the worker cannot be stopped by
    /// any signal while it is inside one, so shutdown has to abandon it.
    struct NeverReturns;

    #[async_trait]
    impl UsageSink for NeverReturns {
        fn name(&self) -> &'static str {
            "never-returns"
        }

        async fn record(&self, _record: &UsageRecord) {
            std::future::pending::<()>().await;
        }

        async fn record_batch(&self, _batch: &[ObservedRecord]) -> Result<(), SinkFailure> {
            std::future::pending::<Result<(), SinkFailure>>().await
        }
    }

    #[tokio::test]
    async fn a_drain_that_had_to_abandon_the_worker_reports_the_backlog_rather_than_zeros() {
        let journal = Arc::new(InMemoryUsageJournal::new());
        let sinks: Vec<Box<dyn UsageSink>> = vec![Box::new(NeverReturns)];
        let handle = DeliveryWorker::new(
            Arc::clone(&journal) as Arc<dyn UsageJournal>,
            Arc::new(sinks),
            settings(Duration::from_secs(30)),
        )
        .spawn();
        for _ in 0..3 {
            journal
                .append(&event_for("GW_INBOUND_ACME_KEY"))
                .await
                .expect("append");
        }
        // Long enough that the worker is inside the write it will never leave.
        tokio::time::sleep(Duration::from_millis(50)).await;

        let report = handle.drain(Duration::from_millis(10)).await;
        assert!(!report.reported, "the worker never reported: {report:?}");
        // The whole point: the backlog is read by the parent instead of being
        // reported as a drained zero, which would read as "safe to decommission".
        assert!(report.counted, "{report:?}");
        assert_eq!(report.undelivered, 3, "{report:?}");
        assert!(!report.drained, "{report:?}");
    }

    /// A destination that is healthy but slow, so a long backlog takes far longer
    /// to deliver than any shutdown bound allows.
    struct Slow(Duration);

    #[async_trait]
    impl UsageSink for Slow {
        fn name(&self) -> &'static str {
            "slow"
        }

        async fn record(&self, _record: &UsageRecord) {
            tokio::time::sleep(self.0).await;
        }

        async fn record_batch(&self, _batch: &[ObservedRecord]) -> Result<(), SinkFailure> {
            tokio::time::sleep(self.0).await;
            Ok(())
        }
    }

    #[tokio::test]
    async fn a_worker_with_a_long_backlog_stops_at_its_bound_instead_of_being_abandoned() {
        let journal = Arc::new(InMemoryUsageJournal::with_capacity(Capacity {
            max_events: 4096,
            max_delivery_attempts: 8,
            retain_acknowledged: Duration::from_secs(60),
            policy: CapacityPolicy::Refuse,
        }));
        let sinks: Vec<Box<dyn UsageSink>> = vec![Box::new(Slow(Duration::from_millis(30)))];
        let handle = DeliveryWorker::new(
            Arc::clone(&journal) as Arc<dyn UsageJournal>,
            Arc::new(sinks),
            settings(Duration::from_secs(30)),
        )
        .spawn();
        // Eight to a claim at thirty milliseconds a batch: more than a second and
        // a half of delivery, so a worker that only noticed shutdown once it ran
        // out of claimable work would be abandoned rather than stopped.
        for _ in 0..400 {
            journal
                .append(&event_for("GW_INBOUND_ACME_KEY"))
                .await
                .expect("append");
        }

        let report = handle.drain(Duration::from_millis(50)).await;
        assert!(
            report.reported,
            "the worker stopped at its bound and reported: {report:?}"
        );
        assert!(report.undelivered > 0, "{report:?}");
        assert!(!report.drained, "{report:?}");
    }
}
