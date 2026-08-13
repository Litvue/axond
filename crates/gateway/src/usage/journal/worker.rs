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
}

impl DrainReport {
    fn observe(&mut self, stats: JournalStats) {
        self.undelivered = stats.pending + stats.in_flight;
        self.quarantined = stats.quarantined;
        self.drained = stats.is_drained();
    }

    pub fn log(&self) {
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
        WorkerHandle {
            stop,
            task: tokio::spawn(self.run(receiver)),
        }
    }

    async fn run(self, mut stop: watch::Receiver<Option<Duration>>) -> DrainReport {
        let mut report = DrainReport::default();
        let mut next_maintain = Instant::now() + self.settings.maintain_interval;
        let budget = loop {
            self.pump_until_idle(&mut report).await;
            if Instant::now() >= next_maintain {
                self.maintain().await;
                self.publish_stats().await;
                next_maintain = Instant::now() + self.settings.maintain_interval;
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
        while Instant::now() < deadline {
            match self.pump(&mut report).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
        }
        if let Ok(stats) = self.journal.stats(&self.settings.consumer).await {
            report.observe(stats);
        }
        report
    }

    /// Deliver until there is nothing claimable, a batch fails, or the journal
    /// is unreachable — whichever comes first.
    async fn pump_until_idle(&self, report: &mut DrainReport) {
        loop {
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
        if !self.write(&claimed).await {
            report.failed += 1;
            crate::telemetry::metrics::record_usage_journal_deliveries(
                self.journal.name(),
                self.settings.consumer.as_str(),
                "failed",
                claimed.len() as u64,
            );
            // Otherwise left claimed and unacknowledged: the lease is the
            // backoff. What cannot be left is an event that has used up its
            // attempts, because its ordering key would wait behind it forever.
            let attempts = self.journal.capacity().max_delivery_attempts;
            for delivery in &claimed {
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
            return Ok(0);
        }

        let mut acknowledged = 0;
        for delivery in &claimed {
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
    }
}

/// The running worker, and the only way to stop it.
pub struct WorkerHandle {
    stop: watch::Sender<Option<Duration>>,
    task: JoinHandle<DrainReport>,
}

impl WorkerHandle {
    /// Stop claiming new work, spend up to `budget` finishing what is
    /// deliverable, and report what was left.
    ///
    /// Bounded, and honest about the bound: an outbox that cannot be drained in
    /// time is reported as undelivered rather than waited on, because the events
    /// are durable and the process is not.
    pub async fn drain(self, budget: Duration) -> DrainReport {
        let _ = self.stop.send(Some(budget));
        // The bound the worker was given plus a margin for the batch it is
        // already writing; a task that overran it is abandoned rather than
        // allowed to hold the process open.
        match tokio::time::timeout(budget + Duration::from_secs(1), self.task).await {
            Ok(Ok(report)) => report,
            Ok(Err(error)) => {
                tracing::error!(error = %error, "usage journal worker panicked");
                DrainReport::default()
            }
            Err(_) => {
                tracing::error!("usage journal worker did not stop inside its bound");
                DrainReport::default()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

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

        async fn stats(&self, consumer: &ConsumerId) -> Result<JournalStats, JournalError> {
            self.0.stats(consumer).await
        }
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
    async fn an_event_the_destination_keeps_refusing_is_quarantined_not_retried_forever() {
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
        journal
            .append(&event_for("GW_INBOUND_ACME_KEY"))
            .await
            .expect("append");

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let stats = journal.stats(&consumer("billing")).await.expect("stats");
            if stats.quarantined == 1 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "the event was never quarantined: {stats:?}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let report = handle.drain(Duration::from_millis(50)).await;
        // The ordering key is free again, and the evidence is still there.
        assert_eq!(report.quarantined, 1, "{report:?}");
        assert_eq!(report.undelivered, 0, "{report:?}");
        assert_eq!(journal.stored_events(), 1);
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
    }
}
