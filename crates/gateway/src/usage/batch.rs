//! Buffered, batched, back-pressured delivery for sinks that talk to a
//! datastore.
//!
//! The durability-vs-latency contract (ADR 0009): the request path enqueues
//! with a non-blocking `try_send` and never waits for a sink. A full buffer
//! therefore *drops* the record and counts it on
//! `axond.usage.records_dropped{reason="buffer_full"}` — usage is valuable, but
//! not more valuable than the request it describes. Every drop is a signal that
//! the destination is too slow for the offered load, or that the buffer is too
//! small; both are visible in the metric before they are visible in a bill.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::mpsc;
use tokio::time::{Instant, timeout_at};

use crate::telemetry::metrics;

use super::{DropReason, ObservedRecord, SinkFailure, UsageRecord, UsageSink};

/// Batching policy for one sink.
#[derive(Debug, Clone, Copy)]
pub struct BatchSettings {
    /// Records the buffer holds before the fan-out starts dropping.
    pub capacity: usize,
    /// Rows per write. The Postgres sink binds one parameter set per row, so
    /// this is bounded by the wire protocol's parameter limit (see
    /// [`super::postgres`]).
    pub max_batch: usize,
    /// How long a partial batch waits for company before it is written anyway.
    pub flush_interval: Duration,
}

/// How often a sustained overflow is logged. A drop per record would turn a
/// sink outage into a log flood; the counter stays exact regardless.
const DROP_LOG_INTERVAL: u64 = 1_000;

/// Wraps a sink in a bounded queue and a flush task.
pub struct BatchedSink {
    name: &'static str,
    tx: mpsc::Sender<ObservedRecord>,
    dropped: Arc<AtomicU64>,
}

impl BatchedSink {
    /// Spawn the flush task. Must be called on the Tokio runtime that will
    /// serve requests.
    pub fn spawn(sink: Arc<dyn UsageSink>, settings: BatchSettings) -> Self {
        let (tx, rx) = mpsc::channel(settings.capacity);
        let name = sink.name();
        let dropped = Arc::new(AtomicU64::new(0));
        tokio::spawn(flush_loop(sink, rx, settings, Arc::clone(&dropped)));
        Self { name, tx, dropped }
    }

    /// Records discarded so far — the observable cost of the contract. The
    /// operator-facing view of this is the `axond.usage.records_dropped` metric.
    #[allow(dead_code)]
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    fn drop_record(&self, reason: DropReason) {
        let total = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
        metrics::record_usage_dropped(self.name, reason.as_str(), 1);
        if total == 1 || total.is_multiple_of(DROP_LOG_INTERVAL) {
            tracing::warn!(
                sink = self.name,
                reason = reason.as_str(),
                dropped = total,
                "usage record dropped rather than delaying the request path"
            );
        }
    }
}

#[async_trait]
impl UsageSink for BatchedSink {
    fn name(&self) -> &'static str {
        self.name
    }

    async fn record(&self, record: &UsageRecord) {
        match self.tx.try_send(ObservedRecord::now(record.clone())) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => self.drop_record(DropReason::BufferFull),
            Err(mpsc::error::TrySendError::Closed(_)) => self.drop_record(DropReason::Shutdown),
        }
    }

    async fn record_batch(&self, batch: &[ObservedRecord]) -> Result<(), SinkFailure> {
        for observed in batch {
            self.record(&observed.record).await;
        }
        Ok(())
    }
}

/// Accumulate up to `max_batch` records, or whatever arrived within
/// `flush_interval` of the first one, then hand the batch to the sink. The loop
/// ends when every sender is gone, flushing what it holds.
async fn flush_loop(
    sink: Arc<dyn UsageSink>,
    mut rx: mpsc::Receiver<ObservedRecord>,
    settings: BatchSettings,
    dropped: Arc<AtomicU64>,
) {
    let mut batch: Vec<ObservedRecord> = Vec::with_capacity(settings.max_batch);
    loop {
        let Some(first) = rx.recv().await else {
            return;
        };
        batch.push(first);
        let deadline = Instant::now() + settings.flush_interval;
        while batch.len() < settings.max_batch {
            match timeout_at(deadline, rx.recv()).await {
                Ok(Some(record)) => batch.push(record),
                // Senders gone: write what is held, then stop.
                Ok(None) => {
                    flush(sink.as_ref(), &mut batch, &dropped).await;
                    return;
                }
                Err(_) => break,
            }
        }
        flush(sink.as_ref(), &mut batch, &dropped).await;
    }
}

async fn flush(sink: &dyn UsageSink, batch: &mut Vec<ObservedRecord>, dropped: &AtomicU64) {
    if batch.is_empty() {
        return;
    }
    let count = batch.len() as u64;
    match sink.record_batch(batch).await {
        Ok(()) => metrics::record_usage_written(sink.name(), count),
        Err(e) => {
            dropped.fetch_add(count, Ordering::Relaxed);
            metrics::record_usage_dropped(sink.name(), DropReason::SinkError.as_str(), count);
            tracing::warn!(
                sink = sink.name(),
                reason = DropReason::SinkError.as_str(),
                records = count,
                error = %e,
                "usage batch dropped: sink rejected it"
            );
        }
    }
    batch.clear();
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use tokio::sync::Notify;

    use super::super::tests::sample_record;
    use super::*;

    /// Records batch sizes, and optionally blocks the flush task so the buffer
    /// can be driven to overflow deterministically.
    #[derive(Default)]
    struct RecordingSink {
        batches: Mutex<Vec<usize>>,
        release: Option<Arc<Notify>>,
        fail: bool,
    }

    #[async_trait]
    impl UsageSink for RecordingSink {
        fn name(&self) -> &'static str {
            "recording"
        }

        async fn record(&self, _record: &UsageRecord) {}

        async fn record_batch(&self, batch: &[ObservedRecord]) -> Result<(), SinkFailure> {
            if let Some(release) = &self.release {
                release.notified().await;
            }
            self.batches.lock().unwrap().push(batch.len());
            if self.fail {
                return Err(SinkFailure::new("destination unavailable"));
            }
            Ok(())
        }
    }

    fn settings(capacity: usize, max_batch: usize, flush_ms: u64) -> BatchSettings {
        BatchSettings {
            capacity,
            max_batch,
            flush_interval: Duration::from_millis(flush_ms),
        }
    }

    #[tokio::test]
    async fn records_are_written_in_batches_not_one_by_one() {
        let sink = Arc::new(RecordingSink::default());
        let batched =
            BatchedSink::spawn(Arc::clone(&sink) as Arc<dyn UsageSink>, settings(64, 8, 5));
        for _ in 0..8 {
            batched.record(&sample_record()).await;
        }
        // The batch closes on `max_batch`, so a single flush covers all eight.
        for _ in 0..50 {
            if !sink.batches.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(*sink.batches.lock().unwrap(), vec![8]);
        assert_eq!(batched.dropped(), 0);
    }

    #[tokio::test]
    async fn a_partial_batch_still_flushes_on_the_interval() {
        let sink = Arc::new(RecordingSink::default());
        let batched = BatchedSink::spawn(
            Arc::clone(&sink) as Arc<dyn UsageSink>,
            settings(64, 500, 20),
        );
        batched.record(&sample_record()).await;
        for _ in 0..50 {
            if !sink.batches.lock().unwrap().is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(*sink.batches.lock().unwrap(), vec![1]);
    }

    #[tokio::test]
    async fn a_stalled_sink_drops_instead_of_blocking_the_caller() {
        let release = Arc::new(Notify::new());
        let sink = Arc::new(RecordingSink {
            release: Some(Arc::clone(&release)),
            ..RecordingSink::default()
        });
        let batched =
            BatchedSink::spawn(Arc::clone(&sink) as Arc<dyn UsageSink>, settings(4, 1, 5));
        // The flush task holds one record and blocks; the buffer takes four more
        // and everything after that is dropped rather than awaited.
        for _ in 0..32 {
            batched.record(&sample_record()).await;
        }
        assert!(batched.dropped() > 0, "a full buffer must drop");
        assert!(
            sink.batches.lock().unwrap().is_empty(),
            "sink still stalled"
        );
        release.notify_waiters();
    }

    #[tokio::test]
    async fn a_failing_sink_counts_the_batch_as_dropped() {
        let sink = Arc::new(RecordingSink {
            fail: true,
            ..RecordingSink::default()
        });
        let batched =
            BatchedSink::spawn(Arc::clone(&sink) as Arc<dyn UsageSink>, settings(64, 2, 5));
        batched.record(&sample_record()).await;
        batched.record(&sample_record()).await;
        for _ in 0..50 {
            if batched.dropped() > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(batched.dropped(), 2);
    }
}
