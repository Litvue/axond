//! Usage collection — the write path.
//!
//! `UsageSink` is the pluggable destination trait (delta B7/§5.2). Records are
//! built **once** at the end of the request pipeline from `gateway-core`'s
//! `UsageReceipt` and fanned out to every configured sink. Sinks are off the
//! request path: they must be async and are expected to buffer/batch.
//!
//! Three sinks ship: `StdoutSink` (the zero-dependency, no-datastore default),
//! `PostgresSink` (durable, batched rows against a versioned schema), and
//! `OtlpUsageSink` (usage as OTel log records, on the exporter stack telemetry
//! already installed). The durability contract is deliberate and documented in
//! ADR 0009: a slow or failing sink **drops**, counted on
//! `axond.usage.records_dropped`, rather than delaying a request.
//!
//! This seam stays independent of every other backend: it is one of the seven
//! responsibilities catalogued in [`crate::backends`], and there is no universal
//! state backend that a Postgres sink and a Postgres control plane would share.
//! Its drop-rather-than-delay durability contract is exactly the kind of
//! per-seam policy a shared trait would have had to flatten.

mod batch;
pub mod identity;
pub mod journal;
mod otlp;
mod postgres;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::config::{
    UndurablePolicy, UsageJournalBackend, UsageJournalConfig, UsageSinkConfig, UsageSinkKind,
};
use crate::credentials::CredentialSource;
use crate::usage::journal::{
    DeliveryMode, DeliveryWorker, JournalError, PostgresJournal, PostgresJournalSettings,
    UsageEvent, UsageJournal, WorkerHandle, WorkerSettings,
};

pub use batch::{BatchSettings, BatchedSink};
pub use journal::{ConsumerId, DRAIN_MARGIN, DrainReport};
pub use otlp::OtlpUsageSink;
pub use postgres::{PostgresSink, PostgresSinkSettings, tls_connector, validate_table_name};

/// The terminal outcome of a request. Every terminated request produces
/// exactly one record — including failures, cancellations, and partial
/// streams — so spend reconciles (delta B6).
///
/// This is the gateway-observed execution outcome, not a delivery receipt.
/// `Ok` means provider work and response middleware completed and the buffered
/// response was eligible to return. `ClientCancelled` means cancellation was
/// observed before that terminal outcome (or before a stream completed). Once
/// an immutable durable event begins committing, a lost acknowledgement cannot
/// truthfully rewrite the same request identity under different content.
///
/// `Deserialize` as well as `Serialize`, because a journaled record is read back
/// by the delivery worker that writes it to a sink.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[allow(dead_code)] // Ok/UpstreamError wired now; the rest as streaming + cancellation land
pub enum Status {
    Ok,
    UpstreamError,
    ClientCancelled,
    Partial,
    Rejected,
}

impl Status {
    /// Stable, low-cardinality label — the same vocabulary the serialized record
    /// uses, so a metric dimension and a usage row agree.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::UpstreamError => "upstream_error",
            Self::ClientCancelled => "client_cancelled",
            Self::Partial => "partial",
            Self::Rejected => "rejected",
        }
    }

    /// Whether the outcome counts against the upstream error rate.
    pub fn is_error(self) -> bool {
        matches!(self, Self::UpstreamError)
    }
}

/// Neutral, versioned usage vocabulary (delta A3). No product-specific terms:
/// this schema lands in customers' own tables, so it is treated as an API.
///
/// Comparable by value, which the journal needs: an append under an
/// already-present idempotency key is a benign retry only if the content matches,
/// and a mismatch has to be refused rather than overwritten.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct UsageRecord {
    pub schema_version: u32,
    /// The event's identity: a globally unique, time-ordered
    /// [`RequestId`](identity::RequestId) rendered as text, so rows can be
    /// deduplicated across a whole fleet rather than within one process.
    /// Distinct from `trace_id`, which one caller trace shares across many
    /// requests.
    pub request_id: String,
    /// Set when the request was traced, joining the row to the caller's trace.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trace_id: Option<String>,
    pub namespace: String,
    /// Opaque namespace attrs copied at admission (ADR 0063).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attrs: Option<serde_json::Value>,
    /// Active budget period at admission (ADR 0063). Nullable; no schema bump.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub period: Option<String>,
    /// Authenticated caller / gateway-key id.
    pub subject: String,
    /// Configured JWS signer that vouched for the caller; absent for static
    /// gateway-key authentication.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signer_kid: Option<String>,
    /// Model name the caller requested (the alias).
    pub model: String,
    /// Provider + concrete model that actually served it.
    pub target_provider: String,
    pub target_model: String,
    pub credential_source: &'static str,
    /// Label of the specific credential in the pool that served the request —
    /// never the secret. Makes per-key spend and error rates attributable.
    pub credential_id: String,
    pub status: Status,
    /// Non-cached prompt tokens billed at the regular input rate.
    pub input_tokens: u64,
    /// Cached prompt tokens billed at the cache-read rate.
    pub cache_read_tokens: u64,
    /// Prompt tokens written to the provider's cache.
    pub cache_write_tokens: u64,
    pub output_tokens: u64,
    /// Book rate × tokens, or `None` when the request was admitted unpriced.
    pub cost_microdollars: Option<u64>,
    /// Version of the approved price book the cost was computed against, or `0`
    /// when the file configuration priced the request. Populating it is what
    /// makes a historical row auditable: the book version, its checksum, and the
    /// catalogue it was approved against together name immutable content, so a
    /// later publication cannot change what this row was charged at (#147).
    pub catalog_version: u64,
    /// The approved price-book resource version, `price/<id>@v<n>`. Absent when
    /// no book priced the request.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price_book: Option<String>,
    /// Checksum of the approved book's canonical body, so two replicas that
    /// report the same book version are provably reporting the same rates.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price_book_checksum: Option<String>,
    /// Identity of the normalized catalogue content the book was approved
    /// against.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub price_catalog: Option<String>,
    pub latency_ms: u64,
    /// Upstream target attempts made for this request across the alias's
    /// targets; the retry count is one less. `1` when the first target served.
    pub attempts: u32,
}

impl UsageRecord {
    pub const SCHEMA_VERSION: u32 = 2;

    pub fn credential_source_str(source: CredentialSource) -> &'static str {
        match source {
            CredentialSource::Platform => "platform",
            CredentialSource::Byok => "byok",
        }
    }

    /// Spend settled against a budget hold. Unpriced requests settle nothing.
    pub fn settle_cost(&self) -> u64 {
        self.cost_microdollars.unwrap_or(0)
    }
}

/// A record plus the instant the fan-out first saw it. A batching sink flushes
/// later than it enqueues, so the row's timestamp comes from here rather than
/// from flush time — a sink's own buffering must not show up as request time.
#[derive(Debug, Clone)]
pub struct ObservedRecord {
    pub record: UsageRecord,
    pub observed_at: SystemTime,
}

impl ObservedRecord {
    pub fn now(record: UsageRecord) -> Self {
        Self {
            record,
            observed_at: SystemTime::now(),
        }
    }
}

/// Why a batch never reached its destination. A bounded vocabulary, because it
/// is a metric dimension.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropReason {
    /// The sink's buffer was full: the request path chose latency over
    /// durability, as the contract says it must.
    BufferFull,
    /// The destination rejected or could not accept the batch.
    SinkError,
    /// The gateway is shutting down and the buffer could not be drained.
    Shutdown,
}

impl DropReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BufferFull => "buffer_full",
            Self::SinkError => "sink_error",
            Self::Shutdown => "shutdown",
        }
    }
}

/// A batch that did not land. Carries only a message: sink failures are
/// operational, not typed control flow, and the fan-out treats them all the
/// same way (count, log, move on).
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct SinkFailure(pub String);

impl SinkFailure {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

/// What one bounded flush achieved for one sink. A sink that writes inline has
/// nothing buffered, so it reports `Flushed { records: 0 }`.
#[derive(Debug, PartialEq, Eq)]
pub enum FlushOutcome {
    /// Everything the sink was holding reached the destination.
    Flushed { records: u64 },
    /// The destination rejected the buffered records; they are counted as
    /// `sink_error` drops, exactly as they would be while serving.
    Failed { records: u64, error: String },
    /// The flush did not finish inside its bound. Whatever was still queued is
    /// counted as a `shutdown` drop, so the records are accounted for rather
    /// than silently missing.
    TimedOut { abandoned: u64 },
}

impl FlushOutcome {
    /// Stable, low-cardinality label — a metric dimension.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Flushed { .. } => "flushed",
            Self::Failed { .. } => "failed",
            Self::TimedOut { .. } => "timeout",
        }
    }

    /// Whether every buffered record reached the destination.
    pub fn is_complete(&self) -> bool {
        matches!(self, Self::Flushed { .. })
    }
}

#[async_trait]
pub trait UsageSink: Send + Sync {
    fn name(&self) -> &'static str;
    async fn record(&self, record: &UsageRecord);

    /// Deliver a batch in as few round trips as the destination allows. `Err`
    /// means the batch is lost, and the caller counts it as dropped. The
    /// default is a sequential walk, which is right for sinks whose write is
    /// already per-record (stdout, the OTel log pipeline).
    async fn record_batch(&self, batch: &[ObservedRecord]) -> Result<(), SinkFailure> {
        for observed in batch {
            self.record(&observed.record).await;
        }
        Ok(())
    }

    /// Write everything buffered, now. Called once on the shutdown path, under
    /// a bound the caller owns; the default is the honest answer for a sink
    /// whose `record` already wrote through.
    async fn flush(&self) -> FlushOutcome {
        FlushOutcome::Flushed { records: 0 }
    }

    /// Give up on whatever is still buffered, counting it as dropped for
    /// `reason`, and report how much that was. Called when a [`UsageSink::flush`]
    /// did not finish inside its bound — the buffer is unreachable at that point,
    /// so the only honest thing left is to account for it.
    fn abandon(&self, reason: DropReason) -> u64 {
        let _ = reason;
        0
    }
}

/// The no-datastore default: one JSON line per record on stdout.
pub struct StdoutSink;

#[async_trait]
impl UsageSink for StdoutSink {
    fn name(&self) -> &'static str {
        "stdout"
    }

    async fn record(&self, record: &UsageRecord) {
        match serde_json::to_string(record) {
            Ok(line) => println!("{line}"),
            Err(e) => tracing::error!(error = %e, "failed to serialize usage record"),
        }
    }
}

/// Fan-out over the configured sinks.
///
/// The fan-out itself is inline and unbuffered: buffering belongs to the sink
/// that needs it, so one slow destination cannot delay the others and each
/// keeps its own bounded queue and drop count ([`BatchedSink`]).
pub struct UsageFanout {
    sinks: Vec<Box<dyn UsageSink>>,
}

impl UsageFanout {
    pub fn new(sinks: Vec<Box<dyn UsageSink>>) -> Self {
        Self { sinks }
    }

    pub async fn record(&self, record: &UsageRecord) {
        for sink in &self.sinks {
            sink.record(record).await;
        }
    }

    /// Flush every sink within one shared `budget`, and report what each one
    /// managed. The budget is shared rather than per-sink so the fan-out's total
    /// contribution to shutdown stays bounded however many sinks are configured;
    /// a sink that runs out of it abandons its buffer with an explicit drop
    /// reason instead of extending the process's life.
    pub async fn flush(&self, budget: Duration) -> FlushReport {
        let deadline = Instant::now() + budget;
        let mut sinks = Vec::with_capacity(self.sinks.len());
        for sink in &self.sinks {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let outcome = match tokio::time::timeout(remaining, sink.flush()).await {
                Ok(outcome) => outcome,
                Err(_) => FlushOutcome::TimedOut {
                    abandoned: sink.abandon(DropReason::Shutdown),
                },
            };
            crate::telemetry::metrics::record_usage_flush(sink.name(), outcome.as_str());
            sinks.push((sink.name(), outcome));
        }
        FlushReport { sinks }
    }
}

/// What the shutdown flush achieved, per sink. Logged as the process's last
/// word on durability.
#[derive(Debug)]
pub struct FlushReport {
    pub sinks: Vec<(&'static str, FlushOutcome)>,
}

impl FlushReport {
    /// Whether every sink drained. False is the signal that usage rows are
    /// missing — the count and the reason are on
    /// `axond.usage.records_dropped`.
    pub fn is_complete(&self) -> bool {
        self.sinks.iter().all(|(_, outcome)| outcome.is_complete())
    }

    pub fn log(&self) {
        for (sink, outcome) in &self.sinks {
            match outcome {
                FlushOutcome::Flushed { records } => {
                    tracing::info!(sink, records, "usage sink flushed on shutdown")
                }
                FlushOutcome::Failed { records, error } => tracing::error!(
                    sink,
                    records,
                    error = %error,
                    reason = DropReason::SinkError.as_str(),
                    "usage sink rejected its buffered records on shutdown"
                ),
                FlushOutcome::TimedOut { abandoned } => tracing::error!(
                    sink,
                    abandoned,
                    reason = DropReason::Shutdown.as_str(),
                    "usage sink flush exceeded its bound; buffered records were abandoned"
                ),
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum UsageSinkError {
    #[error("usage sink `{kind}`: {message}")]
    Invalid { kind: &'static str, message: String },
    #[error("postgres usage sink: {0}")]
    Postgres(#[from] tokio_postgres::Error),
}

impl UsageSinkError {
    fn invalid(kind: &'static str, message: impl Into<String>) -> Self {
        Self::Invalid {
            kind,
            message: message.into(),
        }
    }
}

/// Whether a sink may buffer, which is the same question as whether a write it
/// accepted is allowed to be lost.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Buffering {
    /// Batch behind a bounded queue that drops when full. The telemetry-grade
    /// default: it keeps sinks off the request path.
    Batched,
    /// Write through, so the caller learns whether the row landed. What a
    /// journal consumer needs, because it acknowledges on the answer.
    WriteThrough,
}

/// The per-sink batching keys a journal takes over, in the order an operator
/// reads them in `axond.toml`.
///
/// Enabling the journal replaces the sink's own queue with a durable one, so
/// these three stop describing anything: the queue is the outbox (bounded by
/// `[usage_journal] max_events`), the batch is a claim (`claim_batch`), and the
/// flush cadence is the poll interval (`poll_interval_ms`). They are named at
/// boot rather than silently ignored, because a deployment that tuned them is
/// entitled to know they no longer apply.
const JOURNAL_OWNED_BATCH_KEYS: [&str; 3] = ["buffer_capacity", "max_batch", "flush_interval_ms"];

/// Which of [`JOURNAL_OWNED_BATCH_KEYS`] a sink actually set, and only for the
/// kinds that ever batched: a stdout or OTLP sink never had a queue to lose.
fn journal_owned_batch_keys(configs: &[UsageSinkConfig]) -> Vec<&'static str> {
    let mut named = Vec::new();
    for config in configs {
        if config.kind != UsageSinkKind::Postgres {
            continue;
        }
        let defaults = UsageSinkConfig::default();
        let set = [
            config.buffer_capacity != defaults.buffer_capacity,
            config.max_batch_explicit,
            config.flush_interval_ms != defaults.flush_interval_ms,
        ];
        for (key, was_set) in JOURNAL_OWNED_BATCH_KEYS.iter().zip(set) {
            if was_set && !named.contains(key) {
                named.push(*key);
            }
        }
    }
    named
}

/// How usage leaves the request path, in whichever mode the deployment chose.
///
/// The two modes are deliberately one type rather than two call sites in
/// [`crate::routes`]: the request path asks for an event to be recorded and gets
/// back either nothing to worry about or a refusal, and which of the two is
/// possible is a property of the configuration rather than of the route.
pub struct UsageDelivery {
    /// The telemetry-grade fan-out. Empty in billing-grade mode, where the sinks
    /// belong to the delivery worker.
    fanout: UsageFanout,
    journal: Option<Arc<dyn UsageJournal>>,
    on_undurable: UndurablePolicy,
    /// Management-API index. Attached after the Store opens so `GET .../usage`
    /// can summarize rows the request path already recorded.
    store: std::sync::OnceLock<Arc<dyn crate::store::Store>>,
    /// Bounded queue into the usage-index worker. `append_store` only
    /// `try_send`s; a full queue drops the event rather than spawning work.
    index_tx: std::sync::OnceLock<IndexQueue>,
    /// Occupied slots in `index_tx` (reserved permits plus queued events).
    /// Incremented after a slot is taken and decremented when the worker
    /// receives or drains, so the histogram cannot wrap or exceed the bound.
    index_depth: Arc<AtomicU64>,
    /// Set on drop so the worker abandons queued items after the in-flight write.
    index_stop: Arc<AtomicBool>,
    /// Counters and rate-limited logs shared with the index worker.
    index: Arc<IndexTelemetry>,
    /// Test-only witness for [`UsageDelivery::count_unheard_refusal`]: the loss
    /// counter it moves is a global instrument no test can read back.
    #[cfg(test)]
    unheard: std::sync::atomic::AtomicU64,
}

/// Batching policy for the management usage index. Built from
/// `[storage.usage_index]`; the defaults are what a deployment that never set
/// the table gets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UsageIndexSettings {
    /// Events queued ahead of the worker before the request path drops.
    pub capacity: usize,
    /// Rows per Store transaction, at most
    /// [`MAX_USAGE_INDEX_BATCH`](crate::store::MAX_USAGE_INDEX_BATCH).
    pub max_batch: usize,
    /// How long a partial batch waits for company before it is written anyway.
    pub flush_interval: Duration,
    /// Bound on one cancellable (Postgres) batch write inside the worker. Not
    /// configurable: the session's own `statement_timeout` is 5 s, and this is
    /// the tighter bound so a slow index write is abandoned before it can hold
    /// the worker for longer than a batch is worth.
    pub write_timeout: Duration,
}

impl Default for UsageIndexSettings {
    fn default() -> Self {
        Self {
            capacity: 1024,
            max_batch: 256,
            flush_interval: Duration::from_millis(50),
            write_timeout: Duration::from_secs(2),
        }
    }
}

impl UsageIndexSettings {
    /// The settings as the worker applies them: a batch never exceeds the
    /// queue, the Store's bound, or falls below one row, whatever the file said.
    fn bounded(self) -> Self {
        let capacity = self.capacity.max(1);
        Self {
            capacity,
            max_batch: self
                .max_batch
                .clamp(1, crate::store::MAX_USAGE_INDEX_BATCH)
                .min(capacity),
            ..self
        }
    }
}

/// How one management usage-index event ended. A bounded vocabulary, because it
/// is the `axond.index.outcome` metric dimension.
///
/// `Saturated` and `Closed` were split out of `Timeout` and `Failed`: a full
/// queue used to be counted as a timeout although nothing ever waited, and a
/// missing worker as a Store failure. The two older values keep their names and
/// now mean only what they say.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexOutcome {
    /// The Store committed the row (or already had it).
    Accepted,
    /// The bounded queue was full: the event was dropped without waiting.
    Saturated,
    /// The worker is gone (never started, or stopped), so nothing could take it.
    Closed,
    /// The Store refused the write.
    Failed,
    /// The Store write did not finish inside its deadline.
    Timeout,
}

impl IndexOutcome {
    #[cfg(test)]
    pub const ALL: [Self; 5] = [
        Self::Accepted,
        Self::Saturated,
        Self::Closed,
        Self::Failed,
        Self::Timeout,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Saturated => "saturated",
            Self::Closed => "closed",
            Self::Failed => "failed",
            Self::Timeout => "timeout",
        }
    }
}

/// Every [`IndexOutcome`], as the strings the metric catalogue enumerates. A
/// test holds it to the enum.
pub const INDEX_OUTCOMES: &[&str] = &["accepted", "saturated", "closed", "failed", "timeout"];

/// One event waiting for the index worker, stamped so the worker can report how
/// long it waited.
struct QueuedAppend {
    event: crate::store::UsageAppend,
    enqueued_at: Instant,
}

/// Serializes a blocking enqueue with the worker's depth decrement. Held across
/// `try_send` plus the increment so a successful send is never visible to
/// `recv` before occupancy is recorded, and concurrent failed sends cannot
/// inflate the histogram sample.
struct BlockingIndex {
    tx: std::sync::mpsc::SyncSender<QueuedAppend>,
    admit: Arc<Mutex<()>>,
}

/// The request path's handle on the index worker's queue. Two shapes because
/// the SQLite worker is an OS thread that must wait without a runtime, while
/// the Postgres worker is a task that must wait without a thread.
enum IndexQueue {
    Blocking(BlockingIndex),
    Async(tokio::sync::mpsc::Sender<QueuedAppend>),
}

impl IndexQueue {
    /// Occupy one slot and hand the event to the worker. Returns the occupied
    /// slot count after this send, matching `axond.usage.index.queue.depth`.
    fn try_enqueue(&self, item: QueuedAppend, depth: &AtomicU64) -> Result<u64, IndexOutcome> {
        match self {
            Self::Blocking(queue) => {
                let _admit = queue
                    .admit
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                match queue.tx.try_send(item) {
                    Ok(()) => Ok(depth.fetch_add(1, Ordering::AcqRel) + 1),
                    Err(std::sync::mpsc::TrySendError::Full(_)) => Err(IndexOutcome::Saturated),
                    Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                        Err(IndexOutcome::Closed)
                    }
                }
            }
            Self::Async(tx) => match tx.try_reserve() {
                Ok(permit) => {
                    // The permit occupies a real slot, so the worker cannot recv
                    // this event until `send` below. Depth is then occupied slots,
                    // not in-flight send attempts.
                    let occupied = depth.fetch_add(1, Ordering::AcqRel) + 1;
                    permit.send(item);
                    Ok(occupied)
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    Err(IndexOutcome::Saturated)
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => Err(IndexOutcome::Closed),
            },
        }
    }
}

/// A log line that is emitted at most once per [`Self::INTERVAL`] while the
/// condition persists, carrying how many occurrences it stands for. The metric
/// counters it accompanies are incremented on every occurrence regardless.
struct RateLimitedLog {
    last_emitted: std::sync::Mutex<Option<Instant>>,
    suppressed: AtomicU64,
    #[cfg(test)]
    emitted: AtomicU64,
}

impl RateLimitedLog {
    /// A sustained overflow or outage is one line every ten seconds, not one per
    /// event: the exact count is on the counter.
    const INTERVAL: Duration = Duration::from_secs(10);

    fn new() -> Self {
        Self {
            last_emitted: std::sync::Mutex::new(None),
            suppressed: AtomicU64::new(0),
            #[cfg(test)]
            emitted: AtomicU64::new(0),
        }
    }

    /// `Some(suppressed)` when the caller should log now, with the number of
    /// occurrences since the last line that went unlogged.
    fn should_emit(&self) -> Option<u64> {
        let now = Instant::now();
        let mut last = self
            .last_emitted
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if last.is_some_and(|last| now.duration_since(last) < Self::INTERVAL) {
            self.suppressed.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        *last = Some(now);
        #[cfg(test)]
        self.emitted.fetch_add(1, Ordering::Relaxed);
        Some(self.suppressed.swap(0, Ordering::Relaxed))
    }
}

/// What the index worker and the request path share about the index: the
/// rate-limited logs for its two persistent failure modes, and in tests the
/// exact per-outcome counts the global instruments cannot be read back for.
struct IndexTelemetry {
    /// Request-path drops: the queue was full or the worker is gone.
    drop_log: RateLimitedLog,
    /// Worker-side failures: the Store refused or did not answer in time.
    write_log: RateLimitedLog,
    #[cfg(test)]
    outcomes: std::sync::Mutex<std::collections::BTreeMap<&'static str, u64>>,
    #[cfg(test)]
    batches: AtomicU64,
    #[cfg(test)]
    largest_batch: AtomicU64,
}

impl IndexTelemetry {
    fn new() -> Self {
        Self {
            drop_log: RateLimitedLog::new(),
            write_log: RateLimitedLog::new(),
            #[cfg(test)]
            outcomes: std::sync::Mutex::new(std::collections::BTreeMap::new()),
            #[cfg(test)]
            batches: AtomicU64::new(0),
            #[cfg(test)]
            largest_batch: AtomicU64::new(0),
        }
    }

    /// An event the request path could not hand to the worker.
    fn dropped(&self, outcome: IndexOutcome, request_id: &str) {
        crate::telemetry::metrics::record_usage_index_append(outcome.as_str(), 1);
        self.count(outcome, 1);
        if let Some(suppressed) = self.drop_log.should_emit() {
            tracing::error!(
                request_id = %request_id,
                outcome = outcome.as_str(),
                suppressed,
                "store usage append dropped: {}",
                match outcome {
                    IndexOutcome::Saturated => "the index queue is full",
                    _ => "the index worker is not running",
                }
            );
        }
    }

    /// One Store write of `batch` finished with `outcome`. `queue_age_ms` is
    /// how long the oldest event had already waited when the write *began*,
    /// not when it returned — otherwise a slow Store transaction looks like
    /// queue delay.
    fn wrote(
        &self,
        batch: &[QueuedAppend],
        outcome: IndexOutcome,
        error: Option<&str>,
        queue_age_ms: f64,
    ) {
        let rows = batch.len() as u64;
        crate::telemetry::metrics::record_usage_index_batch(outcome.as_str(), rows, queue_age_ms);
        self.count(outcome, rows);
        #[cfg(test)]
        {
            self.batches.fetch_add(1, Ordering::Relaxed);
            self.largest_batch.fetch_max(rows, Ordering::Relaxed);
        }
        if outcome == IndexOutcome::Accepted {
            return;
        }
        if let Some(suppressed) = self.write_log.should_emit() {
            let first = batch
                .first()
                .map_or("", |first| first.event.request_id.as_str());
            tracing::error!(
                request_id = %first,
                rows,
                outcome = outcome.as_str(),
                error = error.unwrap_or("write deadline elapsed"),
                suppressed,
                "store usage append batch was lost"
            );
        }
    }

    #[cfg(test)]
    fn count(&self, outcome: IndexOutcome, n: u64) {
        *self
            .outcomes
            .lock()
            .expect("outcomes")
            .entry(outcome.as_str())
            .or_default() += n;
    }

    #[cfg(not(test))]
    fn count(&self, _: IndexOutcome, _: u64) {}
}

/// A usage event that could not be made durable, and the request that must now
/// decide what to do about it.
#[derive(Debug, thiserror::Error)]
#[error("the usage event for `{request_id}` could not be journaled ({reason}): {detail}")]
pub struct NotDurable {
    pub request_id: String,
    /// Stable, low-cardinality: the same value the metric carries.
    pub reason: &'static str,
    pub detail: String,
}

impl UsageDelivery {
    /// Telemetry-grade: best effort, non-blocking, lossy under overload.
    pub fn telemetry(fanout: UsageFanout) -> Self {
        Self {
            fanout,
            journal: None,
            on_undurable: UndurablePolicy::Serve,
            store: std::sync::OnceLock::new(),
            index_tx: std::sync::OnceLock::new(),
            index_depth: Arc::new(AtomicU64::new(0)),
            index_stop: Arc::new(AtomicBool::new(false)),
            index: Arc::new(IndexTelemetry::new()),
            #[cfg(test)]
            unheard: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Billing-grade: the record is durable before this returns, or the request
    /// is told it was not.
    pub fn billing(journal: Arc<dyn UsageJournal>, on_undurable: UndurablePolicy) -> Self {
        Self {
            fanout: UsageFanout::new(Vec::new()),
            journal: Some(journal),
            on_undurable,
            store: std::sync::OnceLock::new(),
            index_tx: std::sync::OnceLock::new(),
            index_depth: Arc::new(AtomicU64::new(0)),
            index_stop: Arc::new(AtomicBool::new(false)),
            index: Arc::new(IndexTelemetry::new()),
            #[cfg(test)]
            unheard: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Index usage for `GET /api/v1/namespaces/{ns}/usage`. Idempotent.
    ///
    /// One worker per delivery: an OS thread for a Store whose index write is
    /// blocking (SQLite), a task otherwise (Postgres). Either way the request
    /// path only ever `try_send`s onto a queue of `settings.capacity`, and the
    /// worker writes what has queued in transactions of at most
    /// `settings.max_batch` rows, lingering `settings.flush_interval` for a
    /// partial batch to fill.
    pub fn attach_store(&self, store: Arc<dyn crate::store::Store>, settings: UsageIndexSettings) {
        if self.store.set(Arc::clone(&store)).is_err() {
            return;
        }
        let settings = settings.bounded();
        let stop = Arc::clone(&self.index_stop);
        let telemetry = Arc::clone(&self.index);
        let depth = Arc::clone(&self.index_depth);
        let queue = if store.blocking_usage_index() {
            let (tx, rx) = std::sync::mpsc::sync_channel(settings.capacity);
            let admit = Arc::new(Mutex::new(()));
            let worker_admit = Arc::clone(&admit);
            match std::thread::Builder::new()
                .name("axond-usage-index".into())
                .spawn(move || {
                    sqlite_usage_index_worker(
                        store,
                        rx,
                        stop,
                        settings,
                        telemetry,
                        depth,
                        worker_admit,
                    )
                }) {
                Ok(handle) => drop(handle),
                Err(error) => {
                    tracing::error!(
                        error = %error,
                        "store usage-index worker failed to start"
                    );
                    return;
                }
            }
            IndexQueue::Blocking(BlockingIndex { tx, admit })
        } else if tokio::runtime::Handle::try_current().is_ok() {
            let (tx, rx) = tokio::sync::mpsc::channel(settings.capacity);
            drop(tokio::spawn(async_usage_index_worker(
                store, rx, stop, settings, telemetry, depth,
            )));
            IndexQueue::Async(tx)
        } else {
            tracing::error!("store usage-index worker needs a tokio runtime");
            return;
        };
        let _ = self.index_tx.set(queue);
    }

    /// Best-effort management-index append. Fire-and-forget: `try_send` onto
    /// one worker, never `spawn_blocking` per record. A successful journal
    /// append must not wait here. Idempotency is the Store's `request_id`
    /// primary key, which is also what lets the worker retry nothing: a batch
    /// that fails is counted and dropped, and the next request's row is new.
    fn append_store(&self, record: &UsageRecord) {
        if self.store.get().is_none() {
            return;
        }
        let event = crate::store::UsageAppend {
            request_id: record.request_id.clone(),
            namespace: record.namespace.clone(),
            period: record.period.clone(),
            model: record.model.clone(),
            status: record.status.as_str().to_owned(),
            cost_microdollars: record.cost_microdollars,
        };
        let Some(queue) = self.index_tx.get() else {
            self.index.dropped(IndexOutcome::Closed, &record.request_id);
            return;
        };
        match queue.try_enqueue(
            QueuedAppend {
                event,
                enqueued_at: Instant::now(),
            },
            &self.index_depth,
        ) {
            Ok(occupied) => {
                crate::telemetry::metrics::record_usage_index_enqueued(occupied);
            }
            Err(outcome) => self.index.dropped(outcome, &record.request_id),
        }
    }

    pub fn mode(&self) -> DeliveryMode {
        self.journal
            .as_ref()
            .map_or(DeliveryMode::TelemetryGrade, |journal| journal.mode())
    }

    /// Whether [`record`](Self::record) is an append to a journal rather than a
    /// hand-off to the fan-out.
    ///
    /// Not [`mode`](Self::mode): a journal that cannot outlive its process still
    /// reports [`DeliveryMode::TelemetryGrade`], and the question here is only
    /// whether recording is long enough to be worth protecting from a caller
    /// hanging up inside it.
    pub fn appends(&self) -> bool {
        self.journal.is_some()
    }

    /// Record one terminated request's usage.
    ///
    /// In telemetry-grade mode this is the fan-out and cannot fail. In
    /// billing-grade mode it is a durable append, and the `Err` is the request
    /// path's cue: under [`UndurablePolicy::Refuse`] the caller is told the
    /// request was not recorded rather than being billed for nothing.
    pub async fn record(&self, record: &UsageRecord) -> Result<(), NotDurable> {
        let Some(journal) = self.journal.as_ref() else {
            self.append_store(record);
            self.fanout.record(record).await;
            return Ok(());
        };
        let event = match UsageEvent::new(ObservedRecord::now(record.clone())) {
            Ok(event) => event,
            Err(error) => {
                return self.undurable(record, "invalid_event", error.to_string());
            }
        };
        match journal.append(&event).await {
            Ok(appended) => {
                crate::telemetry::metrics::record_usage_journal_append(
                    journal.name(),
                    if appended.is_new() {
                        "accepted"
                    } else {
                        // A retried append of an identical event. Not an error:
                        // the fact is already durable exactly once.
                        "already_present"
                    },
                );
                self.append_store(record);
                Ok(())
            }
            Err(error) => {
                let reason = match &error {
                    JournalError::AtCapacity { .. } => "at_capacity",
                    JournalError::Conflict { .. } => "conflict",
                    _ => "backend",
                };
                self.undurable(record, reason, error.to_string())
            }
        }
    }

    /// Count the failure, and let the configured policy decide whether the
    /// request survives it.
    fn undurable(
        &self,
        record: &UsageRecord,
        reason: &'static str,
        detail: String,
    ) -> Result<(), NotDurable> {
        let journal = self
            .journal
            .as_ref()
            .map_or("none", |journal| journal.name());
        crate::telemetry::metrics::record_usage_journal_append(journal, reason);
        if self.on_undurable.refuses() {
            return Err(NotDurable {
                request_id: record.request_id.clone(),
                reason,
                detail,
            });
        }
        // Served anyway, by explicit configuration: the event is gone, and it is
        // counted where every other lost usage record is counted.
        tracing::error!(
            request_id = %record.request_id,
            reason,
            detail = %detail,
            "usage event was not journaled and the request was served anyway"
        );
        crate::telemetry::metrics::record_usage_journal_lost(journal, reason, 1);
        Ok(())
    }

    /// Count a refusal that reached nobody as a loss.
    ///
    /// A refusal is not a loss while there is a caller to hand it to: the
    /// request is answered `503`, its spend is unwound, and the event it
    /// describes can be produced again by a retry, so [`record`](Self::record)
    /// leaves the loss counter alone. A caller that hung up before the append
    /// finished never receives that status, its spend has already been settled,
    /// and the billable fact is as gone as one served under
    /// [`UndurablePolicy::Serve`] — which is the counter this is.
    pub fn count_unheard_refusal(&self, refusal: &NotDurable) {
        let journal = self
            .journal
            .as_ref()
            .map_or("none", |journal| journal.name());
        tracing::error!(
            request_id = %refusal.request_id,
            reason = refusal.reason,
            detail = %refusal.detail,
            "a usage event could not be journaled and the caller was gone before it could be \
             told, so the request stands charged with nothing recorded"
        );
        crate::telemetry::metrics::record_usage_journal_lost(journal, refusal.reason, 1);
        #[cfg(test)]
        self.unheard
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// How many refusals were counted as losses because nobody was left to hear
    /// them.
    #[cfg(test)]
    pub fn unheard_refusals(&self) -> u64 {
        self.unheard.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// How many index events ended in `outcome`, exactly, whether they were
    /// dropped on the request path or written (or not) by the worker.
    #[cfg(test)]
    pub fn index_outcome(&self, outcome: IndexOutcome) -> u64 {
        self.index
            .outcomes
            .lock()
            .expect("outcomes")
            .get(outcome.as_str())
            .copied()
            .unwrap_or_default()
    }

    /// Store writes the index worker has made, and the largest of them.
    #[cfg(test)]
    pub fn index_batches(&self) -> (u64, u64) {
        (
            self.index.batches.load(Ordering::Relaxed),
            self.index.largest_batch.load(Ordering::Relaxed),
        )
    }

    /// Log lines the two rate-limited index logs have emitted: `(drops, writes)`.
    #[cfg(test)]
    pub fn index_log_lines(&self) -> (u64, u64) {
        (
            self.index.drop_log.emitted.load(Ordering::Relaxed),
            self.index.write_log.emitted.load(Ordering::Relaxed),
        )
    }

    /// Ask the index worker to exit after its next dequeue, as `Drop` does.
    #[cfg(test)]
    pub fn stop_index_worker(&self) {
        self.index_stop.store(true, Ordering::Release);
    }

    /// Record where the caller has no way to refuse: a stream that has already
    /// been relayed, or a cancellation. The event is still appended durably
    /// first; what changes is that a failure can only be reported, so it is
    /// counted as a loss rather than returned.
    pub async fn record_terminal(&self, record: &UsageRecord) {
        if let Err(error) = self.record(record).await {
            let journal = self
                .journal
                .as_ref()
                .map_or("none", |journal| journal.name());
            tracing::error!(
                request_id = %error.request_id,
                reason = error.reason,
                detail = %error.detail,
                "a terminated request's usage event could not be journaled and cannot be refused"
            );
            crate::telemetry::metrics::record_usage_journal_lost(journal, error.reason, 1);
        }
    }

    /// Flush what is buffered. Telemetry-grade only: a journal's backlog is
    /// durable, so it is drained by the worker's own bounded shutdown rather
    /// than flushed here. The usage-index worker is best-effort: drop the
    /// sender and do not join it.
    pub async fn flush(&self, budget: Duration) -> FlushReport {
        self.fanout.flush(budget).await
    }
}

impl Drop for UsageDelivery {
    fn drop(&mut self) {
        self.index_stop.store(true, Ordering::Release);
    }
}

fn take_index_slot(depth: &AtomicU64) {
    depth.fetch_sub(1, Ordering::AcqRel);
}

fn oldest_queue_age_ms(batch: &[QueuedAppend]) -> f64 {
    batch
        .first()
        .map(|oldest| oldest.enqueued_at.elapsed().as_secs_f64() * 1_000.0)
        .unwrap_or_default()
}

impl QueuedAppend {
    fn take(self, depth: &AtomicU64) -> Self {
        take_index_slot(depth);
        crate::telemetry::metrics::record_usage_index_dequeued(
            self.enqueued_at.elapsed().as_secs_f64() * 1000.0,
        );
        self
    }

    fn take_blocking(self, depth: &AtomicU64, admit: &Mutex<()>) -> Self {
        {
            let _admit = admit
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            take_index_slot(depth);
        }
        crate::telemetry::metrics::record_usage_index_dequeued(
            self.enqueued_at.elapsed().as_secs_f64() * 1000.0,
        );
        self
    }
}

fn drain_blocking_index(
    rx: &std::sync::mpsc::Receiver<QueuedAppend>,
    depth: &AtomicU64,
    admit: &Mutex<()>,
) {
    while rx.try_recv().is_ok() {
        let _admit = admit
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        take_index_slot(depth);
    }
}

/// The blocking (SQLite) index worker: block for the first queued event, gather
/// what else arrives within `flush_interval` up to `max_batch`, write the batch
/// in one transaction on this thread, repeat. Exits when every sender is gone or
/// the delivery is dropped; queued events are then abandoned, which is the
/// crash-loss the index accepts (ADR 0064 keeps the budget charge elsewhere).
fn sqlite_usage_index_worker(
    store: Arc<dyn crate::store::Store>,
    rx: std::sync::mpsc::Receiver<QueuedAppend>,
    stop: Arc<AtomicBool>,
    settings: UsageIndexSettings,
    telemetry: Arc<IndexTelemetry>,
    depth: Arc<AtomicU64>,
    admit: Arc<Mutex<()>>,
) {
    let mut batch: Vec<QueuedAppend> = Vec::with_capacity(settings.max_batch);
    while let Ok(first) = rx.recv() {
        let first = first.take_blocking(&depth, &admit);
        if stop.load(Ordering::Acquire) {
            drain_blocking_index(&rx, &depth, &admit);
            return;
        }
        batch.push(first);
        let deadline = Instant::now() + settings.flush_interval;
        while batch.len() < settings.max_batch {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let next = if remaining.is_zero() {
                rx.try_recv().map_err(|_| ())
            } else {
                rx.recv_timeout(remaining).map_err(|_| ())
            };
            match next {
                Ok(item) => batch.push(item.take_blocking(&depth, &admit)),
                Err(()) => break,
            }
        }
        if stop.load(Ordering::Acquire) {
            drain_blocking_index(&rx, &depth, &admit);
            return;
        }
        let events: Vec<crate::store::UsageAppend> =
            batch.iter().map(|item| item.event.clone()).collect();
        let queue_age_ms = oldest_queue_age_ms(&batch);
        match store.append_usage_batch_sync(&events) {
            Ok(()) => telemetry.wrote(&batch, IndexOutcome::Accepted, None, queue_age_ms),
            Err(error) => telemetry.wrote(
                &batch,
                IndexOutcome::Failed,
                Some(&error.to_string()),
                queue_age_ms,
            ),
        }
        batch.clear();
    }
}

/// The task (Postgres) index worker: the same gather-then-write loop, with each
/// write bounded by `settings.write_timeout`. A write that misses the deadline
/// is dropped and counted as `timeout`; dedup on `request_id` means a statement
/// that finishes server-side after being abandoned costs nothing.
async fn async_usage_index_worker(
    store: Arc<dyn crate::store::Store>,
    mut rx: tokio::sync::mpsc::Receiver<QueuedAppend>,
    stop: Arc<AtomicBool>,
    settings: UsageIndexSettings,
    telemetry: Arc<IndexTelemetry>,
    depth: Arc<AtomicU64>,
) {
    let mut batch: Vec<QueuedAppend> = Vec::with_capacity(settings.max_batch);
    while let Some(first) = rx.recv().await {
        let first = first.take(&depth);
        if stop.load(Ordering::Acquire) {
            while rx.try_recv().is_ok() {
                take_index_slot(&depth);
            }
            return;
        }
        batch.push(first);
        let deadline = tokio::time::Instant::now() + settings.flush_interval;
        while batch.len() < settings.max_batch {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some(item)) => batch.push(item.take(&depth)),
                Ok(None) | Err(_) => break,
            }
        }
        if stop.load(Ordering::Acquire) {
            while rx.try_recv().is_ok() {
                take_index_slot(&depth);
            }
            return;
        }
        let events: Vec<crate::store::UsageAppend> =
            batch.iter().map(|item| item.event.clone()).collect();
        let queue_age_ms = oldest_queue_age_ms(&batch);
        match tokio::time::timeout(settings.write_timeout, store.append_usage_batch(events)).await {
            Ok(Ok(())) => telemetry.wrote(&batch, IndexOutcome::Accepted, None, queue_age_ms),
            Ok(Err(error)) => telemetry.wrote(
                &batch,
                IndexOutcome::Failed,
                Some(&error.to_string()),
                queue_age_ms,
            ),
            Err(_) => telemetry.wrote(&batch, IndexOutcome::Timeout, None, queue_age_ms),
        }
        batch.clear();
    }
}

/// The usage write path a process booted with: how records leave the request,
/// and the worker that delivers them when they are journaled.
pub struct UsageRuntime {
    pub delivery: Arc<UsageDelivery>,
    /// Present exactly when a journal is configured.
    pub worker: Option<WorkerHandle>,
}

/// Build the usage write path from configuration.
///
/// Connecting happens here, so a deployment that asked for billing-grade
/// delivery and cannot reach its outbox refuses to boot rather than discovering
/// at the first request that it must fail closed.
pub async fn build_runtime(
    sinks: &[UsageSinkConfig],
    journal: &UsageJournalConfig,
    env: &HashMap<String, String>,
) -> Result<UsageRuntime, UsageSinkError> {
    if journal.backend == UsageJournalBackend::None {
        let sinks = build_sinks(sinks, env, Buffering::Batched).await?;
        return Ok(UsageRuntime {
            delivery: Arc::new(UsageDelivery::telemetry(UsageFanout::new(sinks))),
            worker: None,
        });
    }
    let dsn_env = journal.dsn_env.as_deref().unwrap_or_default();
    let dsn = env
        .get(dsn_env)
        .filter(|dsn| !dsn.trim().is_empty())
        .ok_or_else(|| {
            UsageSinkError::invalid(
                "journal",
                format!("`{dsn_env}` is unset or empty in the environment"),
            )
        })?;
    let store = PostgresJournal::connect(
        dsn,
        PostgresJournalSettings {
            schema: journal.schema.clone(),
            create_schema: journal.create_schema,
            capacity: journal.capacity(),
            connect_timeout: Duration::from_millis(journal.connect_timeout_ms),
            operation_timeout: Duration::from_millis(journal.operation_timeout_ms),
            connections: journal.connections,
        },
    )
    .await
    .map_err(|error| UsageSinkError::invalid("journal", error.to_string()))?;
    let store: Arc<dyn UsageJournal> = Arc::new(store);
    let capacity = store.capacity();
    if capacity.policy.can_lose_events() {
        tracing::warn!(
            journal = store.name(),
            policy = capacity.policy.as_str(),
            max_events = capacity.max_events,
            "the usage journal may drop accepted events when it fills; \
             `capacity_policy = \"refuse\"` is the billing-grade setting"
        );
    }
    // Split before anything is built: an `otlp` sink is a destination the
    // worker tells, not one it acknowledges on, so it takes no part in the
    // durable contract and none of the checks that contract implies.
    let (advisory, durable): (Vec<UsageSinkConfig>, Vec<UsageSinkConfig>) = sinks
        .iter()
        .cloned()
        .partition(|sink| sink.kind == UsageSinkKind::Otlp);
    if !advisory.is_empty() {
        tracing::info!(
            journal = store.name(),
            sinks = advisory.len(),
            "usage telemetry sinks are exported alongside billing-grade delivery but are not \
             acknowledged on, because they cannot report a failed write"
        );
    }
    // Not refused, because a `stdout` destination is how the mode is tried out
    // and how a shipping pipeline can legitimately collect it. It is warned
    // about because an acknowledgement is only worth what the destination is:
    // once every destination has acknowledged an event, retention forgets it.
    if durable
        .iter()
        .all(|sink| sink.kind == UsageSinkKind::Stdout)
    {
        tracing::warn!(
            journal = store.name(),
            retain_acknowledged_seconds = capacity.retain_acknowledged.as_secs(),
            "the usage journal's only destination is `stdout`, so an acknowledgement means a \
             log line was written and the event is forgotten once retention expires; a \
             billing-grade destination should be one that stores the row"
        );
    }
    let consumer = ConsumerId::parse(&journal.consumer)
        .map_err(|error| UsageSinkError::invalid("journal", error.to_string()))?;
    // Write-through, because the worker acknowledges on what the sink returns: a
    // batching sink would have it acknowledge a row that does not exist yet.
    let owned = journal_owned_batch_keys(&durable);
    if !owned.is_empty() {
        tracing::warn!(
            journal = store.name(),
            keys = owned.join(", "),
            claim_batch = journal.claim_batch,
            poll_interval_ms = journal.poll_interval_ms,
            "the usage journal owns sink batching; these `[[usage_sink]]` keys no \
             longer apply and `[usage_journal]` claim_batch/poll_interval_ms \
             replace them"
        );
    }
    let acknowledged = build_sinks(&durable, env, Buffering::WriteThrough).await?;
    let exported = if advisory.is_empty() {
        Vec::new()
    } else {
        build_sinks(&advisory, env, Buffering::WriteThrough).await?
    };
    let worker = DeliveryWorker::new(
        Arc::clone(&store),
        Arc::new(acknowledged),
        WorkerSettings {
            consumer,
            claim_batch: journal.claim_batch,
            lease: Duration::from_secs(journal.lease_seconds),
            poll_interval: Duration::from_millis(journal.poll_interval_ms),
            maintain_interval: Duration::from_secs(60),
        },
    )
    .also_telling(Arc::new(exported))
    .spawn();
    Ok(UsageRuntime {
        delivery: Arc::new(UsageDelivery::billing(store, journal.on_undurable)),
        worker: Some(worker),
    })
}

/// Build the configured sinks, or the stdout default when none are declared.
///
/// Connecting and (optionally) creating the table happens here so a
/// misconfigured datastore refuses to boot instead of silently dropping every
/// record at request time.
pub async fn build_sinks(
    configs: &[UsageSinkConfig],
    env: &HashMap<String, String>,
    buffering: Buffering,
) -> Result<Vec<Box<dyn UsageSink>>, UsageSinkError> {
    if configs.is_empty() {
        return Ok(vec![Box::new(StdoutSink)]);
    }
    let mut sinks: Vec<Box<dyn UsageSink>> = Vec::with_capacity(configs.len());
    for config in configs {
        match config.kind {
            UsageSinkKind::Stdout => sinks.push(Box::new(StdoutSink)),
            UsageSinkKind::Otlp => sinks.push(Box::new(OtlpUsageSink::new()?)),
            UsageSinkKind::Postgres => {
                let dsn_env = config.dsn_env.as_deref().unwrap_or_default();
                let dsn = env
                    .get(dsn_env)
                    .filter(|dsn| !dsn.trim().is_empty())
                    .ok_or_else(|| {
                        UsageSinkError::invalid(
                            "postgres",
                            format!("`{dsn_env}` is unset or empty in the environment"),
                        )
                    })?;
                let sink = PostgresSink::connect(
                    dsn,
                    PostgresSinkSettings {
                        table: config.table(),
                        create_table: config.create_table,
                    },
                )
                .await?;
                match buffering {
                    Buffering::Batched => sinks.push(Box::new(BatchedSink::spawn(
                        Arc::new(sink),
                        config.batch_settings(),
                    ))),
                    // The journal is the buffer, and it is a durable one, so the
                    // sink's own queue would only add a place for a row to be
                    // lost after it was acknowledged.
                    Buffering::WriteThrough => sinks.push(Box::new(sink)),
                }
            }
        }
    }
    Ok(sinks)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Store;

    /// A record with every field filled, for sink tests.
    pub(super) fn sample_record() -> UsageRecord {
        UsageRecord {
            schema_version: UsageRecord::SCHEMA_VERSION,
            request_id: identity::next_request_id().to_string(),
            trace_id: Some("4bf92f3577b34da6a3ce929d0e0e4736".to_string()),
            namespace: "acme".to_string(),
            attrs: Some(serde_json::json!({"org": "acme", "env": "prod"})),
            period: Some("2026-09".to_string()),
            subject: "GW_INBOUND_ACME_KEY".to_string(),
            signer_kid: Some("verifier-1".to_string()),
            model: "gpt-4o".to_string(),
            target_provider: "openai".to_string(),
            target_model: "gpt-4o-2024-08-06".to_string(),
            credential_source: "byok",
            credential_id: "openai-primary".to_string(),
            status: Status::Ok,
            input_tokens: 120,
            cache_read_tokens: 12,
            cache_write_tokens: 0,
            output_tokens: 34,
            cost_microdollars: Some(640),
            catalog_version: 7,
            price_book: Some("price/res_0190f2c1-6f6a-7c2e-9d3a-6f1c2b4d5e60@v7".to_string()),
            price_book_checksum: Some(
                "sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
                    .to_string(),
            ),
            price_catalog: Some(
                "sha256:2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae"
                    .to_string(),
            ),
            latency_ms: 812,
            attempts: 1,
        }
    }

    /// A sink whose batch write never returns, so the fan-out's bound is the
    /// only thing that ends the flush.
    struct StalledSink;

    #[async_trait]
    impl UsageSink for StalledSink {
        fn name(&self) -> &'static str {
            "stalled"
        }

        async fn record(&self, _record: &UsageRecord) {}

        async fn record_batch(&self, _batch: &[ObservedRecord]) -> Result<(), SinkFailure> {
            std::future::pending().await
        }
    }

    /// A billing-grade delivery over the in-memory contract oracle. The journal's
    /// own tests cover the storage; these cover the decision the request path
    /// makes about the answer it gets back.
    fn billing(capacity: journal::Capacity, on_undurable: UndurablePolicy) -> UsageDelivery {
        let journal = Arc::new(journal::oracle::InMemoryUsageJournal::with_capacity(
            capacity,
        ));
        UsageDelivery::billing(journal, on_undurable)
    }

    fn bounded(max_events: u64) -> journal::Capacity {
        journal::Capacity {
            max_events,
            ..journal::Capacity::BILLING_GRADE
        }
    }

    struct SlowIndexStore;

    #[async_trait]
    impl crate::store::Store for SlowIndexStore {
        async fn put_namespace(
            &self,
            _: crate::store::NamespaceRecord,
        ) -> Result<(), crate::store::StoreError> {
            Ok(())
        }
        async fn get_namespace(
            &self,
            _: &str,
        ) -> Result<Option<crate::store::NamespaceRecord>, crate::store::StoreError> {
            Ok(None)
        }
        async fn list_namespaces(
            &self,
            _: Option<String>,
            _: u32,
        ) -> Result<(Vec<crate::store::NamespaceRecord>, Option<String>), crate::store::StoreError>
        {
            Ok((Vec::new(), None))
        }
        async fn update_namespace(
            &self,
            _: &str,
            _: serde_json::Value,
            _: Option<Vec<String>>,
        ) -> Result<Option<crate::store::NamespaceRecord>, crate::store::StoreError> {
            Ok(None)
        }
        async fn delete_namespace(&self, _: &str) -> Result<bool, crate::store::StoreError> {
            Ok(false)
        }
        fn seed_namespaces_blocking(
            &self,
            _: &[crate::config::Namespace],
        ) -> Result<(), crate::store::StoreError> {
            Ok(())
        }
        async fn put_budget(
            &self,
            _: &str,
            _: &str,
            _: u64,
        ) -> Result<crate::store::BudgetRecord, crate::store::StoreError> {
            Err(crate::store::StoreError::Unavailable("unused".into()))
        }
        async fn get_budget(
            &self,
            _: &str,
            _: &str,
        ) -> Result<Option<crate::store::BudgetRecord>, crate::store::StoreError> {
            Ok(None)
        }
        async fn put_budget_policy(
            &self,
            _: &str,
            _: crate::store::BudgetCadence,
            _: u64,
            _: &str,
            _: Option<&str>,
        ) -> Result<crate::store::BudgetPolicy, crate::store::StoreError> {
            Err(crate::store::StoreError::Unavailable("unused".into()))
        }
        async fn get_budget_policy(
            &self,
            _: &str,
        ) -> Result<Option<crate::store::BudgetPolicy>, crate::store::StoreError> {
            Ok(None)
        }
        async fn admit_budget(
            &self,
            _: &str,
        ) -> Result<crate::store::BudgetAdmit, crate::store::StoreError> {
            Err(crate::store::StoreError::Unavailable("unused".into()))
        }
        async fn charge_budget(
            &self,
            _: &str,
            _: &str,
            _: i64,
            _: u64,
        ) -> Result<(), crate::store::StoreError> {
            Ok(())
        }
        async fn append_usage(
            &self,
            _: crate::store::UsageAppend,
        ) -> Result<(), crate::store::StoreError> {
            std::future::pending().await
        }
        async fn append_usage_batch(
            &self,
            _: Vec<crate::store::UsageAppend>,
        ) -> Result<(), crate::store::StoreError> {
            std::future::pending().await
        }
        fn append_usage_batch_sync(
            &self,
            _: &[crate::store::UsageAppend],
        ) -> Result<(), crate::store::StoreError> {
            Err(crate::store::StoreError::Unavailable(
                "this store has no synchronous usage-index path".into(),
            ))
        }
        async fn summarize_usage(
            &self,
            _: &str,
            _: &str,
        ) -> Result<Vec<crate::store::UsageSummaryRow>, crate::store::StoreError> {
            Ok(Vec::new())
        }
    }

    /// Blocking usage-index store whose first sync write waits until `release`
    /// is dropped, so the queue can be filled without the worker draining it.
    /// Records every batch it is handed, and fails them all when `fail` is set.
    struct ParkingIndexStore {
        entered: Arc<std::sync::Barrier>,
        release: std::sync::Mutex<Option<std::sync::mpsc::Receiver<()>>>,
        batches: std::sync::Mutex<Vec<usize>>,
        fail: bool,
    }

    impl ParkingIndexStore {
        fn parked(
            entered: Arc<std::sync::Barrier>,
            release: std::sync::mpsc::Receiver<()>,
        ) -> Self {
            Self {
                entered,
                release: std::sync::Mutex::new(Some(release)),
                batches: std::sync::Mutex::new(Vec::new()),
                fail: false,
            }
        }

        /// Never parks; every batch is refused.
        fn failing() -> Self {
            Self {
                entered: Arc::new(std::sync::Barrier::new(1)),
                release: std::sync::Mutex::new(None),
                batches: std::sync::Mutex::new(Vec::new()),
                fail: true,
            }
        }

        fn batches(&self) -> Vec<usize> {
            self.batches.lock().expect("batches").clone()
        }
    }

    #[async_trait]
    impl crate::store::Store for ParkingIndexStore {
        async fn put_namespace(
            &self,
            _: crate::store::NamespaceRecord,
        ) -> Result<(), crate::store::StoreError> {
            Ok(())
        }
        async fn get_namespace(
            &self,
            _: &str,
        ) -> Result<Option<crate::store::NamespaceRecord>, crate::store::StoreError> {
            Ok(None)
        }
        async fn list_namespaces(
            &self,
            _: Option<String>,
            _: u32,
        ) -> Result<(Vec<crate::store::NamespaceRecord>, Option<String>), crate::store::StoreError>
        {
            Ok((Vec::new(), None))
        }
        async fn update_namespace(
            &self,
            _: &str,
            _: serde_json::Value,
            _: Option<Vec<String>>,
        ) -> Result<Option<crate::store::NamespaceRecord>, crate::store::StoreError> {
            Ok(None)
        }
        async fn delete_namespace(&self, _: &str) -> Result<bool, crate::store::StoreError> {
            Ok(false)
        }
        fn seed_namespaces_blocking(
            &self,
            _: &[crate::config::Namespace],
        ) -> Result<(), crate::store::StoreError> {
            Ok(())
        }
        async fn put_budget(
            &self,
            _: &str,
            _: &str,
            _: u64,
        ) -> Result<crate::store::BudgetRecord, crate::store::StoreError> {
            Err(crate::store::StoreError::Unavailable("unused".into()))
        }
        async fn get_budget(
            &self,
            _: &str,
            _: &str,
        ) -> Result<Option<crate::store::BudgetRecord>, crate::store::StoreError> {
            Ok(None)
        }
        async fn put_budget_policy(
            &self,
            _: &str,
            _: crate::store::BudgetCadence,
            _: u64,
            _: &str,
            _: Option<&str>,
        ) -> Result<crate::store::BudgetPolicy, crate::store::StoreError> {
            Err(crate::store::StoreError::Unavailable("unused".into()))
        }
        async fn get_budget_policy(
            &self,
            _: &str,
        ) -> Result<Option<crate::store::BudgetPolicy>, crate::store::StoreError> {
            Ok(None)
        }
        async fn admit_budget(
            &self,
            _: &str,
        ) -> Result<crate::store::BudgetAdmit, crate::store::StoreError> {
            Err(crate::store::StoreError::Unavailable("unused".into()))
        }
        async fn charge_budget(
            &self,
            _: &str,
            _: &str,
            _: i64,
            _: u64,
        ) -> Result<(), crate::store::StoreError> {
            Ok(())
        }
        async fn append_usage(
            &self,
            _: crate::store::UsageAppend,
        ) -> Result<(), crate::store::StoreError> {
            Ok(())
        }
        async fn append_usage_batch(
            &self,
            events: Vec<crate::store::UsageAppend>,
        ) -> Result<(), crate::store::StoreError> {
            self.append_usage_batch_sync(&events)
        }
        fn blocking_usage_index(&self) -> bool {
            true
        }
        fn append_usage_sync(
            &self,
            event: crate::store::UsageAppend,
        ) -> Result<(), crate::store::StoreError> {
            self.append_usage_batch_sync(std::slice::from_ref(&event))
        }
        fn append_usage_batch_sync(
            &self,
            events: &[crate::store::UsageAppend],
        ) -> Result<(), crate::store::StoreError> {
            self.batches.lock().expect("batches").push(events.len());
            if let Some(rx) = self.release.lock().expect("release mutex").take() {
                self.entered.wait();
                let _ = rx.recv();
            }
            if self.fail {
                return Err(crate::store::StoreError::Unavailable("index down".into()));
            }
            Ok(())
        }
        async fn summarize_usage(
            &self,
            _: &str,
            _: &str,
        ) -> Result<Vec<crate::store::UsageSummaryRow>, crate::store::StoreError> {
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn telemetry_grade_delivery_cannot_refuse_a_request() {
        let delivery = UsageDelivery::telemetry(UsageFanout::new(vec![Box::new(StdoutSink)]));
        assert_eq!(delivery.mode(), DeliveryMode::TelemetryGrade);
        // The existing default: the record goes out best effort, and the request
        // path has nothing to decide.
        delivery
            .record(&sample_record())
            .await
            .expect("telemetry-grade delivery is infallible");
    }

    #[tokio::test]
    async fn a_store_index_append_does_not_delay_the_record_verdict() {
        let delivery = UsageDelivery::telemetry(UsageFanout::new(Vec::new()));
        delivery.attach_store(Arc::new(SlowIndexStore), UsageIndexSettings::default());
        let started = Instant::now();
        delivery
            .record(&sample_record())
            .await
            .expect("telemetry-grade delivery is infallible");
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "index writes must not stall the inference verdict"
        );
    }

    #[tokio::test]
    async fn a_journaled_append_does_not_wait_on_the_usage_index() {
        let delivery = billing(bounded(8), UndurablePolicy::Refuse);
        delivery.attach_store(Arc::new(SlowIndexStore), UsageIndexSettings::default());
        let started = Instant::now();
        delivery.record(&sample_record()).await.expect("append");
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "a successful journal append must not wait on the secondary index"
        );
    }

    #[tokio::test]
    async fn sqlite_usage_index_worker_lands_a_row_without_blocking_record() {
        let store = Arc::new(crate::store::SqliteStore::open(":memory:").expect("sqlite"));
        let delivery = UsageDelivery::telemetry(UsageFanout::new(Vec::new()));
        delivery.attach_store(
            Arc::clone(&store) as Arc<dyn crate::store::Store>,
            UsageIndexSettings::default(),
        );
        let record = sample_record();
        let started = Instant::now();
        delivery
            .record(&record)
            .await
            .expect("telemetry-grade delivery is infallible");
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "index writes must not stall the inference verdict"
        );
        let period = record.period.as_deref().expect("period");
        let mut rows = Vec::new();
        for _ in 0..50 {
            rows = store
                .summarize_usage(&record.namespace, period)
                .await
                .expect("summarize");
            if !rows.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            rows,
            vec![crate::store::UsageSummaryRow {
                model: record.model,
                status: record.status.as_str().to_owned(),
                count: 1,
                cost_microdollars: record.cost_microdollars.unwrap_or(0),
            }]
        );
    }

    /// Poll `probe` until it holds, or fail after a bounded wait. The index
    /// worker is asynchronous by design, so its effects are awaited, not assumed.
    async fn eventually(what: &str, mut probe: impl FnMut() -> bool) {
        for _ in 0..200 {
            if probe() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("{what} did not happen within the wait");
    }

    fn fast_index(capacity: usize, max_batch: usize) -> UsageIndexSettings {
        UsageIndexSettings {
            capacity,
            max_batch,
            flush_interval: Duration::from_millis(1),
            write_timeout: Duration::from_millis(50),
        }
    }

    /// A full queue drops on the request path without waiting, is counted as
    /// `saturated` exactly once per event (never as `timeout`, which nothing
    /// here did), and logs once rather than once per drop. Everything that did
    /// queue is written when the Store comes back, in batches no larger than
    /// `max_batch`, and nothing is written twice.
    #[tokio::test]
    async fn a_full_sqlite_index_queue_drops_without_blocking_record() {
        const CAPACITY: usize = 8;
        const OVERFLOW: usize = 5;
        let entered = Arc::new(std::sync::Barrier::new(2));
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let store = Arc::new(ParkingIndexStore::parked(Arc::clone(&entered), release_rx));
        let delivery = UsageDelivery::telemetry(UsageFanout::new(Vec::new()));
        delivery.attach_store(
            Arc::clone(&store) as Arc<dyn Store>,
            fast_index(CAPACITY, 4),
        );
        delivery
            .record(&sample_record())
            .await
            .expect("first event is dequeued");
        // The worker is now parked inside its first write with the queue empty.
        entered.wait();
        let started = Instant::now();
        for _ in 0..CAPACITY + OVERFLOW {
            delivery
                .record(&sample_record())
                .await
                .expect("queued or dropped, never awaited");
        }
        assert!(
            started.elapsed() < Duration::from_millis(200),
            "a full index queue must not stall the inference verdict"
        );
        assert_eq!(
            delivery.index_outcome(IndexOutcome::Saturated),
            OVERFLOW as u64,
            "every overflow event is counted exactly once as saturated"
        );
        assert_eq!(delivery.index_outcome(IndexOutcome::Timeout), 0);
        assert_eq!(delivery.index_outcome(IndexOutcome::Failed), 0);
        assert_eq!(
            delivery.index_log_lines().0,
            1,
            "a sustained overflow is one log line, not one per drop"
        );
        drop(release_tx);
        eventually("the queued events are written", || {
            delivery.index_outcome(IndexOutcome::Accepted) == (1 + CAPACITY) as u64
        })
        .await;
        let batches = store.batches();
        assert_eq!(batches.iter().sum::<usize>(), 1 + CAPACITY);
        assert!(batches.iter().all(|size| *size <= 4), "{batches:?}");
        assert!(
            batches.len() < 1 + CAPACITY,
            "the backlog was batched: {batches:?}"
        );
    }

    /// A Store outage costs index rows, not memory or latency: the queue never
    /// holds more than its capacity, no transaction is asked for more than
    /// `max_batch` rows, every event is accounted for exactly once as either
    /// `saturated` or `failed`, and the failing writes log at a bounded rate.
    #[tokio::test]
    async fn an_index_outage_keeps_the_queue_and_batches_bounded() {
        const CAPACITY: usize = 16;
        const MAX_BATCH: usize = 4;
        const SENT: u64 = 400;
        let store = Arc::new(ParkingIndexStore::failing());
        let delivery = UsageDelivery::telemetry(UsageFanout::new(Vec::new()));
        delivery.attach_store(
            Arc::clone(&store) as Arc<dyn Store>,
            fast_index(CAPACITY, MAX_BATCH),
        );
        for _ in 0..SENT {
            delivery
                .record(&sample_record())
                .await
                .expect("best effort");
        }
        eventually("every event is accounted for", || {
            delivery.index_outcome(IndexOutcome::Saturated)
                + delivery.index_outcome(IndexOutcome::Failed)
                == SENT
        })
        .await;
        assert_eq!(delivery.index_outcome(IndexOutcome::Accepted), 0);
        assert_eq!(delivery.index_outcome(IndexOutcome::Timeout), 0);
        let batches = store.batches();
        assert!(
            batches.iter().all(|size| (1..=MAX_BATCH).contains(size)),
            "{batches:?}"
        );
        assert_eq!(
            batches.iter().map(|size| *size as u64).sum::<u64>(),
            delivery.index_outcome(IndexOutcome::Failed)
        );
        let (writes, largest) = delivery.index_batches();
        assert_eq!(writes, batches.len() as u64);
        assert_eq!(largest, *batches.iter().max().expect("some writes") as u64);
        let (drop_lines, write_lines) = delivery.index_log_lines();
        assert!(drop_lines <= 1, "{drop_lines} overflow lines");
        assert_eq!(
            write_lines,
            1,
            "{} failing writes logged once",
            batches.len()
        );
    }

    /// The rate limiter's contract in isolation: the first occurrence logs, the
    /// rest inside the window are suppressed and counted, and the next line after
    /// the window carries the suppressed count so nothing is silently lost.
    #[test]
    fn a_rate_limited_log_emits_once_per_window_and_reports_what_it_skipped() {
        let log = RateLimitedLog::new();
        assert_eq!(log.should_emit(), Some(0));
        for _ in 0..7 {
            assert_eq!(log.should_emit(), None);
        }
        assert_eq!(log.suppressed.load(Ordering::Relaxed), 7);
        *log.last_emitted.lock().expect("lock") =
            Some(Instant::now() - RateLimitedLog::INTERVAL - Duration::from_millis(1));
        assert_eq!(log.should_emit(), Some(7));
        assert_eq!(log.should_emit(), None);
    }

    /// A worker that is gone is `closed`, not `failed`: nothing about the Store
    /// is known, and the request path found nobody to hand the event to.
    #[tokio::test]
    async fn an_index_worker_that_has_stopped_is_counted_as_closed() {
        let store = Arc::new(crate::store::SqliteStore::open(":memory:").expect("sqlite"));
        let delivery = UsageDelivery::telemetry(UsageFanout::new(Vec::new()));
        delivery.attach_store(store, fast_index(8, 8));
        delivery.stop_index_worker();
        // The worker only observes the flag when it dequeues, so the first event
        // is what wakes it into exiting; the ones after it find the queue closed.
        for _ in 0..200 {
            delivery
                .record(&sample_record())
                .await
                .expect("best effort");
            if delivery.index_outcome(IndexOutcome::Closed) > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(delivery.index_outcome(IndexOutcome::Closed) > 0);
        assert_eq!(delivery.index_outcome(IndexOutcome::Failed), 0);
        assert_eq!(delivery.index_outcome(IndexOutcome::Saturated), 0);
    }

    /// A delivery that never got a worker (no store attached is fine; a store
    /// attached and no queue is not) counts `closed` too.
    #[tokio::test]
    async fn a_store_without_a_worker_is_counted_as_closed_not_failed() {
        let delivery = UsageDelivery::telemetry(UsageFanout::new(Vec::new()));
        let _ = delivery.store.set(Arc::new(SlowIndexStore));
        delivery
            .record(&sample_record())
            .await
            .expect("best effort");
        assert_eq!(delivery.index_outcome(IndexOutcome::Closed), 1);
        assert_eq!(delivery.index_outcome(IndexOutcome::Failed), 0);
    }

    /// The async (Postgres-shaped) worker: a write that outlives its deadline is
    /// `timeout`, and the whole batch is counted, once, under that outcome.
    #[tokio::test]
    async fn an_index_write_past_its_deadline_is_counted_as_timeout() {
        let delivery = UsageDelivery::telemetry(UsageFanout::new(Vec::new()));
        delivery.attach_store(Arc::new(SlowIndexStore), fast_index(8, 8));
        for _ in 0..3 {
            delivery
                .record(&sample_record())
                .await
                .expect("best effort");
        }
        eventually("the batch times out", || {
            delivery.index_outcome(IndexOutcome::Timeout) == 3
        })
        .await;
        assert_eq!(delivery.index_outcome(IndexOutcome::Failed), 0);
        assert_eq!(delivery.index_outcome(IndexOutcome::Saturated), 0);
        assert_eq!(delivery.index_batches().0, 1, "one write for the batch");
    }

    /// The async worker classifies a Store refusal as `failed`.
    #[tokio::test]
    async fn an_index_write_the_store_refuses_is_counted_as_failed() {
        let delivery = UsageDelivery::telemetry(UsageFanout::new(Vec::new()));
        delivery.attach_store(Arc::new(crate::store::UnavailableStore), fast_index(8, 8));
        delivery
            .record(&sample_record())
            .await
            .expect("best effort");
        eventually("the write fails", || {
            delivery.index_outcome(IndexOutcome::Failed) == 1
        })
        .await;
        assert_eq!(delivery.index_outcome(IndexOutcome::Timeout), 0);
    }

    /// Bounds hold whatever the file said: a batch larger than the queue or the
    /// Store's ceiling is clamped, and a zero is one.
    #[test]
    fn index_settings_are_bounded_before_the_worker_sees_them() {
        let bounded = UsageIndexSettings {
            capacity: 0,
            max_batch: 0,
            ..UsageIndexSettings::default()
        }
        .bounded();
        assert_eq!((bounded.capacity, bounded.max_batch), (1, 1));
        let bounded = UsageIndexSettings {
            capacity: 100_000,
            max_batch: 50_000,
            ..UsageIndexSettings::default()
        }
        .bounded();
        assert_eq!(bounded.max_batch, crate::store::MAX_USAGE_INDEX_BATCH);
        let bounded = UsageIndexSettings {
            capacity: 8,
            max_batch: 64,
            ..UsageIndexSettings::default()
        }
        .bounded();
        assert_eq!(bounded.max_batch, 8);
    }

    /// Informational, not a gate: the before/after numbers for #468.
    ///
    /// Enqueues the same events through the index worker with `max_batch = 1`
    /// (the previous one-transaction-per-event behaviour) and with the default
    /// batch, against a file-backed SQLite store so each commit pays its fsync.
    /// The queue is sized to the run so neither mode drops and the comparison is
    /// of drain rate and transaction count; a second run at the previous queue
    /// bound (256) shows what the same burst costs in `saturated` drops.
    /// Throughout the drain a request-path Store read is probed for its worst
    /// latency, the "does batching starve inference" number.
    ///
    /// `cargo test -p axond --all-features -- --ignored --nocapture usage_index_bench`
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[ignore = "benchmark: run by hand with --ignored --nocapture"]
    async fn usage_index_bench() {
        const EVENTS: u64 = 4_000;
        let before = UsageIndexSettings {
            max_batch: 1,
            ..UsageIndexSettings::default()
        };
        let after = UsageIndexSettings::default();
        for (label, settings) in [
            (
                "before, unbounded run (max_batch = 1, capacity = 4000)",
                UsageIndexSettings {
                    capacity: EVENTS as usize,
                    ..before
                },
            ),
            (
                "after, unbounded run (max_batch = 256, capacity = 4000)",
                UsageIndexSettings {
                    capacity: EVENTS as usize,
                    ..after
                },
            ),
            (
                "before, previous queue (max_batch = 1, capacity = 256)",
                UsageIndexSettings {
                    capacity: 256,
                    ..before
                },
            ),
            (
                "after, previous queue (max_batch = 256, capacity = 256)",
                UsageIndexSettings {
                    capacity: 256,
                    ..after
                },
            ),
        ] {
            let path = std::env::temp_dir().join(format!(
                "axond-usage-index-bench-{}-{}.sqlite",
                std::process::id(),
                SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("clock")
                    .as_nanos()
            ));
            let store = Arc::new(
                crate::store::SqliteStore::open(path.to_str().expect("utf8")).expect("sqlite"),
            );
            store
                .put_namespace(crate::store::NamespaceRecord {
                    id: "acme".into(),
                    attrs: serde_json::json!({}),
                    blocklist: None,
                })
                .await
                .expect("namespace");
            let delivery = UsageDelivery::telemetry(UsageFanout::new(Vec::new()));
            delivery.attach_store(Arc::clone(&store) as Arc<dyn Store>, settings);
            let record = sample_record();
            let period = record.period.clone().expect("period");
            let started = Instant::now();
            for _ in 0..EVENTS {
                let event = UsageRecord {
                    request_id: identity::next_request_id().to_string(),
                    ..record.clone()
                };
                delivery.record(&event).await.expect("telemetry");
            }
            let enqueued = started.elapsed();
            let mut indexed = 0;
            let mut worst_read = Duration::ZERO;
            let mut reads = 0u64;
            let deadline = Instant::now() + Duration::from_secs(120);
            while Instant::now() < deadline {
                let at = Instant::now();
                store
                    .get_namespace("acme")
                    .await
                    .expect("read")
                    .expect("row");
                worst_read = worst_read.max(at.elapsed());
                reads += 1;
                indexed = store
                    .summarize_usage(&record.namespace, &period)
                    .await
                    .expect("summary")
                    .iter()
                    .map(|row| row.count)
                    .sum::<u64>();
                if indexed + delivery.index_outcome(IndexOutcome::Saturated) >= EVENTS {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            let elapsed = started.elapsed();
            let (batches, largest) = delivery.index_batches();
            println!(
                "{label}:\n  {indexed} indexed of {EVENTS} in {:.2}s ({:.0} records/s; enqueue \
                 took {:?})\n  {batches} store transactions, largest {largest} rows, {} \
                 saturated drops\n  worst request-path store read during the drain {:?} over \
                 {reads} probes",
                elapsed.as_secs_f64(),
                indexed as f64 / elapsed.as_secs_f64(),
                enqueued,
                delivery.index_outcome(IndexOutcome::Saturated),
                worst_read,
            );
            drop(delivery);
            let _ = std::fs::remove_file(&path);
            let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
            let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
        }
    }

    #[tokio::test]
    async fn a_journaled_event_is_durable_before_the_request_is_answered() {
        let delivery = billing(bounded(8), UndurablePolicy::Refuse);
        let record = sample_record();
        delivery.record(&record).await.expect("append");
        // The retry of an identical event is not a second charge and not an
        // error: the fact is durable exactly once.
        delivery
            .record(&record)
            .await
            .expect("an identical append is already durable");
    }

    #[tokio::test]
    async fn a_full_journal_refuses_the_request_rather_than_billing_for_nothing() {
        let delivery = billing(bounded(1), UndurablePolicy::Refuse);
        delivery.record(&sample_record()).await.expect("append");
        let error = delivery
            .record(&sample_record())
            .await
            .expect_err("a full journal cannot make the next event durable");
        assert_eq!(error.reason, "at_capacity");
        // The reason is what the operator alerts on, and the request id is what
        // ties the refusal to the request that was not recorded.
        assert!(error.to_string().contains(&error.request_id), "{error}");
    }

    #[tokio::test]
    async fn a_deployment_that_chose_to_serve_anyway_is_served_and_the_loss_counted() {
        let delivery = billing(bounded(1), UndurablePolicy::Serve);
        delivery.record(&sample_record()).await.expect("append");
        // Explicitly configured: availability over accounting, with the event
        // gone rather than silently deferred.
        delivery
            .record(&sample_record())
            .await
            .expect("`serve` does not refuse the request");
    }

    #[tokio::test]
    async fn a_terminal_record_that_cannot_be_journaled_does_not_unwind_the_response() {
        let delivery = billing(bounded(1), UndurablePolicy::Refuse);
        delivery.record(&sample_record()).await.expect("append");
        // A stream whose bytes were already relayed: there is no answer left to
        // refuse, so the failure can only be counted.
        delivery.record_terminal(&sample_record()).await;
    }

    #[tokio::test]
    async fn a_write_through_sink_has_nothing_to_flush() {
        let fanout = UsageFanout::new(vec![Box::new(StdoutSink)]);
        let report = fanout.flush(Duration::from_secs(5)).await;
        assert!(report.is_complete());
        assert_eq!(
            report.sinks,
            vec![("stdout", FlushOutcome::Flushed { records: 0 })]
        );
    }

    #[tokio::test]
    async fn a_stalled_sink_flush_ends_at_the_bound_with_its_buffer_accounted() {
        let batched = BatchedSink::spawn(
            Arc::new(StalledSink),
            BatchSettings {
                capacity: 16,
                max_batch: 1,
                flush_interval: Duration::from_millis(5),
            },
        );
        let fanout = UsageFanout::new(vec![Box::new(batched)]);
        for _ in 0..4 {
            fanout.record(&sample_record()).await;
        }
        // Give the flush task time to pick up the first record and stall on it.
        tokio::time::sleep(Duration::from_millis(20)).await;
        let report = fanout.flush(Duration::from_millis(50)).await;
        assert!(
            !report.is_complete(),
            "a stalled sink cannot report success"
        );
        let (sink, outcome) = &report.sinks[0];
        assert_eq!(*sink, "stalled");
        assert!(
            matches!(outcome, FlushOutcome::TimedOut { abandoned } if *abandoned > 0),
            "{outcome:?}"
        );
    }

    #[tokio::test]
    async fn no_configured_sink_keeps_the_stdout_default() {
        let sinks = build_sinks(&[], &HashMap::new(), Buffering::Batched)
            .await
            .expect("defaults");
        assert_eq!(sinks.len(), 1);
        assert_eq!(sinks[0].name(), "stdout");
    }

    #[tokio::test]
    async fn a_postgres_sink_whose_dsn_env_is_unset_fails_at_boot() {
        let config = UsageSinkConfig {
            kind: UsageSinkKind::Postgres,
            dsn_env: Some("AXOND_TEST_MISSING_DSN".to_string()),
            ..UsageSinkConfig::default()
        };
        let err = build_sinks(&[config], &HashMap::new(), Buffering::Batched)
            .await
            .err()
            .expect("missing dsn must fail at boot");
        assert!(matches!(err, UsageSinkError::Invalid { .. }), "{err:?}");
    }

    /// Enabling the journal moves buffering into the outbox, so the sink's own
    /// batching keys stop applying. A deployment that set them is told which ones,
    /// because the alternative is settings that silently mean nothing.
    #[test]
    fn a_journal_names_the_sink_batching_keys_it_takes_over() {
        let tuned = UsageSinkConfig {
            kind: UsageSinkKind::Postgres,
            buffer_capacity: 42,
            flush_interval_ms: 250,
            ..UsageSinkConfig::default()
        };
        assert_eq!(
            journal_owned_batch_keys(&[tuned]),
            vec!["buffer_capacity", "flush_interval_ms"]
        );
        // Untouched defaults are not worth a warning, and a sink that never
        // batched has nothing to hand over.
        assert!(
            journal_owned_batch_keys(&[
                UsageSinkConfig {
                    kind: UsageSinkKind::Postgres,
                    ..UsageSinkConfig::default()
                },
                UsageSinkConfig {
                    kind: UsageSinkKind::Stdout,
                    buffer_capacity: 7,
                    ..UsageSinkConfig::default()
                },
            ])
            .is_empty()
        );
    }

    /// The other half of that contract: a write-through sink is the destination
    /// itself, so the worker's acknowledgement speaks for a row the destination
    /// actually accepted rather than for a queue slot.
    #[tokio::test]
    async fn write_through_sinks_are_not_wrapped_in_a_queue() {
        let sinks = build_sinks(&[], &HashMap::new(), Buffering::WriteThrough)
            .await
            .expect("defaults");
        let report = UsageFanout::new(sinks).flush(Duration::from_secs(5)).await;
        assert_eq!(
            report.sinks,
            vec![("stdout", FlushOutcome::Flushed { records: 0 })],
            "a write-through sink has no buffer to flush"
        );
    }
}
