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
use std::sync::Arc;
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
pub use journal::{ConsumerId, DrainReport};
pub use otlp::OtlpUsageSink;
pub use postgres::{PostgresSink, PostgresSinkSettings, tls_connector, validate_table_name};

/// The terminal outcome of a request. Every terminated request produces
/// exactly one record — including failures, cancellations, and partial
/// streams — so spend reconciles (delta B6).
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
    pub cost_microdollars: u64,
    pub catalog_version: u64,
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
        }
    }

    /// Billing-grade: the record is durable before this returns, or the request
    /// is told it was not.
    pub fn billing(journal: Arc<dyn UsageJournal>, on_undurable: UndurablePolicy) -> Self {
        Self {
            fanout: UsageFanout::new(Vec::new()),
            journal: Some(journal),
            on_undurable,
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
    /// than flushed here.
    pub async fn flush(&self, budget: Duration) -> FlushReport {
        self.fanout.flush(budget).await
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
    if durable.iter().all(|sink| sink.kind == UsageSinkKind::Stdout) {
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

    /// A record with every field filled, for sink tests.
    pub(super) fn sample_record() -> UsageRecord {
        UsageRecord {
            schema_version: UsageRecord::SCHEMA_VERSION,
            request_id: identity::next_request_id().to_string(),
            trace_id: Some("4bf92f3577b34da6a3ce929d0e0e4736".to_string()),
            namespace: "acme".to_string(),
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
            cost_microdollars: 640,
            catalog_version: 0,
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
