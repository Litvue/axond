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

mod batch;
mod otlp;
mod postgres;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use serde::Serialize;

use crate::config::{UsageSinkConfig, UsageSinkKind};
use crate::credentials::CredentialSource;

pub use batch::{BatchSettings, BatchedSink};
pub use otlp::OtlpUsageSink;
pub use postgres::{PostgresSink, PostgresSinkSettings, tls_connector, validate_table_name};

/// The terminal outcome of a request. Every terminated request produces
/// exactly one record — including failures, cancellations, and partial
/// streams — so spend reconciles (delta B6).
#[derive(Debug, Clone, Copy, Serialize)]
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
#[derive(Debug, Clone, Serialize)]
pub struct UsageRecord {
    pub schema_version: u32,
    /// Unique per request, so rows can be de-duplicated. Distinct from
    /// `trace_id`, which one caller trace shares across many requests.
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

/// Build the configured sinks, or the stdout default when none are declared.
///
/// Connecting and (optionally) creating the table happens here so a
/// misconfigured datastore refuses to boot instead of silently dropping every
/// record at request time.
pub async fn build_sinks(
    configs: &[UsageSinkConfig],
    env: &HashMap<String, String>,
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
                sinks.push(Box::new(BatchedSink::spawn(
                    Arc::new(sink),
                    config.batch_settings(),
                )));
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
            request_id: "req_0000000000000001".to_string(),
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

    #[tokio::test]
    async fn no_configured_sink_keeps_the_stdout_default() {
        let sinks = build_sinks(&[], &HashMap::new()).await.expect("defaults");
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
        let err = build_sinks(&[config], &HashMap::new())
            .await
            .err()
            .expect("missing dsn must fail at boot");
        assert!(matches!(err, UsageSinkError::Invalid { .. }), "{err:?}");
    }
}
