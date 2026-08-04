//! Usage collection — the write path.
//!
//! `UsageSink` is the pluggable destination trait (delta B7/§5.2). Records are
//! built **once** at the end of the request pipeline from `gateway-core`'s
//! `UsageReceipt` and fanned out to every configured sink. Sinks are off the
//! request path: they must be async and are expected to buffer/batch.
//!
//! This scaffold ships `StdoutSink` (the zero-dependency, no-datastore
//! default). Postgres, Tinybird, ClickHouse, and OTLP sinks are follow-ups
//! that implement this same trait.

use async_trait::async_trait;
use serde::Serialize;

use crate::credentials::CredentialSource;

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
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cost_microdollars: u64,
    pub catalog_version: u64,
    pub latency_ms: u64,
}

impl UsageRecord {
    pub const SCHEMA_VERSION: u32 = 1;

    pub fn credential_source_str(source: CredentialSource) -> &'static str {
        match source {
            CredentialSource::Platform => "platform",
            CredentialSource::Byok => "byok",
        }
    }
}

#[async_trait]
pub trait UsageSink: Send + Sync {
    #[allow(dead_code)] // surfaced in logs/metrics once telemetry lands
    fn name(&self) -> &'static str;
    async fn record(&self, record: &UsageRecord);
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

/// Fan-out over the configured sinks. A bounded queue + batched flush +
/// overflow counter (delta B6) will wrap this; the scaffold calls sinks
/// directly.
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
