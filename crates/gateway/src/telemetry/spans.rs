//! Span shape.
//!
//! One server span per request (created by the middleware) with one child span
//! per upstream attempt. The server span holds what the caller asked for and
//! how it ended; each attempt span holds where it was sent. That split is what
//! makes ordered failover legible: N attempt spans under one server span, the
//! last one carrying the status the caller saw.

use opentelemetry::trace::TraceContextExt;
use tracing::Span;
use tracing::field::Empty;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::telemetry::metrics;
use crate::usage::UsageRecord;

/// The active trace id, when a trace is being recorded. Requests reuse it as
/// their request id so a usage row, a log line, and a span all point at the
/// same identifier.
pub fn trace_id() -> Option<String> {
    if !super::is_exporting() {
        return None;
    }
    let context = Span::current().context();
    let span_context = context.span().span_context().clone();
    span_context
        .is_valid()
        .then(|| span_context.trace_id().to_string())
}

/// Status recorded on a successful upstream attempt.
pub const ATTEMPT_OK: &str = "ok";
/// Status recorded on a failed upstream attempt.
pub const ATTEMPT_ERROR: &str = "error";

/// A child span covering one dispatch to one target. `attempt` is zero-based so
/// the retry count is the highest attempt index observed.
pub fn upstream_attempt_span(
    attempt: u32,
    target_provider: &str,
    target_model: &str,
    credential_source: &'static str,
) -> Span {
    tracing::info_span!(
        target: "axond.upstream",
        "axond.upstream.attempt",
        axond.attempt = attempt,
        axond.target.provider = target_provider,
        axond.target.model = target_model,
        axond.credential_source = credential_source,
        axond.status = Empty,
        axond.latency_ms = Empty,
        axond.ttft_ms = Empty,
    )
}

/// Close out an attempt span with its outcome. `ttft_ms` is the time to the
/// first token: for a non-streamed response the whole body arrives at once, so
/// it equals the attempt latency; the streaming relay reports the real first
/// chunk.
pub fn finish_upstream_attempt(
    span: &Span,
    status: &'static str,
    latency_ms: u64,
    ttft_ms: Option<u64>,
) {
    span.record("axond.status", status);
    span.record("axond.latency_ms", latency_ms);
    if let Some(ttft_ms) = ttft_ms {
        span.record("axond.ttft_ms", ttft_ms);
    }
}

/// Fold the canonical usage record into the active server span and the
/// dimensioned metrics. Called once per terminated request, from the same place
/// the usage record is emitted, so spans, metrics, and sinks cannot disagree.
pub fn record_request(record: &UsageRecord, ttft_ms: Option<u64>, attempts: u32) {
    let span = Span::current();
    span.record("axond.request_id", record.request_id.as_str());
    span.record("axond.namespace", record.namespace.as_str());
    span.record("axond.subject", record.subject.as_str());
    span.record("gen_ai.request.model", record.model.as_str());
    span.record("axond.target.provider", record.target_provider.as_str());
    span.record("axond.target.model", record.target_model.as_str());
    span.record("axond.credential_source", record.credential_source);
    span.record("axond.status", record.status.as_str());
    span.record("axond.retry_count", attempts.saturating_sub(1));
    span.record("gen_ai.usage.input_tokens", record.input_tokens);
    span.record("gen_ai.usage.output_tokens", record.output_tokens);
    span.record("axond.cost_microdollars", record.cost_microdollars);
    span.record("axond.latency_ms", record.latency_ms);
    if let Some(ttft_ms) = ttft_ms {
        span.record("axond.ttft_ms", ttft_ms);
    }
    metrics::record_request(record, ttft_ms);
}
