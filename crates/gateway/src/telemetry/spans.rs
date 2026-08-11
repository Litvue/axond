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

/// The trace the current request belongs to, when one is being recorded. Usage
/// records carry it so a row joins the caller's trace; it is deliberately not
/// the row's identity, since a caller's whole agent loop shares one trace.
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
pub const LEASE_SERVED: &str = "served";
pub const LEASE_RATE_LIMITED: &str = "rate_limited";
pub const LEASE_ERROR: &str = "error";
pub const LEASE_PARKED: &str = "parked";

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

pub fn credential_lease_span(
    credential_id: &str,
    credential_source: &'static str,
    index: usize,
) -> Span {
    tracing::info_span!(
        target: "axond.upstream",
        "axond.credential.lease",
        axond.credential.id = credential_id,
        axond.credential_source = credential_source,
        axond.credential.index = index,
        axond.status = Empty,
    )
}

pub fn finish_credential_lease(span: &Span, status: &'static str) {
    span.record("axond.status", status);
}

/// Outcome recorded on an applied config reload.
pub const RELOAD_APPLIED: &str = "applied";
/// Outcome recorded on a rejected candidate config.
pub const RELOAD_REJECTED: &str = "rejected";

/// A span covering one reload attempt: validating the candidate and, when it
/// passes, publishing it. Rooted rather than nested — a reload is triggered by a
/// signal or a watcher, never by a request.
pub fn config_reload_span(trigger: &'static str) -> Span {
    tracing::info_span!(
        target: "axond.config",
        "axond.config.reload",
        axond.reload.trigger = trigger,
        axond.reload.outcome = Empty,
        axond.config.generation = Empty,
    )
}

/// Close out a reload span with its outcome and the generation now serving,
/// and count the attempt.
pub fn finish_config_reload(
    span: &Span,
    trigger: &'static str,
    outcome: &'static str,
    generation: u64,
) {
    span.record("axond.reload.outcome", outcome);
    span.record("axond.config.generation", generation);
    metrics::record_config_reload(trigger, outcome, generation);
}

/// Fold the canonical usage record into the active server span and the
/// dimensioned metrics. Called once per terminated request, from the same place
/// the usage record is emitted, so spans, metrics, and sinks cannot disagree.
pub fn record_request(record: &UsageRecord, ttft_ms: Option<u64>, attempts: u32) {
    let span = Span::current();
    // The record's own id, not the trace id: a trace covers every request an
    // agent loop makes, so reusing it would collapse distinct usage rows.
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
    span.record("gen_ai.usage.cache_read_tokens", record.cache_read_tokens);
    span.record("gen_ai.usage.cache_write_tokens", record.cache_write_tokens);
    span.record("gen_ai.usage.output_tokens", record.output_tokens);
    span.record("axond.cost_microdollars", record.cost_microdollars);
    span.record("axond.latency_ms", record.latency_ms);
    if let Some(ttft_ms) = ttft_ms {
        span.record("axond.ttft_ms", ttft_ms);
    }
    metrics::record_request(record, ttft_ms);
}

/// Record what a request resolved to, before it is dispatched. A streamed
/// request outlives its server span, so this is the only chance the span gets to
/// say where the stream went; the buffered path fills the same fields from the
/// usage record instead.
pub fn record_routing(
    namespace: &str,
    subject: &str,
    alias: &str,
    target_provider: &str,
    target_model: &str,
    credential_source: &'static str,
) {
    let span = Span::current();
    span.record("axond.namespace", namespace);
    span.record("axond.subject", subject);
    span.record("gen_ai.request.model", alias);
    span.record("axond.target.provider", target_provider);
    span.record("axond.target.model", target_model);
    span.record("axond.credential_source", credential_source);
}

/// The same fold for a streamed request. Streams settle from the response body,
/// which outlives the handler and therefore the server span, so there is no span
/// left to record onto — the metrics and the record's `trace_id` carry it.
pub fn record_streamed(record: &UsageRecord, ttft_ms: Option<u64>) {
    metrics::record_request(record, ttft_ms);
}
