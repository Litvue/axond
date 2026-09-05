//! Span shape.
//!
//! One server span per request (created by the middleware) with one child span
//! per upstream attempt. The server span holds what the caller asked for and
//! how it ended; each attempt span holds where it was sent. That split is what
//! makes ordered failover legible: N attempt spans under one server span, the
//! last one carrying the status the caller saw.

use axum::http::HeaderMap;
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry::trace::TraceContextExt;
use opentelemetry_http::HeaderExtractor;
use opentelemetry_sdk::propagation::TraceContextPropagator;
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

/// The validated correlation trace for one inbound request.
///
/// When export is active the server span is authoritative: it contains the
/// context selected by the configured propagator and gives every child attempt
/// the same trace. With export disabled there is deliberately no tracer or
/// global propagator on the request path, but a usage record still retains a
/// valid caller-supplied W3C trace id. This keeps correlation useful without
/// enabling an exporter and without treating the trace id as the billing event
/// identity.
pub fn request_trace_id(headers: &HeaderMap) -> Option<String> {
    trace_id().or_else(|| inbound_trace_id(headers))
}

fn inbound_trace_id(headers: &HeaderMap) -> Option<String> {
    // Trace Context version 00 has one canonical four-field representation.
    // The SDK extractor deliberately accepts future-version extensions, but
    // this fallback runs without the configured request propagator and must
    // not turn a partially understood header into durable usage correlation.
    let traceparent = headers.get("traceparent")?.to_str().ok()?;
    let mut fields = traceparent.split('-');
    let version = fields.next()?;
    let trace_id = fields.next()?;
    let parent_id = fields.next()?;
    let flags = fields.next()?;
    if fields.next().is_some()
        || version != "00"
        || trace_id.len() != 32
        || parent_id.len() != 16
        || flags.len() != 2
    {
        return None;
    }

    let propagator = TraceContextPropagator::new();
    let context = propagator.extract(&HeaderExtractor(headers));
    let span_context = context.span().span_context().clone();
    (span_context.is_valid() && span_context.is_remote())
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
        axond.upstream.status = Empty,
        axond.upstream.message = Empty,
        axond.latency_ms = Empty,
        axond.ttft_ms = Empty,
        axond.timeout = Empty,
        axond.timeout.bound = Empty,
    )
}

/// Which transport bound an attempt exceeded, as stable low-cardinality labels:
/// `phase` is what was waiting, `bound` whether its own bound or the walk's
/// remaining budget ended the wait. Recorded alongside the attempt's `error`
/// status rather than replacing it, so "failed" queries keep working and "timed
/// out how" becomes answerable.
pub fn record_attempt_timeout(
    span: &Span,
    target_provider: &str,
    target_model: &str,
    phase: &'static str,
    bound: &'static str,
) {
    span.record("axond.timeout", phase);
    span.record("axond.timeout.bound", bound);
    super::metrics::record_upstream_timeout(target_provider, target_model, phase, bound);
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
    if status == ATTEMPT_ERROR {
        span.set_status(opentelemetry::trace::Status::error(
            "upstream attempt failed",
        ));
    }
    span.record("axond.latency_ms", latency_ms);
    if let Some(ttft_ms) = ttft_ms {
        span.record("axond.ttft_ms", ttft_ms);
    }
}

/// Bounded provider diagnostics on the attempt that failed. HTTP status is
/// carried by the transport, not guessed from the gateway's error class.
pub fn record_attempt_failure(span: &Span, error: &gateway_transport::TransportError) {
    use gateway_core::ProviderError;
    if let gateway_transport::TransportError::Upstream { status, .. } = error {
        span.record("axond.upstream.status", u64::from(*status));
    }
    let message = match error.provider_error() {
        Some(ProviderError::ModelUnavailable(failures) | ProviderError::Dependency(failures)) => {
            failures
                .first()
                .map(|failure| failure.message.clone())
                .unwrap_or_else(|| error.to_string())
        }
        Some(
            ProviderError::InvalidRequest(message)
            | ProviderError::ContextWindowExceeded(message)
            | ProviderError::Unsupported(message)
            | ProviderError::InvalidStream(message)
            | ProviderError::RateLimitedStream(message),
        ) => message.clone(),
        _ => crate::error::transport_caller_message(error),
    };
    // Apply the core bound even to errors constructed locally rather than by
    // the HTTP parser. Truncate on a UTF-8 boundary before exporting.
    let mut end = message.len().min(gateway_core::MAX_DIAGNOSTIC_BYTES);
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    span.record("axond.upstream.message", &message[..end]);
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

/// Convergence triggered by the poll interval — the correctness path.
pub const CONVERGENCE_POLLED: &str = "polled";
/// Convergence triggered by a control-plane notification — the latency
/// optimization. A deployment without notifications never records it.
pub const CONVERGENCE_NOTIFIED: &str = "notified";
/// The first convergence attempt of a stateful process.
pub const CONVERGENCE_BOOT: &str = "boot";
/// Convergence triggered by an effective-dated pricing boundary.
pub const CONVERGENCE_PRICING_BOUNDARY: &str = "pricing-boundary";

/// A span covering one convergence attempt: reading desired state and, when it
/// differs, hydrating, compiling, and publishing it.
///
/// Rooted like a reload span, and for the same reason: convergence is driven by a
/// timer or a notification, never by a request.
pub fn revision_convergence_span(trigger: &'static str) -> Span {
    tracing::info_span!(
        target: "axond.revision",
        "axond.revision.converge",
        axond.revision.trigger = trigger,
        axond.revision.outcome = Empty,
        axond.revision.desired = Empty,
        axond.revision.active = Empty,
        axond.revision.lag_ms = Empty,
        axond.revision.reason = Empty,
        axond.config.generation = Empty,
    )
}

/// Close out a convergence span with the outcome and what the replica now
/// reports, and count the attempt.
///
/// Both the desired and the active revision are recorded on every attempt,
/// including converged ones: a span that only carried "active" could not
/// distinguish a replica that is up to date from one whose control plane has
/// moved on.
pub fn finish_revision_convergence(
    span: &Span,
    trigger: &'static str,
    outcome: &crate::convergence::Outcome,
    report: &crate::convergence::RevisionReport,
) {
    span.record("axond.revision.outcome", outcome.as_str());
    if let Some(desired) = report.desired {
        span.record("axond.revision.desired", tracing::field::display(desired));
    }
    if let Some(active) = report.active {
        span.record("axond.revision.active", tracing::field::display(active));
    }
    span.record(
        "axond.revision.lag_ms",
        u64::try_from(report.lag.as_millis()).unwrap_or(u64::MAX),
    );
    if let Some(rejection) = &report.last_rejection {
        span.record("axond.revision.reason", rejection.reason);
    }
    span.record("axond.config.generation", report.generation);
    metrics::record_revision_attempt(trigger, outcome.as_str(), report);
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
    if let Some(cost) = record.cost_microdollars {
        span.record("axond.cost_microdollars", cost);
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    const TRACE_ID: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
    const SPAN_ID: &str = "00f067aa0ba902b7";

    fn headers(traceparent: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            "traceparent",
            traceparent.parse().expect("valid header bytes"),
        );
        headers
    }

    #[test]
    fn a_valid_remote_w3c_trace_is_retained_without_an_exporter() {
        assert_eq!(
            inbound_trace_id(&headers(&format!("00-{TRACE_ID}-{SPAN_ID}-01"))).as_deref(),
            Some(TRACE_ID)
        );
    }

    #[test]
    fn malformed_or_zero_w3c_context_is_not_usage_correlation() {
        for traceparent in [
            "not-a-traceparent",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
            "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra",
        ] {
            assert_eq!(
                inbound_trace_id(&headers(traceparent)),
                None,
                "{traceparent}"
            );
        }
        assert_eq!(inbound_trace_id(&HeaderMap::new()), None);
    }
}
