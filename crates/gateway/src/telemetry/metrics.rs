//! Metric instruments.
//!
//! Instruments are built once, at telemetry init, and stashed in a `OnceLock`.
//! When telemetry is disabled the lock is never filled, so every recording
//! helper returns before it builds a single attribute — that is what keeps the
//! request path free of exporter work in the default posture.
//!
//! Two families, deliberately: `axond.http.*` covers *every* HTTP request
//! (including ones that never reach a provider, like `unknown_model`) with
//! low-cardinality route/status dimensions, while `axond.request.*` and
//! `axond.upstream.*` carry the gateway's own dimensions — namespace, alias,
//! target, credential source, status — and are emitted from the single
//! canonical usage record per request.

use std::sync::OnceLock;

use gateway_core::CircuitState;
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter};
use opentelemetry::{KeyValue, global};

use crate::usage::UsageRecord;

struct Instruments {
    http_requests: Counter<u64>,
    http_duration: Histogram<f64>,
    requests: Counter<u64>,
    request_duration: Histogram<f64>,
    ttft: Histogram<f64>,
    input_tokens: Counter<u64>,
    output_tokens: Counter<u64>,
    cost: Counter<u64>,
    upstream_errors: Counter<u64>,
    circuit_state: Gauge<u64>,
}

static INSTRUMENTS: OnceLock<Instruments> = OnceLock::new();

/// Build the instruments against the installed global meter provider. Called
/// only when OTLP export is enabled.
pub(super) fn init() {
    let _ = INSTRUMENTS.set(Instruments::build(&global::meter(super::SERVICE_NAME)));
}

impl Instruments {
    fn build(meter: &Meter) -> Self {
        Self {
            http_requests: meter
                .u64_counter("axond.http.server.requests")
                .with_description("HTTP requests served, by route and response status.")
                .build(),
            http_duration: meter
                .f64_histogram("axond.http.server.duration")
                .with_unit("ms")
                .with_description("Wall-clock duration of served HTTP requests.")
                .build(),
            requests: meter
                .u64_counter("axond.request.count")
                .with_description("Gateway requests that resolved to a provider target.")
                .build(),
            request_duration: meter
                .f64_histogram("axond.request.duration")
                .with_unit("ms")
                .with_description("End-to-end gateway request duration.")
                .build(),
            ttft: meter
                .f64_histogram("axond.request.time_to_first_token")
                .with_unit("ms")
                .with_description("Time from dispatch to the first token of the response.")
                .build(),
            input_tokens: meter
                .u64_counter("axond.tokens.input")
                .with_description("Prompt tokens billed upstream.")
                .build(),
            output_tokens: meter
                .u64_counter("axond.tokens.output")
                .with_description("Completion tokens billed upstream.")
                .build(),
            cost: meter
                .u64_counter("axond.cost.microdollars")
                .with_unit("uUSD")
                .with_description("Request cost in micro-dollars, priced from the target catalog.")
                .build(),
            upstream_errors: meter
                .u64_counter("axond.upstream.errors")
                .with_description("Upstream attempts that failed, by target.")
                .build(),
            circuit_state: meter
                .u64_gauge("axond.upstream.circuit_state")
                .with_description("Per-target circuit state: 0 closed, 1 half-open, 2 open.")
                .build(),
        }
    }
}

/// Coarse per-request HTTP metrics from the middleware.
pub(super) fn record_http(method: &str, route: &str, status: u16, duration_ms: f64) {
    let Some(instruments) = INSTRUMENTS.get() else {
        return;
    };
    let attributes = [
        KeyValue::new("http.request.method", method.to_owned()),
        KeyValue::new("http.route", route.to_owned()),
        KeyValue::new("http.response.status_code", i64::from(status)),
    ];
    instruments.http_requests.add(1, &attributes);
    instruments.http_duration.record(duration_ms, &attributes);
}

/// Dimensioned metrics derived from the canonical usage record. `ttft_ms` is
/// `None` when the response produced no token (a failed attempt).
pub(super) fn record_request(record: &UsageRecord, ttft_ms: Option<u64>) {
    let Some(instruments) = INSTRUMENTS.get() else {
        return;
    };
    let attributes = [
        KeyValue::new("axond.namespace", record.namespace.clone()),
        KeyValue::new("gen_ai.request.model", record.model.clone()),
        KeyValue::new("axond.target.provider", record.target_provider.clone()),
        KeyValue::new("axond.target.model", record.target_model.clone()),
        KeyValue::new("axond.credential_source", record.credential_source),
        KeyValue::new("axond.status", record.status.as_str()),
    ];
    instruments.requests.add(1, &attributes);
    instruments
        .request_duration
        .record(record.latency_ms as f64, &attributes);
    if let Some(ttft_ms) = ttft_ms {
        instruments.ttft.record(ttft_ms as f64, &attributes);
    }
    instruments
        .input_tokens
        .add(record.input_tokens, &attributes);
    instruments
        .output_tokens
        .add(record.output_tokens, &attributes);
    instruments.cost.add(record.cost_microdollars, &attributes);
    if record.status.is_error() {
        instruments.upstream_errors.add(1, &attributes);
    }
}

/// Publish a target's circuit state. Ordered failover (which owns the breaker)
/// calls this on every transition; the gauge exists here so the metric set is
/// defined in one place.
#[allow(dead_code)]
pub fn record_circuit_state(target_provider: &str, state: CircuitState) {
    let Some(instruments) = INSTRUMENTS.get() else {
        return;
    };
    let value = match state {
        CircuitState::Closed => 0,
        CircuitState::HalfOpen => 1,
        CircuitState::Open => 2,
    };
    instruments.circuit_state.record(
        value,
        &[KeyValue::new(
            "axond.target.provider",
            target_provider.to_owned(),
        )],
    );
}
