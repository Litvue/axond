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
    usage_written: Counter<u64>,
    usage_dropped: Counter<u64>,
    config_reloads: Counter<u64>,
    config_generation: Gauge<u64>,
    budget_capacity_denials: Counter<u64>,
    budget_retained_subjects: Gauge<u64>,
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
            usage_written: meter
                .u64_counter("axond.usage.records_written")
                .with_description("Usage records a sink accepted, by sink.")
                .build(),
            usage_dropped: meter
                .u64_counter("axond.usage.records_dropped")
                .with_description(
                    "Usage records discarded rather than delaying requests, by sink and reason.",
                )
                .build(),
            config_reloads: meter
                .u64_counter("axond.config.reloads")
                .with_description("Config reload attempts, by trigger and outcome.")
                .build(),
            config_generation: meter
                .u64_gauge("axond.config.generation")
                .with_description(
                    "Config generation this replica is serving: 0 at boot, +1 per applied reload.",
                )
                .build(),
            budget_capacity_denials: meter
                .u64_counter("axond.budget.capacity_denials")
                .with_description(
                    "In-memory budget admissions denied because the ledger bound was exhausted.",
                )
                .build(),
            budget_retained_subjects: meter
                .u64_gauge("axond.budget.retained_subjects")
                .with_description(
                    "In-memory budget ledgers retained after capacity-pressure pruning.",
                )
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

/// Usage records a sink durably accepted. Counted by the batching fan-out, so
/// it reflects acknowledged writes rather than enqueues.
pub fn record_usage_written(sink: &'static str, count: u64) {
    let Some(instruments) = INSTRUMENTS.get() else {
        return;
    };
    instruments
        .usage_written
        .add(count, &[KeyValue::new("axond.usage_sink", sink)]);
}

/// Usage records the gateway gave up on. `reason` is the bounded vocabulary of
/// [`crate::usage::DropReason`], so the durability contract is measurable:
/// requests are never stalled, and what that costs is visible here.
pub fn record_usage_dropped(sink: &'static str, reason: &'static str, count: u64) {
    let Some(instruments) = INSTRUMENTS.get() else {
        return;
    };
    instruments.usage_dropped.add(
        count,
        &[
            KeyValue::new("axond.usage_sink", sink),
            KeyValue::new("axond.drop_reason", reason),
        ],
    );
}

/// Publish a reload attempt and the generation now serving. A rejected
/// candidate still reports the generation, so the pair says both "a reload was
/// tried" and "this is what is actually running" (ADR 0011).
pub fn record_config_reload(trigger: &'static str, outcome: &'static str, generation: u64) {
    let Some(instruments) = INSTRUMENTS.get() else {
        return;
    };
    instruments.config_reloads.add(
        1,
        &[
            KeyValue::new("axond.reload.trigger", trigger),
            KeyValue::new("axond.reload.outcome", outcome),
        ],
    );
    instruments.config_generation.record(generation, &[]);
}

/// Record an in-memory budget admission denied by the subject bound.
pub fn record_budget_capacity_denial() {
    let Some(instruments) = INSTRUMENTS.get() else {
        return;
    };
    instruments.budget_capacity_denials.add(1, &[]);
}

/// Record the retained in-memory ledger count after capacity-pressure pruning.
pub fn record_budget_retained_subjects(subjects: usize) {
    let Some(instruments) = INSTRUMENTS.get() else {
        return;
    };
    instruments
        .budget_retained_subjects
        .record(subjects as u64, &[]);
}

/// Publish a target's circuit state. Ordered failover (which owns the breaker)
/// calls this on every transition; the gauge exists here so the metric set is
/// defined in one place. Dimensioned by the full target (provider + model),
/// matching the breaker's per-target key.
pub fn record_circuit_state(target_provider: &str, target_model: &str, state: CircuitState) {
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
        &[
            KeyValue::new("axond.target.provider", target_provider.to_owned()),
            KeyValue::new("axond.target.model", target_model.to_owned()),
        ],
    );
}
