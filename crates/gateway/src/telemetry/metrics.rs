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
use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter, UpDownCounter};
use opentelemetry::{KeyValue, global};

use crate::usage::UsageRecord;

struct Instruments {
    http_requests: Counter<u64>,
    http_duration: Histogram<f64>,
    requests: Counter<u64>,
    request_duration: Histogram<f64>,
    ttft: Histogram<f64>,
    input_tokens: Counter<u64>,
    cache_read_tokens: Counter<u64>,
    cache_write_tokens: Counter<u64>,
    output_tokens: Counter<u64>,
    cost: Counter<u64>,
    upstream_errors: Counter<u64>,
    upstream_timeouts: Counter<u64>,
    circuit_state: Gauge<u64>,
    usage_written: Counter<u64>,
    usage_dropped: Counter<u64>,
    usage_flushes: Counter<u64>,
    journal_appends: Counter<u64>,
    journal_deliveries: Counter<u64>,
    journal_quarantined: Counter<u64>,
    journal_undeliverable: Counter<u64>,
    journal_lost: Counter<u64>,
    journal_depth: Gauge<u64>,
    journal_in_flight: Gauge<u64>,
    journal_quarantine_depth: Gauge<u64>,
    journal_oldest_pending: Gauge<u64>,
    journal_capacity: Gauge<u64>,
    shutdown_phase: Gauge<u64>,
    shutdown_rejections: Counter<u64>,
    shutdown_abandoned: Counter<u64>,
    config_reloads: Counter<u64>,
    config_generation: Gauge<u64>,
    revision_attempts: Counter<u64>,
    revision_rejections: Counter<u64>,
    revision_lag: Gauge<u64>,
    revision_converged: Gauge<u64>,
    revision_desired_at: Gauge<u64>,
    revision_active_at: Gauge<u64>,
    revision_convergence: Histogram<f64>,
    revision_failures: Gauge<u64>,
    last_known_good: Counter<u64>,
    budget_capacity_denials: Counter<u64>,
    budget_namespace_denials: Counter<u64>,
    budget_retained_subjects: Gauge<u64>,
    admission_in_flight: UpDownCounter<i64>,
    admission_rejections: Counter<u64>,
    rate_limit_denials: Counter<u64>,
    rate_limit_capacity_denials: Counter<u64>,
    rate_limit_unavailable_denials: Counter<u64>,
    revocation_denials: Counter<u64>,
    revocation_unavailable_denials: Counter<u64>,
    status_component_state: Gauge<u64>,
    status_observation_age: Gauge<u64>,
    status_refreshes: Counter<u64>,
    // Recorded by whoever schedules catalogue refresh, which `serve` does not
    // construct yet — the same reason `crate::backends` is contract only.
    #[allow(dead_code)]
    catalog_refusals: Counter<u64>,
    #[allow(dead_code)]
    catalog_active_age: Gauge<u64>,
    #[allow(dead_code)]
    catalog_consecutive_refusals: Gauge<u64>,
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
                .with_description("Non-cached prompt tokens billed at the regular input rate.")
                .build(),
            cache_read_tokens: meter
                .u64_counter("axond.tokens.cache_read")
                .with_description("Prompt tokens read from the provider cache.")
                .build(),
            cache_write_tokens: meter
                .u64_counter("axond.tokens.cache_write")
                .with_description("Prompt tokens written to the provider cache.")
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
            upstream_timeouts: meter
                .u64_counter("axond.upstream.timeouts")
                .with_description(
                    "Upstream attempts that exceeded a transport bound, by target and phase.",
                )
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
            usage_flushes: meter
                .u64_counter("axond.usage.flushes")
                .with_description("Shutdown flushes of a buffered usage sink, by sink and outcome.")
                .build(),
            journal_appends: meter
                .u64_counter("axond.usage.journal.appends")
                .with_description(
                    "Billing-grade usage appends, by journal and outcome. Anything other than \
                     `accepted` or `already_present` means the request was not journaled.",
                )
                .build(),
            journal_deliveries: meter
                .u64_counter("axond.usage.journal.deliveries")
                .with_description(
                    "Journaled usage events handed to their destinations, by consumer and \
                     outcome. `redelivered` is expected: delivery is at-least-once.",
                )
                .build(),
            journal_quarantined: meter
                .u64_counter("axond.usage.journal.quarantined")
                .with_description(
                    "Usage events set aside as poison, by consumer and reason. Each one needs an \
                     operator.",
                )
                .build(),
            journal_undeliverable: meter
                .u64_counter("axond.usage.journal.undeliverable")
                .with_description(
                    "Journaled events this build declined to deliver, by reason: written by a \
                     newer schema, or unreadable.",
                )
                .build(),
            journal_lost: meter
                .u64_counter("axond.usage.journal.lost")
                .with_description(
                    "Usage events a billing-grade deployment gave up, by reason. Every increment \
                     is a missing billable fact and should be alerted on.",
                )
                .build(),
            journal_depth: meter
                .u64_gauge("axond.usage.journal.depth")
                .with_description("Journaled usage events awaiting delivery, by consumer.")
                .build(),
            journal_in_flight: meter
                .u64_gauge("axond.usage.journal.in_flight")
                .with_description("Journaled usage events under an unexpired lease, by consumer.")
                .build(),
            journal_quarantine_depth: meter
                .u64_gauge("axond.usage.journal.quarantined_events")
                .with_description("Quarantined usage events still retained, by consumer.")
                .build(),
            journal_oldest_pending: meter
                .u64_gauge("axond.usage.journal.oldest_pending_age")
                .with_unit("s")
                .with_description(
                    "Age of the oldest undelivered journaled event. The delivery lag to alert on.",
                )
                .build(),
            journal_capacity: meter
                .u64_gauge("axond.usage.journal.capacity")
                .with_description(
                    "The journal's event bound, so depth can be read as a fraction of it.",
                )
                .build(),
            shutdown_phase: meter
                .u64_gauge("axond.shutdown.phase")
                .with_description(
                    "Lifecycle phase of this replica: 0 serving, 1 draining, 2 admission closed.",
                )
                .build(),
            shutdown_rejections: meter
                .u64_counter("axond.shutdown.rejected_requests")
                .with_description("Requests refused because admission was closed for shutdown.")
                .build(),
            shutdown_abandoned: meter
                .u64_counter("axond.shutdown.abandoned_requests")
                .with_description(
                    "Requests still in flight when the shutdown deadline expired, and dropped.",
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
            revision_attempts: meter
                .u64_counter("axond.revision.attempts")
                .with_description("Stateful convergence attempts, by trigger and outcome (#142).")
                .build(),
            revision_rejections: meter
                .u64_counter("axond.revision.rejections")
                .with_description(
                    "Desired revisions not applied, by reason; the active revision keeps serving.",
                )
                .build(),
            revision_lag: meter
                .u64_gauge("axond.revision.lag")
                .with_unit("ms")
                .with_description(
                    "How long this replica's active revision has differed from the desired one.",
                )
                .build(),
            revision_converged: meter
                .u64_gauge("axond.revision.converged")
                .with_description(
                    "1 when the active revision equals the desired revision, 0 otherwise.",
                )
                .build(),
            // Revision ids are UUIDv7, so their embedded millisecond timestamp is
            // the one numeric projection a gauge can carry. It identifies a
            // revision across replicas (publication order is time order) without
            // pretending an id is a counter.
            revision_desired_at: meter
                .u64_gauge("axond.revision.desired_at")
                .with_unit("ms")
                .with_description(
                    "Publication timestamp embedded in the desired revision's identifier.",
                )
                .build(),
            revision_active_at: meter
                .u64_gauge("axond.revision.active_at")
                .with_unit("ms")
                .with_description(
                    "Publication timestamp embedded in the active revision's identifier.",
                )
                .build(),
            revision_convergence: meter
                .f64_histogram("axond.revision.convergence_duration")
                .with_unit("ms")
                .with_description(
                    "Time from observing a desired revision to publishing its snapshot.",
                )
                .build(),
            revision_failures: meter
                .u64_gauge("axond.revision.consecutive_failures")
                .with_description(
                    "Consecutive failed convergence attempts, which set the backoff delay.",
                )
                .build(),
            last_known_good: meter
                .u64_counter("axond.revision.last_known_good")
                .with_description(
                    "Signed last-known-good cache operations, by outcome (exported, \
                     export_failed, restored).",
                )
                .build(),
            budget_capacity_denials: meter
                .u64_counter("axond.budget.capacity_denials")
                .with_description(
                    "In-memory budget admissions denied because the ledger bound was exhausted.",
                )
                .build(),
            budget_namespace_denials: meter
                .u64_counter("axond.budget.namespace_denials")
                .with_description(
                    "Budget admissions denied by the namespace-wide cap rather than the subject's.",
                )
                .build(),
            budget_retained_subjects: meter
                .u64_gauge("axond.budget.retained_subjects")
                .with_description(
                    "In-memory budget ledgers retained after capacity-pressure pruning.",
                )
                .build(),
            admission_in_flight: meter
                .i64_up_down_counter("axond.admission.in_flight")
                .with_description(
                    "Admission capacity held right now, by resource: requests, open streams, \
                     tenant slots, and queued requests.",
                )
                .build(),
            admission_rejections: meter
                .u64_counter("axond.admission.rejections")
                .with_description("Requests shed by admission control, by resource and error type.")
                .build(),
            rate_limit_denials: meter
                .u64_counter("axond.rate_limit.denials")
                .with_description("Inbound concurrency admissions denied.")
                .build(),
            rate_limit_capacity_denials: meter
                .u64_counter("axond.rate_limit.capacity_denials")
                .with_description("Inbound rate-limit admissions denied by subject-map capacity.")
                .build(),
            rate_limit_unavailable_denials: meter
                .u64_counter("axond.rate_limit.unavailable_denials")
                .with_description("Rate-limit admissions denied because the store was unavailable.")
                .build(),
            revocation_denials: meter
                .u64_counter("axond.revocation.denials")
                .with_description("Minted tokens denied because their JTI was revoked.")
                .build(),
            revocation_unavailable_denials: meter
                .u64_counter("axond.revocation.unavailable_denials")
                .with_description("Tokens denied because the revocation store was unavailable.")
                .build(),
            status_component_state: meter
                .u64_gauge("axond.status.component_state")
                .with_description(
                    "Last observed dependency state, by component: 0 disabled, 1 ok, \
                     2 degraded, 3 unavailable.",
                )
                .build(),
            status_observation_age: meter
                .u64_gauge("axond.status.observation_age")
                .with_unit("ms")
                .with_description("Age of the cached observation behind each component's state.")
                .build(),
            status_refreshes: meter
                .u64_counter("axond.status.refreshes")
                .with_description("Background status refresh attempts, by component and outcome.")
                .build(),
            catalog_refusals: meter
                .u64_counter("axond.catalog.refusals")
                .with_description(
                    "Catalogue imports refused, by typed reason. The previously imported \
                     catalogue stays active, so this is the only signal a refusal produces.",
                )
                .build(),
            catalog_active_age: meter
                .u64_gauge("axond.catalog.active_age")
                .with_unit("ms")
                .with_description(
                    "Age of the active catalogue: how long since its content was last \
                     admitted or confirmed unchanged.",
                )
                .build(),
            catalog_consecutive_refusals: meter
                .u64_gauge("axond.catalog.consecutive_refusals")
                .with_description(
                    "Catalogue imports refused in a row, reset by any admitted or \
                     confirmed-unchanged import.",
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
        .cache_read_tokens
        .add(record.cache_read_tokens, &attributes);
    instruments
        .cache_write_tokens
        .add(record.cache_write_tokens, &attributes);
    instruments
        .output_tokens
        .add(record.output_tokens, &attributes);
    instruments.cost.add(record.cost_microdollars, &attributes);
    if record.status.is_error() {
        instruments.upstream_errors.add(1, &attributes);
    }
}

/// One upstream attempt that exceeded a transport bound. `phase` is what was
/// waiting ([`gateway_transport::TimeoutKind::label`]) and `bound` whether the
/// phase's own bound or what was left of the failover budget ended the wait
/// ([`gateway_transport::TimeoutBound::label`]) — together, what separates "the
/// provider is slow" from "our own failover budget ran out".
pub fn record_upstream_timeout(
    target_provider: &str,
    target_model: &str,
    phase: &'static str,
    bound: &'static str,
) {
    let Some(instruments) = INSTRUMENTS.get() else {
        return;
    };
    instruments.upstream_timeouts.add(
        1,
        &[
            KeyValue::new("axond.target.provider", target_provider.to_owned()),
            KeyValue::new("axond.target.model", target_model.to_owned()),
            KeyValue::new("axond.timeout", phase),
            KeyValue::new("axond.timeout.bound", bound),
        ],
    );
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

/// Publish the outcome of one sink's shutdown flush. Paired with
/// `axond.usage.records_dropped`, which carries the count that did not land.
pub fn record_usage_flush(sink: &'static str, outcome: &'static str) {
    let Some(instruments) = INSTRUMENTS.get() else {
        return;
    };
    instruments.usage_flushes.add(
        1,
        &[
            KeyValue::new("axond.usage_sink", sink),
            KeyValue::new("axond.flush_outcome", outcome),
        ],
    );
}

/// One billing-grade append's outcome. `outcome` is the bounded vocabulary in
/// [`crate::usage::UsageDelivery`]: `accepted` and `already_present` are the only
/// two under which the event is durable.
pub fn record_usage_journal_append(journal: &'static str, outcome: &'static str) {
    let Some(instruments) = INSTRUMENTS.get() else {
        return;
    };
    instruments.journal_appends.add(
        1,
        &[
            KeyValue::new("axond.usage_journal", journal),
            KeyValue::new("axond.journal.outcome", outcome),
        ],
    );
}

/// Deliveries of journaled events. `redelivered` counts attempts after the
/// first, which at-least-once delivery makes normal rather than exceptional.
pub fn record_usage_journal_deliveries(
    journal: &'static str,
    consumer: &str,
    outcome: &'static str,
    count: u64,
) {
    let Some(instruments) = INSTRUMENTS.get() else {
        return;
    };
    if count == 0 {
        return;
    }
    instruments.journal_deliveries.add(
        count,
        &[
            KeyValue::new("axond.usage_journal", journal),
            KeyValue::new("axond.usage_journal.consumer", consumer.to_owned()),
            KeyValue::new("axond.journal.delivery", outcome),
        ],
    );
}

/// An event taken out of the delivery path for an operator to decide about.
pub fn record_usage_journal_quarantined(
    journal: &'static str,
    consumer: &str,
    reason: &'static str,
) {
    let Some(instruments) = INSTRUMENTS.get() else {
        return;
    };
    instruments.journal_quarantined.add(
        1,
        &[
            KeyValue::new("axond.usage_journal", journal),
            KeyValue::new("axond.usage_journal.consumer", consumer.to_owned()),
            KeyValue::new("axond.journal.poison_reason", reason),
        ],
    );
}

/// An event this build left alone: written by a newer schema, or unreadable.
/// Distinct from a loss, because the event is still journaled.
pub fn record_usage_journal_undeliverable(journal: &'static str, reason: &'static str) {
    let Some(instruments) = INSTRUMENTS.get() else {
        return;
    };
    instruments.journal_undeliverable.add(
        1,
        &[
            KeyValue::new("axond.usage_journal", journal),
            KeyValue::new("axond.journal.reason", reason),
        ],
    );
}

/// Billable facts a billing-grade deployment gave up. The one usage metric that
/// should never move.
pub fn record_usage_journal_lost(journal: &'static str, reason: &'static str, count: u64) {
    let Some(instruments) = INSTRUMENTS.get() else {
        return;
    };
    instruments.journal_lost.add(
        count,
        &[
            KeyValue::new("axond.usage_journal", journal),
            KeyValue::new("axond.journal.loss_reason", reason),
        ],
    );
}

/// Publish one observation of a journal's backlog. Gauges rather than counters:
/// what an operator watches is how far behind delivery is right now.
pub fn record_usage_journal_stats(
    journal: &'static str,
    consumer: &str,
    stats: &crate::usage::journal::JournalStats,
) {
    let Some(instruments) = INSTRUMENTS.get() else {
        return;
    };
    let labels = [
        KeyValue::new("axond.usage_journal", journal),
        KeyValue::new("axond.usage_journal.consumer", consumer.to_owned()),
    ];
    instruments.journal_depth.record(stats.pending, &labels);
    instruments
        .journal_in_flight
        .record(stats.in_flight, &labels);
    instruments
        .journal_quarantine_depth
        .record(stats.quarantined, &labels);
    instruments.journal_oldest_pending.record(
        stats
            .oldest_pending_age
            .map(|age| age.as_secs())
            .unwrap_or_default(),
        &labels,
    );
    instruments.journal_capacity.record(
        stats.capacity.max_events,
        &[KeyValue::new("axond.usage_journal", journal)],
    );
}

/// Publish the lifecycle phase this replica has reached. A gauge rather than a
/// counter: what an operator watching a rollout needs is "is this replica still
/// taking work", and the readiness probe alone cannot distinguish a draining
/// replica from an unhealthy one.
pub fn record_shutdown_phase(phase: crate::shutdown::Phase) {
    let Some(instruments) = INSTRUMENTS.get() else {
        return;
    };
    let value = match phase {
        crate::shutdown::Phase::Serving => 0,
        crate::shutdown::Phase::Draining => 1,
        crate::shutdown::Phase::Closing => 2,
    };
    instruments.shutdown_phase.record(
        value,
        &[KeyValue::new("axond.lifecycle_phase", phase.as_str())],
    );
}

/// A request refused because admission was already closed. Distinct from a
/// dependency `503`: nothing is wrong with the replica, it is leaving.
pub fn record_shutdown_rejection() {
    let Some(instruments) = INSTRUMENTS.get() else {
        return;
    };
    instruments.shutdown_rejections.add(1, &[]);
}

/// Requests dropped because they were still in flight at the shutdown deadline.
/// The documented accounting for a long stream cut short: each one settles as
/// `client_cancelled` in the usage record.
pub fn record_shutdown_abandoned(count: u64) {
    let Some(instruments) = INSTRUMENTS.get() else {
        return;
    };
    if count > 0 {
        instruments.shutdown_abandoned.add(count, &[]);
    }
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

/// Record one convergence attempt and everything the replica now reports about
/// its revisions.
///
/// Emitted from one place so `lag`, `converged`, and the revision gauges cannot
/// disagree with the attempt that produced them — the failure mode of recording
/// them separately is a dashboard that shows a converged replica with rising lag.
pub fn record_revision_attempt(
    trigger: &'static str,
    outcome: &'static str,
    report: &crate::convergence::RevisionReport,
) {
    let Some(instruments) = INSTRUMENTS.get() else {
        return;
    };
    instruments.revision_attempts.add(
        1,
        &[
            KeyValue::new("axond.revision.trigger", trigger),
            KeyValue::new("axond.revision.outcome", outcome),
        ],
    );
    instruments.revision_lag.record(
        u64::try_from(report.lag.as_millis()).unwrap_or(u64::MAX),
        &[],
    );
    instruments
        .revision_converged
        .record(u64::from(report.converged()), &[]);
    instruments
        .revision_failures
        .record(u64::from(report.consecutive_failures), &[]);
    if let Some(desired) = report.desired {
        instruments
            .revision_desired_at
            .record(desired.uuid().timestamp_millis(), &[]);
    }
    if let Some(active) = report.active {
        instruments
            .revision_active_at
            .record(active.uuid().timestamp_millis(), &[]);
    }
    if let Some(took) = report.last_convergence.filter(|_| outcome == "published") {
        instruments
            .revision_convergence
            .record(took.as_secs_f64() * 1_000.0, &[]);
    }
    instruments.config_generation.record(report.generation, &[]);
}

/// Count a desired revision that was not applied, by the stage that refused it.
pub fn record_revision_rejection(reason: &'static str) {
    let Some(instruments) = INSTRUMENTS.get() else {
        return;
    };
    instruments
        .revision_rejections
        .add(1, &[KeyValue::new("axond.revision.reason", reason)]);
}

/// Count a last-known-good cache operation. `export_failed` is a warning rather
/// than an outage; `restored` means a replica booted from cached state and may be
/// serving something older than desired.
pub fn record_last_known_good(outcome: &'static str) {
    let Some(instruments) = INSTRUMENTS.get() else {
        return;
    };
    instruments
        .last_known_good
        .add(1, &[KeyValue::new("axond.revision.outcome", outcome)]);
}

/// Record an in-memory budget admission denied by the subject bound.
pub fn record_budget_capacity_denial() {
    let Some(instruments) = INSTRUMENTS.get() else {
        return;
    };
    instruments.budget_capacity_denials.add(1, &[]);
}

/// Record an admission denied by the namespace-wide spend cap rather than by
/// the subject's own. Both answer `429`, so this is how an operator tells a
/// tenant-wide exhaustion from one noisy key.
pub fn record_budget_namespace_denial() {
    let Some(instruments) = INSTRUMENTS.get() else {
        return;
    };
    instruments.budget_namespace_denials.add(1, &[]);
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

/// Admission capacity taken. `resource` is the closed vocabulary in
/// [`crate::admission`], so saturation is observable without a tenant, subject,
/// or request dimension — the gauge's cardinality is fixed at build time.
pub fn record_admission_acquired(resource: &'static str) {
    let Some(instruments) = INSTRUMENTS.get() else {
        return;
    };
    instruments
        .admission_in_flight
        .add(1, &[KeyValue::new("axond.admission.resource", resource)]);
}

/// Admission capacity returned. Called from the permit's `Drop`, so it pairs
/// with [`record_admission_acquired`] on every exit path.
pub fn record_admission_released(resource: &'static str) {
    let Some(instruments) = INSTRUMENTS.get() else {
        return;
    };
    instruments
        .admission_in_flight
        .add(-1, &[KeyValue::new("axond.admission.resource", resource)]);
}

/// One request shed by admission control. `code` is the same stable error type
/// the caller was answered with, so a dashboard and a caller's logs agree.
pub fn record_admission_rejection(resource: &'static str, code: &'static str) {
    let Some(instruments) = INSTRUMENTS.get() else {
        return;
    };
    instruments.admission_rejections.add(
        1,
        &[
            KeyValue::new("axond.admission.resource", resource),
            KeyValue::new("axond.error.type", code),
        ],
    );
}

pub fn record_rate_limit_denial() {
    let Some(instruments) = INSTRUMENTS.get() else {
        return;
    };
    instruments.rate_limit_denials.add(1, &[]);
}

pub fn record_rate_limit_capacity_denial() {
    let Some(instruments) = INSTRUMENTS.get() else {
        return;
    };
    instruments.rate_limit_capacity_denials.add(1, &[]);
}

pub fn record_rate_limit_unavailable_denial() {
    let Some(instruments) = INSTRUMENTS.get() else {
        return;
    };
    instruments.rate_limit_unavailable_denials.add(1, &[]);
}

pub fn record_revocation_denial() {
    let Some(instruments) = INSTRUMENTS.get() else {
        return;
    };
    instruments.revocation_denials.add(1, &[]);
}

pub fn record_revocation_unavailable_denial() {
    let Some(instruments) = INSTRUMENTS.get() else {
        return;
    };
    instruments.revocation_unavailable_denials.add(1, &[]);
}

/// Publish one component's cached status observation.
///
/// Component-scoped and nothing else: the status registry observes
/// deployment-wide dependencies, so a namespace or subject dimension here would
/// be both unbounded and a leak of the tenancy the redacted status response is
/// careful not to carry.
pub fn record_status_component(
    component: &'static str,
    state: crate::status::ComponentState,
    age: std::time::Duration,
) {
    let Some(instruments) = INSTRUMENTS.get() else {
        return;
    };
    let attributes = [KeyValue::new("axond.status.component", component)];
    instruments
        .status_component_state
        .record(state.gauge_value(), &attributes);
    instruments.status_observation_age.record(
        u64::try_from(age.as_millis()).unwrap_or(u64::MAX),
        &attributes,
    );
}

/// Count one background refresh attempt.
pub fn record_status_refresh(component: &'static str, outcome: &'static str) {
    let Some(instruments) = INSTRUMENTS.get() else {
        return;
    };
    instruments.status_refreshes.add(
        1,
        &[
            KeyValue::new("axond.status.component", component),
            KeyValue::new("axond.status.outcome", outcome),
        ],
    );
}

/// Count one refused catalogue import, by its bounded reason.
///
/// The reason and nothing else: the pointer, source URL, and error text the
/// refusal also carries are unbounded over one upstream document, and belong to
/// the log line the import wrote.
///
/// Every refusal has one to record: a failed refresh carries it on its error, and
/// a refresh that refused without an error carries it in
/// [`Refreshed::Refused`](crate::backends::catalog::Refreshed::Refused), so a
/// caller counting only its error branch would let the run climb without naming a
/// reason.
#[allow(dead_code)]
pub fn record_catalog_refusal(reason: crate::backends::catalog::RefusalReason) {
    let Some(instruments) = INSTRUMENTS.get() else {
        return;
    };
    instruments
        .catalog_refusals
        .add(1, &[KeyValue::new("axond.catalog.reason", reason.as_str())]);
}

/// Publish what is operationally true about the catalogue right now.
///
/// Recorded from a [`CatalogReport`](crate::backends::catalog::CatalogReport) so
/// the gauges, the alert, and the status response cannot disagree. A deployment
/// that has never imported reports no age rather than a zero one: an age of zero
/// reads as "just refreshed", which is the opposite of the truth.
#[allow(dead_code)]
pub fn record_catalog_state(report: &crate::backends::catalog::CatalogReport) {
    let Some(instruments) = INSTRUMENTS.get() else {
        return;
    };
    if let Some(age) = report.active_age() {
        instruments
            .catalog_active_age
            .record(u64::try_from(age.as_millis()).unwrap_or(u64::MAX), &[]);
    }
    instruments
        .catalog_consecutive_refusals
        .record(u64::from(report.consecutive_refusals), &[]);
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
