//! The canonical metric catalogue: every instrument axond may export, the
//! labels each one carries, and the rules that keep those labels bounded.
//!
//! Dashboards, alert rules, and the runbook all reference metrics by name, and
//! the failure mode is silent: a renamed instrument leaves a panel that reads
//! zero and an alert that never fires. So the names and label keys live here as
//! data, [`metrics`](super::metrics) is checked against this list, and
//! [`validate_reference`] is what an asset (or a documentation table) is
//! validated with before it ships.
//!
//! The second rule this file exists to enforce is cardinality. A metric label is
//! multiplied across every series a backend keeps, so the identity dimensions
//! that are safe on a *usage record* — subject, credential id, request id, the
//! revision a replica is serving — are the ones that turn a metric backend into
//! an outage. They are refused by [`validate_label_key`] outright, by key, and
//! the labels whose cardinality follows a deployment's own configuration
//! ([`LabelClass::Configured`]) are refused as *default* labels, where nobody
//! chose them per instrument: see [`validate_default_label_key`].

use std::collections::BTreeSet;

/// The instrument type a metric is recorded through. Part of the contract
/// because an alert that assumes a counter (`rate()`) reads a gauge as noise.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InstrumentKind {
    Counter,
    UpDownCounter,
    Gauge,
    Histogram,
}

impl InstrumentKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Counter => "counter",
            Self::UpDownCounter => "up_down_counter",
            Self::Gauge => "gauge",
            Self::Histogram => "histogram",
        }
    }
}

/// What bounds a label's cardinality.
///
/// The distinction is the whole point of the catalogue: `Closed`, `Numeric`, and
/// `Route` labels have a ceiling fixed by the code, while `Configured` labels
/// grow with what an operator declares. Both kinds are legitimate — a request
/// counter without a namespace cannot answer "which tenant" — but only the
/// former may be applied by default, because a default label is one nobody chose
/// for a specific instrument.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LabelClass {
    /// A closed vocabulary enumerated in [`Label::values`].
    Closed,
    /// A numeric domain fixed outside axond, such as an HTTP status code.
    Numeric,
    /// One of the registered routes. Bounded by the route table rather than
    /// enumerated here, so the table stays the single source of truth.
    Route,
    /// Cardinality follows the deployment's own configuration: namespaces,
    /// aliases, providers, target models.
    Configured,
}

impl LabelClass {
    /// Whether a label of this class may be attached to every metric by default.
    const fn is_default_safe(self) -> bool {
        !matches!(self, Self::Configured)
    }
}

/// One label key on one instrument, with the vocabulary it is allowed to carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Label {
    pub key: &'static str,
    pub class: LabelClass,
    /// The complete vocabulary for a [`LabelClass::Closed`] label, and empty for
    /// every other class.
    pub values: &'static [&'static str],
}

impl Label {
    const fn closed(key: &'static str, values: &'static [&'static str]) -> Self {
        Self {
            key,
            class: LabelClass::Closed,
            values,
        }
    }

    const fn open(key: &'static str, class: LabelClass) -> Self {
        Self {
            key,
            class,
            values: &[],
        }
    }
}

/// One catalogued instrument.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricSpec {
    pub name: &'static str,
    pub kind: InstrumentKind,
    /// The unit as exported, when the instrument declares one.
    pub unit: Option<&'static str>,
    pub labels: &'static [Label],
}

impl MetricSpec {
    pub fn label(&self, key: &str) -> Option<&'static Label> {
        self.labels.iter().find(|label| label.key == key)
    }
}

/// Resource attributes the portable Prometheus assets may use in addition to
/// an instrument's own labels. The collector turns the OTLP
/// `service.instance.id` attribute into `service_instance_id`; it is a
/// deployment-scoped identity for finding one replica, never a tenant/model
/// dimension.
const RESOURCE_LABELS: &[Label] = &[Label::open("service.instance.id", LabelClass::Configured)];

pub fn resource_label(key: &str) -> Option<&'static Label> {
    RESOURCE_LABELS.iter().find(|label| label.key == key)
}

/// Why a metric name, label key, or asset reference was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CatalogError {
    #[error("metric `{name}` is not in the canonical catalogue")]
    UnknownMetric { name: String },
    #[error("metric name `{name}` is malformed: {reason}")]
    MalformedName { name: String, reason: &'static str },
    #[error("label key `{key}` is malformed: {reason}")]
    MalformedLabel { key: String, reason: &'static str },
    #[error("label `{key}` must never be a metric dimension: {reason}")]
    ForbiddenLabel { key: String, reason: &'static str },
    #[error(
        "label `{key}` is `{class}`, so its cardinality follows the deployment's configuration and it cannot be a default label"
    )]
    UnboundedDefaultLabel { key: String, class: &'static str },
    #[error("metric `{metric}` does not declare the label `{key}`")]
    UndeclaredLabel { metric: String, key: String },
    #[error("label `{key}` on metric `{metric}` does not accept the value `{value}`")]
    UnknownLabelValue {
        metric: String,
        key: String,
        value: String,
    },
    #[error("metric `{name}` is catalogued twice")]
    DuplicateMetric { name: String },
    #[error("closed label `{key}` on metric `{metric}` enumerates no values")]
    EmptyVocabulary { metric: String, key: String },
    #[error("label `{key}` on metric `{metric}` is `{class}`, which enumerates no values")]
    UnexpectedVocabulary {
        metric: String,
        key: String,
        class: &'static str,
    },
    #[error("metric `{metric}` declares the label `{key}` twice")]
    DuplicateLabel { metric: String, key: String },
}

/// The label keys no metric may ever carry, and why. Each entry is a caller,
/// credential, request, or revision identity: unbounded in the dimension that
/// matters, and in most cases attributable to one tenant.
///
/// These identities are not lost — they are on the usage record and on spans,
/// which are per-event and sampled rather than multiplied into a stored series.
pub const FORBIDDEN_LABEL_KEYS: &[(&str, &str)] = &[
    ("tenant", "a tenant dimension is per-customer and unbounded"),
    (
        "axond.tenant",
        "a tenant dimension is per-customer and unbounded",
    ),
    (
        "axond.subject",
        "the authenticated subject is per-caller and unbounded",
    ),
    (
        "subject",
        "the authenticated subject is per-caller and unbounded",
    ),
    (
        "axond.credential.id",
        "a credential label identifies one pooled provider key",
    ),
    (
        "credential_id",
        "a credential label identifies one pooled provider key",
    ),
    (
        "axond.credential.index",
        "the rotation index belongs to one credential pool",
    ),
    ("axond.alias", "an alias dimension is per-tenant routing"),
    ("alias", "an alias dimension is per-tenant routing"),
    (
        "axond.model.id",
        "a model id belongs on a declared dimension, never as a bare identity label",
    ),
    (
        "model_id",
        "a model id belongs on a declared dimension, never as a bare identity label",
    ),
    (
        "axond.revision.id",
        "a revision id is unbounded over a deployment's lifetime",
    ),
    (
        "revision_id",
        "a revision id is unbounded over a deployment's lifetime",
    ),
    (
        "axond.revision.desired",
        "a revision id is unbounded over a deployment's lifetime",
    ),
    (
        "axond.revision.active",
        "a revision id is unbounded over a deployment's lifetime",
    ),
    (
        "axond.request_id",
        "a request id is unbounded by construction",
    ),
    ("request_id", "a request id is unbounded by construction"),
    ("axond.jti", "a token id is unbounded and caller-linked"),
    ("jti", "a token id is unbounded and caller-linked"),
    ("token", "a token value is a secret"),
    ("api_key", "a key value is a secret"),
    ("secret", "a secret value is never observable"),
    ("password", "a secret value is never observable"),
    ("dsn", "a DSN carries host and credential material"),
    (
        "url",
        "a connection URL carries host and credential material",
    ),
    ("error", "a raw backend error is unbounded free text"),
    ("message", "a raw error message is unbounded free text"),
    ("detail", "a rejection detail is unbounded free text"),
    ("prompt", "request content is never observable"),
    ("completion", "response content is never observable"),
];

/// `axond.credential_source`: whose key paid for the attempt.
const CREDENTIAL_SOURCE: Label = Label::closed("axond.credential_source", &["platform", "byok"]);
/// `axond.status`: the settled outcome vocabulary shared with the usage record.
const REQUEST_STATUS: Label = Label::closed(
    "axond.status",
    &[
        "ok",
        "upstream_error",
        "client_cancelled",
        "partial",
        "rejected",
    ],
);
const TARGET_PROVIDER: Label = Label::open("axond.target.provider", LabelClass::Configured);
const TARGET_MODEL: Label = Label::open("axond.target.model", LabelClass::Configured);
const STATUS_COMPONENT: Label = Label::closed("axond.status.component", crate::status::COMPONENTS);
/// `axond.catalog.reason`: why an import was refused, bounded by
/// [`RefusalReason`](crate::backends::catalog::RefusalReason).
const CATALOG_REFUSAL_REASON: Label = Label::closed(
    "axond.catalog.reason",
    crate::backends::catalog::REFUSAL_REASONS,
);

/// The method vocabulary, which is a bound rather than an assumption: HTTP
/// permits extension methods, so [`http`](super::http) maps anything outside
/// this list to `_OTHER` before recording it, exactly as it collapses an
/// unmatched path to one route label.
///
/// A test asserts this is exactly [`super::http::METHODS`] plus
/// [`super::http::OTHER_METHOD`], since a slice cannot be extended in a const.
const HTTP_METHODS: &[&str] = &[
    "GET",
    "HEAD",
    "POST",
    "PUT",
    "PATCH",
    "DELETE",
    "OPTIONS",
    "TRACE",
    "CONNECT",
    super::http::OTHER_METHOD,
];

/// Every HTTP request, including the ones that never reach a provider: an
/// unroutable request is still counted, which is why the method vocabulary is
/// the protocol's rather than the route table's.
const HTTP_LABELS: &[Label] = &[
    Label::closed("http.request.method", HTTP_METHODS),
    Label::open("http.route", LabelClass::Route),
    Label::open("http.response.status_code", LabelClass::Numeric),
];

/// The dimensions carried by everything derived from the canonical usage
/// record. `axond.namespace` and the model dimensions are `Configured`: an
/// operator who declares a thousand aliases gets a thousand series, which is the
/// documented cost of per-alias attribution and the reason these keys may not be
/// applied by default.
const REQUEST_LABELS: &[Label] = &[
    Label::open("axond.namespace", LabelClass::Configured),
    Label::open("gen_ai.request.model", LabelClass::Configured),
    TARGET_PROVIDER,
    TARGET_MODEL,
    CREDENTIAL_SOURCE,
    REQUEST_STATUS,
];

const TARGET_LABELS: &[Label] = &[TARGET_PROVIDER, TARGET_MODEL];

const USAGE_SINK: Label = Label::closed("axond.usage_sink", &["stdout", "otlp", "postgres"]);

/// Which durable outbox the billing-grade path is using. `none` appears when an
/// event could not be journaled before one was constructed.
const USAGE_JOURNAL: Label = Label::closed("axond.usage_journal", &["none", "postgres"]);

/// Configured rather than closed: the consumer name is an operator's string, and
/// one deployment's is one series.
const JOURNAL_CONSUMER: Label = Label::open("axond.usage_journal.consumer", LabelClass::Configured);

const POISON_REASONS: &[&str] = crate::usage::journal::POISON_REASONS;

const ADMISSION_RESOURCE: Label = Label::closed(
    "axond.admission.resource",
    &[
        crate::admission::RESOURCE_REQUEST,
        crate::admission::RESOURCE_STREAM,
        crate::admission::RESOURCE_TENANT,
        crate::admission::RESOURCE_QUEUE,
        crate::admission::RESOURCE_DIAGNOSTIC,
        crate::admission::RESOURCE_DIAGNOSTIC_AUTH,
    ],
);

const REVISION_TRIGGER: Label = Label::closed(
    "axond.revision.trigger",
    &[
        super::CONVERGENCE_BOOT,
        super::CONVERGENCE_POLLED,
        super::CONVERGENCE_NOTIFIED,
        super::CONVERGENCE_PRICING_BOUNDARY,
    ],
);

/// The canonical catalogue. Adding an instrument means adding it here: the
/// catalogue is checked against [`metrics`](super::metrics) rather than the
/// other way round, so an uncatalogued instrument fails the build's tests.
pub const CATALOG: &[MetricSpec] = &[
    MetricSpec {
        name: "axond.http.server.requests",
        kind: InstrumentKind::Counter,
        unit: None,
        labels: HTTP_LABELS,
    },
    MetricSpec {
        name: "axond.http.server.duration",
        kind: InstrumentKind::Histogram,
        unit: Some("ms"),
        labels: HTTP_LABELS,
    },
    MetricSpec {
        name: "axond.request.count",
        kind: InstrumentKind::Counter,
        unit: None,
        labels: REQUEST_LABELS,
    },
    MetricSpec {
        name: "axond.request.duration",
        kind: InstrumentKind::Histogram,
        unit: Some("ms"),
        labels: REQUEST_LABELS,
    },
    MetricSpec {
        name: "axond.request.time_to_first_token",
        kind: InstrumentKind::Histogram,
        unit: Some("ms"),
        labels: REQUEST_LABELS,
    },
    MetricSpec {
        name: "axond.tokens.input",
        kind: InstrumentKind::Counter,
        unit: None,
        labels: REQUEST_LABELS,
    },
    MetricSpec {
        name: "axond.tokens.cache_read",
        kind: InstrumentKind::Counter,
        unit: None,
        labels: REQUEST_LABELS,
    },
    MetricSpec {
        name: "axond.tokens.cache_write",
        kind: InstrumentKind::Counter,
        unit: None,
        labels: REQUEST_LABELS,
    },
    MetricSpec {
        name: "axond.tokens.output",
        kind: InstrumentKind::Counter,
        unit: None,
        labels: REQUEST_LABELS,
    },
    MetricSpec {
        name: "axond.cost.microdollars",
        kind: InstrumentKind::Counter,
        unit: Some("uUSD"),
        labels: REQUEST_LABELS,
    },
    MetricSpec {
        name: "axond.upstream.errors",
        kind: InstrumentKind::Counter,
        unit: None,
        labels: REQUEST_LABELS,
    },
    MetricSpec {
        name: "axond.upstream.timeouts",
        kind: InstrumentKind::Counter,
        unit: None,
        labels: &[
            TARGET_PROVIDER,
            TARGET_MODEL,
            Label::closed(
                "axond.timeout",
                &[
                    "connect",
                    "response_headers",
                    "buffered_body",
                    "stream_idle",
                    "overall",
                ],
            ),
            Label::closed("axond.timeout.bound", &["phase", "walk_budget"]),
        ],
    },
    MetricSpec {
        name: "axond.upstream.time_to_first_token",
        kind: InstrumentKind::Histogram,
        unit: Some("ms"),
        labels: TARGET_LABELS,
    },
    MetricSpec {
        name: "axond.upstream.circuit_state",
        kind: InstrumentKind::Gauge,
        unit: None,
        labels: TARGET_LABELS,
    },
    MetricSpec {
        name: "axond.usage.records_written",
        kind: InstrumentKind::Counter,
        unit: None,
        labels: &[USAGE_SINK],
    },
    MetricSpec {
        name: "axond.usage.records_dropped",
        kind: InstrumentKind::Counter,
        unit: None,
        labels: &[
            USAGE_SINK,
            Label::closed(
                "axond.drop_reason",
                &["buffer_full", "sink_error", "shutdown"],
            ),
        ],
    },
    MetricSpec {
        name: "axond.usage.flushes",
        kind: InstrumentKind::Counter,
        unit: None,
        labels: &[
            USAGE_SINK,
            Label::closed("axond.flush_outcome", &["flushed", "failed", "timeout"]),
        ],
    },
    MetricSpec {
        name: "axond.usage.journal.appends",
        kind: InstrumentKind::Counter,
        unit: None,
        labels: &[
            USAGE_JOURNAL,
            Label::closed(
                "axond.journal.outcome",
                &[
                    "accepted",
                    "already_present",
                    "at_capacity",
                    "conflict",
                    "invalid_event",
                    "backend",
                ],
            ),
        ],
    },
    MetricSpec {
        name: "axond.usage.journal.deliveries",
        kind: InstrumentKind::Counter,
        unit: None,
        labels: &[
            USAGE_JOURNAL,
            JOURNAL_CONSUMER,
            Label::closed(
                "axond.journal.delivery",
                &["acknowledged", "redelivered", "failed"],
            ),
        ],
    },
    MetricSpec {
        name: "axond.usage.journal.quarantined",
        kind: InstrumentKind::Counter,
        unit: None,
        labels: &[
            USAGE_JOURNAL,
            JOURNAL_CONSUMER,
            Label::closed("axond.journal.poison_reason", POISON_REASONS),
        ],
    },
    MetricSpec {
        name: "axond.usage.journal.undeliverable",
        kind: InstrumentKind::Counter,
        unit: None,
        labels: &[
            USAGE_JOURNAL,
            Label::closed("axond.journal.reason", &["schema_ahead", "corrupt"]),
        ],
    },
    MetricSpec {
        name: "axond.usage.journal.lost",
        kind: InstrumentKind::Counter,
        unit: None,
        labels: &[
            USAGE_JOURNAL,
            Label::closed(
                "axond.journal.loss_reason",
                &[
                    "capacity_drop",
                    "at_capacity",
                    "conflict",
                    "invalid_event",
                    "backend",
                ],
            ),
        ],
    },
    MetricSpec {
        name: "axond.usage.journal.depth",
        kind: InstrumentKind::Gauge,
        unit: None,
        labels: &[USAGE_JOURNAL, JOURNAL_CONSUMER],
    },
    MetricSpec {
        name: "axond.usage.journal.in_flight",
        kind: InstrumentKind::Gauge,
        unit: None,
        labels: &[USAGE_JOURNAL, JOURNAL_CONSUMER],
    },
    MetricSpec {
        name: "axond.usage.journal.quarantined_events",
        kind: InstrumentKind::Gauge,
        unit: None,
        labels: &[USAGE_JOURNAL, JOURNAL_CONSUMER],
    },
    MetricSpec {
        name: "axond.usage.journal.oldest_pending_age",
        kind: InstrumentKind::Gauge,
        unit: Some("s"),
        labels: &[USAGE_JOURNAL, JOURNAL_CONSUMER],
    },
    MetricSpec {
        name: "axond.usage.journal.capacity",
        kind: InstrumentKind::Gauge,
        unit: None,
        labels: &[USAGE_JOURNAL],
    },
    MetricSpec {
        name: "axond.shutdown.phase",
        kind: InstrumentKind::Gauge,
        unit: None,
        labels: &[Label::closed(
            "axond.lifecycle_phase",
            &["serving", "draining", "closing"],
        )],
    },
    MetricSpec {
        name: "axond.shutdown.rejected_requests",
        kind: InstrumentKind::Counter,
        unit: None,
        labels: &[],
    },
    MetricSpec {
        name: "axond.shutdown.abandoned_requests",
        kind: InstrumentKind::Counter,
        unit: None,
        labels: &[],
    },
    MetricSpec {
        name: "axond.config.reloads",
        kind: InstrumentKind::Counter,
        unit: None,
        labels: &[
            Label::closed(
                "axond.reload.trigger",
                &[crate::reload::TRIGGER_SIGNAL, crate::reload::TRIGGER_WATCH],
            ),
            Label::closed(
                "axond.reload.outcome",
                &[super::RELOAD_APPLIED, super::RELOAD_REJECTED],
            ),
        ],
    },
    MetricSpec {
        name: "axond.config.generation",
        kind: InstrumentKind::Gauge,
        unit: None,
        labels: &[],
    },
    MetricSpec {
        name: "axond.revision.attempts",
        kind: InstrumentKind::Counter,
        unit: None,
        labels: &[
            REVISION_TRIGGER,
            Label::closed(
                "axond.revision.outcome",
                &["published", "converged", "empty", "rejected"],
            ),
        ],
    },
    MetricSpec {
        name: "axond.revision.rejections",
        kind: InstrumentKind::Counter,
        unit: None,
        labels: &[Label::closed(
            "axond.revision.reason",
            crate::convergence::reconciler::REVISION_REASONS,
        )],
    },
    MetricSpec {
        name: "axond.revision.lag",
        kind: InstrumentKind::Gauge,
        unit: Some("ms"),
        labels: &[],
    },
    MetricSpec {
        name: "axond.revision.converged",
        kind: InstrumentKind::Gauge,
        unit: None,
        labels: &[],
    },
    MetricSpec {
        name: "axond.revision.desired_at",
        kind: InstrumentKind::Gauge,
        unit: Some("ms"),
        labels: &[],
    },
    MetricSpec {
        name: "axond.revision.active_at",
        kind: InstrumentKind::Gauge,
        unit: Some("ms"),
        labels: &[],
    },
    MetricSpec {
        name: "axond.revision.convergence_duration",
        kind: InstrumentKind::Histogram,
        unit: Some("ms"),
        labels: &[],
    },
    MetricSpec {
        name: "axond.revision.consecutive_failures",
        kind: InstrumentKind::Gauge,
        unit: None,
        labels: &[],
    },
    MetricSpec {
        name: "axond.revision.last_known_good",
        kind: InstrumentKind::Counter,
        unit: None,
        labels: &[Label::closed(
            "axond.revision.outcome",
            &[
                "exported",
                "export_failed",
                "restored",
                crate::convergence::reconciler::INCOMPATIBLE_REASON,
            ],
        )],
    },
    MetricSpec {
        name: "axond.budget.capacity_denials",
        kind: InstrumentKind::Counter,
        unit: None,
        labels: &[],
    },
    MetricSpec {
        name: "axond.budget.namespace_denials",
        kind: InstrumentKind::Counter,
        unit: None,
        labels: &[],
    },
    MetricSpec {
        name: "axond.budget.retained_subjects",
        kind: InstrumentKind::Gauge,
        unit: None,
        labels: &[],
    },
    MetricSpec {
        name: "axond.middleware.capacity_wait",
        kind: InstrumentKind::Histogram,
        unit: Some("ms"),
        labels: &[],
    },
    MetricSpec {
        name: "axond.middleware.capacity_timeouts",
        kind: InstrumentKind::Counter,
        unit: None,
        labels: &[],
    },
    MetricSpec {
        name: "axond.middleware.response_buffering_duration",
        kind: InstrumentKind::Histogram,
        unit: Some("ms"),
        labels: &[],
    },
    MetricSpec {
        name: "axond.admission.queue.depth",
        kind: InstrumentKind::Histogram,
        unit: None,
        labels: &[],
    },
    MetricSpec {
        name: "axond.admission.in_flight",
        kind: InstrumentKind::UpDownCounter,
        unit: None,
        labels: &[ADMISSION_RESOURCE],
    },
    MetricSpec {
        name: "axond.admission.rejections",
        kind: InstrumentKind::Counter,
        unit: None,
        labels: &[
            ADMISSION_RESOURCE,
            Label::closed(
                "axond.error.type",
                &[
                    "tenant_concurrency_exceeded",
                    "admission_tenant_capacity_exhausted",
                    "stream_capacity_exhausted",
                    "gateway_overloaded",
                    "admission_queue_full",
                    "admission_queue_timeout",
                    "diagnostic_concurrency_exceeded",
                ],
            ),
        ],
    },
    MetricSpec {
        name: "axond.rate_limit.denials",
        kind: InstrumentKind::Counter,
        unit: None,
        labels: &[],
    },
    MetricSpec {
        name: "axond.rate_limit.capacity_denials",
        kind: InstrumentKind::Counter,
        unit: None,
        labels: &[],
    },
    MetricSpec {
        name: "axond.rate_limit.unavailable_denials",
        kind: InstrumentKind::Counter,
        unit: None,
        labels: &[],
    },
    MetricSpec {
        name: "axond.policy.unenforceable_denials",
        kind: InstrumentKind::Counter,
        unit: None,
        labels: &[
            Label::closed("axond.policy.condition", &["ungoverned", "layout"]),
            // Responsibility first: the spend store and the concurrency store
            // are usually the same backend, and the two denials are separate
            // operator problems.
            Label::closed(
                "axond.policy.store",
                &[
                    "budget:in_memory",
                    "budget:redis",
                    "budget:postgres",
                    "rate_limit:redis",
                ],
            ),
        ],
    },
    MetricSpec {
        name: "axond.revocation.denials",
        kind: InstrumentKind::Counter,
        unit: None,
        labels: &[],
    },
    MetricSpec {
        name: "axond.revocation.unavailable_denials",
        kind: InstrumentKind::Counter,
        unit: None,
        labels: &[],
    },
    // The status registry's own instruments. Component-scoped and nothing else:
    // a status metric that carried the namespace it was observed for would leak
    // the tenancy the redacted status response is careful not to.
    MetricSpec {
        name: "axond.status.component_state",
        kind: InstrumentKind::Gauge,
        unit: None,
        labels: &[STATUS_COMPONENT],
    },
    MetricSpec {
        name: "axond.status.observation_age",
        kind: InstrumentKind::Gauge,
        unit: Some("ms"),
        labels: &[STATUS_COMPONENT],
    },
    MetricSpec {
        name: "axond.status.refreshes",
        kind: InstrumentKind::Counter,
        unit: None,
        labels: &[
            STATUS_COMPONENT,
            Label::closed("axond.status.outcome", &["observed", "failed", "disabled"]),
        ],
    },
    // The catalogue's import health. A refused import leaves the previous
    // catalogue active, so the refusal is invisible in every other series: these
    // three are what make "metadata has stopped advancing" observable. The
    // reason is the only dimension — the pointer, the source URL, the content id,
    // and the error text are all unbounded over one upstream document, and they
    // travel on the log line and the operator status surface instead.
    MetricSpec {
        name: "axond.catalog.refusals",
        kind: InstrumentKind::Counter,
        unit: None,
        labels: &[CATALOG_REFUSAL_REASON],
    },
    MetricSpec {
        name: "axond.catalog.active_age",
        kind: InstrumentKind::Gauge,
        unit: Some("ms"),
        labels: &[],
    },
    MetricSpec {
        name: "axond.catalog.consecutive_refusals",
        kind: InstrumentKind::Gauge,
        unit: None,
        labels: &[],
    },
    MetricSpec {
        name: "axond.admin.bindings",
        kind: InstrumentKind::Counter,
        unit: None,
        labels: &[
            Label::closed(
                "outcome",
                &["published", "replayed", "unchanged", "dry_run", "refused"],
            ),
            Label::closed("path", &["imported", "local"]),
        ],
    },
    MetricSpec {
        name: "axond.admin.binding_refusals",
        kind: InstrumentKind::Counter,
        unit: None,
        labels: &[Label::closed(
            "code",
            &[
                "unknown_provider",
                "catalogue_identity_required",
                "not_in_catalogue",
                "ambiguous_callable",
                "observed_unbillable",
                "price_required",
                "catalogue_not_imported",
                "project_required",
                "pin_locked",
                "not_local",
                "price_change_requires_interval",
            ],
        )],
    },
];

/// The catalogued spec for `name`.
pub fn spec(name: &str) -> Option<&'static MetricSpec> {
    CATALOG.iter().find(|spec| spec.name == name)
}

/// A metric name is `axond.` plus dot-separated lower-case segments. Bounded in
/// length because a name is a series key in every backend downstream.
pub fn validate_metric_name(name: &str) -> Result<(), CatalogError> {
    let malformed = |reason| {
        Err(CatalogError::MalformedName {
            name: name.to_owned(),
            reason,
        })
    };
    if !name.starts_with("axond.") {
        return malformed("every axond metric name starts with `axond.`");
    }
    if name.len() > 96 {
        return malformed("metric names are at most 96 characters");
    }
    for segment in name.split('.') {
        if segment.is_empty() {
            return malformed("segments must not be empty");
        }
        if !segment
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        {
            return malformed("segments are lower-case ASCII, digits, and `_`");
        }
    }
    Ok(())
}

/// A label key is well-formed and is not one of the identities that must never
/// become a metric dimension.
pub fn validate_label_key(key: &str) -> Result<(), CatalogError> {
    if key.is_empty() || key.len() > 64 {
        return Err(CatalogError::MalformedLabel {
            key: key.to_owned(),
            reason: "label keys are 1 to 64 characters",
        });
    }
    for segment in key.split('.') {
        if segment.is_empty() {
            return Err(CatalogError::MalformedLabel {
                key: key.to_owned(),
                reason: "segments must not be empty",
            });
        }
        if !segment
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        {
            return Err(CatalogError::MalformedLabel {
                key: key.to_owned(),
                reason: "segments are lower-case ASCII, digits, and `_`",
            });
        }
    }
    if let Some((_, reason)) = FORBIDDEN_LABEL_KEYS
        .iter()
        .find(|(forbidden, _)| *forbidden == key)
    {
        return Err(CatalogError::ForbiddenLabel {
            key: key.to_owned(),
            reason,
        });
    }
    Ok(())
}

/// A *default* label — one attached to every instrument rather than chosen for
/// one — must be well-formed, safe, and bounded by the code rather than by a
/// deployment's configuration.
///
/// This is the stricter of the two rules, and it is what refuses the tempting
/// ones: a default `axond.namespace` would multiply every instrument in the
/// catalogue, including the process-wide gauges, by the tenant count.
pub fn validate_default_label_key(key: &str) -> Result<(), CatalogError> {
    validate_label_key(key)?;
    let class = CATALOG
        .iter()
        .flat_map(|spec| spec.labels)
        .find(|label| label.key == key)
        .map(|label| label.class)
        .unwrap_or(LabelClass::Configured);
    if class.is_default_safe() {
        Ok(())
    } else {
        Err(CatalogError::UnboundedDefaultLabel {
            key: key.to_owned(),
            class: "configured",
        })
    }
}

/// Validate one asset's reference to a metric: the name exists and every label
/// it selects on is declared for that metric. This is what a dashboard, an alert
/// rule, or a documentation table is checked with.
pub fn validate_reference(name: &str, labels: &[&str]) -> Result<(), CatalogError> {
    let spec = spec(name).ok_or_else(|| CatalogError::UnknownMetric {
        name: name.to_owned(),
    })?;
    for key in labels {
        validate_label_key(key)?;
        if spec.label(key).is_none() {
            return Err(CatalogError::UndeclaredLabel {
                metric: name.to_owned(),
                key: (*key).to_owned(),
            });
        }
    }
    Ok(())
}

/// Validate one recorded label value against the metric's vocabulary. Only
/// closed labels have one; every other class accepts what the deployment or the
/// protocol produces.
pub fn validate_label_value(name: &str, key: &str, value: &str) -> Result<(), CatalogError> {
    let spec = spec(name).ok_or_else(|| CatalogError::UnknownMetric {
        name: name.to_owned(),
    })?;
    let label = spec
        .label(key)
        .ok_or_else(|| CatalogError::UndeclaredLabel {
            metric: name.to_owned(),
            key: key.to_owned(),
        })?;
    if label.class == LabelClass::Closed && !label.values.contains(&value) {
        return Err(CatalogError::UnknownLabelValue {
            metric: name.to_owned(),
            key: key.to_owned(),
            value: value.to_owned(),
        });
    }
    Ok(())
}

/// Check the catalogue against its own rules. Every error is collected rather
/// than the first returned, so one run names everything that has to be fixed.
pub fn validate_catalog() -> Vec<CatalogError> {
    let mut failures = Vec::new();
    let mut seen = BTreeSet::new();
    for spec in CATALOG {
        if let Err(error) = validate_metric_name(spec.name) {
            failures.push(error);
        }
        if !seen.insert(spec.name) {
            failures.push(CatalogError::DuplicateMetric {
                name: spec.name.to_owned(),
            });
        }
        let mut keys = BTreeSet::new();
        for label in spec.labels {
            if let Err(error) = validate_label_key(label.key) {
                failures.push(error);
            }
            if !keys.insert(label.key) {
                failures.push(CatalogError::DuplicateLabel {
                    metric: spec.name.to_owned(),
                    key: label.key.to_owned(),
                });
            }
            match label.class {
                LabelClass::Closed if label.values.is_empty() => {
                    failures.push(CatalogError::EmptyVocabulary {
                        metric: spec.name.to_owned(),
                        key: label.key.to_owned(),
                    });
                }
                LabelClass::Closed => {}
                class if !label.values.is_empty() => {
                    failures.push(CatalogError::UnexpectedVocabulary {
                        metric: spec.name.to_owned(),
                        key: label.key.to_owned(),
                        class: match class {
                            LabelClass::Numeric => "numeric",
                            LabelClass::Route => "route",
                            _ => "configured",
                        },
                    });
                }
                _ => {}
            }
        }
    }
    failures
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The catalogued method vocabulary is the list the recorder normalises
    /// against, plus the bucket it normalises into. A client that invents a
    /// method therefore cannot mint a series, which is what `Closed` claims.
    #[test]
    fn the_method_vocabulary_is_exactly_what_the_recorder_can_emit() {
        let recorded: Vec<&str> = super::super::http::METHODS
            .iter()
            .copied()
            .chain(std::iter::once(super::super::http::OTHER_METHOD))
            .collect();
        assert_eq!(HTTP_METHODS, recorded.as_slice());
        for method in recorded {
            validate_label_value("axond.http.server.requests", "http.request.method", method)
                .expect("every recordable method is catalogued");
        }
    }

    /// Every refusal admission can record carries both its resource and its
    /// error code onto `axond.admission.rejections`, so both vocabularies have
    /// to contain it: an undeclared value is one a dashboard drilling into the
    /// shedding it caused is refused for naming.
    #[test]
    fn every_admission_rejection_is_catalogued_by_resource_and_code() {
        for rejection in crate::admission::AdmissionRejection::ALL {
            validate_label_value(
                "axond.admission.rejections",
                "axond.admission.resource",
                rejection.scope(),
            )
            .expect("every rejection's resource is catalogued");
            validate_label_value(
                "axond.admission.rejections",
                "axond.error.type",
                rejection.code(),
            )
            .expect("every rejection's error code is catalogued");
        }
    }

    #[test]
    fn admission_queue_depth_is_a_label_free_histogram() {
        let queue_depth = spec("axond.admission.queue.depth").expect("queue depth is catalogued");
        assert_eq!(queue_depth.kind, InstrumentKind::Histogram);
        assert_eq!(queue_depth.unit, None);
        assert!(queue_depth.labels.is_empty());
        validate_reference("axond.admission.queue.depth", &[])
            .expect("the label-free instrument is a valid reference");
        for forbidden_dimension in [
            "axond.tenant",
            "axond.request_id",
            "axond.subject",
            "axond.alias",
            "axond.admission.resource",
        ] {
            assert!(
                validate_reference("axond.admission.queue.depth", &[forbidden_dimension]).is_err(),
                "queue depth must not acquire `{forbidden_dimension}`"
            );
        }
    }

    /// Two ceilings guard one status read — authenticating it, then answering
    /// it — and a reader holds a slot in each at once. They therefore have to
    /// publish on separate resources, or the gauge would report twice the
    /// readers against a denominator that is neither ceiling.
    #[test]
    fn each_diagnostic_ceiling_holds_capacity_under_its_own_resource() {
        assert_ne!(
            crate::admission::RESOURCE_DIAGNOSTIC,
            crate::admission::RESOURCE_DIAGNOSTIC_AUTH
        );
        for resource in [
            crate::admission::RESOURCE_DIAGNOSTIC,
            crate::admission::RESOURCE_DIAGNOSTIC_AUTH,
        ] {
            validate_label_value(
                "axond.admission.in_flight",
                "axond.admission.resource",
                resource,
            )
            .expect("both diagnostic ceilings are catalogued");
        }
    }

    /// The reconciler labels a rejection with a store category or a compile
    /// reason, and every one of those has to be a value the catalogue accepts —
    /// otherwise an alert on a real rejection is refused as invalid.
    #[test]
    fn every_revision_rejection_reason_is_catalogued() {
        for reason in crate::convergence::reconciler::REVISION_REASONS {
            validate_label_value("axond.revision.rejections", "axond.revision.reason", reason)
                .expect("every emitted rejection reason is catalogued");
        }
        // Read from the compiler rather than from `REVISION_REASONS`, which the
        // loop above cannot notice a compile label going missing from.
        for reason in crate::convergence::CompileError::REASONS {
            assert!(
                crate::convergence::reconciler::REVISION_REASONS.contains(reason),
                "`{reason}` is a compile refusal an alert has to be able to select"
            );
            validate_label_value("axond.revision.rejections", "axond.revision.reason", reason)
                .expect("every compile refusal reason is catalogued");
        }
        // A label the reconciler produces without asking a category for it: read
        // from the reconciler rather than from the list above, which the loop
        // over the catalogue's own constant cannot notice going missing.
        assert!(
            crate::convergence::reconciler::REVISION_REASONS
                .contains(&crate::convergence::reconciler::INCOMPATIBLE_REASON),
            "a revision this build cannot read is labelled ahead of its category"
        );
        for outcome in [
            "exported",
            "export_failed",
            "restored",
            crate::convergence::reconciler::INCOMPATIBLE_REASON,
        ] {
            validate_label_value(
                "axond.revision.last_known_good",
                "axond.revision.outcome",
                outcome,
            )
            .expect("every last-known-good outcome is catalogued");
        }
        for category in [
            crate::backends::FailureCategory::Unavailable,
            crate::backends::FailureCategory::Conflict,
            crate::backends::FailureCategory::NotFound,
            crate::backends::FailureCategory::Invalid,
            crate::backends::FailureCategory::Denied,
            crate::backends::FailureCategory::Corrupt,
        ] {
            let reason = crate::convergence::reconciler::category_reason(category);
            assert!(
                crate::convergence::reconciler::REVISION_REASONS.contains(&reason),
                "`{reason}` is a label a store failure produces"
            );
        }
    }

    /// Every instrument built in `metrics.rs`, read from the source itself: the
    /// catalogue is only a contract if drift between it and the instruments is a
    /// test failure rather than a missing dashboard panel.
    fn built_instruments() -> BTreeSet<String> {
        let source = include_str!("metrics.rs");
        let mut names = BTreeSet::new();
        let mut rest = source;
        while let Some(index) = rest.find("meter\n") {
            rest = &rest[index..];
            let Some(open) = rest.find('"') else { break };
            let after = &rest[open + 1..];
            let Some(close) = after.find('"') else { break };
            names.insert(after[..close].to_owned());
            rest = &after[close..];
        }
        names
    }

    /// Every label key recorded in `metrics.rs`.
    fn recorded_label_keys() -> BTreeSet<String> {
        let source = include_str!("metrics.rs");
        let mut keys = BTreeSet::new();
        let mut rest = source;
        while let Some(index) = rest.find("KeyValue::new(\"") {
            let after = &rest[index + "KeyValue::new(\"".len()..];
            let Some(close) = after.find('"') else { break };
            keys.insert(after[..close].to_owned());
            rest = &after[close..];
        }
        keys
    }

    #[test]
    fn catalog_satisfies_its_own_rules() {
        assert_eq!(validate_catalog(), Vec::new());
    }

    #[test]
    fn catalog_matches_the_instruments_metrics_builds() {
        let built = built_instruments();
        let catalogued: BTreeSet<String> =
            CATALOG.iter().map(|spec| spec.name.to_owned()).collect();
        assert_eq!(
            built, catalogued,
            "the catalogue and the instruments in metrics.rs have drifted"
        );
    }

    #[test]
    fn every_recorded_label_is_catalogued_and_safe() {
        let catalogued: BTreeSet<&str> = CATALOG
            .iter()
            .flat_map(|spec| spec.labels)
            .map(|label| label.key)
            .collect();
        for key in recorded_label_keys() {
            validate_label_key(&key).expect("recorded label keys are safe");
            assert!(
                catalogued.contains(key.as_str()),
                "label `{key}` is recorded but not catalogued"
            );
        }
    }

    #[test]
    fn identity_labels_are_refused() {
        for (key, _) in FORBIDDEN_LABEL_KEYS {
            assert!(
                matches!(
                    validate_label_key(key),
                    Err(CatalogError::ForbiddenLabel { .. })
                ),
                "`{key}` must be refused as a metric label"
            );
            assert!(validate_default_label_key(key).is_err());
        }
    }

    #[test]
    fn configured_labels_are_refused_as_defaults() {
        for key in [
            "axond.namespace",
            "gen_ai.request.model",
            "axond.target.model",
            "axond.target.provider",
            "service.instance.id",
        ] {
            validate_label_key(key).expect("a declared dimension is a legitimate label");
            assert!(
                matches!(
                    validate_default_label_key(key),
                    Err(CatalogError::UnboundedDefaultLabel { .. })
                ),
                "`{key}` must be refused as a default label"
            );
        }
        // An unknown key is treated as configured: a default label has to be
        // catalogued and bounded before it can be applied everywhere.
        assert!(matches!(
            validate_default_label_key("axond.unreviewed_dimension"),
            Err(CatalogError::UnboundedDefaultLabel { .. })
        ));
        for key in [
            "axond.status.component",
            "axond.admission.resource",
            "http.route",
            "http.response.status_code",
        ] {
            validate_default_label_key(key).expect("bounded labels may be defaults");
        }
    }

    #[test]
    fn the_fleet_identity_is_a_configured_resource_label() {
        let label = resource_label("service.instance.id").expect("catalogued resource label");
        assert_eq!(label.class, LabelClass::Configured);
        validate_label_key(label.key).expect("the OTLP resource key is well formed");
        assert!(validate_default_label_key(label.key).is_err());
    }

    #[test]
    fn status_metrics_carry_no_tenancy() {
        for spec in CATALOG
            .iter()
            .filter(|spec| spec.name.starts_with("axond.status."))
        {
            for label in spec.labels {
                validate_default_label_key(label.key).unwrap_or_else(|error| {
                    panic!("status metric {} carries {}: {error}", spec.name, label.key)
                });
            }
        }
    }

    #[test]
    fn malformed_names_and_keys_are_refused() {
        for name in [
            "http.server.requests",
            "axond..requests",
            "axond.Requests",
            "axond.requests-total",
        ] {
            assert!(
                matches!(
                    validate_metric_name(name),
                    Err(CatalogError::MalformedName { .. })
                ),
                "`{name}` must be refused"
            );
        }
        for spec in CATALOG {
            validate_metric_name(spec.name).expect("catalogued names are well-formed");
        }
        for key in ["", "axond..resource", "axond.Resource", "axond.resource!"] {
            assert!(matches!(
                validate_label_key(key),
                Err(CatalogError::MalformedLabel { .. })
            ));
        }
    }

    #[test]
    fn references_are_validated_against_the_catalogue() {
        validate_reference(
            "axond.admission.rejections",
            &["axond.admission.resource", "axond.error.type"],
        )
        .expect("the documented dimensions are declared");
        assert_eq!(
            validate_reference("axond.admission.rejection", &[]),
            Err(CatalogError::UnknownMetric {
                name: "axond.admission.rejection".to_owned()
            })
        );
        assert_eq!(
            validate_reference("axond.admission.rejections", &["axond.namespace"]),
            Err(CatalogError::UndeclaredLabel {
                metric: "axond.admission.rejections".to_owned(),
                key: "axond.namespace".to_owned(),
            })
        );
        assert!(matches!(
            validate_reference("axond.admission.rejections", &["axond.subject"]),
            Err(CatalogError::ForbiddenLabel { .. })
        ));
    }

    #[test]
    fn closed_vocabularies_are_enforced_and_open_ones_are_not() {
        validate_label_value(
            "axond.usage.records_dropped",
            "axond.drop_reason",
            "shutdown",
        )
        .expect("a documented drop reason is in the vocabulary");
        assert_eq!(
            validate_label_value(
                "axond.usage.records_dropped",
                "axond.drop_reason",
                "disk_full"
            ),
            Err(CatalogError::UnknownLabelValue {
                metric: "axond.usage.records_dropped".to_owned(),
                key: "axond.drop_reason".to_owned(),
                value: "disk_full".to_owned(),
            })
        );
        validate_label_value("axond.request.count", "axond.namespace", "tenant-a")
            .expect("a configured dimension has no vocabulary to violate");
    }

    /// The refusal vocabulary is duplicated as strings for the const catalogue,
    /// so the duplicate has to be exactly the enum — in order, with nothing
    /// extra. A reason the enum can emit but the catalogue does not accept is a
    /// series a real refusal would fail to record.
    #[test]
    fn every_refusal_reason_is_catalogued() {
        let reasons: Vec<&str> = crate::backends::catalog::RefusalReason::ALL
            .iter()
            .map(|reason| reason.as_str())
            .collect();
        assert_eq!(
            crate::backends::catalog::REFUSAL_REASONS,
            reasons.as_slice(),
            "the refusal vocabulary and its string duplicate have drifted"
        );
        for reason in reasons {
            validate_label_value("axond.catalog.refusals", "axond.catalog.reason", reason)
                .expect("every emitted refusal reason is catalogued");
        }
    }

    /// The catalogue's operator surface carries a content id; its metrics must
    /// not. A digest is unbounded over a deployment's lifetime, which is exactly
    /// what a label may not be.
    #[test]
    fn catalog_metrics_carry_only_the_bounded_reason() {
        for spec in CATALOG
            .iter()
            .filter(|spec| spec.name.starts_with("axond.catalog."))
        {
            for label in spec.labels {
                assert_eq!(
                    label.class,
                    LabelClass::Closed,
                    "catalogue metric {} carries the unbounded label {}",
                    spec.name,
                    label.key
                );
                validate_default_label_key(label.key).unwrap_or_else(|error| {
                    panic!(
                        "catalogue metric {} carries {}: {error}",
                        spec.name, label.key
                    )
                });
            }
        }
    }

    #[test]
    fn every_component_is_a_status_label_value() {
        let spec = spec("axond.status.component_state").expect("catalogued");
        let label = spec.label("axond.status.component").expect("declared");
        for component in crate::status::Component::ALL {
            assert!(label.values.contains(&component.as_str()));
        }
    }
}
