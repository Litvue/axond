//! The authenticated status contract: what a replica is willing to say about
//! its own dependencies, to whom, and out of what.
//!
//! Three surfaces, three different questions, and conflating them is how a
//! status endpoint becomes an outage of its own (ADR 0031):
//!
//! | Surface | Authenticated | Answers |
//! | --- | --- | --- |
//! | `GET /healthz` | no | is this process alive |
//! | `GET /readyz` | no | should this replica receive traffic |
//! | `GET /admin/v1/status` | yes, `status` capability | why is a dependency degraded |
//!
//! The two probes stay exactly as they are: an orchestrator polls them on every
//! replica every few seconds, so a probe that consulted a backend would multiply
//! a dependency outage by the fleet size and turn a degraded deployment into a
//! restart loop. This module is the *third* surface, and the whole design is
//! about the two properties that keep it from becoming the same hazard:
//!
//! **A status read is a cache read.** Observations are produced by a background
//! refresher ([`registry::StatusRefresher`]) and published into
//! [`registry::CachedStatusRegistry`]; [`registry::CachedStatusRegistry::view`]
//! is synchronous by construction, so a handler *cannot* probe a backend,
//! acquire budget, rate-limit, or revocation state, or wait on anything a
//! request would wait on. Status observation and inference share no locks and no
//! budget: a status request costs a read-lock on a small map, and a hung backend
//! shows up as an ageing observation rather than as a hung handler.
//!
//! **What it reports is bounded and redacted by type.** Every field of
//! [`StatusResponse`] is a bool, a number, or a `&'static str` drawn from a
//! closed vocabulary — [`Component`], [`ComponentState`], [`StatusReason`],
//! [`RefusalReason`](crate::backends::catalog::RefusalReason) — with one
//! deliberate exception, [`CatalogueSummary::content_id`]: a fixed-width digest
//! of content this process hashed, deployment-scope only, and argued for on the
//! type. There is nowhere to put a DSN, a token, a raw backend error, or an
//! unfiltered rejection detail, so redaction is not a filter that can be
//! forgotten. The operator-facing detail a probe *does* collect rides on
//! [`ComponentObservation::detail`], which is logged and never projected into a
//! response.
//!
//! Tenant scope is the second half of that: a caller holding delegated authority
//! for one namespace gets [`StatusScope::Namespace`], which keeps only the
//! components that describe its own request path, coarsens every reason code to
//! the tenant-safe vocabulary, coarsens observation ages to whole seconds, and
//! drops the deployment's revision and catalogue summaries entirely. No
//! response carries a namespace, subject, credential, alias, or revision
//! identifier in any scope, so there is no cross-tenant metadata to leak in the
//! first place.
//!
//! # What is observed
//!
//! One component, today: a replica in `mode = "stateful"` probes the control
//! plane ([`probes::ControlPlaneProbe`]) on the store its administrative surface
//! was built on, wired in
//! [`ReplicaObservability::observing`](crate::state::ReplicaObservability::observing).
//! A stateless replica opens no store and observes nothing.
//!
//! Every other component reports [`ComponentState::Disabled`] until the slice
//! that owns its backend adds a probe and enables it — the two go together, since
//! an enabled component with no probe is one that never gets observed and ages
//! into `unavailable`. Convergence is the other half and is still absent: no
//! release constructs a reconciler, so [`StatusResponse::revision`] is `null`
//! everywhere (#142).

pub mod probes;
pub mod registry;

#[cfg(test)]
mod tests;

use std::time::Duration;

use serde::Serialize;

use crate::backends::FailureCategory;
use crate::backends::catalog::CatalogReport;
use crate::convergence::{RevisionReport, SnapshotSource};
use crate::shutdown::Phase;

/// A dependency a replica reports on.
///
/// Closed on purpose: the component name is a metric label
/// (`axond.status.component`) as well as a response field, and an open component
/// vocabulary would make it an unbounded dimension.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Component {
    /// The control plane a stateful replica converges against.
    ControlPlane,
    /// The model catalogue the active revision was projected from.
    Catalogue,
    /// The secret store provider credentials are resolved through.
    SecretStore,
    /// The durable budget store.
    BudgetStore,
    /// The durable rate-limit store.
    RateLimitStore,
    /// The revocation store token verification consults.
    RevocationStore,
    /// The usage sinks records are written to.
    UsageSink,
    /// The provider credential pools, as last observed by lease attempts.
    ProviderCredentials,
}

/// Every component name, in [`Component::ALL`] order.
///
/// Duplicated as strings so the metric catalogue can name the vocabulary in a
/// const context; a test asserts the two never drift.
pub const COMPONENTS: &[&str] = &[
    "control_plane",
    "catalogue",
    "secret_store",
    "budget_store",
    "rate_limit_store",
    "revocation_store",
    "usage_sink",
    "provider_credentials",
];

impl Component {
    pub const ALL: &'static [Self] = &[
        Self::ControlPlane,
        Self::Catalogue,
        Self::SecretStore,
        Self::BudgetStore,
        Self::RateLimitStore,
        Self::RevocationStore,
        Self::UsageSink,
        Self::ProviderCredentials,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ControlPlane => "control_plane",
            Self::Catalogue => "catalogue",
            Self::SecretStore => "secret_store",
            Self::BudgetStore => "budget_store",
            Self::RateLimitStore => "rate_limit_store",
            Self::RevocationStore => "revocation_store",
            Self::UsageSink => "usage_sink",
            Self::ProviderCredentials => "provider_credentials",
        }
    }

    /// Whether a namespace-scoped caller may see this component at all.
    ///
    /// The test is "does it describe the caller's own request path": a tenant
    /// whose requests are being denied has a legitimate need to know that the
    /// budget store is unavailable, while the control plane, the secret store,
    /// and the usage pipeline describe how the operator runs the deployment and
    /// are visible only at [`StatusScope::Deployment`].
    pub const fn is_tenant_visible(self) -> bool {
        matches!(
            self,
            Self::Catalogue
                | Self::BudgetStore
                | Self::RateLimitStore
                | Self::RevocationStore
                | Self::ProviderCredentials
        )
    }
}

/// What a component's last observation said.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComponentState {
    /// Observed working.
    Ok,
    /// Reachable but impaired, or serving from a stale observation. Requests may
    /// still succeed.
    Degraded,
    /// Observed failing. What a request does about it is the responsibility's
    /// `on_unavailable` policy, not this report.
    Unavailable,
    /// Not configured in this deployment, so never probed. The default posture
    /// for every durable component in a stateless deployment.
    Disabled,
}

impl ComponentState {
    pub const ALL: &'static [Self] = &[Self::Ok, Self::Degraded, Self::Unavailable, Self::Disabled];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Degraded => "degraded",
            Self::Unavailable => "unavailable",
            Self::Disabled => "disabled",
        }
    }

    /// The `axond.status.component_state` gauge value: `0` disabled, `1` ok, `2`
    /// degraded, `3` unavailable.
    ///
    /// A severity ladder that an alert can threshold — `>= 2` is trouble — with
    /// `disabled` deliberately *below* `ok` rather than above `unavailable`. It
    /// is the absence of an observation, not the worst one, and it is what every
    /// component reports in the default stateless posture: ranking it as most
    /// severe would make the obvious alert fire permanently on the most common
    /// deployment.
    pub const fn gauge_value(self) -> u64 {
        match self {
            Self::Disabled => 0,
            Self::Ok => 1,
            Self::Degraded => 2,
            Self::Unavailable => 3,
        }
    }
}

/// Why a component is not `ok`.
///
/// A closed vocabulary, and that is the point: the reason a caller receives is
/// chosen from this list by the code that classified the failure, so a
/// backend's own error text — which carries hosts, DSNs, SQL, and occasionally
/// key material — has no path into a response. The text stays in the log line
/// the observation produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusReason {
    /// The coarse "not working" code. Also what every operator-only reason
    /// collapses to at [`StatusScope::Namespace`].
    Unavailable,
    /// Could not be reached: connection refused, DNS failure, no route.
    Unreachable,
    /// Reached, but did not answer within the probe's bound.
    Timeout,
    /// The backend refused the replica's own credentials.
    AuthenticationRejected,
    /// The backend refused on authorization or policy grounds.
    PermissionDenied,
    /// The stored schema version is not one this binary understands.
    SchemaIncompatible,
    /// Stored data could not be interpreted: a decryption failure, a corrupt or
    /// unknown-version record.
    PayloadCorrupt,
    /// A published revision failed validation.
    ValidationRejected,
    /// A published revision could not be projected into a servable snapshot.
    ProjectionRejected,
    /// A candidate snapshot was refused during compilation.
    SnapshotRejected,
    /// A published revision's approved price book could not be turned into the
    /// rates the request path bills.
    PricingRejected,
    /// This replica's clock is not on the effective-dating timeline, so *which*
    /// approved rates are in force has no answer. Replica-local, unlike every
    /// other revision refusal: a sibling with a correct clock converges.
    ClockUnsynchronised,
    /// A referenced secret could not be resolved.
    SecretUnresolved,
    /// A revision's policy document is one this replica will not start
    /// enforcing: its backends cannot, its durable layout does not match, the
    /// transition is not one this build performs, or it would leave a served
    /// namespace uncapped. The replica keeps the policy it already had.
    PolicyRejected,
    /// The last observation is older than the staleness budget. Reported instead
    /// of a stale `ok`, and deliberately not `unavailable`: a replica serving a
    /// valid snapshot through a control-plane outage is degraded, not down.
    Stale,
    /// Not configured in this deployment.
    NotConfigured,
    /// The replica is draining, so the component is being released.
    Draining,
    /// The component refused for capacity reasons rather than failing.
    CapacityExhausted,
    /// Classified as a failure that this vocabulary has no code for. Present so
    /// a new failure mode degrades to a safe code instead of tempting a caller
    /// to pass through free text.
    Unknown,
}

impl StatusReason {
    pub const ALL: &'static [Self] = &[
        Self::Unavailable,
        Self::Unreachable,
        Self::Timeout,
        Self::AuthenticationRejected,
        Self::PermissionDenied,
        Self::SchemaIncompatible,
        Self::PayloadCorrupt,
        Self::ValidationRejected,
        Self::ProjectionRejected,
        Self::SnapshotRejected,
        Self::PricingRejected,
        Self::ClockUnsynchronised,
        Self::PolicyRejected,
        Self::SecretUnresolved,
        Self::Stale,
        Self::NotConfigured,
        Self::Draining,
        Self::CapacityExhausted,
        Self::Unknown,
    ];

    pub const fn code(self) -> &'static str {
        match self {
            Self::Unavailable => "unavailable",
            Self::Unreachable => "unreachable",
            Self::Timeout => "timeout",
            Self::AuthenticationRejected => "authentication_rejected",
            Self::PermissionDenied => "permission_denied",
            Self::SchemaIncompatible => "schema_incompatible",
            Self::PayloadCorrupt => "payload_corrupt",
            Self::ValidationRejected => "validation_rejected",
            Self::ProjectionRejected => "projection_rejected",
            Self::SnapshotRejected => "snapshot_rejected",
            Self::PricingRejected => "pricing_rejected",
            Self::ClockUnsynchronised => "clock_unsynchronised",
            Self::PolicyRejected => "policy_rejected",
            Self::SecretUnresolved => "secret_unresolved",
            Self::Stale => "stale",
            Self::NotConfigured => "not_configured",
            Self::Draining => "draining",
            Self::CapacityExhausted => "capacity_exhausted",
            Self::Unknown => "unknown",
        }
    }

    /// Whether a namespace-scoped caller may see this code.
    ///
    /// The operator-only codes are the ones that describe the deployment's
    /// internals — its schema version, its stored data, its own credentials,
    /// what it tried to publish. A tenant learns *that* a dependency is not
    /// working, which is what it can act on; it does not learn that the
    /// operator's control-plane password was rotated out from under the replica.
    pub const fn is_tenant_safe(self) -> bool {
        matches!(
            self,
            Self::Unavailable
                | Self::Stale
                | Self::NotConfigured
                | Self::Draining
                | Self::CapacityExhausted
                | Self::Unknown
        )
    }

    /// This code as the given scope may see it: itself when tenant-safe, and the
    /// coarse [`StatusReason::Unavailable`] otherwise.
    pub const fn for_scope(self, scope: StatusScope) -> Self {
        match scope {
            StatusScope::Deployment => self,
            StatusScope::Namespace if self.is_tenant_safe() => self,
            StatusScope::Namespace => Self::Unavailable,
        }
    }

    /// The code for a durable-backend failure.
    pub const fn from_failure(category: FailureCategory) -> Self {
        match category {
            FailureCategory::Unavailable => Self::Unreachable,
            FailureCategory::Conflict => Self::Unknown,
            FailureCategory::NotFound => Self::NotConfigured,
            FailureCategory::Invalid => Self::ValidationRejected,
            FailureCategory::Denied => Self::PermissionDenied,
            FailureCategory::Corrupt => Self::PayloadCorrupt,
        }
    }

    /// The code for a convergence rejection, whose reasons are their own stable
    /// label vocabulary ([`crate::convergence::Rejection::reason`]).
    ///
    /// Mapped rather than forwarded so the two vocabularies can evolve
    /// independently, and so an unrecognised label becomes
    /// [`StatusReason::Unknown`] instead of a new response value.
    pub fn from_revision_reason(reason: &str) -> Self {
        match reason {
            "unavailable" => Self::Unreachable,
            "corrupt" => Self::PayloadCorrupt,
            "incompatible" => Self::SchemaIncompatible,
            // An availability projection is a projection of the same revision:
            // the compile refused to derive a view of it, and an operator repairs
            // it where they repair any other projection refusal.
            "projection" | "availability" => Self::ProjectionRejected,
            "validation" | "invalid" => Self::ValidationRejected,
            "secret" => Self::SecretUnresolved,
            "snapshot" => Self::SnapshotRejected,
            "pricing" => Self::PricingRejected,
            "clock" => Self::ClockUnsynchronised,
            // The ways a published policy is refused before it is enforced. One
            // code, because the operator's next move is the same in every case:
            // read the refusal, which names it, in the log.
            "unsupported" | "migration" | "refused" | "withdrawn" | "ungoverned"
            | "invalid_policy" => Self::PolicyRejected,
            "not_found" => Self::NotConfigured,
            "denied" => Self::PermissionDenied,
            _ => Self::Unknown,
        }
    }
}

/// How much of the deployment a caller is entitled to see.
///
/// Decided from the caller's authority, not from a request parameter: a scope is
/// something a principal has, and letting a query string select one is how a
/// tenant-visible endpoint grows an operator view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatusScope {
    /// A caller acting for one namespace. Sees the components that describe its
    /// own request path, with coarsened reasons and ages and no revision
    /// summary.
    Namespace,
    /// The operator's own authority over the whole deployment.
    Deployment,
}

impl StatusScope {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Namespace => "namespace",
            Self::Deployment => "deployment",
        }
    }

    /// The scope for a caller who does (or does not) hold the operator's own
    /// direct authority over the deployment.
    ///
    /// The predicate itself lives with authentication
    /// ([`crate::principals::InboundKey::holds_direct_operator_authority`]),
    /// which is the only place that knows how a principal was established.
    pub const fn for_operator_authority(direct_operator_authority: bool) -> Self {
        if direct_operator_authority {
            Self::Deployment
        } else {
            Self::Namespace
        }
    }
}

/// One component's observation, as the background refresher produced it.
///
/// The one place free text is allowed in this module, and it is not part of the
/// response: `detail` is what the refresher logs so an operator can correlate a
/// coarse `reason` with the backend's own error. [`StatusView::project`] drops
/// it, and no projection can reintroduce it, because [`ComponentStatus`] has no
/// field it would fit in.
#[derive(Debug, Clone)]
pub struct ComponentObservation {
    pub component: Component,
    pub state: ComponentState,
    /// `None` when the state is [`ComponentState::Ok`], and required otherwise:
    /// a degraded component without a reason is an alert nobody can action.
    pub reason: Option<StatusReason>,
    /// Operator-facing detail, for the log line only. Never serialized.
    pub detail: Option<String>,
}

impl ComponentObservation {
    /// A healthy observation.
    pub const fn ok(component: Component) -> Self {
        Self {
            component,
            state: ComponentState::Ok,
            reason: None,
            detail: None,
        }
    }

    /// A failing observation, with the operator-facing detail that will be
    /// logged and dropped.
    pub fn unavailable(component: Component, reason: StatusReason, detail: String) -> Self {
        Self {
            component,
            state: ComponentState::Unavailable,
            reason: Some(reason),
            detail: Some(detail),
        }
    }

    /// An impaired observation.
    pub fn degraded(component: Component, reason: StatusReason, detail: String) -> Self {
        Self {
            component,
            state: ComponentState::Degraded,
            reason: Some(reason),
            detail: Some(detail),
        }
    }
}

/// One component as a cached read found it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Observed {
    pub component: Component,
    pub state: ComponentState,
    pub reason: Option<StatusReason>,
    /// How long ago the observation was taken. Zero for components that are not
    /// probed at all.
    pub age: Duration,
    /// Whether `age` exceeded the staleness budget, in which case `state` was
    /// already coarsened to [`ComponentState::Degraded`] with
    /// [`StatusReason::Stale`].
    pub stale: bool,
}

/// An immutable, already-taken read of every component.
///
/// Produced by [`registry::CachedStatusRegistry::view`] without any I/O, and
/// projected into a response afterwards, so scope and redaction are decided over
/// data that is already in hand rather than while a backend is being consulted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusView {
    pub components: Vec<Observed>,
}

impl StatusView {
    /// Whether any component's observation aged past the staleness budget.
    pub fn stale(&self) -> bool {
        self.components.iter().any(|observed| observed.stale)
    }

    /// Project this view into the response one caller is entitled to.
    ///
    /// `revision` is the replica's convergence report, when there is one; it is
    /// deployment-scope-only and reduced to bounded fields, so no revision
    /// identifier reaches a response in any scope.
    pub fn project(
        &self,
        scope: StatusScope,
        phase: Phase,
        revision: Option<&RevisionReport>,
    ) -> StatusResponse {
        self.project_with_catalogue(scope, phase, revision, None)
    }

    /// Project this view, including what is operationally true about the model
    /// catalogue.
    ///
    /// `catalogue` is deployment-scope-only for the same reason `revision` is:
    /// which content a replica imported, and how far behind it has fallen, is
    /// how the operator runs the deployment. A tenant still learns that the
    /// catalogue component is degraded, which is all it can act on.
    pub fn project_with_catalogue(
        &self,
        scope: StatusScope,
        phase: Phase,
        revision: Option<&RevisionReport>,
        catalogue: Option<&CatalogReport>,
    ) -> StatusResponse {
        let visible: Vec<&Observed> = self
            .components
            .iter()
            .filter(|observed| match scope {
                StatusScope::Deployment => true,
                StatusScope::Namespace => observed.component.is_tenant_visible(),
            })
            .collect();
        StatusResponse {
            object: "status",
            observed: "replica",
            scope: scope.as_str(),
            phase: phase.as_str(),
            stale: visible.iter().any(|observed| observed.stale),
            components: visible
                .iter()
                .map(|observed| ComponentStatus {
                    component: observed.component.as_str(),
                    state: observed.state.as_str(),
                    reason: observed.reason.map(|reason| reason.for_scope(scope).code()),
                    observed_age_ms: coarsen_age(observed.age, scope),
                })
                .collect(),
            revision: match scope {
                StatusScope::Deployment => revision.map(RevisionSummary::from_report),
                StatusScope::Namespace => None,
            },
            catalogue: match scope {
                StatusScope::Deployment => catalogue.map(CatalogueSummary::from_report),
                StatusScope::Namespace => None,
            },
        }
    }
}

/// Observation ages are exact for an operator and coarsened to whole seconds for
/// a tenant: the exact age of an internal observation is a readout of the
/// refresher's cadence, which is the operator's business.
fn coarsen_age(age: Duration, scope: StatusScope) -> u64 {
    let millis = u64::try_from(age.as_millis()).unwrap_or(u64::MAX);
    match scope {
        StatusScope::Deployment => millis,
        StatusScope::Namespace => (millis / 1_000) * 1_000,
    }
}

/// The authenticated status response.
///
/// Every field is a bool, a number, or a `&'static str` from a closed
/// vocabulary, save the one digest documented on [`CatalogueSummary`]. That is
/// the redaction guarantee: there is no `String` field for a DSN, a token, a raw
/// backend error, an unfiltered rejection detail, or any tenant identifier to be
/// written into, in this scope or another.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StatusResponse {
    pub object: &'static str,
    /// `replica`: this is one process's own view, never a fleet aggregate.
    pub observed: &'static str,
    pub scope: &'static str,
    /// The replica's lifecycle phase, matching what `/readyz` answers from.
    pub phase: &'static str,
    /// Whether any reported component is being served from an observation older
    /// than the staleness budget.
    pub stale: bool,
    pub components: Vec<ComponentStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revision: Option<RevisionSummary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalogue: Option<CatalogueSummary>,
}

/// One component, as one caller may see it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ComponentStatus {
    pub component: &'static str,
    pub state: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<&'static str>,
    /// How long ago the observation behind this entry was taken. Present so a
    /// caller can tell "observed healthy a second ago" from "healthy the last
    /// time anything succeeded".
    pub observed_age_ms: u64,
}

/// The model catalogue an operator is serving metadata from, and whether it is
/// still advancing.
///
/// Answers, in one read, the two questions a refused import raises: *what is
/// active* and *how stale is it*. Without this an operator can see refusals
/// climbing on a dashboard and have no way to tell which content the replica is
/// actually serving, or whether the last import merely arrived late.
///
/// `content_id` is the one `String` in a status response, and it is exactly as
/// bounded as the rest: [`CONTENT_ID_SHORT_HEX`] hex digits of a SHA-256 this
/// process computed over its own normalized content
/// ([`CatalogContentId::short`]). No upstream text, no source URL, no error
/// message, and no pointer reaches it — those stay in the log line the import
/// produced — and it is deployment-scope-only, so it is never a cross-tenant
/// identifier. It is emphatically not a metric label: the catalogue metrics
/// carry the bounded refusal reason and nothing else.
///
/// [`CONTENT_ID_SHORT_HEX`]: crate::backends::catalog::CONTENT_ID_SHORT_HEX
/// [`CatalogContentId::short`]: crate::backends::catalog::CatalogContentId::short
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CatalogueSummary {
    /// The active content's short digest, absent before a first import.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content_id: Option<String>,
    /// How long ago the active content was last confirmed current. Absent when
    /// nothing is active — which is not staleness, it is a deployment that has
    /// never imported.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_age_ms: Option<u64>,
    pub consecutive_refusals: u32,
    /// The last refusal's bounded reason, from
    /// [`RefusalReason`](crate::backends::catalog::RefusalReason).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_refusal: Option<&'static str>,
    /// Whether refusals have persisted across more than one import. The same
    /// condition the alert fires on, so the page and the surface agree.
    pub persistent_refusal: bool,
}

impl CatalogueSummary {
    pub fn from_report(report: &CatalogReport) -> Self {
        Self {
            content_id: report.active.map(|active| active.content_id.short()),
            active_age_ms: report
                .active
                .map(|active| u64::try_from(active.age.as_millis()).unwrap_or(u64::MAX)),
            consecutive_refusals: report.consecutive_refusals,
            last_refusal: report.last_refusal.map(|reason| reason.as_str()),
            persistent_refusal: report.persistent_refusal(),
        }
    }
}

/// The deployment's convergence state, reduced to bounded fields.
///
/// Revision *identifiers* are deliberately absent: they are unbounded over a
/// deployment's lifetime, they are the one field an operator is tempted to make
/// a metric label out of, and `converged` plus `lag_ms` is what answers the
/// question this surface exists for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct RevisionSummary {
    pub converged: bool,
    /// How long desired has differed from active. Zero when converged.
    pub lag_ms: u64,
    /// The active snapshot's generation, which request logs correlate against.
    pub generation: u64,
    pub consecutive_failures: u32,
    /// Where the active snapshot came from, when one is active.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<&'static str>,
    /// The last refusal's code, mapped through
    /// [`StatusReason::from_revision_reason`].
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<&'static str>,
}

impl RevisionSummary {
    pub fn from_report(report: &RevisionReport) -> Self {
        Self {
            converged: report.converged(),
            lag_ms: u64::try_from(report.lag.as_millis()).unwrap_or(u64::MAX),
            generation: report.generation,
            consecutive_failures: report.consecutive_failures,
            source: report.source.map(SnapshotSource::as_str),
            reason: report
                .last_rejection
                .as_ref()
                .map(|rejection| StatusReason::from_revision_reason(rejection.reason).code()),
        }
    }
}
