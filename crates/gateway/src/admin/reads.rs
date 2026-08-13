//! Bounded administrative reads: state, history, audit, convergence.
//!
//! Every projection here is *bounded by its type*, because an administrative read
//! runs against the same control plane a mutation does, and an unbounded one is
//! how a diagnostic query becomes the outage it was meant to diagnose.
//!
//! - **History is a bounded parent walk.** [`ControlPlaneStore`] deliberately
//!   offers no "list revisions" method, so a page is built by following
//!   [`RevisionManifest::parent`] from a starting revision at most
//!   [`HistoryLimit::MAX`] times. There is no query a caller can phrase that
//!   returns more, and retention truncating the chain ends a page instead of
//!   failing it.
//! - **An audit trail is truncated, and says so.** [`AuditPage::truncated`] is
//!   the honest answer to "is this all of it", rather than a response whose size
//!   depends on how eventful a revision was.
//! - **A state read describes resources, never their bodies.** Same rule as the
//!   diff: identity, scope, name, dependencies, and a content checksum.
//! - **Convergence is a projection of what this replica already knows.**
//!   [`ConvergenceResult::of`] reads a [`RevisionReport`], which is cached
//!   replica state; it consults no backend, so it answers during a control-plane
//!   outage — which is precisely when someone asks.
//!
//! Unlike [`crate::status`], these projections *do* name revision ids, mutation
//! ids, and actors: the audience is an administrator of the deployment, and
//! "which revision is desired" is the question. What they still never carry is
//! resource body content, credential material, or raw backend text.
//!
//! [`ControlPlaneStore`]: crate::backends::control_plane::ControlPlaneStore

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Serialize;

use super::diff::ScopeView;
use super::error::AdminError;
use crate::availability::{Availability, TargetRef};
use crate::convergence::RevisionReport;
use crate::desired_state::{
    Actor, AuditEvent, DesiredState, LoadedRevision, ResourceScope, RevisionId, RevisionManifest,
};
use crate::status::StatusScope;

/// How many revisions one history page may contain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistoryLimit(u32);

impl HistoryLimit {
    pub const MAX: u32 = 100;
    pub const DEFAULT: Self = Self(20);

    /// Refuses zero and anything over [`HistoryLimit::MAX`] rather than clamping:
    /// a caller that asked for 10,000 revisions has a pagination bug, and silently
    /// answering with 100 hides it.
    pub const fn parse(requested: u32) -> Result<Self, AdminError> {
        if requested == 0 || requested > Self::MAX {
            return Err(AdminError::HistoryLimitInvalid { max: Self::MAX });
        }
        Ok(Self(requested))
    }

    pub const fn get(self) -> u32 {
        self.0
    }
}

impl Default for HistoryLimit {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// A page request: how many revisions, and where to start.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HistoryRequest {
    pub limit: HistoryLimit,
    /// Start from this revision instead of the newest. The cursor a previous page
    /// returned; a revision is immutable, so paging cannot skip or repeat an
    /// entry the way an offset over mutable rows can.
    pub start: Option<RevisionId>,
}

/// One revision, as history shows it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RevisionRecord {
    pub revision: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    pub created_at_ms: u64,
    /// The mutation that published it, which joins this entry to its audit trail.
    pub mutation: String,
    /// The checksum of the whole desired state, so two deployments can be
    /// compared without shipping either state.
    pub checksum: String,
    pub resources: usize,
    pub blobs: usize,
}

impl RevisionRecord {
    pub fn of(manifest: &RevisionManifest) -> Self {
        Self {
            revision: manifest.id.to_string(),
            parent: manifest.parent.map(|parent| parent.to_string()),
            created_at_ms: millis(manifest.created_at),
            mutation: manifest.mutation.to_string(),
            checksum: manifest.checksum.to_string(),
            resources: manifest.entries.len(),
            blobs: manifest.blobs.len(),
        }
    }
}

/// A bounded page of revisions, newest first.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RevisionPage {
    pub revisions: Vec<RevisionRecord>,
    pub limit: u32,
    /// The revision to pass as `start` for the next page, and `None` when the
    /// walk reached a revision with no parent or one the store no longer retains.
    ///
    /// A cursor is emitted without being loaded, so a page boundary that happens
    /// to fall on the retention edge names a revision that may be pruned before
    /// the caller asks for it. Following it then answers `revision_not_found`
    /// rather than an empty page: a start the caller named is checked, whether it
    /// came from a cursor or from an operator's hand, because "the revision you
    /// asked to resume from is gone" and "there is nothing more" are different
    /// facts about a paginated history.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_start: Option<String>,
}

/// An actor, projected. The same kinds the domain records, so an audit reader
/// can filter on `breakglass` without parsing prose.
///
/// A workload (#144) is named by its tenant and principal rather than by an
/// issuer: it is Axond-owned, and its tenant is what makes the row attributable
/// after the principal it names is revoked. Its key material has no projection
/// here or anywhere else.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ActorView {
    pub kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub issuer: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub component: Option<String>,
    /// The owning tenant of a workload principal, carried so an audit row stays
    /// attributable without hydrating the revision that declared it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tenant: Option<String>,
}

impl ActorView {
    pub fn of(actor: &Actor) -> Self {
        match actor {
            Actor::Human { issuer, subject } => Self {
                kind: "human",
                issuer: Some(issuer.clone()),
                subject: Some(subject.clone()),
                component: None,
                tenant: None,
            },
            Actor::Breakglass => Self {
                kind: "breakglass",
                issuer: None,
                subject: None,
                component: None,
                tenant: None,
            },
            Actor::Workload { tenant, principal } => Self {
                kind: "workload",
                issuer: None,
                subject: Some(principal.to_string()),
                component: None,
                tenant: Some(tenant.to_string()),
            },
            Actor::System { component } => Self {
                kind: "system",
                issuer: None,
                subject: None,
                component: Some(component.clone()),
                tenant: None,
            },
        }
    }
}

/// One audit event, projected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AuditRecord {
    pub event: String,
    pub mutation: String,
    pub actor: ActorView,
    pub kind: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    /// The summary the mutation carried. Bounded and printable at submission by
    /// [`AuditSummary`](super::protocol::AuditSummary), and prefixed with the
    /// attribution when breakglass published it.
    pub summary: String,
    pub recorded_at_ms: u64,
}

impl AuditRecord {
    pub fn of(event: &AuditEvent) -> Self {
        Self {
            event: event.id.to_string(),
            mutation: event.mutation.to_string(),
            actor: ActorView::of(&event.actor),
            kind: event.kind.as_str(),
            target: event.target.map(|target| target.to_string()),
            summary: event.summary.clone(),
            recorded_at_ms: millis(event.recorded_at),
        }
    }
}

/// A revision's audit trail, bounded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AuditPage {
    pub revision: String,
    pub events: Vec<AuditRecord>,
    /// Whether the store held more events than [`AuditPage::MAX_EVENTS`].
    pub truncated: bool,
}

impl AuditPage {
    pub const MAX_EVENTS: usize = 100;

    pub fn of(revision: RevisionId, events: &[AuditEvent]) -> Self {
        Self {
            revision: revision.to_string(),
            events: events
                .iter()
                .take(Self::MAX_EVENTS)
                .map(AuditRecord::of)
                .collect(),
            truncated: events.len() > Self::MAX_EVENTS,
        }
    }
}

/// One resource, as a state read shows it: everything except its body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ResourceRecord {
    pub kind: &'static str,
    pub resource: String,
    pub version: u64,
    pub scope: ScopeView,
    pub slug: String,
    /// The checksum of the resource version's canonical bytes.
    pub content: String,
    /// The exact resource versions this one requires, in stable order.
    pub depends_on: Vec<String>,
}

/// A blob a revision declares. References only: the payload is not projected,
/// and there is no administrative read that returns one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BlobRecord {
    pub kind: &'static str,
    pub digest: String,
    pub size_bytes: u64,
}

/// The complete desired state, projected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StateView {
    /// `None` before the first publication: an empty control plane is not an
    /// error.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revision: Option<String>,
    pub resources: Vec<ResourceRecord>,
    pub blobs: Vec<BlobRecord>,
}

impl StateView {
    pub fn of(revision: Option<&LoadedRevision>) -> Result<Self, AdminError> {
        let empty = DesiredState::new();
        let state = revision.map_or(&empty, LoadedRevision::state);
        let mut resources = Vec::with_capacity(state.resources().len());
        for resource in state.resources() {
            resources.push(ResourceRecord {
                kind: resource.reference.kind.as_str(),
                resource: resource.reference.id.to_string(),
                version: resource.reference.version.get(),
                scope: ScopeView::of(&resource.scope),
                slug: resource.slug.as_str().to_owned(),
                content: resource.content_checksum()?.to_string(),
                depends_on: resource
                    .depends_on
                    .iter()
                    .map(ToString::to_string)
                    .collect(),
            });
        }
        Ok(Self {
            revision: revision.map(|revision| revision.id().to_string()),
            resources,
            blobs: state
                .blobs()
                .map(|blob| BlobRecord {
                    kind: blob.kind.as_str(),
                    digest: blob.digest.to_string(),
                    size_bytes: blob.size_bytes,
                })
                .collect(),
        })
    }
}

/// What this replica has converged onto, and why it has not.
///
/// The contract later resource handlers report after a publication: an
/// administrator who published revision R wants to know whether R is being served
/// yet, and if not, whether that is normal lag or a refusal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ConvergenceResult {
    pub converged: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub desired: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub loaded: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active: Option<String>,
    /// `control-plane` or `last-known-good`: "serving R" means something
    /// different when R came from a cache at boot.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<&'static str>,
    pub generation: u64,
    pub lag_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_convergence_ms: Option<u64>,
    pub consecutive_failures: u32,
    /// The low-cardinality reason the last candidate was refused. The rejection's
    /// operator detail is deliberately not projected: it is a log field, and this
    /// is a response.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_rejection: Option<&'static str>,
    /// Whether a reconciler is attached at all. `false` is not "lagging": the
    /// replica has no revision projection, so it can never converge onto what
    /// was published, and an operator gating a rollout on this read must not
    /// take silence for an all-clear.
    pub reconciling: bool,
}

impl ConvergenceResult {
    /// A replica with no reconciler: converged onto nothing, and honest about it
    /// rather than reading an empty report's `desired == active` as agreement.
    pub const fn unreconciled() -> Self {
        Self {
            converged: false,
            desired: None,
            loaded: None,
            active: None,
            source: None,
            generation: 0,
            lag_ms: 0,
            last_convergence_ms: None,
            consecutive_failures: 0,
            last_rejection: None,
            reconciling: false,
        }
    }

    pub fn of(report: &RevisionReport) -> Self {
        Self {
            reconciling: true,
            converged: report.converged(),
            desired: report.desired.map(|id| id.to_string()),
            loaded: report.loaded.map(|id| id.to_string()),
            active: report.active.map(|id| id.to_string()),
            source: report.source.map(|source| source.as_str()),
            generation: report.generation,
            lag_ms: duration_ms(report.lag),
            last_convergence_ms: report.last_convergence.map(duration_ms),
            consecutive_failures: report.consecutive_failures,
            last_rejection: report
                .last_rejection
                .as_ref()
                .map(|rejection| rejection.reason),
        }
    }
}

/// The state a [`ConvergenceResult`] describes, without the elapsed times that
/// move on their own.
///
/// This is what a conditional read of `/convergence` is validated over, and why
/// that read answers a weak validator: `lag_ms` grows every millisecond a replica
/// is behind, so a digest of the response bytes could never match for the caller
/// that most wants it to — a reconciler waiting for its publication to be served.
/// Everything here changes only when the replica's convergence *state* changes.
#[derive(Debug, Serialize)]
pub struct ConvergenceIdentity {
    converged: bool,
    reconciling: bool,
    desired: Option<String>,
    loaded: Option<String>,
    active: Option<String>,
    source: Option<&'static str>,
    generation: u64,
    /// How long the last accepted candidate took: a fixed measurement of a past
    /// event, unlike `lag_ms`, so it belongs in the validator.
    last_convergence_ms: Option<u64>,
    consecutive_failures: u32,
    last_rejection: Option<&'static str>,
}

impl ConvergenceResult {
    /// The state this result describes, for a validator.
    pub fn identity(&self) -> ConvergenceIdentity {
        ConvergenceIdentity {
            converged: self.converged,
            reconciling: self.reconciling,
            desired: self.desired.clone(),
            loaded: self.loaded.clone(),
            active: self.active.clone(),
            source: self.source,
            generation: self.generation,
            last_convergence_ms: self.last_convergence_ms,
            consecutive_failures: self.consecutive_failures,
            last_rejection: self.last_rejection,
        }
    }
}

/// What this replica derives about one scope's models (#148).
///
/// Replica-local, like [`ConvergenceResult`]: it reads the index the snapshot
/// being served carries and this replica's own circuits, so it answers during
/// the control-plane or provider outage that prompted the question. Two replicas
/// may legitimately answer differently, which is why the answer names the
/// generation it was read from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AvailabilityResult {
    /// The scope asked about, narrowest last.
    pub scope: String,
    /// Whether this replica derives availability at all. `false` is not "nothing
    /// is available": the replica projects no view, so an operator must not read
    /// the empty list as a deployment with no models.
    pub deriving: bool,
    pub targets: Vec<AvailabilityTarget>,
}

/// One target's derived availability.
///
/// Carries the verdict and its evidence, never the evidence's operator detail: a
/// discovery observation's `detail` can hold a provider's error text, and this is
/// a response. What the caller sees is additionally narrowed by the authority
/// the caller holds — an administrator who is not trusted with the whole
/// deployment gets [`Availability::for_scope`] at namespace scope, which drops
/// the discovery source and any reason that describes the deployment's own
/// machinery. Authority rather than the scope named in the query, which is
/// tenant-shaped for every caller of this read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AvailabilityTarget {
    pub provider: String,
    pub model: String,
    pub state: &'static str,
    pub reason: &'static str,
    pub decided_by: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<&'static str>,
    /// Whether the verdict rests on retained evidence rather than a current
    /// observation — the difference between "discovery says so" and "discovery
    /// said so, and has not been able to speak since".
    pub last_known_good: bool,
}

impl AvailabilityResult {
    /// A replica that derives no availability view.
    pub fn underived(scope: &ResourceScope) -> Self {
        Self {
            scope: scope.to_string(),
            deriving: false,
            targets: Vec::new(),
        }
    }

    pub fn of(
        scope: &ResourceScope,
        status: StatusScope,
        targets: Vec<(TargetRef, Availability)>,
    ) -> Self {
        Self {
            scope: scope.to_string(),
            deriving: true,
            targets: targets
                .into_iter()
                .map(|(target, verdict)| AvailabilityTarget::of(&target, verdict, status))
                .collect(),
        }
    }
}

impl AvailabilityTarget {
    fn of(target: &TargetRef, verdict: Availability, scope: StatusScope) -> Self {
        let verdict = verdict.for_scope(scope);
        Self {
            provider: target.provider.as_str().to_owned(),
            model: target.model.as_str().to_owned(),
            state: verdict.state.as_str(),
            reason: verdict.reason.code(),
            decided_by: verdict.decided_by.as_str(),
            observed_at_ms: verdict.observed_at.map(millis),
            expires_at_ms: verdict.expires_at.map(millis),
            source: verdict.source.map(|source| source.as_str()),
            last_known_good: verdict.last_known_good,
        }
    }
}

/// Milliseconds since the Unix epoch, saturating rather than failing: a wrong
/// host clock must not make an audit trail unreadable.
fn millis(at: SystemTime) -> u64 {
    at.duration_since(UNIX_EPOCH).map_or(0, |since| {
        u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
    })
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
