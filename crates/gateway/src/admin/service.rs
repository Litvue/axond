//! The administrative service boundary: the one path durable state changes
//! through.
//!
//! Every mutation, whatever resource it is about, takes exactly this route:
//!
//! 1. **Mode first.** A stateless deployment answers
//!    [`AdminError::StatefulModeRequired`] before anything else, and it does so
//!    without a control-plane backend existing: [`AdminService::stateless`]
//!    holds no store, so "stateless mode did not touch the backend" is a property
//!    of the type rather than of the order of two `if`s.
//! 2. **Authority.** The service takes an [`AdminGrant`], not an identity, and
//!    checks that the grant covers a mutating action at the scope the request
//!    names. An authenticated-but-unauthorized caller cannot reach step 3,
//!    because it has no grant to pass. The reads are held to the same standard
//!    rather than to the action alone: every projection here is deployment-wide,
//!    so reading one takes deployment authority.
//! 3. **Read the complete desired state.** The head revision is read once and
//!    hydrated whole. There is no partial read and no diff to apply: a revision
//!    *is* the complete state, so a candidate is built from state that was
//!    actually published rather than from the caller's idea of it.
//! 4. **Build a complete candidate, then check what it actually changed.** The
//!    handler's [`DesiredStateEdit`] rewrites that state. What comes out is the
//!    full desired state of the deployment, not a patch — which is what makes the
//!    next step meaningful, and also why the granted scope has to be checked
//!    against the delta: an edit is physically able to touch a resource the
//!    request did not claim.
//! 5. **Validate the complete candidate.** Before publication, and by the same
//!    call a store makes ([`RevisionCandidate::validated_checksum`]), so a dry run
//!    and an apply cannot disagree about what is valid.
//! 6. **Diff.** Computed from the two complete states, redacted by
//!    [`SemanticDiff`].
//! 7. **Publish atomically, or stop.** A dry run returns after step 6 and never
//!    calls [`ControlPlaneStore::publish_revision`]; an apply calls it exactly
//!    once, and the store commits the manifest, the resource versions, the audit
//!    event, and the idempotency record together or not at all.
//!
//! The service therefore owns the *protocol* invariants — mode, authority,
//! preconditions, validation-before-publication, dry-run purity, and error
//! translation — while the store owns the transactional ones. Neither can be
//! bypassed by a resource handler, because a handler contributes only an edit and
//! a summary.
//!
//! # Not on the request path
//!
//! Nothing here is reachable from an inference request. Steps 3 and 7 are
//! control-plane calls, and ADR 0027's stance is that a control-plane outage
//! stalls administration and convergence while replicas keep serving from their
//! immutable snapshots. That is why every method is `async` and none is called
//! from [`crate::routes`].
//!
//! [`ControlPlaneStore::publish_revision`]: crate::backends::control_plane::ControlPlaneStore::publish_revision

use std::sync::Arc;
use std::time::SystemTime;

use serde::Serialize;
use tracing::{debug, warn};

use super::auth::{AdminAction, AdminAuthError, AdminGrant, AdminIdentity};
use super::catalogue::{CatalogueRequest, CatalogueView};
use super::diff::SemanticDiff;
use super::error::AdminError;
use super::protocol::{MutationRequest, WriteMode};
use super::reads::{
    AuditPage, AvailabilityResult, ConvergenceResult, HistoryRequest, RevisionPage, RevisionRecord,
    StateView,
};
use crate::availability::{AvailabilityReader, AvailabilityView, ScopeRef};
use crate::backends::control_plane::{ControlPlaneError, ControlPlaneStore};
use crate::backends::secrets::SecretStore;
use crate::config::Mode;
use crate::convergence::RevisionReport;
use crate::desired_state::models::legacy_alias_allowlist;
use crate::desired_state::{
    AccessDenial, AuditEvent, AuditEventId, DenialReason, DesiredState, ExpectedRevision,
    LoadedRevision, Mutation, MutationId, ResourceScope, RevisionCandidate, RevisionId, Surface,
    Uuid7Generator, ValidationError,
};
use crate::status::StatusScope;

/// Whether a grant at `granted` may change a resource scoped to `resource`.
///
/// Containment, not equality: a deployment grant covers everything, a tenant
/// grant covers that tenant's projects, and a project grant covers only itself.
fn scope_covers(granted: &ResourceScope, resource: &ResourceScope) -> bool {
    match granted {
        ResourceScope::Deployment => true,
        ResourceScope::Tenant(tenant) => resource.tenant() == Some(*tenant),
        ResourceScope::Project { tenant, project } => matches!(
            resource,
            ResourceScope::Project {
                tenant: other_tenant,
                project: other_project,
            } if other_tenant == tenant && other_project == project
        ),
    }
}

/// A change to desired state, expressed as a rewrite of the complete state.
///
/// This is the whole extent of what a resource handler contributes, and it is why
/// no resource schema appears in this module: a handler that creates an alias
/// inserts a resource version here, and every protocol property above holds
/// without the service knowing what an alias is.
///
/// Fallible, because a handler's own preconditions ("that credential does not
/// exist in this revision") are validation failures of the same kind the domain
/// raises, and belong in the same typed refusal.
pub trait DesiredStateEdit: Send + Sync {
    fn edit(&self, state: &mut DesiredState) -> Result<(), ValidationError>;
}

impl<F> DesiredStateEdit for F
where
    F: Fn(&mut DesiredState) -> Result<(), ValidationError> + Send + Sync,
{
    fn edit(&self, state: &mut DesiredState) -> Result<(), ValidationError> {
        self(state)
    }
}

/// What a mutation did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "result", rename_all = "snake_case")]
pub enum MutationResult {
    /// A new revision was published.
    Published { revision: String },
    /// The idempotency key had already published this exact desired state, so the
    /// original revision is returned rather than a second one being created.
    Replayed { revision: String },
    /// Nothing was published, and nothing was recorded.
    DryRun,
}

/// The outcome of an administrative mutation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MutationOutcome {
    #[serde(flatten)]
    pub result: MutationResult,
    /// The revision the candidate was built on. `None` for the first publication.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base: Option<String>,
    /// The checksum of the candidate's complete desired state. Equal checksums
    /// mean equal state, which is how a caller confirms a replay was a replay of
    /// its own change.
    pub checksum: String,
    pub mode: &'static str,
    pub diff: SemanticDiff,
}

impl MutationOutcome {
    /// The revision this outcome names, if it published or replayed one.
    pub fn revision(&self) -> Option<&str> {
        match &self.result {
            MutationResult::Published { revision } | MutationResult::Replayed { revision } => {
                Some(revision)
            }
            MutationResult::DryRun => None,
        }
    }
}

/// How much of an availability verdict a caller may be told.
///
/// Authority, not scope, and the distinction matters because an availability
/// read is always *about* one tenant: the scope such a query names is
/// tenant-shaped whoever asks, so deciding disclosure from the grant would
/// coarsen the answer for the root operator too — and leave nobody at all who
/// could see why discovery or this replica's health refused a target, which is
/// the question the read exists to answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AvailabilityAuthority {
    /// The caller holds this authority over the whole deployment, and sees the
    /// discovery source and the reason behind each verdict.
    Deployment,
    /// The caller holds it over a namespace, and sees the namespace projection:
    /// the state, without the deployment's discovery machinery behind it.
    Namespace,
}

impl AvailabilityAuthority {
    /// The caller's authority, from whether the deployment scope would be
    /// granted.
    pub const fn of(deployment_wide: bool) -> Self {
        if deployment_wide {
            Self::Deployment
        } else {
            Self::Namespace
        }
    }

    const fn disclosure(self) -> StatusScope {
        match self {
            Self::Deployment => StatusScope::Deployment,
            Self::Namespace => StatusScope::Namespace,
        }
    }
}

/// The `/admin/v1` service over a [`ControlPlaneStore`].
pub struct AdminService {
    /// `None` in stateless mode — and there is no other way to hold `None`, so a
    /// stateless service cannot reach a backend it does not have.
    store: Option<Arc<dyn ControlPlaneStore>>,
    /// The secret store the material routes administer, `None` when this
    /// deployment has none. Held beside the control plane rather than inside it
    /// because they are two stores with two failure modes: the control plane
    /// being unreachable does not make material unreadable, and the reverse.
    ///
    /// Nothing on the request path can reach it from here — [`AdminService`] is
    /// constructed only by the administrative runtime, and [`crate::routes`]
    /// holds no [`AdminApi`](super::router::AdminApi).
    pub(super) secrets: Option<Arc<dyn SecretStore>>,
    ids: Uuid7Generator,
}

impl AdminService {
    /// The service a stateless deployment runs: every operation is
    /// [`AdminError::StatefulModeRequired`].
    pub fn stateless() -> Self {
        Self {
            store: None,
            secrets: None,
            ids: Uuid7Generator::new(),
        }
    }

    pub fn stateful(store: Arc<dyn ControlPlaneStore>) -> Self {
        Self {
            store: Some(store),
            secrets: None,
            ids: Uuid7Generator::new(),
        }
    }

    /// The service a stateful deployment runs: a control plane, and the secret
    /// store whose material its credential references name.
    #[must_use]
    pub fn with_secrets(mut self, secrets: Arc<dyn SecretStore>) -> Self {
        self.secrets = Some(secrets);
        self
    }

    /// The service for a validated configuration: a store is present exactly when
    /// the mode is stateful, which boot validation already guarantees.
    pub fn for_mode(mode: Mode, store: Option<Arc<dyn ControlPlaneStore>>) -> Self {
        match (mode, store) {
            (Mode::Stateful, Some(store)) => Self::stateful(store),
            _ => Self::stateless(),
        }
    }

    pub const fn mode(&self) -> Mode {
        if self.store.is_some() {
            Mode::Stateful
        } else {
            Mode::Stateless
        }
    }

    /// The store, or the typed refusal a stateless deployment owes.
    pub(super) fn store(&self) -> Result<&Arc<dyn ControlPlaneStore>, AdminError> {
        self.store.as_ref().ok_or(AdminError::StatefulModeRequired)
    }

    /// Check that every resource the edit touched lies inside the granted scope.
    ///
    /// A candidate is the *complete* desired state, so an edit is physically able
    /// to rewrite another tenant's resources while the request claims its own
    /// scope. Domain validation would not object: cross-tenant rules are about
    /// references, not about who submitted the change. This is therefore the only
    /// place the grant's scope can be made to mean what it says, and it is checked
    /// over the delta rather than the whole state so that a tenant-scoped
    /// administrator can still publish a candidate that *contains* everyone
    /// else's resources unchanged — which every candidate necessarily does.
    fn within_scope(
        granted: &ResourceScope,
        before: &DesiredState,
        after: &DesiredState,
    ) -> Result<(), AdminError> {
        let touched = before
            .resources()
            .filter(|resource| after.get(&resource.reference) != Some(resource))
            .chain(
                after
                    .resources()
                    .filter(|resource| before.get(&resource.reference) != Some(resource)),
            );
        for resource in touched {
            if !scope_covers(granted, &resource.scope) {
                return Err(AdminError::Forbidden(AdminAuthError::ScopeNotPermitted));
            }
        }
        Ok(())
    }

    /// Record an authenticated caller's refusal in the durable denial trail.
    ///
    /// Only refusals of *authority* are recorded, and only in stateful mode:
    /// there is nothing to write to otherwise, and a refusal that never reached
    /// an identity — a missing credential, a stateless deployment — attributes to
    /// nobody, so a row for it would be a log line pretending to be an audit
    /// record. What is recorded is the pair an investigator asks about: which
    /// identity reached for which scope, and which rule turned it away.
    ///
    /// A store failure here does not change the answer the caller gets. The
    /// refusal already happened; failing the request because the *record* of it
    /// could not be written would turn an authorization failure into an
    /// availability failure, and would let a caller who can stall the control
    /// plane change a `403` into a `503`. It is logged at `warn`, where the
    /// deployment's own alerting sees it.
    pub(super) async fn record_denial(
        &self,
        identity: &AdminIdentity,
        action: AdminAction,
        surface: Surface,
        scope: &ResourceScope,
        error: &AdminError,
    ) {
        let (Some(store), AdminError::Forbidden(refusal)) = (self.store.as_ref(), error) else {
            return;
        };
        let denial = AccessDenial {
            id: AuditEventId::new(self.ids.next()),
            actor: identity.actor(),
            surface,
            action: action.recorded_action(),
            scope: scope.clone(),
            reason: match refusal {
                AdminAuthError::ActionNotPermitted { .. } => DenialReason::RoleLacksAction,
                _ => DenialReason::OutOfScope,
            },
            recorded_at: SystemTime::now(),
        };
        if let Err(error) = store.record_denial(&denial).await {
            warn!(
                target: "axond.admin",
                error = %error,
                action = action.as_str(),
                "an administrative denial could not be recorded",
            );
        }
    }

    /// Record a mutation's own refusal, from the grant and the request that
    /// carry everything the trail needs.
    async fn denied(&self, grant: &AdminGrant, request: &MutationRequest, error: &AdminError) {
        self.record_denial(
            grant.identity(),
            grant.action(),
            request.surface,
            &request.scope,
            error,
        )
        .await;
    }

    /// Check a grant covers the action about to be performed.
    fn permits(grant: &AdminGrant, action: AdminAction) -> Result<(), AdminError> {
        if grant.action() != action {
            return Err(AdminError::Forbidden(AdminAuthError::ActionNotPermitted {
                action,
            }));
        }
        Ok(())
    }

    /// Check a grant covers a *deployment-wide* answer.
    ///
    /// Every projection here is of the complete deployment: a revision is the
    /// whole desired state, and history and audit are the whole deployment's.
    /// None of them can be narrowed to one tenant without becoming a different
    /// answer, so reading them requires authority over the deployment rather
    /// than trust that each future handler asked its authorizer for a scope wide
    /// enough to justify what came back — which is what made the read paths the
    /// lenient half of the authority model the mutation path enforces. Scoped
    /// projections, which a tenant administrator could be given, do not exist
    /// yet and are #143's to add.
    fn permits_deployment_read(grant: &AdminGrant, action: AdminAction) -> Result<(), AdminError> {
        Self::permits(grant, action)?;
        if grant.scope() != &ResourceScope::Deployment {
            return Err(AdminError::Forbidden(AdminAuthError::ScopeNotPermitted));
        }
        Ok(())
    }

    /// The complete desired state, projected.
    pub async fn desired_state(&self, grant: &AdminGrant) -> Result<StateView, AdminError> {
        Self::permits_deployment_read(grant, AdminAction::ReadState)?;
        let store = self.store()?;
        let revision = store.load_desired_revision().await.map_err(log_store)?;
        StateView::of(revision.as_ref())
    }

    /// One tenant's management catalogue: the enablements in a scope, the aliases
    /// that name them, and why a model is not routable.
    ///
    /// The first *scoped* read on this surface, and scoped in both directions: the
    /// caller names the tenant or project it is asking about, and the grant must
    /// cover it. A deployment-wide grant covers every tenant, a tenant grant its
    /// own tenant, and a project grant its own project — the same containment
    /// [`Self::within_scope`] applies to a mutation, so read authority and write
    /// authority cannot disagree about what a tenant is.
    ///
    /// Unlike [`Self::desired_state`], the answer is bounded by the scope rather
    /// than by the deployment: a tenant administrator cannot enumerate another
    /// tenant's enablements, and cannot learn from this read that one exists.
    pub async fn model_catalogue(
        &self,
        grant: &AdminGrant,
        request: &CatalogueRequest,
    ) -> Result<CatalogueView, AdminError> {
        Self::permits(grant, AdminAction::ReadState)?;
        if !scope_covers(grant.scope(), &request.scope()) {
            return Err(AdminError::Forbidden(AdminAuthError::ScopeNotPermitted));
        }
        let store = self.store()?;
        let revision = store.load_desired_revision().await.map_err(log_store)?;
        CatalogueView::of(revision.as_ref(), request)
    }

    /// A bounded page of revision history, newest first.
    ///
    /// A parent walk rather than a query: see [`crate::admin::reads`]. A parent the
    /// store no longer retains ends the page — retention is expected, and a
    /// truncated history is not an error — while a *requested* start revision that
    /// is not retained is [`AdminError::RevisionNotFound`], because the caller
    /// asked for it by name.
    pub async fn history(
        &self,
        grant: &AdminGrant,
        request: HistoryRequest,
    ) -> Result<RevisionPage, AdminError> {
        Self::permits_deployment_read(grant, AdminAction::ReadHistory)?;
        let store = self.store()?;
        let mut next = match request.start {
            Some(start) => Some(start),
            None => store.desired_revision().await.map_err(log_store)?,
        };
        let mut revisions = Vec::new();
        while let Some(id) = next {
            if revisions.len() >= request.limit.get() as usize {
                break;
            }
            let manifest = match store.load_manifest(id).await {
                Ok(manifest) => manifest,
                // Retention, and only retention, ends the page: an ancestor the
                // store no longer keeps is expected, while an outage or unreadable
                // storage is an operator alert and must not be served as a short
                // page that looks complete. The head or the caller's cursor is
                // never allowed to be missing either — the caller named it.
                Err(ControlPlaneError::RevisionNotFound(_)) if !revisions.is_empty() => break,
                Err(error) => return Err(log_store(error)),
            };
            next = manifest.parent;
            revisions.push(RevisionRecord::of(&manifest));
        }
        let next_start = next
            .filter(|_| revisions.len() >= request.limit.get() as usize)
            .map(|id| id.to_string());
        Ok(RevisionPage {
            revisions,
            limit: request.limit.get(),
            next_start,
        })
    }

    /// A revision's audit trail, bounded by [`AuditPage::MAX_EVENTS`].
    pub async fn audit(
        &self,
        grant: &AdminGrant,
        revision: RevisionId,
    ) -> Result<AuditPage, AdminError> {
        Self::permits_deployment_read(grant, AdminAction::ReadAudit)?;
        let store = self.store()?;
        let events = store.audit_trail(revision).await.map_err(log_store)?;
        Ok(AuditPage::of(revision, &events))
    }

    /// What this replica has converged onto.
    ///
    /// Takes the report rather than reading one, because convergence state is
    /// replica-local and cached: this answers during a control-plane outage, which
    /// is when it is asked. `None` is a replica with no reconciler attached,
    /// which has converged onto nothing and says so.
    pub fn convergence(
        &self,
        grant: &AdminGrant,
        report: Option<&RevisionReport>,
    ) -> Result<ConvergenceResult, AdminError> {
        Self::permits_deployment_read(grant, AdminAction::ReadConvergence)?;
        self.store()?;
        Ok(report.map_or_else(ConvergenceResult::unreconciled, ConvergenceResult::of))
    }

    /// What this replica derives about one scope's models (#148).
    ///
    /// Takes the reader rather than holding one, for the same reason
    /// [`AdminService::convergence`] takes a report: the answer is replica-local
    /// and already in memory, so it reaches no store and survives the outage that
    /// prompted the question. `None` is a replica that derives no view, and says
    /// so rather than answering with an empty catalogue.
    ///
    /// Scoped rather than deployment-wide, and narrowed twice. The grant must
    /// enclose the scope asked about, so a tenant administrator cannot read
    /// another tenant's — or a sibling project's — derived entitlements. And a
    /// caller holding less than deployment authority sees the namespace
    /// projection of each verdict, which keeps the deployment's discovery
    /// machinery out of a tenant's answer.
    ///
    /// The disclosure is decided by [`AvailabilityAuthority`] rather than by the
    /// grant's scope, because they are different questions: this read always
    /// names a tenant, so every grant it produces is tenant-shaped — deciding on
    /// the grant would coarsen the answer for the root operator too, and leave
    /// nobody who could see why discovery or this replica's health refused.
    ///
    /// A project is answered with what it inherits as well as what it overrides:
    /// its enablements are overrides of its tenant's, so reporting only its own
    /// records would tell an operator a project may call nothing whenever it has
    /// overridden nothing.
    pub fn availability(
        &self,
        grant: &AdminGrant,
        scope: &ResourceScope,
        authority: AvailabilityAuthority,
        reader: Option<&dyn AvailabilityReader>,
        now: SystemTime,
    ) -> Result<AvailabilityResult, AdminError> {
        Self::permits(grant, AdminAction::ReadAvailability)?;
        if !grant.scope().contains(scope) {
            return Err(AdminError::Forbidden(AdminAuthError::ScopeNotPermitted));
        }
        self.store()?;
        let Some(reference) = ScopeRef::of(scope) else {
            // Availability is a question about a tenant's models: a
            // deployment-wide answer would be every tenant's entitlements in one
            // document, which is the cross-tenant disclosure the keying exists to
            // prevent.
            return Err(AdminError::RequestInvalid {
                schema: "availability",
                detail: "`tenant`: an availability read must name the tenant it asks about"
                    .to_owned(),
            });
        };
        // Attached but deriving nothing is still deriving nothing: a replica whose
        // snapshot carries no projection says so, rather than answering with an
        // empty list of targets an operator would read as a lost entitlement.
        let Some((index, runtime)) = reader.and_then(AvailabilityReader::read) else {
            return Ok(AvailabilityResult::underived(scope));
        };
        let targets =
            AvailabilityView::new(&index, &runtime).evaluate_inherited_scope(reference, now);
        let status = authority.disclosure();
        Ok(AvailabilityResult::of(scope, status, targets))
    }

    /// Republish a retained revision's complete desired state as a new revision.
    ///
    /// A rollback is not a rewind: nothing is deleted, no revision is reopened,
    /// and the chain keeps moving forward. It is an ordinary mutation whose
    /// candidate happens to be a state that was published before, which is why it
    /// goes through [`AdminService::apply`] and inherits every property that path
    /// has — expected-revision preconditions, idempotent replay, complete
    /// validation, dry run, diff, audit attribution, and one atomic publication.
    ///
    /// The target is hydrated whole rather than diffed onto the head: a revision
    /// is complete, so "the state as of `target`" needs no replay of the
    /// intervening changes and cannot half-apply.
    pub async fn rollback(
        &self,
        grant: &AdminGrant,
        request: &MutationRequest,
        target: RevisionId,
    ) -> Result<MutationOutcome, AdminError> {
        let store = self.store()?;
        if grant.action() != AdminAction::Rollback {
            return Err(AdminError::Forbidden(AdminAuthError::ActionNotPermitted {
                action: grant.action(),
            }));
        }
        let restored = store.load_revision(target).await.map_err(log_store)?;
        let restored = restored.state().clone();
        self.apply(grant, request, &move |state: &mut DesiredState| {
            state.clone_from(&restored);
            Ok(())
        })
        .await
    }

    /// Publish, or rehearse publishing, a complete candidate revision.
    ///
    /// The seven steps in this module's documentation, in order. A dry run stops
    /// after validation and the diff, so it creates no revision, no audit event,
    /// no idempotency record, and no history entry — the fake store's counters are
    /// what a test asserts that on.
    pub async fn apply(
        &self,
        grant: &AdminGrant,
        request: &MutationRequest,
        edit: &dyn DesiredStateEdit,
    ) -> Result<MutationOutcome, AdminError> {
        // 1. Mode, before a backend is looked for.
        let store = self.store()?;

        // 2. Authority: the grant for *this* mutation, at the scope it claims.
        //
        // The verb the request performs, not merely some mutating verb: a
        // rollback grant is not a publication grant, and the service is the place
        // that cannot be bypassed by a handler asking its authorizer for the
        // wrong one. Every refusal below is an authenticated caller reaching past
        // its authority, so each one is written to the denial trail before it is
        // returned — including the ones an authorizer already had a chance to
        // make, because these checks exist precisely for the case where it did
        // not.
        if let Err(error) = Self::permits(grant, AdminAction::for_mutation(request.kind)) {
            self.denied(grant, request, &error).await;
            return Err(error);
        }
        if grant.scope() != &request.scope {
            let error = AdminError::Forbidden(AdminAuthError::ScopeNotPermitted);
            self.denied(grant, request, &error).await;
            return Err(error);
        }

        // 3. The complete desired state the caller built its change on, read once.
        //
        // The base is the *expected* revision, not the head, and the difference
        // only shows when the two disagree. A dry run refuses there and then: it
        // publishes nothing, so no idempotency record can excuse the staleness,
        // and rehearsing against state that is already gone would answer a
        // question nobody asked. An apply carries on to the store, because a
        // retry of a request whose response was lost presents exactly this shape
        // — the original expected revision, now stale, under the original key —
        // and only the store can tell that from a genuine conflict. It checks the
        // key before the revision and replays or refuses atomically; hydrating
        // the base here is what makes the candidate it judges identical to the
        // one the first attempt sent.
        let head = store.desired_revision().await.map_err(log_store)?;
        let expected = request.preconditions.expected;
        if !expected.matches(head) && request.mode().is_dry_run() {
            return Err(AdminError::RevisionConflict {
                expected,
                actual: head,
            });
        }
        let base = match expected {
            ExpectedRevision::Empty => None,
            ExpectedRevision::Exactly(id) => Some(id),
        };
        let current = match base {
            Some(id) => match store.load_revision(id).await {
                Ok(revision) => Some(revision),
                // A base that is both stale and no longer retained is a lost race,
                // not a bogus request: the caller needs the head to re-read from,
                // which a non-retryable 404 would not give it. A missing base that
                // *is* the head is an integrity problem, and stays one.
                Err(ControlPlaneError::RevisionNotFound(_)) if !expected.matches(head) => {
                    return Err(AdminError::RevisionConflict {
                        expected,
                        actual: head,
                    });
                }
                Err(error) => return Err(log_store(error)),
            },
            None => None,
        };
        let empty = DesiredState::new();
        let current_state = current
            .as_ref()
            .map_or(&empty, |revision: &LoadedRevision| revision.state());

        // 4. The complete candidate, and the authority to have changed what it
        // changed rather than only what it said it would.
        let mut candidate_state = current_state.clone();
        edit.edit(&mut candidate_state)?;
        if let Err(error) = Self::within_scope(grant.scope(), current_state, &candidate_state) {
            self.denied(grant, request, &error).await;
            return Err(error);
        }

        let legacy_aliases = legacy_alias_allowlist(current_state, &candidate_state);

        // 5 and 6. Validate the whole candidate, then diff two complete states.
        let mutation = MutationId::new(self.ids.next());
        let submitted_at = SystemTime::now();
        let identity = grant.identity();
        let candidate = RevisionCandidate {
            expected,
            state: candidate_state,
            legacy_aliases,
            mutation: Mutation {
                id: mutation,
                actor: identity.actor(),
                kind: request.kind,
                scope: request.scope.clone(),
                idempotency_key: request.preconditions.idempotency_key.clone(),
                submitted_at,
            },
            audit: AuditEvent {
                id: AuditEventId::new(self.ids.next()),
                mutation,
                actor: identity.actor(),
                kind: request.kind,
                target: None,
                summary: identity.audit_summary(request.summary.as_str()),
                recorded_at: submitted_at,
            },
        };
        // A candidate built on a base that is no longer the head cannot be
        // *judged* invalid: its invalidity may be an artefact of state the
        // caller had not read — a project whose tenant another writer published
        // is the ordinary case. The honest answer is the one the caller can act
        // on, which is "re-read and retry", so staleness outranks invalidity
        // here. A lost-response retry is unaffected: its candidate was valid
        // when it was first built, and is rebuilt from the same base.
        let checksum = match candidate.validated_checksum_for_publication() {
            Ok(checksum) => checksum,
            Err(error) if !expected.matches(head) => {
                debug!(
                    rule = AdminError::from(error).rule(),
                    "an invalid candidate was built on a stale base"
                );
                return Err(AdminError::RevisionConflict {
                    expected,
                    actual: head,
                });
            }
            Err(error) => return Err(error.into()),
        };
        let diff = SemanticDiff::between(Some(current_state), &candidate.state)?;
        let base = base.map(|id| id.to_string());

        // 7. Publish, or stop here.
        if request.mode().is_dry_run() {
            debug!(
                action = grant.action().as_str(),
                breakglass = identity.is_breakglass(),
                added = diff.summary.added,
                removed = diff.summary.removed,
                updated = diff.summary.updated,
                "administrative dry run validated a candidate"
            );
            return Ok(MutationOutcome {
                result: MutationResult::DryRun,
                base,
                checksum: checksum.to_string(),
                mode: WriteMode::DryRun.as_str(),
                diff,
            });
        }
        let manifest = store.publish_revision(candidate).await.map_err(log_store)?;
        // The store replays a repeated key carrying identical state by returning
        // the revision the *first* call published, whose mutation is therefore not
        // this one's. That is the only difference between publishing and
        // replaying, and a caller is told which happened.
        let result = if manifest.mutation == mutation {
            MutationResult::Published {
                revision: manifest.id.to_string(),
            }
        } else {
            MutationResult::Replayed {
                revision: manifest.id.to_string(),
            }
        };
        debug!(
            action = grant.action().as_str(),
            breakglass = identity.is_breakglass(),
            revision = %manifest.id,
            replayed = matches!(result, MutationResult::Replayed { .. }),
            "administrative mutation published"
        );
        Ok(MutationOutcome {
            result,
            base,
            checksum: checksum.to_string(),
            mode: WriteMode::Apply.as_str(),
            diff,
        })
    }
}

/// Translate a store failure and log the part that does not travel.
///
/// The operator detail — which may name a host, a DSN, or a driver internal — is
/// logged here and dropped from the response, so the caller learns the category
/// and the operator learns the cause.
pub(super) fn log_secret(error: crate::backends::secrets::SecretError) -> AdminError {
    let error = AdminError::from_secret(error);
    // Material refusals are deliberately not operational diagnostics: their
    // backend detail is caller-adjacent input and must never become a log field.
    // Secret lifecycle logs below carry only references and ownership metadata;
    // audit attribution remains a separate durable control-plane concern.
    if !matches!(error, AdminError::SecretMaterialRefused { .. })
        && let Some(detail) = error.operator_detail()
    {
        warn!(code = error.code(), detail, "secret-store operation failed");
    }
    error
}

/// Translate a control-plane failure and log the part that does not travel.
///
/// The operator detail — which may name a host, a DSN, or a driver internal — is
/// logged here and dropped from the response, so the caller learns the category
/// and the operator learns the cause.
pub(super) fn log_store(error: ControlPlaneError) -> AdminError {
    let error = AdminError::from_control_plane(error);
    if let Some(detail) = error.operator_detail() {
        warn!(
            code = error.code(),
            detail, "control-plane operation failed"
        );
    }
    error
}
