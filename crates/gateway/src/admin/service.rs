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
//!    because it has no grant to pass.
//! 3. **Read the complete desired state.** The head revision is read once and
//!    hydrated whole. There is no partial read and no diff to apply: a revision
//!    *is* the complete state, so a candidate is built from state that was
//!    actually published rather than from the caller's idea of it.
//! 4. **Build a complete candidate.** The handler's [`DesiredStateEdit`] rewrites
//!    that state. What comes out is the full desired state of the deployment, not
//!    a patch — which is what makes the next step meaningful.
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

use super::auth::{AdminAction, AdminAuthError, AdminGrant};
use super::diff::SemanticDiff;
use super::error::AdminError;
use super::protocol::{MutationRequest, WriteMode};
use super::reads::{
    AuditPage, ConvergenceResult, HistoryRequest, RevisionPage, RevisionRecord, StateView,
};
use crate::backends::control_plane::{ControlPlaneError, ControlPlaneStore};
use crate::config::Mode;
use crate::convergence::RevisionReport;
use crate::desired_state::{
    AuditEvent, AuditEventId, DesiredState, ExpectedRevision, LoadedRevision, Mutation, MutationId,
    RevisionCandidate, RevisionId, Uuid7Generator, ValidationError,
};

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

/// The `/admin/v1` service over a [`ControlPlaneStore`].
pub struct AdminService {
    /// `None` in stateless mode — and there is no other way to hold `None`, so a
    /// stateless service cannot reach a backend it does not have.
    store: Option<Arc<dyn ControlPlaneStore>>,
    ids: Uuid7Generator,
}

impl AdminService {
    /// The service a stateless deployment runs: every operation is
    /// [`AdminError::StatefulModeRequired`].
    pub fn stateless() -> Self {
        Self {
            store: None,
            ids: Uuid7Generator::new(),
        }
    }

    pub fn stateful(store: Arc<dyn ControlPlaneStore>) -> Self {
        Self {
            store: Some(store),
            ids: Uuid7Generator::new(),
        }
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
    fn store(&self) -> Result<&Arc<dyn ControlPlaneStore>, AdminError> {
        self.store.as_ref().ok_or(AdminError::StatefulModeRequired)
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

    /// The complete desired state, projected.
    pub async fn desired_state(&self, grant: &AdminGrant) -> Result<StateView, AdminError> {
        Self::permits(grant, AdminAction::ReadState)?;
        let store = self.store()?;
        let revision = store.load_desired_revision().await.map_err(log_store)?;
        StateView::of(revision.as_ref())
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
        Self::permits(grant, AdminAction::ReadHistory)?;
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
        Self::permits(grant, AdminAction::ReadAudit)?;
        let store = self.store()?;
        let events = store.audit_trail(revision).await.map_err(log_store)?;
        Ok(AuditPage::of(revision, &events))
    }

    /// What this replica has converged onto.
    ///
    /// Takes the report rather than reading one, because convergence state is
    /// replica-local and cached: this answers during a control-plane outage, which
    /// is when it is asked.
    pub fn convergence(
        &self,
        grant: &AdminGrant,
        report: &RevisionReport,
    ) -> Result<ConvergenceResult, AdminError> {
        Self::permits(grant, AdminAction::ReadConvergence)?;
        self.store()?;
        Ok(ConvergenceResult::of(report))
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

        // 2. Authority: a mutating grant, for the scope this mutation claims.
        if !grant.action().mutates() {
            return Err(AdminError::Forbidden(AdminAuthError::ActionNotPermitted {
                action: grant.action(),
            }));
        }
        if grant.scope() != &request.scope {
            return Err(AdminError::Forbidden(AdminAuthError::ScopeNotPermitted));
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
            Some(id) => Some(store.load_revision(id).await.map_err(log_store)?),
            None => None,
        };
        let empty = DesiredState::new();
        let current_state = current
            .as_ref()
            .map_or(&empty, |revision: &LoadedRevision| revision.state());

        // 4. The complete candidate.
        let mut candidate_state = current_state.clone();
        edit.edit(&mut candidate_state)?;

        // 5 and 6. Validate the whole candidate, then diff two complete states.
        let mutation = MutationId::new(self.ids.next());
        let submitted_at = SystemTime::now();
        let identity = grant.identity();
        let candidate = RevisionCandidate {
            expected,
            state: candidate_state,
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
        let checksum = candidate.validated_checksum()?;
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
fn log_store(error: ControlPlaneError) -> AdminError {
    let error = AdminError::from_control_plane(error);
    if let Some(detail) = error.operator_detail() {
        warn!(
            code = error.code(),
            detail, "control-plane operation failed"
        );
    }
    error
}
