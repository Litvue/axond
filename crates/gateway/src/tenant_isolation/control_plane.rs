//! The administrative service, over a real journal rather than the oracle.
//!
//! [`crate::admin::tests`] already states the authority contract against the
//! in-memory store: a scoped grant cannot change what it does not own, and cannot
//! read a deployment-wide projection. Those tests assert about *decisions*. What
//! they cannot assert is the consequence an operator actually cares about — that
//! after the refusal, the durable state of the tenant that was reached for is
//! byte-for-byte what it was, the head has not moved, and the refusal the caller
//! holds names nothing of that tenant.
//!
//! So these scenarios mount [`AdminService`] on [`PostgresControlPlane`] and read
//! the database afterwards. Three of them:
//!
//! * a tenant-scoped administrator cannot publish into another tenant, by
//!   claiming its scope or by editing its rows, and nothing durable moves;
//! * a cross-tenant *reference* is refused by validation, and the refusal the
//!   caller receives names its own resource while the operator's detail names
//!   both;
//! * a tenant-scoped administrator is given no deployment-wide projection, while
//!   the same projection read with deployment authority does contain the other
//!   tenant — so the refusal is load-bearing rather than an empty answer.
//!
//! Human, OIDC-issued authority is not yet wired into the stateful runtime
//! ([`crate::admin::runtime`] grants deployment-scoped breakglass authority), so
//! these scenarios construct the grant a tenant-scoped authorizer will hand the
//! service rather than authenticating one. That is the seam the service enforces
//! at; `docs/security/tenant-isolation-evidence.md` records what remains blocked
//! on the runtime half.

use std::sync::Arc;

use super::harness::{Absent, Journal, caller, other, two_tenant_state};
use crate::admin::auth::{AdminAction, AdminGrant, AdminIdentity};
use crate::admin::error::AdminError;
use crate::admin::protocol::{AuditSummary, MutationPreconditions, MutationRequest, WriteMode};
use crate::admin::reads::HistoryRequest;
use crate::admin::service::{AdminService, DesiredStateEdit, MutationResult};
use crate::desired_state::access::DenialPage;
use crate::desired_state::{
    DesiredState, ExpectedRevision, IdempotencyKey, MutationKind, ResourceScope, RevisionId,
    Surface, fixtures,
};

const ISSUER: &str = "https://idp.example";
const SUBJECT: &str = "alice";

fn identity() -> AdminIdentity {
    AdminIdentity::Human {
        issuer: ISSUER.to_owned(),
        subject: SUBJECT.to_owned(),
    }
}

fn grant(action: AdminAction, scope: ResourceScope) -> AdminGrant {
    AdminGrant::granted(identity(), action, scope)
}

fn request(key: &str, expected: ExpectedRevision, scope: ResourceScope) -> MutationRequest {
    MutationRequest {
        preconditions: MutationPreconditions {
            expected,
            idempotency_key: IdempotencyKey::parse(key).expect("a valid key"),
            mode: WriteMode::Apply,
        },
        kind: MutationKind::Update,
        surface: Surface::Tenant,
        scope,
        summary: AuditSummary::parse("publish two tenants").expect("a valid summary"),
    }
}

fn replace_with(state: DesiredState) -> impl DesiredStateEdit {
    move |target: &mut DesiredState| {
        *target = state.clone();
        Ok(())
    }
}

/// The two tenants, published with deployment authority: the state every scenario
/// then fails to reach across.
async fn published(journal: &Journal) -> (AdminService, RevisionId) {
    let service = AdminService::stateful(journal.store());
    let outcome = service
        .apply(
            &grant(AdminAction::Publish, ResourceScope::Deployment),
            &request(
                "two-tenants",
                ExpectedRevision::Empty,
                ResourceScope::Deployment,
            ),
            &replace_with(two_tenant_state()),
        )
        .await
        .expect("a deployment-scoped publication of two tenants");
    let revision = RevisionId::parse(outcome.revision().expect("a published revision"))
        .expect("a revision id");
    (service, revision)
}

/// What the other tenant's durable rows are, so a scenario can state that they
/// did not change.
async fn other_tenants_rows(journal: &Journal) -> Vec<String> {
    journal
        .stored(&format!(
            "SELECT t::text FROM axond_cp_resource_version t WHERE tenant_id = '{}' \
             ORDER BY resource_kind, resource_id, version",
            other()
        ))
        .await
}

/// A tenant-scoped administrator cannot publish into another tenant — not by
/// attributing the mutation to it, and not by editing its rows under its own
/// attribution — and after both refusals the head, the other tenant's rows, and
/// the other tenant's view of its own denial trail are exactly as they were.
#[tokio::test]
async fn a_tenant_scoped_administrator_cannot_publish_into_another_tenant() {
    let Some(journal) = Journal::open().await else {
        return;
    };
    let (service, revision) = published(&journal).await;
    let before = other_tenants_rows(&journal).await;
    assert!(
        !before.is_empty(),
        "the other tenant has no durable rows, so nothing here is protecting anything"
    );
    let absent = Absent::of_the_other_tenant();
    let mine = ResourceScope::Tenant(caller());

    // (a) Claiming the other tenant's scope: refused on the grant, before any
    // state is read.
    let claimed = service
        .apply(
            &grant(AdminAction::Publish, mine.clone()),
            &request(
                "claim-theirs",
                ExpectedRevision::Exactly(revision),
                ResourceScope::Tenant(other()),
            ),
            &replace_with(two_tenant_state()),
        )
        .await
        .expect_err("a grant for one tenant does not publish as another");

    // (b) Editing the other tenant's rows under its own attribution: refused on
    // the *delta*, which is the check a handler cannot talk its way past.
    let mut seized = two_tenant_state();
    seized
        .insert(fixtures::credential(&other(), 15, "seized"))
        .expect("a state that reaches into the other tenant is constructible");
    let reached = service
        .apply(
            &grant(AdminAction::Publish, mine.clone()),
            &request("reach-theirs", ExpectedRevision::Exactly(revision), mine),
            &replace_with(seized),
        )
        .await
        .expect_err("a tenant-scoped grant does not add another tenant's credential");

    for (surface, error) in [
        ("a claimed scope", &claimed),
        ("an out-of-scope edit", &reached),
    ] {
        assert_eq!(error.code(), "admin_forbidden", "{surface}: {error}");
        assert_eq!(error.status(), axum::http::StatusCode::FORBIDDEN);
        let envelope = serde_json::to_string(&error.envelope()).expect("a serialisable envelope");
        absent.assert_absent(&format!("the refusal of {surface}"), &envelope);
    }

    // Nothing durable moved.
    assert_eq!(
        journal.head().await,
        Some(revision),
        "a refused mutation moved the head"
    );
    assert_eq!(
        other_tenants_rows(&journal).await,
        before,
        "a refused mutation changed the other tenant's rows"
    );

    // Both refusals are recorded, each under the scope the caller *claimed*: the
    // probe of another tenant is auditable by that tenant, and the reach out of
    // scope is auditable by the caller's own. Neither trail carries the other's
    // refusal, which is what makes a denial read per tenant meaningful.
    let store = journal.store();
    let filed = |scope: ResourceScope| {
        let store = Arc::clone(&store);
        async move {
            let tenant = match scope {
                ResourceScope::Tenant(tenant) => tenant,
                other => panic!("a denial read is per tenant, not {other:?}"),
            };
            store
                .denials(&DenialPage::for_scope(Some(tenant)), 10)
                .await
                .expect("the denial trail")
        }
    };
    let theirs = filed(ResourceScope::Tenant(other())).await;
    let ours = filed(ResourceScope::Tenant(caller())).await;
    assert_eq!(theirs.len(), 1, "the probe is not filed once: {theirs:?}");
    assert_eq!(ours.len(), 1, "the reach is not filed once: {ours:?}");
    assert_eq!(theirs[0].scope, ResourceScope::Tenant(other()));
    assert_eq!(ours[0].scope, ResourceScope::Tenant(caller()));
    for denial in [&theirs[0], &ours[0]] {
        assert_eq!(denial.surface, Surface::Tenant);
        assert_eq!(denial.actor, identity().actor());
    }
    assert!(
        store
            .denials(&DenialPage::for_scope(None), 10)
            .await
            .expect("the deployment-scoped denial trail")
            .is_empty(),
        "a tenant's refusal was filed as deployment-wide state"
    );
}

/// A cross-tenant *reference* is refused by validation, and the refusal the
/// caller receives names its own resource: the rule, and the resource it is
/// about, without the resource it reached for.
#[tokio::test]
async fn a_cross_tenant_reference_names_the_caller_and_not_what_it_reached_for() {
    let Some(journal) = Journal::open().await else {
        return;
    };
    let (service, revision) = published(&journal).await;
    let before = other_tenants_rows(&journal).await;

    // The caller's own alias, pointed at the other tenant's credential: valid
    // state to *build*, which is why validation and not the type system is what
    // refuses it.
    let theirs = fixtures::credential(&other(), 13, "secondary");
    let mut state = two_tenant_state();
    state
        .insert(fixtures::alias(
            &caller(),
            16,
            "borrowed",
            &[theirs.reference],
        ))
        .expect("an alias that points across a tenant boundary is constructible");

    let error = service
        .apply(
            &grant(AdminAction::Publish, ResourceScope::Deployment),
            &request(
                "borrow-theirs",
                ExpectedRevision::Exactly(revision),
                ResourceScope::Deployment,
            ),
            &replace_with(state),
        )
        .await
        .expect_err("an alias may not depend on another tenant's credential");

    assert_eq!(error.code(), "validation_failed");
    assert_eq!(error.rule(), Some("cross_tenant_reference"));
    assert_eq!(
        error.reference(),
        Some(fixtures::reference(
            crate::desired_state::ResourceKind::Alias,
            16
        )),
        "the refusal names the caller's own alias"
    );
    let envelope = serde_json::to_string(&error.envelope()).expect("a serialisable envelope");
    Absent::of_the_other_tenant().assert_absent("the validation refusal", &envelope);

    // The tripwire: the refusal *was* about the other tenant's credential, and
    // that is in the operator's detail — which is logged, never returned.
    let detail = error
        .operator_detail()
        .expect("a validation refusal carries an operator detail");
    assert!(
        detail.contains(&theirs.reference.id.to_string()),
        "the refusal is not about the credential it was supposed to be about: {detail}"
    );

    assert_eq!(journal.head().await, Some(revision));
    assert_eq!(other_tenants_rows(&journal).await, before);
}

/// A tenant-scoped grant is given none of the deployment-wide projections, and
/// the projection it was refused really does carry the other tenant's state.
#[tokio::test]
async fn a_tenant_scoped_administrator_reads_no_deployment_wide_projection() {
    let Some(journal) = Journal::open().await else {
        return;
    };
    let (service, revision) = published(&journal).await;
    let mine = ResourceScope::Tenant(caller());
    let scoped = |action| grant(action, mine.clone());
    let absent = Absent::of_the_other_tenant();

    for (surface, error) in [
        (
            "the desired state",
            service
                .desired_state(&scoped(AdminAction::ReadState))
                .await
                .expect_err("deployment-wide state needs deployment authority"),
        ),
        (
            "the revision history",
            service
                .history(&scoped(AdminAction::ReadHistory), HistoryRequest::default())
                .await
                .expect_err("deployment-wide history needs deployment authority"),
        ),
        (
            "the audit trail",
            service
                .audit(&scoped(AdminAction::ReadAudit), revision)
                .await
                .expect_err("a deployment-wide audit trail needs deployment authority"),
        ),
    ] {
        assert_eq!(error.code(), "admin_forbidden", "{surface}: {error}");
        assert!(
            matches!(error, AdminError::Forbidden(_)),
            "{surface} was refused for the wrong reason: {error:?}"
        );
        let envelope = serde_json::to_string(&error.envelope()).expect("a serialisable envelope");
        absent.assert_absent(&format!("the refusal of {surface}"), &envelope);
    }

    // Non-vacuity: with deployment authority the same read answers, and what it
    // answers with is exactly what the scoped grant was refused — every tenant's
    // resources in one projection.
    let view = service
        .desired_state(&grant(AdminAction::ReadState, ResourceScope::Deployment))
        .await
        .expect("deployment authority reads the deployment's state");
    let rendered = serde_json::to_string(&view).expect("a serialisable state view");
    for (label, id) in [
        ("tenant id", other().to_string()),
        ("credential id", fixtures::resource_id(13).to_string()),
    ] {
        assert!(
            rendered.contains(&id),
            "the deployment-wide projection does not carry the other tenant's {label}, \
             so refusing it protects nothing"
        );
    }
}

/// A dry run is still a read of the other tenant's state, so it is refused the
/// same way an apply is — and it leaves the head where it was for its own
/// reasons, which is what makes the assertion above meaningful for both modes.
#[tokio::test]
async fn a_rehearsed_cross_tenant_mutation_is_refused_like_a_real_one() {
    let Some(journal) = Journal::open().await else {
        return;
    };
    let (service, revision) = published(&journal).await;
    let mut rehearsal = request(
        "rehearse-theirs",
        ExpectedRevision::Exactly(revision),
        ResourceScope::Tenant(other()),
    );
    rehearsal.preconditions.mode = WriteMode::DryRun;

    let error = service
        .apply(
            &grant(AdminAction::Publish, ResourceScope::Tenant(caller())),
            &rehearsal,
            &replace_with(two_tenant_state()),
        )
        .await
        .expect_err("a rehearsal is not a way to read another tenant's state");
    assert_eq!(error.code(), "admin_forbidden");
    assert_eq!(journal.head().await, Some(revision));

    // And the same rehearsal within the caller's own scope does run, so the
    // refusal above is about the tenant boundary and not about dry runs.
    let outcome = service
        .apply(
            &grant(AdminAction::Publish, ResourceScope::Deployment),
            &{
                let mut own = request(
                    "rehearse-mine",
                    ExpectedRevision::Exactly(revision),
                    ResourceScope::Deployment,
                );
                own.preconditions.mode = WriteMode::DryRun;
                own
            },
            &replace_with(two_tenant_state()),
        )
        .await
        .expect("a rehearsal of unchanged state within authority");
    assert!(
        matches!(outcome.result, MutationResult::DryRun),
        "a dry run published something: {outcome:?}"
    );
    assert_eq!(journal.head().await, Some(revision));
}
