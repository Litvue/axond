//! What each tenant's own state resolves to: its credentials, its models, its
//! policy — read from a revision that carries another tenant's too.
//!
//! The typed catalogues ([`Credentials`], [`Models`], [`PolicySet`]) are what
//! every later projection reads a revision through, so a lookup in one of them
//! that answered a tenant's question with its neighbour's row would put another
//! tenant's credential in front of a provider, another tenant's model in a
//! catalogue response, or another tenant's budget on a request — without any
//! authorization check having failed. The interesting fixture is therefore
//! deliberately confusable: both tenants enable *the same offering*, from the same
//! deployment-wide snapshot, with a credential of the same shape
//! ([`two_tenant_catalogue_state`]).
//!
//! Read from PostgreSQL rather than from the fixture, because the round trip is
//! part of the claim: a body that lost its owner in storage would resolve exactly
//! as wrongly as a lookup that ignored one.
//!
//! # Where this stops
//!
//! These are the durable projections. What a *served request* is given is the
//! runtime's, asserted over a booted gateway with fake providers in
//! `tests/tenant_isolation.rs`; the remaining gap between the two — a request
//! routed through a namespace projected from this state — is recorded in
//! `docs/security/tenant-isolation-evidence.md`.

use super::harness::{Absent, Journal, MODEL, caller, other, two_tenant_catalogue_state};
use crate::desired_state::{
    Credentials, ExpectedRevision, LoadedRevision, ModelOwner, Models, PolicyScope, PolicySet,
    SecretOwner, fixtures,
};

/// The two tenants and their catalogues, published and hydrated back.
async fn hydrated(journal: &Journal) -> LoadedRevision {
    journal
        .publish(
            "two-catalogues",
            ExpectedRevision::Empty,
            two_tenant_catalogue_state(),
        )
        .await
        .expect("two tenants with catalogues of their own publish");
    journal.hydrated().await
}

/// A tenant's credentials are its own, and the owner a secret must be resolved as
/// is taken from the revision rather than from whoever is asking.
#[tokio::test]
async fn a_tenants_credentials_are_never_another_tenants() {
    let Some(journal) = Journal::open().await else {
        return;
    };
    let revision = hydrated(&journal).await;
    let credentials = Credentials::of(revision.state()).expect("the revision's credentials");

    let mine: Vec<_> = credentials
        .of_owner(SecretOwner::tenant(caller()))
        .collect();
    let theirs: Vec<_> = credentials.of_owner(SecretOwner::tenant(other())).collect();
    assert_eq!(mine.len(), 1, "the caller's credentials: {mine:?}");
    assert_eq!(
        theirs.len(),
        1,
        "the other tenant's credentials: {theirs:?}"
    );
    assert_ne!(
        mine[0].reference, theirs[0].reference,
        "both tenants resolved to one credential"
    );

    // Nothing of the other tenant is reachable through the caller's own view of
    // its credentials — not its credential id, and not the secret that credential
    // would have been resolved against.
    Absent::of_the_other_tenant()
        .assert_absent("the caller's own credentials", &format!("{mine:?}"));

    // And the reverse lookup a resolver uses attributes each secret to the tenant
    // that declared it, which is what stops a caller naming a reference from being
    // handed material resolved as somebody else.
    assert_eq!(
        credentials.owner_of(fixtures::secret_id(13)),
        Some(SecretOwner::tenant(other())),
        "the other tenant's secret is not attributed to it"
    );
    assert_eq!(
        credentials.owner_of(fixtures::secret_id(3)),
        Some(SecretOwner::tenant(caller())),
        "the caller's own secret is not attributed to it"
    );
}

/// Two tenants enabling the same offering get two enablements, and each tenant's
/// aliases resolve within its own project.
#[tokio::test]
async fn the_same_offering_enabled_twice_is_two_tenants_models() {
    let Some(journal) = Journal::open().await else {
        return;
    };
    let revision = hydrated(&journal).await;
    let models = Models::of(revision.state()).expect("the revision's models");
    let offering = fixtures::offering_id(MODEL);

    let mine = models
        .default_for(caller(), offering)
        .expect("the caller enabled the offering for itself");
    let theirs = models
        .default_for(other(), offering)
        .expect("the other tenant enabled the same offering for itself");
    assert_ne!(
        mine.reference, theirs.reference,
        "one enablement is answering for two tenants"
    );
    assert_eq!(mine.body.owner(), ModelOwner::tenant(caller()));
    assert_eq!(theirs.body.owner(), ModelOwner::tenant(other()));

    // What applies inside a project is its tenant's, whichever project of
    // whichever tenant is asked about.
    for (label, tenant, project, expected) in [
        ("its own project", caller(), fixtures::project_id(2), mine),
        (
            "the other tenant's project",
            other(),
            fixtures::project_id(12),
            theirs,
        ),
    ] {
        assert_eq!(
            models
                .effective_for(tenant, project, offering)
                .map(|enablement| enablement.reference),
            Some(expected.reference),
            "{label} resolved to the wrong tenant's enablement"
        );
    }

    // A tenant asked about a project it does not own is never given that project's
    // tenant's row: it falls back to its own default, or to nothing.
    let crossed = models.effective_for(caller(), fixtures::project_id(12), offering);
    assert_ne!(
        crossed.map(|enablement| enablement.reference),
        Some(theirs.reference),
        "naming another tenant's project reached that tenant's enablement"
    );

    // Aliases resolve within one project, and each one's targets are its own
    // tenant's enablements.
    let ours: Vec<_> = models.aliases_of(fixtures::project_id(2)).collect();
    assert_eq!(ours.len(), 1, "the caller's aliases: {ours:?}");
    assert_eq!(ours[0].slug.as_str(), "quick");
    assert_eq!(ours[0].body.tenant(), caller());
    assert_eq!(
        ours[0]
            .body
            .targets()
            .iter()
            .map(|target| target.reference())
            .collect::<Vec<_>>(),
        vec![mine.reference],
        "the caller's alias resolves somewhere other than its own enablement"
    );
    Absent::of_the_other_tenant().assert_absent("the caller's own aliases", &format!("{ours:?}"));

    let theirs: Vec<_> = models.aliases_of(fixtures::project_id(12)).collect();
    assert_eq!(theirs.len(), 1, "the other tenant's aliases: {theirs:?}");
    assert_eq!(theirs[0].body.tenant(), other());
}

/// A tenant's budget is governed by its own policy document, and a project with
/// none of its own falls back to its *tenant's* rather than to whatever else the
/// revision carries.
#[tokio::test]
async fn a_tenants_policy_governs_only_its_own_scopes() {
    let Some(journal) = Journal::open().await else {
        return;
    };
    let revision = hydrated(&journal).await;
    let policies = PolicySet::of(revision.state()).expect("the revision's policies");

    let mine = policies
        .document(PolicyScope::Tenant(caller()))
        .expect("the caller published a policy");
    let theirs = policies
        .document(PolicyScope::Tenant(other()))
        .expect("the other tenant published one too");
    assert_ne!(
        mine.reference, theirs.reference,
        "one document is governing two tenants"
    );

    for (label, scope, expected) in [
        (
            "the caller's own tenant",
            PolicyScope::Tenant(caller()),
            mine,
        ),
        (
            "the caller's own project",
            PolicyScope::Project {
                tenant: caller(),
                project: fixtures::project_id(2),
            },
            mine,
        ),
        (
            "the other tenant's project",
            PolicyScope::Project {
                tenant: other(),
                project: fixtures::project_id(12),
            },
            theirs,
        ),
    ] {
        assert_eq!(
            policies.effective(scope).map(|document| document.reference),
            Some(expected.reference),
            "{label} is governed by the wrong tenant's policy"
        );
    }

    // Fallback climbs to the *named* tenant, so a project id belonging to another
    // tenant does not carry that tenant's document across.
    assert_eq!(
        policies
            .effective(PolicyScope::Project {
                tenant: caller(),
                project: fixtures::project_id(12),
            })
            .map(|document| document.reference),
        Some(mine.reference),
        "naming another tenant's project reached that tenant's policy"
    );
    Absent::of_the_other_tenant()
        .assert_absent("the policy governing the caller", &format!("{mine:?}"));
}
