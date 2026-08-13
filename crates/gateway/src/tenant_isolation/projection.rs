//! What a converging replica makes of two tenants' durable state.
//!
//! [`crate::convergence::tenancy`] asserts the projection against hand-built
//! state. These scenarios assert it against state that made a *round trip*:
//! published into PostgreSQL through the administrative path, hydrated back the
//! way a replica hydrates it, then projected. That is the composition an operator
//! is asking about when they ask whether one tenant's namespace can reach
//! another's — storage, hydration and projection, not the projection alone.
//!
//! Two properties:
//!
//! * each tenant's project becomes its own tenant-qualified namespace, carrying
//!   its own durable identity, with platform fallback off;
//! * the presence of the other tenant changes nothing about a tenant's own
//!   namespace, asserted by projecting the same tenant with and without a
//!   neighbour and comparing.
//!
//! # Where this stops
//!
//! Projection and the compiled-config boot gate, not the whole compilation:
//! resolving the typed credentials this state declares needs a `SecretStore`, and
//! what reaches a provider is asserted end to end over a booted gateway in
//! `tests/tenant_isolation.rs` and swept for material in
//! [`crate::secret_redaction`]. The remaining gap — a *served request* routed
//! through a projected namespace — is the runtime slice's, and is recorded as
//! blocked in `docs/security/tenant-isolation-evidence.md` rather than asserted
//! here against machinery that does not exist yet.

use std::collections::BTreeMap;

use super::harness::{Journal, caller, other};
use crate::config::{Config, Namespace};
use crate::convergence::compile::RevisionProjection;
use crate::convergence::tenancy::TenancyProjection;
use crate::desired_state::{DesiredState, fixtures};

/// The projected namespaces, by id.
fn namespaces(config: &Config) -> BTreeMap<&str, &Namespace> {
    config
        .namespace
        .iter()
        .map(|namespace| (namespace.id.as_str(), namespace))
        .collect()
}

fn project(state: &DesiredState) -> Config {
    let config = TenancyProjection
        .project(&crate::convergence::compile::testing::bootstrap(), state)
        .expect("two tenants project onto one deployment");
    config
        .validate_compiled()
        .expect("a projected config passes the gate a boot passes");
    config
}

/// Each tenant's project is its own namespace, named beyond its tenant, carrying
/// its own durable identity, and borrowing nothing from the platform.
#[tokio::test]
async fn each_tenants_project_is_its_own_namespace_and_borrows_nothing() {
    let Some(journal) = Journal::open().await else {
        return;
    };
    journal.publish_two_tenants().await;
    let revision = journal.hydrated().await;
    let config = project(revision.state());
    let namespaces = namespaces(&config);

    assert_eq!(
        namespaces.keys().copied().collect::<Vec<_>>(),
        vec!["acme/core", "globex/core", "platform"],
        "two tenants' projects did not become two distinct namespaces"
    );

    for (id, tenant, project) in [
        ("acme/core", caller(), fixtures::project_id(2)),
        ("globex/core", other(), fixtures::project_id(12)),
    ] {
        let namespace = namespaces[id];
        let identity = namespace
            .project
            .as_ref()
            .expect("a projected namespace carries the durable object it is");
        assert_eq!(identity.tenant, tenant, "{id} is owned by the wrong tenant");
        assert_eq!(identity.project, project, "{id} is the wrong project");
        assert!(
            !namespace.allow_platform_fallback,
            "{id} may borrow the platform's credentials without being told to"
        );
        assert!(
            !namespace.default,
            "{id} was promoted to the deployment default"
        );
    }

    // The bootstrap's own namespace is untouched: a projection fills sections, it
    // does not rewrite the deployment's local facts.
    assert!(namespaces["platform"].default);
    assert!(namespaces["platform"].project.is_none());
}

/// A tenant's namespace does not depend on who else is in the deployment.
///
/// The same tenant is projected twice — alone, and beside a neighbour — and the
/// namespace it becomes is identical. Without this, "no cross-tenant influence"
/// would be a claim about the projection's *code* rather than about its output:
/// an ordering or a shared-name bug would show up exactly here.
#[tokio::test]
async fn a_neighbour_changes_nothing_about_a_tenants_own_namespace() {
    let Some(journal) = Journal::open().await else {
        return;
    };
    journal.publish_two_tenants().await;
    let together = project(journal.hydrated().await.state());

    let alone = project(&fixtures::state_with_directory());
    assert_eq!(
        namespaces(&alone).keys().copied().collect::<Vec<_>>(),
        vec!["acme/core", "platform"],
        "the single-tenant projection is not the comparison it is meant to be"
    );

    assert_eq!(
        format!("{:?}", namespaces(&alone)["acme/core"]),
        format!("{:?}", namespaces(&together)["acme/core"]),
        "a second tenant changed what the first tenant's namespace is"
    );
}
