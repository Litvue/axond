//! Projecting a revision's policy documents onto the namespaces they govern (#150).
//!
//! [`TenancyProjection`](super::tenancy::TenancyProjection) turns projects into
//! namespaces; this projection decides *what governs* each of them. It runs after
//! an inner projection rather than beside it, because a policy document is
//! attached to a namespace and there is no namespace to attach it to until
//! tenancy has produced one.
//!
//! # What it attaches, and what it refuses to attach
//!
//! A namespace carrying a [`ProjectIdentity`](crate::config::ProjectIdentity) is
//! control-plane-owned, so its limits come from the control plane: the document
//! published for its project, or — whole, never field-by-field — its tenant's
//! ([`PolicySet::effective`]). A namespace the *file* declared keeps the file's
//! limits, because a bootstrap namespace has no tenant a document could name and
//! silently governing it from elsewhere would move a deployment's own limits
//! without an edit.
//!
//! Each attachment carries the
//! [`PolicyGeneration`](crate::desired_state::policy::PolicyGeneration) the
//! document is enforced
//! under, which is `(scope, epoch, source revision, content checksum)`. That is
//! what makes a rollback expressible: republishing yesterday's values under a new
//! epoch is a new generation with old content, and the holds granted under the
//! superseded one keep their own terms until they drain (see
//! [`crate::policy`]).
//!
//! # Values only
//!
//! Nothing here touches a backend, a DSN, a key prefix, a table, or an
//! unavailability stance: those are bootstrap-owned
//! ([`BOOTSTRAP_OWNED_FIELDS`](crate::desired_state::policy::BOOTSTRAP_OWNED_FIELDS)),
//! and a publication that could change them would be a publication that could
//! repoint a ledger. The projection produces *values*; whether this replica may
//! start enforcing them is [`crate::policy::PolicyRuntime::plan`]'s decision, made
//! against the holds it has already granted.

use super::compile::{ProjectionError, RevisionProjection};
use crate::config::{Config, NamespacePolicy};
use crate::desired_state::policy::{PolicyScope, PolicySet};
use crate::desired_state::{DesiredState, RevisionId};

/// Attaches published policy to projected namespaces, after `inner` has produced
/// them.
#[derive(Debug, Clone, Copy, Default)]
pub struct PolicyProjection<P> {
    inner: P,
}

impl<P: RevisionProjection> PolicyProjection<P> {
    /// Run `inner` first, then attach policy to what it projected.
    pub const fn over(inner: P) -> Self {
        Self { inner }
    }
}

impl<P: RevisionProjection> RevisionProjection for PolicyProjection<P> {
    fn name(&self) -> &'static str {
        "policy"
    }

    fn projects_inbound_principals(&self) -> bool {
        self.inner.projects_inbound_principals()
    }

    fn project(
        &self,
        bootstrap: &Config,
        state: &DesiredState,
        source: RevisionId,
    ) -> Result<Config, ProjectionError> {
        let mut config = self.inner.project(bootstrap, state, source)?;
        let policies = PolicySet::of(state).map_err(|error| ProjectionError::Body {
            reference: error.reference(),
            detail: error.to_string(),
        })?;
        if policies.documents().len() == 0 {
            return Ok(config);
        }
        for namespace in &mut config.namespace {
            // A file-declared namespace has no identity, so no document names it.
            let Some(identity) = namespace.project else {
                continue;
            };
            let scope = PolicyScope::Project {
                tenant: identity.tenant,
                project: identity.project,
            };
            let Some(document) = policies.effective(scope) else {
                continue;
            };
            let generation = document.body.generation(source);
            namespace.policy = Some(NamespacePolicy {
                body: document.body.clone(),
                // Stamped with the revision that carried it, so two replicas
                // compiling this revision name the same generation and a fence
                // can tell a stale writer from a forked one.
                generation,
            });
        }
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::super::compile::testing::{bootstrap, revision};
    use super::super::principals::PrincipalProjection;
    use super::super::tenancy::TenancyProjection;
    use super::*;
    use crate::desired_state::fixtures::{project_id, revision_id, state, tenant_id};
    use crate::desired_state::policy::PolicyBody;
    use crate::desired_state::{DesiredState, Slug};
    use crate::policy::fixtures::body;

    fn with_policy(body: PolicyBody) -> DesiredState {
        let mut state = state();
        state
            .insert(body.version(Slug::parse("policy").expect("a valid slug")))
            .expect("a policy resource");
        state
    }

    fn projection() -> PolicyProjection<TenancyProjection> {
        PolicyProjection::over(TenancyProjection)
    }

    #[test]
    fn policy_wrapper_preserves_inner_inbound_principal_capability() {
        assert!(
            PolicyProjection::over(PrincipalProjection).projects_inbound_principals(),
            "the production policy wrapper must not hide the principal projection"
        );
        assert!(!projection().projects_inbound_principals());
    }

    fn policy_of<'a>(config: &'a Config, id: &str) -> Option<&'a NamespacePolicy> {
        config
            .namespace
            .iter()
            .find(|namespace| namespace.id == id)
            .expect("the namespace is projected")
            .policy
            .as_ref()
    }

    #[test]
    fn a_project_document_governs_the_namespace_that_project_projects_to() {
        let scope = PolicyScope::Project {
            tenant: tenant_id(1),
            project: project_id(2),
        };
        let config = projection()
            .project(
                &bootstrap(),
                &with_policy(body(scope, 1, 9_000)),
                revision_id(4),
            )
            .expect("the revision projects");
        let attached = policy_of(&config, "acme/core").expect("the project is governed");
        assert_eq!(attached.body.budget().subject_limit_microdollars(), 9_000);
        assert_eq!(attached.generation.epoch().get(), 1);
        assert_eq!(attached.generation.source(), revision_id(4));
    }

    /// Whole-document fallback: a project with no document of its own is governed
    /// by its tenant's, exactly as published rather than merged with anything.
    #[test]
    fn a_tenant_document_governs_a_project_that_publishes_none_of_its_own() {
        let config = projection()
            .project(
                &bootstrap(),
                &with_policy(body(PolicyScope::Tenant(tenant_id(1)), 2, 4_000)),
                revision_id(4),
            )
            .expect("the revision projects");
        let attached = policy_of(&config, "acme/core").expect("the tenant's policy governs");
        assert_eq!(attached.body.budget().subject_limit_microdollars(), 4_000);
        assert_eq!(
            attached.generation.scope(),
            PolicyScope::Tenant(tenant_id(1))
        );
    }

    /// The file's namespaces keep the file's limits: no document names them, and
    /// inheriting one would move a deployment's own limits without an edit.
    #[test]
    fn a_bootstrap_namespace_is_not_governed_by_any_published_document() {
        let config = projection()
            .project(
                &bootstrap(),
                &with_policy(body(PolicyScope::Tenant(tenant_id(1)), 1, 4_000)),
                revision_id(4),
            )
            .expect("the revision projects");
        assert!(policy_of(&config, "platform").is_none());
    }

    #[test]
    fn a_revision_publishing_no_policy_leaves_every_namespace_on_the_bootstrap_limits() {
        let config = projection()
            .project(&bootstrap(), revision().state(), revision_id(4))
            .expect("the revision projects");
        assert!(
            config
                .namespace
                .iter()
                .all(|namespace| namespace.policy.is_none())
        );
    }
}
