//! Projecting durable workload identities into the request-path snapshot.
//!
//! A workload identity stores only the digest of its one-time `axw1.` key. The
//! digest is enough to rebuild a verifier during convergence, so a replica does
//! not need the control plane — or the original key material — while serving a
//! request. Human identities remain administrative OIDC bindings and are not
//! inference credentials.
//!
//! A workload scoped to a project becomes a caller for that project's projected
//! namespace. A tenant-scoped workload is intentionally not projected here: one
//! presented key cannot select one namespace when a tenant owns several
//! projects, and choosing a default would be a cross-tenant/accounting bug. The
//! same workload remains valid durable administrative state for the future admin
//! authenticator.

use super::compile::{ProjectionError, RevisionProjection};
use crate::config::{Config, ProjectIdentity, ProjectedPrincipal};
use crate::desired_state::access::{Credential, Directory};
use crate::desired_state::{DesiredState, ResourceScope, RevisionId, Tenancy};

/// Adds recoverable project-scoped workload keys to an already projected config.
#[derive(Debug, Clone, Copy, Default)]
pub struct PrincipalProjection;

impl RevisionProjection for PrincipalProjection {
    fn name(&self) -> &'static str {
        "principals"
    }

    fn projects_inbound_principals(&self) -> bool {
        true
    }

    fn project(
        &self,
        bootstrap: &Config,
        state: &DesiredState,
        _source: RevisionId,
    ) -> Result<Config, ProjectionError> {
        let tenancy = Tenancy::of(state).map_err(|error| ProjectionError::Body {
            reference: error.reference(),
            detail: error.to_string(),
        })?;
        let directory = Directory::of(state, &tenancy).map_err(|error| ProjectionError::Body {
            reference: error.reference(),
            detail: error.to_string(),
        })?;
        let mut config = bootstrap.clone();
        config.projected_principals.clear();

        for principal in directory.principals() {
            let Credential::MintedKey {
                digest: Some(digest),
            } = principal.body.credential()
            else {
                // Humans are authenticated by OIDC on the administrative
                // surface, and a revoked workload has no recoverable key.
                continue;
            };
            let ResourceScope::Project { tenant, project } = principal.scope else {
                // A tenant-scoped workload is an administrative principal. It
                // cannot be safely turned into one request-path namespace when
                // the tenant has more than one project.
                continue;
            };
            let identity = ProjectIdentity { tenant, project };
            let Some(namespace) = config
                .namespace
                .iter()
                .find(|namespace| namespace.project == Some(identity))
            else {
                // The enclosing tenancy projection may have withdrawn this
                // tenant, so the workload is retained but serves no traffic.
                continue;
            };
            config.projected_principals.push(ProjectedPrincipal {
                namespace: namespace.id.clone(),
                subject: principal.body.principal().to_string(),
                digest: *digest,
            });
        }
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::super::tenancy::TenancyProjection;
    use super::*;
    use crate::convergence::compile::testing::bootstrap;
    use crate::desired_state::fixtures;
    use crate::desired_state::{DisplayName, ProjectBody, ResourceVersionNumber, Role, Slug};

    fn project_workload_state() -> (DesiredState, String) {
        let key = fixtures::workload_key(0xd0);
        let mut state = fixtures::state();
        state
            .insert(fixtures::workload(
                33,
                "deployer",
                ResourceScope::Project {
                    tenant: fixtures::tenant_id(1),
                    project: fixtures::project_id(2),
                },
                &[Role::Developer],
                Some(&key),
            ))
            .expect("a project-scoped workload is valid");
        (state, key)
    }

    #[test]
    fn a_project_workload_becomes_a_namespace_bound_digest() {
        let (state, key) = project_workload_state();
        let tenancy = TenancyProjection
            .project(&bootstrap(), &state, fixtures::revision_id(3))
            .expect("tenancy projects");
        let config = PrincipalProjection
            .project(&tenancy, &state, fixtures::revision_id(3))
            .expect("principals project");

        assert_eq!(config.projected_principals.len(), 1);
        let projected = &config.projected_principals[0];
        assert_eq!(projected.namespace, "acme/core");
        assert_eq!(projected.subject, fixtures::principal_id(33).to_string());
        assert_eq!(
            projected.digest,
            crate::desired_state::Checksum::of(key.as_bytes())
        );
    }

    #[test]
    fn humans_revoked_workloads_and_tenant_workloads_are_not_inference_keys() {
        let state = fixtures::state_with_directory();
        let tenancy = TenancyProjection
            .project(&bootstrap(), &state, fixtures::revision_id(3))
            .expect("tenancy projects");
        let config = PrincipalProjection
            .project(&tenancy, &state, fixtures::revision_id(3))
            .expect("principals project");
        assert!(config.projected_principals.is_empty());

        let revoked = fixtures::state_with_revoked_workload();
        let tenancy = TenancyProjection
            .project(&bootstrap(), &revoked, fixtures::revision_id(3))
            .expect("tenancy projects");
        let config = PrincipalProjection
            .project(&tenancy, &revoked, fixtures::revision_id(3))
            .expect("principals project");
        assert!(config.projected_principals.is_empty());
    }

    #[test]
    fn a_projected_namespace_rename_keeps_the_principal_on_project_identity() {
        let (mut state, key) = project_workload_state();
        // Replace the project row while retaining its durable project id.
        state
            .supersede(
                ProjectBody::new(
                    fixtures::project_id(2),
                    fixtures::tenant_id(1),
                    DisplayName::parse("Renamed").expect("a display name"),
                )
                .version_at(
                    Slug::parse("renamed").expect("a slug"),
                    ResourceVersionNumber::FIRST.next(),
                ),
            )
            .expect("the project can be renamed");
        let tenancy = TenancyProjection
            .project(&bootstrap(), &state, fixtures::revision_id(3))
            .expect("tenancy projects");
        let config = PrincipalProjection
            .project(&tenancy, &state, fixtures::revision_id(3))
            .expect("principals project");
        assert_eq!(config.projected_principals[0].namespace, "acme/renamed");
        assert_eq!(
            config.projected_principals[0].digest,
            crate::desired_state::Checksum::of(key.as_bytes())
        );
    }
}
