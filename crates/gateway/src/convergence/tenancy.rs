//! Projecting a revision's tenancy onto a servable configuration (#191).
//!
//! This is the first real [`RevisionProjection`]: it reads the one body schema
//! the domain knows ([`crate::desired_state::tenancy`]) and fills the one config
//! section that boundary owns — `[[namespace]]`. Every other section a stateful
//! deployment will eventually own (providers, credentials, models, prices,
//! policies) is left exactly as the bootstrap config had it, because those bodies
//! belong to later slices and a projection that guessed at them would have to be
//! unshipped.
//!
//! # A project *is* a namespace, and it is named beyond its tenant
//!
//! The runtime's tenancy boundary is the namespace (ADR 0003): keys bind to one,
//! credential pools are per `(namespace, provider)`, budgets are charged against
//! it. A project is that boundary made durable, so a project projects to a
//! namespace and nothing else changes about how a request is served.
//!
//! What does change is the *name*. A project slug is unique within its tenant and
//! only within it — two tenants may both have `core` — while a config namespace id
//! is deployment-global. Flattening the slug would silently merge two tenants'
//! projects into one budget, one credential pool, and one key binding. So the
//! projected id is the tenant-qualified `acme/core`
//! ([`QualifiedProject`](crate::desired_state::QualifiedProject)), which is
//! unambiguous because `/` is not a legal [`Slug`](crate::desired_state::Slug)
//! character, and reversible for the same reason.
//!
//! # A name is not an identity
//!
//! Both halves of that name are renameable, and per-namespace durable state —
//! budgets, credential pools, gateway-key bindings — must survive a rename. So a
//! projected namespace carries what it *is* as well as what it is called:
//! [`Namespace::project`] holds the immutable [`ProjectIdentity`], and the id is
//! the name a request uses to reach it. Renaming `acme` to `acme-inc` changes the
//! name callers say and the labels an operator reads, and changes nothing about
//! which durable object was charged: the runtime slice keys per-namespace state on
//! the identity, and treats the id as display and routing only. A file-declared
//! namespace has no identity to carry (`None`) and keeps keying on its id, which
//! is immutable for the same reason the file is: nobody renames it without an
//! edit and a boot.
//!
//! The projection's own charset guarantee is checked where every compiled config
//! is: [`Config::validate_compiled`] holds a generated id to one slug, or two
//! joined by `/`, and refuses a repeated id outright.
//!
//! # What the projection refuses
//!
//! - a tenancy body this build cannot read, or an ownership inconsistency
//!   ([`ProjectionError::Body`]) — the same reading [`DesiredState::validate`]
//!   does, so a revision cannot pass publication and fail here for a different
//!   reason;
//! - a projected namespace whose id a bootstrap namespace already claims
//!   ([`ProjectionError::Incomplete`]): merging them would put durable state and
//!   file-owned state on one name, and dropping either silently is worse;
//! - a bootstrap configuration that declares no default namespace
//!   ([`ProjectionError::Incomplete`] again): a projected project is never
//!   promoted to the deployment default, so there would be nothing to serve a
//!   request that names no namespace. Which project a deployment defaults to is a
//!   decision the later runtime slice makes from desired state; until then a
//!   stateful bootstrap declares its own default, and a refusal that says so is
//!   worth more than a config that fails the boot gate one step later.
//!
//! # Still not wired to `serve`
//!
//! Nothing constructs this in `serve`, and this slice does not change that: a
//! deployment whose aliases, providers, and credentials are all still file-owned
//! gains nothing from projecting tenants, and the sections that would make it
//! gain something are the later slices'. What exists here is the seam, exercised
//! end to end through [`RevisionCompiler`](super::compile::RevisionCompiler).
//!
//! Two things the runtime slice owns, recorded here because this module is what
//! creates the need for them:
//!
//! - **selecting a default namespace** from desired state, which is why a
//!   bootstrap without one is refused above rather than given one;
//! - **`/` where an id is *used*.** A qualified id is the first namespace id no
//!   `axond.toml` could have written. Its charset is now gated for every compiled
//!   config, but a charset rule only says the id is well formed; before a
//!   projected id reaches a request path, `/` still has to be checked everywhere
//!   an id is consumed — metric and trace label values, Redis and Postgres key
//!   composition, gateway-key bindings — and per-namespace durable state has to
//!   key on [`Namespace::project`] rather than on the name.

use std::collections::BTreeSet;

use super::compile::{ProjectionError, RevisionProjection};
use crate::config::{Config, Namespace, ProjectIdentity};
use crate::desired_state::{DesiredState, RevisionId, Tenancy};

/// Projects a revision's projects onto `[[namespace]]`, leaving every
/// bootstrap-owned section alone.
#[derive(Debug, Clone, Copy, Default)]
pub struct TenancyProjection;

impl RevisionProjection for TenancyProjection {
    fn name(&self) -> &'static str {
        "tenancy"
    }

    /// The revision is not read here: a namespace's *name* and identity come
    /// from the tenancy body alone, and only policy generations are revision-
    /// stamped (see [`super::policy::PolicyProjection`]).
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
        let mut config = bootstrap.clone();
        // Refused deliberately, and *before* any project is projected: a
        // deployment needs one namespace to serve a request that names none, and
        // this projection has no authority to nominate one. Promoting a project —
        // the first, the lowest id, the only one — would make an unrelated
        // publication silently move where unnamed traffic lands. Selecting a
        // default from desired state is the later runtime slice's job, so until it
        // exists a stateful bootstrap that declares no default is refused here,
        // naming the missing section, rather than passed on to fail the boot gate
        // as a generic invalid configuration.
        if !bootstrap
            .namespace
            .iter()
            .any(|namespace| namespace.default)
        {
            return Err(ProjectionError::Incomplete {
                detail: "the bootstrap configuration declares no default namespace, and \
                         projecting a project cannot make one the default: a published project \
                         must not silently become where unnamed traffic lands. Declare a \
                         default `[[namespace]]` in the bootstrap configuration; selecting a \
                         default from desired state is not part of this slice"
                    .to_owned(),
            });
        }
        let declared: BTreeSet<String> = bootstrap
            .namespace
            .iter()
            .map(|namespace| namespace.id.clone())
            .collect();

        // Ordered by project id, so two replicas compiling one revision produce
        // the same configuration and not merely an equivalent one.
        for project in tenancy.projects() {
            // A suspended tenant serves nothing. Projecting its projects anyway
            // would make `lifecycle = 'disabled'` a label rather than a decision,
            // and the one thing an operator disables a tenant to stop is its
            // traffic. Its rows are retained — the transition is not an erasure —
            // so this is where the retention stops being servable.
            if !tenancy.is_served(project.body.tenant()) {
                continue;
            }
            let id = tenancy
                .qualified_name(project.body.project())
                .ok_or_else(|| ProjectionError::Body {
                    reference: project.reference,
                    detail: format!("project {} has no declared tenant", project.body.project()),
                })?
                .to_string();
            if declared.contains(&id) {
                return Err(ProjectionError::Incomplete {
                    detail: format!(
                        "namespace `{id}` is declared by the bootstrap configuration and by \
                         {}; one name cannot be owned by both a file and the control plane",
                        project.reference
                    ),
                });
            }
            config.namespace.push(Namespace {
                id,
                // The default namespace is a process-local boot fact: a request
                // that names no namespace is served by whatever the file says,
                // and publishing a revision does not move that target.
                default: false,
                // "Bring your own key" means exactly that (ADR 0003): a projected
                // project borrows nothing until a credential slice gives it its
                // own.
                allow_platform_fallback: false,
                // The id above is a *name*, and a name is renameable. What the
                // namespace is, and what durable per-namespace state therefore
                // keys on, is this pair.
                project: Some(ProjectIdentity {
                    tenant: project.body.tenant(),
                    project: project.body.project(),
                }),
                // Policy is the policy projection's to fill (#150): a namespace
                // arrives here governed by nothing, and that projection decides
                // what governs it.
                policy: None,
            });
        }
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::super::compile::testing::{bootstrap, env, revision};
    use super::super::compile::{CandidateCompiler, RevisionCompiler};
    use super::*;
    use crate::desired_state::fixtures::{
        alias, candidate, project, project_id, revision_id, state, tenant, tenant_body, tenant_id,
    };
    use crate::desired_state::{
        DesiredState, ExpectedRevision, LoadedRevision, ProjectBody, QualifiedProject,
        ResourceScope, ResourceVersion, ResourceVersionNumber, RevisionManifest, Slug,
        TenantLifecycle,
    };

    fn namespaces(config: &Config) -> Vec<&str> {
        config
            .namespace
            .iter()
            .map(|namespace| namespace.id.as_str())
            .collect()
    }

    /// A hydrated revision carrying `state`, so a projection test can start from
    /// desired state rather than from a store.
    fn hydrate(state: DesiredState) -> LoadedRevision {
        let candidate = candidate(ExpectedRevision::Empty, "project", state);
        let manifest = RevisionManifest::of(
            revision_id(9),
            None,
            std::time::SystemTime::UNIX_EPOCH,
            &candidate,
        )
        .expect("a publishable candidate");
        LoadedRevision::assemble(manifest, candidate.state).expect("a consistent revision")
    }

    #[test]
    fn every_project_becomes_a_tenant_qualified_namespace() {
        let mut state = state();
        state.insert(tenant(9, "globex")).expect("a distinct id");
        state
            .insert(project(&tenant_id(9), 12, "core"))
            .expect("a distinct reference");
        let config = TenancyProjection
            .project(&bootstrap(), &state, revision_id(3))
            .expect("the fixture tenancy is projectable");

        // Two tenants' `core` projects are two namespaces, not one: this is the
        // whole reason the id is qualified.
        assert_eq!(
            namespaces(&config),
            ["platform", "acme/core", "globex/core"]
        );
        assert_eq!(
            config
                .namespace
                .iter()
                .filter(|namespace| namespace.default)
                .map(|namespace| namespace.id.as_str())
                .collect::<Vec<_>>(),
            ["platform"],
            "the default namespace stays the boot fact it was"
        );
        assert!(
            config
                .namespace
                .iter()
                .skip(1)
                .all(|namespace| !namespace.allow_platform_fallback),
            "a projected project borrows no other namespace's credentials"
        );
    }

    /// The projected namespaces of a state, as `(name, identity)`.
    fn projected(state: &DesiredState) -> Vec<(String, Option<ProjectIdentity>)> {
        TenancyProjection
            .project(&bootstrap(), state, revision_id(3))
            .expect("projectable")
            .namespace
            .into_iter()
            .filter(|namespace| namespace.project.is_some())
            .map(|namespace| (namespace.id, namespace.project))
            .collect()
    }

    /// A lifecycle transition is what stops traffic, and the rows staying behind
    /// is what keeps the history readable. Both halves are asserted here, because
    /// each without the other is a bug: retention that still serves is a
    /// suspension that suspends nothing, and a stop that deletes is an erasure
    /// nobody asked for.
    #[test]
    fn a_suspended_tenants_projects_stop_being_namespaces_and_keep_being_rows() {
        let mut state = DesiredState::new();
        state.insert(tenant(1, "acme")).expect("fresh");
        state
            .insert(project(&tenant_id(1), 2, "core"))
            .expect("a distinct reference");
        state.insert(tenant(9, "globex")).expect("a distinct id");
        state
            .insert(project(&tenant_id(9), 12, "core"))
            .expect("a distinct reference");
        assert_eq!(
            namespaces(
                &TenancyProjection
                    .project(&bootstrap(), &state)
                    .expect("projectable")
            ),
            ["platform", "acme/core", "globex/core"]
        );

        for lifecycle in [TenantLifecycle::Disabled, TenantLifecycle::Deleted] {
            let mut suspended = state.clone();
            suspended
                .supersede(tenant_body(9, "Globex").in_lifecycle(lifecycle).version_at(
                    Slug::parse("globex").expect("a slug"),
                    ResourceVersionNumber::FIRST.next(),
                ))
                .expect("a tenant this state declares");
            assert_eq!(
                namespaces(
                    &TenancyProjection
                        .project(&bootstrap(), &suspended)
                        .expect("projectable")
                ),
                ["platform", "acme/core"],
                "a {lifecycle} tenant serves nothing, and its neighbour is untouched"
            );
            assert!(
                suspended
                    .resources()
                    .any(|version| version.slug.as_str() == "core"
                        && version.scope == ResourceScope::Tenant(tenant_id(9))),
                "the project row survives the transition it stopped serving"
            );
        }
    }

    #[test]
    fn a_rename_moves_a_namespaces_name_and_not_what_it_is() {
        let mut before = DesiredState::new();
        before.insert(tenant(1, "acme")).expect("fresh");
        before
            .insert(project(&tenant_id(1), 2, "core"))
            .expect("a distinct reference");

        // The same tenant and the same project, renamed: ids come from the seed, so
        // these are the rows a rename produces and not different objects.
        let mut after = DesiredState::new();
        after.insert(tenant(1, "acme-inc")).expect("fresh");
        after
            .insert(project(&tenant_id(1), 2, "platform-core"))
            .expect("a distinct reference");

        let identity = ProjectIdentity {
            tenant: tenant_id(1),
            project: project_id(2),
        };
        assert_eq!(
            projected(&before),
            [("acme/core".to_owned(), Some(identity))]
        );
        assert_eq!(
            projected(&after),
            [("acme-inc/platform-core".to_owned(), Some(identity))],
            "a rename changes the name a request uses and nothing that was accounted"
        );

        // A file-declared namespace has no durable identity to carry, so nothing
        // suggests one may be renamed underneath it.
        assert!(
            TenancyProjection
                .project(&bootstrap(), &before, revision_id(3))
                .expect("projectable")
                .namespace
                .iter()
                .find(|namespace| namespace.id == "platform")
                .expect("the bootstrap namespace survives")
                .project
                .is_none()
        );
    }

    #[test]
    fn everything_the_bootstrap_owns_survives_the_projection_untouched() {
        let bootstrap = bootstrap();
        let config = TenancyProjection
            .project(&bootstrap, &state(), revision_id(3))
            .expect("projectable");

        assert_eq!(config.provider.len(), bootstrap.provider.len());
        assert_eq!(config.model.len(), bootstrap.model.len());
        assert_eq!(config.credential.len(), bootstrap.credential.len());
        assert_eq!(config.gateway_key.len(), bootstrap.gateway_key.len());
        assert_eq!(config.mode, bootstrap.mode);
        assert_eq!(config.namespace[0].id, bootstrap.namespace[0].id);
        assert!(config.namespace[0].default);
        assert_eq!(
            namespaces(&config),
            ["platform", "acme/core"],
            "the projection adds projects and nothing else"
        );

        // A revision holding no project leaves the configuration byte-identical:
        // the projection is additive, so an aliases-only revision is a no-op here.
        let mut tenants_only = DesiredState::new();
        tenants_only.insert(tenant(1, "acme")).expect("fresh");
        tenants_only
            .insert(alias(&tenant_id(1), 4, "fast", &[]))
            .expect("a distinct reference");
        assert_eq!(
            namespaces(
                &TenancyProjection
                    .project(&bootstrap, &tenants_only, revision_id(3))
                    .expect("projectable")
            ),
            ["platform"]
        );
    }

    #[tokio::test]
    async fn a_projected_revision_compiles_through_the_boot_gate_into_a_snapshot() {
        let compiler = RevisionCompiler::with_secrets(
            bootstrap(),
            env(),
            TenancyProjection,
            crate::convergence::secrets::testing::permissive(),
        );
        assert_eq!(compiler.projection_name(), "tenancy");
        let snapshot = compiler
            .compile(&revision(), 3)
            .await
            .expect("a projected tenancy is servable");
        assert_eq!(snapshot.generation, 3);
        assert_eq!(namespaces(&snapshot.config), ["platform", "acme/core"]);
    }

    #[test]
    fn a_project_a_bootstrap_namespace_already_names_is_refused() {
        let bootstrap = Config::from_toml_str(
            r#"
[[namespace]]
id = "acme/core"
default = true

[[gateway_key]]
env = "AXOND_KEY"
namespace = "acme/core"
"#,
        )
        .expect("a valid bootstrap config");
        let error = TenancyProjection
            .project(&bootstrap, &state(), revision_id(3))
            .expect_err("one namespace cannot have two owners");
        assert!(
            matches!(error, ProjectionError::Incomplete { .. }),
            "{error}"
        );
        assert!(error.to_string().contains("acme/core"), "{error}");
    }

    /// The stateful bootstrap shape: `[[namespace]]` is a control-plane-owned
    /// section, so a stateful file declares none and therefore declares no
    /// default. The refusal has to be deliberate and say what is missing —
    /// promoting a project would let an unrelated publication move where unnamed
    /// traffic lands, and projecting anyway would surface as the boot gate's
    /// generic "exactly one namespace must set `default = true`" one stage later,
    /// naming no cause an operator can act on.
    #[tokio::test]
    async fn a_bootstrap_with_no_default_namespace_is_refused_rather_than_given_one() {
        let bootstrap = Config::from_toml_str(
            r#"
mode = "stateful"

[control_plane]
dsn_env = "GW_CONTROL_PLANE_DSN"

[secret_store]
kek_env = "GW_SECRET_STORE_KEK"

[[admin_breakglass]]
env = "GW_ADMIN_BREAKGLASS"
"#,
        )
        .expect("the minimum a stateful replica boots with");
        assert!(
            bootstrap.namespace.is_empty(),
            "a stateful file declares no namespace at all"
        );

        let error = TenancyProjection
            .project(&bootstrap, &state(), revision_id(3))
            .expect_err("a projection may not nominate a default namespace");
        let ProjectionError::Incomplete { detail } = &error else {
            panic!("expected a deliberate incompleteness, got {error:?}");
        };
        assert!(detail.contains("default namespace"), "{detail}");
        assert!(
            detail.contains("not part of this slice"),
            "the refusal says default selection is still gated: {detail}"
        );

        // Compiled, it is a `projection` refusal naming the missing default,
        // rather than a `validation` one from the graph gate after the fact.
        let Err(error) = RevisionCompiler::with_secrets(
            bootstrap,
            env(),
            TenancyProjection,
            crate::convergence::secrets::testing::permissive(),
        )
        .compile(&revision(), 1)
        .await
        else {
            panic!("an incomplete bootstrap does not compile");
        };
        assert_eq!(error.reason(), "projection");
        assert!(error.to_string().contains("default namespace"), "{error}");
    }

    #[test]
    fn a_body_the_projection_cannot_read_is_refused_and_names_the_resource() {
        // Storage the domain would never have accepted: a project filed under a
        // tenant that does not own it. The projection reads bodies through the
        // same view publication validates with, so it reaches the same verdict.
        let moved = ResourceVersion {
            scope: ResourceScope::Tenant(tenant_id(9)),
            ..ProjectBody::new(
                project_id(2),
                tenant_id(1),
                crate::desired_state::DisplayName::parse("Core").expect("a name"),
            )
            .version(Slug::parse("core").expect("a slug"))
        };
        let mut relocated = DesiredState::new();
        relocated.insert(tenant(1, "acme")).expect("fresh");
        relocated
            .insert(tenant(9, "globex"))
            .expect("a distinct id");
        relocated
            .insert(moved.clone())
            .expect("a distinct reference");

        let error = TenancyProjection
            .project(&bootstrap(), &relocated, revision_id(3))
            .expect_err("a project cannot be projected under another tenant");
        let ProjectionError::Body { reference, detail } = &error else {
            panic!("expected a body refusal, got {error:?}");
        };
        assert_eq!(*reference, moved.reference);
        assert!(detail.contains("declares owner"), "{detail}");
    }

    /// The two promises this module can keep about the ids it emits on its own.
    /// Nothing constrains a compiled namespace id's charset, and nothing rejects
    /// duplicates at the config layer, so a projection that emitted a collision or
    /// an id it could not decompose would put two tenants' state on one name with
    /// no later gate to catch it.
    #[test]
    fn every_projected_id_is_distinct_and_decomposes_back_into_its_slugs() {
        let mut state = state();
        state.insert(tenant(9, "globex")).expect("a distinct id");
        state
            .insert(project(&tenant_id(9), 12, "core"))
            .expect("a distinct reference");
        state
            .insert(project(&tenant_id(9), 13, "edge"))
            .expect("a distinct reference");
        let config = TenancyProjection
            .project(&bootstrap(), &state, revision_id(3))
            .expect("projectable");

        let ids = namespaces(&config);
        assert_eq!(
            ids.iter().collect::<BTreeSet<_>>().len(),
            ids.len(),
            "one name cannot mean two namespaces: {ids:?}"
        );

        for id in ids.iter().filter(|id| **id != "platform") {
            let qualified = QualifiedProject::parse(id)
                .unwrap_or_else(|| panic!("a projected id is reversible, `{id}` is not"));
            assert_eq!(
                qualified.to_string(),
                *id,
                "and reversing it is lossless: `{id}`"
            );
            assert!(
                !qualified
                    .tenant
                    .as_str()
                    .contains(QualifiedProject::SEPARATOR)
                    && !qualified
                        .project
                        .as_str()
                        .contains(QualifiedProject::SEPARATOR),
                "the separator appears once, between the slugs: `{id}`"
            );
        }
    }

    #[test]
    fn projection_reads_state_and_nothing_else() {
        // The projection is a pure function of (bootstrap, state): projecting the
        // same revision twice produces the same namespaces, in the same order,
        // which is what lets two replicas converge onto the same configuration.
        let revision = hydrate(state());
        let first = TenancyProjection
            .project(&bootstrap(), revision.state(), revision_id(3))
            .expect("projectable");
        let second = TenancyProjection
            .project(&bootstrap(), revision.state(), revision_id(3))
            .expect("projectable");
        assert_eq!(namespaces(&first), namespaces(&second));
    }
}
