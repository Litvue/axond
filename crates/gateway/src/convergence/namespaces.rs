//! Compiling ADR 0062 flat namespace resources into the existing serving config.

use super::compile::{ProjectionError, RevisionProjection};
use crate::config::{
    CatalogBinding, Config, Credential, GatewayTokenEpoch, Model, Namespace, Provider, Target,
};
use crate::desired_state::{DesiredState, FlatNamespaces, RevisionId};
use crate::namespace::NamespaceGrant;

/// The v2 projection. It never consults or translates the v1 tenancy graph.
#[derive(Debug, Clone, Copy, Default)]
pub struct FlatNamespaceProjection;

impl RevisionProjection for FlatNamespaceProjection {
    fn name(&self) -> &'static str {
        "flat-namespaces-v2"
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
        let flat = FlatNamespaces::of(state).map_err(|error| ProjectionError::Incomplete {
            detail: error.to_string(),
        })?;
        let mut config = bootstrap.clone();
        config.namespace.clear();
        config.flat_namespace_policy.clear();
        config.provider.clear();
        config.credential.clear();
        config.model.clear();
        config.gateway_token_epoch.clear();
        config.projected_principals.clear();

        for (_, body) in flat.namespaces() {
            let namespace = body.namespace().to_string();
            config.namespace.push(Namespace {
                id: namespace.clone(),
                default: body.is_default(),
                allow_platform_fallback: body.allow_platform_fallback(),
                project: None,
                policy: None,
            });
            config.gateway_token_epoch.push(GatewayTokenEpoch {
                namespace: namespace.clone(),
                subject: None,
                min_iat: body.token_epoch(),
            });
            config
                .flat_namespace_policy
                .insert(namespace.clone(), body.policy().clone());
            for provider in body.providers() {
                config.provider.push(Provider {
                    id: runtime_provider(&namespace, provider.id.as_str()),
                    kind: provider.kind.into(),
                    base_url: provider.base_url.clone(),
                });
            }
            for credential in body.credentials() {
                config.credential.push(Credential {
                    namespace: namespace.clone(),
                    provider: runtime_provider(&namespace, credential.provider.as_str()),
                    env: None,
                    secret: Some(credential.secret),
                    id: Some(credential.id.to_string()),
                    weight: credential.weight,
                });
            }
            for alias in body.aliases() {
                let targets = alias
                    .targets
                    .iter()
                    .map(|target| {
                        let catalog = target
                            .catalog
                            .as_ref()
                            .map(|(provider, model)| CatalogBinding::new(provider, model))
                            .transpose()
                            .map_err(|error| ProjectionError::Incomplete {
                                detail: format!(
                                    "namespace `{namespace}` alias `{}` has an invalid approved pricing reference: {error}",
                                    alias.name
                                ),
                            })?;
                        Ok(Target {
                            provider: runtime_provider(&namespace, target.provider.as_str()),
                            model: target.model.clone(),
                            price: target.price,
                            catalog,
                        })
                    })
                    .collect::<Result<Vec<_>, ProjectionError>>()?;
                config.model.push(Model {
                    name: alias.name.to_string(),
                    namespace: Some(namespace.clone()),
                    targets,
                });
            }
        }

        let default_namespace = config
            .namespace
            .iter()
            .find(|namespace| namespace.default)
            .map(|namespace| namespace.id.clone())
            .ok_or_else(|| ProjectionError::Incomplete {
                detail: "flat namespace state has no default namespace".to_owned(),
            })?;
        for (reference, grant) in flat.grants() {
            let anchor = grant
                .grant()
                .namespaces()
                .and_then(|namespaces| namespaces.first())
                .map_or_else(|| default_namespace.clone(), ToString::to_string);
            config
                .projected_principals
                .push(crate::config::ProjectedPrincipal {
                    namespace: anchor,
                    subject: grant
                        .subject()
                        .map_or_else(|| reference.to_string(), str::to_owned),
                    digest: grant.digest(),
                    grant: Some(match grant.grant() {
                        NamespaceGrant::All => NamespaceGrant::all(),
                        NamespaceGrant::Set(namespaces) => {
                            NamespaceGrant::set(namespaces.iter().cloned())
                                .expect("validated bounded namespace set")
                        }
                    }),
                });
        }
        Ok(config)
    }
}

fn runtime_provider(namespace: &str, provider: &str) -> String {
    format!("{provider}@{namespace}")
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use gateway_core::{MiddlewareFailurePosture, MiddlewareScope, ModelPrice};

    use super::*;
    use crate::convergence::compile::testing::{revision_with, stateful_bootstrap};
    use crate::convergence::{CandidateCompiler, RevisionCompiler};
    use crate::desired_state::fixtures;
    use crate::desired_state::{
        Canonical, Checksum, ContentMiddlewareRegistration, FlatProviderKind, InboundGrantBody,
        NamespaceAlias, NamespaceBody, NamespaceCredential, NamespacePolicySpec, NamespaceProvider,
        NamespaceTarget, SecretRef, Slug,
    };
    use crate::namespace::{NamespaceGrant, NamespaceId};

    fn slug(value: &str) -> Slug {
        Slug::parse(value).unwrap()
    }

    fn policy(token_epoch: u64) -> (NamespacePolicySpec, u64) {
        (
            NamespacePolicySpec {
                subject_limit_microdollars: 50_000,
                namespace_limit_microdollars: Some(500_000),
                reservation_ttl_seconds: 60,
                max_in_flight_per_subject: 8,
                lease_ttl_seconds: 30,
                middleware: Vec::new(),
                buffered_response_routes: Vec::new(),
            },
            token_epoch,
        )
    }

    fn namespace(
        id: &str,
        default: bool,
        provider: &str,
        model: &str,
        epoch: u64,
    ) -> NamespaceBody {
        let (policy, token_epoch) = policy(epoch);
        NamespaceBody::new(
            NamespaceId::parse(id).unwrap(),
            default,
            false,
            vec![NamespaceProvider {
                id: slug(provider),
                kind: FlatProviderKind::OpenaiCompatible,
                base_url: format!("https://{id}.example/v1"),
            }],
            Vec::new(),
            vec![NamespaceAlias {
                name: slug("fast"),
                targets: vec![NamespaceTarget {
                    provider: slug(provider),
                    model: model.to_owned(),
                    price: ModelPrice {
                        input_microdollars_per_million: 1,
                        output_microdollars_per_million: 2,
                        reasoning_microdollars_per_million: None,
                        cache_read_microdollars_per_million: None,
                        cache_write_microdollars_per_million: None,
                    },
                    catalog: None,
                }],
            }],
            policy,
            token_epoch,
        )
        .unwrap()
    }

    fn state_with(grants: Vec<InboundGrantBody>) -> DesiredState {
        let mut state = DesiredState::new();
        state
            .insert(
                namespace("acme", true, "shared", "gpt-acme", 101)
                    .version(fixtures::resource_id(1), slug("acme")),
            )
            .unwrap();
        state
            .insert(
                namespace("globex", false, "shared", "gpt-globex", 202)
                    .version(fixtures::resource_id(2), slug("globex")),
            )
            .unwrap();
        for (index, grant) in grants.into_iter().enumerate() {
            state
                .insert(grant.version(
                    fixtures::resource_id(10 + index as u64),
                    slug(&format!("grant-{index}")),
                ))
                .unwrap();
        }
        state
    }

    fn grant(grant: NamespaceGrant, subject: Option<&str>, seed: u8) -> InboundGrantBody {
        InboundGrantBody::new(Checksum::of(&[seed]), grant, subject.map(str::to_owned)).unwrap()
    }

    #[tokio::test]
    async fn two_namespaces_compile_with_isolated_aliases_and_token_epochs() {
        let state = state_with(vec![grant(
            NamespaceGrant::one(NamespaceId::parse("acme").unwrap()),
            Some("acme-worker"),
            1,
        )]);
        state.validate().unwrap();
        let snapshot = RevisionCompiler::new(
            stateful_bootstrap(),
            HashMap::new(),
            FlatNamespaceProjection,
        )
        .compile(&revision_with(state), 7)
        .await
        .expect("a complete flat candidate compiles");

        assert_eq!(snapshot.config.namespace.len(), 2);
        assert_eq!(snapshot.config.model.len(), 2);
        assert_eq!(snapshot.config.model[0].name, "fast");
        assert_eq!(snapshot.config.model[0].namespace.as_deref(), Some("acme"));
        assert_eq!(snapshot.config.model[0].targets[0].model, "gpt-acme");
        assert_eq!(
            snapshot.config.model[1].namespace.as_deref(),
            Some("globex")
        );
        assert_eq!(snapshot.config.model[1].targets[0].model, "gpt-globex");
        assert_eq!(
            snapshot.config.model_for("acme", "fast").unwrap().targets[0].model,
            "gpt-acme"
        );
        assert_eq!(
            snapshot.config.model_for("globex", "fast").unwrap().targets[0].model,
            "gpt-globex"
        );
        assert_eq!(snapshot.config.gateway_token_epoch[0].min_iat, 101);
        assert_eq!(snapshot.config.gateway_token_epoch[1].min_iat, 202);
        assert!(
            snapshot
                .config
                .namespace
                .iter()
                .all(|namespace| namespace.project.is_none()),
            "v2 projection must not fabricate tenant/project identity"
        );
        let active = crate::policy::PolicyView::of(&snapshot.config).policy("acme");
        assert_eq!(active.budget.unwrap().subject_microdollars, 50_000);
        assert_eq!(active.concurrency.unwrap().max_in_flight_per_subject, 8);
    }

    #[tokio::test]
    async fn flat_policy_middleware_and_recovery_stay_namespace_native() {
        let base = namespace("acme", true, "shared", "gpt-acme", 303);
        let mut policy = base.policy().clone();
        policy.middleware.push(
            ContentMiddlewareRegistration::new(
                "test.policy-marker",
                [MiddlewareScope::Request],
                MiddlewareFailurePosture::FailClosed,
                25,
            )
            .unwrap(),
        );
        let body = NamespaceBody::new(
            base.namespace().clone(),
            true,
            false,
            base.providers().to_vec(),
            Vec::new(),
            base.aliases().to_vec(),
            policy,
            303,
        )
        .unwrap();
        let mut state = DesiredState::new();
        state
            .insert(body.version(fixtures::resource_id(1), slug("acme")))
            .unwrap();
        state
            .insert(
                grant(NamespaceGrant::all(), Some("operator"), 1)
                    .version(fixtures::resource_id(2), slug("grant")),
            )
            .unwrap();
        let revision = revision_with(state);
        let snapshot = RevisionCompiler::new(
            stateful_bootstrap(),
            HashMap::new(),
            FlatNamespaceProjection,
        )
        .compile(&revision, 8)
        .await
        .unwrap();
        assert!(
            snapshot
                .middleware("acme")
                .has_scope(MiddlewareScope::Request)
        );

        let cached = snapshot.cached_serving(revision.id());
        let (_, restored) = crate::state::ConfigSnapshot::from_cached_serving(
            stateful_bootstrap(),
            &HashMap::new(),
            cached,
        )
        .unwrap();
        assert_eq!(restored.config.gateway_token_epoch[0].min_iat, 303);
        assert!(
            restored
                .middleware("acme")
                .has_scope(MiddlewareScope::Request)
        );
        assert!(restored.config.namespace[0].project.is_none());
    }

    #[test]
    fn single_set_and_all_grants_project_without_hidden_routing_context() {
        let acme = NamespaceId::parse("acme").unwrap();
        let globex = NamespaceId::parse("globex").unwrap();
        let state = state_with(vec![
            grant(NamespaceGrant::one(acme.clone()), Some("single"), 1),
            grant(
                NamespaceGrant::set([globex.clone(), acme.clone()]).unwrap(),
                Some("set"),
                2,
            ),
            grant(NamespaceGrant::all(), None, 3),
        ]);
        state.validate().unwrap();
        let config = FlatNamespaceProjection
            .project(&stateful_bootstrap(), &state, fixtures::revision_id(4))
            .unwrap();
        assert_eq!(config.projected_principals.len(), 3);
        let grants = config
            .projected_principals
            .iter()
            .map(|principal| principal.grant.as_ref().unwrap())
            .collect::<Vec<_>>();
        assert!(grants[0].permits(&acme));
        assert!(!grants[0].permits(&globex));
        assert!(grants[1].permits(&acme));
        assert!(grants[1].permits(&globex));
        assert!(grants[2].permits(&acme));
        assert!(grants[2].permits(&globex));
    }

    #[test]
    fn unknown_namespace_and_missing_grants_fail_closed() {
        let unknown = state_with(vec![grant(
            NamespaceGrant::one(NamespaceId::parse("unknown").unwrap()),
            None,
            1,
        )]);
        assert!(
            unknown
                .validate()
                .unwrap_err()
                .to_string()
                .contains("unknown namespace")
        );

        let no_grants = state_with(Vec::new());
        assert!(
            no_grants
                .validate()
                .unwrap_err()
                .to_string()
                .contains("no inbound grants")
        );
    }

    #[test]
    fn v1_hierarchy_and_v2_namespaces_cannot_share_a_revision() {
        let mut state = state_with(vec![grant(NamespaceGrant::all(), None, 1)]);
        state.insert(fixtures::tenant(40, "legacy")).unwrap();
        assert!(matches!(
            state.validate(),
            Err(crate::desired_state::revision::ValidationError::MixedStateModels)
        ));
    }

    #[test]
    fn namespace_resources_canonicalize_set_like_fields_deterministically() {
        let first_base = namespace("acme", true, "alpha", "gpt-a", 10);
        let second_provider = NamespaceProvider {
            id: slug("zeta"),
            kind: FlatProviderKind::Anthropic,
            base_url: "https://zeta.example/v1".to_owned(),
        };
        let mut providers = first_base.providers().to_vec();
        providers.push(second_provider.clone());
        let first = NamespaceBody::new(
            first_base.namespace().clone(),
            first_base.is_default(),
            first_base.allow_platform_fallback(),
            providers.clone(),
            first_base.credentials().to_vec(),
            first_base.aliases().to_vec(),
            first_base.policy().clone(),
            first_base.token_epoch(),
        )
        .unwrap();
        providers.reverse();
        let rebuilt = NamespaceBody::new(
            first_base.namespace().clone(),
            first_base.is_default(),
            first_base.allow_platform_fallback(),
            providers,
            first_base.credentials().to_vec(),
            first_base.aliases().to_vec(),
            first_base.policy().clone(),
            first_base.token_epoch(),
        )
        .unwrap();
        assert_eq!(first.checksum().unwrap(), rebuilt.checksum().unwrap());
    }

    #[test]
    fn credential_bodies_round_trip_only_exact_secret_references() {
        let base = namespace("acme", true, "shared", "gpt-acme", 10);
        let credential = NamespaceCredential {
            id: slug("primary"),
            provider: slug("shared"),
            secret: SecretRef::first(fixtures::secret_id(9)),
            weight: 1,
        };
        let body = NamespaceBody::new(
            base.namespace().clone(),
            base.is_default(),
            base.allow_platform_fallback(),
            base.providers().to_vec(),
            vec![credential],
            base.aliases().to_vec(),
            base.policy().clone(),
            base.token_epoch(),
        )
        .unwrap();
        let version = body.version(fixtures::resource_id(20), slug("acme"));
        let read = NamespaceBody::read(&version).unwrap();
        assert_eq!(
            read.credentials()[0].secret,
            SecretRef::first(fixtures::secret_id(9))
        );
        assert!(!format!("{:?}", version.body).contains("plaintext"));
    }
}
