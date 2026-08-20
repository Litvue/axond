//! Compiling ADR 0062 flat namespace resources into the existing serving config.

use super::compile::{ProjectionError, RevisionProjection};
use super::credentials::RuntimeProjection;
use super::policy::PolicyProjection;
use crate::config::{
    CatalogBinding, Config, Credential, GatewayTokenEpoch, Model, Namespace, NamespacePolicy,
    NamespaceStaticPolicy, Provider, Target,
};
use crate::desired_state::policy::{
    BudgetPolicy, ConcurrencyPolicy, PolicyBody, PolicyEpoch, PolicyScope, RevocationPolicy,
};
use crate::desired_state::{DesiredState, FlatNamespaces, RevisionId};

/// The v2 projection. It never consults or translates the v1 tenancy graph.
#[derive(Debug, Clone, Copy, Default)]
pub struct FlatNamespaceProjection;

/// Production projection router for the mutually exclusive durable models.
///
/// Selection is derived from the validated revision itself, never from a boot
/// flag that could make one replica interpret the same revision differently.
#[derive(Debug, Clone, Copy, Default)]
pub struct StateModelProjection;

impl RevisionProjection for StateModelProjection {
    fn name(&self) -> &'static str {
        "state-model"
    }

    fn projects_inbound_principals(&self) -> bool {
        true
    }

    fn project(
        &self,
        bootstrap: &Config,
        state: &DesiredState,
        source: RevisionId,
    ) -> Result<Config, ProjectionError> {
        if state.is_flat_namespace_v2() {
            FlatNamespaceProjection.project(bootstrap, state, source)
        } else {
            PolicyProjection::over(RuntimeProjection).project(bootstrap, state, source)
        }
    }
}

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
        source: RevisionId,
    ) -> Result<Config, ProjectionError> {
        let flat = FlatNamespaces::of(state).map_err(|error| ProjectionError::Incomplete {
            detail: error.to_string(),
        })?;
        let mut config = bootstrap.clone();
        config.namespace.clear();
        config.provider.clear();
        config.credential.clear();
        config.model.clear();
        config.gateway_token_epoch.clear();
        config.projected_principals.clear();

        let (_, deployment) = flat.deployment();
        for provider in deployment.providers() {
            config.provider.push(Provider {
                id: provider.id.to_string(),
                kind: provider.kind.into(),
                base_url: provider.base_url.clone(),
            });
        }

        for (reference, body) in flat.namespaces() {
            let namespace = body.namespace().to_string();
            let middleware = body
                .policy()
                .middleware
                .iter()
                .map(|selection| {
                    deployment
                        .middleware()
                        .iter()
                        .find(|registration| registration.id() == selection)
                        .cloned()
                        .ok_or_else(|| ProjectionError::Incomplete {
                            detail: format!(
                                "namespace `{namespace}` selects missing middleware `{selection}`"
                            ),
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let exact_policy = body
                .policy()
                .exact
                .as_ref()
                .map(|exact| {
                    let policy_body = PolicyBody::new(
                        PolicyScope::Namespace(reference.id),
                        PolicyEpoch::new(body.policy().epoch).map_err(|error| {
                            ProjectionError::Incomplete {
                                detail: format!(
                                    "namespace `{namespace}` has invalid policy epoch: {error}"
                                ),
                            }
                        })?,
                        BudgetPolicy::stored(
                            exact.subject_limit_microdollars,
                            exact.namespace_limit_microdollars,
                            exact.reservation_ttl_seconds,
                        )
                        .map_err(|error| ProjectionError::Incomplete {
                            detail: format!(
                                "namespace `{namespace}` has invalid budget policy: {error}"
                            ),
                        })?,
                        ConcurrencyPolicy::new(
                            exact.max_in_flight_per_subject,
                            exact.lease_ttl_seconds,
                        )
                        .map_err(|error| ProjectionError::Incomplete {
                            detail: format!(
                                "namespace `{namespace}` has invalid concurrency policy: {error}"
                            ),
                        })?,
                        RevocationPolicy::new(body.token_epoch()),
                    );
                    let generation = policy_body.generation(source);
                    Ok(NamespacePolicy {
                        body: policy_body,
                        generation,
                    })
                })
                .transpose()?;
            config.namespace.push(Namespace {
                id: namespace.clone(),
                default: body.is_default(),
                allow_platform_fallback: body.allow_platform_fallback(),
                project: None,
                policy: exact_policy,
                static_policy: Some(NamespaceStaticPolicy {
                    content_middleware: middleware,
                    buffered_response_routes: body.policy().buffered_response_routes.clone(),
                }),
            });
            config.gateway_token_epoch.push(GatewayTokenEpoch {
                namespace: namespace.clone(),
                subject: None,
                min_iat: body.token_epoch(),
            });
            for credential in body.credentials() {
                config.credential.push(Credential {
                    namespace: namespace.clone(),
                    provider: credential.provider.to_string(),
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
                            provider: target.provider.to_string(),
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
                    grant: Some(grant.grant().clone()),
                });
        }
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use gateway_core::{MiddlewareFailurePosture, MiddlewareScope, ModelPrice};

    use super::*;
    use crate::backends::secrets::{SecretError, SecretMaterial, SecretResolver};
    use crate::backends::{Capabilities, Capability};
    use crate::convergence::compile::testing::{revision_with, stateful_bootstrap};
    use crate::convergence::secrets::{MaterialLedger, SecretMaterialization};
    use crate::convergence::{CandidateCompiler, RevisionCompiler};
    use crate::desired_state::fixtures;
    use crate::desired_state::{
        Canonical, Checksum, ContentMiddlewareRegistration, DeploymentBody,
        DeploymentSecretIndexEntry, FlatProviderKind, InboundGrantBody, NamespaceAlias,
        NamespaceBody, NamespaceCredential, NamespaceExactEnforcement, NamespacePolicySpec,
        NamespaceProvider, NamespaceTarget, ResourceKind, ResourceRef, ResourceVersionNumber,
        SecretLifecycle, SecretOwner, SecretRef, Slug,
    };
    use crate::namespace::{NamespaceGrant, NamespaceId};

    fn slug(value: &str) -> Slug {
        Slug::parse(value).unwrap()
    }

    fn policy(token_epoch: u64) -> (NamespacePolicySpec, u64) {
        (
            NamespacePolicySpec {
                epoch: token_epoch,
                exact: Some(NamespaceExactEnforcement {
                    subject_limit_microdollars: 50_000,
                    namespace_limit_microdollars: Some(500_000),
                    reservation_ttl_seconds: 60,
                    max_in_flight_per_subject: 8,
                    lease_ttl_seconds: 30,
                }),
                middleware: Vec::new(),
                buffered_response_routes: Vec::new(),
            },
            token_epoch,
        )
    }

    fn deployment_ref() -> ResourceRef {
        ResourceRef::new(
            ResourceKind::Deployment,
            fixtures::resource_id(50),
            ResourceVersionNumber::FIRST,
        )
    }

    fn deployment(middleware: Vec<ContentMiddlewareRegistration>) -> DeploymentBody {
        deployment_with_secrets(middleware, Vec::new())
    }

    fn deployment_with_secrets(
        middleware: Vec<ContentMiddlewareRegistration>,
        secrets: Vec<DeploymentSecretIndexEntry>,
    ) -> DeploymentBody {
        DeploymentBody::new(
            vec![NamespaceProvider {
                id: slug("shared"),
                kind: FlatProviderKind::OpenaiCompatible,
                base_url: "https://shared.example/v1".to_owned(),
            }],
            Vec::new(),
            middleware,
            Vec::new(),
            secrets,
        )
        .unwrap()
    }

    fn namespace(
        id: &str,
        default: bool,
        provider: &str,
        model: &str,
        epoch: u64,
    ) -> NamespaceBody {
        namespace_with(id, default, false, provider, model, Vec::new(), epoch)
    }

    fn namespace_with(
        id: &str,
        default: bool,
        allow_platform_fallback: bool,
        provider: &str,
        model: &str,
        credentials: Vec<NamespaceCredential>,
        epoch: u64,
    ) -> NamespaceBody {
        let (policy, token_epoch) = policy(epoch);
        NamespaceBody::new(
            NamespaceId::parse(id).unwrap(),
            default,
            allow_platform_fallback,
            deployment_ref(),
            credentials,
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
            .insert(deployment(Vec::new()).version(fixtures::resource_id(50), slug("deployment")))
            .unwrap();
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

    struct RecordingResolver {
        calls: AtomicUsize,
        owner: NamespaceId,
        reference: SecretRef,
        digest: Checksum,
    }

    #[async_trait]
    impl SecretResolver for RecordingResolver {
        fn name(&self) -> &'static str {
            "recording-namespace-resolver"
        }

        fn capabilities(&self) -> Capabilities {
            Capabilities::new(&[Capability::EnvelopeEncryption])
        }

        async fn resolve(
            &self,
            _owner: SecretOwner,
            _reference: &SecretRef,
        ) -> Result<SecretMaterial, SecretError> {
            panic!("flat-v2 must never use the legacy owner resolver")
        }

        async fn exists(
            &self,
            _owner: SecretOwner,
            _reference: &SecretRef,
        ) -> Result<bool, SecretError> {
            panic!("flat-v2 must never use the legacy owner lookup")
        }

        async fn resolve_namespace(
            &self,
            request: &crate::desired_state::NamespaceSecretRequest,
        ) -> Result<SecretMaterial, SecretError> {
            assert_eq!(request.owner(), &self.owner);
            assert_eq!(request.reference(), self.reference);
            assert_eq!(request.ciphertext_digest(), self.digest);
            assert_eq!(request.lifecycle(), SecretLifecycle::Active);
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(SecretMaterial::new("test-material".to_owned()))
        }

        async fn exists_namespace(
            &self,
            _request: &crate::desired_state::NamespaceSecretRequest,
        ) -> Result<bool, SecretError> {
            Ok(true)
        }
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

    #[test]
    fn production_projection_selects_flat_v2_from_the_revision() {
        let state = state_with(vec![grant(NamespaceGrant::all(), None, 1)]);
        let config = StateModelProjection
            .project(&stateful_bootstrap(), &state, fixtures::revision_id(4))
            .unwrap();
        assert_eq!(config.namespace.len(), 2);
        assert!(config.namespace.iter().all(|namespace| {
            namespace.project.is_none()
                && matches!(
                    namespace.policy.as_ref().map(|policy| policy.body.scope()),
                    Some(PolicyScope::Namespace(_))
                )
        }));
    }

    #[tokio::test]
    async fn shared_provider_ids_enable_explicit_platform_credential_fallback() {
        let reference = SecretRef::first(fixtures::secret_id(9));
        let credential = NamespaceCredential {
            id: slug("primary"),
            provider: slug("shared"),
            secret: reference,
            weight: 1,
        };
        let mut state = DesiredState::new();
        state
            .insert(
                deployment_with_secrets(
                    Vec::new(),
                    vec![DeploymentSecretIndexEntry::new(
                        NamespaceId::parse("acme").unwrap(),
                        reference,
                        Checksum::of(b"ciphertext-acme-primary"),
                        SecretLifecycle::Active,
                    )],
                )
                .version(fixtures::resource_id(50), slug("deployment")),
            )
            .unwrap();
        state
            .insert(
                namespace_with(
                    "acme",
                    true,
                    false,
                    "shared",
                    "gpt-acme",
                    vec![credential],
                    1,
                )
                .version(fixtures::resource_id(1), slug("acme")),
            )
            .unwrap();
        state
            .insert(
                namespace_with("globex", false, true, "shared", "gpt-globex", Vec::new(), 1)
                    .version(fixtures::resource_id(2), slug("globex")),
            )
            .unwrap();
        state
            .insert(
                grant(NamespaceGrant::all(), None, 1)
                    .version(fixtures::resource_id(10), slug("grant")),
            )
            .unwrap();
        let revision = revision_with(state);

        let without_resolver =
            match RevisionCompiler::new(stateful_bootstrap(), HashMap::new(), StateModelProjection)
                .compile(&revision, 1)
                .await
            {
                Ok(_) => panic!("credential-bearing v2 state must fail closed without material"),
                Err(error) => error,
            };
        assert_eq!(without_resolver.reason(), "secret");

        let snapshot = RevisionCompiler::with_secrets(
            stateful_bootstrap(),
            HashMap::new(),
            StateModelProjection,
            crate::convergence::secrets::testing::permissive(),
        )
        .compile(&revision, 2)
        .await
        .unwrap();
        assert_eq!(snapshot.config.provider[0].id, "shared");
        let plan = snapshot
            .credentials
            .plan(&snapshot.config, "globex", "shared")
            .expect("globex explicitly borrows the default namespace pool");
        assert_eq!(plan.source, crate::credentials::CredentialSource::Platform);

        let mut cached = snapshot.cached_serving(revision.id());
        match &cached.secrets[0].binding {
            crate::state::CachedSecretBinding::Namespace {
                owner_namespace,
                ciphertext_digest,
                lifecycle,
            } => {
                assert_eq!(owner_namespace, "acme");
                assert_eq!(
                    ciphertext_digest,
                    &Checksum::of(b"ciphertext-acme-primary").to_string()
                );
                assert_eq!(lifecycle, "active");
            }
            crate::state::CachedSecretBinding::Legacy => {
                panic!("flat-v2 cache material must retain namespace authority")
            }
        }
        let (_, restored) = crate::state::ConfigSnapshot::from_cached_serving(
            stateful_bootstrap(),
            &HashMap::new(),
            cached.clone(),
        )
        .unwrap();
        assert!(
            restored
                .credentials
                .is_present(&restored.config, "globex", "shared"),
            "credential-bearing LKG restores platform fallback"
        );
        let mut foreign_owner = cached.clone();
        let crate::state::CachedSecretBinding::Namespace {
            owner_namespace, ..
        } = &mut foreign_owner.secrets[0].binding
        else {
            unreachable!()
        };
        *owner_namespace = "globex".to_owned();
        let error = match crate::state::ConfigSnapshot::from_cached_serving(
            stateful_bootstrap(),
            &HashMap::new(),
            foreign_owner,
        ) {
            Ok(_) => panic!("a signed cache cannot move material to a foreign namespace"),
            Err(error) => error,
        };
        assert!(error.contains("credential belongs to `acme`"), "{error}");

        let mut tombstoned = cached.clone();
        let crate::state::CachedSecretBinding::Namespace { lifecycle, .. } =
            &mut tombstoned.secrets[0].binding
        else {
            unreachable!()
        };
        *lifecycle = "tombstoned".to_owned();
        let error = match crate::state::ConfigSnapshot::from_cached_serving(
            stateful_bootstrap(),
            &HashMap::new(),
            tombstoned,
        ) {
            Ok(_) => panic!("withdrawn cached material cannot reactivate"),
            Err(error) => error,
        };
        assert!(error.contains("cannot restore Tombstoned"), "{error}");

        let mut ownerless = cached.clone();
        ownerless.secrets[0].binding = crate::state::CachedSecretBinding::Legacy;
        let error = match crate::state::ConfigSnapshot::from_cached_serving(
            stateful_bootstrap(),
            &HashMap::new(),
            ownerless,
        ) {
            Ok(_) => panic!("flat-v2 cached material cannot lose authority metadata"),
            Err(error) => error,
        };
        assert!(error.contains("omits namespace authority"), "{error}");

        cached.secrets.clear();
        let error = match crate::state::ConfigSnapshot::from_cached_serving(
            stateful_bootstrap(),
            &HashMap::new(),
            cached,
        ) {
            Ok(_) => panic!("an LKG missing credential material must be rejected"),
            Err(error) => error,
        };
        assert!(
            error.contains("0 secret materials for 1 referenced versions"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn namespace_secret_resolution_is_typed_and_deduplicated() {
        let owner = NamespaceId::parse("acme").unwrap();
        let reference = SecretRef::first(fixtures::secret_id(9));
        let digest = Checksum::of(b"ciphertext-acme-primary");
        let credential = |id: &str| NamespaceCredential {
            id: slug(id),
            provider: slug("shared"),
            secret: reference,
            weight: 1,
        };
        let mut state = DesiredState::new();
        state
            .insert(
                deployment_with_secrets(
                    Vec::new(),
                    vec![DeploymentSecretIndexEntry::new(
                        owner.clone(),
                        reference,
                        digest,
                        SecretLifecycle::Active,
                    )],
                )
                .version(fixtures::resource_id(50), slug("deployment")),
            )
            .unwrap();
        state
            .insert(
                namespace_with(
                    "acme",
                    true,
                    false,
                    "shared",
                    "gpt-acme",
                    vec![credential("primary"), credential("secondary")],
                    1,
                )
                .version(fixtures::resource_id(1), slug("acme")),
            )
            .unwrap();
        state
            .insert(
                grant(NamespaceGrant::all(), None, 1)
                    .version(fixtures::resource_id(10), slug("grant")),
            )
            .unwrap();
        state.validate().unwrap();

        let resolver = Arc::new(RecordingResolver {
            calls: AtomicUsize::new(0),
            owner,
            reference,
            digest,
        });
        let materialization = SecretMaterialization::new(
            Arc::clone(&resolver) as Arc<dyn SecretResolver>,
            MaterialLedger::new(),
        );
        let resolved = materialization.resolve(&state).await.unwrap();
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolver.calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn flat_policy_middleware_and_recovery_stay_namespace_native() {
        let base = namespace("acme", true, "shared", "gpt-acme", 303);
        let mut policy = base.policy().clone();
        policy.exact = None;
        let middleware = ContentMiddlewareRegistration::new(
            "test.policy-marker",
            [MiddlewareScope::Request],
            MiddlewareFailurePosture::FailClosed,
            25,
        )
        .unwrap();
        policy.middleware.push(middleware.id().to_owned());
        let body = NamespaceBody::new(
            base.namespace().clone(),
            true,
            false,
            deployment_ref(),
            Vec::new(),
            base.aliases().to_vec(),
            policy,
            303,
        )
        .unwrap();
        let mut state = DesiredState::new();
        state
            .insert(
                deployment(vec![middleware]).version(fixtures::resource_id(50), slug("deployment")),
            )
            .unwrap();
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
        assert!(
            snapshot.config.namespace[0].policy.is_none(),
            "blob-only static policy must not fabricate exact distributed caps"
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
        assert!(restored.config.namespace[0].policy.is_none());
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
        let first_provider = NamespaceProvider {
            id: slug("alpha"),
            kind: FlatProviderKind::OpenaiCompatible,
            base_url: "https://alpha.example/v1".to_owned(),
        };
        let second_provider = NamespaceProvider {
            id: slug("zeta"),
            kind: FlatProviderKind::Anthropic,
            base_url: "https://zeta.example/v1".to_owned(),
        };
        let mut providers = vec![first_provider, second_provider];
        let first = DeploymentBody::new(
            providers.clone(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .unwrap();
        providers.reverse();
        let rebuilt =
            DeploymentBody::new(providers, Vec::new(), Vec::new(), Vec::new(), Vec::new()).unwrap();
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
            deployment_ref(),
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
