//! Projecting durable providers, catalogue pins, enablements, and aliases into
//! the concrete routing table a request snapshot consumes.
//!
//! The control plane intentionally stores references and immutable resource
//! bodies, not a copy of the catalogue payload. This stage is therefore the
//! only place convergence reads a retained catalogue: it verifies the payload,
//! resolves each pinned offering to one callable provider/model pair, and then
//! stores only those concrete strings in the candidate config. Requests never
//! retain a store handle or parse catalogue bytes.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::backends::catalog::{CatalogSnapshot, ProviderId};
use crate::backends::catalog_pins::{PinnedCatalog, Resolution};
use crate::backends::catalog_store::{self, CatalogStore, CatalogStoreError};
use crate::config::{CatalogBinding, Config, Model, Target};
use crate::convergence::compile::ProjectionError;
use crate::convergence::credentials::runtime_provider_id;
use crate::desired_state::models::{ModelEnablement, Models};
use crate::desired_state::pricing::PricingSnapshot;
use crate::desired_state::providers::Providers;
use crate::desired_state::{Checksum, DesiredState};

/// Project all typed model contracts in `state` onto `config`.
///
/// Untyped/legacy revisions have no model contracts this build can project and
/// are returned unchanged. Once typed enablements exist, the retained catalogue
/// reader is mandatory: a missing, corrupt, withdrawn, ambiguous, or unpriced
/// target refuses the candidate rather than publishing an alias that would fail
/// only after an authenticated caller reaches it.
pub async fn project(
    mut config: Config,
    state: &DesiredState,
    catalogue: Option<&Arc<dyn CatalogStore>>,
    pricing: Option<&PricingSnapshot>,
) -> Result<Config, ProjectionError> {
    let models = Models::of(state).map_err(|error| ProjectionError::Body {
        reference: error.reference(),
        detail: error.to_string(),
    })?;
    if models.enablements().next().is_none() && models.aliases().next().is_none() {
        return Ok(config);
    }
    let Some(catalogue) = catalogue else {
        return Err(ProjectionError::Incomplete {
            detail: "typed model contracts require a durable catalogue reader; no retained \
                     catalogue store is configured for convergence"
                .to_owned(),
        });
    };
    let Some(pricing) = pricing else {
        return Err(ProjectionError::Incomplete {
            detail: "typed model contracts require an effective approved price book; no pricing \
                     snapshot is available for stateful serving"
                .to_owned(),
        });
    };

    let providers = Providers::of(state).map_err(|error| ProjectionError::Body {
        reference: error.reference(),
        detail: error.to_string(),
    })?;
    let snapshots = retained_catalogues(&models, catalogue).await?;
    let mut aliases = Vec::new();

    for alias in models.aliases() {
        if !alias.body.is_enabled() {
            continue;
        }
        let Some(namespace) = config.namespace.iter().find(|namespace| {
            namespace.project
                == Some(crate::config::ProjectIdentity {
                    tenant: alias.body.tenant(),
                    project: alias.body.project(),
                })
        }) else {
            // Tenancy already withdrew a suspended/unknown project. Retaining
            // its durable alias is correct; giving it a request-facing row is
            // not.
            continue;
        };

        let mut targets = Vec::new();
        for target in alias.body.targets() {
            let Some(enablement) = models.enablement(target.enablement) else {
                continue;
            };
            if enablement.reference.version != target.version || !enablement.body.is_enabled() {
                continue;
            }
            let Some(snapshot) = snapshots.get(&enablement.body.offering().snapshot) else {
                return Err(ProjectionError::Incomplete {
                    detail: format!(
                        "{} pins catalogue payload {} but convergence did not retain it",
                        enablement.reference,
                        enablement.body.offering().snapshot
                    ),
                });
            };
            let Some((catalog_provider, published_model)) = callable_target(snapshot, enablement)?
            else {
                continue;
            };
            let catalog_provider_id = ProviderId::parse(&catalog_provider).map_err(|error| {
                ProjectionError::Incomplete {
                    detail: format!(
                        "{} resolves to invalid catalogue provider `{catalog_provider}`: {error}",
                        alias.reference
                    ),
                }
            })?;
            let Some(price) = pricing.price(&catalog_provider_id, &published_model) else {
                // An approved pointer is not enough: the effective book must
                // cover the exact callable id at this compile instant. The
                // request path must never reinterpret an absent stateful book
                // as file pricing, nor turn an uncovered target into free
                // traffic.
                continue;
            };
            let Some(provider) = providers
                .all()
                .filter(|provider| {
                    provider.slug.as_str() == catalog_provider
                        && provider.body.tenant() == alias.body.tenant()
                        && provider
                            .body
                            .project()
                            .is_none_or(|project| project == alias.body.project())
                })
                // A project-owned connection is the explicit override for a
                // tenant-wide connection with the same durable slug. The
                // qualified runtime ids keep both endpoints distinct; this
                // preference decides which one the project alias calls.
                .max_by_key(|provider| provider.body.project() == Some(alias.body.project()))
            else {
                continue;
            };
            if provider.body.wire_family() != enablement.body.wire_family() {
                continue;
            }
            let runtime_provider = runtime_provider_id(&providers, provider);
            if config.provider(&runtime_provider).is_none() {
                return Err(ProjectionError::Incomplete {
                    detail: format!(
                        "{} resolves to catalogue provider `{catalog_provider}`, but no \
                         projected provider connection has that runtime id",
                        alias.reference
                    ),
                });
            }
            if !config.credential.iter().any(|credential| {
                credential.namespace == namespace.id
                    && credential.provider == runtime_provider
                    && credential.secret.is_some()
            }) {
                // A connection without active material is not a serving
                // target. Keeping it out of the alias makes the candidate
                // refuse (or choose a later target) before readiness or a
                // request can discover an uncredentialed route.
                continue;
            }
            let catalog =
                CatalogBinding::new(&catalog_provider, &published_model).map_err(|error| {
                    ProjectionError::Incomplete {
                        detail: format!(
                            "{} resolves to an invalid catalogue callable target: {error}",
                            alias.reference
                        ),
                    }
                })?;
            targets.push(Target {
                provider: runtime_provider,
                model: published_model,
                // Keep the resolved rate on the target as well as on the
                // immutable PricingSnapshot. The request path uses the latter
                // because the catalogue binding is explicit; this value keeps
                // the config structurally complete and auditable.
                price,
                catalog: Some(catalog),
            });
        }
        if targets.is_empty() {
            return Err(ProjectionError::Incomplete {
                detail: format!(
                    "{} publishes enabled alias `{}` without a routable, approved target",
                    alias.reference, alias.slug
                ),
            });
        }
        aliases.push(Model {
            name: alias.slug.as_str().to_owned(),
            namespace: Some(namespace.id.clone()),
            targets,
        });
    }

    config.model.extend(aliases);
    Ok(config)
}

async fn retained_catalogues(
    models: &Models,
    catalogue: &Arc<dyn CatalogStore>,
) -> Result<BTreeMap<Checksum, CatalogSnapshot>, ProjectionError> {
    let mut snapshots = BTreeMap::new();
    for enablement in models
        .enablements()
        .filter(|enablement| enablement.body.is_enabled())
    {
        let digest = enablement.body.offering().snapshot;
        if snapshots.contains_key(&digest) {
            continue;
        }
        let retained = catalogue
            .retained_by_raw_digest(digest)
            .await
            .map_err(store_refusal)?
            .ok_or_else(|| ProjectionError::Incomplete {
                detail: format!(
                    "{} pins catalogue payload {digest}, but that exact retained snapshot is \
                     unavailable",
                    enablement.reference
                ),
            })?;
        let snapshot =
            catalog_store::hydrate(&retained).map_err(|error| ProjectionError::Incomplete {
                detail: format!(
                    "catalogue payload {digest} could not be rehydrated for {}: {error}",
                    enablement.reference
                ),
            })?;
        snapshots.insert(digest, snapshot);
    }
    Ok(snapshots)
}

fn callable_target(
    snapshot: &CatalogSnapshot,
    enablement: &ModelEnablement,
) -> Result<Option<(String, String)>, ProjectionError> {
    let pinned =
        PinnedCatalog::of_snapshot(snapshot).map_err(|error| ProjectionError::Incomplete {
            detail: format!(
                "catalogue {} cannot resolve {}: {error}",
                snapshot.source.content_id, enablement.reference
            ),
        })?;
    match pinned.resolve(enablement.body.offering()) {
        Resolution::Callable(callable) => Ok(Some((
            callable.provider().to_string(),
            callable.published_model_id().to_owned(),
        ))),
        Resolution::Withdrawn => Ok(None),
        Resolution::OtherSnapshot { pinned } => Err(ProjectionError::Incomplete {
            detail: format!(
                "{} resolves against the wrong catalogue snapshot {pinned}",
                enablement.reference
            ),
        }),
        Resolution::Ambiguous { callables } => Err(ProjectionError::Incomplete {
            detail: format!(
                "{} resolves to {} ambiguous callable catalogue targets",
                enablement.reference,
                callables.len()
            ),
        }),
    }
}

fn store_refusal(error: CatalogStoreError) -> ProjectionError {
    ProjectionError::Incomplete {
        detail: format!("the retained catalogue store could not answer convergence: {error}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::catalog::{CatalogSnapshot, SourceValidators};
    use crate::backends::catalog_store::{CatalogStore, InMemoryCatalogStore, RetainedCatalog};
    use crate::backends::models_dev::ModelsDevAdapter;
    use crate::config::Config;
    use crate::convergence::compile::RevisionProjection;
    use crate::convergence::credentials::RuntimeProjection;
    use crate::desired_state::credentials::ProviderCredentialBody;
    use crate::desired_state::fixtures;
    use crate::desired_state::models::{
        ApprovedPrice, CatalogOffering, ModelAliasBody, ModelEnablementBody, ModelOwner, OfferingId,
    };
    use crate::desired_state::pricing::{
        Approval, EffectiveInstant, EffectiveInterval, PriceBookBody, RulePrecedence,
    };
    use crate::desired_state::resource::{
        BlobKind, BlobRef, ResourceBody, ResourceKind, ResourceScope, ResourceVersion,
        ResourceVersionNumber,
    };
    use crate::desired_state::tenancy::DisplayName;
    use crate::desired_state::{ProviderBody, SecretLifecycle, SecretOwner, Slug, WireFamily};
    use std::time::{SystemTime, UNIX_EPOCH};

    const CATALOG: &str = include_str!("../backends/fixtures/models_dev/catalog.identity.json");

    fn bootstrap() -> Config {
        Config::from_toml_str(
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
        .expect("the stateful bootstrap parses")
    }

    fn state_and_snapshot() -> (crate::desired_state::DesiredState, CatalogSnapshot) {
        let snapshot = ModelsDevAdapter::default()
            .parse(CATALOG.as_bytes(), SourceValidators::default(), UNIX_EPOCH)
            .expect("the catalogue fixture parses");
        let tenant = fixtures::tenant_id(1);
        let project = fixtures::project_id(2);
        let catalog_reference = fixtures::reference(ResourceKind::CatalogModel, 5);
        let catalog = ResourceVersion::new(
            catalog_reference,
            ResourceScope::Deployment,
            Slug::parse("models-dev").expect("a slug"),
            ResourceBody::Blob(BlobRef::of(BlobKind::CatalogSnapshot, CATALOG.as_bytes())),
        );
        let provider = ProviderBody::for_tenant(
            fixtures::resource_id(40),
            tenant,
            DisplayName::parse("OpenAI").expect("a name"),
            WireFamily::OpenaiChat,
            "https://api.openai.com/v1",
        )
        .version(Slug::parse("openai").expect("a slug"));
        let credential = ProviderCredentialBody::staged(
            fixtures::resource_id(41),
            SecretOwner::project(tenant, project),
            fixtures::resource_id(40),
            DisplayName::parse("OpenAI key").expect("a name"),
            fixtures::secret_ref(41),
        )
        .transitioned(SecretLifecycle::Active)
        .expect("staged material activates")
        .version(Slug::parse("openai-key").expect("a slug"));
        let offering = CatalogOffering::new(
            OfferingId::of("openai", "openai/gpt-5.5").expect("an offering id"),
            snapshot.source.raw.digest,
        );
        let enablement_reference = fixtures::reference(ResourceKind::ModelEnablement, 30);
        let enablement = ModelEnablementBody::new(
            fixtures::resource_id(30),
            ModelOwner::project(tenant, project),
            offering,
            WireFamily::OpenaiChat,
        )
        .approving(ApprovedPrice::version(
            fixtures::resource_id(70),
            ResourceVersionNumber::FIRST,
        ))
        .version(Slug::parse("gpt-5-5").expect("a slug"), catalog_reference);
        let price = PriceBookBody::new(
            snapshot.content.content_id(),
            ResourceVersionNumber::FIRST,
            Approval::Approved {
                by: fixtures::actor(),
                at: EffectiveInstant::EPOCH,
                citation: None,
            },
        )
        .with_rule(fixtures::price_rule(
            fixtures::priced_target("openai", "openai/gpt-5.5"),
            RulePrecedence::Baseline,
            EffectiveInterval::from(EffectiveInstant::EPOCH),
            2_500_000,
            10_000_000,
        ))
        .version(
            fixtures::resource_id(70),
            Slug::parse("baseline").expect("a slug"),
        );
        let alias = ModelAliasBody::new(
            fixtures::resource_id(32),
            tenant,
            project,
            WireFamily::OpenaiChat,
            [crate::desired_state::AliasTarget::first(
                fixtures::resource_id(30),
            )],
        )
        .version(Slug::parse("fast").expect("a slug"));
        let mut state = crate::desired_state::DesiredState::new();
        state.declare_blob(*catalog.body.blob().expect("the catalog blob"));
        state
            .insert(fixtures::tenant(1, "acme"))
            .and_then(|state| state.insert(fixtures::project(&tenant, 2, "core")))
            .and_then(|state| state.insert(catalog))
            .and_then(|state| state.insert(provider))
            .and_then(|state| state.insert(credential))
            .and_then(|state| state.insert(enablement))
            .and_then(|state| state.insert(price))
            .and_then(|state| state.insert(alias))
            .and_then(|state| {
                let key = fixtures::workload_key(0xd0);
                state.insert(fixtures::workload(
                    33,
                    "caller",
                    ResourceScope::Project { tenant, project },
                    &[crate::desired_state::Role::Developer],
                    Some(&key),
                ))
            })
            .expect("the serving state is distinct");
        assert_eq!(enablement_reference.kind, ResourceKind::ModelEnablement);
        (state, snapshot)
    }

    #[tokio::test]
    async fn projects_a_pinned_catalogue_target_into_a_namespace_owned_alias() {
        let (state, snapshot) = state_and_snapshot();
        let store: Arc<dyn CatalogStore> = Arc::new(InMemoryCatalogStore::new());
        store
            .activate(
                &RetainedCatalog {
                    source: snapshot.source.clone(),
                    payload: crate::backends::catalog::RawPayload::new(CATALOG.as_bytes()),
                },
                SystemTime::now(),
            )
            .await
            .expect("the exact payload is retained");
        let config = RuntimeProjection
            .project(&bootstrap(), &state, fixtures::revision_id(3))
            .expect("tenancy, providers, and principals project");
        let models = Models::of(&state).expect("the typed model state reads");
        let enablement = models
            .enablement(fixtures::resource_id(30))
            .expect("the enablement is present");
        assert!(enablement.body.billable_price().is_some());
        assert!(
            callable_target(&snapshot, enablement)
                .expect("the pinned catalogue resolves")
                .is_some()
        );
        let pricing = crate::desired_state::pricing::PriceBooks::of(&state)
            .expect("the deployment price book reads")
            .snapshot_at(EffectiveInstant::EPOCH)
            .expect("the deployment price book is present");
        let config = project(config, &state, Some(&store), Some(&pricing))
            .await
            .expect("the pinned callable target projects");
        config
            .validate_compiled()
            .expect("the projected serving graph passes the boot gate");
        assert_eq!(config.model.len(), 1);
        let model = &config.model[0];
        assert_eq!(model.name, "fast");
        assert_eq!(model.namespace.as_deref(), Some("acme/core"));
        assert_eq!(model.targets[0].provider, "openai");
        assert_eq!(model.targets[0].model, "openai/gpt-5.5");
        assert_eq!(model.targets[0].price.input_microdollars_per_million, 2_500);
        assert_eq!(
            model.targets[0]
                .catalog
                .as_ref()
                .map(|binding| (binding.provider.to_string(), binding.model.as_str())),
            Some(("openai".to_owned(), "openai/gpt-5.5"))
        );
    }
}
