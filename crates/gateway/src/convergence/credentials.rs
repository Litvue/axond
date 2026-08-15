//! Projecting a revision's provider credentials onto the pools a provider call
//! leases from (#145).
//!
//! This is the runtime half of the credential lifecycle. The store half already
//! exists: a credential body pins one *exact* secret version, and
//! [`SecretMaterialization`](super::secrets::SecretMaterialization) unwraps every
//! version a candidate requires while that candidate is compiled. What was
//! missing is this: turning those resolved versions into `[[credential]]` entries,
//! so the material an administrator staged is what a provider call is actually
//! authenticated with.
//!
//! # Why this makes rotation a publication rather than a redeploy
//!
//! A credential entry this projection emits carries a
//! [`SecretRef`](crate::desired_state::SecretRef), not a value, and
//! [`Credentials::resolve`](crate::credentials::Credentials::resolve) fills it
//! from the candidate's resolved set. So:
//!
//! - **rotation** is a new revision naming a new version. It compiles, resolves
//!   the new version, and is published as a whole new snapshot — the pool a
//!   request leases from is replaced, never mutated;
//! - **staging** is not service. Only [`SecretLifecycle::Active`] material is
//!   projected, even though staged material resolves: staging exists so a
//!   candidate can be *compiled* against material before any traffic reaches it;
//! - **revocation and disabling** withdraw a credential from the next snapshot's
//!   pools without touching the snapshot that is serving, which keeps its own
//!   material alive until its last request finishes;
//! - **a version that cannot be resolved** refuses the candidate, so the last
//!   known good snapshot keeps serving. There is no partially credentialed pool.
//!
//! None of it restarts anything, and none of it happens on the request path.
//!
//! # Which namespace a credential serves
//!
//! The runtime's tenancy boundary is the namespace and pools are keyed
//! `(namespace, provider)` (ADR 0003, ADR 0006), so projecting a credential is
//! deciding which namespace owns it. The answer comes from what
//! [`TenancyProjection`] already established
//! rather than from a second mapping:
//!
//! - a **project-scoped** credential serves the namespace whose
//!   [`ProjectIdentity`] is its owner;
//! - a **tenant-scoped** credential serves every namespace of that tenant — a
//!   tenant-wide default — *except* a `(namespace, provider)` pair where that
//!   project has a credential of its own. Bring-your-own-key wins over the
//!   tenant's, for the same reason a namespace does not borrow the platform's
//!   without saying so. The claim is the *declaration*, not the material:
//!   disabling or revoking a project's own key empties that pool rather than
//!   silently promoting the tenant's, because a key that quietly starts billing
//!   another account — and would be the one a leak implicated — is not what
//!   withdrawing a key asks for. Deleting the credential releases the pair, so
//!   falling back to the tenant default is something an operator states. A
//!   `staged` key holds nothing: preparing a project's own key must not take that
//!   project off the tenant's before the new one can serve it. A key withdrawn
//!   *from* staging — staged, then disabled or revoked, which is how a key that
//!   leaked before activation is handled — does hold the pair, because a key
//!   pulled for cause is a reason to stop serving that provider rather than to
//!   move its traffic onto an account nobody nominated;
//! - a credential whose owner has no projected namespace (a tenant with no
//!   projects, or a suspended one, whose projects are deliberately not projected)
//!   is *not* projected. It is logged by reference and skipped rather than
//!   refused: withdrawing a tenant's traffic must not also stop every revision
//!   that mentions its credentials from compiling.
//!
//! What is deliberately *not* here: any authorization decision. Which principal
//! may create, rotate, or read a credential is #252's, and the projection reads
//! only the ownership the revision already recorded.
//!
//! # Which provider it authenticates to
//!
//! A credential body names a provider *resource*; a config credential names a
//! provider *id*. The bridge is the provider resource's readable slug, qualified
//! by durable ownership when that slug is reused, and the projection emits the
//! corresponding runtime provider id. The two declarations must agree on more
//! than the id: a credential
//! whose provider resource speaks one wire family while the `[[provider]]` of
//! that id is declared as another is refused, because presenting a key in a wire
//! family its account does not belong to is a key sent to the wrong upstream.
//! A credential naming a provider the deployment cannot dial is refused
//! rather than dropped: it is an operator-actionable mismatch between a published
//! revision and a file, and silently serving traffic with no credential for that
//! provider is the failure mode it would otherwise become.
//!
//! That refusal is scoped to credentials that would actually land in a pool,
//! which is why the namespaces a credential serves are computed first: a
//! withdrawn tenant's leftover key serves nothing, and letting it refuse the
//! candidate would turn one dormant tenant into a fleet that stops converging.
//!
//! A stateful file may not declare `[[provider]]` at all. The production
//! convergence chain constructs provider connections first, then projects these
//! credentials against their deterministic runtime ids. A credential that names
//! no durable connection is refused before publication.

use std::collections::{BTreeMap, HashSet};

use super::compile::{ProjectionError, RevisionProjection};
use super::principals::PrincipalProjection;
use super::tenancy::TenancyProjection;
use crate::config::{Config, Credential, ProjectIdentity, Provider, ProviderKind, ProviderWire};
use crate::desired_state::credentials::{Credentials, ProviderCredential};
use crate::desired_state::providers::{Provider as DurableProvider, Providers};
use crate::desired_state::{DesiredState, RevisionId, SecretLifecycle, SecretOwner, WireFamily};

/// Projects a revision's active provider credentials onto `[[credential]]`,
/// leaving every other section as it was given.
///
/// Runs *after* tenancy: it reads the namespaces the config carries and adds no
/// namespace of its own, so a credential can only ever land in a namespace some
/// other authority already decided exists.
#[derive(Debug, Clone, Copy, Default)]
pub struct CredentialProjection;

/// Projects durable provider connections onto the deployment-wide provider
/// table. A provider resource's slug is the stable human label; when two
/// tenants reuse it, the runtime id is qualified by durable ownership so one
/// endpoint cannot silently replace another.
#[derive(Debug, Clone, Copy, Default)]
pub struct ProviderProjection;

/// The production projection: tenancy, credentials, and inbound principals for
/// the namespaces they authenticate.
///
/// This projection intentionally does not project inbound gateway principals.
/// Stateful compilation reports that as a typed `unsupported` refusal instead
/// of constructing a keyless serving snapshot. Adding a principal source here
/// is the narrow point at which stateful serving can become Ready.
///
/// One type rather than a generic chain because the order is not a configuration
/// choice — a credential's namespace has to exist before the credential can name
/// it — and because a rejection should say which stage refused it.
#[derive(Debug, Clone, Copy, Default)]
pub struct RuntimeProjection;

impl RevisionProjection for RuntimeProjection {
    fn name(&self) -> &'static str {
        "runtime"
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
        let namespaces = TenancyProjection.project(bootstrap, state, source)?;
        let providers = ProviderProjection.project(&namespaces, state, source)?;
        let credentials = CredentialProjection.project(&providers, state, source)?;
        PrincipalProjection.project(&credentials, state, source)
    }
}

impl RevisionProjection for ProviderProjection {
    fn name(&self) -> &'static str {
        "providers"
    }

    fn project(
        &self,
        bootstrap: &Config,
        state: &DesiredState,
        _source: RevisionId,
    ) -> Result<Config, ProjectionError> {
        let providers = Providers::of(state).map_err(|error| ProjectionError::Body {
            reference: error.reference(),
            detail: error.to_string(),
        })?;
        let mut config = bootstrap.clone();
        let mut seen = HashSet::new();
        for provider in providers.all() {
            let id = runtime_provider_id(&providers, provider);
            if !seen.insert(id.clone()) {
                return Err(ProjectionError::Incomplete {
                    detail: format!(
                        "{} declares runtime provider id `{id}` more than once; one runtime provider \
                         id cannot safely choose between durable endpoints",
                        provider.reference
                    ),
                });
            }
            let kind = match provider.body.wire_family() {
                WireFamily::OpenaiChat => ProviderKind::OpenaiCompatible,
                WireFamily::AnthropicMessages => ProviderKind::Anthropic,
            };
            config.provider.push(Provider {
                id,
                kind,
                base_url: provider.body.endpoint().to_owned(),
            });
        }
        Ok(config)
    }
}

impl RevisionProjection for CredentialProjection {
    fn name(&self) -> &'static str {
        "credentials"
    }

    fn project(
        &self,
        bootstrap: &Config,
        state: &DesiredState,
        _source: RevisionId,
    ) -> Result<Config, ProjectionError> {
        let credentials = Credentials::of(state).map_err(|error| ProjectionError::Body {
            reference: error.reference(),
            detail: error.to_string(),
        })?;
        let providers = Providers::of(state).map_err(|error| ProjectionError::Body {
            reference: error.reference(),
            detail: error.to_string(),
        })?;
        let mut config = bootstrap.clone();
        let namespaces = ProjectedNamespaces::index(bootstrap);

        // Project-scoped credentials first, so the pairs they claim are known
        // before a tenant's defaults are considered for the same pairs. A key
        // claims its pair from the moment it is more than a preparation until it
        // is deleted: staging is traffic-neutral, a tombstone releases the pair.
        let (owned, inherited): (Vec<&ProviderCredential>, Vec<&ProviderCredential>) = credentials
            .all()
            .filter(|credential| claims(credential.body.lifecycle()))
            .partition(|credential| credential.body.project().is_some());

        let mut claimed: HashSet<(String, String)> = HashSet::new();
        for credential in &owned {
            // Namespaces first, so a credential that lands in no pool is skipped
            // before its provider is checked: an owner whose traffic is withdrawn
            // must not be able to refuse a revision it does not serve.
            let serves = serving(&namespaces, credential);
            if serves.is_empty() {
                continue;
            }
            let active = credential.body.lifecycle() == SecretLifecycle::Active;
            let provider = match provider_id(&config, &providers, credential) {
                Ok(provider) => provider,
                // A withdrawn credential for a provider this deployment cannot
                // dial holds no pool open, and refusing the candidate for it
                // would make disabling a key a way to stop convergence.
                Err(_) if !active => continue,
                Err(error) => return Err(error),
            };
            for namespace in serves {
                claimed.insert((namespace.to_owned(), provider.clone()));
                if active {
                    config
                        .credential
                        .push(entry(namespace, &provider, credential));
                }
            }
        }
        for credential in &inherited {
            if credential.body.lifecycle() != SecretLifecycle::Active {
                continue;
            }
            let serves = serving(&namespaces, credential);
            if serves.is_empty() {
                continue;
            }
            let provider = provider_id(&config, &providers, credential)?;
            for namespace in serves {
                if claimed.contains(&(namespace.to_owned(), provider.clone())) {
                    // The project brought its own key for this provider, whether
                    // or not that key is currently serving.
                    continue;
                }
                config
                    .credential
                    .push(entry(namespace, &provider, credential));
            }
        }
        Ok(config)
    }
}

/// Whether a credential in `lifecycle` holds the pool it names.
///
/// Total, so a new lifecycle cannot be added without deciding whether it takes a
/// pool over. `Staged` does not: preparing a project's own key must not take that
/// project's traffic off the tenant default before the key can serve it.
/// `Tombstoned` does not either — the material is gone.
///
/// `Disabled` and `Revoked` do, whether or not the key ever served: a lifecycle
/// records what may be done with material, not a history, and withdrawing a key
/// staged for a project is still that project saying which account this provider
/// is paid for through. Falling back is an operator's statement — a deletion.
const fn claims(lifecycle: SecretLifecycle) -> bool {
    match lifecycle {
        SecretLifecycle::Active | SecretLifecycle::Disabled | SecretLifecycle::Revoked => true,
        SecretLifecycle::Staged | SecretLifecycle::Tombstoned => false,
    }
}

/// Every projected namespace `credential` serves, or none — logged by reference,
/// because an owner with no projected namespace (a tenant with no projects, or a
/// suspended one) is a fact about tenancy, not a reason to refuse a revision.
fn serving<'a>(
    namespaces: &'a ProjectedNamespaces,
    credential: &ProviderCredential,
) -> Vec<&'a str> {
    let serves = namespaces.serving(credential.body.owner());
    if serves.is_empty() {
        tracing::debug!(
            credential = %credential.reference,
            "a credential whose owner has no projected namespace is not projected"
        );
    }
    serves
}

/// One `[[credential]]` entry: a namespace, a provider, an attribution label, and
/// an opaque reference to the version its material comes from.
fn entry(namespace: &str, provider: &str, credential: &ProviderCredential) -> Credential {
    Credential {
        namespace: namespace.to_owned(),
        provider: provider.to_owned(),
        // Projected credentials carry no env var: their material is the secret
        // store's, and this entry names the exact version it is unwrapped from.
        env: None,
        secret: Some(credential.body.secret()),
        // The slug, not the resource id: this label is what an operator reads on
        // a usage record and in a pool status, and it is a reference either way.
        id: Some(credential.slug.as_str().to_owned()),
        weight: 1,
    }
}

/// The config provider id a credential authenticates to.
fn provider_id(
    config: &Config,
    providers: &Providers,
    credential: &ProviderCredential,
) -> Result<String, ProjectionError> {
    let declared =
        providers
            .get(credential.body.provider())
            .ok_or_else(|| ProjectionError::Incomplete {
                detail: format!(
                    "{} authenticates to provider `{}`, which this revision does not declare",
                    credential.reference,
                    credential.body.provider()
                ),
            })?;
    let provider_id = runtime_provider_id(providers, declared);
    let Some(bootstrap) = config
        .provider
        .iter()
        .find(|provider| provider.id == provider_id)
    else {
        return Err(ProjectionError::Incomplete {
            detail: format!(
                "{} authenticates to provider `{provider_id}`, which this deployment does not declare: a \
                 provider's endpoint and wire family are still bootstrap-owned, so a credential \
                 for a provider no `[[provider]]` names could not dial anything",
                credential.reference
            ),
        });
    };
    // The runtime id matching is not enough: the two declarations must also agree on
    // what they are talking to. A key presented in the wrong wire family's
    // request is a credential leaked to the wrong upstream account, so the
    // mismatch is refused for the same reason an undeclared provider is.
    let wire = wire(declared.body.wire_family());
    if bootstrap.kind.wire() != wire {
        return Err(ProjectionError::Incomplete {
            detail: format!(
                "{} authenticates to provider `{provider_id}`, which this revision speaks {} to while \
                 this deployment declares it as {}: a credential must not be presented in a wire \
                 family its account does not belong to",
                credential.reference,
                wire,
                bootstrap.kind.wire()
            ),
        });
    }
    Ok(provider_id)
}

/// The config id for one durable provider connection. A unique slug stays
/// readable; a reused slug carries the owner's durable scope, making the
/// mapping deterministic across replicas and safe across tenants.
pub(crate) fn runtime_provider_id(providers: &Providers, provider: &DurableProvider) -> String {
    let duplicate = providers
        .all()
        .filter(|other| other.slug == provider.slug)
        .nth(1)
        .is_some();
    if !duplicate {
        return provider.slug.as_str().to_owned();
    }
    match provider.body.project() {
        Some(project) => format!("{}@{}/{}", provider.slug, provider.body.tenant(), project),
        None => format!("{}@{}", provider.slug, provider.body.tenant()),
    }
}

/// The wire a family is spoken over. Total, so a new family cannot be added
/// without deciding what this projection does with it.
const fn wire(family: WireFamily) -> ProviderWire {
    match family {
        WireFamily::OpenaiChat => ProviderWire::Openai,
        WireFamily::AnthropicMessages => ProviderWire::Anthropic,
    }
}

/// The projected namespaces of a config, indexed by what they *are* rather than
/// by what they are called: a rename moves the name, never the binding.
struct ProjectedNamespaces {
    by_identity: BTreeMap<ProjectIdentity, String>,
}

impl ProjectedNamespaces {
    fn index(config: &Config) -> Self {
        Self {
            by_identity: config
                .namespace
                .iter()
                .filter_map(|namespace| {
                    namespace
                        .project
                        .map(|identity| (identity, namespace.id.clone()))
                })
                .collect(),
        }
    }

    /// Every namespace `owner` serves: one for a project, all of a tenant's for a
    /// tenant. Ordered by identity, so a pool's entries are in the same order on
    /// every replica.
    fn serving(&self, owner: SecretOwner) -> Vec<&str> {
        match owner.project {
            Some(project) => self
                .by_identity
                .get(&ProjectIdentity {
                    tenant: owner.tenant,
                    project,
                })
                .map(String::as_str)
                .into_iter()
                .collect(),
            None => self
                .by_identity
                .iter()
                .filter(|(identity, _)| identity.tenant == owner.tenant)
                .map(|(_, id)| id.as_str())
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::compile::testing::bootstrap;
    use super::*;
    use std::collections::HashMap;

    use crate::credentials::Credentials as Pools;
    use crate::desired_state::credentials::ProviderCredentialBody;
    use crate::desired_state::fixtures;
    use crate::desired_state::{
        ProjectId, ProviderBody, ResourceVersion, SecretRef, Slug, TenantId, WireFamily,
    };

    const TENANT: u64 = 1;
    const PROJECT: u64 = 2;
    const CREDENTIAL: u64 = 3;

    fn tenant() -> TenantId {
        fixtures::tenant_id(TENANT)
    }

    fn project() -> ProjectId {
        fixtures::project_id(PROJECT)
    }

    /// The provider connection every credential here authenticates to, whose slug
    /// matches the bootstrap's one `[[provider]]`.
    fn connection() -> ResourceVersion {
        ProviderBody::for_tenant(
            fixtures::provider_id(CREDENTIAL),
            tenant(),
            fixtures::display_name("OpenAI"),
            WireFamily::OpenaiChat,
            "https://api.openai.com/v1",
        )
        .version(Slug::parse("openai").expect("fixture slug"))
    }

    /// A credential in `lifecycle`, owned by `owner`, pinning `secret`.
    fn credential(
        seed: u64,
        slug: &str,
        owner: SecretOwner,
        secret: SecretRef,
        lifecycle: SecretLifecycle,
    ) -> ResourceVersion {
        let mut body = ProviderCredentialBody::staged(
            fixtures::resource_id(seed),
            owner,
            fixtures::provider_id(CREDENTIAL),
            fixtures::display_name(slug),
            secret,
        );
        if lifecycle != SecretLifecycle::Staged {
            body = walk(&body, lifecycle);
        }
        body.version(Slug::parse(slug).expect("fixture slug"))
    }

    /// `body`, walked to `lifecycle` through the transitions the domain permits —
    /// nothing here may invent a state the lifecycle would refuse.
    fn walk(body: &ProviderCredentialBody, lifecycle: SecretLifecycle) -> ProviderCredentialBody {
        let active = body
            .transitioned(SecretLifecycle::Active)
            .expect("staged material may be activated");
        match lifecycle {
            SecretLifecycle::Staged => body.clone(),
            SecretLifecycle::Active => active,
            // Terminal only from revoked: a tombstone is a deletion of material
            // already taken out of service, never a shortcut around revocation.
            SecretLifecycle::Tombstoned => active
                .transitioned(SecretLifecycle::Revoked)
                .and_then(|revoked| revoked.transitioned(SecretLifecycle::Tombstoned))
                .expect("a revoked version may be tombstoned"),
            other => active.transitioned(other).expect("a permitted transition"),
        }
    }

    /// A tenant with one project, one provider connection, and `credentials`.
    fn state(credentials: impl IntoIterator<Item = ResourceVersion>) -> DesiredState {
        let mut state = DesiredState::new();
        state
            .insert(fixtures::tenant(TENANT, "acme"))
            .and_then(|state| state.insert(fixtures::project(&tenant(), PROJECT, "core")))
            .and_then(|state| state.insert(connection()))
            .expect("a valid tenancy");
        for credential in credentials {
            state.insert(credential).expect("a distinct reference");
        }
        state
    }

    /// The projected pool entries, as `(namespace, provider, label, version)`.
    ///
    /// A `SecretRef` rather than a value on purpose: what a pool entry carries is
    /// the reference, and asserting on it is asserting the pinning.
    fn projected(state: &DesiredState) -> Vec<(String, String, String, Option<SecretRef>)> {
        RuntimeProjection
            .project(&bootstrap(), state, fixtures::revision_id(3))
            .expect("a projectable revision")
            .credential
            .into_iter()
            .map(|credential| {
                let label = credential.label().to_owned();
                (
                    credential.namespace,
                    credential.provider,
                    label,
                    credential.secret,
                )
            })
            .collect()
    }

    #[test]
    fn an_active_credential_becomes_the_pool_entry_its_namespace_leases_from() {
        let entries = projected(&state([credential(
            CREDENTIAL,
            "primary",
            SecretOwner::project(tenant(), project()),
            fixtures::secret_ref(CREDENTIAL),
            SecretLifecycle::Active,
        )]));

        assert_eq!(
            entries,
            [(
                "acme/core".to_owned(),
                "openai".to_owned(),
                "primary".to_owned(),
                Some(fixtures::secret_ref(CREDENTIAL)),
            )],
            "an active credential serves its project's namespace, pinned to its own version"
        );
    }

    /// Staged material resolves — that is how a candidate is compiled against it —
    /// but resolving is not serving, and only an activation puts it in a pool.
    #[test]
    fn staged_material_is_not_projected_onto_any_pool() {
        assert_eq!(
            projected(&state([credential(
                CREDENTIAL,
                "primary",
                SecretOwner::project(tenant(), project()),
                fixtures::secret_ref(CREDENTIAL),
                SecretLifecycle::Staged,
            )])),
            [],
        );
    }

    #[test]
    fn disabling_or_revoking_withdraws_a_credential_from_the_next_snapshot() {
        for lifecycle in [
            SecretLifecycle::Disabled,
            SecretLifecycle::Revoked,
            SecretLifecycle::Tombstoned,
        ] {
            assert_eq!(
                projected(&state([credential(
                    CREDENTIAL,
                    "primary",
                    SecretOwner::project(tenant(), project()),
                    fixtures::secret_ref(CREDENTIAL),
                    lifecycle,
                )])),
                [],
                "{lifecycle:?} material must not be projected onto a pool"
            );
        }
    }

    /// A rotation is a different pinned version, and nothing else: same namespace,
    /// same provider, new reference. The old snapshot is not touched, because a
    /// projection produces a *new* config every time.
    #[test]
    fn a_rotation_moves_the_pool_to_the_successor_version() {
        let versions = |secret| {
            projected(&state([credential(
                CREDENTIAL,
                "primary",
                SecretOwner::project(tenant(), project()),
                secret,
                SecretLifecycle::Active,
            )]))
        };

        let before = versions(fixtures::secret_ref_at(CREDENTIAL, 1));
        let after = versions(fixtures::secret_ref_at(CREDENTIAL, 2));
        assert_eq!(before[0].3, Some(fixtures::secret_ref_at(CREDENTIAL, 1)));
        assert_eq!(after[0].3, Some(fixtures::secret_ref_at(CREDENTIAL, 2)));
        assert_eq!(
            (&before[0].0, &before[0].1),
            (&after[0].0, &after[0].1),
            "a rotation changes the version a pool names, not which pool it is"
        );
    }

    /// Two active credentials for one provider are two keys in one pool, each
    /// pinned to its own version. A rotation's *overlap* is this: the outgoing
    /// credential keeps serving while its replacement is activated, and neither
    /// entry knows anything about the other's material.
    #[test]
    fn two_active_credentials_become_two_keys_in_one_pool() {
        let entries = projected(&state([
            credential(
                CREDENTIAL,
                "primary",
                SecretOwner::project(tenant(), project()),
                fixtures::secret_ref_at(CREDENTIAL, 1),
                SecretLifecycle::Active,
            ),
            credential(
                CREDENTIAL + 10,
                "successor",
                SecretOwner::project(tenant(), project()),
                fixtures::secret_ref(CREDENTIAL + 10),
                SecretLifecycle::Active,
            ),
        ]));

        assert_eq!(
            entries
                .iter()
                .map(|entry| (entry.2.as_str(), entry.3))
                .collect::<Vec<_>>(),
            [
                ("primary", Some(fixtures::secret_ref_at(CREDENTIAL, 1))),
                ("successor", Some(fixtures::secret_ref(CREDENTIAL + 10))),
            ],
        );
    }

    /// A tenant's credential is a default for its projects, and a project's own
    /// credential replaces it for that provider rather than joining it: a
    /// bring-your-own key is not a second attempt after the tenant's.
    #[test]
    fn a_projects_own_credential_replaces_its_tenants_default() {
        let mut state = state([
            credential(
                CREDENTIAL,
                "tenant-wide",
                SecretOwner::tenant(tenant()),
                fixtures::secret_ref_at(CREDENTIAL, 1),
                SecretLifecycle::Active,
            ),
            credential(
                CREDENTIAL + 10,
                "project-own",
                SecretOwner::project(tenant(), project()),
                fixtures::secret_ref(CREDENTIAL + 10),
                SecretLifecycle::Active,
            ),
        ]);
        assert_eq!(
            projected(&state)
                .iter()
                .map(|entry| (entry.0.clone(), entry.2.clone()))
                .collect::<Vec<_>>(),
            [("acme/core".to_owned(), "project-own".to_owned())],
        );

        // A second project of the same tenant has no key of its own, so the
        // tenant's default is what serves it.
        state
            .insert(fixtures::project(&tenant(), PROJECT + 20, "labs"))
            .expect("a distinct reference");
        assert_eq!(
            projected(&state)
                .iter()
                .map(|entry| (entry.0.clone(), entry.2.clone()))
                .collect::<Vec<_>>(),
            [
                ("acme/core".to_owned(), "project-own".to_owned()),
                ("acme/labs".to_owned(), "tenant-wide".to_owned()),
            ],
        );
    }

    /// And withdrawing that key does not hand its traffic to the tenant's. A
    /// disabled or revoked project credential still claims its pool: the pool
    /// empties, which fails calls loudly, rather than quietly billing the
    /// tenant's account and putting the tenant's key on the wire. Deleting the
    /// credential is the explicit statement that the default may take over.
    #[test]
    fn withdrawing_a_projects_own_credential_does_not_promote_its_tenants_default() {
        let tenant_wide = || {
            credential(
                CREDENTIAL,
                "tenant-wide",
                SecretOwner::tenant(tenant()),
                fixtures::secret_ref_at(CREDENTIAL, 1),
                SecretLifecycle::Active,
            )
        };
        let project_own = |lifecycle| {
            credential(
                CREDENTIAL + 10,
                "project-own",
                SecretOwner::project(tenant(), project()),
                fixtures::secret_ref(CREDENTIAL + 10),
                lifecycle,
            )
        };

        for withdrawn in [SecretLifecycle::Disabled, SecretLifecycle::Revoked] {
            assert_eq!(
                projected(&state([tenant_wide(), project_own(withdrawn)])),
                [],
                "{withdrawn:?} must empty the pool, not fall back"
            );
        }

        // Withdrawn straight out of staging — a key that leaked before it ever
        // served — the pair is still the project's: pulling a key for cause is a
        // reason to stop calling the provider, not to bill the tenant's account.
        for withdrawn in [SecretLifecycle::Disabled, SecretLifecycle::Revoked] {
            let body = ProviderCredentialBody::staged(
                fixtures::resource_id(CREDENTIAL + 10),
                SecretOwner::project(tenant(), project()),
                fixtures::provider_id(CREDENTIAL),
                fixtures::display_name("project-own"),
                fixtures::secret_ref(CREDENTIAL + 10),
            )
            .transitioned(withdrawn)
            .expect("staged material may be withdrawn without serving");
            assert_eq!(
                projected(&state([
                    tenant_wide(),
                    body.version(Slug::parse("project-own").expect("fixture slug")),
                ])),
                [],
                "a never-activated {withdrawn:?} key still holds its pair"
            );
        }

        // Preparing that key claims nothing: the tenant's default keeps serving
        // until the project's own key is activated, so staging is traffic-neutral.
        assert_eq!(
            projected(&state([
                tenant_wide(),
                project_own(SecretLifecycle::Staged)
            ]))
            .iter()
            .map(|entry| (entry.0.clone(), entry.2.clone()))
            .collect::<Vec<_>>(),
            [("acme/core".to_owned(), "tenant-wide".to_owned())],
        );

        // Deleted, the claim is released and the tenant's default serves again.
        assert_eq!(
            projected(&state([
                tenant_wide(),
                project_own(SecretLifecycle::Tombstoned)
            ]))
            .iter()
            .map(|entry| (entry.0.clone(), entry.2.clone()))
            .collect::<Vec<_>>(),
            [("acme/core".to_owned(), "tenant-wide".to_owned())],
        );
    }

    /// Cross-tenant isolation, at the projection: a tenant's credential reaches
    /// its own tenant's namespaces and no other's, whatever provider they share.
    #[test]
    fn a_tenants_credential_never_lands_in_another_tenants_pool() {
        let mut state = state([credential(
            CREDENTIAL,
            "acme-key",
            SecretOwner::tenant(tenant()),
            fixtures::secret_ref(CREDENTIAL),
            SecretLifecycle::Active,
        )]);
        let other = fixtures::tenant_id(9);
        state
            .insert(fixtures::tenant(9, "globex"))
            .and_then(|state| state.insert(fixtures::project(&other, 12, "core")))
            .expect("a distinct tenant");

        let namespaces: Vec<String> = projected(&state).into_iter().map(|entry| entry.0).collect();
        assert_eq!(namespaces, ["acme/core"]);
    }

    /// A tenant with no project has no namespace, so its credential is skipped
    /// rather than refused: withdrawing a tenant's traffic must not stop every
    /// revision that mentions its credentials from compiling.
    #[test]
    fn a_credential_whose_owner_serves_no_namespace_is_skipped_not_refused() {
        let mut state = DesiredState::new();
        state
            .insert(fixtures::tenant(TENANT, "acme"))
            .and_then(|state| state.insert(connection()))
            .and_then(|state| {
                state.insert(credential(
                    CREDENTIAL,
                    "primary",
                    SecretOwner::tenant(tenant()),
                    fixtures::secret_ref(CREDENTIAL),
                    SecretLifecycle::Active,
                ))
            })
            .expect("a valid revision");

        assert_eq!(projected(&state), []);
    }

    /// And it is skipped *before* its provider is checked. Otherwise a withdrawn
    /// tenant's leftover key, naming a provider this deployment does not declare,
    /// would refuse every revision — one dormant tenant stopping the fleet from
    /// converging on a credential that serves nothing.
    #[test]
    fn a_credential_serving_no_namespace_does_not_refuse_for_its_provider() {
        let mut state = DesiredState::new();
        let elsewhere = ProviderBody::for_tenant(
            fixtures::provider_id(CREDENTIAL),
            tenant(),
            fixtures::display_name("Elsewhere"),
            WireFamily::OpenaiChat,
            "https://elsewhere.example/v1",
        )
        .version(Slug::parse("elsewhere").expect("fixture slug"));
        state
            .insert(fixtures::tenant(TENANT, "acme"))
            .and_then(|state| state.insert(elsewhere))
            .and_then(|state| {
                state.insert(credential(
                    CREDENTIAL,
                    "primary",
                    SecretOwner::tenant(tenant()),
                    fixtures::secret_ref(CREDENTIAL),
                    SecretLifecycle::Active,
                ))
            })
            .expect("a valid revision");

        assert_eq!(projected(&state), []);
    }

    /// Provider connections are projected from durable state now, so a
    /// credential can bring its matching endpoint into a stateful candidate.
    #[test]
    fn a_durable_provider_connection_is_projected_for_its_credential() {
        let mut state = DesiredState::new();
        let connection = ProviderBody::for_tenant(
            fixtures::provider_id(CREDENTIAL),
            tenant(),
            fixtures::display_name("Elsewhere"),
            WireFamily::OpenaiChat,
            "https://elsewhere.example/v1",
        )
        .version(Slug::parse("elsewhere").expect("fixture slug"));
        state
            .insert(fixtures::tenant(TENANT, "acme"))
            .and_then(|state| state.insert(fixtures::project(&tenant(), PROJECT, "core")))
            .and_then(|state| state.insert(connection))
            .and_then(|state| {
                state.insert(credential(
                    CREDENTIAL,
                    "primary",
                    SecretOwner::project(tenant(), project()),
                    fixtures::secret_ref(CREDENTIAL),
                    SecretLifecycle::Active,
                ))
            })
            .expect("a valid revision");

        let config = RuntimeProjection
            .project(&bootstrap(), &state, fixtures::revision_id(3))
            .expect("the durable connection supplies the provider endpoint");
        assert_eq!(
            config
                .provider
                .iter()
                .map(|p| p.id.as_str())
                .collect::<Vec<_>>(),
            ["openai", "elsewhere",]
        );
        assert_eq!(config.credential[0].provider, "elsewhere");
    }

    /// And matching the id is not enough. A revision whose provider speaks
    /// Anthropic, projected onto the OpenAI-kind `[[provider]]` of that id, would
    /// present an Anthropic key in an OpenAI-shaped request: the key goes to an
    /// account it does not belong to, so the candidate is refused instead.
    #[test]
    fn a_credential_whose_provider_speaks_another_wire_family_refuses() {
        let mut state = DesiredState::new();
        let mismatched = ProviderBody::for_tenant(
            fixtures::provider_id(CREDENTIAL),
            tenant(),
            fixtures::display_name("OpenAI"),
            WireFamily::AnthropicMessages,
            "https://api.openai.com/v1",
        )
        .version(Slug::parse("openai").expect("fixture slug"));
        state
            .insert(fixtures::tenant(TENANT, "acme"))
            .and_then(|state| state.insert(fixtures::project(&tenant(), PROJECT, "core")))
            .and_then(|state| state.insert(mismatched))
            .and_then(|state| {
                state.insert(credential(
                    CREDENTIAL,
                    "primary",
                    SecretOwner::project(tenant(), project()),
                    fixtures::secret_ref(CREDENTIAL),
                    SecretLifecycle::Active,
                ))
            })
            .expect("a valid revision");

        let Err(error) = RuntimeProjection.project(&bootstrap(), &state, fixtures::revision_id(3))
        else {
            panic!("a wire family mismatch must refuse the candidate");
        };
        assert!(
            matches!(error, ProjectionError::Incomplete { .. }),
            "{error:?}"
        );
    }

    /// The pool half of the contract, without a store: a projected entry names a
    /// version, and a candidate that did not resolve that version cannot build a
    /// snapshot from it. This is what keeps a half-credentialed pool from ever
    /// being published.
    #[test]
    fn a_pool_entry_whose_version_was_not_resolved_refuses_to_build() {
        let config = RuntimeProjection
            .project(
                &bootstrap(),
                &state([credential(
                    CREDENTIAL,
                    "primary",
                    SecretOwner::project(tenant(), project()),
                    fixtures::secret_ref(CREDENTIAL),
                    SecretLifecycle::Active,
                )]),
                fixtures::revision_id(3),
            )
            .expect("a projectable revision");

        let Err(error) = Pools::resolve(
            &config,
            &HashMap::new(),
            &crate::convergence::secrets::ResolvedSecrets::default(),
        ) else {
            panic!("an unresolved version must not yield a pool");
        };
        let rendered = error.to_string();
        assert!(
            rendered.contains(&fixtures::secret_ref(CREDENTIAL).to_string()),
            "the refusal names the version by reference: {rendered}"
        );
    }
}
