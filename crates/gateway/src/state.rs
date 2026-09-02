//! Shared application state.
//!
//! State splits in two. Process-level resources — the HTTP client pool, the
//! connected usage sinks, the budget store — are built once at boot and live for
//! the process. Everything *derived from the config file* lives in a
//! [`ConfigSnapshot`] behind an [`ArcSwap`], so a reload publishes a whole new
//! snapshot in one atomic store (ADR 0011).
//!
//! Readers take the snapshot once, at the top of a request, and hold that `Arc`
//! for the request's lifetime. A request therefore resolves its alias, its
//! credential, and its circuit against one consistent config, even if a reload
//! lands mid-flight.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use gateway_core::{
    AnthropicAdapter, CircuitBreaker, OpenAiCompatibleAdapter, OpenAiFlavor, ProviderAdapter,
};
use gateway_transport::{HttpDispatcher, build_client};
use secrecy::zeroize::Zeroize;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};

use crate::admission::{AdmissionControl, DiagnosticCredential};
use crate::aliases::AliasScope;
use crate::availability::{AvailabilityIndex, AvailabilityReader, RuntimeObservations};
use crate::backends::catalog::CatalogReport;
use crate::backends::catalog_runtime::CatalogStatus;
use crate::backends::control_plane::ControlPlaneStore;
use crate::backends::health::BackendHealth;
use crate::backends::secrets::SecretMaterial;
use crate::budget::BudgetStore;
use crate::config::{
    CatalogBinding, Config, GatewayVerifierAlgorithm, Namespace, NamespacePolicy,
    NamespaceStaticPolicy, ProjectIdentity, ProjectedPrincipal, ProviderKind, StorageBackend,
};
use crate::convergence::SystemClock;
use crate::convergence::secrets::{MaterialLedger, ResolvedSecretBinding, ResolvedSecrets};
use crate::convergence::{RevisionReport, RevisionStatus};
use crate::credentials::{CredentialError, Credentials};
use crate::desired_state::mutation::Actor;
use crate::desired_state::policy::{
    BudgetPolicy, BufferedResponseRoute, ConcurrencyPolicy, PolicyBody, PolicyEpoch, PolicyScope,
    RevocationPolicy,
};
use crate::desired_state::pricing::{
    Approval, EffectiveInstant, EffectiveInterval, PricedTarget, PricingSnapshot,
};
use crate::desired_state::tenancy::DisplayName;
use crate::desired_state::{
    AuthorizationSnapshot, Checksum, ResourceId, ResourceKind, ResourceRef, ResourceVersionNumber,
};
use crate::desired_state::{ProjectId, RevisionId, SecretRef, TenantId, WorkloadKey};
use crate::key_material::{self, KeyMaterialError};
use crate::middleware::{MiddlewareChain, MiddlewarePlan, MiddlewarePlanError, MiddlewareRuntime};
use crate::policy::PolicyRuntime;
use crate::principals::{
    Capability, ConfigPrincipals, GatewayKeyEntry, NamespaceEpoch, Presented, PrincipalAuthority,
    PrincipalShapeError, PrincipalStoreChain, ProjectedPrincipals, TokenVerifier,
    TokenVerifierBuildError, configured_token_epochs, resolve_token_epoch,
};
#[cfg(test)]
use crate::rate_limit::NoLimit;
use crate::rate_limit::RateLimiter;
use crate::revocation::RevocationStore;
use crate::shutdown::Lifecycle;
use crate::status::Component;
use crate::status::probes::{BackendProbe, CatalogProbe, ControlPlaneProbe};
use crate::status::registry::{
    CachedStatusRegistry, ObservationPlan, StatusRefresher, StatusSettings,
};
use crate::store::Store;
use crate::usage::UsageDelivery;
#[cfg(test)]
use crate::usage::UsageFanout;

pub use crate::principals::InboundKey;

#[derive(Clone)]
pub struct AppState(pub Arc<Inner>);

pub struct Inner {
    pub dispatcher: HttpDispatcher,
    /// Process-level grace for byte-faithful provider bytes that follow a
    /// semantic terminal event. Read at boot with the rest of `[transport]`.
    pub stream_terminal_grace: Duration,
    /// How a terminated request's usage leaves the process. Telemetry-grade by
    /// default; a durable append when a journal is configured.
    pub usage: Arc<UsageDelivery>,
    pub budget: Box<dyn BudgetStore>,
    /// Process-level ceilings. Like the HTTP client's bounds these own state
    /// built at boot, so a reloaded `[admission]` section applies on restart.
    pub admission: AdmissionControl,
    pub rate_limiter: Box<dyn RateLimiter>,
    pub revocation: Box<dyn RevocationStore>,
    /// The stateful policy this replica is enforcing, and the holds outstanding
    /// under each generation of it (#150). Process-level like the stores that
    /// read it: a publication replaces the values, never the connections.
    pub policy: Arc<PolicyRuntime>,
    /// Drain state and the in-flight count. Process-level, like the sinks: a
    /// reload replaces what a request is served *with*, never whether the
    /// process is still accepting requests at all.
    pub lifecycle: Arc<Lifecycle>,
    /// What the authenticated status view reads. Process-level and *cached*: the
    /// registry is filled by a background refresher, so a status request reads a
    /// map rather than a backend (ADR 0031).
    pub status: Arc<CachedStatusRegistry>,
    /// This replica's convergence state, when it converges against a control
    /// plane at all. `None` in the stateless posture, where a replica serves the
    /// file it booted from and there is no revision to lag behind — and `None`
    /// in every shipped binary today, because no release constructs a
    /// reconciler at all (#142). The slice that does must hand *its* status
    /// handle here and to
    /// [`AdminApi::with_convergence`](crate::admin::router::AdminApi::with_convergence):
    /// two instances would let one replica tell two convergence stories.
    pub revision: Option<Arc<RevisionStatus>>,
    /// What the background catalogue import last reported, when this deployment
    /// imports one at all. A read of a mutex over a bounded report: the request
    /// path never reaches the source or the store, and holding this handle is
    /// what makes that structural rather than a rule (ADR 0043).
    pub catalogue: Option<Arc<CatalogStatus>>,
    /// Durable namespace (and later budget) store. Required (ADR 0063).
    pub store: std::sync::Arc<dyn crate::store::Store>,
    /// Constructor-only override for primitive tests. Shipped chains live only
    /// in [`ConfigSnapshot`].
    #[cfg(test)]
    pub middleware: MiddlewareChain,
    /// Global and per-id blocking capacity for content middleware. This
    /// is process-owned in production and isolated per constructed test state.
    pub middleware_runtime: MiddlewareRuntime,
    config: ArcSwap<ConfigSnapshot>,
}

/// What a replica reports about itself, as distinct from what it serves.
///
/// Passed in rather than built inside [`AppState::new_with_policy`] because
/// the two fields have no stateless implementation to default to *usefully*: a
/// stateless replica has an all-`disabled` registry and no convergence, and a
/// stateful one is handed the registry its probes publish into and the status the
/// reconciler writes.
pub struct ReplicaObservability {
    pub status: Arc<CachedStatusRegistry>,
    pub revision: Option<Arc<RevisionStatus>>,
    pub catalogue: Option<Arc<CatalogStatus>>,
}

impl ReplicaObservability {
    /// The stateless posture: every component `disabled`, nothing probed, no
    /// revision.
    pub fn stateless() -> Self {
        Self {
            status: Arc::new(CachedStatusRegistry::stateless()),
            revision: None,
            catalogue: None,
        }
    }

    /// The posture this replica's own configuration implies: every dependency it
    /// opened is observed, and every component it does not have stays `disabled`.
    ///
    /// The plan carries both halves of that — the enabled set and the probes —
    /// because they are one fact, and the pacing in it comes from the stores' own
    /// timeouts ([`ObservationPlan::observe`]) rather than from a default: a probe
    /// cut off before its backend's bounds have elapsed publishes a timeout the
    /// backend never had.
    ///
    /// The refresher is returned rather than spawned so the caller owns its
    /// lifetime: it has to stop with the process, and a task spawned out of a
    /// constructor would outlive the drain that is supposed to end it. It is
    /// `None` for a plan with nothing in it, which is the stateless posture: a
    /// loop observing no components would be a timer that publishes nothing.
    pub fn observing(plan: ObservationPlan) -> (Self, Option<StatusRefresher>) {
        if plan.is_empty() {
            return (Self::stateless(), None);
        }
        let (pacing, probes) = plan.into_parts();
        debug_assert!(pacing.validate().is_ok(), "the derived pacing is valid");
        let status = Arc::new(CachedStatusRegistry::new(pacing, Arc::new(SystemClock)));
        let refresher = StatusRefresher::new(Arc::clone(&status), probes);
        (
            Self {
                status,
                // Still `None`: no release constructs a reconciler, so there is
                // no convergence state to report and an empty report would be a
                // false all-clear (#142).
                revision: None,
                catalogue: None,
            },
            Some(refresher),
        )
    }

    /// Attach the one convergence report the reconciler writes.
    #[must_use]
    pub fn with_revision(mut self, revision: Arc<RevisionStatus>) -> Self {
        self.revision = Some(revision);
        self
    }

    /// The plan a deployment's own stores imply: one probe per dependency that
    /// exposes a reachability handle, and nothing for the backends that have none.
    ///
    /// The mapping from store to [`Component`] is made here, in one place, rather
    /// than by each store naming its own component: one Redis server can back the
    /// caps, the leases, and the denylist at once, and which of those a given
    /// handle speaks for is the deployment's arrangement, not the store's.
    pub fn plan(
        control_plane: Option<(Arc<dyn ControlPlaneStore>, StatusSettings)>,
        budget: &dyn BudgetStore,
        rate_limiter: &dyn RateLimiter,
        revocation: &dyn RevocationStore,
    ) -> ObservationPlan {
        let mut plan = ObservationPlan::stateless();
        if let Some((store, pacing)) = control_plane {
            plan.observe(Arc::new(ControlPlaneProbe::new(store)), pacing);
        }
        let request_path: [(Component, Option<Arc<dyn BackendHealth>>); 3] = [
            (Component::BudgetStore, budget.health()),
            (Component::RateLimitStore, rate_limiter.health()),
            (Component::RevocationStore, revocation.health()),
        ];
        for (component, health) in request_path {
            let Some(health) = health else {
                continue;
            };
            let pacing = BackendProbe::pacing(component, &health);
            plan.observe(Arc::new(BackendProbe::new(component, health)), pacing);
        }
        plan
    }

    /// Extend the deployment-derived observation plan with the bounded
    /// process-local catalogue report, when one is running.
    pub fn plan_with_catalogue(
        control_plane: Option<(Arc<dyn ControlPlaneStore>, StatusSettings)>,
        budget: &dyn BudgetStore,
        rate_limiter: &dyn RateLimiter,
        revocation: &dyn RevocationStore,
        catalogue: Option<Arc<CatalogStatus>>,
    ) -> ObservationPlan {
        let mut plan = Self::plan(control_plane, budget, rate_limiter, revocation);
        if let Some(catalogue) = catalogue {
            let mut pacing = StatusSettings::default();
            pacing.enabled.push(Component::Catalogue);
            plan.observe(Arc::new(CatalogProbe::new(catalogue)), pacing);
        }
        plan
    }

    /// Report on the catalogue the background import is keeping current.
    ///
    /// Separate from the constructors because catalogue imports are orthogonal to
    /// the mode: a stateless deployment may import metadata into a development
    /// store, and a stateful one may import none at all.
    #[must_use]
    pub fn with_catalogue(mut self, catalogue: Arc<CatalogStatus>) -> Self {
        self.catalogue = Some(catalogue);
        self
    }

    /// The stateless posture with a process-local catalogue import.
    #[cfg(test)]
    pub fn stateless_with_catalogue(catalogue: Arc<CatalogStatus>) -> (Self, StatusRefresher) {
        let mut settings = StatusSettings::default();
        settings.enabled.push(Component::Catalogue);
        let status = Arc::new(CachedStatusRegistry::new(settings, Arc::new(SystemClock)));
        let refresher = StatusRefresher::new(
            Arc::clone(&status),
            vec![Arc::new(CatalogProbe::new(Arc::clone(&catalogue)))],
        );
        (
            Self {
                status,
                revision: None,
                catalogue: Some(catalogue),
            },
            refresher,
        )
    }
}

/// The config and everything resolved from it: the credential graph, the
/// inbound-key table, and the per-target circuits. Immutable once published —
/// a reload builds a replacement rather than mutating this one.
pub struct ConfigSnapshot {
    pub config: Config,
    pub credentials: Credentials,
    /// Namespace-scoped content middleware compiled from the same desired-state
    /// generation as the rest of this serving snapshot.
    middleware: MiddlewarePlan,
    /// Per-target circuit breaker, keyed by the target's qualified model
    /// (`provider/model`). In-memory and per-replica, consistent with running
    /// stateless by default (ADR 0002); distinct from the per-credential health
    /// that lives on `Credentials` (ADR 0008).
    pub target_circuits: CircuitBreaker,
    principals: PrincipalStoreChain,
    /// How many times the config has been replaced: `0` is the boot config, and
    /// each applied reload increments it. Published as a metric so an operator
    /// can tell which generation a replica is serving.
    pub generation: u64,
    pub gateway_key_fingerprints: HashMap<String, String>,
    pub gateway_verifier_fingerprints: HashMap<String, String>,
    pub gateway_minting_fingerprint: Option<String>,
    pub gateway_minting: Option<ResolvedMinting>,
    gateway_token_epochs: HashMap<String, NamespaceEpoch>,
    /// The durable secret material this snapshot was compiled against, unwrapped
    /// once during compilation and held for the snapshot's whole life.
    ///
    /// Holding it *here* is what ties material's lifetime to the revision it
    /// belongs to. A rotation publishes a new snapshot that holds the new version
    /// while requests still serving the old snapshot keep the old one alive, and
    /// the material is zeroized when the last such request finishes — not when
    /// the administrator's call returns. A request never reaches the secret store,
    /// because everything it could ask for is already in the snapshot it holds.
    secrets: ResolvedSecrets,
    /// Derived availability evidence, projected onto the snapshot rather than
    /// resolved from the config (#206).
    ///
    /// Carried *beside* the config, never inside it: an availability index says
    /// what is currently reachable, and the config says what the deployment
    /// declares. Keeping the two apart is what makes the direction of authority
    /// one-way — a projection cannot add a model, a namespace, or a credential, so
    /// no amount of discovery evidence can enlarge what is served.
    ///
    /// [`ConfigSnapshot::build`] produces `None`. A compiler holding availability
    /// evidence attaches a derived one during compilation (#148), off the request
    /// path and before publication; no request consults a verdict, and nothing
    /// polls a provider to produce it.
    ///
    /// Absent rather than empty, because those are different answers: a replica
    /// that derives nothing must not report an empty catalogue, which reads
    /// identically to a tenant that has just lost every entitlement.
    availability: Option<Arc<AvailabilityIndex>>,
    /// The approved pricing this snapshot serves under, when it was compiled from
    /// a revision that published a price book (#201).
    ///
    /// Part of the snapshot rather than a second published value, because that is
    /// what makes pricing and routing atomic: a request loads one pointer, so it
    /// cannot be routed by revision *N+1* and priced by *N*. `None` for a
    /// file-configured deployment, whose prices are the ones `[[model]]` declares.
    pricing: Option<PricingSnapshot>,
    /// The administrative identity directory admitted with this revision. It
    /// is swapped alongside the serving snapshot and is never consulted by an
    /// inference request.
    admin_authorization: Option<Arc<AuthorizationSnapshot>>,
}

pub struct ResolvedMinting {
    pub kid: String,
    pub algorithm: crate::mint::MintAlgorithm,
    pub key_material: SecretString,
    pub audience: String,
    pub max_ttl: Duration,
    pub scope: Option<HashSet<Capability>>,
    pub aliases: Option<AliasScope>,
    pub max_request_microdollars: Option<u64>,
}

/// The process-independent portion of a compiled stateful snapshot.
///
/// Bootstrap configuration is intentionally not duplicated here: it remains
/// owned by the deployment file. Only the durable projection and the values
/// derived from it are recorded, which makes a cache restore obey the same
/// boundary as ordinary convergence.
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct CachedServingSnapshot {
    pub(crate) revision: String,
    pub(crate) generation: u64,
    pub(crate) namespaces: Vec<CachedNamespace>,
    pub(crate) providers: Vec<CachedProvider>,
    pub(crate) models: Vec<CachedModel>,
    pub(crate) credentials: Vec<CachedCredential>,
    pub(crate) principals: Vec<CachedPrincipal>,
    pub(crate) secrets: Vec<CachedSecret>,
    pub(crate) pricing: Option<CachedPricing>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct CachedNamespace {
    pub(crate) id: String,
    pub(crate) default: bool,
    pub(crate) allow_platform_fallback: bool,
    pub(crate) project: Option<CachedProjectIdentity>,
    pub(crate) policy: Option<CachedPolicy>,
    pub(crate) static_policy: Option<CachedStaticPolicy>,
    pub(crate) token_epoch: Option<u64>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct CachedProjectIdentity {
    pub(crate) tenant: String,
    pub(crate) project: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CachedPolicy {
    pub(crate) scope: CachedPolicyScope,
    pub(crate) epoch: u64,
    pub(crate) subject_limit_microdollars: u64,
    pub(crate) namespace_limit_microdollars: Option<u64>,
    pub(crate) reservation_ttl_seconds: u64,
    pub(crate) max_in_flight_per_subject: u64,
    pub(crate) lease_ttl_seconds: u64,
    pub(crate) minimum_token_epoch: u64,
    pub(crate) content_middleware: Vec<CachedContentMiddleware>,
    #[serde(default)]
    pub(crate) buffered_response_routes: Vec<String>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CachedStaticPolicy {
    pub(crate) content_middleware: Vec<CachedContentMiddleware>,
    pub(crate) buffered_response_routes: Vec<String>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CachedPolicyScope {
    Namespace { resource: String },
    Tenant { tenant: String },
    Project { tenant: String, project: String },
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CachedContentMiddleware {
    pub(crate) id: String,
    pub(crate) scopes: Vec<gateway_core::MiddlewareScope>,
    pub(crate) failure_posture: gateway_core::MiddlewareFailurePosture,
    pub(crate) max_duration_milliseconds: u64,
    #[serde(default)]
    pub(crate) guardrail: Option<CachedContentGuardrail>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CachedContentGuardrail {
    pub(crate) key_env: String,
    pub(crate) key_fingerprint: String,
    pub(crate) rules: Vec<gateway_core::GuardrailRule>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct CachedProvider {
    pub(crate) id: String,
    pub(crate) kind: String,
    pub(crate) base_url: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct CachedModel {
    pub(crate) name: String,
    pub(crate) namespace: Option<String>,
    pub(crate) targets: Vec<CachedTarget>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct CachedTarget {
    pub(crate) provider: String,
    pub(crate) model: String,
    pub(crate) price: gateway_core::ModelPrice,
    pub(crate) catalog: Option<CachedCatalogBinding>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct CachedCatalogBinding {
    pub(crate) provider: String,
    pub(crate) model: String,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct CachedCredential {
    pub(crate) namespace: String,
    pub(crate) provider: String,
    pub(crate) env: Option<String>,
    pub(crate) id: Option<String>,
    pub(crate) weight: u32,
    pub(crate) secret: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct CachedPrincipal {
    pub(crate) namespace: String,
    pub(crate) subject: String,
    pub(crate) digest: String,
    #[serde(default)]
    pub(crate) all_namespaces: bool,
    #[serde(default)]
    pub(crate) namespaces: Vec<String>,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct CachedSecret {
    pub(crate) reference: String,
    pub(crate) binding: CachedSecretBinding,
    /// This field is only present inside the encrypted cache payload. It must
    /// never be written to the signed desired-state cache or an unencrypted
    /// diagnostic.
    pub(crate) material: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "scope", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum CachedSecretBinding {
    Legacy,
    Namespace {
        owner_namespace: String,
        ciphertext_digest: String,
        lifecycle: String,
    },
}

impl Drop for CachedSecret {
    fn drop(&mut self) {
        self.material.zeroize();
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct CachedPricing {
    pub(crate) book: String,
    pub(crate) checksum: String,
    pub(crate) catalog: String,
    pub(crate) catalog_version: Option<u64>,
    pub(crate) approval: CachedApproval,
    pub(crate) effective_from: u64,
    pub(crate) effective_until: Option<u64>,
    pub(crate) targets: Vec<CachedPriceTarget>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub(crate) enum CachedApproval {
    Draft,
    Approved {
        actor: CachedActor,
        at: u64,
        citation: Option<String>,
    },
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum CachedActor {
    Human { issuer: String, subject: String },
    Breakglass,
    Workload { tenant: String, principal: String },
    System { component: String },
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct CachedPriceTarget {
    pub(crate) provider: String,
    pub(crate) published_model_id: String,
    pub(crate) price: gateway_core::ModelPrice,
}

impl ConfigSnapshot {
    /// Whether this snapshot is flat-v2 and carries provider credential
    /// material.
    ///
    /// Such snapshots are deliberately ineligible for cross-restart recovery
    /// until an authenticated monotonic revision/tombstone floor can prove that
    /// cached material has not since been revoked.
    pub(crate) fn credential_bearing_flat_v2(&self) -> bool {
        self.config.credential.iter().any(|credential| {
            credential.secret.is_some()
                && self.config.namespace.iter().any(|namespace| {
                    namespace.id == credential.namespace && namespace.static_policy.is_some()
                })
        })
    }

    /// Capture the exact projection needed to rebuild a serving snapshot after
    /// a process restart. The caller encrypts the returned structure before it
    /// reaches disk; the cache writer explicitly zeroizes the temporary copy
    /// after serialization.
    pub(crate) fn cached_serving(&self, revision: RevisionId) -> CachedServingSnapshot {
        let config = &self.config;
        CachedServingSnapshot {
            revision: revision.to_string(),
            generation: self.generation,
            namespaces: config
                .namespace
                .iter()
                .map(|namespace| CachedNamespace {
                    id: namespace.id.clone(),
                    default: namespace.default,
                    allow_platform_fallback: namespace.allow_platform_fallback,
                    project: namespace.project.map(|identity| CachedProjectIdentity {
                        tenant: identity.tenant.to_string(),
                        project: identity.project.to_string(),
                    }),
                    policy: namespace.policy.as_ref().map(|policy| CachedPolicy {
                        scope: match policy.body.scope() {
                            PolicyScope::Namespace(resource) => CachedPolicyScope::Namespace {
                                resource: resource.to_string(),
                            },
                            PolicyScope::Tenant(tenant) => CachedPolicyScope::Tenant {
                                tenant: tenant.to_string(),
                            },
                            PolicyScope::Project { tenant, project } => {
                                CachedPolicyScope::Project {
                                    tenant: tenant.to_string(),
                                    project: project.to_string(),
                                }
                            }
                        },
                        epoch: policy.body.epoch().get(),
                        subject_limit_microdollars: policy
                            .body
                            .budget()
                            .subject_limit_microdollars(),
                        namespace_limit_microdollars: policy
                            .body
                            .budget()
                            .namespace_limit_microdollars(),
                        reservation_ttl_seconds: policy.body.budget().reservation_ttl_seconds(),
                        max_in_flight_per_subject: policy
                            .body
                            .concurrency()
                            .max_in_flight_per_subject(),
                        lease_ttl_seconds: policy.body.concurrency().lease_ttl_seconds(),
                        minimum_token_epoch: policy.body.revocation().minimum_token_epoch(),
                        content_middleware: policy
                            .body
                            .content_middleware()
                            .iter()
                            .map(|registration| CachedContentMiddleware {
                                id: registration.id().to_owned(),
                                scopes: registration.scopes().to_vec(),
                                failure_posture: registration.failure_posture(),
                                max_duration_milliseconds: registration.max_duration_milliseconds(),
                                guardrail: registration.guardrail().map(|guardrail| {
                                    CachedContentGuardrail {
                                        key_env: guardrail.key_env().to_owned(),
                                        key_fingerprint: self
                                            .middleware
                                            .guardrail_key_fingerprint(&namespace.id)
                                            .expect("a compiled guardrail has a key fingerprint")
                                            .to_owned(),
                                        rules: guardrail.rules().to_vec(),
                                    }
                                }),
                            })
                            .collect(),
                        buffered_response_routes: policy
                            .body
                            .buffered_response_routes()
                            .iter()
                            .map(|route| route.as_str().to_owned())
                            .collect(),
                    }),
                    static_policy: namespace.static_policy.as_ref().map(|policy| {
                        CachedStaticPolicy {
                            content_middleware: policy
                                .content_middleware
                                .iter()
                                .map(|registration| CachedContentMiddleware {
                                    id: registration.id().to_owned(),
                                    scopes: registration.scopes().to_vec(),
                                    failure_posture: registration.failure_posture(),
                                    max_duration_milliseconds: registration
                                        .max_duration_milliseconds(),
                                    guardrail: registration.guardrail().map(|guardrail| {
                                        CachedContentGuardrail {
                                            key_env: guardrail.key_env().to_owned(),
                                            key_fingerprint: self
                                                .middleware
                                                .guardrail_key_fingerprint(&namespace.id)
                                                .expect(
                                                    "a compiled guardrail has a key fingerprint",
                                                )
                                                .to_owned(),
                                            rules: guardrail.rules().to_vec(),
                                        }
                                    }),
                                })
                                .collect(),
                            buffered_response_routes: policy
                                .buffered_response_routes
                                .iter()
                                .map(|route| route.as_str().to_owned())
                                .collect(),
                        }
                    }),
                    token_epoch: config
                        .gateway_token_epoch
                        .iter()
                        .find(|epoch| epoch.namespace == namespace.id && epoch.subject.is_none())
                        .map(|epoch| epoch.min_iat),
                })
                .collect(),
            providers: config
                .provider
                .iter()
                .map(|provider| CachedProvider {
                    id: provider.id.clone(),
                    kind: match provider.kind {
                        ProviderKind::Openai => "openai",
                        ProviderKind::Anthropic => "anthropic",
                        ProviderKind::OpenaiCompatible => "openai-compatible",
                    }
                    .to_owned(),
                    base_url: provider.base_url.clone(),
                })
                .collect(),
            models: config
                .model
                .iter()
                .map(|model| CachedModel {
                    name: model.name.clone(),
                    namespace: model.namespace.clone(),
                    targets: model
                        .targets
                        .iter()
                        .map(|target| CachedTarget {
                            provider: target.provider.clone(),
                            model: target.model.clone(),
                            price: target.price,
                            catalog: target.catalog.as_ref().map(|binding| CachedCatalogBinding {
                                provider: binding.provider.to_string(),
                                model: binding.model.clone(),
                            }),
                        })
                        .collect(),
                })
                .collect(),
            credentials: config
                .credential
                .iter()
                .map(|credential| CachedCredential {
                    namespace: credential.namespace.clone(),
                    provider: credential.provider.clone(),
                    env: credential.env.clone(),
                    id: credential.id.clone(),
                    weight: credential.weight,
                    secret: credential.secret.map(|reference| reference.to_string()),
                })
                .collect(),
            principals: config
                .projected_principals
                .iter()
                .map(|principal| CachedPrincipal {
                    namespace: principal.namespace.clone(),
                    subject: principal.subject.clone(),
                    digest: principal.digest.to_string(),
                    all_namespaces: principal
                        .grant
                        .as_ref()
                        .is_some_and(crate::namespace::NamespaceGrant::is_all),
                    namespaces: principal
                        .grant
                        .as_ref()
                        .and_then(crate::namespace::NamespaceGrant::namespaces)
                        .into_iter()
                        .flat_map(|namespaces| namespaces.iter().map(ToString::to_string))
                        .collect(),
                })
                .collect(),
            secrets: self
                .secrets
                .references()
                .into_iter()
                .filter_map(|reference| {
                    self.secrets.get(reference).map(|material| CachedSecret {
                        reference: reference.to_string(),
                        binding: match material.binding() {
                            ResolvedSecretBinding::Legacy => CachedSecretBinding::Legacy,
                            ResolvedSecretBinding::Namespace(request) => {
                                CachedSecretBinding::Namespace {
                                    owner_namespace: request.owner().to_string(),
                                    ciphertext_digest: request.ciphertext_digest().to_string(),
                                    lifecycle: request.lifecycle().as_str().to_owned(),
                                }
                            }
                        },
                        material: material.expose().to_owned(),
                    })
                })
                .collect(),
            pricing: self.pricing.as_ref().map(cached_pricing),
        }
    }

    /// Rebuild a compiled snapshot over the current bootstrap-owned settings.
    /// Every durable field is parsed and the ordinary compiled snapshot gate is
    /// run again before the value can be published.
    pub(crate) fn from_cached_serving(
        mut bootstrap: Config,
        env: &HashMap<String, String>,
        cached: CachedServingSnapshot,
    ) -> Result<(RevisionId, Self), String> {
        let flat_v2 = cached.flat_v2();
        if cached.credential_bearing_flat_v2() {
            return Err(
                "credential-bearing flat-v2 compiled snapshots are not eligible for cold restoration until an authenticated monotonic revision/tombstone floor exists"
                    .to_owned(),
            );
        }
        verify_cached_guardrail_keys(&cached, env)?;
        let revision = RevisionId::parse(&cached.revision).map_err(|error| error.to_string())?;
        let restored_namespaces = cached
            .namespaces
            .into_iter()
            .map(|namespace| cached_namespace(namespace, revision))
            .collect::<Result<Vec<_>, _>>()?;
        bootstrap.namespace = restored_namespaces
            .iter()
            .map(|(namespace, _)| namespace.clone())
            .collect();
        bootstrap.gateway_token_epoch = restored_namespaces
            .into_iter()
            .filter_map(|(namespace, minimum_token_epoch)| {
                minimum_token_epoch.map(|min_iat| crate::config::GatewayTokenEpoch {
                    namespace: namespace.id,
                    subject: None,
                    min_iat,
                })
            })
            .collect();
        bootstrap.provider = cached
            .providers
            .into_iter()
            .map(|provider| {
                let kind = match provider.kind.as_str() {
                    "openai" => ProviderKind::Openai,
                    "anthropic" => ProviderKind::Anthropic,
                    "openai-compatible" => ProviderKind::OpenaiCompatible,
                    other => return Err(format!("cached provider kind `{other}` is unsupported")),
                };
                Ok(crate::config::Provider {
                    id: provider.id,
                    kind,
                    base_url: provider.base_url,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        bootstrap.model = cached
            .models
            .into_iter()
            .map(|model| {
                Ok(crate::config::Model {
                    name: model.name,
                    namespace: model.namespace,
                    targets: model
                        .targets
                        .into_iter()
                        .map(|target| {
                            Ok(crate::config::Target {
                                provider: target.provider,
                                model: target.model,
                                price: target.price,
                                catalog: target
                                    .catalog
                                    .map(|binding| {
                                        CatalogBinding::new(&binding.provider, &binding.model)
                                            .map_err(|error| error.to_string())
                                    })
                                    .transpose()?,
                            })
                        })
                        .collect::<Result<Vec<_>, String>>()?,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        bootstrap.credential = cached
            .credentials
            .into_iter()
            .map(|credential| {
                Ok(crate::config::Credential {
                    namespace: credential.namespace,
                    provider: credential.provider,
                    env: credential.env,
                    secret: credential
                        .secret
                        .as_deref()
                        .map(SecretRef::parse)
                        .transpose()
                        .map_err(|error| error.to_string())?,
                    id: credential.id,
                    weight: credential.weight,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        bootstrap.projected_principals = cached
            .principals
            .into_iter()
            .map(|principal| {
                Ok(ProjectedPrincipal {
                    namespace: principal.namespace.clone(),
                    subject: principal.subject,
                    digest: Checksum::parse(&principal.digest)
                        .map_err(|error| error.to_string())?,
                    grant: if principal.all_namespaces {
                        Some(crate::namespace::NamespaceGrant::all())
                    } else if principal.namespaces.is_empty() {
                        None
                    } else {
                        Some(
                            crate::namespace::NamespaceGrant::set(
                                principal
                                    .namespaces
                                    .iter()
                                    .map(|namespace| {
                                        crate::namespace::NamespaceId::parse(namespace)
                                    })
                                    .collect::<Result<Vec<_>, _>>()
                                    .map_err(|error| error.to_string())?,
                            )
                            .map_err(|error| error.to_string())?,
                        )
                    },
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        bootstrap
            .validate_compiled()
            .map_err(|error| error.to_string())?;
        let mut expected_secret_owners = HashMap::new();
        if flat_v2 {
            for credential in &bootstrap.credential {
                let Some(reference) = credential.secret else {
                    continue;
                };
                if let Some(first) =
                    expected_secret_owners.insert(reference, credential.namespace.clone())
                    && first != credential.namespace
                {
                    return Err(format!(
                        "compiled cache shares secret {reference} between namespaces `{first}` and `{}`",
                        credential.namespace
                    ));
                }
            }
        }
        let materials = cached
            .secrets
            .into_iter()
            .map(|mut secret| {
                let reference =
                    SecretRef::parse(&secret.reference).map_err(|error| error.to_string())?;
                if flat_v2 && !expected_secret_owners.contains_key(&reference) {
                    return Err(format!(
                        "compiled cache contains unreferenced secret {reference}"
                    ));
                }
                let binding = match secret.binding {
                    CachedSecretBinding::Legacy => ResolvedSecretBinding::Legacy,
                    CachedSecretBinding::Namespace { .. } => {
                        return Err(format!(
                            "compiled cache carries namespace-bound secret {reference}; flat-v2 credential material is not eligible for cold restoration"
                        ));
                    }
                };
                let material = std::mem::take(&mut secret.material);
                Ok((reference, SecretMaterial::new(material), binding))
            })
            .collect::<Result<Vec<_>, String>>()?;
        if flat_v2 && materials.len() != expected_secret_owners.len() {
            return Err(format!(
                "compiled cache contains {} secret materials for {} referenced versions",
                materials.len(),
                expected_secret_owners.len()
            ));
        }
        let secrets = ResolvedSecrets::from_cached(MaterialLedger::new(), materials)?;
        let mut snapshot = Self::build_compiled_with(bootstrap, env, cached.generation, secrets)
            .map_err(|error| error.to_string())?;
        if let Some(pricing) = cached.pricing {
            snapshot = snapshot.with_pricing(pricing_snapshot(pricing)?);
        }
        Ok((revision, snapshot))
    }
}

fn verify_cached_guardrail_keys(
    cached: &CachedServingSnapshot,
    env: &HashMap<String, String>,
) -> Result<(), String> {
    for namespace in &cached.namespaces {
        let identity = namespace.project.as_ref().map_or_else(
            || namespace.id.clone(),
            |project| format!("{}/{}", project.tenant, project.project),
        );
        let registrations = namespace
            .static_policy
            .as_ref()
            .map(|policy| policy.content_middleware.as_slice())
            .or_else(|| {
                namespace
                    .policy
                    .as_ref()
                    .map(|policy| policy.content_middleware.as_slice())
            })
            .unwrap_or_default();
        for registration in registrations {
            let Some(guardrail) = &registration.guardrail else {
                continue;
            };
            let actual =
                crate::middleware::guardrail_key_fingerprint(&identity, &guardrail.key_env, env)
                    .map_err(|error| error.to_string())?;
            if actual != guardrail.key_fingerprint {
                return Err(format!(
                    "cached guardrail key reference `{}` for namespace `{}` resolves to different material",
                    guardrail.key_env, namespace.id
                ));
            }
        }
    }
    Ok(())
}

fn restore_cached_middleware(
    registrations: Vec<CachedContentMiddleware>,
) -> Result<Vec<crate::desired_state::ContentMiddlewareRegistration>, String> {
    registrations
        .into_iter()
        .map(|registration| {
            let middleware = crate::desired_state::ContentMiddlewareRegistration::new(
                registration.id,
                registration.scopes,
                registration.failure_posture,
                registration.max_duration_milliseconds,
            )
            .map_err(|error| error.to_string())?;
            let Some(guardrail) = registration.guardrail else {
                return Ok(middleware);
            };
            let guardrail = crate::desired_state::policy::ContentGuardrailRegistration::new(
                guardrail.key_env,
                guardrail.rules,
            )
            .map_err(|error| error.to_string())?;
            middleware
                .with_guardrail(guardrail)
                .map_err(|error| error.to_string())
        })
        .collect()
}

fn restore_cached_buffered_routes(routes: &[String]) -> Result<Vec<BufferedResponseRoute>, String> {
    routes
        .iter()
        .map(|route| BufferedResponseRoute::parse(route).map_err(|error| error.to_string()))
        .collect()
}

impl CachedServingSnapshot {
    fn flat_v2(&self) -> bool {
        self.namespaces
            .iter()
            .any(|namespace| namespace.static_policy.is_some())
    }

    fn credential_bearing_flat_v2(&self) -> bool {
        self.flat_v2()
            && self
                .credentials
                .iter()
                .any(|credential| credential.secret.is_some())
    }

    pub(crate) fn zeroize_secrets(&mut self) {
        for secret in &mut self.secrets {
            secret.material.zeroize();
        }
    }
}

fn cached_namespace(
    namespace: CachedNamespace,
    revision: RevisionId,
) -> Result<(Namespace, Option<u64>), String> {
    let has_static_policy = namespace.static_policy.is_some();
    let project = namespace
        .project
        .map(|identity| -> Result<ProjectIdentity, String> {
            Ok(ProjectIdentity {
                tenant: TenantId::parse(&identity.tenant).map_err(|error| error.to_string())?,
                project: ProjectId::parse(&identity.project).map_err(|error| error.to_string())?,
            })
        })
        .transpose()?;
    let policy = namespace
        .policy
        .map(|policy| -> Result<(NamespacePolicy, u64), String> {
            let scope = match policy.scope {
                CachedPolicyScope::Namespace { resource } => PolicyScope::Namespace(
                    crate::desired_state::ResourceId::parse(&resource)
                        .map_err(|error| error.to_string())?,
                ),
                CachedPolicyScope::Tenant { tenant } => PolicyScope::Tenant(
                    TenantId::parse(&tenant).map_err(|error| error.to_string())?,
                ),
                CachedPolicyScope::Project { tenant, project } => PolicyScope::Project {
                    tenant: TenantId::parse(&tenant).map_err(|error| error.to_string())?,
                    project: ProjectId::parse(&project).map_err(|error| error.to_string())?,
                },
            };
            let minimum_token_epoch = policy.minimum_token_epoch;
            let content_middleware = restore_cached_middleware(policy.content_middleware)?;
            let buffered_response_routes =
                restore_cached_buffered_routes(&policy.buffered_response_routes)?;
            let body = PolicyBody::new(
                scope,
                PolicyEpoch::new(policy.epoch).map_err(|error| error.to_string())?,
                BudgetPolicy::stored(
                    policy.subject_limit_microdollars,
                    policy.namespace_limit_microdollars,
                    policy.reservation_ttl_seconds,
                )
                .map_err(|error| error.to_string())?,
                ConcurrencyPolicy::new(policy.max_in_flight_per_subject, policy.lease_ttl_seconds)
                    .map_err(|error| error.to_string())?,
                RevocationPolicy::new(policy.minimum_token_epoch),
            )
            .with_content_middleware(content_middleware)
            .map_err(|error| error.to_string())?
            .with_buffered_response_routes(buffered_response_routes)
            .map_err(|error| error.to_string())?;
            let generation = body.generation(revision);
            Ok((NamespacePolicy { body, generation }, minimum_token_epoch))
        })
        .transpose()?;
    let static_policy = namespace
        .static_policy
        .map(|policy| {
            Ok::<_, String>(NamespaceStaticPolicy {
                content_middleware: restore_cached_middleware(policy.content_middleware)?,
                buffered_response_routes: restore_cached_buffered_routes(
                    &policy.buffered_response_routes,
                )?,
            })
        })
        .transpose()?;
    let policy_token_epoch = policy.as_ref().and_then(|(_, minimum)| {
        matches!(
            policy.as_ref().map(|(policy, _)| policy.body.scope()),
            Some(PolicyScope::Namespace(_))
        )
        .then_some(*minimum)
    });
    if has_static_policy && namespace.token_epoch.is_none() {
        return Err(format!(
            "compiled cache omits the token epoch for flat-v2 namespace `{}`",
            namespace.id
        ));
    }
    if let (Some(explicit), Some(policy)) = (namespace.token_epoch, policy_token_epoch)
        && explicit != policy
    {
        return Err(format!(
            "compiled cache gives namespace `{}` conflicting token epochs {explicit} and {policy}",
            namespace.id
        ));
    }
    let minimum_token_epoch = namespace.token_epoch.or(policy_token_epoch);
    Ok((
        Namespace {
            id: namespace.id,
            default: namespace.default,
            allow_platform_fallback: namespace.allow_platform_fallback,
            project,
            policy: policy.map(|(policy, _)| policy),
            static_policy,
        },
        minimum_token_epoch,
    ))
}

fn cached_pricing(pricing: &PricingSnapshot) -> CachedPricing {
    CachedPricing {
        book: pricing.book().to_string(),
        checksum: pricing.checksum().to_string(),
        catalog: pricing.catalog().checksum().to_string(),
        catalog_version: pricing.catalog_version().map(|version| version.get()),
        approval: match pricing.approval() {
            Approval::Draft => CachedApproval::Draft,
            Approval::Approved { by, at, citation } => CachedApproval::Approved {
                actor: cached_actor(by),
                at: at.millis(),
                citation: citation
                    .as_ref()
                    .map(|citation| citation.as_str().to_owned()),
            },
        },
        effective_from: pricing.effective().starts().millis(),
        effective_until: pricing.effective().ends().map(|instant| instant.millis()),
        targets: pricing
            .targets()
            .map(|(target, price)| CachedPriceTarget {
                provider: target.provider.to_string(),
                published_model_id: target.published_model_id.clone(),
                price: *price,
            })
            .collect(),
    }
}

fn cached_actor(actor: &Actor) -> CachedActor {
    match actor {
        Actor::Human { issuer, subject } => CachedActor::Human {
            issuer: issuer.clone(),
            subject: subject.clone(),
        },
        Actor::Breakglass => CachedActor::Breakglass,
        Actor::Workload { tenant, principal } => CachedActor::Workload {
            tenant: tenant.to_string(),
            principal: principal.to_string(),
        },
        Actor::System { component } => CachedActor::System {
            component: component.clone(),
        },
    }
}

fn restore_actor(actor: CachedActor) -> Result<Actor, String> {
    Ok(match actor {
        CachedActor::Human { issuer, subject } => Actor::Human { issuer, subject },
        CachedActor::Breakglass => Actor::Breakglass,
        CachedActor::Workload { tenant, principal } => Actor::Workload {
            tenant: TenantId::parse(&tenant).map_err(|error| error.to_string())?,
            principal: crate::desired_state::PrincipalId::parse(&principal)
                .map_err(|error| error.to_string())?,
        },
        CachedActor::System { component } => Actor::System { component },
    })
}

fn parse_resource_reference(text: &str) -> Result<ResourceRef, String> {
    let (kind, rest) = text
        .split_once('/')
        .ok_or_else(|| "cached price-book reference has no kind separator".to_owned())?;
    let (id, version) = rest
        .rsplit_once('@')
        .ok_or_else(|| "cached price-book reference has no version".to_owned())?;
    let kind = ResourceKind::ALL
        .iter()
        .copied()
        .find(|candidate| candidate.as_str() == kind)
        .ok_or_else(|| format!("cached price-book kind `{kind}` is unsupported"))?;
    let id = ResourceId::parse(id).map_err(|error| error.to_string())?;
    let version = version
        .strip_prefix('v')
        .and_then(|value| value.parse::<u64>().ok())
        .and_then(ResourceVersionNumber::new)
        .ok_or_else(|| "cached price-book version is invalid".to_owned())?;
    Ok(ResourceRef::new(kind, id, version))
}

fn pricing_snapshot(cached: CachedPricing) -> Result<PricingSnapshot, String> {
    let approval = match cached.approval {
        CachedApproval::Draft => Approval::Draft,
        CachedApproval::Approved {
            actor,
            at,
            citation,
        } => Approval::Approved {
            by: restore_actor(actor)?,
            at: EffectiveInstant::from_millis(at),
            citation: citation
                .as_deref()
                .map(DisplayName::parse)
                .transpose()
                .map_err(|error| error.to_string())?,
        },
    };
    let effective = match cached.effective_until {
        Some(until) => EffectiveInterval::bounded(
            EffectiveInstant::from_millis(cached.effective_from),
            EffectiveInstant::from_millis(until),
        )
        .map_err(|error| error.to_string())?,
        None => EffectiveInterval::from(EffectiveInstant::from_millis(cached.effective_from)),
    };
    let targets = cached
        .targets
        .into_iter()
        .map(|target| {
            Ok((
                PricedTarget::new(
                    crate::backends::catalog::ProviderId::parse(&target.provider)
                        .map_err(|error| error.to_string())?,
                    target.published_model_id,
                ),
                target.price,
            ))
        })
        .collect::<Result<std::collections::BTreeMap<_, _>, String>>()?;
    Ok(PricingSnapshot::from_cached(
        parse_resource_reference(&cached.book)?,
        Checksum::parse(&cached.checksum).map_err(|error| error.to_string())?,
        crate::backends::catalog::CatalogContentId::from_checksum(
            Checksum::parse(&cached.catalog).map_err(|error| error.to_string())?,
        ),
        match cached.catalog_version {
            None => None,
            Some(version) => Some(
                ResourceVersionNumber::new(version)
                    .ok_or_else(|| "cached catalogue version is zero".to_owned())?,
            ),
        },
        approval,
        effective,
        targets,
    ))
}

/// Why a config could not become a servable snapshot. Names the offending
/// reference — an env-var name or path and a namespace — never a secret's value.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error(transparent)]
    Middleware(#[from] MiddlewarePlanError),
    #[error(transparent)]
    Credentials(#[from] CredentialError),
    #[error("store: {0}")]
    Store(String),
    #[error(
        "gateway_key for namespace `{namespace}` references env var `{env}`, which is unset or empty"
    )]
    MissingGatewayKey { namespace: String, env: String },
    #[error("gateway_key for namespace `{namespace}` file `{path}` failed ({kind}): {error}")]
    GatewayKeyFile {
        namespace: String,
        path: String,
        kind: std::io::ErrorKind,
        error: String,
    },
    #[error("gateway_key for namespace `{namespace}` file `{path}` is empty")]
    EmptyGatewayKeyFile { namespace: String, path: String },
    #[error("gateway_key for namespace `{namespace}` file `{path}` is not valid UTF-8")]
    InvalidGatewayKeyFileUtf8 { namespace: String, path: String },
    #[error(
        "gateway_key for namespace `{namespace}` uses the reserved `{shape}` workload-key shape"
    )]
    ReservedGatewayKeyShape {
        namespace: String,
        shape: &'static str,
    },
    #[error("gateway_key for namespace `{namespace}` must declare exactly one non-empty source")]
    InvalidGatewayKeySource { namespace: String },
    #[error(
        "gateway_key sources `{env}` (namespace `{namespace}`) and `{other_env}` (namespace `{other_namespace}`) hold the same secret, so the caller's namespace would be ambiguous"
    )]
    DuplicateGatewayKey {
        env: String,
        namespace: String,
        other_env: String,
        other_namespace: String,
    },
    #[error(transparent)]
    PrincipalShapes(#[from] PrincipalShapeError),
    #[error(transparent)]
    TokenVerifier(#[from] TokenVerifierBuildError),
    #[error(
        "no inbound gateway key resolved: inbound authentication fails closed and there is no keyless mode"
    )]
    NoInboundKeys,
    #[error("gateway_minting signing key `{reference}` is invalid: {error}")]
    MintingKey { reference: String, error: String },
    #[error("gateway_minting references unknown verifier kid `{kid}`")]
    MintingVerifierNotFound { kid: String },
    #[error("gateway_minting must declare exactly one non-empty source")]
    InvalidMintingSource,
    #[error("gateway_minting references env var `{env}`, which is unset or empty")]
    MissingMintingKey { env: String },
    #[error("gateway_minting file `{path}` failed ({kind}): {error}")]
    MintingKeyFile {
        path: String,
        kind: std::io::ErrorKind,
        error: String,
    },
    #[error("gateway_minting file `{path}` is empty")]
    EmptyMintingKeyFile { path: String },
    #[error("gateway_minting file `{path}` is not valid UTF-8")]
    InvalidMintingKeyFileUtf8 { path: String },
    #[error("gateway_minting requires a non-empty gateway token audience")]
    MissingMintingAudience,
    #[error("gateway_minting aliases are invalid: {error}")]
    InvalidMintingAliases { error: String },
    #[error("gateway_minting scope contains invalid capability `{value}`")]
    InvalidMintingCapability { value: String },
    #[error("gateway_minting signing key `{reference}` does not match verifier `{kid}`")]
    MintingKeyMismatch { kid: String, reference: String },
}

impl ConfigSnapshot {
    /// Resolve a validated config against an environment snapshot. Fails when a
    /// declared credential's or gateway key's env var is missing or empty, or
    /// when two gateway keys resolve to the same secret — the credential graph
    /// and the inbound-key table are both resolved before the snapshot is
    /// published, never at request time.
    pub fn build(
        config: Config,
        env: &HashMap<String, String>,
        generation: u64,
    ) -> Result<Self, SnapshotError> {
        Self::build_with(config, env, generation, ResolvedSecrets::default())
    }

    /// Build the keyless stateful bootstrap snapshot. It is intentionally not
    /// a serving snapshot: the reconciler must replace it with a projected
    /// snapshot containing inbound principals before authenticated traffic can
    /// pass the convergence gate.
    pub(crate) fn build_bootstrap(
        config: Config,
        env: &HashMap<String, String>,
        generation: u64,
    ) -> Result<Self, SnapshotError> {
        Self::build_with_mode(config, env, generation, ResolvedSecrets::default(), true)
    }

    /// [`ConfigSnapshot::build`], taking ownership of durable material a
    /// candidate's compilation already unwrapped.
    ///
    /// The stateless path is the same call with an empty set: `env:` and `file:`
    /// references resolve here exactly as they did before typed credentials
    /// existed, so a deployment with no secret store is unaffected by any of this.
    pub fn build_with(
        config: Config,
        env: &HashMap<String, String>,
        generation: u64,
        secrets: ResolvedSecrets,
    ) -> Result<Self, SnapshotError> {
        Self::build_with_mode(config, env, generation, secrets, false)
    }

    /// Build a snapshot for a hydrated durable revision.
    ///
    /// The initial stateful bootstrap is allowed to be keyless while inference
    /// is refused and the administrative surface comes up. A compiled candidate
    /// is different: it may publish only when the revision supplied at least one
    /// request-addressable inbound principal.
    pub fn build_compiled_with(
        config: Config,
        env: &HashMap<String, String>,
        generation: u64,
        secrets: ResolvedSecrets,
    ) -> Result<Self, SnapshotError> {
        Self::build_with_mode(config, env, generation, secrets, false)
    }

    fn build_with_mode(
        config: Config,
        env: &HashMap<String, String>,
        generation: u64,
        secrets: ResolvedSecrets,
        allow_keyless_bootstrap: bool,
    ) -> Result<Self, SnapshotError> {
        let gateway_token_epochs = configured_token_epochs(&config);
        let middleware = MiddlewarePlan::compile(&config, env)?;
        // The one place both kinds of provider credential become one pool: env
        // references from the boot environment, projected ones from the material
        // this candidate resolved. Neither reaches a store from here.
        let credentials = Credentials::resolve(&config, env, &secrets)?;
        let target_circuits = CircuitBreaker::new(
            config.failover.failure_threshold,
            Duration::from_secs(config.failover.cooldown_seconds),
        );
        let mut inbound_keys: Vec<GatewayKeyEntry> = Vec::new();
        let mut gateway_key_fingerprints = HashMap::new();
        for k in &config.gateway_key {
            let source = k
                .source()
                .ok_or_else(|| SnapshotError::InvalidGatewayKeySource {
                    namespace: k.namespace.clone(),
                })?;
            let label = k
                .source_label()
                .ok_or_else(|| SnapshotError::InvalidGatewayKeySource {
                    namespace: k.namespace.clone(),
                })?;
            let secret = key_material::resolve(source, env).map_err(|error| match error {
                KeyMaterialError::MissingEnv { name } => SnapshotError::MissingGatewayKey {
                    namespace: k.namespace.clone(),
                    env: name,
                },
                KeyMaterialError::FileRead { path, kind, error } => SnapshotError::GatewayKeyFile {
                    namespace: k.namespace.clone(),
                    path,
                    kind,
                    error,
                },
                KeyMaterialError::EmptyFile { path } => SnapshotError::EmptyGatewayKeyFile {
                    namespace: k.namespace.clone(),
                    path,
                },
                KeyMaterialError::InvalidUtf8 { path } => {
                    SnapshotError::InvalidGatewayKeyFileUtf8 {
                        namespace: k.namespace.clone(),
                        path,
                    }
                }
            })?;
            if secret.starts_with(WorkloadKey::PREFIX) {
                return Err(SnapshotError::ReservedGatewayKeyShape {
                    namespace: k.namespace.clone(),
                    shape: WorkloadKey::PREFIX,
                });
            }
            // Two keys resolving to one secret is ambiguous authority — one
            // namespace would silently win — so reject it. Compared here on the
            // operator-supplied values at boot, never at request time.
            if let Some(other) = inbound_keys.iter().find(|e| {
                crate::principals::constant_time_eq(
                    e.secret.expose_secret().as_bytes(),
                    secret.as_bytes(),
                )
            }) {
                return Err(SnapshotError::DuplicateGatewayKey {
                    env: label.to_owned(),
                    namespace: k.namespace.clone(),
                    other_env: other.caller.subject.clone(),
                    other_namespace: other.caller.namespace.clone(),
                });
            }
            inbound_keys.push(GatewayKeyEntry {
                secret: SecretString::from(secret.clone()),
                caller: InboundKey {
                    namespace: k.namespace.clone(),
                    subject: label.to_owned(),
                    authority: PrincipalAuthority::StaticKey,
                    signer_kid: None,
                    scope: None,
                    alias_scope: None,
                    max_request_microdollars: None,
                    can_mint: k.can_mint,
                    jti: None,
                    namespace_grant: Some(crate::namespace::NamespaceGrant::all()),
                },
            });
            gateway_key_fingerprints
                .insert(label.to_owned(), key_material::fingerprint(label, &secret));
        }
        // Inbound authentication fails closed: there is no keyless deployment
        // that serves inference. A stateful replica cannot declare
        // `[[gateway_key]]` at all — the section is rejected by
        // `Config::validate_stateful` — because its inbound principals arrive
        // with a compiled revision instead of the file, and until that compiler
        // exists the runtime answers every inference request with
        // `ops::inference_refusal` instead of the snapshot. A keyless snapshot
        // is therefore admissible exactly while that refusal stands, and the
        // condition is asked of the refusal itself rather than of the mode: when
        // the projection lands and `inference_refusal` returns `None`, this
        // rejects the keyless snapshot again with no edit here, which is the
        // only ordering that cannot serve inference from an empty snapshot.
        let projected_principals = ProjectedPrincipals::new(config.projected_principals.clone());
        if inbound_keys.is_empty()
            && projected_principals.count() == 0
            && !(allow_keyless_bootstrap && config.mode == crate::config::Mode::Stateful)
        {
            return Err(SnapshotError::NoInboundKeys);
        }
        let inbound_keys: Arc<[GatewayKeyEntry]> = inbound_keys.into();
        let config_principals = ConfigPrincipals::new(Arc::clone(&inbound_keys));
        let verifier = TokenVerifier::build(&config, env)?;
        let gateway_verifier_fingerprints = verifier
            .as_ref()
            .map(TokenVerifier::fingerprints)
            .unwrap_or_default();
        let gateway_minting = if let Some(minting) = config.gateway_minting.as_ref() {
            let config_verifier = config
                .gateway_verifier
                .iter()
                .find(|verifier| verifier.kid == minting.kid)
                .ok_or_else(|| SnapshotError::MintingVerifierNotFound {
                    kid: minting.kid.clone(),
                })?;
            let source = minting
                .source()
                .ok_or(SnapshotError::InvalidMintingSource)?;
            let material = key_material::resolve(source, env).map_err(|error| match error {
                KeyMaterialError::MissingEnv { name } => {
                    SnapshotError::MissingMintingKey { env: name }
                }
                KeyMaterialError::FileRead { path, kind, error } => {
                    SnapshotError::MintingKeyFile { path, kind, error }
                }
                KeyMaterialError::EmptyFile { path } => SnapshotError::EmptyMintingKeyFile { path },
                KeyMaterialError::InvalidUtf8 { path } => {
                    SnapshotError::InvalidMintingKeyFileUtf8 { path }
                }
            })?;
            let algorithm = match config_verifier.alg {
                GatewayVerifierAlgorithm::EdDsa => crate::mint::MintAlgorithm::EdDsa,
                GatewayVerifierAlgorithm::Hs256 => crate::mint::MintAlgorithm::Hs256,
            };
            crate::mint::validate_signing_material(algorithm, &material, &minting.kid).map_err(
                |error| SnapshotError::MintingKey {
                    reference: minting.source_label().unwrap_or(&minting.kid).to_owned(),
                    error: error.to_string(),
                },
            )?;
            if !verifier.as_ref().is_some_and(|verifier| {
                verifier.signing_material_matches(&minting.kid, config_verifier.alg, &material)
            }) {
                return Err(SnapshotError::MintingKeyMismatch {
                    kid: minting.kid.clone(),
                    reference: minting.source_label().unwrap_or(&minting.kid).to_owned(),
                });
            }
            let audience = config
                .gateway_token
                .as_ref()
                .map(|token| token.audience.trim())
                .filter(|audience| !audience.is_empty())
                .ok_or(SnapshotError::MissingMintingAudience)?
                .to_owned();
            let scope = minting
                .scope
                .as_ref()
                .map(|values| {
                    values
                        .iter()
                        .map(|value| {
                            Capability::parse(value).ok_or_else(|| {
                                SnapshotError::InvalidMintingCapability {
                                    value: value.clone(),
                                }
                            })
                        })
                        .collect::<Result<HashSet<_>, _>>()
                })
                .transpose()?;
            let aliases = minting
                .aliases
                .as_ref()
                .map(|values| AliasScope::parse(values.iter().map(String::as_str)))
                .transpose()
                .map_err(|error| SnapshotError::InvalidMintingAliases {
                    error: error.to_string(),
                })?;
            Some(ResolvedMinting {
                kid: minting.kid.clone(),
                algorithm,
                key_material: SecretString::from(material),
                audience,
                max_ttl: minting.max_ttl.unwrap_or(config_verifier.max_ttl),
                scope,
                aliases,
                max_request_microdollars: minting.max_request_microdollars,
            })
        } else {
            None
        };
        let mut stores: Vec<Box<dyn crate::principals::PrincipalStore>> =
            vec![Box::new(projected_principals)];
        stores.extend(
            verifier
                .into_iter()
                .map(|verifier| Box::new(verifier) as Box<dyn crate::principals::PrincipalStore>),
        );
        let principals = PrincipalStoreChain::new(stores, config_principals)?;
        let gateway_minting_fingerprint = config
            .gateway_minting
            .as_ref()
            .zip(gateway_minting.as_ref())
            .map(|(minting, resolved)| {
                key_material::fingerprint(
                    minting.source_label().unwrap_or(&resolved.kid),
                    resolved.key_material.expose_secret(),
                )
            });
        Ok(Self {
            config,
            credentials,
            middleware,
            target_circuits,
            principals,
            generation,
            gateway_key_fingerprints,
            gateway_verifier_fingerprints,
            gateway_minting_fingerprint,
            gateway_minting,
            gateway_token_epochs,
            secrets,
            // Never a populated index, and never an optimistic one: a snapshot is
            // compiled from configuration, and availability is derived afterwards
            // by whatever produced the evidence.
            availability: None,
            pricing: None,
            admin_authorization: None,
        })
    }

    /// The durable material this snapshot holds.
    ///
    /// A lookup by exact reference, never a resolution: nothing here can reach a
    /// secret store, which is what makes "no request touches the store" a property
    /// of the type rather than a convention.
    pub const fn secrets(&self) -> &ResolvedSecrets {
        &self.secrets
    }

    /// Content middleware selected by the policy governing `namespace` in this
    /// exact serving generation.
    pub(crate) fn middleware(&self, namespace: &str) -> &MiddlewareChain {
        self.middleware.for_namespace(namespace)
    }

    /// The derived availability index this snapshot carries, if it derives one.
    #[allow(dead_code)]
    pub fn availability(&self) -> Option<&AvailabilityIndex> {
        self.availability.as_deref()
    }

    /// The index as a handle, for carrying the evidence an outgoing snapshot holds
    /// onto its replacement without cloning the records.
    pub fn availability_handle(&self) -> Option<Arc<AvailabilityIndex>> {
        self.availability.clone()
    }

    /// Project a derived availability index onto a snapshot that has not been
    /// published yet.
    ///
    /// Consuming, deliberately: a published snapshot is immutable and replaced
    /// whole ([`AppState::publish`]), so availability is attached on the way
    /// to publication rather than mutated underneath a reader holding the `Arc`.
    /// Nothing else about the snapshot changes — the config, the credential graph,
    /// and the circuits are the ones compilation produced.
    ///
    /// # Reloads re-project, deliberately
    ///
    /// [`ConfigSnapshot::build`] derives no view at all, so a reload keeps
    /// availability only by asking for it:
    ///
    /// ```ignore
    /// let outgoing = state.snapshot();
    /// let next = match outgoing.availability_handle() {
    ///     Some(availability) => {
    ///         ConfigSnapshot::build(config, &env, generation)?.with_availability(availability)
    ///     }
    ///     None => ConfigSnapshot::build(config, &env, generation)?,
    /// };
    /// ```
    ///
    /// Silent inheritance is the behaviour being refused, not an oversight: evidence
    /// is derived against a particular catalogue, credential set, and set of
    /// namespaces, so a reload that changed any of those would be carrying verdicts
    /// about targets the new config may no longer declare. A reload therefore either
    /// re-derives availability or re-projects the outgoing handle because it knows
    /// nothing relevant changed — and either way the choice is visible at the call
    /// site.
    ///
    /// The file reloader ([`crate::reload`]) makes the second choice, and can:
    /// nothing availability is derived from is in the file. The four durable
    /// dimensions come from the revision's enablements, connections, credentials,
    /// and policy documents, the evidence from discovery, and health is overlaid
    /// at read time from the serving snapshot's own circuits — so a reload can
    /// neither invalidate a verdict nor restate one. Dropping or blanking the
    /// index would make a `SIGHUP` the way an operator loses the answer to which
    /// models a tenant can reach, and keep it lost: convergence compiles only
    /// when desired state changes, which a file edit is not.
    #[must_use]
    pub fn with_availability(mut self, availability: Arc<AvailabilityIndex>) -> Self {
        self.availability = Some(availability);
        self
    }

    /// Attach the approved pricing a revision resolved to.
    ///
    /// Consuming rather than a setter: a published snapshot is immutable, so
    /// pricing is attached while the snapshot is still owned by the compiler that
    /// built it and never after it is visible to a request.
    #[must_use]
    pub fn with_pricing(mut self, pricing: PricingSnapshot) -> Self {
        self.pricing = Some(pricing);
        self
    }

    /// The approved pricing this snapshot serves under, if any.
    pub const fn pricing(&self) -> Option<&PricingSnapshot> {
        self.pricing.as_ref()
    }

    /// The immutable administrative authorization view compiled with this
    /// snapshot, if the snapshot came from a typed stateful revision.
    pub fn admin_authorization_handle(&self) -> Option<Arc<AuthorizationSnapshot>> {
        self.admin_authorization.clone()
    }

    /// Attach the administrative authorization view before publication.
    #[must_use]
    pub fn with_admin_authorization(mut self, authorization: Arc<AuthorizationSnapshot>) -> Self {
        self.admin_authorization = Some(authorization);
        self
    }

    pub async fn resolve_principal(
        &self,
        presented: &Presented<'_>,
    ) -> Result<Option<InboundKey>, crate::principals::PrincipalStoreError> {
        self.principals.resolve(presented).await
    }

    pub fn principal_store_name(&self, presented: &Presented<'_>) -> &'static str {
        self.principals.owner_name(presented)
    }

    /// What authenticating this credential will cost, before any of it is spent:
    /// whether resolving it can reach a backend, or only memory.
    pub fn diagnostic_credential(&self, presented: &Presented<'_>) -> DiagnosticCredential {
        if self.principals.resolves_in_memory(presented) {
            DiagnosticCredential::Local
        } else {
            DiagnosticCredential::Minted
        }
    }

    /// How many inbound gateway keys are enforced. For the boot log and reload
    /// metrics — the count is safe to surface, the secrets are not.
    pub fn inbound_key_count(&self) -> usize {
        self.principals.config_count() + self.config.projected_principals.len()
    }

    pub fn token_verifier_count(&self) -> usize {
        self.config.gateway_verifier.len()
    }

    pub(crate) fn gateway_token_epoch(&self, namespace: &str, subject: &str) -> Option<u64> {
        resolve_token_epoch(&self.gateway_token_epochs, namespace, subject)
    }
}

impl AppState {
    /// Fails when a declared credential's or gateway key's env var is missing or
    /// empty — both are resolved at boot, not at request time.
    #[cfg(test)]
    pub fn new(
        config: Config,
        env: &HashMap<String, String>,
        usage: UsageFanout,
        budget: Box<dyn BudgetStore>,
    ) -> Result<Self, SnapshotError> {
        Self::new_with_rate_limiter(
            config,
            env,
            usage,
            budget,
            Box::new(NoLimit),
            Box::new(crate::revocation::NoDenylist),
        )
    }

    /// Test-only: production builds go through [`AppState::new_with_policy`],
    /// which threads the one [`PolicyRuntime`] the backends read and the
    /// observability the mode it booted in decides.
    #[cfg(test)]
    pub fn new_with_rate_limiter(
        config: Config,
        env: &HashMap<String, String>,
        usage: UsageFanout,
        budget: Box<dyn BudgetStore>,
        rate_limiter: Box<dyn RateLimiter>,
        revocation: Box<dyn RevocationStore>,
    ) -> Result<Self, SnapshotError> {
        Self::new_with_observability(
            config,
            env,
            usage,
            budget,
            rate_limiter,
            revocation,
            ReplicaObservability::stateless(),
        )
    }

    /// What this replica serves, plus what it reports about itself, for a
    /// deployment whose usage is telemetry-grade. The boot path builds its
    /// delivery first and calls [`AppState::with_resources`].
    #[cfg(test)]
    pub fn new_with_observability(
        config: Config,
        env: &HashMap<String, String>,
        usage: UsageFanout,
        budget: Box<dyn BudgetStore>,
        rate_limiter: Box<dyn RateLimiter>,
        revocation: Box<dyn RevocationStore>,
        observability: ReplicaObservability,
    ) -> Result<Self, SnapshotError> {
        Self::with_resources(
            config,
            env,
            Arc::new(UsageDelivery::telemetry(usage)),
            budget,
            rate_limiter,
            revocation,
            observability,
        )
    }

    /// Every process-level resource already connected, usage delivery included,
    /// so a deployment that cannot reach a datastore it asked for has already
    /// failed before this is called.
    ///
    /// The policy runtime is this replica's own, which is what makes this a
    /// caller-without-a-runtime constructor rather than the boot path: the
    /// serving binary builds its stores *reading* a runtime and must hand that
    /// same one to [`AppState::new_with_policy`], since a state publishing into
    /// a runtime its stores do not read enforces nothing.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn with_resources(
        config: Config,
        env: &HashMap<String, String>,
        usage: Arc<UsageDelivery>,
        budget: Box<dyn BudgetStore>,
        rate_limiter: Box<dyn RateLimiter>,
        revocation: Box<dyn RevocationStore>,
        observability: ReplicaObservability,
    ) -> Result<Self, SnapshotError> {
        let policy = Arc::new(PolicyRuntime::bootstrap(&config));
        Self::new_with_policy(
            config,
            env,
            usage,
            budget,
            rate_limiter,
            revocation,
            policy,
            observability,
        )
    }

    /// The serving constructor: the stores were built reading `policy`, so the
    /// state that publishes into it must be the state they read.
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_policy(
        config: Config,
        env: &HashMap<String, String>,
        usage: Arc<UsageDelivery>,
        budget: Box<dyn BudgetStore>,
        rate_limiter: Box<dyn RateLimiter>,
        revocation: Box<dyn RevocationStore>,
        policy: Arc<PolicyRuntime>,
        observability: ReplicaObservability,
    ) -> Result<Self, SnapshotError> {
        // The transport bounds configure the shared client, so they are read
        // once here: a reload validates a change and reports that it needs a
        // restart rather than swapping the pool under in-flight requests.
        let limits = config.transport.limits();
        let stream_terminal_grace =
            Duration::from_millis(config.transport.stream_terminal_grace_ms);
        let admission = AdmissionControl::from_config(&config.admission);
        let snapshot = if config.mode == crate::config::Mode::Stateful {
            ConfigSnapshot::build_bootstrap(config, env, 0)?
        } else {
            ConfigSnapshot::build(config, env, 0)?
        };
        Ok(AppState(Arc::new(Inner {
            dispatcher: HttpDispatcher::with_limits(
                build_client(&limits).expect("the upstream HTTP client builds"),
                limits,
            ),
            stream_terminal_grace,
            usage,
            budget,
            admission,
            rate_limiter,
            revocation,
            policy,
            lifecycle: Arc::new(Lifecycle::new()),
            status: observability.status,
            revision: observability.revision,
            catalogue: observability.catalogue,
            store: open_store_sync(&snapshot.config)?,
            #[cfg(test)]
            middleware: MiddlewareChain::empty(),
            middleware_runtime: MiddlewareRuntime::default(),
            config: ArcSwap::from_pointee(snapshot),
        })))
    }

    pub fn store(&self) -> Option<&std::sync::Arc<dyn crate::store::Store>> {
        Some(&self.0.store)
    }

    #[allow(dead_code)]
    pub fn set_store(&mut self, store: std::sync::Arc<dyn crate::store::Store>) {
        if let Some(inner) = std::sync::Arc::get_mut(&mut self.0) {
            inner.store = store;
        }
    }

    /// Install a test or boot-constructed content chain before the state is
    /// shared with the router. Runtime policy delivery will replace this
    /// constructor-only hook with a snapshot-owned chain.
    #[cfg(test)]
    pub fn with_middleware_chain(mut self, middleware: MiddlewareChain) -> Self {
        Arc::get_mut(&mut self.0)
            .expect("middleware chain must be installed before AppState is cloned")
            .middleware = middleware;
        self
    }

    /// The process lifecycle: what readiness reports and what admission checks.
    pub fn lifecycle(&self) -> &Arc<Lifecycle> {
        &self.0.lifecycle
    }

    /// The cached dependency observations the authenticated status view projects.
    pub fn status(&self) -> &Arc<CachedStatusRegistry> {
        &self.0.status
    }

    /// This replica's convergence report, when it converges at all.
    pub fn revision_report(&self) -> Option<RevisionReport> {
        self.0.revision.as_ref().map(|status| status.report())
    }

    /// What the catalogue import last reported, when this deployment imports.
    ///
    /// `None` covers both "imports nothing" and "has not finished its first
    /// attempt", which are the same thing to a caller: there is nothing to say
    /// about a catalogue yet.
    pub fn catalogue_report(&self) -> Option<CatalogReport> {
        self.0
            .catalogue
            .as_ref()
            .and_then(|catalogue| catalogue.report())
    }

    /// The config snapshot a request runs against. Taken once per request and
    /// held for its duration, so a concurrent reload cannot half-apply.
    pub fn config(&self) -> Arc<ConfigSnapshot> {
        self.0.config.load_full()
    }

    /// Publish a new snapshot. In-flight requests keep the snapshot they already
    /// hold; every request that starts after this call sees the new one.
    pub fn publish(&self, snapshot: ConfigSnapshot) {
        self.0.config.store(Arc::new(snapshot));
    }

    /// The stateful policy this replica enforces.
    pub fn policy(&self) -> &Arc<PolicyRuntime> {
        &self.0.policy
    }
}

/// A replica answers availability questions from what it is already serving.
///
/// Both halves come from the loaded snapshot, and neither reaches a store: the
/// index is the projection compilation attached to it, and the health is the
/// circuits that snapshot's own requests have been tripping. Loaded once, so an
/// answer cannot describe one revision's targets with another revision's
/// circuits.
///
/// The health is [`CircuitBreaker::observed`] rather than
/// [`CircuitBreaker::snapshot`]: the question this read answers is what the
/// replica would do with the next request, so a target whose cooldown has
/// elapsed reports as impaired rather than as refused. Reading still moves
/// nothing — an operator looking at a target cannot spend its probe.
impl AvailabilityReader for AppState {
    fn read(&self) -> Option<(Arc<AvailabilityIndex>, RuntimeObservations)> {
        let snapshot = self.config();
        let index = snapshot.availability_handle()?;
        let runtime = RuntimeObservations::of_circuits(snapshot.target_circuits.observed());
        Some((index, runtime))
    }
}

/// Build the zero-size adapter for a provider kind. Adapters carry no state,
/// so this is cheap to call per request.
pub fn adapter_for(kind: ProviderKind) -> Box<dyn ProviderAdapter> {
    match kind {
        ProviderKind::Openai => Box::new(OpenAiCompatibleAdapter::openai()),
        ProviderKind::OpenaiCompatible => {
            Box::new(OpenAiCompatibleAdapter::new(OpenAiFlavor::Compatible))
        }
        ProviderKind::Anthropic => Box::new(AnthropicAdapter::new()),
    }
}

fn open_store_sync(config: &Config) -> Result<Arc<dyn crate::store::Store>, SnapshotError> {
    let storage = config
        .storage
        .as_ref()
        .ok_or_else(|| SnapshotError::Store("`[storage]` is required".into()))?;
    match storage.backend {
        StorageBackend::Sqlite => {
            let path = storage.path.as_deref().unwrap_or(":memory:");
            let store = crate::store::SqliteStore::open(path)
                .map_err(|error| SnapshotError::Store(error.to_string()))?;
            for namespace in &config.namespace {
                let record = crate::store::NamespaceRecord {
                    id: namespace.id.clone(),
                    attrs: serde_json::json!({}),
                    blocklist: None,
                };
                let _ = futures::executor::block_on(store.put_namespace(record));
            }
            Ok(Arc::new(store))
        }
        StorageBackend::Postgres => {
            // `serve()` replaces this with a connected Postgres store before
            // the listener binds. Tests that never connect still get a store.
            let store = crate::store::SqliteStore::open(":memory:")
                .map_err(|error| SnapshotError::Store(error.to_string()))?;
            Ok(Arc::new(store))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::availability::{AvailabilityKey, AvailabilityRecord, ScopeRef, TargetRef};
    use crate::budget::NoBudget;
    use crate::desired_state::ContentMiddlewareRegistration;
    use crate::desired_state::fixtures::{policy_body, revision_id, secret_ref, tenant_id};
    use crate::desired_state::policy::{ContentGuardrailRegistration, PolicyScope};
    use crate::desired_state::{TenantId, Uuid7};
    use crate::usage::UsageSink;
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use ring::rand::SystemRandom;
    use ring::signature::{Ed25519KeyPair, KeyPair};
    use std::sync::atomic::{AtomicU64, Ordering};

    fn temp_file(contents: &[u8]) -> String {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "axond-state-key-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::write(&path, contents).unwrap();
        path.to_str().unwrap().to_owned()
    }

    fn config_with(gateway_keys: &str) -> Config {
        Config::from_toml_str(&format!(
            r#"
[storage]
backend = "sqlite"
path = ":memory:"

[[namespace]]
id = "platform"
default = true

[[provider]]
id = "openai"
kind = "openai"
base_url = "https://api.openai.com/v1"

{gateway_keys}

[[model]]
name = "gpt-4o"
targets = [{{ provider = "openai", model = "gpt-4o", price = {{ input_microdollars_per_million = 1, output_microdollars_per_million = 1 }} }}]
"#
        ))
        .expect("valid config")
    }

    const PLATFORM_KEY: &str = r#"
[[gateway_key]]
env = "AXOND_KEY"
namespace = "platform"
"#;

    fn middleware_policy(epoch: u64, id: &str) -> NamespacePolicy {
        let body = policy_body(PolicyScope::Tenant(tenant_id(1)), epoch)
            .with_content_middleware(vec![
                ContentMiddlewareRegistration::new(
                    id,
                    [gateway_core::MiddlewareScope::Request],
                    gateway_core::MiddlewareFailurePosture::FailClosed,
                    25,
                )
                .expect("valid registration"),
            ])
            .expect("registration attaches")
            .with_buffered_response_routes([
                BufferedResponseRoute::Responses,
                BufferedResponseRoute::Messages,
            ])
            .expect("buffering routes attach");
        let generation = body.generation(revision_id(epoch));
        NamespacePolicy { body, generation }
    }

    fn redaction_policy(epoch: u64) -> NamespacePolicy {
        let registration = ContentMiddlewareRegistration::new(
            "axond.redact",
            [
                gateway_core::MiddlewareScope::Request,
                gateway_core::MiddlewareScope::Response,
                gateway_core::MiddlewareScope::StreamEvent,
            ],
            gateway_core::MiddlewareFailurePosture::FailClosed,
            25,
        )
        .unwrap()
        .with_guardrail(
            ContentGuardrailRegistration::new(
                "GW_GUARDRAIL_KEY",
                vec![gateway_core::GuardrailRule {
                    id: "email".to_owned(),
                    pattern: r"[a-z]+@example\.com".to_owned(),
                    action: gateway_core::GuardrailAction::Redact,
                }],
            )
            .unwrap(),
        )
        .unwrap();
        let body = policy_body(PolicyScope::Tenant(tenant_id(1)), epoch)
            .with_content_middleware(vec![registration])
            .unwrap()
            .with_buffered_response_routes([
                BufferedResponseRoute::Responses,
                BufferedResponseRoute::Messages,
            ])
            .unwrap();
        let generation = body.generation(revision_id(epoch));
        NamespacePolicy { body, generation }
    }

    #[tokio::test]
    async fn middleware_policy_hot_reload_rollback_and_rejection_are_snapshot_atomic() {
        let env = HashMap::from([("AXOND_KEY".to_owned(), "platform-secret".to_owned())]);
        let sinks: Vec<Box<dyn UsageSink>> = Vec::new();
        let state = AppState::new(
            config_with(PLATFORM_KEY),
            &env,
            UsageFanout::new(sinks),
            Box::new(NoBudget),
        )
        .expect("base state starts");

        let mut added = config_with(PLATFORM_KEY);
        added.namespace[0].policy = Some(middleware_policy(1, "test.policy-marker"));
        state.publish(ConfigSnapshot::build(added, &env, 1).expect("addition compiles"));
        let held_added = state.config();
        let mut request = gateway_core::ProviderRequest {
            model: "alias".to_owned(),
            body: serde_json::json!({}),
        };
        held_added
            .middleware("platform")
            .request(&state.0.middleware_runtime, &mut request)
            .await
            .expect("added chain runs");
        assert_eq!(request.body["policy_middleware"], "test.policy-marker");

        let removed = config_with(PLATFORM_KEY);
        state.publish(ConfigSnapshot::build(removed, &env, 2).expect("removal compiles"));
        assert!(state.config().middleware("platform").is_empty());
        assert_eq!(held_added.middleware("platform").len(), 1);

        let mut invalid = config_with(PLATFORM_KEY);
        invalid.namespace[0].policy = Some(middleware_policy(3, "test.not-compiled"));
        assert!(matches!(
            ConfigSnapshot::build(invalid, &env, 3),
            Err(SnapshotError::Middleware(_))
        ));
        assert_eq!(state.config().generation, 2);
        assert!(state.config().middleware("platform").is_empty());

        let mut rollback = config_with(PLATFORM_KEY);
        rollback.namespace[0].policy = Some(middleware_policy(4, "test.policy-marker"));
        state.publish(ConfigSnapshot::build(rollback, &env, 4).expect("rollback compiles"));
        assert_eq!(state.config().middleware("platform").len(), 1);
    }

    #[test]
    fn cached_snapshot_restores_registered_middleware_and_refuses_missing_code() {
        let env = HashMap::from([("AXOND_KEY".to_owned(), "platform-secret".to_owned())]);
        let mut config = config_with(PLATFORM_KEY);
        config.namespace[0].policy = Some(middleware_policy(1, "test.policy-marker"));
        let snapshot = ConfigSnapshot::build(config, &env, 7).expect("snapshot compiles");
        let revision = revision_id(7);

        let cached = snapshot.cached_serving(revision);
        let (restored_revision, restored) =
            ConfigSnapshot::from_cached_serving(config_with(PLATFORM_KEY), &env, cached)
                .expect("registered middleware restores");
        assert_eq!(restored_revision, revision);
        assert_eq!(restored.middleware("platform").len(), 1);
        assert_eq!(
            restored.config.namespace[0]
                .policy
                .as_ref()
                .unwrap()
                .body
                .buffered_response_routes(),
            [
                BufferedResponseRoute::Messages,
                BufferedResponseRoute::Responses,
            ]
        );

        let mut unavailable = snapshot.cached_serving(revision);
        unavailable.namespaces[0]
            .policy
            .as_mut()
            .unwrap()
            .content_middleware[0]
            .id = "test.not-compiled".to_owned();
        let error =
            match ConfigSnapshot::from_cached_serving(config_with(PLATFORM_KEY), &env, unavailable)
            {
                Ok(_) => panic!("cache cannot silently drop unavailable middleware"),
                Err(error) => error,
            };
        assert!(
            error.contains("not compiled into this axond build"),
            "{error}"
        );
    }

    #[test]
    fn legacy_cache_restores_one_tenant_key_shared_across_namespaces() {
        let env = HashMap::from([("AXOND_KEY".to_owned(), "platform-secret".to_owned())]);
        let snapshot =
            ConfigSnapshot::build(config_with(PLATFORM_KEY), &env, 7).expect("snapshot compiles");
        let revision = revision_id(7);
        let reference = secret_ref(991);
        let mut cached = snapshot.cached_serving(revision);
        let mut sibling = cached.namespaces[0].clone();
        sibling.id = "sibling".to_owned();
        sibling.default = false;
        cached.namespaces.push(sibling);
        cached.credentials = ["platform", "sibling"]
            .into_iter()
            .map(|namespace| CachedCredential {
                namespace: namespace.to_owned(),
                provider: "openai".to_owned(),
                env: None,
                id: Some("tenant-default".to_owned()),
                weight: 1,
                secret: Some(reference.to_string()),
            })
            .collect();
        cached.secrets.push(CachedSecret {
            reference: reference.to_string(),
            binding: CachedSecretBinding::Legacy,
            material: "shared-provider-key".to_owned(),
        });

        let (_, restored) =
            ConfigSnapshot::from_cached_serving(config_with(PLATFORM_KEY), &env, cached)
                .expect("legacy shared tenant material remains recoverable");
        assert_eq!(restored.config.credential.len(), 2);
        assert!(restored.secrets.get(reference).is_some());
    }

    #[test]
    fn legacy_cache_restores_staged_material_not_in_the_serving_pool() {
        let env = HashMap::from([("AXOND_KEY".to_owned(), "platform-secret".to_owned())]);
        let snapshot =
            ConfigSnapshot::build(config_with(PLATFORM_KEY), &env, 7).expect("snapshot compiles");
        let revision = revision_id(7);
        let reference = secret_ref(992);
        let mut cached = snapshot.cached_serving(revision);
        cached.secrets.push(CachedSecret {
            reference: reference.to_string(),
            binding: CachedSecretBinding::Legacy,
            material: "staged-provider-key".to_owned(),
        });

        let (_, restored) =
            ConfigSnapshot::from_cached_serving(config_with(PLATFORM_KEY), &env, cached)
                .expect("legacy staged material remains recoverable");
        assert!(restored.secrets.get(reference).is_some());
        assert_eq!(restored.cached_serving(revision).secrets.len(), 1);
    }

    #[tokio::test]
    async fn cached_snapshot_restores_guardrail_rules_and_key_reference() {
        let env = HashMap::from([
            ("AXOND_KEY".to_owned(), "platform-secret".to_owned()),
            ("GW_GUARDRAIL_KEY".to_owned(), STANDARD.encode([7_u8; 32])),
        ]);
        let mut config = config_with(PLATFORM_KEY);
        config.namespace[0].policy = Some(redaction_policy(1));
        let snapshot = ConfigSnapshot::build(config, &env, 7).expect("guardrail compiles");
        let revision = revision_id(7);
        let cached = snapshot.cached_serving(revision);
        let missing_key_env =
            HashMap::from([("AXOND_KEY".to_owned(), "platform-secret".to_owned())]);
        let error = match ConfigSnapshot::from_cached_serving(
            config_with(PLATFORM_KEY),
            &missing_key_env,
            cached.clone(),
        ) {
            Ok(_) => panic!("a cache cannot bypass guardrail key resolution"),
            Err(error) => error,
        };
        assert!(error.contains("GW_GUARDRAIL_KEY"), "{error}");
        let rotated_env = HashMap::from([
            ("AXOND_KEY".to_owned(), "platform-secret".to_owned()),
            ("GW_GUARDRAIL_KEY".to_owned(), STANDARD.encode([8_u8; 32])),
        ]);
        let error = match ConfigSnapshot::from_cached_serving(
            config_with(PLATFORM_KEY),
            &rotated_env,
            cached.clone(),
        ) {
            Ok(_) => panic!("a cache cannot silently change guardrail key identity"),
            Err(error) => error,
        };
        assert!(error.contains("resolves to different material"), "{error}");
        let (_, restored) =
            ConfigSnapshot::from_cached_serving(config_with(PLATFORM_KEY), &env, cached)
                .expect("guardrail cache restores");
        let registration = &restored.config.namespace[0]
            .policy
            .as_ref()
            .unwrap()
            .body
            .content_middleware()[0];
        let guardrail = registration.guardrail().expect("guardrail configuration");
        assert_eq!(guardrail.key_env(), "GW_GUARDRAIL_KEY");
        assert_eq!(guardrail.rules()[0].id, "email");

        let mut request = gateway_core::ProviderRequest {
            model: "alias".to_owned(),
            body: serde_json::json!({"messages": [{
                "role": "user",
                "content": "alice@example.com"
            }]}),
        };
        let mut execution = restored
            .middleware("platform")
            .start_with_protected_values(
                &MiddlewareRuntime::default(),
                &mut request,
                &[],
                gateway_core::MiddlewareSurface::ChatCompletions,
            )
            .await
            .expect("restored guardrail masks");
        assert_ne!(request.body["messages"][0]["content"], "alice@example.com");
        let mut response = gateway_core::ProviderResponse {
            body: serde_json::json!({"choices": [{"message": {
                "content": request.body["messages"][0]["content"].clone()
            }}]}),
            usage: gateway_core::ModelUsage::default(),
        };
        execution
            .response(&mut response)
            .await
            .expect("restored guardrail unmasks");
        assert_eq!(
            response.body["choices"][0]["message"]["content"],
            "alice@example.com"
        );
    }

    #[test]
    fn cached_guardrail_records_reject_unknown_nested_fields() {
        let env = HashMap::from([
            ("AXOND_KEY".to_owned(), "platform-secret".to_owned()),
            ("GW_GUARDRAIL_KEY".to_owned(), STANDARD.encode([7_u8; 32])),
        ]);
        let mut config = config_with(PLATFORM_KEY);
        config.namespace[0].policy = Some(redaction_policy(1));
        let snapshot = ConfigSnapshot::build(config, &env, 7).expect("guardrail compiles");
        let cached = snapshot.cached_serving(revision_id(7));

        for path in ["middleware", "guardrail"] {
            let mut value = serde_json::to_value(&cached).expect("cache serializes");
            let registration = &mut value["namespaces"][0]["policy"]["content_middleware"][0];
            let object = if path == "middleware" {
                registration.as_object_mut().expect("middleware record")
            } else {
                registration["guardrail"]
                    .as_object_mut()
                    .expect("guardrail record")
            };
            object.insert("unknown".to_owned(), serde_json::json!(true));
            assert!(
                serde_json::from_value::<CachedServingSnapshot>(value).is_err(),
                "unknown {path} field was ignored"
            );
        }
    }

    #[test]
    fn encrypted_cache_round_trips_buffered_routes_and_old_payloads_default_empty() {
        let env = HashMap::from([("AXOND_KEY".to_owned(), "platform-secret".to_owned())]);
        let mut config = config_with(PLATFORM_KEY);
        config.namespace[0].policy = Some(middleware_policy(1, "test.policy-marker"));
        let snapshot = ConfigSnapshot::build(config, &env, 7).expect("snapshot compiles");
        let revision = revision_id(7);
        let cache_path = temp_file(b"signed-cache-placeholder");
        let cache = crate::convergence::LastKnownGood::new(&cache_path, &[0x59; 32])
            .expect("cache key is valid");
        let bytes = cache
            .encode_compiled(&snapshot, revision)
            .expect("compiled cache encrypts");
        assert!(
            !bytes
                .windows("responses".len())
                .any(|window| window == b"responses")
        );
        cache
            .write_compiled(&bytes)
            .expect("encrypted compiled cache writes");
        let loaded = cache
            .load_compiled()
            .expect("encrypted compiled cache authenticates")
            .expect("compiled cache exists");
        assert_eq!(
            loaded.namespaces[0]
                .policy
                .as_ref()
                .unwrap()
                .buffered_response_routes,
            ["messages", "responses"]
        );

        let mut old_payload = serde_json::to_value(snapshot.cached_serving(revision)).unwrap();
        old_payload["namespaces"][0]["policy"]
            .as_object_mut()
            .unwrap()
            .remove("buffered_response_routes");
        let old_payload: CachedServingSnapshot = serde_json::from_value(old_payload).unwrap();
        assert!(
            old_payload.namespaces[0]
                .policy
                .as_ref()
                .unwrap()
                .buffered_response_routes
                .is_empty()
        );

        let _ = std::fs::remove_file(cache.compiled_path());
        let _ = std::fs::remove_file(cache_path);
    }

    /// The production observation plan, not only the status route's projection,
    /// must mark an enabled importer as configured. Otherwise the component
    /// would remain `disabled` forever even though the background task is
    /// running and the catalogue report is available to operators.
    #[test]
    fn catalogue_imports_enable_the_catalogue_status_component() {
        let plan = ReplicaObservability::plan_with_catalogue(
            None,
            &crate::budget::NoBudget,
            &crate::rate_limit::NoLimit,
            &crate::revocation::NoDenylist,
            Some(Arc::new(CatalogStatus::new())),
        );

        assert_eq!(plan.components(), &[Component::Catalogue]);
        let (observability, refresher) = ReplicaObservability::observing(plan);
        assert!(refresher.is_some());
        let catalogue = observability
            .status
            .view()
            .components
            .into_iter()
            .find(|observed| observed.component == Component::Catalogue)
            .expect("the catalogue component is in every status view");
        assert_eq!(
            catalogue.state,
            crate::status::ComponentState::Unavailable,
            "enabled-but-not-yet-observed is not disabled"
        );
    }

    /// Availability is projected onto a snapshot, not compiled into it: a built
    /// snapshot knows nothing, and attaching an index leaves every config section
    /// exactly as compilation produced it (#206).
    #[test]
    fn projecting_availability_leaves_the_config_untouched() {
        let env = HashMap::from([("AXOND_KEY".to_owned(), "secret".to_owned())]);
        let snapshot =
            ConfigSnapshot::build(config_with(PLATFORM_KEY), &env, 0).expect("the key resolves");
        assert!(
            snapshot.availability().is_none(),
            "a compiled snapshot derives no view, which is not an empty one"
        );

        let scope = ScopeRef::tenant(TenantId::new(
            Uuid7::from_parts(1, 0, 1).expect("a valid id"),
        ));
        let target = TargetRef::parse("openai", "gpt-4o-preview").expect("a well-formed target");
        let index = AvailabilityIndex::builder()
            .record(
                AvailabilityKey::new(scope, target),
                AvailabilityRecord::enabled(),
            )
            .build();

        let models: Vec<String> = snapshot
            .config
            .model
            .iter()
            .map(|model| model.name.clone())
            .collect();
        let projected = snapshot.with_availability(Arc::new(index));
        assert_eq!(
            projected
                .availability()
                .expect("the projected snapshot derives a view")
                .len(),
            1
        );
        assert_eq!(
            projected
                .config
                .model
                .iter()
                .map(|model| model.name.clone())
                .collect::<Vec<_>>(),
            models,
            "an index describes reachability and can never enlarge what is served"
        );
        assert!(projected.config.model("gpt-4o-preview").is_none());
    }

    /// A rebuild starts from the empty index and carries evidence forward only when
    /// it re-projects the outgoing handle: the reload/projection handoff is explicit
    /// at the call site rather than an inheritance nobody wrote down (#206).
    #[test]
    fn a_rebuilt_snapshot_carries_availability_only_when_it_re_projects_it() {
        let env = HashMap::from([("AXOND_KEY".to_owned(), "secret".to_owned())]);
        let scope = ScopeRef::tenant(TenantId::new(
            Uuid7::from_parts(1, 0, 1).expect("a valid id"),
        ));
        let target = TargetRef::parse("openai", "gpt-4o").expect("a well-formed target");
        let index = AvailabilityIndex::builder()
            .record(
                AvailabilityKey::new(scope, target),
                AvailabilityRecord::enabled(),
            )
            .build();
        let outgoing = ConfigSnapshot::build(config_with(PLATFORM_KEY), &env, 0)
            .expect("the key resolves")
            .with_availability(Arc::new(index));

        let rebuilt =
            ConfigSnapshot::build(config_with(PLATFORM_KEY), &env, 1).expect("the key resolves");
        assert!(
            rebuilt.availability().is_none(),
            "a rebuild inherits no evidence it did not ask for"
        );

        let carried = ConfigSnapshot::build(config_with(PLATFORM_KEY), &env, 1)
            .expect("the key resolves")
            .with_availability(
                outgoing
                    .availability_handle()
                    .expect("the outgoing snapshot derives a view"),
            );
        assert_eq!(carried.availability(), outgoing.availability());
        assert_eq!(carried.generation, 1);
    }

    /// A declared key whose env var is unset or empty is a boot failure, not a
    /// silently dropped entry that would widen or empty the key table.
    #[test]
    fn a_declared_gateway_key_without_its_env_var_refuses_to_resolve() {
        for env in [
            HashMap::new(),
            HashMap::from([("AXOND_KEY".to_owned(), String::new())]),
        ] {
            let Err(err) = ConfigSnapshot::build(config_with(PLATFORM_KEY), &env, 0) else {
                panic!("the key cannot be resolved");
            };
            assert!(
                matches!(
                    err,
                    SnapshotError::MissingGatewayKey { ref env, ref namespace }
                        if env == "AXOND_KEY" && namespace == "platform"
                ),
                "{err}"
            );
            // The message names the reference, never a value.
            assert!(err.to_string().contains("AXOND_KEY"), "{err}");
        }
    }

    #[test]
    fn a_declared_gateway_verifier_without_its_env_var_refuses_to_resolve() {
        let config = config_with(
            r#"
[[gateway_key]]
env = "AXOND_KEY"
namespace = "platform"

[gateway_token]
audience = "test"

[[gateway_verifier]]
kid = "test"
alg = "HS256"
env = "JWT_SECRET"
namespaces = ["platform"]
max_ttl = "15m"
"#,
        );
        let Err(err) = ConfigSnapshot::build(
            config,
            &HashMap::from([("AXOND_KEY".to_owned(), "static-secret".to_owned())]),
            0,
        ) else {
            panic!("the verifier cannot be resolved");
        };
        assert!(
            matches!(
                err,
                SnapshotError::TokenVerifier(
                    crate::principals::TokenVerifierBuildError::MissingKey { ref kid, ref env }
                ) if kid == "test" && env == "JWT_SECRET"
            ),
            "{err}"
        );
    }

    #[test]
    fn minting_signing_material_fails_closed_without_disclosing_material() {
        let config = Config::from_toml_str(
            r#"
[[namespace]]
id = "platform"
default = true

[[gateway_key]]
env = "AXOND_KEY"
namespace = "platform"
can_mint = true

[gateway_token]
audience = "test"

[[gateway_verifier]]
kid = "test"
alg = "HS256"
env = "JWT_SECRET"
namespaces = ["platform"]
max_ttl = "15m"

[gateway_minting]
kid = "test"
env = "SIGNING_SECRET"
scope = ["chat", "models"]
"#,
        )
        .unwrap();
        let env = HashMap::from([
            ("AXOND_KEY".to_owned(), "static-secret".to_owned()),
            (
                "JWT_SECRET".to_owned(),
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            ),
            ("SIGNING_SECRET".to_owned(), "too-short".to_owned()),
        ]);
        let Err(error) = ConfigSnapshot::build(config, &env, 0) else {
            panic!("short HS256 signing material must fail");
        };
        let message = error.to_string();
        assert!(message.contains("SIGNING_SECRET"));
        assert!(!message.contains("too-short"));

        let document = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let pair = Ed25519KeyPair::from_pkcs8(document.as_ref()).unwrap();
        let config = Config::from_toml_str(
            r#"
[[namespace]]
id = "platform"
default = true

[[gateway_key]]
env = "AXOND_KEY"
namespace = "platform"
can_mint = true

[gateway_token]
audience = "test"

[[gateway_verifier]]
kid = "test"
alg = "EdDSA"
env = "VERIFYING_KEY"
namespaces = ["platform"]
max_ttl = "15m"

[gateway_minting]
kid = "test"
env = "SIGNING_KEY"
scope = ["chat", "models"]
"#,
        )
        .unwrap();
        let env = HashMap::from([
            ("AXOND_KEY".to_owned(), "static-secret".to_owned()),
            (
                "VERIFYING_KEY".to_owned(),
                STANDARD.encode(pair.public_key().as_ref()),
            ),
            ("SIGNING_KEY".to_owned(), "not-base64".to_owned()),
        ]);
        let Err(error) = ConfigSnapshot::build(config, &env, 0) else {
            panic!("invalid Ed25519 signing material must fail");
        };
        let message = error.to_string();
        assert!(message.contains("SIGNING_KEY"));
        assert!(!message.contains("not-base64"));
    }

    #[test]
    fn minting_signing_material_must_match_verifier_for_both_algorithms() {
        let hs_config = Config::from_toml_str(
            r#"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "AXOND_KEY"
namespace = "platform"
can_mint = true
[gateway_token]
audience = "test"
[[gateway_verifier]]
kid = "test"
alg = "HS256"
env = "JWT_SECRET"
namespaces = ["platform"]
max_ttl = "15m"
[gateway_minting]
kid = "test"
env = "SIGNING_SECRET"
scope = ["chat", "models"]
"#,
        )
        .unwrap();
        let hs_secret = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned();
        let mut hs_env = HashMap::from([
            ("AXOND_KEY".to_owned(), "static-secret".to_owned()),
            ("JWT_SECRET".to_owned(), hs_secret.clone()),
            ("SIGNING_SECRET".to_owned(), hs_secret.clone()),
        ]);
        assert!(ConfigSnapshot::build(hs_config.clone(), &hs_env, 0).is_ok());
        hs_env.insert(
            "SIGNING_SECRET".to_owned(),
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
        );
        let Err(error) = ConfigSnapshot::build(hs_config, &hs_env, 0) else {
            panic!("mismatched HS256 material must fail");
        };
        let message = error.to_string();
        assert!(message.contains("SIGNING_SECRET"));
        assert!(!message.contains("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"));

        let first = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let first_pair = Ed25519KeyPair::from_pkcs8(first.as_ref()).unwrap();
        let second = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let config = Config::from_toml_str(
            r#"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "AXOND_KEY"
namespace = "platform"
can_mint = true
[gateway_token]
audience = "test"
[[gateway_verifier]]
kid = "test"
alg = "EdDSA"
env = "VERIFYING_KEY"
namespaces = ["platform"]
max_ttl = "15m"
[gateway_minting]
kid = "test"
env = "SIGNING_KEY"
scope = ["chat", "models"]
"#,
        )
        .unwrap();
        let mut ed_env = HashMap::from([
            ("AXOND_KEY".to_owned(), "static-secret".to_owned()),
            (
                "VERIFYING_KEY".to_owned(),
                STANDARD.encode(first_pair.public_key().as_ref()),
            ),
            ("SIGNING_KEY".to_owned(), STANDARD.encode(first.as_ref())),
        ]);
        assert!(ConfigSnapshot::build(config.clone(), &ed_env, 0).is_ok());
        ed_env.insert("SIGNING_KEY".to_owned(), STANDARD.encode(second.as_ref()));
        let Err(error) = ConfigSnapshot::build(config, &ed_env, 0) else {
            panic!("mismatched Ed25519 material must fail");
        };
        let message = error.to_string();
        assert!(message.contains("SIGNING_KEY"));
        assert!(!message.contains(&STANDARD.encode(second.as_ref())));
    }

    #[tokio::test]
    async fn a_static_gateway_key_resolves_from_a_file_and_uses_its_path_as_subject() {
        let path = temp_file(b"static-file-secret");
        let config = config_with(&format!(
            "[[gateway_key]]\nfile = \"{path}\"\nnamespace = \"platform\"\n"
        ));
        let snapshot = ConfigSnapshot::build(config, &HashMap::new(), 0).expect("resolves");
        let principal = snapshot
            .resolve_principal(&Presented {
                credential: "static-file-secret",
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(principal.subject, path);
        std::fs::remove_file(path).unwrap();
    }

    /// Two keys holding one secret cannot both be honoured: the table is keyed
    /// by the secret, so one namespace would silently win.
    #[test]
    fn two_gateway_keys_sharing_one_secret_refuse_to_resolve() {
        let config = config_with(
            r#"
[[gateway_key]]
env = "AXOND_KEY"
namespace = "platform"

[[gateway_key]]
env = "AXOND_OTHER_KEY"
namespace = "platform"
"#,
        );
        let env = HashMap::from([
            ("AXOND_KEY".to_owned(), "shared".to_owned()),
            ("AXOND_OTHER_KEY".to_owned(), "shared".to_owned()),
        ]);
        let Err(err) = ConfigSnapshot::build(config, &env, 0) else {
            panic!("an ambiguous key table must not resolve");
        };
        assert!(
            matches!(err, SnapshotError::DuplicateGatewayKey { .. }),
            "{err}"
        );
        let message = err.to_string();
        assert!(message.contains("AXOND_KEY") && message.contains("AXOND_OTHER_KEY"));
        assert!(!message.contains("shared"), "{message}");
    }

    #[test]
    fn file_backed_gateway_key_errors_redact_material() {
        let first = temp_file(b"file-shared-secret");
        let second = temp_file(b"file-shared-secret");
        let config = config_with(&format!(
            "[[gateway_key]]\nfile = \"{first}\"\nnamespace = \"platform\"\n\n[[gateway_key]]\nfile = \"{second}\"\nnamespace = \"platform\"\n"
        ));
        let Err(error) = ConfigSnapshot::build(config, &HashMap::new(), 0) else {
            panic!("duplicate file-backed keys must be rejected");
        };
        let message = format!("{error:?} {error}");
        assert!(
            message.contains(&first) && message.contains(&second),
            "{message}"
        );
        assert!(!message.contains("file-shared-secret"), "{message}");
        std::fs::remove_file(first).unwrap();
        std::fs::remove_file(second).unwrap();

        let failed = temp_file(b"");
        let config = config_with(&format!(
            "[[gateway_key]]\nfile = \"{failed}\"\nnamespace = \"platform\"\n"
        ));
        let Err(error) = ConfigSnapshot::build(config, &HashMap::new(), 0) else {
            panic!("empty file-backed key must be rejected");
        };
        let message = format!("{error:?} {error}");
        assert!(message.contains(&failed), "{message}");
        assert!(!message.contains("file-shared-secret"), "{message}");
        std::fs::remove_file(failed).unwrap();
    }

    #[tokio::test]
    async fn a_resolved_key_is_bound_to_its_namespace_and_env_var() {
        let env = HashMap::from([("AXOND_KEY".to_owned(), "inbound-secret".to_owned())]);
        let snapshot = ConfigSnapshot::build(config_with(PLATFORM_KEY), &env, 0).expect("resolves");
        let key = snapshot
            .resolve_principal(&Presented {
                credential: "inbound-secret",
            })
            .await
            .expect("principal resolution succeeds")
            .expect("the presented secret resolves its caller");
        assert_eq!(key.namespace, "platform");
        assert_eq!(key.subject, "AXOND_KEY");
        assert_eq!(snapshot.inbound_key_count(), 1);
        assert!(
            snapshot
                .resolve_principal(&Presented {
                    credential: "wrong-secret",
                })
                .await
                .expect("principal resolution succeeds")
                .is_none()
        );
    }

    #[tokio::test]
    async fn a_projected_workload_key_resolves_from_its_durable_digest() {
        let key = crate::desired_state::fixtures::workload_key(0xd0);
        let mut config = config_with(PLATFORM_KEY);
        config.projected_principals = vec![crate::config::ProjectedPrincipal {
            namespace: "platform".to_owned(),
            subject: crate::desired_state::fixtures::principal_id(33).to_string(),
            digest: crate::desired_state::Checksum::of(key.as_bytes()),
            grant: None,
        }];
        let env = HashMap::from([("AXOND_KEY".to_owned(), "inbound-secret".to_owned())]);
        let snapshot = ConfigSnapshot::build(config, &env, 0)
            .expect("a digest-backed principal does not need secret material");
        let principal = snapshot
            .resolve_principal(&Presented { credential: &key })
            .await
            .expect("principal resolution succeeds")
            .expect("the projected workload key resolves");
        assert_eq!(principal.namespace, "platform");
        assert_eq!(
            principal.subject,
            crate::desired_state::fixtures::principal_id(33).to_string()
        );
        assert_eq!(principal.authority, PrincipalAuthority::WorkloadKey);
        assert_eq!(snapshot.inbound_key_count(), 2);
        assert!(
            snapshot
                .resolve_principal(&Presented {
                    credential: "axw1.not-a-key",
                })
                .await
                .expect("malformed workload keys fail closed")
                .is_none()
        );
    }

    /// Issuance epochs belong only to minted tokens; the static breakglass key
    /// remains resolvable when a namespace-wide epoch is configured.
    #[tokio::test]
    async fn a_static_gateway_key_ignores_token_epochs() {
        let config = config_with(&format!(
            "{PLATFORM_KEY}\n[[gateway_token_epoch]]\nnamespace = \"platform\"\nmin_iat = 9_999_999_999\n"
        ));
        let env = HashMap::from([("AXOND_KEY".to_owned(), "inbound-secret".to_owned())]);
        let snapshot = ConfigSnapshot::build(config, &env, 0).expect("resolves");
        assert_eq!(
            snapshot
                .resolve_principal(&Presented {
                    credential: "inbound-secret",
                })
                .await
                .expect("principal resolution succeeds")
                .expect("static key resolves")
                .namespace,
            "platform"
        );
    }

    /// A stateful deployment starts with a keyless bootstrap object, but that
    /// object is not admissible as a serving snapshot.
    #[test]
    fn a_stateful_bootstrap_compiles_without_an_inbound_key() {
        let env = HashMap::new();
        let stateful = Config::from_toml_str(
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
        .expect("valid stateful bootstrap");
        let snapshot =
            ConfigSnapshot::build_bootstrap(stateful, &env, 0).expect("compiles keyless bootstrap");
        assert_eq!(snapshot.inbound_key_count(), 0);
    }

    #[test]
    fn a_compiled_revision_without_a_request_addressable_principal_is_refused() {
        let env = HashMap::new();
        let stateful = Config::from_toml_str(
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
        .expect("a valid stateful bootstrap");
        let error = match ConfigSnapshot::build_compiled_with(
            stateful,
            &env,
            1,
            ResolvedSecrets::default(),
        ) {
            Ok(_) => panic!("a candidate without an inbound principal cannot serve"),
            Err(error) => error,
        };
        assert!(matches!(error, SnapshotError::NoInboundKeys));
    }

    #[test]
    fn a_static_key_cannot_shadow_the_projected_workload_shape() {
        let config = config_with(PLATFORM_KEY);
        let env = HashMap::from([("AXOND_KEY".to_owned(), "axw1.shadowed".to_owned())]);
        let error = match ConfigSnapshot::build(config, &env, 0) {
            Ok(_) => panic!("the reserved workload shape must remain unambiguous"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            SnapshotError::ReservedGatewayKeyShape { .. }
        ));
    }

    /// The keyless snapshot above is admissible only because the runtime answers
    /// inference with [`crate::ops::inference_refusal`] instead of that snapshot.
    /// This is the coupling, asserted rather than described: a mode that would
    /// serve inference from a snapshot has to have an inbound key, so a future
    /// change that stops refusing stateful inference fails here instead of
    /// authenticating callers against an empty key set.
    /// A normal candidate build never permits a keyless stateful snapshot. The
    /// explicit bootstrap constructor is the only keyless path.
    #[test]
    fn stateful_candidates_require_projected_inbound_keys() {
        // A mode that serves inference has no keyless form to begin with:
        // configuration refuses it before a snapshot is ever built.
        let error = Config::from_toml_str(
            r#"
[[namespace]]
id = "platform"
default = true
"#,
        )
        .expect_err("a keyless stateless bootstrap serves inference to nobody");
        assert!(format!("{error}").contains("gateway_key"), "{error}");

        let stateful = Config::from_toml_str(
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
        .expect("a valid stateful bootstrap");
        assert!(matches!(
            ConfigSnapshot::build(stateful, &HashMap::new(), 0),
            Err(SnapshotError::NoInboundKeys)
        ));
    }

    /// The secret is held as `SecretString`, so debugging or logging an entry
    /// renders the redaction placeholder, never the key material.
    #[test]
    fn a_resolved_key_entry_never_renders_its_secret() {
        let env = HashMap::from([("AXOND_KEY".to_owned(), "inbound-secret".to_owned())]);
        let snapshot = ConfigSnapshot::build(config_with(PLATFORM_KEY), &env, 0).expect("resolves");
        let rendered = snapshot.principals.config_first_secret_debug();
        assert!(!rendered.contains("inbound-secret"), "{rendered}");
    }
}
