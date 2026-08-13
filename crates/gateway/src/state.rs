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
use secrecy::{ExposeSecret, SecretString};

use crate::admission::{AdmissionControl, DiagnosticCredential};
use crate::aliases::AliasScope;
use crate::availability::AvailabilityIndex;
use crate::budget::BudgetStore;
use crate::config::{Config, GatewayVerifierAlgorithm, ProviderKind};
use crate::convergence::secrets::ResolvedSecrets;
use crate::convergence::{RevisionReport, RevisionStatus};
use crate::credentials::{CredentialError, Credentials};
use crate::desired_state::pricing::PricingSnapshot;
use crate::key_material::{self, KeyMaterialError};
use crate::policy::PolicyRuntime;
use crate::principals::{
    Capability, ConfigPrincipals, GatewayKeyEntry, NamespaceEpoch, Presented, PrincipalAuthority,
    PrincipalShapeError, PrincipalStoreChain, TokenVerifier, TokenVerifierBuildError,
    configured_token_epochs, resolve_token_epoch,
};
#[cfg(test)]
use crate::rate_limit::NoLimit;
use crate::rate_limit::RateLimiter;
use crate::revocation::RevocationStore;
use crate::shutdown::Lifecycle;
use crate::status::registry::CachedStatusRegistry;
use crate::usage::UsageDelivery;
#[cfg(test)]
use crate::usage::UsageFanout;

pub use crate::principals::InboundKey;

#[derive(Clone)]
pub struct AppState(pub Arc<Inner>);

pub struct Inner {
    pub dispatcher: HttpDispatcher,
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
    config: ArcSwap<ConfigSnapshot>,
}

/// What a replica reports about itself, as distinct from what it serves.
///
/// Passed in rather than built inside [`AppState::with_resources`] because
/// the two fields have no stateless implementation to default to *usefully*: a
/// stateless replica has an all-`disabled` registry and no convergence, and a
/// stateful one is handed the registry its probes publish into and the status the
/// reconciler writes.
pub struct ReplicaObservability {
    pub status: Arc<CachedStatusRegistry>,
    pub revision: Option<Arc<RevisionStatus>>,
}

impl ReplicaObservability {
    /// The stateless posture: every component `disabled`, nothing probed, no
    /// revision.
    pub fn stateless() -> Self {
        Self {
            status: Arc::new(CachedStatusRegistry::stateless()),
            revision: None,
        }
    }
}

/// The config and everything resolved from it: the credential graph, the
/// inbound-key table, and the per-target circuits. Immutable once published —
/// a reload builds a replacement rather than mutating this one.
pub struct ConfigSnapshot {
    pub config: Config,
    pub credentials: Credentials,
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
    /// [`ConfigSnapshot::build`] always produces the empty index: this slice is
    /// contract only, nothing polls a provider, and no request consults a verdict.
    #[allow(dead_code)]
    availability: Arc<AvailabilityIndex>,
    /// The approved pricing this snapshot serves under, when it was compiled from
    /// a revision that published a price book (#201).
    ///
    /// Part of the snapshot rather than a second published value, because that is
    /// what makes pricing and routing atomic: a request loads one pointer, so it
    /// cannot be routed by revision *N+1* and priced by *N*. `None` for a
    /// file-configured deployment, whose prices are the ones `[[model]]` declares.
    pricing: Option<PricingSnapshot>,
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

/// Why a config could not become a servable snapshot. Names the offending
/// reference — an env-var name or path and a namespace — never a secret's value.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error(transparent)]
    Credentials(#[from] CredentialError),
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
        let gateway_token_epochs = configured_token_epochs(&config);
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
        if inbound_keys.is_empty() && crate::ops::inference_refusal(&config).is_none() {
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
        let stores = verifier
            .into_iter()
            .map(|verifier| Box::new(verifier) as Box<dyn crate::principals::PrincipalStore>)
            .collect();
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
            availability: Arc::new(AvailabilityIndex::empty()),
            pricing: None,
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

    /// The derived availability index this snapshot carries.
    #[allow(dead_code)]
    pub fn availability(&self) -> &AvailabilityIndex {
        &self.availability
    }

    /// The index as a handle, for carrying the evidence an outgoing snapshot holds
    /// onto its replacement without cloning the records.
    #[allow(dead_code)]
    pub fn availability_handle(&self) -> Arc<AvailabilityIndex> {
        Arc::clone(&self.availability)
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
    /// [`ConfigSnapshot::build`] always yields the empty index, so a reload keeps
    /// availability only by asking for it:
    ///
    /// ```ignore
    /// let outgoing = state.snapshot();
    /// let next = ConfigSnapshot::build(config, &env, generation)?
    ///     .with_availability(outgoing.availability_handle());
    /// ```
    ///
    /// Silent inheritance is the behaviour being refused, not an oversight: evidence
    /// is derived against a particular catalogue, credential set, and set of
    /// namespaces, so a reload that changed any of those would be carrying verdicts
    /// about targets the new config may no longer declare. A reload therefore either
    /// re-derives availability or re-projects the outgoing handle because it knows
    /// nothing relevant changed — and either way the choice is visible at the call
    /// site. Until a projection slice lands, nothing constructs an index at all.
    #[must_use]
    #[allow(dead_code)]
    pub fn with_availability(mut self, availability: Arc<AvailabilityIndex>) -> Self {
        self.availability = availability;
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
        self.principals.config_count()
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
    /// which threads the one [`PolicyRuntime`] the backends read.
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

    /// The boot path: every process-level resource is already connected, usage
    /// delivery included, so a deployment that cannot reach a datastore it asked
    /// for has already failed before this is called.
    ///
    /// The policy runtime is this replica's own, for a caller whose stores were
    /// not built against one; a stateful boot builds the stores against a runtime
    /// and calls [`AppState::new_with_policy`] instead.
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
        let admission = AdmissionControl::from_config(&config.admission);
        let snapshot = ConfigSnapshot::build(config, env, 0)?;
        Ok(AppState(Arc::new(Inner {
            dispatcher: HttpDispatcher::with_limits(
                build_client(&limits).expect("the upstream HTTP client builds"),
                limits,
            ),
            usage,
            budget,
            admission,
            rate_limiter,
            revocation,
            policy,
            lifecycle: Arc::new(Lifecycle::new()),
            status: observability.status,
            revision: observability.revision,
            config: ArcSwap::from_pointee(snapshot),
        })))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::availability::{AvailabilityKey, AvailabilityRecord, ScopeRef, TargetRef};
    use crate::desired_state::{TenantId, Uuid7};
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

    /// Availability is projected onto a snapshot, not compiled into it: a built
    /// snapshot knows nothing, and attaching an index leaves every config section
    /// exactly as compilation produced it (#206).
    #[test]
    fn projecting_availability_leaves_the_config_untouched() {
        let env = HashMap::from([("AXOND_KEY".to_owned(), "secret".to_owned())]);
        let snapshot =
            ConfigSnapshot::build(config_with(PLATFORM_KEY), &env, 0).expect("the key resolves");
        assert!(
            snapshot.availability().is_empty(),
            "a compiled snapshot carries no derived evidence, and no optimistic default"
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
        assert_eq!(projected.availability().len(), 1);
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
            rebuilt.availability().is_empty(),
            "a rebuild inherits no evidence it did not ask for"
        );

        let carried = ConfigSnapshot::build(config_with(PLATFORM_KEY), &env, 1)
            .expect("the key resolves")
            .with_availability(outgoing.availability_handle());
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

    /// A stateful deployment may not declare `[[gateway_key]]` at all — its
    /// inbound principals arrive with a compiled revision — so it must compile
    /// a keyless snapshot rather than hit the fail-closed refusal stateless
    /// mode answers with; otherwise the administrative surface never binds.
    #[test]
    fn a_stateful_deployment_compiles_without_an_inbound_key() {
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
        let snapshot = ConfigSnapshot::build(stateful, &env, 0).expect("compiles keyless");
        assert_eq!(snapshot.inbound_key_count(), 0);
    }

    /// The keyless snapshot above is admissible only because the runtime answers
    /// inference with [`crate::ops::inference_refusal`] instead of that snapshot.
    /// This is the coupling, asserted rather than described: a mode that would
    /// serve inference from a snapshot has to have an inbound key, so a future
    /// change that stops refusing stateful inference fails here instead of
    /// authenticating callers against an empty key set.
    #[test]
    fn only_a_mode_whose_inference_is_refused_may_compile_without_an_inbound_key() {
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
        assert!(
            crate::ops::inference_refusal(&stateful).is_some(),
            "a keyless stateful snapshot is admissible only while inference is refused; \
             when the revision projection lands, `ConfigSnapshot::build` must require a key \
             again rather than authenticate callers against an empty key set"
        );
        assert!(ConfigSnapshot::build(stateful, &HashMap::new(), 0).is_ok());
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
