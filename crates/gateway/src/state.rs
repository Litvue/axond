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
use gateway_transport::HttpDispatcher;
use secrecy::{ExposeSecret, SecretString};

use crate::aliases::AliasScope;
use crate::budget::BudgetStore;
use crate::config::{Config, GatewayVerifierAlgorithm, ProviderKind};
use crate::credentials::{CredentialError, Credentials};
use crate::key_material::{self, KeyMaterialError};
use crate::principals::{
    Capability, ConfigPrincipals, GatewayKeyEntry, Presented, PrincipalShapeError,
    PrincipalStoreChain, TokenVerifier, TokenVerifierBuildError,
};
#[cfg(test)]
use crate::rate_limit::NoLimit;
use crate::rate_limit::RateLimiter;
use crate::revocation::RevocationStore;
use crate::usage::UsageFanout;

pub use crate::principals::InboundKey;

#[derive(Clone)]
pub struct AppState(pub Arc<Inner>);

pub struct Inner {
    pub dispatcher: HttpDispatcher,
    pub usage: UsageFanout,
    pub budget: Box<dyn BudgetStore>,
    pub rate_limiter: Box<dyn RateLimiter>,
    pub revocation: Box<dyn RevocationStore>,
    config: ArcSwap<ConfigSnapshot>,
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
        let credentials = Credentials::from_env(&config, env)?;
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
                    signer_kid: None,
                    scope: None,
                    alias_scope: None,
                    max_request_microdollars: None,
                    jti: None,
                    can_mint: k.can_mint,
                },
            });
            gateway_key_fingerprints
                .insert(label.to_owned(), key_material::fingerprint(label, &secret));
        }
        if inbound_keys.is_empty() {
            return Err(SnapshotError::NoInboundKeys);
        }
        let inbound_keys: Arc<[GatewayKeyEntry]> = inbound_keys.into();
        let config_principals = ConfigPrincipals::new(Arc::clone(&inbound_keys));
        let verifier = TokenVerifier::build(&config, env)?;
        let gateway_verifier_fingerprints = verifier
            .as_ref()
            .map(TokenVerifier::fingerprints)
            .unwrap_or_default();
        let stores = verifier
            .into_iter()
            .map(|verifier| Box::new(verifier) as Box<dyn crate::principals::PrincipalStore>)
            .collect();
        let principals = PrincipalStoreChain::new(stores, config_principals)?;
        let gateway_minting = if let Some(minting) = config.gateway_minting.as_ref() {
            let verifier = config
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
            let algorithm = match verifier.alg {
                GatewayVerifierAlgorithm::EdDsa => crate::mint::MintAlgorithm::EdDsa,
                GatewayVerifierAlgorithm::Hs256 => crate::mint::MintAlgorithm::Hs256,
            };
            crate::mint::validate_signing_material(algorithm, &material, &minting.kid).map_err(
                |error| SnapshotError::MintingKey {
                    reference: minting.source_label().unwrap_or(&minting.kid).to_owned(),
                    error: error.to_string(),
                },
            )?;
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
                max_ttl: minting.max_ttl.unwrap_or(verifier.max_ttl),
                scope,
                aliases,
                max_request_microdollars: minting.max_request_microdollars,
            })
        } else {
            None
        };
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
        })
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

    /// How many inbound gateway keys are enforced. For the boot log and reload
    /// metrics — the count is safe to surface, the secrets are not.
    pub fn inbound_key_count(&self) -> usize {
        self.principals.config_count()
    }

    pub fn token_verifier_count(&self) -> usize {
        self.config.gateway_verifier.len()
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

    pub fn new_with_rate_limiter(
        config: Config,
        env: &HashMap<String, String>,
        usage: UsageFanout,
        budget: Box<dyn BudgetStore>,
        rate_limiter: Box<dyn RateLimiter>,
        revocation: Box<dyn RevocationStore>,
    ) -> Result<Self, SnapshotError> {
        let snapshot = ConfigSnapshot::build(config, env, 0)?;
        Ok(AppState(Arc::new(Inner {
            dispatcher: HttpDispatcher::new(reqwest::Client::new()),
            usage,
            budget,
            rate_limiter,
            revocation,
            config: ArcSwap::from_pointee(snapshot),
        })))
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
