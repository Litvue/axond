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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use gateway_core::{
    AnthropicAdapter, CircuitBreaker, OpenAiCompatibleAdapter, OpenAiFlavor, ProviderAdapter,
};
use gateway_transport::HttpDispatcher;
use secrecy::{ExposeSecret, SecretString};

use crate::budget::BudgetStore;
use crate::config::{Config, ProviderKind};
use crate::credentials::{CredentialError, Credentials};
use crate::principals::{
    ConfigPrincipals, GatewayKeyEntry, Presented, PrincipalShapeError, PrincipalStoreChain,
    TokenVerifier, TokenVerifierBuildError,
};
use crate::usage::UsageFanout;

pub use crate::principals::InboundKey;

#[derive(Clone)]
pub struct AppState(pub Arc<Inner>);

pub struct Inner {
    pub dispatcher: HttpDispatcher,
    pub usage: UsageFanout,
    pub budget: Box<dyn BudgetStore>,
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
}

/// Why a config could not become a servable snapshot. Names the offending
/// reference — an env-var name and a namespace — never a secret's value.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error(transparent)]
    Credentials(#[from] CredentialError),
    #[error(
        "gateway_key for namespace `{namespace}` references env var `{env}`, which is unset or empty"
    )]
    MissingGatewayKey { namespace: String, env: String },
    #[error(
        "gateway_key env vars `{env}` (namespace `{namespace}`) and `{other_env}` (namespace `{other_namespace}`) hold the same secret, so the caller's namespace would be ambiguous"
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
        for k in &config.gateway_key {
            let secret = env.get(&k.env).filter(|v| !v.is_empty()).ok_or_else(|| {
                SnapshotError::MissingGatewayKey {
                    namespace: k.namespace.clone(),
                    env: k.env.clone(),
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
                    env: k.env.clone(),
                    namespace: k.namespace.clone(),
                    other_env: other.caller.subject.clone(),
                    other_namespace: other.caller.namespace.clone(),
                });
            }
            inbound_keys.push(GatewayKeyEntry {
                secret: SecretString::from(secret.clone()),
                caller: InboundKey {
                    namespace: k.namespace.clone(),
                    subject: k.env.clone(),
                    signer_kid: None,
                },
            });
        }
        if inbound_keys.is_empty() {
            return Err(SnapshotError::NoInboundKeys);
        }
        let inbound_keys: Arc<[GatewayKeyEntry]> = inbound_keys.into();
        let config_principals = ConfigPrincipals::new(Arc::clone(&inbound_keys));
        let stores = TokenVerifier::build(&config, env)?
            .into_iter()
            .map(|verifier| Box::new(verifier) as Box<dyn crate::principals::PrincipalStore>)
            .collect();
        let principals = PrincipalStoreChain::new(stores, config_principals)?;
        Ok(Self {
            config,
            credentials,
            target_circuits,
            principals,
            generation,
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
    pub fn new(
        config: Config,
        env: &HashMap<String, String>,
        usage: UsageFanout,
        budget: Box<dyn BudgetStore>,
    ) -> Result<Self, SnapshotError> {
        let snapshot = ConfigSnapshot::build(config, env, 0)?;
        Ok(AppState(Arc::new(Inner {
            dispatcher: HttpDispatcher::new(reqwest::Client::new()),
            usage,
            budget,
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
