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

use crate::budget::BudgetStore;
use crate::config::{Config, ProviderKind};
use crate::credentials::{CredentialError, Credentials};
use crate::usage::UsageFanout;

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
    /// Inbound gateway-key secret → (namespace, subject). Never empty: inbound
    /// authentication fails closed, so a snapshot that resolved no key is not
    /// published (ADR 0013).
    pub inbound_keys: HashMap<String, InboundKey>,
    /// How many times the config has been replaced: `0` is the boot config, and
    /// each applied reload increments it. Published as a metric so an operator
    /// can tell which generation a replica is serving.
    pub generation: u64,
}

#[derive(Clone)]
pub struct InboundKey {
    pub namespace: String,
    pub subject: String,
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
        let mut inbound_keys = HashMap::new();
        for k in &config.gateway_key {
            let secret = env.get(&k.env).filter(|v| !v.is_empty()).ok_or_else(|| {
                SnapshotError::MissingGatewayKey {
                    namespace: k.namespace.clone(),
                    env: k.env.clone(),
                }
            })?;
            // Keyed by the secret, so two keys sharing a value would resolve to
            // one namespace: ambiguous authority, and a declared key dropped.
            if let Some(other) = inbound_keys.insert(
                secret.clone(),
                InboundKey {
                    namespace: k.namespace.clone(),
                    subject: k.env.clone(),
                },
            ) {
                return Err(SnapshotError::DuplicateGatewayKey {
                    env: k.env.clone(),
                    namespace: k.namespace.clone(),
                    other_env: other.subject,
                    other_namespace: other.namespace,
                });
            }
        }
        if inbound_keys.is_empty() {
            return Err(SnapshotError::NoInboundKeys);
        }
        Ok(Self {
            config,
            credentials,
            target_circuits,
            inbound_keys,
            generation,
        })
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
    fn a_resolved_key_is_bound_to_its_namespace_and_env_var() {
        let env = HashMap::from([("AXOND_KEY".to_owned(), "inbound-secret".to_owned())]);
        let snapshot = ConfigSnapshot::build(config_with(PLATFORM_KEY), &env, 0).expect("resolves");
        let key = snapshot
            .inbound_keys
            .get("inbound-secret")
            .expect("the resolved secret is the lookup key");
        assert_eq!(key.namespace, "platform");
        assert_eq!(key.subject, "AXOND_KEY");
        assert_eq!(snapshot.inbound_keys.len(), 1);
    }
}
