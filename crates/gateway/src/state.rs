//! Shared, immutable-after-boot application state.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

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
    pub config: Config,
    pub credentials: Credentials,
    pub dispatcher: HttpDispatcher,
    pub usage: UsageFanout,
    pub budget: Box<dyn BudgetStore>,
    /// Per-target circuit breaker, keyed by the target's qualified model
    /// (`provider/model`). In-memory and per-replica, consistent with running
    /// stateless by default (ADR 0002); distinct from the per-credential health
    /// that lives on `Credentials` (ADR 0008).
    pub target_circuits: CircuitBreaker,
    /// Inbound gateway-key secret → (namespace, subject). Empty ⇒ unauthenticated.
    pub inbound_keys: HashMap<String, InboundKey>,
}

#[derive(Clone)]
pub struct InboundKey {
    pub namespace: String,
    pub subject: String,
}

impl AppState {
    /// Fails when a declared credential's env var is missing or empty — the
    /// credential graph is validated at boot, not at request time.
    pub fn new(
        config: Config,
        env: &HashMap<String, String>,
        usage: UsageFanout,
        budget: Box<dyn BudgetStore>,
    ) -> Result<Self, CredentialError> {
        let credentials = Credentials::from_env(&config, env)?;
        let dispatcher = HttpDispatcher::new(reqwest::Client::new());
        let target_circuits = CircuitBreaker::new(
            config.failover.failure_threshold,
            Duration::from_secs(config.failover.cooldown_seconds),
        );
        let mut inbound_keys = HashMap::new();
        for k in &config.gateway_key {
            if let Some(secret) = env.get(&k.env).filter(|v| !v.is_empty()) {
                inbound_keys.insert(
                    secret.clone(),
                    InboundKey {
                        namespace: k.namespace.clone(),
                        subject: k.env.clone(),
                    },
                );
            }
        }
        Ok(AppState(Arc::new(Inner {
            config,
            credentials,
            dispatcher,
            usage,
            budget,
            target_circuits,
            inbound_keys,
        })))
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
