//! Shared, immutable-after-boot application state.

use std::collections::HashMap;
use std::sync::Arc;

use gateway_core::{AnthropicAdapter, OpenAiCompatibleAdapter, OpenAiFlavor, ProviderAdapter};
use gateway_transport::HttpDispatcher;

use crate::config::{Config, ProviderKind};
use crate::credentials::Credentials;
use crate::quota::QuotaStore;
use crate::usage::UsageFanout;

#[derive(Clone)]
pub struct AppState(pub Arc<Inner>);

pub struct Inner {
    pub config: Config,
    pub credentials: Credentials,
    pub dispatcher: HttpDispatcher,
    pub usage: UsageFanout,
    pub quota: Box<dyn QuotaStore>,
    /// Inbound gateway-key secret → (namespace, subject). Empty ⇒ unauthenticated.
    pub inbound_keys: HashMap<String, InboundKey>,
}

#[derive(Clone)]
pub struct InboundKey {
    pub namespace: String,
    pub subject: String,
}

impl AppState {
    pub fn new(
        config: Config,
        env: &HashMap<String, String>,
        usage: UsageFanout,
        quota: Box<dyn QuotaStore>,
    ) -> Self {
        let credentials = Credentials::from_env(&config, env);
        let dispatcher = HttpDispatcher::new(reqwest::Client::new());
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
        AppState(Arc::new(Inner {
            config,
            credentials,
            dispatcher,
            usage,
            quota,
            inbound_keys,
        }))
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
