//! HTTP transport for the Axond gateway.
//!
//! `gateway-core` is I/O-free: it encodes/decodes provider wire formats but
//! never touches the network. This crate is the missing half — it owns the
//! shared `reqwest` client, credential injection, timeouts, and (eventually)
//! retries and connection pooling — and drives a [`gateway_core::ProviderAdapter`]
//! against a real upstream.
//!
//! Scope of this scaffold: a single non-streaming OpenAI-compatible dispatch
//! path, enough to prove the core ↔ transport seam. Streaming (SSE relay),
//! per-attempt failover, and the native Anthropic transport are tracked as
//! follow-ups — the decoding logic already lives in `gateway-core`
//! (`SseDecoder`, `AnthropicAdapter`), so this crate only has to feed bytes in.

use gateway_core::{ProviderAdapter, ProviderError, ProviderRequest, ProviderResponse, Surface};
use secrecy::{ExposeSecret, SecretString};

/// Where and how to reach one upstream provider endpoint.
///
/// Credentials are held as [`SecretString`] so they cannot be logged or
/// `Debug`-printed. A resolved [`Upstream`] is produced by the credential
/// layer (namespace + provider → concrete endpoint + key); the transport
/// never reads the environment itself.
pub struct Upstream {
    pub base_url: String,
    pub api_key: SecretString,
    /// Header carrying the key. OpenAI-family: `Authorization: Bearer …`.
    /// Anthropic: `x-api-key`. Defaults to bearer.
    pub auth: AuthScheme,
}

pub enum AuthScheme {
    Bearer,
    Header(&'static str),
}

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error(transparent)]
    Provider(#[from] ProviderError),
    #[error("transport: {0}")]
    Http(String),
}

/// A pooled HTTP client that drives a `gateway-core` adapter against an
/// upstream. Construct once and share (`Clone` is cheap — `reqwest::Client`
/// is an `Arc` internally).
#[derive(Clone)]
pub struct HttpDispatcher {
    client: reqwest::Client,
}

impl HttpDispatcher {
    pub fn new(client: reqwest::Client) -> Self {
        Self { client }
    }

    /// Non-streaming dispatch: encode via the adapter, POST, decode the
    /// response body back into a [`ProviderResponse`] with normalized usage.
    pub async fn dispatch(
        &self,
        adapter: &dyn ProviderAdapter,
        upstream: &Upstream,
        surface: Surface,
        request: ProviderRequest,
    ) -> Result<ProviderResponse, TransportError> {
        let body = adapter.encode_request(surface, request)?;
        let url = format!(
            "{}/chat/completions",
            upstream.base_url.trim_end_matches('/')
        );

        let mut req = self.client.post(url).json(&body);
        req = match &upstream.auth {
            AuthScheme::Bearer => req.bearer_auth(upstream.api_key.expose_secret()),
            AuthScheme::Header(name) => req.header(*name, upstream.api_key.expose_secret()),
        };

        let resp = req
            .send()
            .await
            .map_err(|e| TransportError::Http(e.to_string()))?;
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| TransportError::Http(e.to_string()))?;

        if !status.is_success() {
            return Err(
                ProviderError::from_upstream(adapter.name(), status.as_u16(), &text).into(),
            );
        }

        let json: serde_json::Value = serde_json::from_str(&text)
            .map_err(|e| TransportError::Http(format!("decode upstream body: {e}")))?;
        Ok(adapter.decode_response(surface, json)?)
    }
}
