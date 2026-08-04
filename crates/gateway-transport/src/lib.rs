//! HTTP transport for the Axond gateway.
//!
//! `gateway-core` is I/O-free: it encodes/decodes provider wire formats but
//! never touches the network. This crate is the missing half — it owns the
//! shared `reqwest` client, credential injection, timeouts, and (eventually)
//! retries and connection pooling — and drives a [`gateway_core::ProviderAdapter`]
//! against a real upstream.
//!
//! Two dispatch shapes are served. Adapter dispatch ([`HttpDispatcher::dispatch`])
//! encodes through a `gateway-core` adapter, which may translate wire formats.
//! Native dispatch ([`HttpDispatcher::send`]) forwards an already-shaped body to
//! the provider's own path and returns its response untouched, for a caller that
//! already speaks the target's wire. Both have a streamed twin that hands back
//! undecoded bytes, since decoding is `gateway-core`'s job.

use std::pin::Pin;

use bytes::Bytes;
use futures::{Stream, StreamExt};
use gateway_core::{ProviderAdapter, ProviderError, ProviderRequest, ProviderResponse, Surface};
use opentelemetry::global;
use opentelemetry_http::HeaderInjector;
use secrecy::{ExposeSecret, SecretString};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Raw upstream response bytes, yielded as they arrive. Dropping the stream
/// aborts the in-flight upstream request.
pub type ByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, TransportError>> + Send>>;

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

/// A request forwarded to a provider in the provider's own wire shape.
///
/// Nothing here is translated: the body is the caller's, the path is the
/// provider's, and `headers` carries whatever the wire shape itself requires
/// (Anthropic's `anthropic-version`, for instance). Deciding those is wire
/// knowledge and belongs to the caller; the transport only sends them.
pub struct NativeCall {
    /// Provider name, used to attribute a failure to the upstream.
    pub provider: &'static str,
    /// Path appended to the provider's `base_url`, e.g. `/messages`.
    pub path: &'static str,
    pub body: serde_json::Value,
    pub headers: Vec<(&'static str, String)>,
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
            .headers(trace_context_headers())
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

    /// Native dispatch: POST an already-shaped body to the provider's own path
    /// and hand back its response body unchanged. Nothing is encoded or decoded,
    /// so a caller speaking the target's wire gets exactly what the provider
    /// said — the fidelity a translated round trip cannot promise.
    pub async fn send(
        &self,
        upstream: &Upstream,
        call: &NativeCall,
    ) -> Result<serde_json::Value, TransportError> {
        let resp = self
            .native_request(upstream, call, false)
            .send()
            .await
            .map_err(|e| TransportError::Http(e.to_string()))?;
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| TransportError::Http(e.to_string()))?;
        if !status.is_success() {
            return Err(ProviderError::from_upstream(call.provider, status.as_u16(), &text).into());
        }
        serde_json::from_str(&text)
            .map_err(|e| TransportError::Http(format!("decode upstream body: {e}")))
    }

    /// Streaming native dispatch, the streamed twin of [`Self::send`]: the
    /// undecoded upstream byte stream, so the relay can forward the provider's
    /// own events. A non-success status is reported before any bytes flow.
    pub async fn send_stream(
        &self,
        upstream: &Upstream,
        call: &NativeCall,
    ) -> Result<ByteStream, TransportError> {
        let resp = self
            .native_request(upstream, call, true)
            .send()
            .await
            .map_err(|e| TransportError::Http(e.to_string()))?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(ProviderError::from_upstream(call.provider, status.as_u16(), &text).into());
        }
        Ok(Box::pin(resp.bytes_stream().map(|chunk| {
            chunk.map_err(|e| TransportError::Http(e.to_string()))
        })))
    }

    fn native_request(
        &self,
        upstream: &Upstream,
        call: &NativeCall,
        stream: bool,
    ) -> reqwest::RequestBuilder {
        let mut body = call.body.clone();
        // Only the streamed twin asserts `stream`: a buffered native body is
        // forwarded exactly as the caller wrote it, and a route without a
        // `stream` parameter at all (embeddings) would reject one.
        if stream && let Some(object) = body.as_object_mut() {
            object.insert("stream".to_owned(), serde_json::Value::Bool(true));
        }
        let url = format!("{}{}", upstream.base_url.trim_end_matches('/'), call.path);
        let mut req = self.client.post(url).json(&body);
        req = match &upstream.auth {
            AuthScheme::Bearer => req.bearer_auth(upstream.api_key.expose_secret()),
            AuthScheme::Header(name) => req.header(*name, upstream.api_key.expose_secret()),
        };
        for (name, value) in &call.headers {
            req = req.header(*name, value);
        }
        req.headers(trace_context_headers())
    }

    /// Streaming dispatch: encode via the adapter, POST with `stream: true`,
    /// and hand back the undecoded upstream byte stream. Decoding is the
    /// caller's job (`gateway-core`'s `SseDecoder` plus the adapter's stream
    /// decoder) so the transport stays free of wire semantics.
    ///
    /// A non-success status is drained and reported as a typed provider error
    /// before any bytes reach the caller, so a failed stream never opens.
    pub async fn dispatch_stream(
        &self,
        adapter: &dyn ProviderAdapter,
        upstream: &Upstream,
        surface: Surface,
        request: ProviderRequest,
    ) -> Result<ByteStream, TransportError> {
        let mut body = adapter.encode_request(surface, request)?;
        if let Some(object) = body.as_object_mut() {
            object.insert("stream".to_owned(), serde_json::Value::Bool(true));
        }
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
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(
                ProviderError::from_upstream(adapter.name(), status.as_u16(), &text).into(),
            );
        }

        Ok(Box::pin(resp.bytes_stream().map(|chunk| {
            chunk.map_err(|e| TransportError::Http(e.to_string()))
        })))
    }
}

/// Inject the active span's W3C context (`traceparent`/`tracestate`) into the
/// upstream request so the caller's trace continues past the gateway. With no
/// tracer installed the global propagator is a no-op and no headers are added.
fn trace_context_headers() -> reqwest::header::HeaderMap {
    let context = tracing::Span::current().context();
    let mut headers = reqwest::header::HeaderMap::new();
    global::get_text_map_propagator(|propagator| {
        propagator.inject_context(&context, &mut HeaderInjector(&mut headers))
    });
    // The W3C propagator emits `tracestate` unconditionally; an empty one says
    // nothing, so it is not worth a header.
    let empty: Vec<_> = headers
        .iter()
        .filter(|(_, value)| value.is_empty())
        .map(|(name, _)| name.clone())
        .collect();
    for name in empty {
        headers.remove(name);
    }
    headers
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_propagator_means_no_outbound_headers() {
        assert!(trace_context_headers().is_empty());
    }
}
