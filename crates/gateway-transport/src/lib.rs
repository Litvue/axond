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
//!
//! Every call is bounded. [`TransportLimits`] carries the per-phase bounds —
//! connecting, waiting for response headers, reading a buffered body, and
//! waiting for the next chunk of an open stream — and every dispatch also takes
//! the caller's [`Deadline`], the authoritative wall-clock budget for the whole
//! failover walk. The tighter of the two governs each phase, so an in-flight
//! call cannot outlive the walk that started it. Once a stream has opened the
//! deadline stops applying: a long answer is not a stalled one, and the stream
//! is governed by the idle bound instead.

use std::pin::Pin;
use std::time::{Duration, Instant};

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

/// Per-phase bounds on one upstream call.
///
/// Wall-clock bounds are separate on purpose: connecting, waiting for headers,
/// and reading a body fail for different reasons and are tuned differently, and
/// the stream idle bound must not be confused with a total stream lifetime —
/// a long answer is legitimate, a silent socket is not. Byte bounds exist so a
/// provider (or something impersonating one) cannot make the gateway hold an
/// unbounded body in memory.
#[derive(Debug, Clone, Copy)]
pub struct TransportLimits {
    /// Bound on establishing the TCP + TLS connection.
    pub connect_timeout: Duration,
    /// Bound on waiting for the upstream's response headers (time to first
    /// byte), measured from the moment the request is dispatched.
    pub response_header_timeout: Duration,
    /// Bound on reading a whole buffered response body once headers arrived.
    pub buffered_body_timeout: Duration,
    /// Bound on waiting for the next chunk of an already-open stream.
    pub stream_idle_timeout: Duration,
    /// Largest buffered success body that will be read.
    pub max_response_bytes: u64,
    /// Largest provider error body that will be read before it is truncated.
    /// Error bodies are diagnostic, so they are bounded harder than success
    /// bodies and truncated rather than rejected.
    pub max_error_bytes: u64,
}

impl Default for TransportLimits {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_millis(5_000),
            response_header_timeout: Duration::from_millis(30_000),
            buffered_body_timeout: Duration::from_millis(30_000),
            stream_idle_timeout: Duration::from_millis(120_000),
            max_response_bytes: 32 * 1024 * 1024,
            max_error_bytes: 64 * 1024,
        }
    }
}

/// The shared client the limits describe. Only the connect bound belongs to the
/// client itself; the rest are applied per call, where the caller's deadline can
/// tighten them.
pub fn build_client(limits: &TransportLimits) -> reqwest::Result<reqwest::Client> {
    reqwest::Client::builder()
        .connect_timeout(limits.connect_timeout)
        .redirect(reqwest::redirect::Policy::none())
        .build()
}

/// The authoritative wall-clock budget for the work a call belongs to — the
/// failover walk's overall deadline. [`Deadline::unbounded`] is for callers that
/// have none (an already-open stream's idle waits, and tests).
#[derive(Debug, Clone, Copy, Default)]
pub struct Deadline(Option<Instant>);

impl Deadline {
    pub fn at(instant: Instant) -> Self {
        Self(Some(instant))
    }

    pub const fn unbounded() -> Self {
        Self(None)
    }

    /// Time left, or `None` when the deadline is unbounded. A spent deadline
    /// reports `Duration::ZERO` rather than underflowing.
    pub fn remaining(self) -> Option<Duration> {
        self.0
            .map(|at| at.saturating_duration_since(Instant::now()))
    }

    pub fn is_expired(self) -> bool {
        self.remaining() == Some(Duration::ZERO)
    }
}

/// Which phase of a call ran out of time. Distinct values because they mean
/// different things to an operator: a connect timeout is egress or DNS, no
/// headers is an overloaded provider, a stalled stream is a half-dead socket.
///
/// The phase is reported independently of [`TimeoutBound`], which says *whose*
/// bound elapsed. A stalled phase is always named, even when the walk's
/// remaining budget rather than the phase's own bound is what cut it off —
/// otherwise a target that goes silent late in a walk would be indistinguishable
/// from the gateway giving up before dispatching anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeoutKind {
    Connect,
    ResponseHeaders,
    BufferedBody,
    StreamIdle,
    /// No phase ran: the walk's budget was already spent, so nothing was
    /// dispatched. This is the gateway's own bound and says nothing about a
    /// target.
    Overall,
}

impl TimeoutKind {
    /// Stable, low-cardinality label for telemetry.
    pub fn label(self) -> &'static str {
        match self {
            Self::Connect => "connect",
            Self::ResponseHeaders => "response_headers",
            Self::BufferedBody => "buffered_body",
            Self::StreamIdle => "stream_idle",
            Self::Overall => "overall",
        }
    }

    fn phase(self) -> &'static str {
        match self {
            Self::Connect => "connecting to the provider",
            Self::ResponseHeaders => "waiting for provider response headers",
            Self::BufferedBody => "reading the provider response body",
            Self::StreamIdle => "waiting for the next provider stream chunk",
            Self::Overall => "the request's failover budget",
        }
    }
}

/// Whose bound produced the budget a phase waited out. Orthogonal to
/// [`TimeoutKind`]: the phase says *what* was waiting, this says *why* the wait
/// ended, and only the latter decides whether the gateway is looking at its own
/// exhausted budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeoutBound {
    /// The phase's own configured `[transport]` bound.
    Phase,
    /// What was left of the walk's overall deadline, which was tighter than the
    /// phase bound.
    WalkBudget,
}

impl TimeoutBound {
    /// Stable, low-cardinality label for telemetry.
    pub fn label(self) -> &'static str {
        match self {
            Self::Phase => "phase",
            Self::WalkBudget => "walk_budget",
        }
    }
}

fn timeout_message(kind: TimeoutKind, bound: TimeoutBound, budget_ms: u64) -> String {
    match (kind, bound) {
        (TimeoutKind::Overall, _) => {
            "the request's failover budget was spent before this attempt was dispatched".to_owned()
        }
        (kind, TimeoutBound::WalkBudget) => format!(
            "{} exceeded the {budget_ms}ms left of the request's failover budget",
            kind.phase()
        ),
        (kind, TimeoutBound::Phase) => {
            format!("{} exceeded its {budget_ms}ms bound", kind.phase())
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error(transparent)]
    Provider(#[from] ProviderError),
    #[error("transport: {0}")]
    Http(String),
    /// A bound was exceeded. The message names the phase, whose bound it was,
    /// and the budget — never the upstream URL.
    #[error("transport: {}", timeout_message(*kind, *bound, *budget_ms))]
    Timeout {
        kind: TimeoutKind,
        bound: TimeoutBound,
        budget_ms: u64,
    },
    #[error("transport: provider response body exceeded its {limit_bytes}-byte bound")]
    BodyTooLarge { limit_bytes: u64 },
}

impl TransportError {
    /// The phase that ran out of time, when this failure is a timeout.
    pub fn timeout_kind(&self) -> Option<TimeoutKind> {
        match self {
            Self::Timeout { kind, .. } => Some(*kind),
            _ => None,
        }
    }

    /// Whose bound ended that phase, when this failure is a timeout.
    pub fn timeout_bound(&self) -> Option<TimeoutBound> {
        match self {
            Self::Timeout { bound, .. } => Some(*bound),
            _ => None,
        }
    }
}

/// A transport failure, described without the upstream URL's credential-bearing
/// parts. `reqwest` renders the whole URL into its message, and this message
/// reaches logs, spans, and the caller's error body — so a query string or
/// userinfo an operator put in a provider's `base_url` would travel with it.
fn transport_failure(e: &reqwest::Error) -> TransportError {
    TransportError::Http(redact_url(e.to_string(), e.url()))
}

fn redact_url(message: String, url: Option<&reqwest::Url>) -> String {
    let Some(url) = url else {
        return message;
    };
    let mut redacted = url.clone();
    redacted.set_query(None);
    redacted.set_fragment(None);
    let _ = redacted.set_password(None);
    let _ = redacted.set_username("");
    message.replace(url.as_str(), redacted.as_str())
}

/// Classify a `reqwest` failure. A timeout raised by the client itself is the
/// connect bound (the only one the client owns); everything else is described
/// without the URL's credential-bearing parts.
fn classify(e: &reqwest::Error, limits: &TransportLimits) -> TransportError {
    if e.is_timeout() {
        return TransportError::Timeout {
            kind: TimeoutKind::Connect,
            bound: TimeoutBound::Phase,
            budget_ms: limits.connect_timeout.as_millis() as u64,
        };
    }
    transport_failure(e)
}

/// A pooled HTTP client that drives a `gateway-core` adapter against an
/// upstream. Construct once and share (`Clone` is cheap — `reqwest::Client`
/// is an `Arc` internally).
#[derive(Clone)]
pub struct HttpDispatcher {
    client: reqwest::Client,
    limits: TransportLimits,
}

impl HttpDispatcher {
    /// A dispatcher on the default bounds. Prefer [`Self::with_limits`], which
    /// takes the operator's configured ones.
    pub fn new(client: reqwest::Client) -> Self {
        Self::with_limits(client, TransportLimits::default())
    }

    pub fn with_limits(client: reqwest::Client, limits: TransportLimits) -> Self {
        Self { client, limits }
    }

    pub fn limits(&self) -> &TransportLimits {
        &self.limits
    }

    /// The budget for one phase: its own bound, or what is left of the caller's
    /// overall deadline when that is tighter. A spent deadline is a failure
    /// before any socket work, since dispatching could only overrun it — the one
    /// case where no phase ever waited and [`TimeoutKind::Overall`] is the whole
    /// story.
    fn budget(
        &self,
        own: Duration,
        deadline: Deadline,
    ) -> Result<(Duration, TimeoutBound), TransportError> {
        match deadline.remaining() {
            Some(remaining) if remaining.is_zero() => Err(TransportError::Timeout {
                kind: TimeoutKind::Overall,
                bound: TimeoutBound::WalkBudget,
                budget_ms: 0,
            }),
            Some(remaining) if remaining < own => Ok((remaining, TimeoutBound::WalkBudget)),
            _ => Ok((own, TimeoutBound::Phase)),
        }
    }

    /// Dispatch and wait for response headers under the tighter of the header
    /// bound and the caller's deadline.
    async fn send_bounded(
        &self,
        req: reqwest::RequestBuilder,
        deadline: Deadline,
    ) -> Result<reqwest::Response, TransportError> {
        let (budget, bound) = self.budget(self.limits.response_header_timeout, deadline)?;
        match tokio::time::timeout(budget, req.send()).await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(e)) => Err(classify(&e, &self.limits)),
            Err(_) => Err(TransportError::Timeout {
                kind: TimeoutKind::ResponseHeaders,
                bound,
                budget_ms: budget.as_millis() as u64,
            }),
        }
    }

    /// Read a whole buffered body under the tighter of the body bound and the
    /// caller's deadline, refusing one that exceeds the byte bound rather than
    /// buffering it.
    async fn buffered_body(
        &self,
        resp: reqwest::Response,
        deadline: Deadline,
    ) -> Result<String, TransportError> {
        let (budget, bound) = self.budget(self.limits.buffered_body_timeout, deadline)?;
        let limit = self.limits.max_response_bytes;
        match tokio::time::timeout(budget, read_bounded(resp, limit)).await {
            Ok(result) => result,
            Err(_) => Err(TransportError::Timeout {
                kind: TimeoutKind::BufferedBody,
                bound,
                budget_ms: budget.as_millis() as u64,
            }),
        }
    }

    /// Read as much of a provider error body as the bounds allow. Best-effort by
    /// design: the status is already known, so a slow or oversized error body
    /// yields a truncated (or empty) message rather than replacing the
    /// provider's own failure with a transport one.
    async fn error_body(&self, resp: reqwest::Response, deadline: Deadline) -> String {
        let budget = deadline
            .remaining()
            .unwrap_or(self.limits.buffered_body_timeout)
            .min(self.limits.buffered_body_timeout);
        tokio::time::timeout(budget, read_truncated(resp, self.limits.max_error_bytes))
            .await
            .unwrap_or_default()
    }

    /// The upstream byte stream, with every wait for a chunk bounded by the idle
    /// bound. The caller's overall deadline deliberately does not apply: the
    /// stream is open and being useful, and cutting a long answer off at the
    /// failover budget would truncate a working response.
    fn idle_bounded_stream(&self, resp: reqwest::Response) -> ByteStream {
        let idle = self.limits.stream_idle_timeout;
        let limits = self.limits;
        let upstream = resp
            .bytes_stream()
            .map(move |chunk| chunk.map_err(|e| classify(&e, &limits)));
        Box::pin(futures::stream::unfold(
            Some(Box::pin(upstream)),
            move |state| async move {
                let mut upstream = state?;
                match tokio::time::timeout(idle, upstream.next()).await {
                    // Dropping the upstream stream here is what cancels the
                    // stalled request instead of leaking the connection.
                    Err(_) => Some((
                        Err(TransportError::Timeout {
                            kind: TimeoutKind::StreamIdle,
                            bound: TimeoutBound::Phase,
                            budget_ms: idle.as_millis() as u64,
                        }),
                        None,
                    )),
                    Ok(None) => None,
                    Ok(Some(item)) => {
                        let failed = item.is_err();
                        Some((item, (!failed).then_some(upstream)))
                    }
                }
            },
        ))
    }

    /// Non-streaming dispatch: encode via the adapter, POST, decode the
    /// response body back into a [`ProviderResponse`] with normalized usage.
    pub async fn dispatch(
        &self,
        adapter: &dyn ProviderAdapter,
        upstream: &Upstream,
        surface: Surface,
        request: ProviderRequest,
        deadline: Deadline,
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

        let resp = self
            .send_bounded(req.headers(trace_context_headers()), deadline)
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let text = self.error_body(resp, deadline).await;
            return Err(
                ProviderError::from_upstream(adapter.name(), status.as_u16(), &text).into(),
            );
        }
        let text = self.buffered_body(resp, deadline).await?;

        let json: serde_json::Value = serde_json::from_str(&text)
            .map_err(|e| TransportError::Http(format!("decode upstream body: {e}")))?;
        Ok(adapter.decode_response(surface, json)?)
    }

    /// `GET` a JSON body from a provider path (model listing). Auth is the
    /// same as native POST; there is no request body.
    pub async fn get_json(
        &self,
        provider: &str,
        upstream: &Upstream,
        path: &str,
        headers: &[(&'static str, String)],
        deadline: Deadline,
    ) -> Result<serde_json::Value, TransportError> {
        let url = format!("{}{}", upstream.base_url.trim_end_matches('/'), path);
        let mut req = self.client.get(url);
        req = match &upstream.auth {
            AuthScheme::Bearer => req.bearer_auth(upstream.api_key.expose_secret()),
            AuthScheme::Header(name) => req.header(*name, upstream.api_key.expose_secret()),
        };
        for (name, value) in headers {
            req = req.header(*name, value);
        }
        let resp = self
            .send_bounded(req.headers(trace_context_headers()), deadline)
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let text = self.error_body(resp, deadline).await;
            return Err(ProviderError::from_upstream(provider, status.as_u16(), &text).into());
        }
        let text = self.buffered_body(resp, deadline).await?;
        serde_json::from_str(&text)
            .map_err(|e| TransportError::Http(format!("decode upstream body: {e}")))
    }

    /// Native dispatch: POST an already-shaped body to the provider's own path
    /// and hand back its response body unchanged. Nothing is encoded or decoded,
    /// so a caller speaking the target's wire gets exactly what the provider
    /// said — the fidelity a translated round trip cannot promise.
    pub async fn send(
        &self,
        upstream: &Upstream,
        call: &NativeCall,
        deadline: Deadline,
    ) -> Result<serde_json::Value, TransportError> {
        let resp = self
            .send_bounded(self.native_request(upstream, call, false), deadline)
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let text = self.error_body(resp, deadline).await;
            return Err(ProviderError::from_upstream(call.provider, status.as_u16(), &text).into());
        }
        let text = self.buffered_body(resp, deadline).await?;
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
        deadline: Deadline,
    ) -> Result<ByteStream, TransportError> {
        let resp = self
            .send_bounded(self.native_request(upstream, call, true), deadline)
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let text = self.error_body(resp, deadline).await;
            return Err(ProviderError::from_upstream(call.provider, status.as_u16(), &text).into());
        }
        Ok(self.idle_bounded_stream(resp))
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
        deadline: Deadline,
    ) -> Result<ByteStream, TransportError> {
        let mut body = adapter.encode_request(surface, request)?;
        if let Some(object) = body.as_object_mut() {
            object.insert("stream".to_owned(), serde_json::Value::Bool(true));
        }
        let url = format!(
            "{}/chat/completions",
            upstream.base_url.trim_end_matches('/')
        );

        let mut req = self
            .client
            .post(url)
            .headers(trace_context_headers())
            .json(&body);
        req = match &upstream.auth {
            AuthScheme::Bearer => req.bearer_auth(upstream.api_key.expose_secret()),
            AuthScheme::Header(name) => req.header(*name, upstream.api_key.expose_secret()),
        };

        let resp = self.send_bounded(req, deadline).await?;
        let status = resp.status();
        if !status.is_success() {
            let text = self.error_body(resp, deadline).await;
            return Err(
                ProviderError::from_upstream(adapter.name(), status.as_u16(), &text).into(),
            );
        }

        Ok(self.idle_bounded_stream(resp))
    }
}

/// Read a whole body, refusing one that would exceed `limit`. The bound is
/// checked before each chunk is appended, so an oversized body is never fully
/// buffered to discover its size.
async fn read_bounded(mut resp: reqwest::Response, limit: u64) -> Result<String, TransportError> {
    let mut body: Vec<u8> = Vec::new();
    while let Some(chunk) = resp.chunk().await.map_err(|e| transport_failure(&e))? {
        if body.len() as u64 + chunk.len() as u64 > limit {
            return Err(TransportError::BodyTooLarge { limit_bytes: limit });
        }
        body.extend_from_slice(&chunk);
    }
    Ok(String::from_utf8_lossy(&body).into_owned())
}

/// Read up to `limit` bytes and stop, for a body that is diagnostic rather than
/// the answer.
async fn read_truncated(mut resp: reqwest::Response, limit: u64) -> String {
    let limit = limit as usize;
    let mut body: Vec<u8> = Vec::new();
    while body.len() < limit {
        match resp.chunk().await {
            Ok(Some(chunk)) => {
                let room = limit - body.len();
                body.extend_from_slice(&chunk[..chunk.len().min(room)]);
            }
            Ok(None) | Err(_) => break,
        }
    }
    String::from_utf8_lossy(&body).into_owned()
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

    /// A provider `base_url` is operator-supplied, so it may carry a secret in
    /// its query or userinfo. The described failure keeps the host and path,
    /// which is what makes it diagnosable, and drops the rest.
    ///
    /// Driven through a real `reqwest` failure rather than a hand-built string:
    /// the redaction works by matching how `reqwest` renders the URL, so a
    /// rendering change upstream has to fail here rather than silently turn the
    /// redaction into a no-op.
    #[tokio::test]
    async fn a_described_failure_keeps_the_endpoint_and_drops_its_secrets() {
        // Port 1 refuses immediately: the transport-error path without a
        // timeout or a network dependency.
        let error = reqwest::Client::new()
            .post("http://127.0.0.1:1/v1/chat/completions?api-key=secret#frag")
            .send()
            .await
            .expect_err("an unreachable port cannot answer");
        let TransportError::Http(message) = transport_failure(&error) else {
            panic!("a reqwest failure is a transport failure");
        };

        assert!(
            message.contains("127.0.0.1:1/v1/chat/completions"),
            "{message}"
        );
        for leaked in ["secret", "api-key", "frag"] {
            assert!(!message.contains(leaked), "{message} leaked `{leaked}`");
        }
    }

    /// The overall deadline outranks the phase bounds, and a spent one is a
    /// failure before any socket work: dispatching could only overrun it.
    #[test]
    fn the_tighter_of_the_phase_bound_and_the_deadline_governs() {
        let limits = TransportLimits {
            response_header_timeout: Duration::from_millis(5_000),
            ..TransportLimits::default()
        };
        let dispatcher = HttpDispatcher::with_limits(reqwest::Client::new(), limits);
        let phase = limits.response_header_timeout;

        let (budget, bound) = dispatcher
            .budget(phase, Deadline::unbounded())
            .expect("an unbounded deadline cannot be spent");
        assert_eq!((budget, bound), (phase, TimeoutBound::Phase));

        let (budget, bound) = dispatcher
            .budget(
                phase,
                Deadline::at(Instant::now() + Duration::from_millis(50)),
            )
            .expect("time is left");
        assert_eq!(bound, TimeoutBound::WalkBudget);
        assert!(budget <= Duration::from_millis(50), "{budget:?}");

        let (budget, bound) = dispatcher
            .budget(
                phase,
                Deadline::at(Instant::now() + Duration::from_secs(60)),
            )
            .expect("time is left");
        assert_eq!((budget, bound), (phase, TimeoutBound::Phase));

        let spent = Deadline::at(Instant::now() - Duration::from_millis(1));
        assert!(spent.is_expired());
        let error = dispatcher
            .budget(phase, spent)
            .expect_err("a spent deadline cannot dispatch");
        assert_eq!(error.timeout_kind(), Some(TimeoutKind::Overall));
    }

    /// The phase that stalled is named even when the walk's remaining budget is
    /// what cut it off. Only the two together say whether a target went silent
    /// or the gateway ran out of budget, and the caller of
    /// [`TransportError::timeout_kind`] decides target health on that basis.
    #[test]
    fn a_walk_bounded_wait_still_names_the_phase_that_stalled() {
        let error = TransportError::Timeout {
            kind: TimeoutKind::ResponseHeaders,
            bound: TimeoutBound::WalkBudget,
            budget_ms: 40,
        };

        assert_eq!(error.timeout_kind(), Some(TimeoutKind::ResponseHeaders));
        assert_eq!(error.timeout_bound(), Some(TimeoutBound::WalkBudget));
        let message = error.to_string();
        assert!(message.contains("response headers"), "{message}");
        assert!(message.contains("failover budget"), "{message}");

        // Nothing was dispatched, so no phase can be named.
        let unattempted = TransportError::Timeout {
            kind: TimeoutKind::Overall,
            bound: TimeoutBound::WalkBudget,
            budget_ms: 0,
        };
        assert_eq!(unattempted.timeout_kind(), Some(TimeoutKind::Overall));
        assert!(
            unattempted.to_string().contains("before this attempt"),
            "{unattempted}"
        );
    }

    /// A timeout is the gateway's own verdict, so its message names the phase
    /// and the budget — and nothing about the endpoint it was waiting on.
    #[test]
    fn a_timeout_message_names_its_phase_and_no_endpoint() {
        for (kind, phrase) in [
            (TimeoutKind::Connect, "connecting"),
            (TimeoutKind::ResponseHeaders, "response headers"),
            (TimeoutKind::BufferedBody, "response body"),
            (TimeoutKind::StreamIdle, "stream chunk"),
        ] {
            let message = TransportError::Timeout {
                kind,
                bound: TimeoutBound::Phase,
                budget_ms: 250,
            }
            .to_string();
            assert!(message.contains(phrase), "{message}");
            assert!(message.contains("250ms"), "{message}");
            assert!(!message.contains("http"), "{message}");
            assert!(!kind.label().contains(' '), "{}", kind.label());
        }
    }

    #[test]
    fn userinfo_is_dropped_too() {
        let url: reqwest::Url = "https://user:pw@example.test/v1/messages"
            .parse()
            .expect("static url");
        let message = redact_url(format!("error sending request for url ({url})"), Some(&url));

        assert!(message.contains("example.test/v1/messages"), "{message}");
        assert!(!message.contains("pw"), "{message}");
    }
}
