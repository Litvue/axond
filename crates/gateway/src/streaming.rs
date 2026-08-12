//! Server-sent-events relay for streamed completions.
//!
//! The relay decodes the upstream stream with `gateway-core` (`SseDecoder` +
//! the provider's `ProviderStreamDecoder`) and re-emits OpenAI-shaped chunks,
//! so a target reaches the caller in the OpenAI chunk shape whichever wire it
//! spoke upstream. On a native route the same relay forwards the provider's
//! bytes untouched and decodes only to observe usage ([`Framing`]). Bytes are
//! forwarded event-by-event as they arrive: nothing buffers a whole response,
//! and the outbound body inherits the client's backpressure because axum only
//! polls it as the socket drains.
//!
//! Accounting is attached to the body, not to the handler: a client that hangs
//! up mid-stream drops the body, which drops the upstream response (cancelling
//! the in-flight request) and commits the spend accrued so far. See ADR 0005.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::Response;
use bytes::Bytes;
use futures::StreamExt;
use futures::future::BoxFuture;
use gateway_core::{
    ModelPrice, ModelUsage, ProviderStreamDecoder, ProviderStreamEvent, SseDecoder,
};
use gateway_transport::{ByteStream, TransportError};
use opentelemetry::Context;
use serde_json::{Value, json};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::admission::AdmissionPermit;
use crate::budget::{BudgetKey, Reservation};
use crate::credentials::{CredentialLease, CredentialSource};
use crate::rate_limit::RateLimitPermit;
use crate::routes::next_request_id;
use crate::state::AppState;
use crate::telemetry;
use crate::usage::{Status, UsageRecord};

/// Everything the relay needs to attribute a streamed request once it ends.
pub struct StreamContext {
    pub namespace: String,
    pub subject: String,
    pub signer_kid: Option<String>,
    pub alias: String,
    pub target_provider: String,
    pub target_model: String,
    pub source: CredentialSource,
    pub credential_id: String,
    /// Captured in the handler while the server span is live; settlement may run
    /// in a detached task where the span context is no longer current.
    pub trace_id: Option<String>,
    pub price: ModelPrice,
    pub budget_key: BudgetKey,
    /// The hold this request was admitted under, settled once when the stream
    /// ends however it ends.
    pub reservation: Reservation,
    pub rate_limit_permit: Option<RateLimitPermit>,
    /// The admission capacity the request was let in under. An open stream holds
    /// it for as long as the relay lives, so completion, cancellation, and the
    /// duration bound all return it through the accounting's own drop.
    pub admission_permit: Option<AdmissionPermit>,
    /// Input tokens estimated from the request body when the hold was taken.
    /// A stream that ends before the provider reports usage still consumed its
    /// prompt, so this is what the partial charge is priced from.
    pub estimated_input_tokens: u64,
    /// Upstream target attempts made before this stream opened (or the walk
    /// gave up), so the settled record matches the buffered path's attribution.
    pub attempts: u32,
}

pub struct OpenedStream {
    pub bytes: ByteStream,
    pub decoder: Box<dyn ProviderStreamDecoder>,
}

type RotationOpener = Arc<
    dyn Fn(CredentialLease, u32, usize) -> BoxFuture<'static, Result<OpenedStream, TransportError>>
        + Send
        + Sync,
>;

pub struct RotationHandle {
    remaining: VecDeque<(CredentialLease, usize)>,
    serving: CredentialLease,
    opener: RotationOpener,
    deadline: Option<Instant>,
    record_failure: Arc<dyn Fn(&CredentialLease) + Send + Sync>,
    record_success: Arc<dyn Fn(&CredentialLease) + Send + Sync>,
}

fn is_stream_rate_limited(err: &TransportError) -> bool {
    matches!(err, TransportError::Provider(error) if error.is_credential_rate_limited())
}

impl RotationHandle {
    #[cfg(test)]
    pub fn new(
        leases: Vec<CredentialLease>,
        serving: CredentialLease,
        first_lease_index: usize,
        opener: impl Fn(
            CredentialLease,
            u32,
            usize,
        ) -> BoxFuture<'static, Result<OpenedStream, TransportError>>
        + Send
        + Sync
        + 'static,
        record_failure: impl Fn(&CredentialLease) + Send + Sync + 'static,
        record_success: impl Fn(&CredentialLease) + Send + Sync + 'static,
    ) -> Self {
        Self::new_with_deadline(
            leases,
            serving,
            first_lease_index,
            opener,
            None,
            record_failure,
            record_success,
        )
    }

    pub fn new_with_deadline(
        leases: Vec<CredentialLease>,
        serving: CredentialLease,
        first_lease_index: usize,
        opener: impl Fn(
            CredentialLease,
            u32,
            usize,
        ) -> BoxFuture<'static, Result<OpenedStream, TransportError>>
        + Send
        + Sync
        + 'static,
        deadline: Option<Instant>,
        record_failure: impl Fn(&CredentialLease) + Send + Sync + 'static,
        record_success: impl Fn(&CredentialLease) + Send + Sync + 'static,
    ) -> Self {
        Self {
            remaining: leases
                .into_iter()
                .enumerate()
                .map(|(offset, lease)| (lease, first_lease_index + offset))
                .collect(),
            serving,
            opener: Arc::new(opener),
            deadline,
            record_failure: Arc::new(record_failure),
            record_success: Arc::new(record_success),
        }
    }

    fn record_serving_failure(&self) {
        (self.record_failure)(&self.serving);
    }

    fn record_serving_success(&self) {
        (self.record_success)(&self.serving);
    }

    async fn open_next(
        &mut self,
    ) -> Result<Option<(CredentialLease, OpenedStream)>, TransportError> {
        while let Some((lease, lease_index)) = self.remaining.pop_front() {
            // The walk's budget bounds the reopen itself, not just the decision
            // to attempt one: a rotation that hangs would otherwise outlive the
            // deadline it was checked against.
            let remaining = match self.deadline {
                Some(deadline) => {
                    let left = deadline.saturating_duration_since(Instant::now());
                    if left.is_zero() {
                        return Ok(None);
                    }
                    Some(left)
                }
                None => None,
            };
            let open = (self.opener)(lease.clone(), 0, lease_index);
            let opened = match remaining {
                Some(left) => match tokio::time::timeout(left, open).await {
                    Ok(opened) => opened,
                    Err(_) => return Ok(None),
                },
                None => open.await,
            };
            match opened {
                Ok(opened) => {
                    self.serving = lease.clone();
                    return Ok(Some((lease, opened)));
                }
                Err(err) if is_stream_rate_limited(&err) => {
                    (self.record_failure)(&lease);
                }
                // A non-rate-limit reopen error ends this credential walk,
                // matching stream-open dispatch before target failover.
                Err(err) => return Err(err),
            }
        }
        Ok(None)
    }
}

pub async fn open_stream_with_attempt_span<F>(
    ctx: &StreamContext,
    attempt_span: &tracing::Span,
    lease_id: &str,
    lease_index: usize,
    open: F,
) -> Result<ByteStream, TransportError>
where
    F: Future<Output = Result<ByteStream, TransportError>>,
{
    let lease_span = attempt_span.in_scope(|| {
        telemetry::credential_lease_span(
            lease_id,
            UsageRecord::credential_source_str(ctx.source),
            lease_index,
        )
    });
    let opened = async { open.instrument(lease_span.clone()).await }
        .instrument(attempt_span.clone())
        .await;
    telemetry::finish_credential_lease(
        &lease_span,
        match &opened {
            Ok(_) => telemetry::LEASE_SERVED,
            Err(err) if is_stream_rate_limited(err) => telemetry::LEASE_RATE_LIMITED,
            Err(_) => telemetry::LEASE_ERROR,
        },
    );
    opened
}

pub async fn open_stream_with_lease_parent<F>(
    ctx: &StreamContext,
    lease_id: &str,
    lease_index: usize,
    open: F,
    parent: Context,
) -> Result<ByteStream, TransportError>
where
    F: Future<Output = Result<ByteStream, TransportError>>,
{
    let lease_span = telemetry::credential_lease_span(
        lease_id,
        UsageRecord::credential_source_str(ctx.source),
        lease_index,
    );
    let _ = lease_span.set_parent(parent);
    let opened = open.instrument(lease_span.clone()).await;
    telemetry::finish_credential_lease(
        &lease_span,
        match &opened {
            Ok(_) => telemetry::LEASE_SERVED,
            Err(err) if is_stream_rate_limited(err) => telemetry::LEASE_RATE_LIMITED,
            Err(_) => telemetry::LEASE_ERROR,
        },
    );
    opened
}

/// Build the client-facing SSE response from an already-opened upstream stream.
///
/// Native bytes are committed as they arrive and never rotate mid-relay.
/// OpenAI-normalized streams may use the supplied handle only while no content
/// has been emitted, because a second independent completion cannot safely
/// resume a partially delivered wire.
pub fn relay_opened(
    state: AppState,
    ctx: StreamContext,
    decoder: Box<dyn ProviderStreamDecoder>,
    bytes: ByteStream,
    started: Instant,
    framing: Framing,
    rotation: Option<RotationHandle>,
) -> Response {
    // A stream's *total* lifetime, as opposed to the transport's idle bound,
    // which a trickle of keepalives resets forever.
    let limits = state.0.admission.limits();
    let deadline = limits.max_stream_duration.map(|budget| started + budget);
    let max_bytes = limits.max_stream_bytes;
    let relay = Relay {
        bytes,
        carry: Vec::new(),
        sse: Some(SseDecoder::default()),
        decoder,
        pending: VecDeque::new(),
        phase: Phase::Streaming,
        framing,
        accounting: Accounting::new(state, ctx, started),
        rotation,
        queued_downstream: false,
        deadline,
        max_bytes,
        relayed_bytes: 0,
    };

    let body = Body::from_stream(futures::stream::unfold(relay, |mut relay| async move {
        relay.next_chunk().await.map(|chunk| (chunk, relay))
    }));

    let mut response = Response::new(body);
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    response
}

/// Settle a streamed request that never opened a stream (every target failed or
/// was skipped) as a single upstream-error usage record, so a failed stream
/// still reconciles exactly one record like the buffered path.
pub fn settle_upstream_error(state: AppState, ctx: StreamContext, started: Instant) {
    let mut accounting = Accounting::new(state, ctx, started);
    accounting.settle(Status::UpstreamError);
}

/// How the relay frames what it sends the caller.
///
/// The OpenAI-compatible routes re-emit each decoded event and close with the
/// `[DONE]` sentinel an OpenAI SDK waits for. A native route relays the
/// provider's own bytes verbatim — the decoder runs only to observe usage — and
/// closes on the provider's own terminal event, because an SDK reading its
/// native wire would choke on a foreign sentinel. Responses is byte-faithful
/// like Native, but uses a Responses-shaped terminal error.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Framing {
    OpenAiSse,
    Native,
    Responses,
}

impl Framing {
    fn reemits(self) -> bool {
        self == Self::OpenAiSse
    }

    fn done(self) -> Option<Bytes> {
        match self {
            Self::OpenAiSse => Some(done_event()),
            Self::Native => None,
            Self::Responses => None,
        }
    }

    /// A stream that broke after the first byte cannot be retried, so the
    /// failure is delivered as a terminal event in the shape the caller's SDK
    /// parses.
    fn error(self, message: &str) -> Bytes {
        match self {
            Self::OpenAiSse => error_event(message),
            Self::Native => native_error_event(message),
            Self::Responses => responses_error_event(message),
        }
    }
}

enum Phase {
    Streaming,
    Failed(String),
    Finished,
    Ended,
}

struct Relay {
    bytes: ByteStream,
    /// Trailing bytes of a UTF-8 sequence split across upstream chunks.
    carry: Vec<u8>,
    /// Taken at end of stream, where `finish` reports a truncated event.
    sse: Option<SseDecoder>,
    decoder: Box<dyn ProviderStreamDecoder>,
    pending: VecDeque<Bytes>,
    phase: Phase,
    framing: Framing,
    accounting: Accounting,
    rotation: Option<RotationHandle>,
    queued_downstream: bool,
    /// When this stream must end, whatever the upstream is still willing to
    /// send (`admission.max_stream_duration_ms`).
    deadline: Option<Instant>,
    /// How many upstream bytes this stream may relay
    /// (`admission.max_stream_bytes`).
    max_bytes: Option<u64>,
    relayed_bytes: u64,
}

/// What a caller is told when the total-duration bound ends its stream. Static:
/// it names the bound that fired and nothing about the request.
const STREAM_DURATION_EXCEEDED: &str = "stream exceeded the gateway's maximum stream duration";

/// The same, for the relayed-bytes bound.
const STREAM_BYTES_EXCEEDED: &str = "stream exceeded the gateway's maximum stream size";

impl Relay {
    async fn next_chunk(&mut self) -> Option<Result<Bytes, Infallible>> {
        loop {
            if let Some(chunk) = self.pending.pop_front() {
                return Some(Ok(chunk));
            }
            match &self.phase {
                Phase::Streaming => self.poll_upstream().await,
                Phase::Failed(message) => {
                    let message = message.clone();
                    self.pending.push_back(self.framing.error(&message));
                    self.pending.extend(self.framing.done());
                    self.accounting.settle(Status::UpstreamError);
                    self.phase = Phase::Ended;
                }
                Phase::Finished => {
                    self.pending.extend(self.framing.done());
                    if let Some(rotation) = self.rotation.as_ref() {
                        rotation.record_serving_success();
                    }
                    self.accounting.settle(Status::Ok);
                    self.phase = Phase::Ended;
                }
                Phase::Ended => return None,
            }
        }
    }

    async fn poll_upstream(&mut self) {
        let next = match self.deadline {
            Some(deadline) => {
                match tokio::time::timeout_at(deadline.into(), self.bytes.next()).await {
                    Ok(next) => next,
                    Err(_) => {
                        tracing::warn!(
                            provider = %self.accounting.ctx.target_provider,
                            model = %self.accounting.ctx.target_model,
                            committed = self.queued_downstream,
                            "open stream exceeded the maximum stream duration"
                        );
                        self.phase = Phase::Failed(STREAM_DURATION_EXCEEDED.to_owned());
                        return;
                    }
                }
            }
            None => self.bytes.next().await,
        };
        match next {
            Some(Ok(chunk)) => {
                self.relayed_bytes = self.relayed_bytes.saturating_add(chunk.len() as u64);
                if self.max_bytes.is_some_and(|max| self.relayed_bytes > max) {
                    tracing::warn!(
                        provider = %self.accounting.ctx.target_provider,
                        model = %self.accounting.ctx.target_model,
                        relayed_bytes = self.relayed_bytes,
                        "open stream exceeded the maximum stream size"
                    );
                    self.phase = Phase::Failed(STREAM_BYTES_EXCEEDED.to_owned());
                    return;
                }
                // A native stream is byte-faithful: the provider's own bytes go
                // out as they arrive, and the decode below only observes usage.
                if !self.framing.reemits() {
                    self.queued_downstream = true;
                    self.pending.push_back(chunk.clone());
                }
                let text = self.decode_utf8(&chunk);
                let pushed = match self.sse.as_mut() {
                    Some(sse) => sse.push(&text),
                    None => Ok(Vec::new()),
                };
                match pushed {
                    Ok(events) => {
                        for event in events {
                            match self.decoder.decode(event) {
                                Ok(decoded) => self.emit(decoded),
                                Err(err)
                                    if err.is_credential_rate_limited()
                                        && self.framing == Framing::OpenAiSse
                                        && matches!(self.phase, Phase::Streaming)
                                        && !self.queued_downstream =>
                                {
                                    match self.rotate().await {
                                        Ok(true) => return,
                                        Ok(false) => {
                                            self.phase = Phase::Failed(err.to_string());
                                            return;
                                        }
                                        Err(open_err) => {
                                            self.phase = Phase::Failed(open_err.to_string());
                                            return;
                                        }
                                    }
                                }
                                Err(err)
                                    if err.is_credential_rate_limited()
                                        && !matches!(self.phase, Phase::Streaming) =>
                                {
                                    // A post-Done rate limit follows a successful
                                    // relay; do not penalize the serving credential.
                                    return;
                                }
                                Err(err) if err.is_credential_rate_limited() => {
                                    if let Some(rotation) = self.rotation.as_ref() {
                                        rotation.record_serving_failure();
                                    }
                                    self.phase = Phase::Failed(err.to_string());
                                    return;
                                }
                                Err(err) => {
                                    self.phase = Phase::Failed(err.to_string());
                                    return;
                                }
                            }
                        }
                    }
                    Err(err) => self.phase = Phase::Failed(err.to_string()),
                }
            }
            Some(Err(err)) => {
                // An open stream that goes silent is terminated honestly: bytes
                // may already be committed downstream, so there is nothing to
                // retry and no new completion to splice in.
                if let Some(kind) = err.timeout_kind() {
                    telemetry::metrics::record_upstream_timeout(
                        &self.accounting.ctx.target_provider,
                        &self.accounting.ctx.target_model,
                        kind.label(),
                        err.timeout_bound()
                            .map(gateway_transport::TimeoutBound::label)
                            .unwrap_or_default(),
                    );
                    tracing::warn!(
                        provider = %self.accounting.ctx.target_provider,
                        model = %self.accounting.ctx.target_model,
                        timeout = kind.label(),
                        committed = self.queued_downstream,
                        "open stream exceeded a transport bound"
                    );
                }
                self.phase = Phase::Failed(err.to_string());
            }
            None => self.finish_upstream(),
        }
    }

    /// Chunk boundaries fall wherever the socket puts them, so a multi-byte
    /// character can straddle two chunks: only the valid prefix is decoded and
    /// the remainder waits for the next chunk. Genuinely invalid bytes are
    /// replaced rather than stalling the stream.
    fn decode_utf8(&mut self, chunk: &[u8]) -> String {
        self.carry.extend_from_slice(chunk);
        match std::str::from_utf8(&self.carry) {
            Ok(_) => {
                let text = String::from_utf8_lossy(&self.carry).into_owned();
                self.carry.clear();
                text
            }
            Err(err) if err.error_len().is_none() => {
                let rest = self.carry.split_off(err.valid_up_to());
                let text = String::from_utf8_lossy(&self.carry).into_owned();
                self.carry = rest;
                text
            }
            Err(_) => {
                let text = String::from_utf8_lossy(&self.carry).into_owned();
                self.carry.clear();
                text
            }
        }
    }

    /// An upstream that ends mid-event is a truncated answer, not a complete
    /// one: `SseDecoder::finish` reports the leftover so the caller gets an
    /// error rather than a `[DONE]` it would read as success.
    fn finish_upstream(&mut self) {
        if let Some(sse) = self.sse.take()
            && let Err(err) = sse.finish()
        {
            self.phase = Phase::Failed(err.to_string());
            return;
        }
        if !self.carry.is_empty() {
            self.phase = Phase::Failed("stream ended mid-character".to_owned());
            return;
        }
        match self.decoder.finish() {
            Ok(decoded) => {
                self.emit(decoded);
                self.phase = Phase::Finished;
            }
            Err(err) => self.phase = Phase::Failed(err.to_string()),
        }
    }

    /// Frame decoded events for the client. `Done` carries the stream's
    /// authoritative usage; the `[DONE]` sentinel itself is written once, from
    /// the terminal phase, so a provider that ends the connection without one
    /// still gets a well-formed close.
    fn emit(&mut self, events: Vec<ProviderStreamEvent>) {
        for event in events {
            match event {
                ProviderStreamEvent::Data { event, data } => {
                    self.accounting.mark_first_token();
                    self.accounting.count_relayed(&data);
                    if self.framing.reemits() {
                        let rendered = data_event(event.as_deref(), &data);
                        self.queued_downstream = true;
                        self.pending.push_back(rendered);
                    }
                }
                ProviderStreamEvent::Done(usage) => {
                    self.accounting.usage = usage;
                    self.phase = Phase::Finished;
                }
            }
        }
    }

    async fn rotate(&mut self) -> Result<bool, TransportError> {
        let Some(rotation) = self.rotation.as_mut() else {
            return Ok(false);
        };
        rotation.record_serving_failure();
        self.bytes = futures::stream::empty().boxed();
        self.carry.clear();
        self.sse = Some(SseDecoder::default());
        self.decoder = match rotation.open_next().await {
            Ok(Some((lease, opened))) => {
                self.accounting.fold_attempt();
                self.accounting.ctx.credential_id = lease.id.clone();
                self.bytes = opened.bytes;
                self.sse = Some(SseDecoder::default());
                self.phase = Phase::Streaming;
                opened.decoder
            }
            Ok(None) => return Ok(false),
            Err(err) => return Err(err),
        };
        Ok(true)
    }
}

/// Generated text in one relayed chunk, across the shapes the adapters emit:
/// OpenAI chat deltas, Anthropic's content-block deltas, and OpenAI Responses
/// top-level string deltas. Anything else
/// contributes nothing rather than guessing, so an unmeasurable stream is
/// charged its prompt only.
fn relayed_text_len(data: &Value) -> usize {
    let mut chars = 0;
    if let Some(choices) = data.get("choices").and_then(Value::as_array) {
        for choice in choices {
            for pointer in ["/delta/content", "/delta/reasoning_content"] {
                chars += choice
                    .pointer(pointer)
                    .and_then(Value::as_str)
                    .map_or(0, text_chars);
            }
        }
    }
    for pointer in ["/delta/text", "/content_block/text", "/delta/partial_json"] {
        chars += data
            .pointer(pointer)
            .and_then(Value::as_str)
            .map_or(0, text_chars);
    }
    chars += data
        .get("delta")
        .and_then(Value::as_str)
        .map_or(0, text_chars);
    chars
}

/// Characters, not bytes: the tokens-per-character heuristic would otherwise
/// over-charge multi-byte scripts by the width of their encoding.
fn text_chars(text: &str) -> usize {
    text.chars().count()
}

fn data_event(name: Option<&str>, data: &Value) -> Bytes {
    let payload = serde_json::to_string(data).unwrap_or_else(|_| "{}".to_owned());
    match name {
        Some(name) => Bytes::from(format!("event: {name}\ndata: {payload}\n\n")),
        None => Bytes::from(format!("data: {payload}\n\n")),
    }
}

fn done_event() -> Bytes {
    Bytes::from_static(b"data: [DONE]\n\n")
}

fn error_event(message: &str) -> Bytes {
    let payload = json!({ "error": { "type": "upstream_stream_error", "message": message } });
    Bytes::from(format!(
        "event: error\ndata: {}\n\n",
        serde_json::to_string(&payload).unwrap_or_else(|_| "{}".to_owned())
    ))
}

/// The provider-native terminal error: an Anthropic-shaped `error` event, which
/// its SDKs raise on. When the upstream is what failed it has usually sent its
/// own `error` event already — the SDK stops at the first one, so the duplicate
/// is never read, and a transport failure that sent nothing still terminates.
fn native_error_event(message: &str) -> Bytes {
    let payload = json!({
        "type": "error",
        "error": { "type": "upstream_stream_error", "message": message }
    });
    Bytes::from(format!(
        "event: error\ndata: {}\n\n",
        serde_json::to_string(&payload).unwrap_or_else(|_| "{}".to_owned())
    ))
}

fn responses_error_event(message: &str) -> Bytes {
    let payload = json!({
        "type": "error",
        "code": "upstream_stream_error",
        "message": message,
    });
    Bytes::from(format!(
        "event: error\ndata: {}\n\n",
        serde_json::to_string(&payload).unwrap_or_else(|_| "{}".to_owned())
    ))
}

/// Terminal accounting for one streamed request: exactly one usage record and
/// exactly one budget settlement, whichever way the stream ends.
///
/// The `Drop` arm is the cancellation path — a dropped body means the client
/// went away, so the spend accrued up to that point is still charged. A stream
/// that ends before the provider reports usage is charged its *measured partial*
/// spend (ADR 0010): the prompt it consumed plus the output it actually relayed,
/// counted here as it goes. A stream that relayed nothing is charged nothing and
/// its whole hold is released.
struct Accounting {
    state: AppState,
    ctx: StreamContext,
    started: Instant,
    usage: ModelUsage,
    /// Characters of generated text relayed to the client, which is the only
    /// measure of output a stream has before the provider's usage arrives.
    relayed_chars: usize,
    carried_output_tokens: u64,
    /// Time to the first relayed token, which for a stream is the number a
    /// caller actually feels.
    ttft_ms: Option<u64>,
    settled: bool,
}

impl Accounting {
    fn new(state: AppState, ctx: StreamContext, started: Instant) -> Self {
        Self {
            state,
            ctx,
            started,
            usage: ModelUsage::default(),
            relayed_chars: 0,
            carried_output_tokens: 0,
            ttft_ms: None,
            settled: false,
        }
    }

    fn mark_first_token(&mut self) {
        self.ttft_ms
            .get_or_insert_with(|| self.started.elapsed().as_millis() as u64);
    }

    fn count_relayed(&mut self, data: &Value) {
        self.relayed_chars = self.relayed_chars.saturating_add(relayed_text_len(data));
    }

    fn fold_attempt(&mut self) {
        const CHARS_PER_TOKEN: usize = 4;
        // Today the production decoders report usage only in terminal events,
        // so a permitted pre-content rotation carries no provider usage. Keep
        // this fallback for decoders that learn usage before rotation.
        let output = if self.usage.output_tokens > 0 {
            self.usage.output_tokens
        } else {
            self.relayed_chars.div_ceil(CHARS_PER_TOKEN) as u64
        };
        self.carried_output_tokens = self.carried_output_tokens.saturating_add(output);
        self.usage = ModelUsage::default();
        self.relayed_chars = 0;
    }

    /// The usage the request is charged for. The provider's own numbers win
    /// whenever they arrived; otherwise — a cancelled or broken stream — the
    /// charge is derived from what was measurably relayed, and a stream that
    /// produced nothing is charged nothing.
    fn chargeable_usage(&self) -> ModelUsage {
        const CHARS_PER_TOKEN: usize = 4;
        if self.carried_output_tokens == 0
            && (self.usage.input_tokens > 0
                || self.usage.output_tokens > 0
                || self.usage.cache_read_tokens > 0
                || self.usage.cache_write_tokens > 0)
        {
            return self.usage;
        }
        let has_output = self.carried_output_tokens > 0
            || self.usage.output_tokens > 0
            || self.relayed_chars > 0;
        if !has_output && self.usage.input_tokens == 0 {
            return ModelUsage::default();
        }
        let mut usage = self.usage;
        usage.input_tokens = if self.usage.input_tokens > 0 {
            self.usage.input_tokens
        } else if has_output {
            self.ctx.estimated_input_tokens
        } else {
            0
        };
        usage.output_tokens = self.carried_output_tokens
            + if self.usage.output_tokens > 0 {
                self.usage.output_tokens
            } else {
                self.relayed_chars.div_ceil(CHARS_PER_TOKEN) as u64
            };
        usage
    }

    fn settle(&mut self, status: Status) {
        if self.settled {
            return;
        }
        self.settled = true;
        let state = self.state.clone();
        let usage = self.chargeable_usage();
        let latency_ms = self.started.elapsed().as_millis() as u64;
        let cost = self.ctx.price.cost_microdollars(gateway_core::Usage {
            input_tokens: usage.input_tokens,
            output_tokens: usage.output_tokens,
            reasoning_tokens: usage.reasoning_tokens,
            cache_read_tokens: usage.cache_read_tokens,
            cache_write_tokens: usage.cache_write_tokens,
        });
        let record = UsageRecord {
            schema_version: UsageRecord::SCHEMA_VERSION,
            request_id: next_request_id(),
            namespace: self.ctx.namespace.clone(),
            subject: self.ctx.subject.clone(),
            signer_kid: self.ctx.signer_kid.clone(),
            model: self.ctx.alias.clone(),
            target_provider: self.ctx.target_provider.clone(),
            target_model: self.ctx.target_model.clone(),
            credential_source: UsageRecord::credential_source_str(self.ctx.source),
            credential_id: self.ctx.credential_id.clone(),
            trace_id: self.ctx.trace_id.clone(),
            status,
            input_tokens: usage.input_tokens,
            cache_read_tokens: usage.cache_read_tokens,
            cache_write_tokens: usage.cache_write_tokens,
            output_tokens: usage.output_tokens,
            cost_microdollars: cost,
            catalog_version: 0,
            latency_ms,
            attempts: self.ctx.attempts,
        };
        telemetry::record_streamed(&record, self.ttft_ms);
        let budget_key = self.ctx.budget_key.clone();
        let reservation = self.ctx.reservation.clone();
        spawn_settlement(async move {
            state.0.budget.settle(&budget_key, &reservation, cost).await;
            state.0.usage.record(&record).await;
        });
    }
}

impl Drop for Accounting {
    fn drop(&mut self) {
        self.settle(Status::ClientCancelled);
    }
}

/// Settlements spawned and not yet finished. Tracked because shutdown has to
/// wait for them: a stream cut short at the deadline settles *after* its body is
/// dropped, and flushing the sinks before that ran would lose exactly the spend
/// the drain was protecting.
static OUTSTANDING_SETTLEMENTS: AtomicU64 = AtomicU64::new(0);
/// Bumped when a settlement finishes. A `watch` channel rather than a `Notify`
/// because a `Notify` waiter only enqueues on its first poll, so a wake-up
/// racing the count check would be lost and the shutdown wait would sleep out
/// its whole budget instead of noticing the work was already done.
static SETTLED: std::sync::LazyLock<tokio::sync::watch::Sender<u64>> =
    std::sync::LazyLock::new(|| tokio::sync::watch::Sender::new(0));

/// Settlement outlives the request body, so it runs detached. Outside a
/// runtime (process teardown) there is nothing left to settle onto.
pub(crate) fn spawn_settlement<F>(future: F)
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        OUTSTANDING_SETTLEMENTS.fetch_add(1, Ordering::AcqRel);
        handle.spawn(async move {
            future.await;
            if OUTSTANDING_SETTLEMENTS.fetch_sub(1, Ordering::AcqRel) == 1 {
                SETTLED.send_modify(|version| *version += 1);
            }
        });
    }
}

/// Wait up to `bound` for detached settlements to reach the sinks, and report
/// how many are still outstanding. Called once on the shutdown path, before the
/// sinks are flushed.
pub(crate) async fn await_settlements(bound: Duration) -> u64 {
    let _ = tokio::time::timeout(bound, async {
        // Subscribed before the count is read, so a settlement finishing in
        // between bumps a version this receiver has not seen and `changed()`
        // returns at once rather than sleeping on a wake-up that already fired.
        let mut settled = SETTLED.subscribe();
        while OUTSTANDING_SETTLEMENTS.load(Ordering::Acquire) != 0 {
            if settled.changed().await.is_err() {
                return;
            }
        }
    })
    .await;
    OUTSTANDING_SETTLEMENTS.load(Ordering::Acquire)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;

    use async_trait::async_trait;
    use axum::body::Body;
    use axum::http::{HeaderMap, Request, StatusCode};
    use axum::response::IntoResponse;
    use axum::routing::post;
    use futures::StreamExt;
    use gateway_core::{
        NativeMessagesDecoder, OpenAiCompatibleAdapter, ProviderAdapter, ProviderError, Surface,
    };
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
    use serde_json::json;
    use tower::util::ServiceExt;
    use tracing_subscriber::layer::SubscriberExt;

    use crate::budget::{Admission, BudgetStore, Denial};
    use crate::config::Config;
    use crate::rate_limit::RateLimiter;
    use crate::routes::router;
    use crate::usage::{UsageFanout, UsageSink};

    use super::*;

    #[derive(Default)]
    struct Ledger {
        records: Mutex<Vec<Value>>,
        commits: Mutex<Vec<u64>>,
    }

    impl Ledger {
        fn settlements(&self) -> Vec<u64> {
            self.commits.lock().expect("ledger").clone()
        }
    }

    struct LedgerSink(Arc<Ledger>);

    #[async_trait]
    impl UsageSink for LedgerSink {
        fn name(&self) -> &'static str {
            "ledger"
        }
        async fn record(&self, record: &UsageRecord) {
            let value = serde_json::to_value(record).expect("record serializes");
            self.0.records.lock().expect("ledger").push(value);
        }
    }

    /// Admits everything and records what each request settled for, so the
    /// charging policy can be asserted end to end.
    struct LedgerBudget(Arc<Ledger>);

    #[async_trait]
    impl BudgetStore for LedgerBudget {
        fn name(&self) -> &'static str {
            "ledger"
        }
        async fn reserve(&self, _key: &BudgetKey, estimated_microdollars: u64) -> Admission {
            Admission::Allowed(Reservation {
                id: "ledger".to_owned(),
                estimate_microdollars: estimated_microdollars,
            })
        }
        async fn settle(
            &self,
            _key: &BudgetKey,
            _reservation: &Reservation,
            actual_microdollars: u64,
        ) {
            self.0
                .commits
                .lock()
                .expect("ledger")
                .push(actual_microdollars);
        }
    }

    /// Denies everything, standing in for a budget that is exhausted or a store
    /// that is unreachable.
    struct DenyingBudget(Denial);

    #[async_trait]
    impl BudgetStore for DenyingBudget {
        fn name(&self) -> &'static str {
            "denying"
        }
        async fn reserve(&self, _key: &BudgetKey, _estimated_microdollars: u64) -> Admission {
            Admission::Denied(self.0)
        }
        async fn settle(
            &self,
            _key: &BudgetKey,
            _reservation: &Reservation,
            _actual_microdollars: u64,
        ) {
        }
    }

    /// A fake upstream that replies with a fixed SSE body.
    async fn upstream_serving(body: &'static str) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let app = axum::Router::new().route(
            "/chat/completions",
            post(move || async move {
                (
                    [(header::CONTENT_TYPE, "text/event-stream")],
                    Body::from(body),
                )
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        format!("http://{addr}")
    }

    async fn pooled_upstream() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let app = axum::Router::new().route(
            "/chat/completions",
            post(|headers: HeaderMap| async move {
                if headers
                    .get(header::AUTHORIZATION)
                    .and_then(|value| value.to_str().ok())
                    == Some("Bearer a")
                {
                    (
                        StatusCode::TOO_MANY_REQUESTS,
                        axum::Json(json!({"error": {"type": "rate_limit_exceeded"}})),
                    )
                        .into_response()
                } else {
                    (
                        [(header::CONTENT_TYPE, "text/event-stream")],
                        Body::from(OPENAI_STREAM),
                    )
                        .into_response()
                }
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        format!("http://{addr}")
    }

    fn pooled_config(base_url: &str) -> Config {
        Config::from_toml_str(&format!(
            r#"
[[namespace]]
id = "platform"
default = true

[[provider]]
id = "openai"
kind = "openai"
base_url = "{base_url}"

{GATEWAY_KEY}

[credential_pool]
failure_threshold = 1
cooldown_seconds = 60

[[credential]]
namespace = "platform"
provider = "openai"
env = "GW_TEST_KEY_A"
id = "a"

[[credential]]
namespace = "platform"
provider = "openai"
env = "GW_TEST_KEY_B"
id = "b"

[[model]]
name = "gpt-4o"
targets = [{{ provider = "openai", model = "gpt-4o", price = {{ input_microdollars_per_million = 1000000, output_microdollars_per_million = 2000000 }} }}]
"#
        ))
        .expect("config")
    }

    fn pooled_state(base_url: &str, ledger: Arc<Ledger>) -> AppState {
        AppState::new(
            pooled_config(base_url),
            &HashMap::from([
                ("GW_TEST_KEY_A".to_owned(), "a".to_owned()),
                ("GW_TEST_KEY_B".to_owned(), "b".to_owned()),
                ("GW_TEST_INBOUND_KEY".to_owned(), CALLER_SECRET.to_owned()),
            ]),
            UsageFanout::new(vec![Box::new(LedgerSink(ledger.clone()))]),
            Box::new(LedgerBudget(ledger)),
        )
        .expect("state")
    }

    fn state_for(base_url: &str, ledger: Arc<Ledger>) -> AppState {
        let sinks: Vec<Box<dyn UsageSink>> = vec![Box::new(LedgerSink(ledger.clone()))];
        AppState::new(
            single_target_config(base_url),
            &test_env(),
            UsageFanout::new(sinks),
            Box::new(LedgerBudget(ledger)),
        )
        .expect("state")
    }

    /// The same single-target gateway, with the budget store under test.
    fn state_with_budget(base_url: &str, budget: Box<dyn BudgetStore>) -> AppState {
        AppState::new(
            single_target_config(base_url),
            &test_env(),
            UsageFanout::new(Vec::new()),
            budget,
        )
        .expect("state")
    }

    /// The inbound key the test configs declare, and the secret a caller
    /// presents for it: inbound auth is always enforced (ADR 0013).
    const GATEWAY_KEY: &str = r#"
[[gateway_key]]
env = "GW_TEST_INBOUND_KEY"
namespace = "platform"
"#;
    const CALLER_SECRET: &str = "inbound-secret";

    fn test_env() -> HashMap<String, String> {
        HashMap::from([
            ("GW_TEST_OPENAI_KEY".to_owned(), "sk-test".to_owned()),
            ("GW_TEST_INBOUND_KEY".to_owned(), CALLER_SECRET.to_owned()),
        ])
    }

    /// A JSON `POST` that already carries the caller's gateway key.
    fn authorized(uri: &str) -> axum::http::request::Builder {
        Request::post(uri)
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::AUTHORIZATION, format!("Bearer {CALLER_SECRET}"))
    }

    fn single_target_config(base_url: &str) -> Config {
        Config::from_toml_str(&format!(
            r#"
[[namespace]]
id = "platform"
default = true

[[provider]]
id = "openai"
kind = "openai"
base_url = "{base_url}"

{GATEWAY_KEY}

[[credential]]
namespace = "platform"
provider = "openai"
env = "GW_TEST_OPENAI_KEY"

[[model]]
name = "gpt-4o"
targets = [{{ provider = "openai", model = "gpt-4o", price = {{ input_microdollars_per_million = 1000000, output_microdollars_per_million = 2000000, cache_read_microdollars_per_million = 1000000 }} }}]
"#
        ))
        .expect("config")
    }

    fn stream_request() -> Request<Body> {
        let body = json!({
            "model": "gpt-4o",
            "stream": true,
            "stream_options": { "include_usage": true },
            "messages": [{ "role": "user", "content": "hi" }]
        });
        authorized("/v1/chat/completions")
            .body(Body::from(serde_json::to_vec(&body).expect("body")))
            .expect("request")
    }

    fn context() -> StreamContext {
        StreamContext {
            namespace: "platform".to_owned(),
            subject: "GW_TEST_INBOUND_KEY".to_owned(),
            signer_kid: Some("test-kid".to_owned()),
            alias: "gpt-4o".to_owned(),
            target_provider: "openai".to_owned(),
            target_model: "gpt-4o".to_owned(),
            source: CredentialSource::Platform,
            credential_id: "openai-primary".to_owned(),
            trace_id: None,
            price: ModelPrice {
                input_microdollars_per_million: 1_000_000,
                output_microdollars_per_million: 2_000_000,
                reasoning_microdollars_per_million: None,
                cache_read_microdollars_per_million: None,
                cache_write_microdollars_per_million: None,
            },
            budget_key: BudgetKey {
                namespace: "platform".to_owned(),
                subject: "GW_TEST_INBOUND_KEY".to_owned(),
            },
            reservation: Reservation {
                id: "test".to_owned(),
                estimate_microdollars: 1_000,
            },
            rate_limit_permit: None,
            admission_permit: None,
            estimated_input_tokens: 8,
            attempts: 1,
        }
    }

    /// The ledger is written from a detached settlement task; poll briefly
    /// rather than racing it.
    async fn settled(ledger: &Ledger) -> Value {
        for _ in 0..100 {
            if let Some(record) = ledger.records.lock().expect("ledger").first() {
                return record.clone();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("no usage record was settled");
    }

    #[tokio::test]
    async fn streaming_usage_preserves_the_signer_kid() {
        let ledger = Arc::new(Ledger::default());
        let mut accounting = Accounting::new(
            state_for("http://127.0.0.1:1", ledger.clone()),
            context(),
            Instant::now(),
        );
        accounting.settle(Status::Ok);
        let record = settled(&ledger).await;
        assert_eq!(record["signer_kid"], "test-kid");
    }

    #[test]
    fn responses_deltas_contribute_to_partial_output_charge() {
        let first = json!({
            "type": "response.output_text.delta",
            "delta": "The capital"
        });
        let second = json!({
            "type": "response.output_text.delta",
            "delta": " is Paris."
        });
        assert_eq!(
            relayed_text_len(&first) + relayed_text_len(&second),
            "The capital is Paris.".chars().count()
        );
    }

    const OPENAI_STREAM: &str = concat!(
        "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hel\"}}]}\n\n",
        "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"lo\"}}]}\n\n",
        "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":11,\"completion_tokens\":3}}\n\n",
        "data: [DONE]\n\n",
    );

    const CACHE_ONLY_STREAM: &str = concat!(
        "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":0,\"prompt_tokens_details\":{\"cached_tokens\":5}}}\n\n",
        "data: [DONE]\n\n",
    );

    #[tokio::test]
    async fn relays_events_and_settles_one_usage_record() {
        let ledger = Arc::new(Ledger::default());
        let base_url = upstream_serving(OPENAI_STREAM).await;
        let resp = router(state_for(&base_url, ledger.clone()))
            .oneshot(stream_request())
            .await
            .expect("response");

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some("text/event-stream")
        );

        let mut body = resp.into_body().into_data_stream();
        let mut relayed = String::new();
        while let Some(chunk) = body.next().await {
            relayed.push_str(&String::from_utf8_lossy(&chunk.expect("chunk")));
        }
        assert_eq!(relayed.matches("data: ").count(), 4);
        assert!(relayed.contains("\"content\":\"hel\""));
        assert!(relayed.ends_with("data: [DONE]\n\n"));

        let record = settled(&ledger).await;
        assert_eq!(ledger.records.lock().expect("ledger").len(), 1);
        assert_eq!(record["status"], "ok");
        assert_eq!(record["input_tokens"], 11);
        assert_eq!(record["output_tokens"], 3);
        // 11 input @ 1 µ$/token + 3 output @ 2 µ$/token.
        assert_eq!(record["cost_microdollars"], 17);
        assert_eq!(ledger.settlements(), vec![17]);
    }

    #[tokio::test]
    async fn cache_only_provider_usage_is_charged_when_output_is_zero() {
        let ledger = Arc::new(Ledger::default());
        let base_url = upstream_serving(CACHE_ONLY_STREAM).await;
        let resp = router(state_for(&base_url, ledger.clone()))
            .oneshot(stream_request())
            .await
            .expect("response");

        assert_eq!(resp.status(), StatusCode::OK);
        let mut body = resp.into_body().into_data_stream();
        while let Some(chunk) = body.next().await {
            let _ = chunk.expect("chunk");
        }

        let record = settled(&ledger).await;
        assert_eq!(record["input_tokens"], 0);
        assert_eq!(record["cache_read_tokens"], 5);
        assert_eq!(record["output_tokens"], 0);
        assert_eq!(record["cost_microdollars"], 5);
        assert_eq!(ledger.settlements(), vec![5]);
    }

    /// Anthropic's own event stream, including a signed thinking block and the
    /// usage split across `message_start` and `message_delta`.
    const NATIVE_STREAM: &str = concat!(
        "event: message_start\n",
        "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"usage\":{\"input_tokens\":11,\"cache_read_input_tokens\":2}}}\n\n",
        "event: content_block_start\n",
        "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"thinking\",\"thinking\":\"\",\"signature\":\"sig-1\"}}\n\n",
        "event: content_block_delta\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n",
        "event: message_delta\n",
        "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":3}}\n\n",
        "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
    );

    /// A native stream reaches the caller exactly as the provider sent it — same
    /// event names, same payloads, no OpenAI `[DONE]` an Anthropic SDK would not
    /// expect — while the gateway still books one usage record from the two
    /// events that carry usage.
    #[tokio::test]
    async fn a_native_stream_is_relayed_byte_for_byte_and_still_settles_usage() {
        let ledger = Arc::new(Ledger::default());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let app = axum::Router::new().route(
            "/messages",
            post(|| async {
                (
                    [(header::CONTENT_TYPE, "text/event-stream")],
                    Body::from(NATIVE_STREAM),
                )
            }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let cfg = Config::from_toml_str(&format!(
            r#"
[[namespace]]
id = "platform"
default = true

[[provider]]
id = "anthropic"
kind = "anthropic"
base_url = "http://{addr}"

{GATEWAY_KEY}

[[credential]]
namespace = "platform"
provider = "anthropic"
env = "GW_TEST_OPENAI_KEY"

[[model]]
name = "claude"
targets = [{{ provider = "anthropic", model = "claude-sonnet-4-5", price = {{ input_microdollars_per_million = 1000000, output_microdollars_per_million = 2000000 }} }}]
"#
        ))
        .expect("config");
        let sinks: Vec<Box<dyn UsageSink>> = vec![Box::new(LedgerSink(ledger.clone()))];
        let state = AppState::new(
            cfg,
            &test_env(),
            UsageFanout::new(sinks),
            Box::new(LedgerBudget(ledger.clone())),
        )
        .expect("state");

        let body = json!({
            "model": "claude",
            "stream": true,
            "max_tokens": 64,
            "messages": [{ "role": "user", "content": "hi" }]
        });
        let resp = router(state)
            .oneshot(
                authorized("/v1/messages")
                    .body(Body::from(serde_json::to_vec(&body).expect("body")))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(resp.status(), StatusCode::OK);

        let mut stream = resp.into_body().into_data_stream();
        let mut relayed = String::new();
        while let Some(chunk) = stream.next().await {
            relayed.push_str(&String::from_utf8_lossy(&chunk.expect("chunk")));
        }
        assert_eq!(relayed, NATIVE_STREAM);
        assert!(!relayed.contains("[DONE]"));

        let record = settled(&ledger).await;
        assert_eq!(record["status"], "ok");
        assert_eq!(record["input_tokens"], 11);
        assert_eq!(record["output_tokens"], 3);
        // 11 input + 2 cached reads @ 1 µ$/token, 3 output @ 2 µ$/token.
        assert_eq!(record["cost_microdollars"], 19);
        assert_eq!(ledger.settlements(), vec![19]);
    }

    #[tokio::test]
    async fn mid_stream_upstream_failure_becomes_a_terminal_error_event() {
        let ledger = Arc::new(Ledger::default());
        let base_url = upstream_serving(concat!(
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\n",
            "data: {not json}\n\n",
        ))
        .await;
        let resp = router(state_for(&base_url, ledger.clone()))
            .oneshot(stream_request())
            .await
            .expect("response");
        assert_eq!(resp.status(), StatusCode::OK);

        let mut body = resp.into_body().into_data_stream();
        let mut relayed = String::new();
        while let Some(chunk) = body.next().await {
            relayed.push_str(&String::from_utf8_lossy(&chunk.expect("chunk")));
        }
        assert!(relayed.contains("event: error"));
        assert!(relayed.ends_with("data: [DONE]\n\n"));

        let record = settled(&ledger).await;
        assert_eq!(record["status"], "upstream_error");
    }

    #[tokio::test]
    async fn reported_zero_output_wins_without_rotation() {
        let ledger = Arc::new(Ledger::default());
        let base_url = upstream_serving(
            "data: {\"choices\":[{\"delta\":{\"content\":\"relayed text\"}}]}\n\n\
data: {\"choices\":[],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":0}}\n\n\
data: [DONE]\n\n",
        )
        .await;
        let resp = router(state_for(&base_url, ledger.clone()))
            .oneshot(stream_request())
            .await
            .expect("response");
        let mut body = resp.into_body().into_data_stream();
        while let Some(chunk) = body.next().await {
            chunk.expect("chunk");
        }
        let record = settled(&ledger).await;
        assert_eq!(record["input_tokens"], 5);
        assert_eq!(record["output_tokens"], 0);
    }

    #[test]
    fn fold_attempt_carries_reported_output_and_chargeable_usage_keeps_prompt_once() {
        let mut accounting = Accounting::new(
            state_for("http://127.0.0.1:1", Arc::new(Ledger::default())),
            context(),
            Instant::now(),
        );
        accounting.usage = ModelUsage {
            input_tokens: 7,
            output_tokens: 2,
            ..ModelUsage::default()
        };
        accounting.fold_attempt();
        assert_eq!(accounting.carried_output_tokens, 2);
        assert_eq!(accounting.usage, ModelUsage::default());

        accounting.usage.input_tokens = 5;
        accounting.usage.output_tokens = 1;
        assert_eq!(accounting.chargeable_usage().input_tokens, 5);
        assert_eq!(accounting.chargeable_usage().output_tokens, 3);
    }

    #[tokio::test]
    async fn multibyte_characters_survive_a_chunk_boundary() {
        let mut relay = Relay {
            bytes: Box::pin(futures::stream::empty()),
            carry: Vec::new(),
            sse: Some(SseDecoder::default()),
            decoder: gateway_core::ProviderAdapter::stream_decoder(
                &gateway_core::OpenAiCompatibleAdapter::openai(),
                gateway_core::Surface::ChatCompletions,
            )
            .expect("decoder"),
            pending: VecDeque::new(),
            phase: Phase::Streaming,
            framing: Framing::OpenAiSse,
            deadline: None,
            max_bytes: None,
            relayed_bytes: 0,
            accounting: Accounting::new(
                state_for("http://127.0.0.1:1", Arc::new(Ledger::default())),
                context(),
                Instant::now(),
            ),
            rotation: None,
            queued_downstream: false,
        };
        let text = "héllo — 日本語";
        let bytes = text.as_bytes();
        let mut decoded = String::new();
        for chunk in bytes.chunks(3) {
            decoded.push_str(&relay.decode_utf8(chunk));
        }
        assert_eq!(decoded, text);
        assert!(relay.carry.is_empty());
    }

    #[tokio::test]
    async fn a_truncated_final_event_is_reported_rather_than_completed() {
        let ledger = Arc::new(Ledger::default());
        let base_url = upstream_serving(concat!(
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\n",
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\"",
        ))
        .await;
        let resp = router(state_for(&base_url, ledger.clone()))
            .oneshot(stream_request())
            .await
            .expect("response");

        let mut body = resp.into_body().into_data_stream();
        let mut relayed = String::new();
        while let Some(chunk) = body.next().await {
            relayed.push_str(&String::from_utf8_lossy(&chunk.expect("chunk")));
        }
        assert!(relayed.contains("event: error"));
        assert!(relayed.contains("incomplete"));

        let record = settled(&ledger).await;
        assert_eq!(record["status"], "upstream_error");
    }

    #[tokio::test]
    async fn client_disconnect_settles_the_stream_as_cancelled() {
        let ledger = Arc::new(Ledger::default());
        let base_url = upstream_serving(OPENAI_STREAM).await;
        let resp = router(state_for(&base_url, ledger.clone()))
            .oneshot(stream_request())
            .await
            .expect("response");

        let mut body = resp.into_body().into_data_stream();
        body.next().await.expect("first chunk").expect("chunk");
        drop(body);

        let record = settled(&ledger).await;
        assert_eq!(record["status"], "client_cancelled");
        // The provider never reported usage, so the charge is the measured
        // partial spend: the prompt it consumed plus the text actually relayed —
        // not zero, and not the reserved estimate (ADR 0010).
        let charged = record["cost_microdollars"].as_u64().expect("cost");
        assert!(charged > 0, "a cancelled stream must not be free");
        assert!(record["input_tokens"].as_u64().expect("input") > 0);
        assert!(record["output_tokens"].as_u64().expect("output") > 0);
        assert_eq!(ledger.settlements(), vec![charged]);
    }

    #[tokio::test]
    async fn accounting_drop_releases_rate_limit_permit_once() {
        let limiter = crate::rate_limit::InMemoryRateLimiter::new(1, 10);
        let key = crate::rate_limit::RateLimitKey {
            namespace: "platform".to_owned(),
            subject: "GW_TEST_INBOUND_KEY".to_owned(),
        };
        let permit = limiter.acquire(&key).await.expect("permit");
        let mut ctx = context();
        ctx.rate_limit_permit = Some(permit);
        let state = state_for("http://127.0.0.1:1", Arc::new(Ledger::default()));
        drop(Accounting::new(state, ctx, Instant::now()));
        let replacement = limiter.acquire(&key).await.expect("released permit");
        drop(replacement);
        let replacement = limiter.acquire(&key).await.expect("idempotent release");
        drop(replacement);
    }

    /// A stream that fails before the provider reports usage is charged the
    /// same way: what it relayed, priced from the catalog.
    #[tokio::test]
    async fn a_broken_stream_charges_what_it_relayed() {
        let ledger = Arc::new(Ledger::default());
        let base_url = upstream_serving(concat!(
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"partial answer\"}}]}\n\n",
            "data: {not json}\n\n",
        ))
        .await;
        let resp = router(state_for(&base_url, ledger.clone()))
            .oneshot(stream_request())
            .await
            .expect("response");

        let mut body = resp.into_body().into_data_stream();
        while let Some(chunk) = body.next().await {
            let _ = chunk.expect("chunk");
        }

        let record = settled(&ledger).await;
        assert_eq!(record["status"], "upstream_error");
        // 14 relayed characters ≈ 4 output tokens @ 2 µ$ + 8 estimated input
        // tokens @ 1 µ$.
        assert_eq!(record["output_tokens"], 4);
        let charged = record["cost_microdollars"].as_u64().expect("cost");
        assert!(charged > 0);
        assert_eq!(ledger.settlements(), vec![charged]);
    }

    /// A stream that never opened relayed nothing, so there is nothing to
    /// measure and nothing to charge — the whole hold goes back.
    #[tokio::test]
    async fn a_stream_that_never_opened_is_charged_nothing() {
        let ledger = Arc::new(Ledger::default());
        let base_url = failing_to_open_upstream(StatusCode::INTERNAL_SERVER_ERROR).await;
        let resp = router(state_for(&base_url, ledger.clone()))
            .oneshot(stream_request())
            .await
            .expect("response");
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);

        let record = settled(&ledger).await;
        assert_eq!(record["status"], "upstream_error");
        assert_eq!(record["cost_microdollars"], 0);
        assert_eq!(ledger.settlements(), vec![0]);
    }

    #[tokio::test]
    async fn an_unavailable_budget_store_rejects_the_request() {
        let base_url = upstream_serving(OPENAI_STREAM).await;
        for (denial, status) in [
            (Denial::Exceeded, StatusCode::TOO_MANY_REQUESTS),
            (Denial::StoreUnavailable, StatusCode::SERVICE_UNAVAILABLE),
        ] {
            let state = state_with_budget(&base_url, Box::new(DenyingBudget(denial)));
            let resp = router(state)
                .oneshot(stream_request())
                .await
                .expect("response");
            assert_eq!(resp.status(), status);
        }
    }

    /// An upstream that never opens a stream: it answers a non-200 status, so
    /// the dispatch fails before a single byte is relayed.
    async fn failing_to_open_upstream(status: StatusCode) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let app = axum::Router::new().route(
            "/chat/completions",
            post(move || async move { (status, "upstream is unavailable") }),
        );
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        format!("http://{addr}")
    }

    fn two_target_stream_state(url_a: &str, url_b: &str, ledger: Arc<Ledger>) -> AppState {
        let cfg = Config::from_toml_str(&format!(
            r#"
[[namespace]]
id = "platform"
default = true

[[provider]]
id = "pa"
kind = "openai"
base_url = "{url_a}"

[[provider]]
id = "pb"
kind = "openai"
base_url = "{url_b}"

{GATEWAY_KEY}

[[credential]]
namespace = "platform"
provider = "pa"
env = "KA"

[[credential]]
namespace = "platform"
provider = "pb"
env = "KB"

[[model]]
name = "gpt-4o"
targets = [
  {{ provider = "pa", model = "m-a", price = {{ input_microdollars_per_million = 1000000, output_microdollars_per_million = 2000000 }} }},
  {{ provider = "pb", model = "m-b", price = {{ input_microdollars_per_million = 1000000, output_microdollars_per_million = 2000000 }} }},
]
"#
        ))
        .expect("config");
        let env = HashMap::from([
            ("KA".to_owned(), "a".to_owned()),
            ("KB".to_owned(), "b".to_owned()),
            ("GW_TEST_INBOUND_KEY".to_owned(), CALLER_SECRET.to_owned()),
        ]);
        let sinks: Vec<Box<dyn UsageSink>> = vec![Box::new(LedgerSink(ledger.clone()))];
        AppState::new(
            cfg,
            &env,
            UsageFanout::new(sinks),
            Box::new(LedgerBudget(ledger)),
        )
        .expect("state")
    }

    #[tokio::test]
    async fn streaming_fails_over_to_the_next_target_before_the_first_byte() {
        let ledger = Arc::new(Ledger::default());
        let url_a = failing_to_open_upstream(StatusCode::INTERNAL_SERVER_ERROR).await;
        let url_b = upstream_serving(OPENAI_STREAM).await;
        let resp = router(two_target_stream_state(&url_a, &url_b, ledger.clone()))
            .oneshot(stream_request())
            .await
            .expect("response");

        // The first target failed to open, so failover picked the second and the
        // client only ever saw a successful `200` stream.
        assert_eq!(resp.status(), StatusCode::OK);
        let mut body = resp.into_body().into_data_stream();
        let mut relayed = String::new();
        while let Some(chunk) = body.next().await {
            relayed.push_str(&String::from_utf8_lossy(&chunk.expect("chunk")));
        }
        assert!(relayed.contains("\"content\":\"hel\""));
        assert!(relayed.ends_with("data: [DONE]\n\n"));

        let record = settled(&ledger).await;
        assert_eq!(record["status"], "ok");
        assert_eq!(record["target_provider"], "pb");
        assert_eq!(record["target_model"], "m-b");
        assert_eq!(record["attempts"], 2);
    }

    #[tokio::test]
    async fn stream_open_rotates_a_rate_limited_credential() {
        let ledger = Arc::new(Ledger::default());
        let base_url = pooled_upstream().await;
        let state = pooled_state(&base_url, ledger.clone());
        let resp = router(state.clone())
            .oneshot(stream_request())
            .await
            .expect("response");

        assert_eq!(resp.status(), StatusCode::OK);
        let mut body = resp.into_body().into_data_stream();
        let mut relayed = String::new();
        while let Some(chunk) = body.next().await {
            relayed.push_str(&String::from_utf8_lossy(&chunk.expect("chunk")));
        }
        assert!(relayed.contains("\"content\":\"hel\""));
        let record = settled(&ledger).await;
        assert_eq!(record["credential_id"], "b");
    }

    #[tokio::test]
    async fn non_retryable_stream_open_does_not_try_the_next_target() {
        let ledger = Arc::new(Ledger::default());
        let url_a = failing_to_open_upstream(StatusCode::BAD_REQUEST).await;
        let url_b = upstream_serving(OPENAI_STREAM).await;
        let state = two_target_stream_state(&url_a, &url_b, ledger.clone());
        let resp = router(state)
            .oneshot(stream_request())
            .await
            .expect("response");

        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
        let record = settled(&ledger).await;
        assert_eq!(record["target_provider"], "pa");
        assert_eq!(record["attempts"], 1);
    }

    fn test_opened_stream(body: &'static str) -> OpenedStream {
        OpenedStream {
            bytes: futures::stream::iter(vec![Ok(Bytes::from_static(body.as_bytes()))]).boxed(),
            decoder: OpenAiCompatibleAdapter::openai()
                .stream_decoder(Surface::ChatCompletions)
                .expect("decoder"),
        }
    }

    fn test_lease(id: &str) -> CredentialLease {
        CredentialLease::test(id)
    }

    #[tokio::test]
    async fn pre_content_rate_limit_rotates_and_carries_usage_once() {
        let ledger = Arc::new(Ledger::default());
        let failures = Arc::new(AtomicUsize::new(0));
        let failed_ids = Arc::new(Mutex::new(Vec::new()));
        let state = state_for("http://127.0.0.1:1", ledger.clone());
        let opener = |_lease: CredentialLease, _attempt: u32, _index: usize| {
            Box::pin(async {
                Ok(test_opened_stream(
                    "data: {\"choices\":[{\"delta\":{\"content\":\"b\"}}]}\n\ndata: [DONE]\n\n",
                ))
            }) as futures::future::BoxFuture<'static, _>
        };
        let failures_for_callback = failures.clone();
        let failed_ids_for_callback = failed_ids.clone();
        let rotation = RotationHandle::new(
            vec![test_lease("b")],
            test_lease("a"),
            1,
            opener,
            move |lease| {
                failures_for_callback.fetch_add(1, Ordering::SeqCst);
                failed_ids_for_callback
                    .lock()
                    .expect("failed ids")
                    .push(lease.id.clone());
            },
            |_| {},
        );
        let response = relay_opened(
            state,
            context(),
            OpenAiCompatibleAdapter::openai()
                .stream_decoder(Surface::ChatCompletions)
                .expect("decoder"),
            futures::stream::iter(vec![Ok(Bytes::from_static(
                b"data: {\"error\":{\"type\":\"rate_limit_exceeded\"}}\n\n",
            ))])
            .boxed(),
            Instant::now(),
            Framing::OpenAiSse,
            Some(rotation),
        );
        let mut body = response.into_body().into_data_stream();
        let mut output = String::new();
        while let Some(chunk) = body.next().await {
            output.push_str(&String::from_utf8_lossy(&chunk.expect("chunk")));
        }
        assert!(output.contains("\"content\":\"b\""));
        assert_eq!(output.matches("data: [DONE]").count(), 1);
        assert!(!output.contains("rate_limit_exceeded"));
        let record = settled(&ledger).await;
        assert_eq!(record["input_tokens"], 8);
        assert_eq!(record["output_tokens"], 1);
        assert_eq!(ledger.settlements().len(), 1);
        assert_eq!(failures.load(Ordering::SeqCst), 1);
        assert_eq!(*failed_ids.lock().expect("failed ids"), ["a"]);
    }

    #[tokio::test]
    async fn rotation_skips_rate_limited_reopens_before_serving() {
        let ledger = Arc::new(Ledger::default());
        let opens = Arc::new(AtomicUsize::new(0));
        let failures = Arc::new(AtomicUsize::new(0));
        let opens_for_opener = opens.clone();
        let opener = move |_lease: CredentialLease, _attempt: u32, _index: usize| {
            let opens = opens_for_opener.clone();
            Box::pin(async move {
                if opens.fetch_add(1, Ordering::SeqCst) < 2 {
                    Err(gateway_transport::TransportError::Provider(
                        ProviderError::from_upstream("test", 429, "rate limited"),
                    ))
                } else {
                    Ok(test_opened_stream(
                        "data: {\"choices\":[{\"delta\":{\"content\":\"c\"}}]}\n\ndata: [DONE]\n\n",
                    ))
                }
            }) as futures::future::BoxFuture<'static, _>
        };
        let failures_for_callback = failures.clone();
        let response = relay_opened(
            state_for("http://127.0.0.1:1", ledger.clone()),
            context(),
            OpenAiCompatibleAdapter::openai()
                .stream_decoder(Surface::ChatCompletions)
                .expect("decoder"),
            futures::stream::iter(vec![Ok(Bytes::from_static(
                b"data: {\"error\":{\"type\":\"rate_limit_exceeded\"}}\n\n",
            ))])
            .boxed(),
            Instant::now(),
            Framing::OpenAiSse,
            Some(RotationHandle::new(
                vec![test_lease("b"), test_lease("c"), test_lease("d")],
                test_lease("a"),
                1,
                opener,
                move |_| {
                    failures_for_callback.fetch_add(1, Ordering::SeqCst);
                },
                |_| {},
            )),
        );
        let mut body = response.into_body().into_data_stream();
        let mut output = String::new();
        while let Some(chunk) = body.next().await {
            output.push_str(&String::from_utf8_lossy(&chunk.expect("chunk")));
        }
        assert!(output.contains("\"content\":\"c\""));
        assert_eq!(opens.load(Ordering::SeqCst), 3);
        assert_eq!(failures.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn rate_limit_after_done_does_not_reopen_the_stream() {
        let ledger = Arc::new(Ledger::default());
        let ledger_for_relay = ledger.clone();
        let calls = Arc::new(AtomicUsize::new(0));
        let failures = Arc::new(AtomicUsize::new(0));
        let calls_for_open = calls.clone();
        let opener = move |_lease: CredentialLease, _attempt: u32, _index: usize| {
            let calls = calls_for_open.clone();
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(test_opened_stream(
                    "data: {\"choices\":[{\"delta\":{\"content\":\"replacement\"}}]}\n\ndata: [DONE]\n\n",
                ))
            }) as futures::future::BoxFuture<'static, _>
        };
        let response = relay_opened(
            state_for("http://127.0.0.1:1", ledger_for_relay),
            context(),
            OpenAiCompatibleAdapter::openai()
                .stream_decoder(Surface::ChatCompletions)
                .expect("decoder"),
            futures::stream::iter(vec![Ok(Bytes::from_static(
                b"data: [DONE]\n\ndata: {\"error\":{\"type\":\"rate_limit_exceeded\"}}\n\n",
            ))])
            .boxed(),
            Instant::now(),
            Framing::OpenAiSse,
            Some(RotationHandle::new(
                vec![test_lease("b")],
                test_lease("a"),
                1,
                opener,
                {
                    let failures = failures.clone();
                    move |_| {
                        failures.fetch_add(1, Ordering::SeqCst);
                    }
                },
                |_| {},
            )),
        );
        let mut body = response.into_body().into_data_stream();
        let mut output = String::new();
        while let Some(chunk) = body.next().await {
            output.push_str(&String::from_utf8_lossy(&chunk.expect("chunk")));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(!output.contains("event: error"));
        assert!(output.ends_with("data: [DONE]\n\n"));
        assert_eq!(settled(&ledger).await["status"], "ok");
    }

    #[tokio::test]
    async fn stream_open_429_closes_lease_as_rate_limited() {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let subscriber = tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("axond-test")));
        let dispatch = tracing::Dispatch::new(subscriber);
        let _default = tracing::dispatcher::set_default(&dispatch);
        let server = tracing::info_span!("http.server.request");
        let parent_context = server.context();
        let attempt = telemetry::upstream_attempt_span(0, "test", "model", "platform");
        let _ = attempt.set_parent(parent_context);
        let entered = attempt.enter();
        let result =
            open_stream_with_attempt_span(&context(), &attempt, "rate-limited", 0, async {
                Err(TransportError::Provider(ProviderError::from_upstream(
                    "test",
                    429,
                    "rate limited",
                )))
            })
            .await;
        drop(entered);
        assert!(result.is_err());
        telemetry::finish_upstream_attempt(&attempt, telemetry::ATTEMPT_ERROR, 0, None);
        drop(attempt);
        drop(server);
        provider.force_flush().unwrap();
        let spans = exporter.get_finished_spans().unwrap();
        let server = spans
            .iter()
            .find(|span| span.name == "http.server.request")
            .expect("server span");
        let attempt = spans
            .iter()
            .find(|span| span.name == "axond.upstream.attempt")
            .expect("attempt span");
        assert_eq!(attempt.parent_span_id, server.span_context.span_id());
        assert_eq!(
            attempt.span_context.trace_id(),
            server.span_context.trace_id()
        );
        let lease = spans
            .iter()
            .find(|span| span.name == "axond.credential.lease")
            .expect("lease span");
        assert_eq!(lease.parent_span_id, attempt.span_context.span_id());
        let status = lease
            .attributes
            .iter()
            .find(|kv| kv.key.as_str() == "axond.status")
            .map(|kv| kv.value.to_string());
        assert_eq!(status.as_deref(), Some("rate_limited"));
    }

    #[tokio::test]
    async fn post_content_rate_limit_is_terminal_without_rotation() {
        let ledger = Arc::new(Ledger::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let failures = Arc::new(AtomicUsize::new(0));
        let calls_for_open = calls.clone();
        let opener = move |_lease: CredentialLease, _attempt: u32, _index: usize| {
            let calls = calls_for_open.clone();
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(test_opened_stream(
                    "data: {\"choices\":[{\"delta\":{\"content\":\"b\"}}]}\n\ndata: [DONE]\n\n",
                ))
            }) as futures::future::BoxFuture<'static, _>
        };
        let response = relay_opened(
            state_for("http://127.0.0.1:1", ledger.clone()),
            context(),
            OpenAiCompatibleAdapter::openai()
                .stream_decoder(Surface::ChatCompletions)
                .expect("decoder"),
            futures::stream::iter(vec![Ok(Bytes::from_static(
                b"data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\ndata: {\"error\":{\"type\":\"rate_limit_exceeded\"}}\n\n",
            ))])
            .boxed(),
            Instant::now(),
            Framing::OpenAiSse,
            Some(RotationHandle::new(
                vec![test_lease("b")],
                test_lease("a"),
                1,
                opener,
                {
                    let failures = failures.clone();
                    move |_| {
                        failures.fetch_add(1, Ordering::SeqCst);
                    }
                },
                |_| {},
            )),
        );
        let mut body = response.into_body().into_data_stream();
        let mut output = String::new();
        while let Some(chunk) = body.next().await {
            output.push_str(&String::from_utf8_lossy(&chunk.expect("chunk")));
        }
        assert!(output.contains("\"content\":\"a\""));
        assert!(output.contains("OpenAI stream rate limited"));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(failures.load(Ordering::SeqCst), 1);
        assert_eq!(failures.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn native_rate_limit_is_terminal_without_rotation() {
        let ledger = Arc::new(Ledger::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let failures = Arc::new(AtomicUsize::new(0));
        let calls_for_open = calls.clone();
        let opener = move |_lease: CredentialLease, _attempt: u32, _index: usize| {
            let calls = calls_for_open.clone();
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(test_opened_stream("data: [DONE]\n\n"))
            }) as futures::future::BoxFuture<'static, _>
        };
        let response = relay_opened(
            state_for("http://127.0.0.1:1", ledger),
            context(),
            Box::new(NativeMessagesDecoder::new()),
            futures::stream::iter(vec![Ok(Bytes::from_static(
                b"data: {\"error\":{\"type\":\"rate_limit_exceeded\"}}\n\n",
            ))])
            .boxed(),
            Instant::now(),
            Framing::Native,
            Some(RotationHandle::new(
                vec![test_lease("b")],
                test_lease("a"),
                1,
                opener,
                {
                    let failures = failures.clone();
                    move |_| {
                        failures.fetch_add(1, Ordering::SeqCst);
                    }
                },
                |_| {},
            )),
        );
        let mut body = response.into_body().into_data_stream();
        while let Some(chunk) = body.next().await {
            let _ = chunk.expect("chunk");
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(failures.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn rotation_stops_before_opening_after_deadline() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_open = calls.clone();
        let opener = move |_lease: CredentialLease, _attempt: u32, _index: usize| {
            let calls = calls_for_open.clone();
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(TransportError::Provider(ProviderError::from_upstream(
                    "test",
                    429,
                    "rate limited",
                )))
            }) as futures::future::BoxFuture<'static, _>
        };
        let mut rotation = RotationHandle::new_with_deadline(
            vec![test_lease("b"), test_lease("c")],
            test_lease("a"),
            1,
            opener,
            Some(Instant::now() - Duration::from_millis(1)),
            |_| {},
            |_| {},
        );

        assert!(
            rotation
                .open_next()
                .await
                .expect("rotation result")
                .is_none()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}
