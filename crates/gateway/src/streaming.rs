//! Server-sent-events relay for streamed completions.
//!
//! The relay decodes the upstream stream with `gateway-core` (`SseDecoder` +
//! the provider's `ProviderStreamDecoder`) and re-emits OpenAI-shaped chunks,
//! so a target reaches the caller in the OpenAI chunk shape whichever wire it
//! spoke upstream. On a native route the same relay normally forwards the
//! provider's bytes untouched and decodes only to observe usage ([`Framing`]).
//! The explicit policy-buffered posture is the sole exception: it reconstructs
//! transformed events behind a finite byte bound and releases them only after
//! successful upstream completion. Incremental postures inherit the client's
//! backpressure because axum only polls the upstream as the socket drains.
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
use gateway_core::{ModelUsage, ProviderStreamDecoder, ProviderStreamEvent, SseDecoder, SseEvent};
use gateway_transport::{ByteStream, TransportError};
use opentelemetry::Context;
use serde_json::{Value, json};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::admission::AdmissionPermit;
use crate::budget::{BudgetKey, Reservation};
use crate::credentials::{CredentialLease, CredentialSource};
use crate::error::transport_caller_message;
use crate::middleware::MiddlewareExecution;
use crate::pricing::RequestPrice;
use crate::rate_limit::RateLimitPermit;
use crate::state::AppState;
use crate::telemetry;
use crate::usage::identity::EventIdentity;
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
    /// The identity of the single usage event this stream will settle as, minted
    /// in the handler when the request was admitted. Carried rather than derived:
    /// settlement may run in a detached task where the server span is no longer
    /// current, and every way a stream can end — terminal, cancelled, rotated,
    /// never opened — has to report the same event.
    pub identity: EventIdentity,
    /// The rates this stream is charged at, and the approved pricing that set
    /// them. Copied from the request's snapshot before the stream opened, so a
    /// price book published while it is relaying changes nothing about how it
    /// settles (#147).
    pub price: RequestPrice,
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

/// How one opened upstream stream reaches the caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamDelivery {
    /// Decode and re-emit each event as it arrives.
    Reemit,
    /// Forward the provider's exact chunks while decoding only for accounting.
    Passthrough,
    /// Decode, transform, and hold the complete reconstructed SSE body until
    /// the upstream terminates successfully.
    PolicyBuffered,
    /// Decode and validate the complete stream while holding the provider's
    /// exact chunks, then release those original bytes only after every
    /// applicable middleware callback approves their strict parsed view. The
    /// parser refuses SSE fields the callback cannot see and ambiguous duplicate
    /// JSON keys; the caller still receives the provider's lexical byte spelling.
    PolicyValidatedPassthrough,
}

impl StreamDelivery {
    fn is_policy_buffered(self) -> bool {
        matches!(
            self,
            Self::PolicyBuffered | Self::PolicyValidatedPassthrough
        )
    }
}

/// The pinned response transformation and the wire-delivery posture selected
/// from the same serving snapshot.
pub struct StreamMiddleware {
    execution: MiddlewareExecution,
    delivery: StreamDelivery,
    mutates_rendered_output: bool,
}

impl StreamMiddleware {
    pub fn new(execution: MiddlewareExecution, delivery: StreamDelivery) -> Self {
        let mutates_rendered_output = execution.has_stream_event_mutator();
        Self {
            execution,
            delivery,
            mutates_rendered_output,
        }
    }
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
#[allow(dead_code)]
pub fn relay_opened(
    state: AppState,
    ctx: StreamContext,
    decoder: Box<dyn ProviderStreamDecoder>,
    bytes: ByteStream,
    started: Instant,
    framing: Framing,
    rotation: Option<RotationHandle>,
) -> Response {
    relay_opened_with_middleware(
        state,
        ctx,
        OpenedStream { decoder, bytes },
        started,
        framing,
        rotation,
        StreamMiddleware::new(
            MiddlewareExecution::default(),
            if framing.reemits() {
                StreamDelivery::Reemit
            } else {
                StreamDelivery::Passthrough
            },
        ),
    )
}

/// Variant of [`relay_opened`] used by the middleware chain. The execution is
/// moved into `Accounting`, which is owned by the response body and therefore
/// survives the handler and drops on normal completion, client hangup, or
/// cancellation together with the existing accounting owner.
pub fn relay_opened_with_middleware(
    state: AppState,
    ctx: StreamContext,
    opened: OpenedStream,
    started: Instant,
    framing: Framing,
    rotation: Option<RotationHandle>,
    middleware: StreamMiddleware,
) -> Response {
    let OpenedStream { decoder, bytes } = opened;
    let StreamMiddleware {
        execution: middleware_execution,
        delivery,
        mutates_rendered_output,
    } = middleware;
    let finalization_required = middleware_execution.has_stream_event_scope();
    // A stream's *total* lifetime, as opposed to the transport's idle bound,
    // which a trickle of keepalives resets forever.
    let limits = state.0.admission.limits();
    let deadline = limits.max_stream_duration.map(|budget| started + budget);
    let max_bytes = limits.max_stream_bytes;
    let stream_terminal_grace = state.0.stream_terminal_grace;
    let relay = Relay {
        bytes,
        carry: Vec::new(),
        sse: Some(SseDecoder::default()),
        decoder,
        pending: VecDeque::new(),
        phase: Phase::Streaming,
        framing,
        delivery,
        buffered: VecDeque::new(),
        buffering_started: None,
        buffered_bytes: 0,
        rendered_byte_limit: rendered_stream_byte_limit(
            delivery,
            mutates_rendered_output,
            max_bytes,
        ),
        terminal_seen: false,
        stream_terminal_grace,
        terminal_deadline: None,
        finalization_required,
        accounting: Accounting::new_with_middleware(state, ctx, started, middleware_execution),
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
#[allow(dead_code)]
pub fn settle_upstream_error(state: AppState, ctx: StreamContext, started: Instant) {
    let mut accounting = Accounting::new(state, ctx, started);
    accounting.settle(Status::UpstreamError);
}

/// Settle a streamed request that never opened a stream while retaining the
/// same response-lifetime ownership rule for middleware state.
#[allow(dead_code)]
pub fn settle_upstream_error_with_middleware(
    state: AppState,
    ctx: StreamContext,
    started: Instant,
    middleware_execution: MiddlewareExecution,
) {
    let mut accounting = Accounting::new_with_middleware(state, ctx, started, middleware_execution);
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

    fn middleware_error(self, message: &str) -> Bytes {
        match self {
            Self::OpenAiSse => middleware_error_event(message),
            Self::Native => native_middleware_error_event(message),
            Self::Responses => responses_middleware_error_event(message),
        }
    }
}

enum Phase {
    Streaming,
    Failed(String),
    MiddlewareFailed(String),
    Finished,
    Draining,
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
    delivery: StreamDelivery,
    /// Reconstructed output held privately for explicit policy buffering.
    buffered: VecDeque<Bytes>,
    /// When the first decoded event would have become caller-visible without
    /// buffering. Its elapsed time is the gateway-added buffering cost.
    buffering_started: Option<Instant>,
    /// Cumulative bytes in reconstructed or held downstream events. This is
    /// distinct from provider bytes because response middleware can expand a
    /// short placeholder before the event is rendered.
    buffered_bytes: u64,
    /// Post-decoding bytes produced or retained for caller delivery. A mutator
    /// and either policy-buffered posture have a finite hard ceiling even when
    /// the raw upstream/configured ceiling is disabled. Ordinary and block-only
    /// re-emission follow only the configured ceiling.
    rendered_byte_limit: Option<u64>,
    /// A terminal decoder event has been observed. Buffered policy streams keep
    /// validating until transport EOF so no later provider bytes can bypass
    /// middleware. Byte-faithful passthrough keeps relaying raw chunks until EOF
    /// but stops interpreting provider extensions after their terminal event.
    terminal_seen: bool,
    /// Fixed grace after a byte-faithful semantic terminal event. It does not
    /// reset when provider extension chunks arrive.
    stream_terminal_grace: Duration,
    terminal_deadline: Option<Instant>,
    /// A stream-event chain owns response-lifetime state that must be finalized
    /// only after the provider's semantic terminal event and strict EOF checks.
    /// Keeping this separate from `terminal_seen` preserves the immediate close
    /// of ordinary re-emitted streams that have no stream-event middleware.
    finalization_required: bool,
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

/// Reconstructed mutating output and policy buffering must remain bounded even
/// when the configured raw-stream byte ceiling is disabled. Ordinary and
/// validation-only incremental re-emission retain the configured semantics.
const MAX_POLICY_BUFFERED_BYTES: u64 = 64 * 1024 * 1024;

fn rendered_stream_byte_limit(
    delivery: StreamDelivery,
    mutates_rendered_output: bool,
    configured: Option<u64>,
) -> Option<u64> {
    if delivery.is_policy_buffered()
        || (delivery == StreamDelivery::Reemit && mutates_rendered_output)
    {
        Some(
            configured
                .unwrap_or(MAX_POLICY_BUFFERED_BYTES)
                .min(MAX_POLICY_BUFFERED_BYTES),
        )
    } else if delivery == StreamDelivery::Reemit {
        configured
    } else {
        None
    }
}

impl Relay {
    fn reserve_rendered_bytes(&mut self, bytes: usize) -> bool {
        let Ok(bytes) = u64::try_from(bytes) else {
            return false;
        };
        let Some(next) = self.buffered_bytes.checked_add(bytes) else {
            return false;
        };
        if self.rendered_byte_limit.is_some_and(|limit| next > limit) {
            return false;
        }
        self.buffered_bytes = next;
        true
    }

    fn fail_rendered_bytes(&mut self) {
        self.phase = if self.finalization_required {
            Phase::MiddlewareFailed(STREAM_BYTES_EXCEEDED.to_owned())
        } else {
            Phase::Failed(STREAM_BYTES_EXCEEDED.to_owned())
        };
    }

    async fn next_chunk(&mut self) -> Option<Result<Bytes, Infallible>> {
        loop {
            if !self.pending.is_empty() {
                if matches!(self.phase, Phase::Draining)
                    && self
                        .deadline
                        .is_some_and(|deadline| Instant::now() >= deadline)
                {
                    self.pending.clear();
                    self.fail_stream_duration();
                    continue;
                }
                return self.pending.pop_front().map(Ok);
            }
            match &self.phase {
                Phase::Streaming => self.poll_upstream().await,
                Phase::Failed(message) => {
                    let message = message.clone();
                    self.buffered.clear();
                    self.pending.push_back(self.framing.error(&message));
                    self.pending.extend(self.framing.done());
                    self.accounting.settle(Status::UpstreamError);
                    self.phase = Phase::Ended;
                }
                Phase::MiddlewareFailed(message) => {
                    let message = message.clone();
                    self.buffered.clear();
                    self.pending
                        .push_back(self.framing.middleware_error(&message));
                    self.pending.extend(self.framing.done());
                    self.accounting.settle(if self.queued_downstream {
                        Status::Partial
                    } else {
                        Status::Rejected
                    });
                    self.phase = Phase::Ended;
                }
                Phase::Finished => {
                    if self.delivery.is_policy_buffered()
                        && self
                            .deadline
                            .is_some_and(|deadline| Instant::now() >= deadline)
                    {
                        self.fail_stream_duration();
                        continue;
                    }
                    let policy_buffered = self.delivery.is_policy_buffered();
                    let done = self.framing.done();
                    if matches!(
                        self.delivery,
                        StreamDelivery::Reemit | StreamDelivery::PolicyBuffered
                    ) && done
                        .as_ref()
                        .is_some_and(|done| !self.reserve_rendered_bytes(done.len()))
                    {
                        self.fail_rendered_bytes();
                        continue;
                    }
                    if policy_buffered {
                        let buffering_ms = self
                            .buffering_started
                            .map_or(0.0, |started| started.elapsed().as_secs_f64() * 1_000.0);
                        telemetry::metrics::record_middleware_buffering_duration(buffering_ms);
                        if !self.buffered.is_empty() {
                            self.accounting.mark_downstream_first_token();
                        }
                        self.pending.append(&mut self.buffered);
                        self.queued_downstream = !self.pending.is_empty();
                    }
                    self.pending.extend(done);
                    if let Some(rotation) = self.rotation.as_ref() {
                        rotation.record_serving_success();
                    }
                    if policy_buffered {
                        self.phase = Phase::Draining;
                    } else {
                        self.accounting.settle(Status::Ok);
                        self.phase = Phase::Ended;
                    }
                }
                Phase::Draining => {
                    if self
                        .deadline
                        .is_some_and(|deadline| Instant::now() >= deadline)
                    {
                        self.fail_stream_duration();
                    } else {
                        self.accounting.settle(Status::Ok);
                        self.phase = Phase::Ended;
                    }
                }
                Phase::Ended => return None,
            }
        }
    }

    async fn poll_upstream(&mut self) {
        let (wait_deadline, terminal_bound) = match (self.deadline, self.terminal_deadline) {
            (Some(total), Some(terminal)) if terminal <= total => (Some(terminal), true),
            (Some(total), _) => (Some(total), false),
            (None, Some(terminal)) => (Some(terminal), true),
            (None, None) => (None, false),
        };
        let next = match wait_deadline {
            Some(deadline) => {
                match tokio::time::timeout_at(deadline.into(), self.bytes.next()).await {
                    Ok(next) => next,
                    Err(_) => {
                        if terminal_bound {
                            tracing::warn!(
                                provider = %self.accounting.ctx.target_provider,
                                model = %self.accounting.ctx.target_model,
                                grace_ms = self.stream_terminal_grace.as_millis(),
                                "byte-faithful upstream remained open after its terminal event until the post-terminal grace elapsed"
                            );
                            self.phase = Phase::Finished;
                            return;
                        }
                        if self.delivery == StreamDelivery::Passthrough && self.terminal_seen {
                            tracing::warn!(
                                provider = %self.accounting.ctx.target_provider,
                                model = %self.accounting.ctx.target_model,
                                "byte-faithful upstream remained open after its terminal event until the total stream bound"
                            );
                            self.phase = Phase::Finished;
                            return;
                        }
                        self.fail_stream_duration();
                        return;
                    }
                }
            }
            None => self.bytes.next().await,
        };
        match next {
            Some(Ok(chunk)) => {
                let Some(relayed_bytes) = self
                    .relayed_bytes
                    .checked_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX))
                else {
                    self.phase = Phase::Failed(STREAM_BYTES_EXCEEDED.to_owned());
                    return;
                };
                self.relayed_bytes = relayed_bytes;
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
                match self.delivery {
                    StreamDelivery::Passthrough => {
                        self.accounting.mark_downstream_first_token();
                        self.queued_downstream = true;
                        self.pending.push_back(chunk.clone());
                    }
                    StreamDelivery::PolicyValidatedPassthrough => {
                        if !self.reserve_rendered_bytes(chunk.len()) {
                            self.fail_rendered_bytes();
                            return;
                        }
                        self.buffered.push_back(chunk.clone());
                    }
                    StreamDelivery::Reemit | StreamDelivery::PolicyBuffered => {}
                }
                // The provider's terminal event is semantic, not necessarily
                // the HTTP body's final byte. Native extensions can follow it,
                // and a byte-faithful route must continue forwarding them until
                // transport EOF. They no longer affect decoding or accounting.
                if self.delivery == StreamDelivery::Passthrough && self.terminal_seen {
                    return;
                }
                let text = match self.decode_utf8(&chunk) {
                    Ok(text) => text,
                    Err(error) => {
                        self.phase = Phase::Failed(error.to_owned());
                        return;
                    }
                };
                let pushed = match self.sse.as_mut() {
                    Some(sse) if self.delivery == StreamDelivery::PolicyValidatedPassthrough => {
                        sse.push_strict(&text).map_err(|error| error.to_string())
                    }
                    Some(sse) => sse.push(&text).map_err(|error| error.to_string()),
                    None => Ok(Vec::new()),
                };
                match pushed {
                    Ok(events) => {
                        for event in events {
                            // OpenAI Responses has a semantic terminal event of
                            // its own. Some compatible providers append the
                            // Chat-style `[DONE]` sentinel, but that sentinel
                            // cannot stand in for `response.completed` when a
                            // policy must finalize before releasing bytes.
                            if self.finalization_required
                                && self.framing == Framing::Responses
                                && !self.terminal_seen
                                && is_terminal_sentinel(&event)
                            {
                                self.phase = Phase::Failed(
                                    "stream ended before its semantic terminal event".to_owned(),
                                );
                                return;
                            }
                            if self.delivery.is_policy_buffered() && self.terminal_seen {
                                if self.framing == Framing::Responses
                                    && is_terminal_sentinel(&event)
                                {
                                    continue;
                                }
                                self.phase = Phase::Failed(
                                    "stream carried bytes after its terminal event".to_owned(),
                                );
                                return;
                            }
                            match self.decoder.decode(event) {
                                Ok(decoded) => {
                                    self.emit(decoded).await;
                                    if !matches!(self.phase, Phase::Streaming) {
                                        return;
                                    }
                                    if self.delivery == StreamDelivery::Passthrough
                                        && self.terminal_seen
                                    {
                                        return;
                                    }
                                }
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
                                            // The rotation's opener error reaches no other
                                            // surface, so the operator keeps the whole
                                            // failure here; the caller gets the wording
                                            // every transport failure is given.
                                            tracing::warn!(
                                                provider = %self.accounting.ctx.target_provider,
                                                model = %self.accounting.ctx.target_model,
                                                error = %open_err,
                                                "upstream attempt failed on the transport"
                                            );
                                            self.phase =
                                                Phase::Failed(transport_caller_message(&open_err));
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
                    Err(err) => self.phase = Phase::Failed(err),
                }
            }
            Some(Err(err)) => {
                if self.delivery == StreamDelivery::Passthrough && self.terminal_seen {
                    // The semantic terminal event completed provider work. A
                    // proxy that times out or loses the transport while closing
                    // remains operator-visible, but is not an upstream timeout:
                    // the response was already served successfully.
                    if let Some(kind) = err.timeout_kind() {
                        tracing::warn!(
                            provider = %self.accounting.ctx.target_provider,
                            model = %self.accounting.ctx.target_model,
                            timeout = kind.label(),
                            "byte-faithful upstream timed out while closing after its terminal event"
                        );
                    } else {
                        tracing::warn!(
                            provider = %self.accounting.ctx.target_provider,
                            model = %self.accounting.ctx.target_model,
                            error = %err,
                            "byte-faithful upstream failed while closing after its terminal event"
                        );
                    }
                    self.phase = Phase::Finished;
                    return;
                }
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
                } else {
                    // The operator keeps the whole failure, endpoint included;
                    // the caller gets the wording below.
                    tracing::warn!(
                        provider = %self.accounting.ctx.target_provider,
                        model = %self.accounting.ctx.target_model,
                        committed = self.queued_downstream,
                        error = %err,
                        "open stream failed on the transport"
                    );
                }
                // In-band and caller-facing, so it is worded the way the
                // buffered path words it: never with the endpoint `reqwest`
                // rendered into its message.
                self.phase = Phase::Failed(transport_caller_message(&err));
            }
            None => self.finish_upstream().await,
        }
    }

    fn fail_stream_duration(&mut self) {
        tracing::warn!(
            provider = %self.accounting.ctx.target_provider,
            model = %self.accounting.ctx.target_model,
            committed = self.queued_downstream,
            "open stream exceeded the maximum stream duration"
        );
        self.phase = Phase::Failed(STREAM_DURATION_EXCEEDED.to_owned());
    }

    /// Chunk boundaries fall wherever the socket puts them, so a multi-byte
    /// character can straddle two chunks: only the valid prefix is decoded and
    /// the remainder waits for the next chunk. A policy-observed stream must be
    /// strict because validated byte-faithful delivery would otherwise inspect
    /// replacement text and release different, malformed source bytes. Ordinary
    /// passthrough retains the legacy lossy observer because the decoder does not
    /// govern those bytes.
    fn decode_utf8(&mut self, chunk: &[u8]) -> Result<String, &'static str> {
        self.carry.extend_from_slice(chunk);
        let strict = self.delivery.is_policy_buffered() || self.finalization_required;
        match std::str::from_utf8(&self.carry) {
            Ok(_) => {
                let text = String::from_utf8_lossy(&self.carry).into_owned();
                self.carry.clear();
                Ok(text)
            }
            Err(err) if err.error_len().is_none() => {
                let rest = self.carry.split_off(err.valid_up_to());
                let text = String::from_utf8_lossy(&self.carry).into_owned();
                self.carry = rest;
                Ok(text)
            }
            Err(_) if strict => {
                self.carry.clear();
                Err("stream contained invalid UTF-8")
            }
            Err(_) => {
                let text = String::from_utf8_lossy(&self.carry).into_owned();
                self.carry.clear();
                Ok(text)
            }
        }
    }

    /// An upstream that ends mid-event is a truncated answer, not a complete
    /// one: `SseDecoder::finish` reports the leftover so the caller gets an
    /// error rather than a `[DONE]` it would read as success.
    async fn finish_upstream(&mut self) {
        if self.delivery == StreamDelivery::Passthrough && self.terminal_seen {
            if self.finalization_required {
                self.phase = Phase::MiddlewareFailed(
                    "stream middleware finalization requires a validated delivery posture"
                        .to_owned(),
                );
                return;
            }
            // Once the provider's authoritative terminal event has been
            // observed, later bytes are opaque byte-faithful extensions. The
            // relay deliberately stops parsing them, so residual SSE or UTF-8
            // state cannot truthfully be interpreted as truncation at EOF.
            self.sse.take();
            self.carry.clear();
            self.phase = Phase::Finished;
            return;
        }
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
        // Decoder EOF is allowed to synthesize a terminal usage event for the
        // legacy relay, but policy-observed streams require the provider's own
        // semantic terminal event. Check before `decoder.finish()` so its
        // compatibility synthesis cannot turn a clean-but-nonterminal EOF into
        // successful finalization and release buffered content.
        if self.finalization_required && !self.terminal_seen {
            self.phase =
                Phase::Failed("stream ended before its semantic terminal event".to_owned());
            return;
        }
        match self.decoder.finish() {
            Ok(decoded) => {
                self.emit(decoded).await;
                if !matches!(self.phase, Phase::Streaming) {
                    return;
                }
                if self.finish_middleware().await {
                    self.phase = Phase::Finished;
                }
            }
            Err(err) => self.phase = Phase::Failed(err.to_string()),
        }
    }

    /// Finalize stream-event middleware after every provider-side success check
    /// has completed, but before policy-buffered bytes or a normal `[DONE]`
    /// marker can become caller-visible. The execution object independently
    /// enforces at-most-once invocation; this flag also avoids repeated calls as
    /// the relay moves through its terminal phases.
    async fn finish_middleware(&mut self) -> bool {
        if !self.finalization_required {
            return true;
        }
        let invoked = match self.deadline {
            Some(deadline) => tokio::time::timeout_at(
                deadline.into(),
                self.accounting.middleware_execution.finish_stream(),
            )
            .await
            .map_err(|_| ()),
            None => Ok(self.accounting.middleware_execution.finish_stream().await),
        };
        let result = match invoked {
            Ok(result) => result,
            Err(()) => {
                self.fail_stream_duration();
                return false;
            }
        };
        self.finalization_required = false;
        if let Err(error) = result {
            self.phase = Phase::MiddlewareFailed(error.to_string());
            return false;
        }
        true
    }

    /// Frame decoded events for the client. `Done` carries the stream's
    /// authoritative usage; the `[DONE]` sentinel itself is written once, from
    /// the terminal phase, so a provider that ends the connection without one
    /// still gets a well-formed close.
    async fn emit(&mut self, events: Vec<ProviderStreamEvent>) {
        for mut event in events {
            if self
                .deadline
                .is_some_and(|deadline| Instant::now() >= deadline)
            {
                self.fail_stream_duration();
                return;
            }
            match &event {
                ProviderStreamEvent::Data { data, .. } => {
                    if (self.delivery.is_policy_buffered()
                        || (self.delivery == StreamDelivery::Reemit && self.finalization_required))
                        && self.terminal_seen
                    {
                        self.phase = Phase::Failed(
                            "stream carried data after its terminal event".to_owned(),
                        );
                        return;
                    }
                    self.accounting.mark_upstream_first_token();
                    self.accounting.count_observed_output(data);
                    if self.delivery.is_policy_buffered() {
                        self.buffering_started.get_or_insert_with(Instant::now);
                    }
                }
                ProviderStreamEvent::Done(usage) => {
                    self.accounting.usage = *usage;
                    if self.delivery.is_policy_buffered()
                        || self.delivery == StreamDelivery::Passthrough
                        || (self.delivery == StreamDelivery::Reemit && self.finalization_required)
                    {
                        if self.terminal_seen {
                            self.phase = Phase::Failed(
                                "stream emitted more than one terminal event".to_owned(),
                            );
                            return;
                        }
                        self.terminal_seen = true;
                        if self.delivery == StreamDelivery::Passthrough {
                            self.terminal_deadline =
                                Some(Instant::now() + self.stream_terminal_grace);
                        }
                    } else {
                        self.phase = Phase::Finished;
                    }
                    continue;
                }
            }

            let invoked = match self.deadline {
                Some(deadline) => tokio::time::timeout_at(
                    deadline.into(),
                    self.accounting
                        .middleware_execution
                        .stream_event(&mut event),
                )
                .await
                .map_err(|_| ()),
                None => Ok(self
                    .accounting
                    .middleware_execution
                    .stream_event(&mut event)
                    .await),
            };
            let result = match invoked {
                Ok(result) => result,
                Err(()) => {
                    self.fail_stream_duration();
                    return;
                }
            };
            if let Err(error) = result {
                self.phase = Phase::MiddlewareFailed(error.to_string());
                return;
            }
            let ProviderStreamEvent::Data { event, data } = event else {
                // MiddlewareExecution owns the invariant that terminal usage
                // is never dispatched and a data event remains a data event.
                unreachable!("stream middleware changed a data event into terminal usage");
            };
            match self.delivery {
                StreamDelivery::Passthrough => {}
                StreamDelivery::Reemit => {
                    let rendered = data_event(event.as_deref(), &data);
                    if !self.reserve_rendered_bytes(rendered.len()) {
                        self.fail_rendered_bytes();
                        return;
                    }
                    self.accounting.mark_downstream_first_token();
                    self.queued_downstream = true;
                    self.pending.push_back(rendered);
                }
                StreamDelivery::PolicyBuffered => {
                    let rendered = data_event(event.as_deref(), &data);
                    if !self.reserve_rendered_bytes(rendered.len()) {
                        self.fail_rendered_bytes();
                        return;
                    }
                    self.buffered.push_back(rendered);
                }
                StreamDelivery::PolicyValidatedPassthrough => {}
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

/// OpenAI-compatible Responses implementations may emit both the semantic
/// `response.completed` event and the older data-only terminal sentinel. The
/// latter carries no content for middleware and is the sole event tolerated
/// after a policy-buffered terminal event; any named or data-bearing extension
/// still fails closed before buffered bytes are released.
fn is_terminal_sentinel(event: &SseEvent) -> bool {
    event.event.is_none() && event.data.trim() == "[DONE]"
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

fn middleware_error_event(message: &str) -> Bytes {
    let payload = json!({ "error": { "type": "middleware_stream_error", "message": message } });
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

fn native_middleware_error_event(message: &str) -> Bytes {
    let payload = json!({
        "type": "error",
        "error": { "type": "middleware_stream_error", "message": message }
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

fn responses_middleware_error_event(message: &str) -> Bytes {
    let payload = json!({
        "type": "error",
        "code": "middleware_stream_error",
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
/// spend (ADR 0010): the prompt it consumed plus decoded provider output observed
/// before termination. In an incremental posture that output was relayed; in an
/// explicit buffering posture it may still be held. A stream with no observed
/// provider output is charged nothing and its whole hold is released.
struct Accounting {
    state: AppState,
    ctx: StreamContext,
    started: Instant,
    usage: ModelUsage,
    /// Characters of generated provider text decoded so far, which is the only
    /// measure of output available before authoritative provider usage arrives.
    observed_output_chars: usize,
    carried_output_tokens: u64,
    /// The pinned chain generation and request-scope state. It is intentionally
    /// owned here rather than by the handler so cancellation and client hangup
    /// retain the same response-lifetime drop boundary as budget and admission
    /// accounting.
    middleware_execution: MiddlewareExecution,
    upstream_ttft_recorded: bool,
    /// Time to the first relayed token, which for a stream is the number a
    /// caller actually feels.
    ttft_ms: Option<u64>,
    settled: bool,
}

impl Accounting {
    #[allow(dead_code)]
    fn new(state: AppState, ctx: StreamContext, started: Instant) -> Self {
        Self::new_with_middleware(state, ctx, started, MiddlewareExecution::default())
    }

    fn new_with_middleware(
        state: AppState,
        ctx: StreamContext,
        started: Instant,
        middleware_execution: MiddlewareExecution,
    ) -> Self {
        Self {
            state,
            ctx,
            started,
            usage: ModelUsage::default(),
            observed_output_chars: 0,
            carried_output_tokens: 0,
            middleware_execution,
            upstream_ttft_recorded: false,
            ttft_ms: None,
            settled: false,
        }
    }

    fn mark_upstream_first_token(&mut self) {
        if !self.upstream_ttft_recorded {
            self.upstream_ttft_recorded = true;
            telemetry::metrics::record_upstream_ttft(
                &self.ctx.target_provider,
                &self.ctx.target_model,
                self.started.elapsed().as_secs_f64() * 1_000.0,
            );
        }
    }

    fn mark_downstream_first_token(&mut self) {
        self.ttft_ms
            .get_or_insert_with(|| self.started.elapsed().as_millis() as u64);
    }

    fn count_observed_output(&mut self, data: &Value) {
        self.observed_output_chars = self
            .observed_output_chars
            .saturating_add(relayed_text_len(data));
    }

    fn fold_attempt(&mut self) {
        const CHARS_PER_TOKEN: usize = 4;
        // Today the production decoders report usage only in terminal events,
        // so a permitted pre-content rotation carries no provider usage. Keep
        // this fallback for decoders that learn usage before rotation.
        let output = if self.usage.output_tokens > 0 {
            self.usage.output_tokens
        } else {
            self.observed_output_chars.div_ceil(CHARS_PER_TOKEN) as u64
        };
        self.carried_output_tokens = self.carried_output_tokens.saturating_add(output);
        self.usage = ModelUsage::default();
        self.observed_output_chars = 0;
    }

    /// The usage the request is charged for. The provider's own numbers win
    /// whenever they arrived; otherwise — a cancelled or broken stream — the
    /// charge is derived from provider output measurably decoded before the
    /// failure, and a stream that produced nothing is charged nothing.
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
            || self.observed_output_chars > 0;
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
                self.observed_output_chars.div_ceil(CHARS_PER_TOKEN) as u64
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
            request_id: self.ctx.identity.request_id.to_string(),
            trace_id: self.ctx.identity.trace_id.clone(),
            namespace: self.ctx.namespace.clone(),
            subject: self.ctx.subject.clone(),
            signer_kid: self.ctx.signer_kid.clone(),
            model: self.ctx.alias.clone(),
            target_provider: self.ctx.target_provider.clone(),
            target_model: self.ctx.target_model.clone(),
            credential_source: UsageRecord::credential_source_str(self.ctx.source),
            credential_id: self.ctx.credential_id.clone(),
            status,
            input_tokens: usage.input_tokens,
            cache_read_tokens: usage.cache_read_tokens,
            cache_write_tokens: usage.cache_write_tokens,
            output_tokens: usage.output_tokens,
            cost_microdollars: cost,
            catalog_version: self.ctx.price.catalog_version(),
            price_book: self.ctx.price.identity().map(|id| id.book()),
            price_book_checksum: self.ctx.price.identity().map(|id| id.checksum()),
            price_catalog: self.ctx.price.identity().map(|id| id.catalog()),
            latency_ms,
            attempts: self.ctx.attempts,
        };
        telemetry::record_streamed(&record, self.ttft_ms);
        let budget_key = self.ctx.budget_key.clone();
        let reservation = self.ctx.reservation.clone();
        let core_budget = self.middleware_execution.take_core_budget();
        spawn_settlement(async move {
            // The hold first, as on the buffered path: a stream cannot be
            // refused after it has been relayed, so the record cannot change
            // what the caller gets, while a durable append bounded by the
            // journal's operation timeout would put the charge behind a slow
            // outbox and let shutdown's settle share expire with the
            // reservation uncharged. The append is inside this tracked
            // settlement either way, so a caller hanging up does not cancel it.
            if let Some(hold) = core_budget {
                hold.settle(cost).await;
            } else {
                state.0.budget.settle(&budget_key, &reservation, cost).await;
            }
            state.0.usage.record_terminal(&record).await;
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
        DeterministicGuardrail, GuardrailAction, GuardrailRule, Middleware, MiddlewareDeclaration,
        MiddlewareError, MiddlewareOutcome, MiddlewarePhase, MiddlewareScope, MiddlewareSurface,
        NativeMessagesDecoder, OpenAiCompatibleAdapter, ProviderAdapter, ProviderError,
        ProviderRequest, Surface,
    };
    use http_body_util::BodyExt;
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
    use serde_json::json;
    use tower::util::ServiceExt;
    use tracing_subscriber::layer::SubscriberExt;

    use crate::admission::{AdmissionRejection, RequestKind};
    use crate::backends::catalog::ProviderId;
    use crate::budget::{Admission, BudgetStore, Denial};
    use crate::config::Config;
    use crate::desired_state::fixtures::approved_pricing_snapshot;
    use crate::middleware::{MiddlewareChain, MiddlewareRuntime};
    use crate::pricing::PriceIdentity;
    use crate::rate_limit::RateLimiter;
    use crate::routes::router;
    use crate::usage::identity::{RequestId, next_request_id};
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

    struct MiddlewareDropCounter(Arc<AtomicUsize>);

    struct SlowStreamMiddleware {
        declaration: MiddlewareDeclaration,
        delay: Duration,
    }

    struct FinalizingMiddleware {
        declaration: MiddlewareDeclaration,
        calls: Arc<AtomicUsize>,
        fail: bool,
    }

    struct BlockingStatefulFinalizer {
        declaration: MiddlewareDeclaration,
        calls: Arc<AtomicUsize>,
        active: Arc<AtomicUsize>,
        release: Arc<std::sync::atomic::AtomicBool>,
        drops: Arc<AtomicUsize>,
    }

    struct ReleaseFinalizer(Arc<std::sync::atomic::AtomicBool>);

    impl Drop for ReleaseFinalizer {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    impl Middleware for SlowStreamMiddleware {
        fn declaration(&self) -> &MiddlewareDeclaration {
            &self.declaration
        }

        fn apply(
            &self,
            phase: MiddlewarePhase<'_>,
            _state: Option<&mut gateway_core::MiddlewareState>,
        ) -> gateway_core::MiddlewareResult {
            if matches!(phase, MiddlewarePhase::StreamEvent(_)) {
                std::thread::sleep(self.delay);
            }
            Ok(MiddlewareOutcome::continue_without_state())
        }
    }

    impl Middleware for FinalizingMiddleware {
        fn declaration(&self) -> &MiddlewareDeclaration {
            &self.declaration
        }

        fn apply(
            &self,
            _phase: MiddlewarePhase<'_>,
            _state: Option<&mut gateway_core::MiddlewareState>,
        ) -> gateway_core::MiddlewareResult {
            Ok(MiddlewareOutcome::continue_without_state())
        }

        fn finish_stream(
            &self,
            _state: Option<&mut gateway_core::MiddlewareState>,
        ) -> Result<(), MiddlewareError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                Err(MiddlewareError::Failed)
            } else {
                Ok(())
            }
        }
    }

    impl Middleware for BlockingStatefulFinalizer {
        fn declaration(&self) -> &MiddlewareDeclaration {
            &self.declaration
        }

        fn apply(
            &self,
            phase: MiddlewarePhase<'_>,
            _state: Option<&mut gateway_core::MiddlewareState>,
        ) -> gateway_core::MiddlewareResult {
            if matches!(phase, MiddlewarePhase::Request(_)) {
                return Ok(MiddlewareOutcome::continue_with_state(
                    gateway_core::MiddlewareState::new(MiddlewareDropCounter(Arc::clone(
                        &self.drops,
                    ))),
                ));
            }
            Ok(MiddlewareOutcome::continue_without_state())
        }

        fn finish_stream(
            &self,
            state: Option<&mut gateway_core::MiddlewareState>,
        ) -> Result<(), MiddlewareError> {
            assert!(
                state
                    .and_then(|state| state.downcast_mut::<MiddlewareDropCounter>())
                    .is_some(),
                "relay finalizer receives request-lifetime state"
            );
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.active.fetch_add(1, Ordering::SeqCst);
            while !self.release.load(Ordering::Acquire) {
                std::thread::sleep(Duration::from_millis(1));
            }
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        }
    }

    impl Drop for MiddlewareDropCounter {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

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
                generation: None,
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

    fn state_for_with_rate_limit(base_url: &str, ledger: Arc<Ledger>) -> AppState {
        let sinks: Vec<Box<dyn UsageSink>> = vec![Box::new(LedgerSink(ledger.clone()))];
        AppState::new_with_rate_limiter(
            single_target_config(base_url),
            &test_env(),
            UsageFanout::new(sinks),
            Box::new(LedgerBudget(ledger)),
            Box::new(crate::rate_limit::InMemoryRateLimiter::new(1, 10)),
            Box::new(crate::revocation::NoDenylist),
        )
        .expect("rate-limited state")
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
            identity: EventIdentity {
                request_id: next_request_id(),
                trace_id: None,
            },
            price: RequestPrice::configured(gateway_core::ModelPrice {
                input_microdollars_per_million: 1_000_000,
                output_microdollars_per_million: 2_000_000,
                reasoning_microdollars_per_million: None,
                cache_read_microdollars_per_million: None,
                cache_write_microdollars_per_million: None,
            }),
            budget_key: BudgetKey {
                namespace: "platform".to_owned(),
                subject: "GW_TEST_INBOUND_KEY".to_owned(),
            },
            reservation: Reservation {
                id: "test".to_owned(),
                estimate_microdollars: 1_000,
                generation: None,
            },
            rate_limit_permit: None,
            admission_permit: None,
            estimated_input_tokens: 8,
            attempts: 1,
        }
    }

    #[tokio::test]
    async fn accounting_owns_middleware_state_until_the_response_owner_drops() {
        let dropped = Arc::new(AtomicUsize::new(0));
        let mut middleware_state = gateway_core::MiddlewareStateBag::new(1);
        middleware_state.insert(
            0,
            gateway_core::MiddlewareState::new(MiddlewareDropCounter(Arc::clone(&dropped))),
        );
        let state = state_for("http://127.0.0.1:1", Arc::new(Ledger::default()));
        let accounting = Accounting::new_with_middleware(
            state,
            context(),
            Instant::now(),
            MiddlewareExecution::from_state_bag_for_test(middleware_state),
        );
        assert_eq!(dropped.load(Ordering::SeqCst), 0);
        drop(accounting);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
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

    /// A stream settles at the pricing its request opened under, and says so:
    /// the row a stream writes carries the same book, checksum and catalogue as
    /// a buffered row, however many credentials the attempt walked through.
    #[tokio::test]
    async fn a_streamed_row_names_the_pricing_the_request_opened_under() {
        let pricing = approved_pricing_snapshot();
        let mut ctx = context();
        ctx.price = RequestPrice::approved(
            pricing
                .price(
                    &ProviderId::parse("openai").expect("a catalogue provider id"),
                    "gpt-4o",
                )
                .expect("the fixture book prices it"),
            PriceIdentity::of(&pricing),
        );

        let ledger = Arc::new(Ledger::default());
        let mut accounting = Accounting::new(
            state_for("http://127.0.0.1:1", ledger.clone()),
            ctx,
            Instant::now(),
        );
        accounting.settle(Status::Ok);
        let record = settled(&ledger).await;

        assert_eq!(
            record["catalog_version"],
            crate::desired_state::fixtures::catalog_version().get()
        );
        assert_ne!(record["catalog_version"], pricing.book().version.get());
        assert_eq!(record["price_book"], pricing.book().to_string());
        assert_eq!(
            record["price_book_checksum"],
            pricing.checksum().to_string()
        );
        assert_eq!(record["price_catalog"], pricing.catalog().to_string());
    }

    /// A deployment priced by its configuration file names no price book, so a
    /// row from it stays exactly the shape older readers already parse.
    #[tokio::test]
    async fn a_row_priced_by_configuration_names_no_price_book() {
        let ledger = Arc::new(Ledger::default());
        let mut accounting = Accounting::new(
            state_for("http://127.0.0.1:1", ledger.clone()),
            context(),
            Instant::now(),
        );
        accounting.settle(Status::Ok);
        let record = settled(&ledger).await;

        assert_eq!(record["catalog_version"], 0);
        assert!(record.get("price_book").is_none());
        assert!(record.get("price_book_checksum").is_none());
        assert!(record.get("price_catalog").is_none());
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

    #[test]
    fn rendered_limits_distinguish_empty_block_only_mutating_and_buffered_streams() {
        assert_eq!(
            rendered_stream_byte_limit(StreamDelivery::PolicyBuffered, true, None),
            Some(MAX_POLICY_BUFFERED_BYTES)
        );
        assert_eq!(
            rendered_stream_byte_limit(
                StreamDelivery::PolicyBuffered,
                true,
                Some(MAX_POLICY_BUFFERED_BYTES * 2),
            ),
            Some(MAX_POLICY_BUFFERED_BYTES)
        );
        assert_eq!(
            rendered_stream_byte_limit(StreamDelivery::PolicyBuffered, true, Some(1_024)),
            Some(1_024)
        );
        assert_eq!(
            rendered_stream_byte_limit(StreamDelivery::PolicyValidatedPassthrough, false, None,),
            Some(MAX_POLICY_BUFFERED_BYTES)
        );
        assert_eq!(
            rendered_stream_byte_limit(
                StreamDelivery::PolicyValidatedPassthrough,
                false,
                Some(MAX_POLICY_BUFFERED_BYTES * 2),
            ),
            Some(MAX_POLICY_BUFFERED_BYTES)
        );
        assert_eq!(
            rendered_stream_byte_limit(StreamDelivery::Passthrough, false, None),
            None
        );
        assert_eq!(
            rendered_stream_byte_limit(StreamDelivery::Reemit, false, Some(1_024)),
            Some(1_024)
        );
        assert_eq!(
            rendered_stream_byte_limit(StreamDelivery::Reemit, false, None),
            None,
            "an empty or block-only OpenAI chain must preserve disabled max_stream_bytes"
        );
        assert_eq!(
            rendered_stream_byte_limit(StreamDelivery::Reemit, true, None),
            Some(MAX_POLICY_BUFFERED_BYTES)
        );
        assert_eq!(
            rendered_stream_byte_limit(
                StreamDelivery::Reemit,
                true,
                Some(MAX_POLICY_BUFFERED_BYTES * 2),
            ),
            Some(MAX_POLICY_BUFFERED_BYTES)
        );
    }

    #[tokio::test]
    async fn stream_declarations_select_the_rendered_limit_without_reclassifying_validation_only() {
        let empty = StreamMiddleware::new(MiddlewareExecution::default(), StreamDelivery::Reemit);
        assert!(!empty.mutates_rendered_output);
        assert_eq!(
            rendered_stream_byte_limit(empty.delivery, empty.mutates_rendered_output, None,),
            None
        );

        let validation_only = StreamMiddleware::new(
            finalizing_execution(Arc::new(AtomicUsize::new(0)), false).await,
            StreamDelivery::Reemit,
        );
        assert!(!validation_only.mutates_rendered_output);
        assert_eq!(
            rendered_stream_byte_limit(
                validation_only.delivery,
                validation_only.mutates_rendered_output,
                None,
            ),
            None
        );

        let mut declaration =
            MiddlewareDeclaration::new("test.declared-mutator", [MiddlewareScope::StreamEvent]);
        declaration.mutates_response = true;
        let chain = MiddlewareChain::new(vec![Arc::new(FinalizingMiddleware {
            declaration,
            calls: Arc::new(AtomicUsize::new(0)),
            fail: false,
        }) as Arc<dyn Middleware>])
        .expect("declared mutator chain");
        let mut request = ProviderRequest {
            model: "gpt-4o".to_owned(),
            body: json!({}),
        };
        let execution = chain
            .start(&MiddlewareRuntime::default(), &mut request)
            .await
            .expect("declared mutator execution");
        let mutating = StreamMiddleware::new(execution, StreamDelivery::Reemit);
        assert!(mutating.mutates_rendered_output);
        assert_eq!(
            rendered_stream_byte_limit(mutating.delivery, mutating.mutates_rendered_output, None,),
            Some(MAX_POLICY_BUFFERED_BYTES)
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

    async fn finalizing_execution(calls: Arc<AtomicUsize>, fail: bool) -> MiddlewareExecution {
        let declaration =
            MiddlewareDeclaration::new("test.finalizer", [MiddlewareScope::StreamEvent]);
        let chain = MiddlewareChain::new(vec![Arc::new(FinalizingMiddleware {
            declaration,
            calls,
            fail,
        }) as Arc<dyn Middleware>])
        .expect("finalizing middleware chain");
        let mut request = ProviderRequest {
            model: "gpt-4o".to_owned(),
            body: json!({}),
        };
        chain
            .start(&MiddlewareRuntime::default(), &mut request)
            .await
            .expect("finalizing middleware execution")
    }

    async fn blocking_stateful_finalizing_execution(
        calls: Arc<AtomicUsize>,
        active: Arc<AtomicUsize>,
        release: Arc<std::sync::atomic::AtomicBool>,
        drops: Arc<AtomicUsize>,
    ) -> (MiddlewareExecution, MiddlewareRuntime) {
        let mut declaration = MiddlewareDeclaration::new(
            "test.blocking-stateful-finalizer",
            [MiddlewareScope::Request, MiddlewareScope::StreamEvent],
        );
        declaration.max_duration = Duration::from_secs(5);
        let chain = MiddlewareChain::new(vec![Arc::new(BlockingStatefulFinalizer {
            declaration,
            calls,
            active,
            release,
            drops,
        }) as Arc<dyn Middleware>])
        .expect("blocking stateful finalizer chain");
        let mut request = ProviderRequest {
            model: "gpt-4o".to_owned(),
            body: json!({}),
        };
        let runtime = MiddlewareRuntime::default();
        let execution = chain
            .start(&runtime, &mut request)
            .await
            .expect("blocking stateful finalizer execution");
        (execution, runtime)
    }

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
    async fn successful_stream_finalizes_once_after_eof_before_done() {
        let ledger = Arc::new(Ledger::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let response = relay_opened_with_middleware(
            state_for("http://127.0.0.1:1", ledger.clone()),
            context(),
            OpenedStream {
                decoder: OpenAiCompatibleAdapter::openai()
                    .stream_decoder(Surface::ChatCompletions)
                    .expect("decoder"),
                bytes: futures::stream::iter(vec![Ok(Bytes::from_static(
                    OPENAI_STREAM.as_bytes(),
                ))])
                .boxed(),
            },
            Instant::now(),
            Framing::OpenAiSse,
            None,
            StreamMiddleware::new(
                finalizing_execution(Arc::clone(&calls), false).await,
                StreamDelivery::Reemit,
            ),
        );

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.ends_with("data: [DONE]\n\n"), "{body}");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(settled(&ledger).await["status"], "ok");
    }

    #[tokio::test]
    async fn restored_reemit_amplification_refuses_atomically_before_queue() {
        let secret = "s".repeat(512);
        let mut declaration = MiddlewareDeclaration::new(
            "axond.redact",
            [
                MiddlewareScope::Request,
                MiddlewareScope::Response,
                MiddlewareScope::StreamEvent,
            ],
        );
        declaration.mutates_response = true;
        declaration.max_duration = Duration::from_secs(1);
        let guardrail = DeterministicGuardrail::compile(
            declaration,
            &[7_u8; 32],
            &[GuardrailRule {
                id: "large-secret".to_owned(),
                pattern: secret.clone(),
                action: GuardrailAction::Redact,
            }],
        )
        .expect("amplification guardrail");
        let chain = MiddlewareChain::new(vec![Arc::new(guardrail) as Arc<dyn Middleware>])
            .expect("guardrail chain");
        let mut request = ProviderRequest {
            model: "gpt-4o".to_owned(),
            body: json!({"messages": [{"role": "user", "content": secret.clone()}]}),
        };
        let runtime = MiddlewareRuntime::default();
        let execution = chain
            .start_with_protected_values(
                &runtime,
                &mut request,
                &[],
                MiddlewareSurface::ChatCompletions,
            )
            .await
            .expect("redacted request");
        let token = request.body["messages"][0]["content"]
            .as_str()
            .expect("placeholder")
            .to_owned();
        let upstream = format!(
            "data: {{\"choices\":[{{\"index\":0,\"delta\":{{\"content\":\"{token}{token}\"}}}}]}}\n\ndata: [DONE]\n\n"
        );

        let ledger = Arc::new(Ledger::default());
        let mut config = single_target_config("http://127.0.0.1:1");
        config.admission.max_stream_bytes = 256;
        let state = AppState::new(
            config,
            &test_env(),
            UsageFanout::new(vec![Box::new(LedgerSink(Arc::clone(&ledger)))]),
            Box::new(LedgerBudget(Arc::clone(&ledger))),
        )
        .expect("small downstream stream budget");
        let response = relay_opened_with_middleware(
            state,
            context(),
            OpenedStream {
                decoder: OpenAiCompatibleAdapter::openai()
                    .stream_decoder(Surface::ChatCompletions)
                    .expect("decoder"),
                bytes: futures::stream::iter(vec![Ok(Bytes::from(upstream))]).boxed(),
            },
            Instant::now(),
            Framing::OpenAiSse,
            None,
            StreamMiddleware::new(execution, StreamDelivery::Reemit),
        );

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("middleware_stream_error"), "{body}");
        assert!(body.contains(STREAM_BYTES_EXCEEDED), "{body}");
        assert!(!body.contains(&secret), "{body}");
        assert!(!body.contains(&token), "{body}");
        assert_eq!(settled(&ledger).await["status"], "rejected");
    }

    #[tokio::test]
    async fn reemit_finalizer_failure_keeps_safe_deltas_and_errors_before_done() {
        let ledger = Arc::new(Ledger::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let response = relay_opened_with_middleware(
            state_for("http://127.0.0.1:1", ledger.clone()),
            context(),
            OpenedStream {
                decoder: OpenAiCompatibleAdapter::openai()
                    .stream_decoder(Surface::ChatCompletions)
                    .expect("decoder"),
                bytes: futures::stream::iter(vec![Ok(Bytes::from_static(
                    OPENAI_STREAM.as_bytes(),
                ))])
                .boxed(),
            },
            Instant::now(),
            Framing::OpenAiSse,
            None,
            StreamMiddleware::new(
                finalizing_execution(Arc::clone(&calls), true).await,
                StreamDelivery::Reemit,
            ),
        );

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("\"content\":\"hel\""), "{body}");
        let error = body
            .find("middleware_stream_error")
            .expect("middleware error");
        let done = body.rfind("data: [DONE]").expect("done marker");
        assert!(error < done, "{body}");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(settled(&ledger).await["status"], "partial");
    }

    #[tokio::test]
    async fn buffered_finalizer_failure_releases_no_provider_content() {
        let ledger = Arc::new(Ledger::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let response = relay_opened_with_middleware(
            state_for("http://127.0.0.1:1", ledger.clone()),
            context(),
            OpenedStream {
                decoder: OpenAiCompatibleAdapter::openai()
                    .stream_decoder(Surface::ChatCompletions)
                    .expect("decoder"),
                bytes: futures::stream::iter(vec![Ok(Bytes::from_static(
                    OPENAI_STREAM.as_bytes(),
                ))])
                .boxed(),
            },
            Instant::now(),
            Framing::OpenAiSse,
            None,
            StreamMiddleware::new(
                finalizing_execution(Arc::clone(&calls), true).await,
                StreamDelivery::PolicyBuffered,
            ),
        );

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("middleware_stream_error"), "{body}");
        assert!(!body.contains("\"content\":\"hel\""), "{body}");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(settled(&ledger).await["status"], "rejected");
    }

    #[tokio::test]
    async fn truncated_stream_never_runs_success_finalization() {
        let ledger = Arc::new(Ledger::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let response = relay_opened_with_middleware(
            state_for("http://127.0.0.1:1", ledger.clone()),
            context(),
            OpenedStream {
                decoder: OpenAiCompatibleAdapter::openai()
                    .stream_decoder(Surface::ChatCompletions)
                    .expect("decoder"),
                bytes: futures::stream::iter(vec![Ok(Bytes::from_static(
                    b"data: {\"choices\":[{\"delta\":{\"content\":\"truncated\"}}]",
                ))])
                .boxed(),
            },
            Instant::now(),
            Framing::OpenAiSse,
            None,
            StreamMiddleware::new(
                finalizing_execution(Arc::clone(&calls), false).await,
                StreamDelivery::PolicyBuffered,
            ),
        );

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("upstream_stream_error"), "{body}");
        assert!(!body.contains("truncated"), "{body}");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(settled(&ledger).await["status"], "upstream_error");
    }

    #[tokio::test]
    async fn complete_nonterminal_eof_never_impersonates_successful_policy_finalization() {
        const CHAT: &str =
            "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hidden\"}}]}\n\n";
        const NATIVE: &str = concat!(
            "event: message_start\n",
            "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"usage\":{\"input_tokens\":1}}}\n\n",
            "event: content_block_start\n",
            "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
            "event: content_block_delta\n",
            "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hidden\"}}\n\n",
        );
        const RESPONSES: &str = concat!(
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hidden\"}\n\n",
        );
        const RESPONSES_DONE_SENTINEL: &str = concat!(
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hidden\"}\n\n",
            "data: [DONE]\n\n",
        );
        let cases: Vec<(
            &'static str,
            Framing,
            Box<dyn ProviderStreamDecoder>,
            &'static str,
        )> = vec![
            (
                "chat",
                Framing::OpenAiSse,
                OpenAiCompatibleAdapter::openai()
                    .stream_decoder(Surface::ChatCompletions)
                    .expect("Chat decoder"),
                CHAT,
            ),
            (
                "native",
                Framing::Native,
                Box::new(NativeMessagesDecoder::new()),
                NATIVE,
            ),
            (
                "responses-eof",
                Framing::Responses,
                OpenAiCompatibleAdapter::openai()
                    .stream_decoder(Surface::Responses)
                    .expect("Responses decoder"),
                RESPONSES,
            ),
            (
                "responses-done-sentinel",
                Framing::Responses,
                OpenAiCompatibleAdapter::openai()
                    .stream_decoder(Surface::Responses)
                    .expect("Responses decoder"),
                RESPONSES_DONE_SENTINEL,
            ),
        ];

        for (label, framing, decoder, wire) in cases {
            let ledger = Arc::new(Ledger::default());
            let calls = Arc::new(AtomicUsize::new(0));
            let response = relay_opened_with_middleware(
                state_for("http://127.0.0.1:1", ledger.clone()),
                context(),
                OpenedStream {
                    decoder,
                    bytes: futures::stream::iter(vec![Ok(Bytes::from_static(wire.as_bytes()))])
                        .boxed(),
                },
                Instant::now(),
                framing,
                None,
                StreamMiddleware::new(
                    finalizing_execution(Arc::clone(&calls), false).await,
                    StreamDelivery::PolicyBuffered,
                ),
            );

            let body = response.into_body().collect().await.unwrap().to_bytes();
            let body = String::from_utf8(body.to_vec()).unwrap();
            assert!(body.contains("upstream_stream_error"), "{label}: {body}");
            assert!(
                body.contains("stream ended before its semantic terminal event"),
                "{label}: {body}"
            );
            assert!(!body.contains("hidden"), "{label}: {body}");
            assert_eq!(calls.load(Ordering::SeqCst), 0, "{label}");
            assert_eq!(settled(&ledger).await["status"], "upstream_error");
        }
    }

    #[tokio::test]
    async fn responses_done_sentinel_cannot_finalize_validated_passthrough_policy() {
        const WIRE: &str = concat!(
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"visible-before-failure\"}\n\n",
            "data: [DONE]\n\n",
        );
        let ledger = Arc::new(Ledger::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let response = relay_opened_with_middleware(
            state_for("http://127.0.0.1:1", ledger.clone()),
            context(),
            OpenedStream {
                decoder: OpenAiCompatibleAdapter::openai()
                    .stream_decoder(Surface::Responses)
                    .expect("Responses decoder"),
                bytes: futures::stream::iter(vec![Ok(Bytes::from_static(WIRE.as_bytes()))]).boxed(),
            },
            Instant::now(),
            Framing::Responses,
            None,
            StreamMiddleware::new(
                finalizing_execution(Arc::clone(&calls), false).await,
                StreamDelivery::PolicyValidatedPassthrough,
            ),
        );

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(!body.contains("visible-before-failure"), "{body}");
        assert!(body.contains("upstream_stream_error"), "{body}");
        assert!(
            body.contains("stream ended before its semantic terminal event"),
            "{body}"
        );
        assert!(!body.contains("data: [DONE]"), "{body}");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(settled(&ledger).await["status"], "upstream_error");
    }

    #[tokio::test]
    async fn overall_stream_duration_includes_blocking_middleware_callbacks() {
        let ledger = Arc::new(Ledger::default());
        let mut config = single_target_config("http://127.0.0.1:1");
        config.admission.max_stream_duration_ms = 20;
        let state = AppState::new(
            config,
            &test_env(),
            UsageFanout::new(vec![Box::new(LedgerSink(ledger.clone()))]),
            Box::new(LedgerBudget(ledger.clone())),
        )
        .expect("duration-bound state");

        let mut declaration =
            MiddlewareDeclaration::new("test.slow-stream", [MiddlewareScope::StreamEvent]);
        declaration.max_duration = Duration::from_secs(1);
        let chain = MiddlewareChain::new(vec![Arc::new(SlowStreamMiddleware {
            declaration,
            delay: Duration::from_millis(200),
        }) as Arc<dyn Middleware>])
        .expect("slow stream chain");
        let mut request = ProviderRequest {
            model: "gpt-4o".to_owned(),
            body: json!({}),
        };
        let execution = chain
            .start(&MiddlewareRuntime::default(), &mut request)
            .await
            .expect("middleware execution");
        let started = Instant::now();
        let response = relay_opened_with_middleware(
            state,
            context(),
            OpenedStream {
                decoder: OpenAiCompatibleAdapter::openai()
                    .stream_decoder(Surface::ChatCompletions)
                    .expect("decoder"),
                bytes: futures::stream::iter(vec![Ok(Bytes::from_static(
                    b"data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"blocked\"}}]}\n\ndata: [DONE]\n\n",
                ))])
                .boxed(),
            },
            started,
            Framing::OpenAiSse,
            None,
            StreamMiddleware::new(execution, StreamDelivery::Reemit),
        );

        let body = tokio::time::timeout(Duration::from_millis(150), response.into_body().collect())
            .await
            .expect("overall duration stops the slow callback")
            .expect("stream body")
            .to_bytes();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains(STREAM_DURATION_EXCEEDED), "{body}");
        assert!(!body.contains("blocked"), "{body}");
        assert!(started.elapsed() < Duration::from_millis(150));
        assert_eq!(settled(&ledger).await["status"], "upstream_error");
    }

    #[tokio::test]
    async fn policy_buffered_partial_drain_settles_cancelled_once_and_releases_admission() {
        let ledger = Arc::new(Ledger::default());
        let mut config = single_target_config("http://127.0.0.1:1");
        config.admission.max_in_flight = 1;
        config.admission.max_in_flight_streams = 1;
        let state = AppState::new(
            config,
            &test_env(),
            UsageFanout::new(vec![Box::new(LedgerSink(ledger.clone()))]),
            Box::new(LedgerBudget(ledger.clone())),
        )
        .expect("single-slot state");
        let mut ctx = context();
        ctx.admission_permit = Some(
            state
                .0
                .admission
                .admit("platform", RequestKind::Streamed)
                .await
                .expect("first stream is admitted"),
        );
        let response = relay_opened_with_middleware(
            state.clone(),
            ctx,
            OpenedStream {
                decoder: OpenAiCompatibleAdapter::openai()
                    .stream_decoder(Surface::ChatCompletions)
                    .expect("decoder"),
                bytes: futures::stream::iter(vec![Ok(Bytes::from_static(
                    OPENAI_STREAM.as_bytes(),
                ))])
                .boxed(),
            },
            Instant::now(),
            Framing::OpenAiSse,
            None,
            StreamMiddleware::new(
                MiddlewareExecution::default(),
                StreamDelivery::PolicyBuffered,
            ),
        );

        let mut body = response.into_body().into_data_stream();
        let first = body
            .next()
            .await
            .expect("first buffered chunk")
            .expect("chunk");
        assert!(!first.is_empty());
        assert!(ledger.records.lock().expect("ledger").is_empty());
        assert!(matches!(
            state
                .0
                .admission
                .admit("platform", RequestKind::Streamed)
                .await,
            Err(AdmissionRejection::Global)
        ));

        drop(body);
        let record = settled(&ledger).await;
        assert_eq!(record["status"], "client_cancelled");
        assert_eq!(ledger.records.lock().expect("ledger").len(), 1);
        assert_eq!(ledger.settlements().len(), 1);
        let replacement = state
            .0
            .admission
            .admit("platform", RequestKind::Streamed)
            .await
            .expect("dropping the body releases admission");
        drop(replacement);
    }

    #[tokio::test]
    async fn dropping_body_during_policy_finalization_settles_and_drops_state_once() {
        let ledger = Arc::new(Ledger::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let _release_on_panic = ReleaseFinalizer(Arc::clone(&release));
        let (execution, runtime) = blocking_stateful_finalizing_execution(
            Arc::clone(&calls),
            Arc::clone(&active),
            Arc::clone(&release),
            Arc::clone(&drops),
        )
        .await;
        let response = relay_opened_with_middleware(
            state_for("http://127.0.0.1:1", Arc::clone(&ledger)),
            context(),
            OpenedStream {
                decoder: OpenAiCompatibleAdapter::openai()
                    .stream_decoder(Surface::ChatCompletions)
                    .expect("decoder"),
                bytes: futures::stream::iter(vec![Ok(Bytes::from_static(
                    OPENAI_STREAM.as_bytes(),
                ))])
                .boxed(),
            },
            Instant::now(),
            Framing::OpenAiSse,
            None,
            StreamMiddleware::new(execution, StreamDelivery::PolicyBuffered),
        );

        let mut body = response.into_body().into_data_stream();
        let polling = tokio::spawn(async move { body.next().await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while active.load(Ordering::Acquire) == 0 {
                assert!(
                    !polling.is_finished(),
                    "policy-buffered content escaped before finalization"
                );
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("blocking finalizer becomes active");
        assert!(
            !polling.is_finished(),
            "policy-buffered provider content escaped during finalization"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert!(ledger.records.lock().expect("ledger").is_empty());

        polling.abort();
        assert!(
            polling
                .await
                .expect_err("body poll is cancelled")
                .is_cancelled()
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            while runtime.abandoned_for_test("test.blocking-stateful-finalizer") != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled finalizer is tracked as abandoned");
        let record = settled(&ledger).await;
        assert_eq!(record["status"], "client_cancelled");
        assert_eq!(ledger.records.lock().expect("ledger").len(), 1);
        assert_eq!(ledger.settlements().len(), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 0);

        release.store(true, Ordering::Release);
        tokio::time::timeout(Duration::from_secs(1), async {
            while active.load(Ordering::Acquire) != 0
                || drops.load(Ordering::Acquire) != 1
                || runtime.abandoned_for_test("test.blocking-stateful-finalizer") != 0
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("abandoned finalizer exits and drops request state");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn policy_validated_partial_drain_settles_cancelled_once() {
        const FIRST: &str = concat!(
            "event: message_start\n",
            "data: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":3}}}\n\n",
        );
        const LAST: &str = concat!(
            "event: message_delta\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{}}\n\n",
            "event: message_stop\n",
            "data: {\"type\":\"message_stop\"}\n\n",
        );
        let ledger = Arc::new(Ledger::default());
        let response = relay_opened_with_middleware(
            state_for("http://127.0.0.1:1", ledger.clone()),
            context(),
            OpenedStream {
                decoder: Box::new(NativeMessagesDecoder::new()),
                bytes: futures::stream::iter(vec![
                    Ok(Bytes::from_static(FIRST.as_bytes())),
                    Ok(Bytes::from_static(LAST.as_bytes())),
                ])
                .boxed(),
            },
            Instant::now(),
            Framing::Native,
            None,
            StreamMiddleware::new(
                MiddlewareExecution::default(),
                StreamDelivery::PolicyValidatedPassthrough,
            ),
        );

        let mut body = response.into_body().into_data_stream();
        assert_eq!(
            body.next().await.expect("first raw chunk").expect("chunk"),
            Bytes::from_static(FIRST.as_bytes())
        );
        assert!(ledger.records.lock().expect("ledger").is_empty());
        drop(body);
        let record = settled(&ledger).await;
        assert_eq!(record["status"], "client_cancelled");
        assert_eq!(ledger.records.lock().expect("ledger").len(), 1);
        assert_eq!(ledger.settlements().len(), 1);
    }

    #[tokio::test]
    async fn byte_faithful_passthrough_relays_tail_bytes_through_transport_eof() {
        const NATIVE_TERMINAL: &str = concat!(
            "event: message_start\n",
            "data: {\"type\":\"message_start\",\"message\":{\"usage\":{}}}\n\n",
            "event: message_delta\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{}}\n\n",
            "event: message_stop\n",
            "data: {\"type\":\"message_stop\"}\n\n",
        );
        const NATIVE_TAIL: &str = ": provider-extension-after-stop\n\n";
        const RESPONSES_TERMINAL: &str = concat!(
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":3,\"output_tokens\":1}}}\n\n",
        );
        const RESPONSES_TAIL: &str = concat!(
            "event: provider.extension\n",
            "data: {\"type\":\"provider.extension\",\"opaque\":true}\n\n",
        );
        let cases: Vec<(
            Framing,
            Box<dyn ProviderStreamDecoder>,
            &'static str,
            &'static str,
        )> = vec![
            (
                Framing::Native,
                Box::new(NativeMessagesDecoder::new()),
                NATIVE_TERMINAL,
                NATIVE_TAIL,
            ),
            (
                Framing::Responses,
                OpenAiCompatibleAdapter::openai()
                    .stream_decoder(Surface::Responses)
                    .expect("Responses decoder"),
                RESPONSES_TERMINAL,
                RESPONSES_TAIL,
            ),
        ];

        for (framing, decoder, terminal, tail) in cases {
            let ledger = Arc::new(Ledger::default());
            let response = relay_opened_with_middleware(
                state_for("http://127.0.0.1:1", ledger.clone()),
                context(),
                OpenedStream {
                    decoder,
                    bytes: futures::stream::iter(vec![
                        Ok(Bytes::from_static(terminal.as_bytes())),
                        Ok(Bytes::from_static(tail.as_bytes())),
                    ])
                    .boxed(),
                },
                Instant::now(),
                framing,
                None,
                StreamMiddleware::new(MiddlewareExecution::default(), StreamDelivery::Passthrough),
            );
            let body = response.into_body().collect().await.unwrap().to_bytes();
            assert_eq!(body, Bytes::from(format!("{terminal}{tail}")));
            assert_eq!(settled(&ledger).await["status"], "ok");
        }
    }

    #[tokio::test]
    async fn byte_faithful_terminal_with_incomplete_tail_ends_cleanly_at_eof() {
        const TERMINAL: &str = concat!(
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":3,\"output_tokens\":1}}}\n\n",
        );
        let mut wire = TERMINAL.as_bytes().to_vec();
        wire.extend_from_slice(b"event: provider.extension\ndata: ");
        wire.push(0xf0); // first byte of a four-byte UTF-8 sequence

        let ledger = Arc::new(Ledger::default());
        let response = relay_opened_with_middleware(
            state_for("http://127.0.0.1:1", ledger.clone()),
            context(),
            OpenedStream {
                decoder: OpenAiCompatibleAdapter::openai()
                    .stream_decoder(Surface::Responses)
                    .expect("Responses decoder"),
                bytes: futures::stream::iter(vec![Ok(Bytes::from(wire.clone()))]).boxed(),
            },
            Instant::now(),
            Framing::Responses,
            None,
            StreamMiddleware::new(MiddlewareExecution::default(), StreamDelivery::Passthrough),
        );

        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body.as_ref(), wire.as_slice());
        assert_eq!(settled(&ledger).await["status"], "ok");
    }

    #[tokio::test]
    async fn policy_buffered_responses_tolerate_done_after_response_completed() {
        const COMPLETED: &str = concat!(
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":3,\"output_tokens\":1}}}\n\n",
        );
        const SENTINEL: &str = "data: [DONE]\n\n";

        for delivery in [
            StreamDelivery::PolicyBuffered,
            StreamDelivery::PolicyValidatedPassthrough,
        ] {
            let ledger = Arc::new(Ledger::default());
            let response = relay_opened_with_middleware(
                state_for("http://127.0.0.1:1", ledger.clone()),
                context(),
                OpenedStream {
                    decoder: OpenAiCompatibleAdapter::openai()
                        .stream_decoder(Surface::Responses)
                        .expect("Responses decoder"),
                    bytes: futures::stream::iter(vec![
                        Ok(Bytes::from_static(COMPLETED.as_bytes())),
                        Ok(Bytes::from_static(SENTINEL.as_bytes())),
                    ])
                    .boxed(),
                },
                Instant::now(),
                Framing::Responses,
                None,
                StreamMiddleware::new(MiddlewareExecution::default(), delivery),
            );

            let body = response.into_body().collect().await.unwrap().to_bytes();
            let body = String::from_utf8(body.to_vec()).unwrap();
            assert!(body.contains("response.completed"), "{body}");
            assert!(!body.contains("upstream_stream_error"), "{body}");
            if delivery == StreamDelivery::PolicyValidatedPassthrough {
                assert_eq!(body, format!("{COMPLETED}{SENTINEL}"));
            }
            assert_eq!(settled(&ledger).await["status"], "ok");
        }
    }

    #[tokio::test]
    async fn policy_observed_streams_reject_invalid_utf8_without_releasing_source_bytes() {
        for delivery in [
            StreamDelivery::PolicyBuffered,
            StreamDelivery::PolicyValidatedPassthrough,
        ] {
            let ledger = Arc::new(Ledger::default());
            let response = relay_opened_with_middleware(
                state_for("http://127.0.0.1:1", ledger.clone()),
                context(),
                OpenedStream {
                    decoder: Box::new(NativeMessagesDecoder::new()),
                    bytes: futures::stream::iter(vec![Ok(Bytes::from_static(
                        b"event: message_start\ndata: \xff\n\n",
                    ))])
                    .boxed(),
                },
                Instant::now(),
                Framing::Native,
                None,
                StreamMiddleware::new(MiddlewareExecution::default(), delivery),
            );

            let body = response.into_body().collect().await.unwrap().to_bytes();
            assert!(
                !body.as_ref().contains(&0xff),
                "malformed source byte leaked"
            );
            let body = String::from_utf8(body.to_vec()).expect("typed error is UTF-8");
            assert!(body.contains("upstream_stream_error"), "{body}");
            assert!(body.contains("stream contained invalid UTF-8"), "{body}");
            assert_eq!(settled(&ledger).await["status"], "upstream_error");
        }
    }

    #[tokio::test]
    async fn byte_faithful_terminal_transport_failure_does_not_retract_completion() {
        const COMPLETED: &str = concat!(
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":3,\"output_tokens\":1}}}\n\n",
        );
        let failures = [
            TransportError::Http("proxy held the completed body open".to_owned()),
            TransportError::Timeout {
                kind: gateway_transport::TimeoutKind::StreamIdle,
                bound: gateway_transport::TimeoutBound::Phase,
                budget_ms: 10,
            },
        ];
        for failure in failures {
            let ledger = Arc::new(Ledger::default());
            let response = relay_opened_with_middleware(
                state_for("http://127.0.0.1:1", ledger.clone()),
                context(),
                OpenedStream {
                    decoder: OpenAiCompatibleAdapter::openai()
                        .stream_decoder(Surface::Responses)
                        .expect("Responses decoder"),
                    bytes: futures::stream::iter(vec![
                        Ok(Bytes::from_static(COMPLETED.as_bytes())),
                        Err(failure),
                    ])
                    .boxed(),
                },
                Instant::now(),
                Framing::Responses,
                None,
                StreamMiddleware::new(MiddlewareExecution::default(), StreamDelivery::Passthrough),
            );

            let body = response.into_body().collect().await.unwrap().to_bytes();
            assert_eq!(body, Bytes::from_static(COMPLETED.as_bytes()));
            assert_eq!(settled(&ledger).await["status"], "ok");
        }
    }

    #[tokio::test]
    async fn byte_faithful_terminal_open_body_ends_cleanly_at_total_bound() {
        const COMPLETED: &str = concat!(
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":3,\"output_tokens\":1}}}\n\n",
        );
        let ledger = Arc::new(Ledger::default());
        let mut config = single_target_config("http://127.0.0.1:1");
        config.admission.max_stream_duration_ms = 30;
        let state = AppState::new(
            config,
            &test_env(),
            UsageFanout::new(vec![Box::new(LedgerSink(ledger.clone()))]),
            Box::new(LedgerBudget(ledger.clone())),
        )
        .expect("duration-bound state");
        let bytes = futures::stream::once(async {
            Ok::<_, TransportError>(Bytes::from_static(COMPLETED.as_bytes()))
        })
        .chain(futures::stream::pending())
        .boxed();
        let response = relay_opened_with_middleware(
            state,
            context(),
            OpenedStream {
                decoder: OpenAiCompatibleAdapter::openai()
                    .stream_decoder(Surface::Responses)
                    .expect("Responses decoder"),
                bytes,
            },
            Instant::now(),
            Framing::Responses,
            None,
            StreamMiddleware::new(MiddlewareExecution::default(), StreamDelivery::Passthrough),
        );

        let body = tokio::time::timeout(Duration::from_millis(150), response.into_body().collect())
            .await
            .expect("total stream bound closes a completed body")
            .unwrap()
            .to_bytes();
        assert_eq!(body, Bytes::from_static(COMPLETED.as_bytes()));
        assert_eq!(settled(&ledger).await["status"], "ok");
    }

    #[tokio::test]
    async fn byte_faithful_terminal_grace_closes_body_and_releases_admission() {
        const COMPLETED: &str = concat!(
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":3,\"output_tokens\":1}}}\n\n",
        );
        let ledger = Arc::new(Ledger::default());
        let mut config = single_target_config("http://127.0.0.1:1");
        config.admission.max_in_flight = 1;
        config.admission.max_in_flight_streams = 1;
        config.admission.max_stream_duration_ms = 0;
        config.transport.stream_terminal_grace_ms = 25;
        let state = AppState::new(
            config,
            &test_env(),
            UsageFanout::new(vec![Box::new(LedgerSink(ledger.clone()))]),
            Box::new(LedgerBudget(ledger.clone())),
        )
        .expect("terminal-grace state");
        let mut ctx = context();
        ctx.admission_permit = Some(
            state
                .0
                .admission
                .admit("platform", RequestKind::Streamed)
                .await
                .expect("completed stream is admitted"),
        );
        let bytes = futures::stream::once(async {
            Ok::<_, TransportError>(Bytes::from_static(COMPLETED.as_bytes()))
        })
        .chain(futures::stream::pending())
        .boxed();
        let response = relay_opened_with_middleware(
            state.clone(),
            ctx,
            OpenedStream {
                decoder: OpenAiCompatibleAdapter::openai()
                    .stream_decoder(Surface::Responses)
                    .expect("Responses decoder"),
                bytes,
            },
            Instant::now(),
            Framing::Responses,
            None,
            StreamMiddleware::new(MiddlewareExecution::default(), StreamDelivery::Passthrough),
        );

        let body = tokio::time::timeout(Duration::from_millis(150), response.into_body().collect())
            .await
            .expect("post-terminal grace closes an otherwise open body")
            .unwrap()
            .to_bytes();
        assert_eq!(body, Bytes::from_static(COMPLETED.as_bytes()));
        assert_eq!(settled(&ledger).await["status"], "ok");

        let replacement = state
            .0
            .admission
            .admit("platform", RequestKind::Streamed)
            .await
            .expect("terminal grace returns request and stream capacity");
        drop(replacement);
    }

    #[tokio::test]
    async fn incremental_completion_is_not_relabelled_while_the_caller_drains() {
        let ledger = Arc::new(Ledger::default());
        let mut config = single_target_config("http://127.0.0.1:1");
        config.admission.max_stream_duration_ms = 200;
        let state = AppState::new(
            config,
            &test_env(),
            UsageFanout::new(vec![Box::new(LedgerSink(ledger.clone()))]),
            Box::new(LedgerBudget(ledger.clone())),
        )
        .expect("duration-bound state");
        let response = relay_opened_with_middleware(
            state,
            context(),
            OpenedStream {
                decoder: OpenAiCompatibleAdapter::openai()
                    .stream_decoder(Surface::ChatCompletions)
                    .expect("chat decoder"),
                bytes: futures::stream::iter(vec![Ok(Bytes::from_static(
                    OPENAI_STREAM.as_bytes(),
                ))])
                .boxed(),
            },
            Instant::now(),
            Framing::OpenAiSse,
            None,
            StreamMiddleware::new(MiddlewareExecution::default(), StreamDelivery::Reemit),
        );

        let mut body = response.into_body().into_data_stream();
        for _ in 0..3 {
            body.next()
                .await
                .expect("decoded data event")
                .expect("chunk");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
        let mut remainder = String::new();
        while let Some(chunk) = body.next().await {
            remainder.push_str(&String::from_utf8_lossy(&chunk.expect("chunk")));
        }
        assert_eq!(remainder, "data: [DONE]\n\n");
        assert_eq!(settled(&ledger).await["status"], "ok");
    }

    #[tokio::test]
    async fn policy_validated_passthrough_rejects_duplicate_json_keys_without_leaking() {
        const AMBIGUOUS: &str = concat!(
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"safe\",\"delta\":\"evil-duplicate\"}\n\n",
        );
        let ledger = Arc::new(Ledger::default());
        let response = relay_opened_with_middleware(
            state_for("http://127.0.0.1:1", ledger.clone()),
            context(),
            OpenedStream {
                decoder: OpenAiCompatibleAdapter::openai()
                    .stream_decoder(Surface::Responses)
                    .expect("Responses decoder"),
                bytes: futures::stream::iter(vec![Ok(Bytes::from_static(AMBIGUOUS.as_bytes()))])
                    .boxed(),
            },
            Instant::now(),
            Framing::Responses,
            None,
            StreamMiddleware::new(
                MiddlewareExecution::default(),
                StreamDelivery::PolicyValidatedPassthrough,
            ),
        );

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("upstream_stream_error"), "{body}");
        assert!(body.contains("duplicate JSON object keys"), "{body}");
        assert!(!body.contains("evil-duplicate"), "{body}");
        assert_eq!(settled(&ledger).await["status"], "upstream_error");
    }

    #[tokio::test]
    async fn policy_validated_passthrough_rejects_bytes_after_terminal_events() {
        const NATIVE_TRAILING: &str = concat!(
            "event: message_start\n",
            "data: {\"type\":\"message_start\",\"message\":{\"usage\":{}}}\n\n",
            "event: message_delta\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{}}\n\n",
            "event: message_stop\n",
            "data: {\"type\":\"message_stop\"}\n\n",
            "event: content_block_delta\n",
            "data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"evil-native\"}}\n\n",
        );
        const RESPONSES_TRAILING: &str = concat!(
            "event: response.completed\n",
            "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"usage\":{\"input_tokens\":3,\"output_tokens\":1}}}\n\n",
            "event: response.output_text.delta\n",
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"evil-responses\"}\n\n",
        );
        const COMMENT_TRAILING: &str = concat!(
            "event: message_start\n",
            "data: {\"type\":\"message_start\",\"message\":{\"usage\":{}}}\n\n",
            "event: message_delta\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{}}\n\n",
            "event: message_stop\n",
            "data: {\"type\":\"message_stop\"}\n\n",
            ": evil-comment\n\n",
        );
        const NO_DATA_TRAILING: &str = concat!(
            "event: message_start\n",
            "data: {\"type\":\"message_start\",\"message\":{\"usage\":{}}}\n\n",
            "event: message_delta\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{}}\n\n",
            "event: message_stop\n",
            "data: {\"type\":\"message_stop\"}\n\n",
            "event: opaque\n",
            "id: evil-no-data\n\n",
        );
        const MIXED_TERMINAL_METADATA: &str = concat!(
            "event: message_start\n",
            "data: {\"type\":\"message_start\",\"message\":{\"usage\":{}}}\n\n",
            "event: message_delta\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{}}\n\n",
            "event: message_stop\n",
            ": evil-mixed-terminal\n",
            "data: {\"type\":\"message_stop\"}\n\n",
        );
        const NATIVE_SENTINEL: &str = concat!(
            "event: message_start\n",
            "data: {\"type\":\"message_start\",\"message\":{\"usage\":{}}}\n\n",
            "event: message_delta\n",
            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{}}\n\n",
            "event: message_stop\n",
            "data: {\"type\":\"message_stop\"}\n\n",
            "data: [DONE]\n\n",
        );
        let cases: Vec<(
            Framing,
            Box<dyn ProviderStreamDecoder>,
            &'static str,
            &'static str,
        )> = vec![
            (
                Framing::Native,
                Box::new(NativeMessagesDecoder::new()),
                NATIVE_TRAILING,
                "evil-native",
            ),
            (
                Framing::Responses,
                OpenAiCompatibleAdapter::openai()
                    .stream_decoder(Surface::Responses)
                    .expect("Responses decoder"),
                RESPONSES_TRAILING,
                "evil-responses",
            ),
            (
                Framing::Native,
                Box::new(NativeMessagesDecoder::new()),
                COMMENT_TRAILING,
                "evil-comment",
            ),
            (
                Framing::Native,
                Box::new(NativeMessagesDecoder::new()),
                NO_DATA_TRAILING,
                "evil-no-data",
            ),
            (
                Framing::Native,
                Box::new(NativeMessagesDecoder::new()),
                MIXED_TERMINAL_METADATA,
                "evil-mixed-terminal",
            ),
            (
                Framing::Native,
                Box::new(NativeMessagesDecoder::new()),
                NATIVE_SENTINEL,
                "[DONE]",
            ),
        ];

        for (framing, decoder, upstream, forbidden) in cases {
            let ledger = Arc::new(Ledger::default());
            let response = relay_opened_with_middleware(
                state_for("http://127.0.0.1:1", ledger.clone()),
                context(),
                OpenedStream {
                    decoder,
                    bytes: futures::stream::iter(vec![Ok(Bytes::from_static(upstream.as_bytes()))])
                        .boxed(),
                },
                Instant::now(),
                framing,
                None,
                StreamMiddleware::new(
                    MiddlewareExecution::default(),
                    StreamDelivery::PolicyValidatedPassthrough,
                ),
            );
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let body = String::from_utf8(body.to_vec()).unwrap();
            assert!(body.contains("upstream_stream_error"), "{body}");
            assert!(!body.contains(forbidden), "{body}");
            assert_eq!(settled(&ledger).await["status"], "upstream_error");
        }
    }

    #[tokio::test]
    async fn policy_buffered_drain_stops_at_the_total_stream_deadline() {
        let ledger = Arc::new(Ledger::default());
        let mut config = single_target_config("http://127.0.0.1:1");
        config.admission.max_stream_duration_ms = 30;
        let state = AppState::new(
            config,
            &test_env(),
            UsageFanout::new(vec![Box::new(LedgerSink(ledger.clone()))]),
            Box::new(LedgerBudget(ledger.clone())),
        )
        .expect("duration-bound state");
        let response = relay_opened_with_middleware(
            state,
            context(),
            OpenedStream {
                decoder: OpenAiCompatibleAdapter::openai()
                    .stream_decoder(Surface::ChatCompletions)
                    .expect("decoder"),
                bytes: futures::stream::iter(vec![Ok(Bytes::from_static(
                    OPENAI_STREAM.as_bytes(),
                ))])
                .boxed(),
            },
            Instant::now(),
            Framing::OpenAiSse,
            None,
            StreamMiddleware::new(
                MiddlewareExecution::default(),
                StreamDelivery::PolicyBuffered,
            ),
        );

        let mut body = response.into_body().into_data_stream();
        let first = body
            .next()
            .await
            .expect("first buffered event")
            .expect("chunk");
        assert!(String::from_utf8_lossy(&first).contains("hel"));
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut remainder = String::new();
        while let Some(chunk) = body.next().await {
            remainder.push_str(&String::from_utf8_lossy(&chunk.expect("chunk")));
        }
        assert!(remainder.contains(STREAM_DURATION_EXCEEDED), "{remainder}");
        assert!(!remainder.contains("\"content\":\"lo\""), "{remainder}");
        assert_eq!(settled(&ledger).await["status"], "upstream_error");
    }

    #[tokio::test]
    async fn policy_buffered_final_drain_transition_honors_the_total_deadline() {
        let ledger = Arc::new(Ledger::default());
        let mut config = single_target_config("http://127.0.0.1:1");
        config.admission.max_stream_duration_ms = 30;
        let state = AppState::new(
            config,
            &test_env(),
            UsageFanout::new(vec![Box::new(LedgerSink(ledger.clone()))]),
            Box::new(LedgerBudget(ledger.clone())),
        )
        .expect("duration-bound state");
        let response = relay_opened_with_middleware(
            state,
            context(),
            OpenedStream {
                decoder: OpenAiCompatibleAdapter::openai()
                    .stream_decoder(Surface::ChatCompletions)
                    .expect("decoder"),
                bytes: futures::stream::iter(vec![Ok(Bytes::from_static(
                    OPENAI_STREAM.as_bytes(),
                ))])
                .boxed(),
            },
            Instant::now(),
            Framing::OpenAiSse,
            None,
            StreamMiddleware::new(
                MiddlewareExecution::default(),
                StreamDelivery::PolicyBuffered,
            ),
        );

        let mut body = response.into_body().into_data_stream();
        for _ in 0..4 {
            body.next()
                .await
                .expect("all buffered frames precede completion")
                .expect("chunk");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        let remainder = body
            .next()
            .await
            .expect("expired final transition emits a terminal error")
            .expect("chunk");
        assert!(
            String::from_utf8_lossy(&remainder).contains(STREAM_DURATION_EXCEEDED),
            "{remainder:?}"
        );
        while body.next().await.is_some() {}
        assert_eq!(settled(&ledger).await["status"], "upstream_error");
    }

    /// A streamed request settles under the identity the handler minted, not one
    /// invented in the detached settlement task.
    #[tokio::test]
    async fn a_streamed_request_settles_under_a_parseable_event_identity() {
        let ledger = Arc::new(Ledger::default());
        let base_url = upstream_serving(OPENAI_STREAM).await;
        let resp = router(state_for(&base_url, ledger.clone()))
            .oneshot(stream_request())
            .await
            .expect("response");
        let mut body = resp.into_body().into_data_stream();
        while body.next().await.is_some() {}

        let record = settled(&ledger).await;
        let id = record["request_id"].as_str().expect("a request id");
        RequestId::parse(id).unwrap_or_else(|e| panic!("`{id}` is not an event identity: {e}"));
    }

    /// Every way a stream can end settles the *same* event: the id belongs to the
    /// request, so a cancelled stream and a completed one are one billable fact
    /// each, both nameable by whatever else logged the request.
    #[tokio::test]
    async fn a_cancelled_stream_settles_the_identity_the_handler_minted() {
        let ledger = Arc::new(Ledger::default());
        let ctx = context();
        let minted = ctx.identity.request_id;
        let accounting = Accounting::new(
            state_for("http://127.0.0.1:1", ledger.clone()),
            ctx,
            Instant::now(),
        );
        // Dropped without settling: the caller went away mid-stream.
        drop(accounting);

        let record = settled(&ledger).await;
        assert_eq!(record["status"], "client_cancelled");
        assert_eq!(record["request_id"], minted.to_string());
    }

    /// A walk that never opened a stream still settles one record, under the same
    /// identity the request was admitted with.
    #[tokio::test]
    async fn a_stream_that_never_opened_settles_the_same_identity() {
        let ledger = Arc::new(Ledger::default());
        let ctx = context();
        let minted = ctx.identity.request_id;
        settle_upstream_error(
            state_for("http://127.0.0.1:1", ledger.clone()),
            ctx,
            Instant::now(),
        );

        let record = settled(&ledger).await;
        assert_eq!(record["status"], "upstream_error");
        assert_eq!(record["request_id"], minted.to_string());
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
        "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"thinking\",\"thinking\":\"\",\"signature\":\"\"}}\n\n",
        "event: content_block_delta\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"hi\"}}\n\n",
        "event: content_block_delta\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"signature_delta\",\"signature\":\"sig-1\"}}\n\n",
        "event: content_block_stop\n",
        "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
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
    async fn a_mid_stream_transport_failure_does_not_name_the_endpoint_it_failed_against() {
        let ledger = Arc::new(Ledger::default());
        let response = relay_opened(
            state_for("http://127.0.0.1:1", ledger.clone()),
            context(),
            OpenAiCompatibleAdapter::openai()
                .stream_decoder(Surface::ChatCompletions)
                .expect("decoder"),
            futures::stream::iter(vec![
                Ok(Bytes::from_static(
                    b"data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"}}]}\n\n",
                )),
                Err(TransportError::Http(
                    "error sending request for url \
                     (http://provider.internal:9443/v1/chat/completions)"
                        .to_owned(),
                )),
            ])
            .boxed(),
            Instant::now(),
            Framing::OpenAiSse,
            None,
        );
        let mut body = response.into_body().into_data_stream();
        let mut relayed = String::new();
        while let Some(chunk) = body.next().await {
            relayed.push_str(&String::from_utf8_lossy(&chunk.expect("chunk")));
        }
        assert!(relayed.contains("upstream_stream_error"));
        assert!(relayed.contains("upstream transport failure"));
        assert!(!relayed.contains("provider.internal"));
        assert!(!relayed.contains("9443"));

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
            delivery: StreamDelivery::Reemit,
            buffered: VecDeque::new(),
            buffering_started: None,
            buffered_bytes: 0,
            rendered_byte_limit: None,
            terminal_seen: false,
            stream_terminal_grace: Duration::from_secs(1),
            terminal_deadline: None,
            finalization_required: false,
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
            decoded.push_str(&relay.decode_utf8(chunk).expect("valid UTF-8 chunk"));
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
        let state = state_for_with_rate_limit(&base_url, ledger.clone());
        let resp = router(state.clone())
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
        drop(
            state
                .0
                .rate_limiter
                .acquire(&crate::rate_limit::RateLimitKey {
                    namespace: "platform".to_owned(),
                    subject: "GW_TEST_INBOUND_KEY".to_owned(),
                })
                .await
                .expect("middleware-owned permit was released on client disconnect"),
        );
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
        crate::telemetry::testing::keep_callsites_answerable();
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

    /// A rotation that cannot reopen upstream is the one transport failure with
    /// no other surface to fall back on: the caller is told only that the
    /// transport failed, so if the operator's log does not carry the reason
    /// nothing does.
    #[tokio::test]
    async fn a_rotation_that_cannot_reopen_leaves_the_reason_in_the_log() {
        const FAILURE: &str = "error sending request for url \
                               (http://provider.internal:9443/v1/chat/completions)";

        crate::telemetry::testing::keep_callsites_answerable();
        let logged = Arc::new(Mutex::new(Vec::<u8>::new()));
        let sink = logged.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || CollectingWriter(sink.clone()))
            .with_ansi(false)
            .finish();

        let _log = tracing::subscriber::set_default(subscriber);
        let output = {
            let opener = |_lease: CredentialLease, _attempt: u32, _index: usize| {
                Box::pin(async { Err(TransportError::Http(FAILURE.to_owned())) })
                    as futures::future::BoxFuture<'static, _>
            };
            let response = relay_opened(
                state_for("http://127.0.0.1:1", Arc::new(Ledger::default())),
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
                    vec![test_lease("b")],
                    test_lease("a"),
                    1,
                    opener,
                    |_| {},
                    |_| {},
                )),
            );
            let mut body = response.into_body().into_data_stream();
            let mut output = String::new();
            while let Some(chunk) = body.next().await {
                output.push_str(&String::from_utf8_lossy(&chunk.expect("chunk")));
            }
            output
        };

        assert!(
            output.contains("upstream transport failure") && !output.contains("provider.internal"),
            "the caller is not told the endpoint it never chose: {output}"
        );
        let log = String::from_utf8(logged.lock().expect("log").clone()).expect("utf-8 log");
        assert!(
            log.contains("upstream attempt failed on the transport")
                && log.contains("provider.internal:9443"),
            "the operator keeps the reason the rotation could not reopen: {log}"
        );
    }

    /// Collects a subscriber's output where a test can read it back.
    struct CollectingWriter(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for CollectingWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("log").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
}
