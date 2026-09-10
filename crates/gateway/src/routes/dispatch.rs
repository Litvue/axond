//! Credential-pool dispatch and target failover for buffered and streaming
//! inference.
//!
//! One encoded body is reused across credentials in a pool (`PreparedPoolCall`).
//! Accounting is not done here: the walk returns the attempt, and
//! [`super::accounting`] records spend.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::response::Response;
use gateway_core::{
    CircuitDecision, FailoverDecision, FailoverPolicy, NativeMessagesDecoder, ProviderAdapter,
    ProviderError, ProviderRequest, ProviderResponse, ProviderStreamDecoder, Surface,
};
use gateway_transport::{
    AuthScheme, Deadline, EncodedJson, NativeCall, TimeoutBound, TimeoutKind, TransportError,
    Upstream,
};
use serde_json::Value;
use tracing::{Instrument, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::config::{Model, Provider, ProviderKind, Target};
use crate::credentials::{CredentialLease, CredentialPlan, CredentialSource};
use crate::error::GatewayError;
use crate::middleware::MiddlewareExecution;
use crate::pricing::{AliasPrices, Ineligible, RequestPrice};
use crate::state::{AppState, ConfigSnapshot, InboundKey, adapter_for};
use crate::streaming::{self, StreamContext, StreamDelivery};
use crate::telemetry;
use crate::usage::UsageRecord;
use crate::usage::identity::EventIdentity;

use super::accounting::{BudgetHold, BudgetReservation};
use super::{Route, Wire, target_key};

/// The target that produced the outcome (served it, or made the last attempt),
/// carried out of the failover walk so the caller can price and attribute it.
pub(super) struct ServedTarget {
    pub(super) provider: String,
    pub(super) model: String,
    pub(super) price: RequestPrice,
    pub(super) source: CredentialSource,
    pub(super) credential_id: String,
}

/// The result of the buffered failover walk: the terminating attempt's result,
/// the target that produced it, and the attempt/timing attribution.
pub(super) struct FailoverOutcome {
    pub(super) result: Result<ProviderResponse, TransportError>,
    pub(super) served: ServedTarget,
    pub(super) attempts: u32,
    pub(super) latency_ms: u64,
    pub(super) ttft_ms: Option<u64>,
}

/// Walk an alias's targets in order, dispatching the credential pool at each and
/// advancing on a retryable upstream failure. This is the outer loop that #7's
/// native routes build on: per-target circuit gating and the attempt/wall-clock
/// bounds live here, while `dispatch_over_pool` owns credential rotation within
/// one target.
///
/// A `Return`/`Ok` outcome that actually dispatched carries a `ServedTarget` so
/// the handler can price and attribute it. A walk that never dispatched (every
/// target skipped by an open circuit, or none had a credential) is a typed
/// error rather than an outcome — nothing reached a provider, so there is no
/// usage to record.
pub(super) async fn dispatch_with_failover(
    state: &AppState,
    snapshot: &ConfigSnapshot,
    caller: &InboundKey,
    model: &Model,
    prices: &AliasPrices,
    body: &Value,
    wire: &Wire,
) -> Result<FailoverOutcome, GatewayError> {
    let cfg = &snapshot.config;
    let policy = FailoverPolicy;
    let deadline = Instant::now() + Duration::from_millis(cfg.failover.overall_timeout_ms);
    let max_attempts = wire.route.max_attempts(cfg.failover.max_attempts);
    let pinned = wire.route.pins_affinity();
    let continuation = wire.route.is_continuation(body);

    let mut walk = FailoverWalk::new(caller, model.targets.len());
    for (index, target) in model.targets.iter().enumerate() {
        if pinned && index > 0 {
            break;
        }
        if walk.attempts >= max_attempts || Instant::now() >= deadline {
            break;
        }
        // An ineligible target is skipped exactly like one behind an open
        // circuit: it is configured and discoverable, but nothing approved says
        // what it costs, so it cannot be dispatched under a budget hold.
        let Some(price) = prices.get(index) else {
            if continuation {
                return Err(GatewayError::ContinuationAffinityUnavailable {
                    provider: target.provider.clone(),
                    model: target.model.clone(),
                });
            }
            walk.note_unpriced(&model.name, prices.ineligible(index));
            continue;
        };
        let Some(provider) = cfg.provider(&target.provider) else {
            if continuation {
                return Err(GatewayError::ContinuationAffinityUnavailable {
                    provider: target.provider.clone(),
                    model: target.model.clone(),
                });
            }
            continue;
        };
        let circuit_key = target_key(target);
        if let CircuitDecision::Skip = snapshot.target_circuits.allow(&circuit_key) {
            if continuation {
                return Err(GatewayError::ContinuationAffinityUnavailable {
                    provider: target.provider.clone(),
                    model: target.model.clone(),
                });
            }
            walk.skipped_open.push(circuit_key);
            continue;
        }
        let Some(plan) = (if pinned {
            snapshot
                .credentials
                .plan_pinned(cfg, &caller.namespace, &provider.id)
        } else {
            snapshot
                .credentials
                .plan(cfg, &caller.namespace, &provider.id)
        }) else {
            if continuation {
                return Err(GatewayError::ContinuationAffinityUnavailable {
                    provider: target.provider.clone(),
                    model: target.model.clone(),
                });
            }
            walk.note_missing_credential(&provider.id);
            continue;
        };

        let mut req_body = body.clone();
        req_body["model"] = Value::String(target.model.clone());
        let attempt_span = telemetry::upstream_attempt_span(
            walk.attempts,
            &target.provider,
            &target.model,
            UsageRecord::credential_source_str(plan.source),
        );
        let started = Instant::now();
        let attempt = dispatch_over_pool(
            state,
            snapshot,
            provider,
            &plan,
            &target.model,
            req_body,
            wire,
            Deadline::at(deadline),
        )
        .instrument(attempt_span.clone())
        .await;
        let latency_ms = started.elapsed().as_millis() as u64;
        // A non-streamed response arrives whole, so the first token lands with
        // the last one; the streaming relay reports the real first chunk.
        let ttft_ms = attempt.result.is_ok().then_some(latency_ms);
        if let Err(err) = &attempt.result {
            note_attempt_failure(&attempt_span, target, err);
        }
        telemetry::finish_upstream_attempt(
            &attempt_span,
            if attempt.result.is_ok() {
                telemetry::ATTEMPT_OK
            } else {
                telemetry::ATTEMPT_ERROR
            },
            latency_ms,
            ttft_ms,
        );
        walk.attempts += 1;

        let served = ServedTarget {
            provider: target.provider.clone(),
            model: target.model.clone(),
            price,
            source: plan.source,
            credential_id: attempt.credential_id.clone(),
        };
        match attempt.result {
            Ok(response) => {
                record_target_success(snapshot, target, &circuit_key);
                return Ok(FailoverOutcome {
                    result: Ok(response),
                    served,
                    attempts: walk.attempts,
                    latency_ms,
                    ttft_ms,
                });
            }
            Err(err) => {
                record_target_failure(snapshot, target, &circuit_key, &err);
                let has_next = index + 1 < walk.total
                    && walk.attempts < max_attempts
                    && Instant::now() < deadline;
                let decision = policy.decide(&as_provider_error(&err), has_next);
                walk.last = Some((err, served, latency_ms, ttft_ms));
                if decision == FailoverDecision::Return {
                    let (err, served, latency_ms, ttft_ms) = walk.last.take().expect("just set");
                    return Ok(FailoverOutcome {
                        result: Err(err),
                        served,
                        attempts: walk.attempts,
                        latency_ms,
                        ttft_ms,
                    });
                }
            }
        }
    }

    if let Some((err, served, latency_ms, ttft_ms)) = walk.last {
        return Ok(FailoverOutcome {
            result: Err(err),
            served,
            attempts: walk.attempts,
            latency_ms,
            ttft_ms,
        });
    }
    Err(walk.into_error())
}

pub(super) enum StreamLeaseParent<'a> {
    Attempt(&'a tracing::Span),
    Rotation(opentelemetry::Context),
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn open_stream_lease(
    state: &AppState,
    ctx: &StreamContext,
    provider: &Provider,
    target: &Target,
    body: &Value,
    wire: &Wire,
    lease: &CredentialLease,
    lease_index: usize,
    parent: StreamLeaseParent<'_>,
    deadline: Deadline,
) -> Result<
    (
        Box<dyn ProviderStreamDecoder>,
        gateway_transport::ByteStream,
    ),
    TransportError,
> {
    let adapter = adapter_for(provider.kind);
    let decoder = match wire.route {
        Route::ChatCompletions => adapter
            .stream_decoder(Surface::ChatCompletions)
            .map_err(TransportError::Provider)?,
        Route::Responses => adapter
            .stream_decoder(Surface::Responses)
            .map_err(TransportError::Provider)?,
        _ => Box::new(NativeMessagesDecoder::new()) as Box<dyn ProviderStreamDecoder>,
    };
    let upstream = Upstream {
        base_url: provider.base_url.clone(),
        api_key: lease.secret.clone(),
        auth: auth_scheme(provider.kind),
    };
    let mut request_body = body.clone();
    request_body["model"] = Value::String(target.model.clone());
    let opened = match wire.route {
        Route::ChatCompletions => {
            let request = ProviderRequest {
                model: target.model.clone(),
                body: request_body,
            };
            match parent {
                StreamLeaseParent::Attempt(span) => {
                    streaming::open_stream_with_attempt_span(
                        ctx,
                        span,
                        &lease.id,
                        lease_index,
                        state.0.dispatcher.dispatch_stream(
                            adapter.as_ref(),
                            &upstream,
                            Surface::ChatCompletions,
                            request,
                            deadline,
                        ),
                    )
                    .await?
                }
                StreamLeaseParent::Rotation(parent) => {
                    let open = state.0.dispatcher.dispatch_stream(
                        adapter.as_ref(),
                        &upstream,
                        Surface::ChatCompletions,
                        request,
                        deadline,
                    );
                    streaming::open_stream_with_lease_parent(
                        ctx,
                        &lease.id,
                        lease_index,
                        open,
                        parent,
                    )
                    .await?
                }
            }
        }
        _ => {
            let call = wire.call(request_body, adapter.name());
            match parent {
                StreamLeaseParent::Attempt(span) => {
                    streaming::open_stream_with_attempt_span(
                        ctx,
                        span,
                        &lease.id,
                        lease_index,
                        state.0.dispatcher.send_stream(&upstream, &call, deadline),
                    )
                    .await?
                }
                StreamLeaseParent::Rotation(parent) => {
                    let open = state.0.dispatcher.send_stream(&upstream, &call, deadline);
                    streaming::open_stream_with_lease_parent(
                        ctx,
                        &lease.id,
                        lease_index,
                        open,
                        parent,
                    )
                    .await?
                }
            }
        }
    };
    Ok((decoder, opened))
}

/// Walk targets and their credential pools for a streamed request. HTTP
/// open-time 429s rotate on both wires. The relay receives remaining leases for
/// OpenAI-normalized framing, where a rate-limit event before content can be
/// retried without splicing bytes already sent to the caller.
pub(super) async fn stream_with_failover(
    state: &AppState,
    snapshot: Arc<ConfigSnapshot>,
    caller: &InboundKey,
    model: &Model,
    attrs: Option<Value>,
    request: StreamRequest<'_>,
) -> Result<Response, GatewayError> {
    let StreamRequest {
        alias,
        body,
        prices,
        wire,
        identity,
        mut middleware_execution,
        delivery,
        mut hold,
    } = request;
    let mut reservation_guard = middleware_execution
        .core_budget_context()
        .is_none()
        .then(|| {
            BudgetReservation::new(
                state.clone(),
                hold.key.clone(),
                hold.reservation.clone(),
                middleware_execution.settlement_slot(),
            )
        });
    let cfg = &snapshot.config;
    let policy = FailoverPolicy;
    let deadline = Instant::now() + Duration::from_millis(cfg.failover.overall_timeout_ms);
    let max_attempts = wire.route.max_attempts(cfg.failover.max_attempts);
    let pinned = wire.route.pins_affinity();
    let continuation = wire.route.is_continuation(&body);

    let mut walk = FailoverWalk::new(caller, model.targets.len());
    let mut last_ctx: Option<(StreamContext, Instant)> = None;
    'targets: for (index, target) in model.targets.iter().enumerate() {
        if pinned && index > 0 {
            break;
        }
        if walk.attempts >= max_attempts || Instant::now() >= deadline {
            break;
        }
        // Ineligible: discoverable, but not dispatchable under a budget hold.
        let Some(price) = prices.get(index) else {
            if continuation {
                return Err(GatewayError::ContinuationAffinityUnavailable {
                    provider: target.provider.clone(),
                    model: target.model.clone(),
                });
            }
            walk.note_unpriced(&model.name, prices.ineligible(index));
            continue;
        };
        let Some(provider) = cfg.provider(&target.provider) else {
            if continuation {
                return Err(GatewayError::ContinuationAffinityUnavailable {
                    provider: target.provider.clone(),
                    model: target.model.clone(),
                });
            }
            continue;
        };
        let circuit_key = target_key(target);
        if let CircuitDecision::Skip = snapshot.target_circuits.allow(&circuit_key) {
            if continuation {
                return Err(GatewayError::ContinuationAffinityUnavailable {
                    provider: target.provider.clone(),
                    model: target.model.clone(),
                });
            }
            walk.skipped_open.push(circuit_key);
            continue;
        }
        let Some(plan) = (if pinned {
            snapshot
                .credentials
                .plan_pinned(cfg, &caller.namespace, &provider.id)
        } else {
            snapshot
                .credentials
                .plan(cfg, &caller.namespace, &provider.id)
        }) else {
            if continuation {
                return Err(GatewayError::ContinuationAffinityUnavailable {
                    provider: target.provider.clone(),
                    model: target.model.clone(),
                });
            }
            walk.note_missing_credential(&provider.id);
            continue;
        };
        if plan.attempts.is_empty() {
            walk.note_missing_credential(&provider.id);
            continue;
        }
        let target_attempt = walk.attempts;
        let mut attempt_started: Option<Instant> = None;
        let mut attempt_span: Option<tracing::Span> = None;
        for (lease_index, lease) in plan.attempts.iter().enumerate() {
            if Instant::now() >= deadline {
                if lease_index > 0 {
                    let started = attempt_started.expect("attempt start");
                    let span = attempt_span.as_ref().expect("attempt span");
                    if let Some(error) = &walk.last_error {
                        telemetry::record_attempt_failure(span, error);
                    }
                    telemetry::finish_upstream_attempt(
                        span,
                        telemetry::ATTEMPT_ERROR,
                        started.elapsed().as_millis() as u64,
                        None,
                    );
                    walk.attempts += 1;
                }
                break 'targets;
            }
            if attempt_span.is_none() {
                let span = telemetry::upstream_attempt_span(
                    target_attempt,
                    &target.provider,
                    &target.model,
                    UsageRecord::credential_source_str(plan.source),
                );
                for (index, skipped) in plan.parked.iter().enumerate() {
                    let lease_span = span.in_scope(|| {
                        telemetry::credential_lease_span(
                            &skipped.id,
                            UsageRecord::credential_source_str(plan.source),
                            index,
                        )
                    });
                    telemetry::finish_credential_lease(&lease_span, telemetry::LEASE_PARKED);
                }
                attempt_started = Some(Instant::now());
                attempt_span = Some(span);
            }
            let span = attempt_span.as_ref().expect("attempt span");
            let mut ctx = StreamContext {
                namespace: caller.namespace.clone(),
                attrs: attrs.clone().or_else(|| caller.attrs.clone()),
                subject: caller.subject.clone(),
                signer_kid: caller.signer_kid.clone(),
                alias: alias.clone(),
                target_provider: target.provider.clone(),
                target_model: target.model.clone(),
                source: plan.source,
                credential_id: lease.id.clone(),
                identity: identity.clone(),
                price,
                budget_key: hold.key.clone(),
                reservation: hold.reservation.clone(),
                rate_limit_permit: None,
                admission_permit: None,
                estimated_input_tokens: hold.estimated_input_tokens,
                attempts: 0,
            };
            let started = Instant::now();
            let opened = open_stream_lease(
                state,
                &ctx,
                provider,
                target,
                &body,
                wire,
                lease,
                plan.parked.len() + lease_index,
                StreamLeaseParent::Attempt(span),
                Deadline::at(deadline),
            )
            .await;
            ctx.attempts = target_attempt + 1;
            match opened {
                Ok((decoder, bytes)) => {
                    if let Some(guard) = reservation_guard.take() {
                        guard.disarm();
                    }
                    telemetry::finish_upstream_attempt(
                        span,
                        telemetry::ATTEMPT_OK,
                        attempt_started
                            .expect("attempt start")
                            .elapsed()
                            .as_millis() as u64,
                        None,
                    );
                    ctx.rate_limit_permit = hold.permit.take();
                    ctx.admission_permit = hold.admission.take();
                    record_target_success(&snapshot, target, &circuit_key);
                    telemetry::record_routing(
                        &ctx.namespace,
                        &ctx.subject,
                        &ctx.alias,
                        &ctx.target_provider,
                        &ctx.target_model,
                        UsageRecord::credential_source_str(ctx.source),
                    );
                    let remaining = plan.attempts[lease_index + 1..].to_vec();
                    let state_for_open = state.clone();
                    let provider_for_open = Arc::new(provider.clone());
                    let target_for_open = target.clone();
                    let wire_for_open = wire.clone();
                    let body_for_open = body.clone();
                    let caller_for_open = caller.clone();
                    let alias_for_open = alias.clone();
                    let hold_key_for_open = hold.key.clone();
                    let reservation_for_open = hold.reservation.clone();
                    let estimate_for_open = hold.estimated_input_tokens;
                    let source_for_open = plan.source;
                    let identity_for_open = identity.clone();
                    let attrs_for_open = attrs.clone();
                    let parent_context_for_open =
                        attempt_span.as_ref().expect("attempt span").context();
                    let opener =
                        move |next_lease: CredentialLease, _attempt: u32, lease_index: usize| {
                            let state = state_for_open.clone();
                            let provider = provider_for_open.clone();
                            let target = target_for_open.clone();
                            let wire = wire_for_open.clone();
                            let body = body_for_open.clone();
                            let caller = caller_for_open.clone();
                            let alias = alias_for_open.clone();
                            let budget_key = hold_key_for_open.clone();
                            let reservation = reservation_for_open.clone();
                            let parent_context = parent_context_for_open.clone();
                            let identity = identity_for_open.clone();
                            let attrs = attrs_for_open.clone();
                            Box::pin(async move {
                                let ctx = StreamContext {
                                    namespace: caller.namespace,
                                    attrs: attrs.or(caller.attrs),
                                    subject: caller.subject,
                                    signer_kid: caller.signer_kid,
                                    alias,
                                    target_provider: target.provider.clone(),
                                    target_model: target.model.clone(),
                                    source: source_for_open,
                                    credential_id: next_lease.id.clone(),
                                    // The rotation serves the same request, so it
                                    // carries the same event identity rather than
                                    // re-reading a span it no longer runs under.
                                    identity,
                                    // The same immutable pricing the request
                                    // opened under: a rotation changes the
                                    // credential, never what the request costs.
                                    price,
                                    budget_key,
                                    reservation,
                                    rate_limit_permit: None,
                                    // Rotation re-opens upstream for a relay that
                                    // already holds the request's permits.
                                    admission_permit: None,
                                    estimated_input_tokens: estimate_for_open,
                                    attempts: 0,
                                };
                                open_stream_lease(
                                    &state,
                                    &ctx,
                                    provider.as_ref(),
                                    &target,
                                    &body,
                                    &wire,
                                    &next_lease,
                                    lease_index,
                                    StreamLeaseParent::Rotation(parent_context),
                                    Deadline::at(deadline),
                                )
                                .await
                                .map(|(decoder, bytes)| streaming::OpenedStream { decoder, bytes })
                            }) as futures::future::BoxFuture<'static, _>
                        };
                    let snapshot_for_health = snapshot.clone();
                    let rotation = streaming::RotationHandle::new_with_deadline(
                        remaining,
                        lease.clone(),
                        plan.parked.len() + lease_index + 1,
                        opener,
                        Some(deadline),
                        move |lease| snapshot_for_health.credentials.record_failure(lease),
                        {
                            let snapshot = snapshot.clone();
                            move |lease| snapshot.credentials.record_success(lease)
                        },
                    );
                    return Ok(streaming::relay_opened_with_middleware(
                        state.clone(),
                        ctx,
                        streaming::OpenedStream { decoder, bytes },
                        started,
                        wire.route.framing(),
                        Some(rotation),
                        streaming::StreamMiddleware::new(middleware_execution, delivery),
                    ));
                }
                Err(err) if is_credential_exhausted(&err) => {
                    snapshot.credentials.record_failure(lease);
                    last_ctx = Some((ctx, started));
                    walk.last_error = Some(err);
                    continue;
                }
                Err(err) => {
                    note_attempt_failure(span, target, &err);
                    record_target_failure(&snapshot, target, &circuit_key, &err);
                    let has_next = index + 1 < walk.total
                        && walk.attempts < max_attempts
                        && Instant::now() < deadline;
                    let decision = policy.decide(&as_provider_error(&err), has_next);
                    last_ctx = Some((ctx, started));
                    walk.last_error = Some(err);
                    if decision == FailoverDecision::Return {
                        telemetry::finish_upstream_attempt(
                            span,
                            telemetry::ATTEMPT_ERROR,
                            attempt_started
                                .expect("attempt start")
                                .elapsed()
                                .as_millis() as u64,
                            None,
                        );
                        walk.attempts += 1;
                        break 'targets;
                    }
                    break;
                }
            }
        }
        let span = attempt_span.as_ref().expect("attempt span");
        if let Some(error) = &walk.last_error {
            telemetry::record_attempt_failure(span, error);
        }
        telemetry::finish_upstream_attempt(
            span,
            telemetry::ATTEMPT_ERROR,
            attempt_started
                .expect("attempt start")
                .elapsed()
                .as_millis() as u64,
            None,
        );
        walk.attempts += 1;
    }

    if let Some(err) = walk.last_error.take() {
        if let Some((mut ctx, started)) = last_ctx {
            if let Some(guard) = reservation_guard.take() {
                guard.disarm();
            }
            ctx.attempts = walk.attempts;
            ctx.rate_limit_permit = hold.permit.take();
            ctx.admission_permit = hold.admission.take();
            streaming::settle_upstream_error_with_middleware(
                state.clone(),
                ctx,
                started,
                middleware_execution,
            );
        } else {
            if !middleware_execution.release_core_budget().await {
                reservation_guard
                    .take()
                    .expect("legacy budget guard")
                    .release()
                    .await;
            }
        }
        return Err(err.into());
    }
    if !middleware_execution.release_core_budget().await {
        reservation_guard
            .take()
            .expect("legacy budget guard")
            .release()
            .await;
    }
    Err(walk.into_error())
}
/// One streamed request as the failover walk sees it: the alias it resolved,
/// the body to forward, the wire it speaks, and the budget hold it was admitted
/// under.
pub(super) struct StreamRequest<'a> {
    pub(super) alias: String,
    pub(super) body: Value,
    /// What each target is charged at under the snapshot the request started
    /// with, resolved before admission so the relay's settlement cannot depend on
    /// a price book published while the stream was open.
    pub(super) prices: &'a AliasPrices,
    pub(super) wire: &'a Wire,
    /// The identity of the usage event this request will settle as, minted at
    /// admission and cloned into every stream context the walk builds — including
    /// a credential rotation's — so a stream that rotates, ends, is cancelled, or
    /// never opens all report the same event.
    pub(super) identity: EventIdentity,
    /// Pinned chain plus request-scope state, moved into the relay's
    /// response-lifetime accounting owner when a stream opens.
    pub(super) middleware_execution: MiddlewareExecution,
    pub(super) delivery: StreamDelivery,
    pub(super) hold: BudgetHold,
}
/// Mutable bookkeeping shared by the buffered and streaming failover walks: how
/// many upstream attempts have been made, which targets were circuit-skipped,
/// and the reason to surface if nothing ever dispatched.
pub(super) struct FailoverWalk {
    namespace: String,
    total: usize,
    attempts: u32,
    skipped_open: Vec<String>,
    no_credential: Option<GatewayError>,
    /// The refusal for a target skipped because nothing approved prices it,
    /// carried so a walk pinned to that target reports the pricing refusal
    /// instead of a generic "nothing to attempt" request error.
    unpriced: Option<GatewayError>,
    /// The last buffered attempt's error + attribution, carried so a walk that
    /// exhausts its targets still returns a real upstream error.
    last: Option<(TransportError, ServedTarget, u64, Option<u64>)>,
    /// The last streaming open error (the streaming context is carried
    /// separately since it is consumed to settle the usage record).
    last_error: Option<TransportError>,
}

impl FailoverWalk {
    fn new(caller: &InboundKey, total: usize) -> Self {
        Self {
            namespace: caller.namespace.clone(),
            total,
            attempts: 0,
            skipped_open: Vec::new(),
            no_credential: None,
            unpriced: None,
            last: None,
            last_error: None,
        }
    }

    /// Remember that a candidate was skipped for want of an approved price. The
    /// operator-facing identity of the book stays in the log; the walk keeps only
    /// the stable redacted reason a caller may be told (#147).
    fn note_unpriced(&mut self, alias: &str, refusal: Option<&Ineligible>) {
        let Some(refusal) = refusal else {
            return;
        };
        if self.unpriced.is_none() {
            tracing::warn!(
                model = %alias,
                detail = %refusal.detail(),
                "skipping a target with no approved price"
            );
            self.unpriced = Some(GatewayError::ModelNotPriced {
                alias: alias.to_owned(),
                reason: refusal.reason().to_owned(),
            });
        }
    }

    fn note_missing_credential(&mut self, provider: &str) {
        self.no_credential
            .get_or_insert_with(|| GatewayError::NoCredential {
                namespace: self.namespace.clone(),
                provider: provider.to_owned(),
            });
    }

    /// The error for a walk that never dispatched: an open circuit on every
    /// candidate is a distinct, retriable condition from having no credential.
    fn into_error(self) -> GatewayError {
        if !self.skipped_open.is_empty() {
            return ProviderError::AllCircuitsOpen(self.skipped_open).into();
        }
        self.no_credential
            .or(self.unpriced)
            .unwrap_or_else(|| ProviderError::InvalidRequest("no attemptable target".into()).into())
    }
}
pub(super) fn auth_scheme(kind: ProviderKind) -> AuthScheme {
    match kind {
        ProviderKind::Anthropic => AuthScheme::Header("x-api-key"),
        ProviderKind::Openai | ProviderKind::OpenaiCompatible => AuthScheme::Bearer,
    }
}

/// Record bounded failure diagnostics and any timeout class. Transport URLs
/// stay in operator logs; provider HTTP status and message reach attempt spans.
pub(super) fn note_attempt_failure(span: &tracing::Span, target: &Target, err: &TransportError) {
    telemetry::record_attempt_failure(span, err);
    if let Some(kind) = err.timeout_kind() {
        let bound = err
            .timeout_bound()
            .map(TimeoutBound::label)
            .unwrap_or_default();
        telemetry::record_attempt_timeout(
            span,
            &target.provider,
            &target.model,
            kind.label(),
            bound,
        );
        warn!(
            provider = %target.provider,
            model = %target.model,
            timeout = kind.label(),
            timeout_bound = bound,
            "upstream attempt exceeded a transport bound"
        );
        return;
    }
    // An `Http` failure is the one the caller is told only that the transport
    // failed, so this line is the one place its reason survives: a DNS failure,
    // a refused connect, and a TLS handshake failure are the same answer and
    // different incidents. The endpoint stays here, in the operator's log, where
    // it is already credential-redacted and where the operator configured it. A
    // provider's own verdict reaches the caller intact and is not repeated here.
    if matches!(err, TransportError::Http(_)) {
        warn!(
            provider = %target.provider,
            model = %target.model,
            error = %err,
            "upstream attempt failed on the transport"
        );
    }
}

pub(super) fn record_target_success(snapshot: &ConfigSnapshot, target: &Target, circuit_key: &str) {
    snapshot.target_circuits.record_success(circuit_key);
    telemetry::metrics::record_circuit_state(
        &target.provider,
        &target.model,
        snapshot.target_circuits.state(circuit_key),
    );
}

/// A target failure trips its circuit only when it reflects on the *target*'s
/// health. A `429` that exhausted the pool is credential-scoped (ADR 0006) and a
/// `404` names a missing deployment, not an unhealthy target — both fail over
/// without opening the target's breaker. A walk budget spent before this target
/// was ever dispatched to belongs in the same category; a target that was given
/// time and stalled does not, however short that time was.
pub(super) fn record_target_failure(
    snapshot: &ConfigSnapshot,
    target: &Target,
    circuit_key: &str,
    err: &TransportError,
) {
    if as_provider_error(err).affects_provider_health()
        && !is_credential_exhausted(err)
        && !was_never_dispatched(err)
    {
        snapshot.target_circuits.record_failure(circuit_key);
        telemetry::metrics::record_circuit_state(
            &target.provider,
            &target.model,
            snapshot.target_circuits.state(circuit_key),
        );
    }
}

/// View a transport error through the core retryability taxonomy so the failover
/// policy and the breaker share one definition of "retryable". A transport-level
/// error (no provider status) is a target-scoped dependency failure.
pub(super) fn as_provider_error(err: &TransportError) -> ProviderError {
    match err {
        TransportError::Provider(pe) | TransportError::Upstream { error: pe, .. } => pe.clone(),
        TransportError::Http(message) => ProviderError::transport("upstream", message.clone()),
        // A timeout says nothing conclusive about the target beyond "it did not
        // answer in time", which is exactly a target-scoped dependency failure.
        // An oversized body is the same: the target produced something this
        // gateway will not serve.
        TransportError::Timeout { .. } | TransportError::BodyTooLarge { .. } => {
            ProviderError::transport("upstream", err.to_string())
        }
    }
}

/// The upstream attempt that terminated the request, plus the credential that
/// made it (for attribution).
pub(super) struct PooledAttempt {
    result: Result<ProviderResponse, TransportError>,
    credential_id: String,
}

pub(super) enum PreparedPoolCall {
    Adapter(EncodedJson),
    Native(NativeCall),
}

pub(super) fn prepare_pool_call(
    adapter: &dyn ProviderAdapter,
    wire: &Wire,
    target_model: &str,
    body: Value,
) -> Result<PreparedPoolCall, TransportError> {
    match wire.route {
        Route::ChatCompletions => adapter
            .encode_request(
                Surface::ChatCompletions,
                ProviderRequest {
                    model: target_model.to_string(),
                    body,
                },
            )
            .map(|encoded| PreparedPoolCall::Adapter(EncodedJson::from_value(&encoded)))
            .map_err(TransportError::from),
        _ => Ok(PreparedPoolCall::Native(wire.call(body, adapter.name()))),
    }
}

/// Walk the credential pool: dispatch with the first credential, and on a
/// credential-scoped failure (rate limit / quota) park that credential and
/// retry the *same* target with the next one. Target-level failover is a
/// separate concern and is not attempted here.
#[allow(clippy::too_many_arguments)]
pub(super) async fn dispatch_over_pool(
    state: &AppState,
    snapshot: &ConfigSnapshot,
    provider: &Provider,
    plan: &CredentialPlan,
    target_model: &str,
    body: Value,
    wire: &Wire,
    deadline: Deadline,
) -> PooledAttempt {
    let adapter = adapter_for(provider.kind);
    let mut exhausted: Option<PooledAttempt> = None;
    let mut body = Some(body);
    let mut prepared: Option<PreparedPoolCall> = None;

    for (index, skipped) in plan.parked.iter().enumerate() {
        let span = telemetry::credential_lease_span(
            &skipped.id,
            UsageRecord::credential_source_str(plan.source),
            index,
        );
        telemetry::finish_credential_lease(&span, telemetry::LEASE_PARKED);
    }

    for (index, lease) in plan.attempts.iter().enumerate() {
        let lease_span = telemetry::credential_lease_span(
            &lease.id,
            UsageRecord::credential_source_str(plan.source),
            plan.parked.len() + index,
        );
        let upstream = Upstream {
            base_url: provider.base_url.clone(),
            api_key: lease.secret.clone(),
            auth: auth_scheme(provider.kind),
        };
        if prepared.is_none() {
            match prepare_pool_call(
                adapter.as_ref(),
                wire,
                target_model,
                body.take().expect("request body is prepared once"),
            ) {
                Ok(call) => prepared = Some(call),
                Err(err) => {
                    telemetry::finish_credential_lease(&lease_span, telemetry::LEASE_ERROR);
                    return PooledAttempt {
                        result: Err(err),
                        credential_id: lease.id.clone(),
                    };
                }
            }
        }
        let prepared = prepared.as_ref().expect("prepared on first attempt");
        let result = async {
            match prepared {
                PreparedPoolCall::Adapter(encoded) => {
                    state
                        .0
                        .dispatcher
                        .dispatch_encoded(
                            adapter.as_ref(),
                            &upstream,
                            Surface::ChatCompletions,
                            encoded,
                            deadline,
                        )
                        .await
                }
                PreparedPoolCall::Native(call) => state
                    .0
                    .dispatcher
                    .send(&upstream, call, deadline)
                    .await
                    .map(|body| ProviderResponse {
                        usage: wire.route.native_usage(&body),
                        body,
                    }),
            }
        }
        .instrument(lease_span.clone())
        .await;
        match result {
            Ok(response) => {
                telemetry::finish_credential_lease(&lease_span, telemetry::LEASE_SERVED);
                snapshot.credentials.record_success(lease);
                return PooledAttempt {
                    result: Ok(response),
                    credential_id: lease.id.clone(),
                };
            }
            Err(err) if is_credential_exhausted(&err) => {
                telemetry::finish_credential_lease(&lease_span, telemetry::LEASE_RATE_LIMITED);
                snapshot.credentials.record_failure(lease);
                tracing::warn!(
                    provider = %provider.id,
                    credential = %lease.id,
                    "credential is rate-limited or out of quota; trying the next in the pool"
                );
                exhausted = Some(PooledAttempt {
                    result: Err(err),
                    credential_id: lease.id.clone(),
                });
            }
            Err(err) => {
                telemetry::finish_credential_lease(&lease_span, telemetry::LEASE_ERROR);
                return PooledAttempt {
                    result: Err(err),
                    credential_id: lease.id.clone(),
                };
            }
        }
    }

    exhausted.unwrap_or_else(|| PooledAttempt {
        result: Err(ProviderError::InvalidRequest("empty credential pool".into()).into()),
        credential_id: String::new(),
    })
}

/// A `429` (rate limit or exhausted quota) is attributable to the *credential*,
/// so it parks that key and falls to the next. Every other upstream failure is
/// the target's problem, not the key's.
pub(super) fn is_credential_exhausted(err: &TransportError) -> bool {
    err.provider_error()
        .is_some_and(ProviderError::is_credential_rate_limited)
}

/// `TimeoutKind::Overall` is the one timeout no target earned: the walk's budget
/// was already spent, so nothing was dispatched and there is no evidence about
/// this target to record. Parking a target the gateway never called would let
/// one slow target take healthy ones out of rotation.
///
/// Every other timeout names the phase that stalled — including one cut short by
/// what was left of `failover.overall_timeout_ms` — because a target that
/// accepted a request and produced nothing in the time it was given *is*
/// evidence, and treating a late-in-the-walk stall as the gateway's own problem
/// would keep a black-holing target's breaker closed forever.
pub(super) fn was_never_dispatched(err: &TransportError) -> bool {
    err.timeout_kind() == Some(TimeoutKind::Overall)
}
