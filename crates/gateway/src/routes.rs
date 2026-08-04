//! HTTP surface.
//!
//! Passthrough-first (delta A1): the OpenAI-shaped `/v1/chat/completions` route
//! forwards the caller's body to a same-shaped upstream and only rewrites the
//! `model` field to the resolved target. Cross-provider translation (e.g.
//! routing an OpenAI request to Anthropic) reuses `gateway-core`'s adapters and
//! is wired in as failover lands. A `stream: true` request takes the SSE relay
//! in [`crate::streaming`].
//!
//! The provider-native routes — Anthropic's `/v1/messages` and OpenAI-shaped
//! `/v1/embeddings` — take the same path (ADR 0012). A caller already speaking
//! the target's wire has its body forwarded to the provider's own endpoint with
//! only `model` rewritten, so signed thinking and tool-use blocks survive
//! byte-for-byte; only how usage is read back differs per route. `/v1/responses`
//! is deferred past beta and answers with a typed `501` rather than being a
//! missing route (delta B3).
//!
//! An alias's `targets` are tried in configured order (ADR 0008). The failover
//! walk is the *outer* loop around credential-pool dispatch: each target has an
//! in-memory per-target circuit breaker, a retryable upstream failure advances
//! to the next target, and the walk is bounded by both a total attempt count and
//! an overall wall-clock budget. Streaming can fail over only while opening the
//! upstream; once bytes flow, a mid-stream failure is terminal.

use std::time::{Duration, Instant};

use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use gateway_core::{
    CircuitDecision, FailoverDecision, FailoverPolicy, FailoverTarget, ModelPrice, ModelUsage,
    NativeMessagesDecoder, ProviderError, ProviderRequest, ProviderResponse, ProviderStreamDecoder,
    Surface, Usage,
};
use gateway_transport::{AuthScheme, NativeCall, TransportError, Upstream};
use serde_json::{Value, json};
use tracing::Instrument;

use crate::budget::{Admission, BudgetKey, Denial, Reservation};
use crate::config::{Model, Provider, ProviderKind, Target};
use crate::credentials::{CredentialPlan, CredentialSource};
use crate::error::GatewayError;
use crate::state::{AppState, ConfigSnapshot, InboundKey, adapter_for};
use crate::streaming::{self, Framing, StreamContext};
use crate::telemetry;
use crate::usage::{Status, UsageRecord};

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/v1/models", get(list_models))
        .route("/v1/chat/completions", post(chat_completions))
        .route("/v1/messages", post(native_messages))
        .route("/v1/embeddings", post(embeddings))
        .route("/v1/responses", post(responses))
        .with_state(state)
}

async fn healthz() -> &'static str {
    "ok"
}

/// Liveness is trivially true; real readiness (config loaded, at least one
/// credential present) is a follow-up — kept honest rather than always-200.
async fn readyz() -> &'static str {
    "ready"
}

async fn list_models(State(state): State<AppState>) -> Json<Value> {
    let data: Vec<Value> = state
        .config()
        .config
        .model
        .iter()
        .map(|m| json!({ "id": m.name, "object": "model", "owned_by": "axond" }))
        .collect();
    Json(json!({ "object": "list", "data": data }))
}

/// Resolve the caller's namespace + subject from the inbound key. When no
/// gateway keys are configured the gateway is open (dev mode) and uses the
/// default namespace.
///
/// The key travels as `Authorization: Bearer` or, because that is what an
/// Anthropic SDK pointed at the gateway sends, as `x-api-key`. Both name the
/// same gateway key; the scheme is the client's, not a second credential space.
fn authenticate(
    snapshot: &ConfigSnapshot,
    headers: &HeaderMap,
) -> Result<InboundKey, GatewayError> {
    if snapshot.inbound_keys.is_empty() {
        return Ok(InboundKey {
            namespace: snapshot.config.default_namespace().to_string(),
            subject: "anonymous".to_string(),
        });
    }
    let token = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .or_else(|| headers.get("x-api-key").and_then(|v| v.to_str().ok()))
        .ok_or(GatewayError::Unauthorized)?;
    snapshot
        .inbound_keys
        .get(token)
        .cloned()
        .ok_or(GatewayError::Unauthorized)
}

/// The wire shape a route speaks, which is the only thing that differs between
/// the routes: the upstream path, which provider kinds can serve it, and how
/// usage is read out of the provider's answer. Everything else — aliasing,
/// failover, credential pools, budgets, usage — is shared.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Route {
    /// OpenAI-shaped chat, dispatched *through* the provider adapter so a target
    /// on another wire can still serve it by translation.
    ChatCompletions,
    /// Anthropic-shaped Messages, forwarded verbatim to an Anthropic target.
    NativeMessages,
    /// OpenAI-shaped embeddings, forwarded verbatim to an OpenAI-family target.
    Embeddings,
}

impl Route {
    /// The caller-facing path, for error messages.
    fn label(self) -> &'static str {
        match self {
            Self::ChatCompletions => "/v1/chat/completions",
            Self::NativeMessages => "/v1/messages",
            Self::Embeddings => "/v1/embeddings",
        }
    }

    /// Path appended to the provider's `base_url`.
    fn upstream_path(self) -> &'static str {
        match self {
            Self::ChatCompletions => "/chat/completions",
            Self::NativeMessages => "/messages",
            Self::Embeddings => "/embeddings",
        }
    }

    /// Whether a provider of this kind speaks the route's wire shape. A native
    /// route has no translation to fall back on, so an alias whose target cannot
    /// serve the shape is a configuration mistake worth naming.
    fn serves(self, kind: ProviderKind) -> bool {
        match self {
            Self::ChatCompletions => true,
            Self::NativeMessages => kind == ProviderKind::Anthropic,
            Self::Embeddings => {
                matches!(kind, ProviderKind::Openai | ProviderKind::OpenaiCompatible)
            }
        }
    }

    fn streamable(self) -> bool {
        self != Self::Embeddings
    }

    fn framing(self) -> Framing {
        match self {
            Self::ChatCompletions => Framing::OpenAiSse,
            Self::NativeMessages | Self::Embeddings => Framing::Native,
        }
    }

    /// Usage from a *native* response, mapped onto the canonical record every
    /// route produces. Wire knowledge lives in `gateway-core`.
    fn native_usage(self, response: &Value) -> ModelUsage {
        match self {
            Self::NativeMessages => gateway_core::native_message_usage(response),
            // Chat never takes this path (its adapter reports usage), so the
            // OpenAI-shaped prompt-only reader is the honest default.
            Self::ChatCompletions | Self::Embeddings => gateway_core::embeddings_usage(response),
        }
    }

    /// Pre-dispatch estimate the budget hold is priced from. Embeddings produce
    /// no completion, so nothing is held for output.
    fn estimate(self, body: &Value) -> Usage {
        let estimate = estimate_usage(body);
        match self {
            Self::Embeddings => Usage {
                output_tokens: 0,
                ..estimate
            },
            _ => estimate,
        }
    }

    /// Headers the wire shape itself requires upstream. Anthropic needs a
    /// version; the caller's own value wins so an SDK pinned to a newer wire
    /// keeps its behaviour, and its `anthropic-beta` opt-ins travel as sent.
    fn wire_headers(self, headers: &HeaderMap) -> Vec<(&'static str, String)> {
        if self != Self::NativeMessages {
            return Vec::new();
        }
        let mut wire = vec![(
            "anthropic-version",
            headers
                .get("anthropic-version")
                .and_then(|value| value.to_str().ok())
                .unwrap_or(gateway_core::AnthropicAdapter::VERSION)
                .to_owned(),
        )];
        if let Some(beta) = headers.get("anthropic-beta").and_then(|v| v.to_str().ok()) {
            wire.push(("anthropic-beta", beta.to_owned()));
        }
        wire
    }
}

/// A route plus the wire headers this request carries upstream, threaded through
/// the shared failover walk so both dispatch shapes reuse one request path.
struct Wire {
    route: Route,
    headers: Vec<(&'static str, String)>,
}

impl Wire {
    /// Reject an alias whose targets cannot speak the route's wire *before*
    /// anything is reserved or dispatched: a native route has no translation to
    /// fall back on, and failing over into a target that cannot serve the shape
    /// would turn a config mistake into a confusing upstream `404`.
    fn check_targets(
        &self,
        cfg: &crate::config::Config,
        model: &Model,
        alias: &str,
    ) -> Result<(), GatewayError> {
        for target in &model.targets {
            let Some(provider) = cfg.provider(&target.provider) else {
                continue;
            };
            if !self.route.serves(provider.kind) {
                return Err(GatewayError::UnsupportedWire {
                    route: self.route.label(),
                    alias: alias.to_owned(),
                    provider: provider.id.clone(),
                });
            }
        }
        Ok(())
    }

    fn call(&self, body: Value, provider: &'static str) -> NativeCall {
        NativeCall {
            provider,
            path: self.route.upstream_path(),
            body,
            headers: self.headers.clone(),
        }
    }
}

async fn chat_completions(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Response, GatewayError> {
    serve(state, headers, body, Route::ChatCompletions).await
}

/// Anthropic-native Messages. The caller's body already speaks the target's
/// wire, so it is forwarded to the provider's `/messages` untouched but for the
/// `model` alias — which is what keeps signed thinking and tool-use blocks
/// intact through the gateway (ADR 0012).
async fn native_messages(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Response, GatewayError> {
    serve(state, headers, body, Route::NativeMessages).await
}

async fn embeddings(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Result<Response, GatewayError> {
    serve(state, headers, body, Route::Embeddings).await
}

/// Deferred past beta (ADR 0012): the OpenAI Responses API is a stateful
/// surface, and serving it honestly needs more than passthrough. The route
/// exists and says so, because a missing route is indistinguishable from a
/// misconfigured `base_url`.
async fn responses() -> Result<Json<Value>, GatewayError> {
    Err(GatewayError::NotImplemented(
        "the OpenAI Responses API (`/v1/responses`), deferred past beta by ADR 0012 \
         in favour of `/v1/chat/completions`,",
    ))
}

/// The one request path every route shares: authenticate against a single
/// config snapshot, resolve the alias, hold a budget estimate, dispatch through
/// the failover walk, then settle the hold and record exactly one usage record.
/// Routes differ only in the wire they speak — where the body goes upstream and
/// how usage is read back out (see [`Route`]).
async fn serve(
    state: AppState,
    headers: HeaderMap,
    body: Value,
    route: Route,
) -> Result<Response, GatewayError> {
    // One snapshot for the whole request: a reload that lands mid-request
    // cannot change the alias, credential, or circuit this request resolved.
    let snapshot = state.config();
    let caller = authenticate(&snapshot, &headers)?;
    let cfg = &snapshot.config;

    let streamed = route.streamable() && body.get("stream").and_then(Value::as_bool) == Some(true);

    let alias = body
        .get("model")
        .and_then(Value::as_str)
        .ok_or_else(|| GatewayError::BadRequest("missing `model`".into()))?
        .to_string();

    let model = cfg
        .model(&alias)
        .ok_or_else(|| GatewayError::UnknownModel(alias.clone()))?;
    let wire = Wire {
        route,
        headers: route.wire_headers(&headers),
    };
    wire.check_targets(cfg, model, &alias)?;

    // Budget is denominated in micro-dollars. Hold a conservative cost estimate
    // from the first target's price before dispatch; settle the hold against the
    // real cost — priced at whichever target actually served — after.
    let budget_key = BudgetKey {
        namespace: caller.namespace.clone(),
        subject: caller.subject.clone(),
    };
    let estimate = route.estimate(&body);
    let estimated_cost = model.targets[0].price.cost_microdollars(estimate);
    let reservation = match state.0.budget.reserve(&budget_key, estimated_cost).await {
        Admission::Allowed(reservation) => reservation,
        Admission::Denied(Denial::Exceeded) => return Err(GatewayError::BudgetExceeded(alias)),
        Admission::Denied(Denial::StoreUnavailable) => return Err(GatewayError::BudgetUnavailable),
    };

    if streamed {
        return stream_with_failover(
            &state,
            &snapshot,
            &caller,
            model,
            StreamRequest {
                alias,
                body,
                wire: &wire,
                hold: BudgetHold {
                    key: budget_key,
                    reservation,
                    estimated_input_tokens: estimate.input_tokens,
                },
            },
        )
        .await;
    }

    let outcome =
        match dispatch_with_failover(&state, &snapshot, &caller, model, &body, &wire).await {
            Ok(outcome) => outcome,
            Err(err) => {
                // Nothing reached a provider, so nothing was consumed: the whole
                // estimate goes back rather than lingering until it expires.
                state.0.budget.release(&budget_key, &reservation).await;
                return Err(err);
            }
        };
    let served = &outcome.served;
    match outcome.result {
        Ok(response) => {
            let usage = to_usage(&response.usage);
            let cost = served.price.cost_microdollars(usage);
            state.0.budget.settle(&budget_key, &reservation, cost).await;
            record_usage(
                &state,
                RecordArgs {
                    caller: &caller,
                    alias: &alias,
                    target_provider: &served.provider,
                    target_model: &served.model,
                    source: served.source,
                    credential_id: &served.credential_id,
                    status: Status::Ok,
                    input_tokens: response.usage.input_tokens,
                    output_tokens: response.usage.output_tokens,
                    cost_microdollars: cost,
                    latency_ms: outcome.latency_ms,
                    ttft_ms: outcome.ttft_ms,
                    attempts: outcome.attempts,
                },
            )
            .await;
            Ok(Json(response.body).into_response())
        }
        Err(err) => {
            // The charging policy is "what was actually consumed" (ADR 0010),
            // and a buffered failure reports no usage at all: providers do not
            // return a usage block with an error, and nothing was relayed to
            // measure. Spend is therefore genuinely unknowable and charged as
            // zero — the streamed path, which can measure what it relayed,
            // charges its partial spend.
            state.0.budget.release(&budget_key, &reservation).await;
            record_usage(
                &state,
                RecordArgs {
                    caller: &caller,
                    alias: &alias,
                    target_provider: &served.provider,
                    target_model: &served.model,
                    source: served.source,
                    credential_id: &served.credential_id,
                    status: Status::UpstreamError,
                    input_tokens: 0,
                    output_tokens: 0,
                    cost_microdollars: 0,
                    latency_ms: outcome.latency_ms,
                    ttft_ms: outcome.ttft_ms,
                    attempts: outcome.attempts,
                },
            )
            .await;
            Err(err.into())
        }
    }
}

/// The target that produced the outcome (served it, or made the last attempt),
/// carried out of the failover walk so the caller can price and attribute it.
struct ServedTarget {
    provider: String,
    model: String,
    price: ModelPrice,
    source: CredentialSource,
    credential_id: String,
}

/// The result of the buffered failover walk: the terminating attempt's result,
/// the target that produced it, and the attempt/timing attribution.
struct FailoverOutcome {
    result: Result<ProviderResponse, TransportError>,
    served: ServedTarget,
    attempts: u32,
    latency_ms: u64,
    ttft_ms: Option<u64>,
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
async fn dispatch_with_failover(
    state: &AppState,
    snapshot: &ConfigSnapshot,
    caller: &InboundKey,
    model: &Model,
    body: &Value,
    wire: &Wire,
) -> Result<FailoverOutcome, GatewayError> {
    let cfg = &snapshot.config;
    let policy = FailoverPolicy;
    let deadline = Instant::now() + Duration::from_millis(cfg.failover.overall_timeout_ms);
    let max_attempts = cfg.failover.max_attempts;

    let mut walk = FailoverWalk::new(caller, model.targets.len());
    for (index, target) in model.targets.iter().enumerate() {
        if walk.attempts >= max_attempts || Instant::now() >= deadline {
            break;
        }
        let Some(provider) = cfg.provider(&target.provider) else {
            continue;
        };
        let circuit_key = target_key(target);
        if let CircuitDecision::Skip = snapshot.target_circuits.allow(&circuit_key) {
            walk.skipped_open.push(circuit_key);
            continue;
        }
        let Some(plan) = snapshot
            .credentials
            .plan(cfg, &caller.namespace, &provider.id)
        else {
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
        )
        .instrument(attempt_span.clone())
        .await;
        let latency_ms = started.elapsed().as_millis() as u64;
        // A non-streamed response arrives whole, so the first token lands with
        // the last one; the streaming relay reports the real first chunk.
        let ttft_ms = attempt.result.is_ok().then_some(latency_ms);
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
            price: target.price,
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

/// Fail over across targets for a streamed request. Failover is only possible
/// while opening the upstream — once the relay begins emitting, a mid-stream
/// failure is terminal (ADR 0005), so this loop attempts the open per target and
/// hands off to the relay the moment one succeeds.
async fn stream_with_failover(
    state: &AppState,
    snapshot: &ConfigSnapshot,
    caller: &InboundKey,
    model: &Model,
    request: StreamRequest<'_>,
) -> Result<Response, GatewayError> {
    let StreamRequest {
        alias,
        body,
        wire,
        hold,
    } = request;
    let cfg = &snapshot.config;
    let policy = FailoverPolicy;
    let deadline = Instant::now() + Duration::from_millis(cfg.failover.overall_timeout_ms);
    let max_attempts = cfg.failover.max_attempts;

    let mut walk = FailoverWalk::new(caller, model.targets.len());
    let mut last_ctx: Option<(StreamContext, Instant)> = None;
    for (index, target) in model.targets.iter().enumerate() {
        if walk.attempts >= max_attempts || Instant::now() >= deadline {
            break;
        }
        let Some(provider) = cfg.provider(&target.provider) else {
            continue;
        };
        let circuit_key = target_key(target);
        if let CircuitDecision::Skip = snapshot.target_circuits.allow(&circuit_key) {
            walk.skipped_open.push(circuit_key);
            continue;
        }
        let Some(plan) = snapshot
            .credentials
            .plan(cfg, &caller.namespace, &provider.id)
        else {
            walk.note_missing_credential(&provider.id);
            continue;
        };
        // The stream path uses the first credential in the pool; per-credential
        // rotation mid-stream (skip-on-429) is a follow-up.
        let Some(lease) = plan.attempts.first() else {
            walk.note_missing_credential(&provider.id);
            continue;
        };

        let upstream = Upstream {
            base_url: provider.base_url.clone(),
            api_key: lease.secret.clone(),
            auth: auth_scheme(provider.kind),
        };
        let mut req_body = body.clone();
        req_body["model"] = Value::String(target.model.clone());
        let mut ctx = StreamContext {
            namespace: caller.namespace.clone(),
            subject: caller.subject.clone(),
            alias: alias.clone(),
            target_provider: target.provider.clone(),
            target_model: target.model.clone(),
            source: plan.source,
            credential_id: lease.id.clone(),
            trace_id: telemetry::trace_id(),
            price: target.price,
            budget_key: hold.key.clone(),
            reservation: hold.reservation.clone(),
            estimated_input_tokens: hold.estimated_input_tokens,
            attempts: 0,
        };

        let adapter = adapter_for(provider.kind);
        // Decoder creation is a property of the provider kind, not the upstream,
        // so its failure is the same for every remaining target — surface it
        // rather than failing over. A native stream is relayed verbatim, so its
        // decoder only observes usage.
        let decoder = match wire.route {
            Route::ChatCompletions => match adapter.stream_decoder(Surface::ChatCompletions) {
                Ok(decoder) => decoder,
                Err(err) => {
                    state.0.budget.release(&hold.key, &hold.reservation).await;
                    return Err(err.into());
                }
            },
            _ => Box::new(NativeMessagesDecoder::new()) as Box<dyn ProviderStreamDecoder>,
        };
        let started = Instant::now();
        let dispatcher = &state.0.dispatcher;
        let opened = match wire.route {
            Route::ChatCompletions => {
                let request = ProviderRequest {
                    model: target.model.clone(),
                    body: req_body,
                };
                streaming::open_stream(
                    &ctx,
                    walk.attempts,
                    dispatcher.dispatch_stream(
                        adapter.as_ref(),
                        &upstream,
                        Surface::ChatCompletions,
                        request,
                    ),
                )
                .await
            }
            _ => {
                let call = wire.call(req_body, adapter.name());
                streaming::open_stream(
                    &ctx,
                    walk.attempts,
                    dispatcher.send_stream(&upstream, &call),
                )
                .await
            }
        };
        walk.attempts += 1;
        ctx.attempts = walk.attempts;

        match opened {
            Ok(bytes) => {
                record_target_success(snapshot, target, &circuit_key);
                telemetry::record_routing(
                    &ctx.namespace,
                    &ctx.subject,
                    &ctx.alias,
                    &ctx.target_provider,
                    &ctx.target_model,
                    UsageRecord::credential_source_str(ctx.source),
                );
                return Ok(streaming::relay_opened(
                    state.clone(),
                    ctx,
                    decoder,
                    bytes,
                    started,
                    wire.route.framing(),
                ));
            }
            Err(err) => {
                record_target_failure(snapshot, target, &circuit_key, &err);
                let has_next = index + 1 < walk.total
                    && walk.attempts < max_attempts
                    && Instant::now() < deadline;
                let decision = policy.decide(&as_provider_error(&err), has_next);
                last_ctx = Some((ctx, started));
                walk.last_error = Some(err);
                if decision == FailoverDecision::Return {
                    break;
                }
            }
        }
    }

    if let Some(err) = walk.last_error.take() {
        if let Some((mut ctx, started)) = last_ctx {
            ctx.attempts = walk.attempts;
            streaming::settle_upstream_error(state.clone(), ctx, started);
        } else {
            state.0.budget.release(&hold.key, &hold.reservation).await;
        }
        return Err(err.into());
    }
    state.0.budget.release(&hold.key, &hold.reservation).await;
    Err(walk.into_error())
}

/// One streamed request as the failover walk sees it: the alias it resolved,
/// the body to forward, the wire it speaks, and the budget hold it was admitted
/// under.
struct StreamRequest<'a> {
    alias: String,
    body: Value,
    wire: &'a Wire,
    hold: BudgetHold,
}

/// The budget reservation a request is dispatched under, plus the input-token
/// estimate it was priced from. The streaming relay needs both: the hold to
/// settle, and the estimate to price a stream that ends before the provider
/// reports authoritative usage.
struct BudgetHold {
    key: BudgetKey,
    reservation: Reservation,
    estimated_input_tokens: u64,
}

/// Mutable bookkeeping shared by the buffered and streaming failover walks: how
/// many upstream attempts have been made, which targets were circuit-skipped,
/// and the reason to surface if nothing ever dispatched.
struct FailoverWalk {
    namespace: String,
    total: usize,
    attempts: u32,
    skipped_open: Vec<String>,
    no_credential: Option<GatewayError>,
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
            last: None,
            last_error: None,
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
            .unwrap_or_else(|| ProviderError::InvalidRequest("no attemptable target".into()).into())
    }
}

/// The circuit-breaker key for a target: its qualified `provider/model`, so two
/// aliases pointing at the same concrete target share one breaker.
fn target_key(target: &Target) -> String {
    FailoverTarget::new(&target.provider, &target.model).qualified_model()
}

fn auth_scheme(kind: ProviderKind) -> AuthScheme {
    match kind {
        ProviderKind::Anthropic => AuthScheme::Header("x-api-key"),
        ProviderKind::Openai | ProviderKind::OpenaiCompatible => AuthScheme::Bearer,
    }
}

fn record_target_success(snapshot: &ConfigSnapshot, target: &Target, circuit_key: &str) {
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
/// without opening the target's breaker.
fn record_target_failure(
    snapshot: &ConfigSnapshot,
    target: &Target,
    circuit_key: &str,
    err: &TransportError,
) {
    if as_provider_error(err).affects_provider_health() && !is_credential_exhausted(err) {
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
fn as_provider_error(err: &TransportError) -> ProviderError {
    match err {
        TransportError::Provider(pe) => pe.clone(),
        TransportError::Http(message) => ProviderError::transport("upstream", message.clone()),
    }
}

/// The upstream attempt that terminated the request, plus the credential that
/// made it (for attribution).
struct PooledAttempt {
    result: Result<ProviderResponse, TransportError>,
    credential_id: String,
}

/// Walk the credential pool: dispatch with the first credential, and on a
/// credential-scoped failure (rate limit / quota) park that credential and
/// retry the *same* target with the next one. Target-level failover is a
/// separate concern and is not attempted here.
async fn dispatch_over_pool(
    state: &AppState,
    snapshot: &ConfigSnapshot,
    provider: &Provider,
    plan: &CredentialPlan,
    target_model: &str,
    body: Value,
    wire: &Wire,
) -> PooledAttempt {
    let adapter = adapter_for(provider.kind);
    let mut exhausted: Option<PooledAttempt> = None;

    for lease in &plan.attempts {
        let upstream = Upstream {
            base_url: provider.base_url.clone(),
            api_key: lease.secret.clone(),
            auth: match provider.kind {
                ProviderKind::Anthropic => AuthScheme::Header("x-api-key"),
                ProviderKind::Openai | ProviderKind::OpenaiCompatible => AuthScheme::Bearer,
            },
        };
        let result = match wire.route {
            Route::ChatCompletions => {
                let request = ProviderRequest {
                    model: target_model.to_string(),
                    body: body.clone(),
                };
                state
                    .0
                    .dispatcher
                    .dispatch(
                        adapter.as_ref(),
                        &upstream,
                        Surface::ChatCompletions,
                        request,
                    )
                    .await
            }
            // Native: the provider's answer is handed back untouched, and only
            // its usage block is read into the canonical record.
            route => state
                .0
                .dispatcher
                .send(&upstream, &wire.call(body.clone(), adapter.name()))
                .await
                .map(|body| ProviderResponse {
                    usage: route.native_usage(&body),
                    body,
                }),
        };
        match result {
            Ok(response) => {
                snapshot.credentials.record_success(lease);
                return PooledAttempt {
                    result: Ok(response),
                    credential_id: lease.id.clone(),
                };
            }
            Err(err) if is_credential_exhausted(&err) => {
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
fn is_credential_exhausted(err: &TransportError) -> bool {
    let TransportError::Provider(ProviderError::Dependency(failures)) = err else {
        return false;
    };
    failures.iter().any(|failure| failure.status == Some(429))
}

fn to_usage(u: &gateway_core::ModelUsage) -> Usage {
    Usage {
        input_tokens: u.input_tokens,
        output_tokens: u.output_tokens,
        reasoning_tokens: u.reasoning_tokens,
        cache_read_tokens: u.cache_read_tokens,
        cache_write_tokens: u.cache_write_tokens,
    }
}

/// Conservative pre-dispatch usage estimate: input tokens from the request body
/// (~4 chars/token) plus a reserved output allowance (`max_tokens` when present,
/// else a default). Priced with a target's `ModelPrice` it becomes the held
/// estimate, which settlement replaces with the real cost.
fn estimate_usage(body: &Value) -> Usage {
    const DEFAULT_MAX_OUTPUT_TOKENS: u64 = 1_024;
    let input_tokens = (serde_json::to_string(body).map(|s| s.len()).unwrap_or(0) / 4) as u64;
    let output_tokens = body
        .get("max_tokens")
        .or_else(|| body.get("max_completion_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(DEFAULT_MAX_OUTPUT_TOKENS);
    Usage {
        input_tokens,
        output_tokens,
        reasoning_tokens: 0,
        cache_read_tokens: 0,
        cache_write_tokens: 0,
    }
}

/// Monotonic per-process request id. The trace it belongs to travels in the
/// record's `trace_id`, which a caller's whole agent loop shares. `pub` so the
/// streaming relay can stamp the same id on its settled usage record.
pub fn next_request_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    format!("req_{:016x}", COUNTER.fetch_add(1, Ordering::Relaxed))
}

struct RecordArgs<'a> {
    caller: &'a InboundKey,
    alias: &'a str,
    target_provider: &'a str,
    target_model: &'a str,
    source: CredentialSource,
    credential_id: &'a str,
    status: Status,
    input_tokens: u64,
    output_tokens: u64,
    cost_microdollars: u64,
    latency_ms: u64,
    /// Time to the first token, when one was produced.
    ttft_ms: Option<u64>,
    /// Upstream attempts made; the retry count is one less.
    attempts: u32,
}

async fn record_usage(state: &AppState, args: RecordArgs<'_>) {
    let ttft_ms = args.ttft_ms;
    let attempts = args.attempts;
    let record = UsageRecord {
        schema_version: UsageRecord::SCHEMA_VERSION,
        request_id: next_request_id(),
        trace_id: telemetry::trace_id(),
        namespace: args.caller.namespace.clone(),
        subject: args.caller.subject.clone(),
        model: args.alias.to_string(),
        target_provider: args.target_provider.to_string(),
        target_model: args.target_model.to_string(),
        credential_source: UsageRecord::credential_source_str(args.source),
        credential_id: args.credential_id.to_string(),
        status: args.status,
        input_tokens: args.input_tokens,
        output_tokens: args.output_tokens,
        cost_microdollars: args.cost_microdollars,
        catalog_version: 0,
        latency_ms: args.latency_ms,
        attempts,
    };
    telemetry::record_request(&record, ttft_ms, attempts);
    state.0.usage.record(&record).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::budget::NoBudget;
    use crate::config::Config;
    use crate::usage::{StdoutSink, UsageFanout, UsageSink};
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tower::util::ServiceExt;

    fn test_state() -> AppState {
        let cfg = Config::from_toml_str(
            r#"
[[namespace]]
id = "platform"
default = true

[[provider]]
id = "openai"
kind = "openai"
base_url = "https://api.openai.com/v1"

[[model]]
name = "gpt-4o"
targets = [{ provider = "openai", model = "gpt-4o", price = { input_microdollars_per_million = 2500000, output_microdollars_per_million = 10000000 } }]
"#,
        )
        .unwrap();
        let sinks: Vec<Box<dyn UsageSink>> = vec![Box::new(StdoutSink)];
        AppState::new(
            cfg,
            &HashMap::new(),
            UsageFanout::new(sinks),
            Box::new(NoBudget),
        )
        .expect("no credentials to resolve")
    }

    #[tokio::test]
    async fn healthz_is_ok() {
        let resp = router(test_state())
            .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn models_lists_configured_aliases() {
        let resp = router(test_state())
            .oneshot(Request::get("/v1/models").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let json: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["data"][0]["id"], "gpt-4o");
    }

    #[tokio::test]
    async fn unknown_model_is_typed_404_not_a_missing_route() {
        let body = serde_json::to_vec(&json!({"model": "nope", "messages": []})).unwrap();
        let resp = router(test_state())
            .oneshot(
                Request::post("/v1/chat/completions")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let json: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["error"]["type"], "unknown_model");
    }

    /// Deferred past beta, but the route still answers for itself: a caller
    /// cannot tell a missing route from a misconfigured `base_url`.
    #[tokio::test]
    async fn the_responses_route_is_a_typed_501_that_names_its_deferral() {
        let resp = router(test_state())
            .oneshot(Request::post("/v1/responses").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let json: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["error"]["type"], "not_implemented");
        let message = json["error"]["message"].as_str().unwrap();
        assert!(message.contains("/v1/chat/completions"), "{message}");
    }

    /// An alias whose targets cannot speak the route's wire is the caller's
    /// mistake, answered as a typed 4xx before anything is dispatched — there is
    /// no translation to fall back on for a native route.
    #[tokio::test]
    async fn an_openai_only_alias_on_the_native_route_is_a_typed_4xx() {
        let body = serde_json::to_vec(&json!({ "model": "gpt-4o", "messages": [] })).unwrap();
        let resp = router(test_state())
            .oneshot(
                Request::post("/v1/messages")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let json: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["error"]["type"], "unsupported_wire");
    }

    /// Collects records so attribution can be asserted.
    #[derive(Clone, Default)]
    struct CapturingSink(Arc<Mutex<Vec<UsageRecord>>>);

    #[async_trait::async_trait]
    impl UsageSink for CapturingSink {
        fn name(&self) -> &'static str {
            "capture"
        }

        async fn record(&self, record: &UsageRecord) {
            self.0.lock().unwrap().push(record.clone());
        }
    }

    /// A stand-in provider that rate-limits one key and serves the other, so the
    /// pool walk is exercised over real HTTP.
    async fn rate_limiting_upstream(exhausted_key: &'static str) -> String {
        let app = Router::new().route(
            "/chat/completions",
            post(move |headers: HeaderMap| async move {
                let authorized = headers
                    .get(axum::http::header::AUTHORIZATION)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.strip_prefix("Bearer "))
                    .unwrap_or_default()
                    != exhausted_key;
                if authorized {
                    (
                        StatusCode::OK,
                        Json(json!({
                            "id": "chatcmpl-1",
                            "choices": [],
                            "usage": { "prompt_tokens": 10, "completion_tokens": 5 }
                        })),
                    )
                } else {
                    (
                        StatusCode::TOO_MANY_REQUESTS,
                        Json(json!({ "error": { "message": "rate limit exceeded" } })),
                    )
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}")
    }

    #[tokio::test]
    async fn a_rate_limited_credential_falls_to_the_next_and_is_attributed() {
        let base_url = rate_limiting_upstream("sk-exhausted").await;
        let cfg = Config::from_toml_str(&format!(
            r#"
[[namespace]]
id = "platform"
default = true

[[provider]]
id = "openai"
kind = "openai"
base_url = "{base_url}"

[[credential]]
namespace = "platform"
provider = "openai"
env = "K1"
id = "openai-a"

[[credential]]
namespace = "platform"
provider = "openai"
env = "K2"
id = "openai-b"

[[model]]
name = "gpt-4o"
targets = [{{ provider = "openai", model = "gpt-4o", price = {{ input_microdollars_per_million = 2500000, output_microdollars_per_million = 10000000 }} }}]
"#
        ))
        .unwrap();
        let env: HashMap<String, String> = [("K1", "sk-exhausted"), ("K2", "sk-good")]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let captured = CapturingSink::default();
        let sinks: Vec<Box<dyn UsageSink>> = vec![Box::new(captured.clone())];
        let state = AppState::new(cfg, &env, UsageFanout::new(sinks), Box::new(NoBudget)).unwrap();

        let body = serde_json::to_vec(&json!({"model": "gpt-4o", "messages": []})).unwrap();
        let resp = router(state)
            .oneshot(
                Request::post("/v1/chat/completions")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let records = captured.0.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].credential_id, "openai-b");
        assert_eq!(records[0].credential_source, "platform");
        // The pool made one target attempt (credential rotation is inner).
        assert_eq!(records[0].attempts, 1);
    }

    /// A stand-in provider whose health is flipped at test time, counting the
    /// requests that actually reached it. Serves `200` while `healthy`, and the
    /// given status otherwise.
    async fn controllable_upstream(
        healthy: Arc<AtomicBool>,
        unhealthy_status: StatusCode,
    ) -> (String, Arc<AtomicUsize>) {
        let hits = Arc::new(AtomicUsize::new(0));
        let counter = hits.clone();
        let app = Router::new().route(
            "/chat/completions",
            post(move || {
                let healthy = healthy.clone();
                let counter = counter.clone();
                async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    if healthy.load(Ordering::SeqCst) {
                        (
                            StatusCode::OK,
                            Json(json!({
                                "id": "chatcmpl-1",
                                "choices": [],
                                "usage": { "prompt_tokens": 10, "completion_tokens": 5 }
                            })),
                        )
                            .into_response()
                    } else {
                        (
                            unhealthy_status,
                            Json(json!({ "error": { "message": "upstream is unwell" } })),
                        )
                            .into_response()
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}"), hits)
    }

    /// Two targets (`pa/m-a` then `pb/m-b`) behind one alias, sharing one
    /// `AppState` so the per-target circuit persists across requests.
    fn two_target_state(
        url_a: &str,
        url_b: &str,
        failover: &str,
        captured: CapturingSink,
    ) -> AppState {
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

[[credential]]
namespace = "platform"
provider = "pa"
env = "KA"
id = "cred-a"

[[credential]]
namespace = "platform"
provider = "pb"
env = "KB"
id = "cred-b"

{failover}

[[model]]
name = "gpt-4o"
targets = [
  {{ provider = "pa", model = "m-a", price = {{ input_microdollars_per_million = 1000000, output_microdollars_per_million = 1000000 }} }},
  {{ provider = "pb", model = "m-b", price = {{ input_microdollars_per_million = 1000000, output_microdollars_per_million = 1000000 }} }},
]
"#
        ))
        .unwrap();
        let env: HashMap<String, String> = [("KA", "ka"), ("KB", "kb")]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let sinks: Vec<Box<dyn UsageSink>> = vec![Box::new(captured)];
        AppState::new(cfg, &env, UsageFanout::new(sinks), Box::new(NoBudget)).unwrap()
    }

    fn chat_request() -> Request<Body> {
        let body = serde_json::to_vec(&json!({"model": "gpt-4o", "messages": []})).unwrap();
        Request::post("/v1/chat/completions")
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap()
    }

    /// Records what each request held and what it settled for, so the charging
    /// policy is asserted through the real request path.
    #[derive(Default, Clone)]
    struct RecordingBudget(Arc<Mutex<Vec<(u64, u64)>>>);

    #[async_trait::async_trait]
    impl crate::budget::BudgetStore for RecordingBudget {
        fn name(&self) -> &'static str {
            "recording"
        }
        async fn reserve(&self, _key: &BudgetKey, estimated_microdollars: u64) -> Admission {
            self.0.lock().unwrap().push((estimated_microdollars, 0));
            Admission::Allowed(Reservation {
                id: "recording".to_owned(),
                estimate_microdollars: estimated_microdollars,
            })
        }
        async fn settle(
            &self,
            _key: &BudgetKey,
            _reservation: &Reservation,
            actual_microdollars: u64,
        ) {
            if let Some(last) = self.0.lock().unwrap().last_mut() {
                last.1 = actual_microdollars;
            }
        }
    }

    /// One target, one credential, and the budget store under test.
    fn budgeted_state(base_url: &str, budget: Box<dyn crate::budget::BudgetStore>) -> AppState {
        let cfg = Config::from_toml_str(&format!(
            r#"
[[namespace]]
id = "platform"
default = true

[[provider]]
id = "openai"
kind = "openai"
base_url = "{base_url}"

[[credential]]
namespace = "platform"
provider = "openai"
env = "K1"

[[model]]
name = "gpt-4o"
targets = [{{ provider = "openai", model = "gpt-4o", price = {{ input_microdollars_per_million = 1000000, output_microdollars_per_million = 1000000 }} }}]
"#
        ))
        .unwrap();
        let env = HashMap::from([("K1".to_owned(), "sk-test".to_owned())]);
        let sinks: Vec<Box<dyn UsageSink>> = vec![Box::new(StdoutSink)];
        AppState::new(cfg, &env, UsageFanout::new(sinks), budget).unwrap()
    }

    /// The reserved estimate is a ceiling, not the charge: a completed request
    /// settles the cost of the usage the provider reported.
    #[tokio::test]
    async fn a_buffered_response_settles_its_measured_cost() {
        let (base_url, _) =
            controllable_upstream(Arc::new(AtomicBool::new(true)), StatusCode::OK).await;
        let budget = RecordingBudget::default();
        let state = budgeted_state(&base_url, Box::new(budget.clone()));

        let resp = router(state).oneshot(chat_request()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let ledger = budget.0.lock().unwrap();
        let (estimated, settled) = ledger[0];
        // 10 input + 5 output tokens at 1 µ$ each.
        assert_eq!(settled, 15);
        assert!(
            estimated > settled,
            "the estimate should be the conservative ceiling ({estimated} vs {settled})"
        );
    }

    /// A buffered failure reports no usage at all, so the spend is unknowable
    /// and charged as zero — and the hold is released rather than left to
    /// expire (ADR 0010).
    #[tokio::test]
    async fn a_buffered_upstream_failure_charges_nothing_and_releases_its_hold() {
        let (base_url, _) = controllable_upstream(
            Arc::new(AtomicBool::new(false)),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
        let budget = RecordingBudget::default();
        let state = budgeted_state(&base_url, Box::new(budget.clone()));

        let resp = router(state).oneshot(chat_request()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);

        let ledger = budget.0.lock().unwrap();
        assert_eq!(ledger.len(), 1);
        assert_eq!(ledger[0].1, 0);
    }

    /// The two denials are different answers to the caller: over-cap is the
    /// caller's problem, an unenforceable cap is the gateway's.
    #[tokio::test]
    async fn a_denied_request_never_reaches_the_provider() {
        struct Denying(Denial);

        #[async_trait::async_trait]
        impl crate::budget::BudgetStore for Denying {
            fn name(&self) -> &'static str {
                "denying"
            }
            async fn reserve(&self, _key: &BudgetKey, _estimated: u64) -> Admission {
                Admission::Denied(self.0)
            }
            async fn settle(&self, _key: &BudgetKey, _reservation: &Reservation, _actual: u64) {}
        }

        let (base_url, hits) =
            controllable_upstream(Arc::new(AtomicBool::new(true)), StatusCode::OK).await;
        for (denial, expected) in [
            (Denial::Exceeded, StatusCode::TOO_MANY_REQUESTS),
            (Denial::StoreUnavailable, StatusCode::SERVICE_UNAVAILABLE),
        ] {
            let state = budgeted_state(&base_url, Box::new(Denying(denial)));
            let resp = router(state).oneshot(chat_request()).await.unwrap();
            assert_eq!(resp.status(), expected);
        }
        assert_eq!(hits.load(Ordering::SeqCst), 0);
    }

    /// The cap is per `(namespace, subject)`, and two gateways sharing one store
    /// see each other's spend — the fleet enforces one cap, not one per replica.
    #[tokio::test]
    async fn two_replicas_sharing_one_store_enforce_a_single_cap() {
        let (base_url, _) =
            controllable_upstream(Arc::new(AtomicBool::new(true)), StatusCode::OK).await;
        // `max_tokens` bounds the reserved output allowance, so the estimate is
        // small and known: 16 output tokens plus the body's input estimate.
        let capped_request = || {
            let body =
                serde_json::to_vec(&json!({"model": "gpt-4o", "messages": [], "max_tokens": 16}))
                    .unwrap();
            Request::post("/v1/chat/completions")
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap()
        };
        let shared = Arc::new(crate::budget::InMemoryBudget::new(30));

        struct Shared(Arc<crate::budget::InMemoryBudget>);

        #[async_trait::async_trait]
        impl crate::budget::BudgetStore for Shared {
            fn name(&self) -> &'static str {
                "shared"
            }
            async fn reserve(&self, key: &BudgetKey, estimated: u64) -> Admission {
                self.0.reserve(key, estimated).await
            }
            async fn settle(&self, key: &BudgetKey, reservation: &Reservation, actual: u64) {
                self.0.settle(key, reservation, actual).await;
            }
        }

        let replica_a = router(budgeted_state(&base_url, Box::new(Shared(shared.clone()))));
        let replica_b = router(budgeted_state(&base_url, Box::new(Shared(shared))));

        let first = replica_a.oneshot(capped_request()).await.unwrap();
        assert_eq!(first.status(), StatusCode::OK);
        // The 15 µ$ the other replica settled leaves no room for a second
        // estimate under the shared cap.
        let second = replica_b.oneshot(capped_request()).await.unwrap();
        assert_eq!(second.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    #[tokio::test]
    async fn a_retryable_first_target_fails_over_to_the_second() {
        let (url_a, hits_a) = controllable_upstream(
            Arc::new(AtomicBool::new(false)),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
        let (url_b, hits_b) =
            controllable_upstream(Arc::new(AtomicBool::new(true)), StatusCode::OK).await;
        let captured = CapturingSink::default();
        let state = two_target_state(&url_a, &url_b, "", captured.clone());

        let resp = router(state).oneshot(chat_request()).await.unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(hits_a.load(Ordering::SeqCst), 1);
        assert_eq!(hits_b.load(Ordering::SeqCst), 1);
        let records = captured.0.lock().unwrap();
        assert_eq!(records.len(), 1);
        // The second target served, and both target attempts are attributed.
        assert_eq!(records[0].status.as_str(), "ok");
        assert_eq!(records[0].target_provider, "pb");
        assert_eq!(records[0].target_model, "m-b");
        assert_eq!(records[0].credential_id, "cred-b");
        assert_eq!(records[0].attempts, 2);
    }

    #[tokio::test]
    async fn a_non_retryable_error_is_not_failed_over() {
        let (url_a, hits_a) =
            controllable_upstream(Arc::new(AtomicBool::new(false)), StatusCode::BAD_REQUEST).await;
        let (url_b, hits_b) =
            controllable_upstream(Arc::new(AtomicBool::new(true)), StatusCode::OK).await;
        let captured = CapturingSink::default();
        let state = two_target_state(&url_a, &url_b, "", captured.clone());

        let resp = router(state).oneshot(chat_request()).await.unwrap();

        // A 4xx-class (non-retryable) error stops the walk: the error is returned
        // and the second target is never tried.
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(hits_a.load(Ordering::SeqCst), 1);
        assert_eq!(hits_b.load(Ordering::SeqCst), 0);
        let records = captured.0.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].status.as_str(), "upstream_error");
        assert_eq!(records[0].target_provider, "pa");
        assert_eq!(records[0].attempts, 1);
    }

    /// What the upstream was actually sent, so passthrough can be asserted from
    /// the provider's side as well as the caller's.
    type Received = Arc<Mutex<Vec<(Value, HeaderMap)>>>;

    /// A stand-in provider serving one native path, answering with a fixed body
    /// (or SSE text) and recording every request it received.
    async fn native_upstream(path: &'static str, answer: Response) -> (String, Received) {
        let received: Received = Arc::new(Mutex::new(Vec::new()));
        let seen = received.clone();
        let answer = Arc::new(Mutex::new(Some(answer)));
        let app = Router::new().route(
            path,
            post(move |headers: HeaderMap, Json(body): Json<Value>| {
                let seen = seen.clone();
                let answer = answer.clone();
                async move {
                    seen.lock().unwrap().push((body, headers));
                    answer.lock().unwrap().take().expect("one request")
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}"), received)
    }

    /// One alias over one target of the given provider kind, at 1 µ$/token both
    /// ways so a settled cost reads as a token count.
    fn native_state(kind: &str, base_url: &str, captured: CapturingSink) -> AppState {
        let cfg = Config::from_toml_str(&format!(
            r#"
[[namespace]]
id = "platform"
default = true

[[provider]]
id = "p"
kind = "{kind}"
base_url = "{base_url}"

[[credential]]
namespace = "platform"
provider = "p"
env = "K1"
id = "cred-a"

[[model]]
name = "alias"
targets = [{{ provider = "p", model = "upstream-model", price = {{ input_microdollars_per_million = 1000000, output_microdollars_per_million = 1000000 }} }}]
"#
        ))
        .unwrap();
        let env = HashMap::from([("K1".to_owned(), "sk-test".to_owned())]);
        let sinks: Vec<Box<dyn UsageSink>> = vec![Box::new(captured)];
        AppState::new(cfg, &env, UsageFanout::new(sinks), Box::new(NoBudget)).unwrap()
    }

    /// The point of serving the native wire: a body a translation would mangle
    /// (a signed thinking block, a tool-use block) crosses the gateway
    /// unchanged, and only `model` differs from what the caller sent.
    #[tokio::test]
    async fn a_native_message_is_forwarded_verbatim_with_only_the_model_rewritten() {
        let upstream_answer = json!({
            "id": "msg_1",
            "type": "message",
            "role": "assistant",
            "content": [
                { "type": "thinking", "thinking": "deliberating", "signature": "sig-abc" },
                { "type": "tool_use", "id": "toolu_1", "name": "search", "input": { "q": "x" } }
            ],
            "stop_reason": "tool_use",
            "usage": {
                "input_tokens": 10,
                "output_tokens": 5,
                "cache_creation_input_tokens": 2,
                "cache_read_input_tokens": 1
            }
        });
        let (base_url, received) =
            native_upstream("/messages", Json(upstream_answer.clone()).into_response()).await;
        let captured = CapturingSink::default();
        let state = native_state("anthropic", &base_url, captured.clone());

        let sent = json!({
            "model": "alias",
            "max_tokens": 64,
            "thinking": { "type": "enabled", "budget_tokens": 32 },
            "messages": [{
                "role": "assistant",
                "content": [
                    { "type": "thinking", "thinking": "earlier", "signature": "sig-prior" }
                ]
            }],
            "tools": [{ "name": "search", "input_schema": { "type": "object" } }]
        });
        let resp = router(state)
            .oneshot(
                Request::post("/v1/messages")
                    .header("content-type", "application/json")
                    .header("anthropic-version", "2099-01-01")
                    .body(Body::from(serde_json::to_vec(&sent).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let returned: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(returned, upstream_answer);

        let requests = received.lock().unwrap();
        let (forwarded, headers) = &requests[0];
        let mut expected = sent.clone();
        expected["model"] = json!("upstream-model");
        assert_eq!(forwarded, &expected);
        // The caller's pinned wire version is honoured, and the provider key is
        // injected in Anthropic's own scheme.
        assert_eq!(headers["anthropic-version"], "2099-01-01");
        assert_eq!(headers["x-api-key"], "sk-test");

        let records = captured.0.lock().unwrap();
        assert_eq!(records[0].input_tokens, 10);
        assert_eq!(records[0].output_tokens, 5);
        // Anthropic's cache counters are billed too, at the input rate here.
        assert_eq!(records[0].cost_microdollars, 18);
        assert_eq!(records[0].status.as_str(), "ok");
    }

    /// Embeddings have no completion to bill, so the record carries input only
    /// even when the provider reports something else.
    #[tokio::test]
    async fn embeddings_pass_through_and_bill_input_only() {
        let answer = json!({
            "object": "list",
            "data": [{ "object": "embedding", "index": 0, "embedding": [0.25, -0.5] }],
            "usage": { "prompt_tokens": 8, "total_tokens": 8, "completion_tokens": 4 }
        });
        let (base_url, received) =
            native_upstream("/embeddings", Json(answer.clone()).into_response()).await;
        let captured = CapturingSink::default();
        let state = native_state("openai", &base_url, captured.clone());

        let sent = json!({ "model": "alias", "input": ["one", "two"], "dimensions": 2 });
        let resp = router(state)
            .oneshot(
                Request::post("/v1/embeddings")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&sent).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(serde_json::from_slice::<Value>(&bytes).unwrap(), answer);

        // Forwarded as sent but for the model — notably without a `stream` field,
        // which the embeddings endpoint does not accept.
        let requests = received.lock().unwrap();
        let (forwarded, _) = &requests[0];
        assert_eq!(
            forwarded,
            &json!({ "model": "upstream-model", "input": ["one", "two"], "dimensions": 2 })
        );

        let records = captured.0.lock().unwrap();
        assert_eq!(records[0].input_tokens, 8);
        assert_eq!(records[0].output_tokens, 0);
        assert_eq!(records[0].cost_microdollars, 8);
    }

    #[tokio::test]
    async fn a_tripped_target_is_skipped_then_recovers_via_a_half_open_probe() {
        let healthy_a = Arc::new(AtomicBool::new(false));
        let (url_a, hits_a) =
            controllable_upstream(healthy_a.clone(), StatusCode::INTERNAL_SERVER_ERROR).await;
        let (url_b, hits_b) =
            controllable_upstream(Arc::new(AtomicBool::new(true)), StatusCode::OK).await;
        let captured = CapturingSink::default();
        // One failure trips the target; a 1s cooldown then allows a probe.
        let failover = "[failover]\nmax_attempts = 3\noverall_timeout_ms = 30000\nfailure_threshold = 1\ncooldown_seconds = 1";
        let state = two_target_state(&url_a, &url_b, failover, captured.clone());

        // Request 1: pa fails (trips its circuit) and pb serves.
        let resp = router(state.clone()).oneshot(chat_request()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(hits_a.load(Ordering::SeqCst), 1);
        assert_eq!(hits_b.load(Ordering::SeqCst), 1);

        // Request 2: pa's circuit is open, so it is skipped entirely — pb serves
        // on the first attempt without pa being touched again.
        let resp = router(state.clone()).oneshot(chat_request()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(hits_a.load(Ordering::SeqCst), 1);
        assert_eq!(hits_b.load(Ordering::SeqCst), 2);
        {
            let records = captured.0.lock().unwrap();
            assert_eq!(records[1].target_provider, "pb");
            assert_eq!(records[1].attempts, 1);
        }

        // pa recovers; after the cooldown a single half-open probe reaches it.
        healthy_a.store(true, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(1_100)).await;

        let resp = router(state).oneshot(chat_request()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(hits_a.load(Ordering::SeqCst), 2);
        let records = captured.0.lock().unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(records[2].target_provider, "pa");
        assert_eq!(records[2].attempts, 1);
    }
}
