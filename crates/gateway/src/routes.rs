//! HTTP surface.
//!
//! Passthrough-first (delta A1): the OpenAI-shaped `/v1/chat/completions` route
//! forwards the caller's body to a same-shaped upstream and only rewrites the
//! `model` field to the resolved target. Cross-provider translation (e.g.
//! routing an OpenAI request to Anthropic) is deferred, so an alias whose
//! targets cannot serve a route's wire is rejected up front rather than
//! dispatched (ADR 0012). A `stream: true` request takes the SSE relay in
//! [`crate::streaming`].
//!
//! The provider-native routes — Anthropic's `/v1/messages` and OpenAI-shaped
//! `/v1/embeddings` — take the same path (ADR 0012). A caller already speaking
//! the target's wire has its body forwarded to the provider's own endpoint with
//! only `model` rewritten, so signed thinking and tool-use blocks survive intact
//! (verbatim bytes when streamed, re-serialized values when buffered); only how
//! usage is read back differs per route. `/v1/responses`
//! is deferred past beta and answers with a typed `501` rather than being a
//! missing route (delta B3).
//!
//! An alias's `targets` are tried in configured order (ADR 0008). The failover
//! walk is the *outer* loop around credential-pool dispatch: each target has an
//! in-memory per-target circuit breaker, a retryable upstream failure advances
//! to the next target, and the walk is bounded by both a total attempt count and
//! an overall wall-clock budget. Streaming rotates credentials while opening on
//! both wires, and may rotate after an OpenAI-framed stream fails before content
//! is emitted; native streams and partially delivered streams remain terminal.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::{Extension, RawQuery, Request, State};
use axum::http::HeaderMap;
use axum::middleware::{Next, from_fn_with_state};
use axum::response::{IntoResponse, Response};
use axum::routing::{MethodRouter, get, post};
use axum::{Json, Router};
use gateway_core::{
    CircuitDecision, FailoverDecision, FailoverPolicy, FailoverTarget, ModelPrice, ModelUsage,
    NativeMessagesDecoder, ProviderError, ProviderRequest, ProviderResponse, ProviderStreamDecoder,
    Surface, Usage,
};
use gateway_transport::{AuthScheme, NativeCall, TransportError, Upstream};
use serde_json::{Value, json};
use tracing::{Instrument, debug, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::budget::{Admission, BudgetKey, Denial, Reservation};
use crate::config::{Model, Provider, ProviderKind, ProviderWire, Target};
use crate::credentials::{CredentialLease, CredentialPlan, CredentialSource, CredentialStatusView};
use crate::error::GatewayError;
use crate::principals::{Capability, Presented, PrincipalStoreError, TokenVerificationError};
use crate::rate_limit::{RateLimitKey, RateLimitPermit};
use crate::state::{AppState, ConfigSnapshot, InboundKey, adapter_for};
use crate::streaming::{self, Framing, StreamContext};
use crate::telemetry;
use crate::usage::{Status, UsageRecord};

pub fn router(state: AppState) -> Router {
    route_specs()
        .into_iter()
        .fold(Router::new(), |router, spec| {
            let route = (spec.router)();
            let route = match spec.auth {
                AuthPosture::LivenessProbe => route,
                AuthPosture::Authenticated => route.layer(from_fn_with_state(
                    (state.clone(), spec.capability),
                    authenticate_middleware,
                )),
            };
            router.route(spec.path, route)
        })
        .with_state(state)
}

/// Whether a route is one of the two unauthenticated liveness probes or must
/// pass inbound authentication before its handler can run.
#[derive(Clone, Copy, PartialEq, Eq)]
enum AuthPosture {
    LivenessProbe,
    Authenticated,
}

/// A route's complete registration: adding a route requires declaring its
/// authentication posture here rather than silently omitting the layer.
struct RouteSpec {
    path: &'static str,
    auth: AuthPosture,
    capability: Option<Capability>,
    router: fn() -> MethodRouter<AppState>,
}

/// The single route table: its posture is the source of truth for registration
/// and for the sweep test that keeps the unauthenticated set closed.
fn route_specs() -> [RouteSpec; 8] {
    [
        RouteSpec {
            path: "/healthz",
            auth: AuthPosture::LivenessProbe,
            capability: None,
            router: || get(healthz),
        },
        RouteSpec {
            path: "/readyz",
            auth: AuthPosture::LivenessProbe,
            capability: None,
            router: || get(readyz),
        },
        RouteSpec {
            path: "/v1/models",
            auth: AuthPosture::Authenticated,
            capability: Some(Capability::Models),
            router: || get(list_models),
        },
        RouteSpec {
            path: "/v1/credentials",
            auth: AuthPosture::Authenticated,
            capability: Some(Capability::Credentials),
            router: || get(list_credentials),
        },
        RouteSpec {
            path: "/v1/chat/completions",
            auth: AuthPosture::Authenticated,
            capability: Some(Capability::Chat),
            router: || post(chat_completions),
        },
        RouteSpec {
            path: "/v1/messages",
            auth: AuthPosture::Authenticated,
            capability: Some(Capability::Messages),
            router: || post(native_messages),
        },
        RouteSpec {
            path: "/v1/embeddings",
            auth: AuthPosture::Authenticated,
            capability: Some(Capability::Embeddings),
            router: || post(embeddings),
        },
        RouteSpec {
            path: "/v1/responses",
            auth: AuthPosture::Authenticated,
            capability: None,
            router: || post(responses),
        },
    ]
}

async fn healthz() -> &'static str {
    "ok"
}

/// Liveness is trivially true; real readiness (config loaded, at least one
/// credential present) is a follow-up — kept honest rather than always-200.
async fn readyz() -> &'static str {
    "ready"
}

/// Replica-local Tier 0 credential status. Presence is expressed by each
/// configured entry (boot resolves it or boot fails), never by an always-true
/// field. Credential ids are attribution labels only; secrets remain write-only.
async fn list_credentials(
    Extension(snapshot): Extension<Arc<ConfigSnapshot>>,
    Extension(caller): Extension<InboundKey>,
    RawQuery(raw_query): RawQuery,
) -> Result<Json<Value>, GatewayError> {
    let namespaces = parse_credential_query(raw_query.as_deref())?;
    let view = match namespaces.as_deref() {
        None => CredentialStatusView::Namespace(&caller.namespace),
        Some("all") => {
            if caller.namespace != snapshot.config.default_namespace()
                || !caller
                    .scope
                    .as_ref()
                    .is_some_and(|scope| scope.contains(&Capability::CredentialsAll))
            {
                return Err(GatewayError::ScopeInsufficient(Capability::CredentialsAll));
            }
            CredentialStatusView::All
        }
        Some(_) => {
            return Err(GatewayError::BadRequest(
                "invalid `namespaces` value".into(),
            ));
        }
    };
    Ok(Json(json!({
        "object": "list",
        "observed": "replica",
        "data": snapshot.credentials.status(&snapshot.config, view),
    })))
}

fn parse_credential_query(raw_query: Option<&str>) -> Result<Option<String>, GatewayError> {
    let mut namespaces = None;
    for pair in raw_query.unwrap_or_default().split('&') {
        if pair.is_empty() {
            continue;
        }
        let (raw_key, raw_value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = decode_query_component(raw_key)?;
        let value = decode_query_component(raw_value)?;
        if key == "namespaces" {
            if namespaces.is_some() {
                return Err(GatewayError::BadRequest(
                    "duplicate query parameter `namespaces`".into(),
                ));
            }
            namespaces = Some(value);
        }
    }
    Ok(namespaces)
}

fn decode_query_component(value: &str) -> Result<String, GatewayError> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'+' => decoded.push(b' '),
            b'%' if index + 2 < bytes.len() => {
                let high = hex_digit(bytes[index + 1]);
                let low = hex_digit(bytes[index + 2]);
                let (Some(high), Some(low)) = (high, low) else {
                    return Err(GatewayError::BadRequest(
                        "invalid query string encoding".into(),
                    ));
                };
                decoded.push((high << 4) | low);
                index += 2;
            }
            b'%' => {
                return Err(GatewayError::BadRequest(
                    "invalid query string encoding".into(),
                ));
            }
            byte => decoded.push(byte),
        }
        index += 1;
    }
    String::from_utf8(decoded)
        .map_err(|_| GatewayError::BadRequest("invalid query string encoding".into()))
}

fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// The model catalog, gated behind a gateway key and scoped to the caller's
/// namespace: a caller sees only the aliases it could actually invoke — those
/// with at least one target whose provider resolves a credential for the
/// caller's namespace (its own, or the platform's when fallback is allowed).
/// So a BYOK tenant cannot enumerate aliases it is not entitled to.
async fn list_models(
    Extension(snapshot): Extension<Arc<ConfigSnapshot>>,
    Extension(caller): Extension<InboundKey>,
) -> Result<Json<Value>, GatewayError> {
    let cfg = &snapshot.config;
    let data: Vec<Value> = cfg
        .model
        .iter()
        .filter(|m| {
            caller
                .alias_scope
                .as_ref()
                .is_none_or(|scope| scope.permits(&m.name))
                && m.targets.iter().any(|t| {
                    snapshot
                        .credentials
                        .is_present(cfg, &caller.namespace, &t.provider)
                })
        })
        .map(|m| json!({ "id": m.name, "object": "model", "owned_by": "axond" }))
        .collect();
    Ok(Json(json!({ "object": "list", "data": data })))
}

/// Resolve the caller's namespace + subject from the inbound key. Every request
/// must present a configured gateway key: authentication fails closed, and a
/// snapshot with no key never reaches a request (ADR 0013).
///
/// The key travels as `Authorization: Bearer` or, because that is what an
/// Anthropic SDK pointed at the gateway sends, as `x-api-key`. Both name the
/// same gateway key; the scheme is the client's, not a second credential space.
async fn authenticate(
    snapshot: &ConfigSnapshot,
    headers: &HeaderMap,
) -> Result<InboundKey, GatewayError> {
    let credential = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .or_else(|| headers.get("x-api-key").and_then(|v| v.to_str().ok()))
        .ok_or(GatewayError::Unauthorized)?;
    let presented = Presented { credential };
    let store = snapshot.principal_store_name(&presented);
    let principal = match snapshot.resolve_principal(&presented).await {
        Ok(principal) => principal,
        Err(PrincipalStoreError::Unauthorized(error)) => {
            debug!(
                store,
                error = %error,
                "token rejected during principal resolution"
            );
            return Err(GatewayError::TokenUnauthorized(error));
        }
        Err(PrincipalStoreError::Forbidden(error)) => {
            debug!(
                store,
                error = %error,
                "token rejected during principal resolution"
            );
            return Err(GatewayError::TokenForbidden(error));
        }
        Err(error) => {
            // A layer error is terminal by design; it must not fall through to
            // another authority just because the owning layer is unavailable.
            warn!(
                store,
                error = %error,
                "principal store resolution failed"
            );
            return Err(GatewayError::Unauthorized);
        }
    };
    principal.ok_or(GatewayError::Unauthorized)
}

/// Authenticate once per request, before handler extractors, and carry the
/// resolved snapshot and caller into the handler. A reload landing mid-request
/// therefore cannot change what this request resolved; failures return `401`
/// before any typed handler error, including `/v1/responses`'s `501`.
async fn authenticate_middleware(
    State((state, capability)): State<(AppState, Option<Capability>)>,
    headers: HeaderMap,
    mut request: Request,
    next: Next,
) -> Result<Response, GatewayError> {
    let snapshot = state.config();
    let caller = authenticate(&snapshot, &headers).await?;
    if let Some(jti) = &caller.jti {
        match state.0.revocation.is_revoked(jti).await {
            Ok(true) => {
                crate::telemetry::metrics::record_revocation_denial();
                return Err(GatewayError::TokenUnauthorized(
                    TokenVerificationError::Revoked,
                ));
            }
            Ok(false) => {}
            Err(crate::revocation::RevocationError::Unavailable { .. }) => {
                crate::telemetry::metrics::record_revocation_unavailable_denial();
                return Err(GatewayError::RevocationUnavailable);
            }
            Err(error) => {
                warn!(error = %error, "revocation store check failed");
                crate::telemetry::metrics::record_revocation_unavailable_denial();
                return Err(GatewayError::RevocationUnavailable);
            }
        }
    }
    if let Some(capability) = capability
        && let Some(scope) = caller.scope.as_ref()
        && (!scope.contains(&capability)
            || !namespace_allows(&snapshot, &caller.namespace, capability))
    {
        debug!(
            namespace = %caller.namespace,
            subject = %caller.subject,
            signer_kid = ?caller.signer_kid,
            %capability,
            "token scope denied route"
        );
        return Err(GatewayError::ScopeInsufficient(capability));
    }
    request.extensions_mut().insert(snapshot);
    request.extensions_mut().insert(caller);
    Ok(next.run(request).await)
}

fn namespace_allows(snapshot: &ConfigSnapshot, namespace: &str, capability: Capability) -> bool {
    let route = match capability {
        Capability::Chat => Some(Route::ChatCompletions),
        Capability::Messages => Some(Route::NativeMessages),
        Capability::Embeddings => Some(Route::Embeddings),
        Capability::Models => None,
        Capability::Credentials | Capability::CredentialsAll => None,
    };
    let Some(route) = route else {
        return true;
    };
    snapshot.config.model.iter().any(|model| {
        model.targets.iter().any(|target| {
            snapshot
                .config
                .provider(&target.provider)
                .is_some_and(|provider| {
                    route.serves(provider.kind)
                        && snapshot.credentials.is_present(
                            &snapshot.config,
                            namespace,
                            &target.provider,
                        )
                })
        })
    })
}

/// The wire shape a route speaks, which is the only thing that differs between
/// the routes: the upstream path, which provider kinds can serve it, and how
/// usage is read out of the provider's answer. Everything else — aliasing,
/// failover, credential pools, budgets, usage — is shared.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Route {
    /// OpenAI-shaped chat, dispatched through the provider adapter to an
    /// OpenAI-family target's `/chat/completions`.
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

    /// Whether a provider of this kind speaks the route's wire shape. No route
    /// translates between wires, so an alias whose target cannot serve the shape
    /// is a configuration mistake worth naming.
    fn serves(self, kind: ProviderKind) -> bool {
        self.wire() == kind.wire()
    }

    fn wire(self) -> ProviderWire {
        match self {
            Self::ChatCompletions | Self::Embeddings => ProviderWire::Openai,
            Self::NativeMessages => ProviderWire::Anthropic,
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
#[derive(Clone)]
struct Wire {
    route: Route,
    headers: Vec<(&'static str, String)>,
}

impl Wire {
    /// Reject an alias whose targets cannot speak the route's wire *before*
    /// anything is reserved or dispatched: no route translates between wires,
    /// and failing over into a target that cannot serve the shape would turn a
    /// config mistake into a confusing upstream `404`.
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
    Extension(snapshot): Extension<Arc<ConfigSnapshot>>,
    Extension(caller): Extension<InboundKey>,
    Json(body): Json<Value>,
) -> Result<Response, GatewayError> {
    serve(
        state,
        headers,
        body,
        Route::ChatCompletions,
        snapshot,
        caller,
    )
    .await
}

/// Anthropic-native Messages. The caller's body already speaks the target's
/// wire, so it is forwarded to the provider's `/messages` untouched but for the
/// `model` alias — which is what keeps signed thinking and tool-use blocks
/// intact through the gateway (ADR 0012).
async fn native_messages(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(snapshot): Extension<Arc<ConfigSnapshot>>,
    Extension(caller): Extension<InboundKey>,
    Json(body): Json<Value>,
) -> Result<Response, GatewayError> {
    serve(
        state,
        headers,
        body,
        Route::NativeMessages,
        snapshot,
        caller,
    )
    .await
}

async fn embeddings(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(snapshot): Extension<Arc<ConfigSnapshot>>,
    Extension(caller): Extension<InboundKey>,
    Json(body): Json<Value>,
) -> Result<Response, GatewayError> {
    serve(state, headers, body, Route::Embeddings, snapshot, caller).await
}

/// Deferred past beta (ADR 0012): the OpenAI Responses API is a stateful
/// surface, and serving it honestly needs more than passthrough. Its route
/// layer authenticates callers before this handler returns the typed `501`,
/// because a missing route is indistinguishable from a misconfigured `base_url`.
async fn responses() -> Result<Json<Value>, GatewayError> {
    Err(GatewayError::NotImplemented(
        "the OpenAI Responses API (`/v1/responses`), deferred past beta by ADR 0012 \
         in favour of `/v1/chat/completions`,",
    ))
}

/// The one request path every route shares: use the authenticated request
/// context, resolve the alias, hold a budget estimate, dispatch through the
/// failover walk, then settle the hold and record exactly one usage record.
/// Routes differ only in the wire they speak — where the body goes upstream and
/// how usage is read back out (see [`Route`]).
async fn serve(
    state: AppState,
    headers: HeaderMap,
    body: Value,
    route: Route,
    snapshot: Arc<ConfigSnapshot>,
    caller: InboundKey,
) -> Result<Response, GatewayError> {
    let cfg = &snapshot.config;

    let streamed = route.streamable() && body.get("stream").and_then(Value::as_bool) == Some(true);

    let alias = body
        .get("model")
        .and_then(Value::as_str)
        .ok_or_else(|| GatewayError::BadRequest("missing `model`".into()))?
        .to_string();

    if let Some(scope) = &caller.alias_scope
        && !scope.permits(&alias)
    {
        return Err(GatewayError::TokenForbidden(
            TokenVerificationError::AliasNotPermitted { alias },
        ));
    }

    let model = cfg
        .model(&alias)
        .ok_or_else(|| GatewayError::UnknownModel(alias.clone()))?;
    let wire = Wire {
        route,
        headers: route.wire_headers(&headers),
    };
    wire.check_targets(cfg, model, &alias)?;

    // The permit is held for the request's lifetime: buffered paths drop it at
    // scope end, and a stream moves it into the accounting owner that settles it.
    let rate_limit_key = RateLimitKey {
        namespace: caller.namespace.clone(),
        subject: caller.subject.clone(),
    };
    let rate_limit_permit = state
        .0
        .rate_limiter
        .acquire(&rate_limit_key)
        .await
        .map_err(|error| match error {
            crate::rate_limit::RateLimitError::StoreUnavailable => {
                GatewayError::RateLimitUnavailable
            }
            crate::rate_limit::RateLimitError::Exceeded
            | crate::rate_limit::RateLimitError::SubjectCapacityExceeded => {
                GatewayError::RateLimitExceeded {
                    retry_after_seconds: None,
                }
            }
        })?;

    // Budget is denominated in micro-dollars. Hold a conservative cost estimate
    // from the first target's price before dispatch; settle the hold against the
    // real cost — priced at whichever target actually served — after.
    let budget_key = BudgetKey {
        namespace: caller.namespace.clone(),
        subject: caller.subject.clone(),
    };
    let estimate = route.estimate(&body);
    let estimated_cost = model.targets[0].price.cost_microdollars(estimate);
    if let Some(ceiling) = caller.max_request_microdollars
        && estimated_cost > ceiling
    {
        return Err(GatewayError::RequestCostCeilingExceeded {
            alias: alias.clone(),
            estimated_microdollars: estimated_cost,
            ceiling_microdollars: ceiling,
        });
    }
    let reservation = match state.0.budget.reserve(&budget_key, estimated_cost).await {
        Admission::Allowed(reservation) => reservation,
        Admission::Denied(Denial::Exceeded) => return Err(GatewayError::BudgetExceeded(alias)),
        Admission::Denied(Denial::StoreUnavailable) => return Err(GatewayError::BudgetUnavailable),
    };

    if streamed {
        return stream_with_failover(
            &state,
            snapshot.clone(),
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
                    permit: Some(rate_limit_permit),
                },
            },
        )
        .await;
    }

    let reservation = BudgetReservation::new(state.clone(), budget_key, reservation);
    let outcome =
        match dispatch_with_failover(&state, &snapshot, &caller, model, &body, &wire).await {
            Ok(outcome) => outcome,
            Err(err) => {
                // Nothing reached a provider, so nothing was consumed: the whole
                // estimate goes back rather than lingering until it expires.
                reservation.release().await;
                return Err(err);
            }
        };
    let served = &outcome.served;
    match outcome.result {
        Ok(response) => {
            let usage = to_usage(&response.usage);
            let cost = served.price.cost_microdollars(usage);
            reservation.settle(cost).await;
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
            reservation.release().await;
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

enum StreamLeaseParent<'a> {
    Attempt(&'a tracing::Span),
    Rotation(opentelemetry::Context),
}

#[allow(clippy::too_many_arguments)]
async fn open_stream_lease(
    state: &AppState,
    ctx: &StreamContext,
    provider: &Provider,
    target: &Target,
    body: &Value,
    wire: &Wire,
    lease: &CredentialLease,
    lease_index: usize,
    parent: StreamLeaseParent<'_>,
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
                        state.0.dispatcher.send_stream(&upstream, &call),
                    )
                    .await?
                }
                StreamLeaseParent::Rotation(parent) => {
                    let open = state.0.dispatcher.send_stream(&upstream, &call);
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
async fn stream_with_failover(
    state: &AppState,
    snapshot: Arc<ConfigSnapshot>,
    caller: &InboundKey,
    model: &Model,
    request: StreamRequest<'_>,
) -> Result<Response, GatewayError> {
    let StreamRequest {
        alias,
        body,
        wire,
        mut hold,
    } = request;
    let cfg = &snapshot.config;
    let policy = FailoverPolicy;
    let deadline = Instant::now() + Duration::from_millis(cfg.failover.overall_timeout_ms);
    let max_attempts = cfg.failover.max_attempts;

    let mut walk = FailoverWalk::new(caller, model.targets.len());
    let mut last_ctx: Option<(StreamContext, Instant)> = None;
    'targets: for (index, target) in model.targets.iter().enumerate() {
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
        if plan.attempts.is_empty() {
            walk.note_missing_credential(&provider.id);
            continue;
        }
        let target_attempt = walk.attempts;
        let attempt_started = Instant::now();
        let attempt_span = telemetry::upstream_attempt_span(
            target_attempt,
            &target.provider,
            &target.model,
            UsageRecord::credential_source_str(plan.source),
        );
        for (index, skipped) in plan.parked.iter().enumerate() {
            let span = attempt_span.in_scope(|| {
                telemetry::credential_lease_span(
                    &skipped.id,
                    UsageRecord::credential_source_str(plan.source),
                    index,
                )
            });
            telemetry::finish_credential_lease(&span, telemetry::LEASE_PARKED);
        }
        for (lease_index, lease) in plan.attempts.iter().enumerate() {
            let mut ctx = StreamContext {
                namespace: caller.namespace.clone(),
                subject: caller.subject.clone(),
                signer_kid: caller.signer_kid.clone(),
                alias: alias.clone(),
                target_provider: target.provider.clone(),
                target_model: target.model.clone(),
                source: plan.source,
                credential_id: lease.id.clone(),
                trace_id: telemetry::trace_id(),
                price: target.price,
                budget_key: hold.key.clone(),
                reservation: hold.reservation.clone(),
                rate_limit_permit: None,
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
                StreamLeaseParent::Attempt(&attempt_span),
            )
            .await;
            ctx.attempts = target_attempt + 1;
            match opened {
                Ok((decoder, bytes)) => {
                    telemetry::finish_upstream_attempt(
                        &attempt_span,
                        telemetry::ATTEMPT_OK,
                        attempt_started.elapsed().as_millis() as u64,
                        None,
                    );
                    ctx.rate_limit_permit = hold.permit.take();
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
                    let parent_context_for_open = attempt_span.context();
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
                            Box::pin(async move {
                                let ctx = StreamContext {
                                    namespace: caller.namespace,
                                    subject: caller.subject,
                                    signer_kid: caller.signer_kid,
                                    alias,
                                    target_provider: target.provider.clone(),
                                    target_model: target.model.clone(),
                                    source: source_for_open,
                                    credential_id: next_lease.id.clone(),
                                    trace_id: telemetry::trace_id(),
                                    price: target.price,
                                    budget_key,
                                    reservation,
                                    rate_limit_permit: None,
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
                                )
                                .await
                                .map(|(decoder, bytes)| streaming::OpenedStream { decoder, bytes })
                            }) as futures::future::BoxFuture<'static, _>
                        };
                    let snapshot_for_health = snapshot.clone();
                    let rotation = streaming::RotationHandle::new(
                        remaining,
                        lease.clone(),
                        plan.parked.len() + lease_index + 1,
                        opener,
                        move |lease| snapshot_for_health.credentials.record_failure(lease),
                        {
                            let snapshot = snapshot.clone();
                            move |lease| snapshot.credentials.record_success(lease)
                        },
                    );
                    return Ok(streaming::relay_opened(
                        state.clone(),
                        ctx,
                        decoder,
                        bytes,
                        started,
                        wire.route.framing(),
                        Some(rotation),
                    ));
                }
                Err(err) if is_credential_exhausted(&err) => {
                    snapshot.credentials.record_failure(lease);
                    last_ctx = Some((ctx, started));
                    walk.last_error = Some(err);
                    continue;
                }
                Err(err) => {
                    record_target_failure(&snapshot, target, &circuit_key, &err);
                    let has_next = index + 1 < walk.total
                        && walk.attempts < max_attempts
                        && Instant::now() < deadline;
                    let decision = policy.decide(&as_provider_error(&err), has_next);
                    last_ctx = Some((ctx, started));
                    walk.last_error = Some(err);
                    if decision == FailoverDecision::Return {
                        telemetry::finish_upstream_attempt(
                            &attempt_span,
                            telemetry::ATTEMPT_ERROR,
                            attempt_started.elapsed().as_millis() as u64,
                            None,
                        );
                        walk.attempts += 1;
                        break 'targets;
                    }
                    break;
                }
            }
        }
        telemetry::finish_upstream_attempt(
            &attempt_span,
            telemetry::ATTEMPT_ERROR,
            attempt_started.elapsed().as_millis() as u64,
            None,
        );
        walk.attempts += 1;
    }

    if let Some(err) = walk.last_error.take() {
        if let Some((mut ctx, started)) = last_ctx {
            ctx.attempts = walk.attempts;
            ctx.rate_limit_permit = hold.permit.take();
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
    permit: Option<RateLimitPermit>,
}

/// A buffered request's reservation must be reconciled even when its handler is
/// dropped while the upstream request is in flight. Streaming `Accounting`
/// covers cancellation once the relay exists; this guard covers the buffered path.
struct BudgetReservation {
    state: AppState,
    key: BudgetKey,
    reservation: Option<Reservation>,
}

impl BudgetReservation {
    fn new(state: AppState, key: BudgetKey, reservation: Reservation) -> Self {
        Self {
            state,
            key,
            reservation: Some(reservation),
        }
    }

    /// Disarm before awaiting so dropping the guard after settlement cannot
    /// submit a second budget operation.
    async fn settle(mut self, actual_microdollars: u64) {
        let reservation = self
            .reservation
            .take()
            .expect("budget reservation guard must be armed");
        self.state
            .0
            .budget
            .settle(&self.key, &reservation, actual_microdollars)
            .await;
    }

    /// Disarm before awaiting so the explicit release and the drop fallback
    /// cannot both reconcile the same hold.
    async fn release(mut self) {
        let reservation = self
            .reservation
            .take()
            .expect("budget reservation guard must be armed");
        self.state.0.budget.release(&self.key, &reservation).await;
    }
}

impl Drop for BudgetReservation {
    fn drop(&mut self) {
        let Some(reservation) = self.reservation.take() else {
            return;
        };
        let state = self.state.clone();
        let key = self.key.clone();
        streaming::spawn_settlement(async move {
            state.0.budget.release(&key, &reservation).await;
        });
    }
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
            auth: match provider.kind {
                ProviderKind::Anthropic => AuthScheme::Header("x-api-key"),
                ProviderKind::Openai | ProviderKind::OpenaiCompatible => AuthScheme::Bearer,
            },
        };
        let result = async {
            match wire.route {
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
                route => state
                    .0
                    .dispatcher
                    .send(&upstream, &wire.call(body.clone(), adapter.name()))
                    .await
                    .map(|body| ProviderResponse {
                        usage: route.native_usage(&body),
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
fn is_credential_exhausted(err: &TransportError) -> bool {
    matches!(err, TransportError::Provider(error) if error.is_credential_rate_limited())
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
        signer_kid: args.caller.signer_kid.clone(),
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
    use crate::aliases::AliasScope;
    use crate::budget::NoBudget;
    use crate::config::Config;
    use crate::rate_limit::{InMemoryRateLimiter, NoLimit, RateLimitKey, RateLimiter};
    use crate::usage::{StdoutSink, UsageFanout, UsageSink};
    use axum::body::Body;
    use axum::http::{Method, Request, StatusCode};
    use http_body_util::BodyExt;
    use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
    use serde::Serialize;
    use std::collections::HashMap;
    use std::future::pending;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, SystemTime};
    use tokio::sync::oneshot;
    use tower::util::ServiceExt;
    use tracing_subscriber::layer::SubscriberExt;

    /// The inbound key every test config declares, and the secret the caller
    /// presents for it. Inbound auth is always enforced (ADR 0013).
    const GATEWAY_KEY: &str = r#"
[[gateway_key]]
env = "AXOND_INBOUND_KEY"
namespace = "platform"
"#;
    const CALLER_SECRET: &str = "inbound-secret";

    /// The given provider-credential env vars, plus the inbound key's.
    fn env_with<const N: usize>(credentials: [(&str, &str); N]) -> HashMap<String, String> {
        credentials
            .into_iter()
            .chain([("AXOND_INBOUND_KEY", CALLER_SECRET)])
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect()
    }

    /// A JSON `POST` that already carries the caller's gateway key.
    fn authorized(uri: &str) -> axum::http::request::Builder {
        Request::post(uri)
            .header("content-type", "application/json")
            .header(
                axum::http::header::AUTHORIZATION,
                format!("Bearer {CALLER_SECRET}"),
            )
    }

    fn test_state() -> AppState {
        let cfg = Config::from_toml_str(&format!(
            r#"
[[namespace]]
id = "platform"
default = true

[[provider]]
id = "openai"
kind = "openai"
base_url = "https://api.openai.com/v1"

[[credential]]
namespace = "platform"
provider = "openai"
env = "AXOND_PLATFORM_OPENAI"

{GATEWAY_KEY}

[gateway_token]
audience = "test-audience"

[[gateway_verifier]]
kid = "scope-test-kid"
alg = "HS256"
env = "JWT_SECRET"
namespaces = ["platform"]
max_ttl = "15m"

[[model]]
name = "gpt-4o"
targets = [{{ provider = "openai", model = "gpt-4o", price = {{ input_microdollars_per_million = 2500000, output_microdollars_per_million = 10000000 }} }}]

[[model]]
name = "claude-3"
targets = [{{ provider = "openai", model = "claude-3", price = {{ input_microdollars_per_million = 2500000, output_microdollars_per_million = 10000000 }} }}]
"#
        ))
        .unwrap();
        let sinks: Vec<Box<dyn UsageSink>> = vec![Box::new(StdoutSink)];
        let mut env = env_with([("AXOND_PLATFORM_OPENAI", "sk-platform-test")]);
        env.insert(
            "JWT_SECRET".to_owned(),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
        );
        AppState::new(cfg, &env, UsageFanout::new(sinks), Box::new(NoBudget))
            .expect("credentials resolve")
    }

    #[test]
    fn namespace_authority_follows_reachable_provider_wires() {
        let snapshot = test_state().config();
        assert!(namespace_allows(&snapshot, "platform", Capability::Models));
        assert!(namespace_allows(&snapshot, "platform", Capability::Chat));
        assert!(namespace_allows(
            &snapshot,
            "platform",
            Capability::Embeddings
        ));
        assert!(!namespace_allows(
            &snapshot,
            "platform",
            Capability::Messages
        ));
    }

    async fn scoped_route_state() -> AppState {
        scoped_route_state_with_revocation(Box::new(crate::revocation::NoDenylist)).await
    }

    async fn scoped_route_state_with_revocation(
        revocation: Box<dyn crate::revocation::RevocationStore>,
    ) -> AppState {
        let (chat_url, _) =
            controllable_upstream(Arc::new(AtomicBool::new(true)), StatusCode::OK).await;
        let (messages_url, _) = native_upstream(
            "/messages",
            Json(json!({
                "id": "msg-1",
                "usage": { "input_tokens": 1, "output_tokens": 1 }
            }))
            .into_response(),
        )
        .await;
        let (embeddings_url, _) = native_upstream(
            "/embeddings",
            Json(json!({
                "object": "list",
                "data": [],
                "usage": { "prompt_tokens": 1, "total_tokens": 1 }
            }))
            .into_response(),
        )
        .await;
        let config = Config::from_toml_str(&format!(
            r#"
[[namespace]]
id = "platform"
default = true

[[provider]]
id = "chat"
kind = "openai"
base_url = "{chat_url}"

[[provider]]
id = "messages"
kind = "anthropic"
base_url = "{messages_url}"

[[provider]]
id = "embeddings"
kind = "openai"
base_url = "{embeddings_url}"

[[credential]]
namespace = "platform"
provider = "chat"
env = "CHAT_KEY"

[[credential]]
namespace = "platform"
provider = "messages"
env = "MESSAGES_KEY"

[[credential]]
namespace = "platform"
provider = "embeddings"
env = "EMBEDDINGS_KEY"

[[gateway_key]]
env = "STATIC_KEY"
namespace = "platform"

[gateway_token]
audience = "scope-tests"

[[gateway_verifier]]
kid = "scope-test-kid"
alg = "HS256"
env = "JWT_SECRET"
namespaces = ["platform"]
max_ttl = "15m"

[[model]]
name = "chat-model"
targets = [{{ provider = "chat", model = "chat-model", price = {{ input_microdollars_per_million = 1, output_microdollars_per_million = 1 }} }}]

[[model]]
name = "messages-model"
targets = [{{ provider = "messages", model = "messages-model", price = {{ input_microdollars_per_million = 1, output_microdollars_per_million = 1 }} }}]

[[model]]
name = "embeddings-model"
targets = [{{ provider = "embeddings", model = "embeddings-model", price = {{ input_microdollars_per_million = 1, output_microdollars_per_million = 1 }} }}]
"#
        ))
        .expect("scope test config");
        let env = HashMap::from([
            ("CHAT_KEY".to_owned(), "chat-key".to_owned()),
            ("MESSAGES_KEY".to_owned(), "messages-key".to_owned()),
            ("EMBEDDINGS_KEY".to_owned(), "embeddings-key".to_owned()),
            ("STATIC_KEY".to_owned(), "static-key".to_owned()),
            (
                "JWT_SECRET".to_owned(),
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            ),
        ]);
        AppState::new_with_rate_limiter(
            config,
            &env,
            UsageFanout::new(vec![Box::new(StdoutSink)]),
            Box::new(NoBudget),
            Box::new(NoLimit),
            revocation,
        )
        .expect("scope test state")
    }

    fn scoped_token(scope: Option<Vec<&'static str>>) -> String {
        scoped_token_for("scope-tests", scope)
    }

    fn scoped_token_for(audience: &'static str, scope: Option<Vec<&'static str>>) -> String {
        scoped_token_for_namespace(audience, "platform", scope)
    }

    fn scoped_token_for_namespace(
        audience: &'static str,
        namespace: &'static str,
        scope: Option<Vec<&'static str>>,
    ) -> String {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_secs();
        let claims = TestTokenClaims {
            exp: now + 900,
            iat: now,
            jti: "scope-test-jti",
            aud: audience,
            ns: namespace,
            sub: "scope-caller",
            scope,
            max_request_microdollars: None,
        };
        let mut header = Header::new(Algorithm::HS256);
        header.kid = Some("scope-test-kid".to_owned());
        format!(
            "axt1.{}",
            encode(
                &header,
                &claims,
                &EncodingKey::from_secret(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            )
            .expect("scope test token")
        )
    }

    async fn response_error_type(response: Response) -> Value {
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).expect("typed error JSON")
    }

    #[tokio::test]
    async fn denylisted_minted_token_returns_token_revoked() {
        let calls = Arc::new(AtomicUsize::new(0));
        let state = scoped_route_state_with_revocation(Box::new(FakeRevocation {
            mode: FakeRevocationMode::Revoked,
            calls: Arc::clone(&calls),
        }))
        .await;
        let response = scoped_route_request(state, "/v1/models", &scoped_token(None)).await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            response_error_type(response).await["error"]["type"],
            "token_revoked"
        );
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn unavailable_revocation_store_returns_revocation_unavailable() {
        let state = scoped_route_state_with_revocation(Box::new(FakeRevocation {
            mode: FakeRevocationMode::Unavailable,
            calls: Arc::new(AtomicUsize::new(0)),
        }))
        .await;
        let response = scoped_route_request(state, "/v1/models", &scoped_token(None)).await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response_error_type(response).await["error"]["type"],
            "revocation_unavailable"
        );
    }

    #[tokio::test]
    async fn revocation_store_allow_admits_the_minted_token() {
        let state = scoped_route_state_with_revocation(Box::new(FakeRevocation {
            mode: FakeRevocationMode::Allow,
            calls: Arc::new(AtomicUsize::new(0)),
        }))
        .await;
        let response = scoped_route_request(state, "/v1/models", &scoped_token(None)).await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn tier_zero_and_static_key_requests_do_not_consult_revocation() {
        let response = scoped_route_request(
            scoped_route_state().await,
            "/v1/models",
            &scoped_token(None),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);

        let calls = Arc::new(AtomicUsize::new(0));
        let state = scoped_route_state_with_revocation(Box::new(FakeRevocation {
            mode: FakeRevocationMode::Revoked,
            calls: Arc::clone(&calls),
        }))
        .await;
        let response = scoped_route_request(state, "/v1/models", "static-key").await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }

    async fn assert_scope_denial(response: Response, capability: &str) {
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"]["type"], "token_scope_insufficient");
        assert!(
            body["error"]["message"]
                .as_str()
                .unwrap()
                .contains(capability),
            "scope denial did not name {capability}: {body}"
        );
    }

    async fn scoped_route_request(state: AppState, path: &str, token: &str) -> Response {
        let (method, body) = match path.split('?').next().unwrap_or(path) {
            "/v1/models" => (Method::GET, Vec::new()),
            "/v1/credentials" => (Method::GET, Vec::new()),
            "/v1/chat/completions" => (
                Method::POST,
                serde_json::to_vec(&json!({
                    "model": "chat-model",
                    "messages": []
                }))
                .unwrap(),
            ),
            "/v1/messages" => (
                Method::POST,
                serde_json::to_vec(&json!({
                    "model": "messages-model",
                    "max_tokens": 16,
                    "messages": []
                }))
                .unwrap(),
            ),
            "/v1/embeddings" => (
                Method::POST,
                serde_json::to_vec(&json!({
                    "model": "embeddings-model",
                    "input": ["hello"]
                }))
                .unwrap(),
            ),
            _ => panic!("unknown scoped route {path}"),
        };
        router(state)
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(path)
                    .header("authorization", format!("Bearer {token}"))
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn scoped_models_token_allows_models_and_denies_chat() {
        let state = scoped_route_state().await;
        assert_scope_denial(
            scoped_route_request(
                state.clone(),
                "/v1/models",
                &scoped_token(Some(vec!["chat"])),
            )
            .await,
            "models",
        )
        .await;
        assert_eq!(
            scoped_route_request(state, "/v1/models", &scoped_token(Some(vec!["models"])))
                .await
                .status(),
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn credentials_status_requires_its_scope_and_supports_operator_view() {
        let response = scoped_route_request(
            scoped_route_state().await,
            "/v1/credentials",
            &scoped_token(Some(vec!["chat"])),
        )
        .await;
        assert_scope_denial(response, "credentials").await;

        let response = scoped_route_request(
            scoped_route_state().await,
            "/v1/credentials",
            &scoped_token(Some(vec!["credentials"])),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(body["observed"], "replica");
        assert_eq!(body["data"][0]["state"], "healthy");
        assert_eq!(body["data"][0]["source"], "platform");

        let response = scoped_route_request(
            scoped_route_state().await,
            "/v1/credentials?namespaces=all",
            &scoped_token(Some(vec!["credentials"])),
        )
        .await;
        assert_scope_denial(response, "credentials:all").await;

        let response = scoped_route_request(
            scoped_route_state().await,
            "/v1/credentials?namespaces=all",
            &scoped_token(Some(vec!["credentials", "credentials:all"])),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);

        let response = scoped_route_request(
            scoped_route_state().await,
            "/v1/credentials?namespaces=tenant",
            &scoped_token(Some(vec!["credentials"])),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn credentials_status_scope_less_static_keys_keep_tenant_view() {
        let response =
            scoped_route_request(scoped_route_state().await, "/v1/credentials", "static-key").await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(body["data"][0]["namespace"], "platform");

        let response = scoped_route_request(
            scoped_route_state().await,
            "/v1/credentials?namespaces=all",
            "static-key",
        )
        .await;
        assert_scope_denial(response, "credentials:all").await;
    }

    #[tokio::test]
    async fn credentials_status_tenant_operator_scope_cannot_view_all_namespaces() {
        let token = scoped_token_for_namespace(
            "scope-tests",
            "acme",
            Some(vec!["credentials", "credentials:all"]),
        );
        let response =
            scoped_route_request(isolated_tenant_state(), "/v1/credentials", &token).await;
        assert_eq!(response.status(), StatusCode::OK);

        let response = scoped_route_request(
            isolated_tenant_state(),
            "/v1/credentials?namespaces=all",
            &token,
        )
        .await;
        assert_scope_denial(response, "credentials:all").await;

        let platform_token = scoped_token(Some(vec!["credentials", "credentials:all"]));
        let response = scoped_route_request(
            isolated_tenant_state(),
            "/v1/credentials?namespaces=all",
            &platform_token,
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        let namespaces: Vec<_> = body["data"]
            .as_array()
            .expect("credential list")
            .iter()
            .filter_map(|entry| entry["namespace"].as_str())
            .collect();
        assert_eq!(namespaces, ["acme", "beta"]);
    }

    #[tokio::test]
    async fn credentials_status_never_serializes_secret_material() {
        let response = scoped_route_request(
            scoped_route_state().await,
            "/v1/credentials?namespaces=all",
            &scoped_token(Some(vec!["credentials", "credentials:all"])),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let serialized = String::from_utf8(body.to_vec()).expect("json body");
        for secret in [
            "chat-key",
            "messages-key",
            "embeddings-key",
            "static-key",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ] {
            assert!(
                !serialized.contains(secret),
                "credential status leaked secret material: {secret}"
            );
        }
    }

    #[tokio::test]
    async fn credentials_status_rejects_duplicate_and_empty_namespaces_query_values() {
        for path in [
            "/v1/credentials?namespaces=all&namespaces=beta",
            "/v1/credentials?namespaces=",
        ] {
            let response = scoped_route_request(
                scoped_route_state().await,
                path,
                &scoped_token(Some(vec!["credentials"])),
            )
            .await;
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            let body: Value =
                serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                    .unwrap();
            assert_eq!(body["error"]["type"], "bad_request");
            let message = body["error"]["message"].as_str().expect("error message");
            if path.contains("all&namespaces") {
                assert!(message.contains("duplicate"));
            } else {
                assert!(message.contains("invalid `namespaces` value"));
            }
        }
    }

    fn isolated_tenant_state() -> AppState {
        let config = Config::from_toml_str(
            r#"
[[namespace]]
id = "platform"
default = true

[[namespace]]
id = "acme"

[[namespace]]
id = "beta"

[[provider]]
id = "openai"
kind = "openai"
base_url = "https://api.openai.com/v1"

[[credential]]
namespace = "acme"
provider = "openai"
env = "ACME_SECRET"
id = "acme-label"

[[credential]]
namespace = "beta"
provider = "openai"
env = "BETA_SECRET"
id = "beta-label"

[[gateway_key]]
env = "STATIC_KEY"
namespace = "acme"

[gateway_token]
audience = "scope-tests"

[[gateway_verifier]]
kid = "scope-test-kid"
alg = "HS256"
env = "JWT_SECRET"
namespaces = ["platform", "acme", "beta"]
max_ttl = "15m"
"#,
        )
        .expect("isolated tenant config");
        let env = HashMap::from([
            ("ACME_SECRET".to_owned(), "acme-secret".to_owned()),
            ("BETA_SECRET".to_owned(), "beta-secret".to_owned()),
            ("STATIC_KEY".to_owned(), "static-secret".to_owned()),
            (
                "JWT_SECRET".to_owned(),
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            ),
        ]);
        AppState::new(
            config,
            &env,
            UsageFanout::new(vec![Box::new(StdoutSink)]),
            Box::new(NoBudget),
        )
        .expect("isolated tenant state")
    }

    #[tokio::test]
    async fn credentials_status_isolated_between_tenant_namespaces() {
        let response = scoped_route_request(
            isolated_tenant_state(),
            "/v1/credentials",
            &scoped_token_for_namespace("scope-tests", "acme", Some(vec!["credentials"])),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        let ids: Vec<&str> = body["data"]
            .as_array()
            .expect("status list")
            .iter()
            .map(|entry| entry["credential_id"].as_str().expect("credential id"))
            .collect();
        assert_eq!(ids, ["acme-label"]);
    }

    #[tokio::test]
    async fn scoped_chat_token_allows_chat_and_denies_embeddings() {
        let state = scoped_route_state().await;
        assert_scope_denial(
            scoped_route_request(
                state.clone(),
                "/v1/chat/completions",
                &scoped_token(Some(vec!["embeddings"])),
            )
            .await,
            "chat",
        )
        .await;
        assert_eq!(
            scoped_route_request(
                state,
                "/v1/chat/completions",
                &scoped_token(Some(vec!["chat"]))
            )
            .await
            .status(),
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn scoped_messages_token_allows_messages_and_denies_chat() {
        let state = scoped_route_state().await;
        assert_scope_denial(
            scoped_route_request(
                state.clone(),
                "/v1/messages",
                &scoped_token(Some(vec!["chat"])),
            )
            .await,
            "messages",
        )
        .await;
        assert_eq!(
            scoped_route_request(state, "/v1/messages", &scoped_token(Some(vec!["messages"])),)
                .await
                .status(),
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn scoped_embeddings_token_allows_embeddings_and_denies_messages() {
        let state = scoped_route_state().await;
        assert_scope_denial(
            scoped_route_request(
                state.clone(),
                "/v1/embeddings",
                &scoped_token(Some(vec!["messages"])),
            )
            .await,
            "embeddings",
        )
        .await;
        assert_eq!(
            scoped_route_request(
                state,
                "/v1/embeddings",
                &scoped_token(Some(vec!["embeddings"])),
            )
            .await
            .status(),
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn scope_less_tokens_and_static_keys_reach_all_provider_routes() {
        for path in [
            "/v1/models",
            "/v1/chat/completions",
            "/v1/messages",
            "/v1/embeddings",
        ] {
            assert_eq!(
                scoped_route_request(scoped_route_state().await, path, &scoped_token(None))
                    .await
                    .status(),
                StatusCode::OK,
                "scope-less token denied {path}"
            );
            assert_eq!(
                scoped_route_request(scoped_route_state().await, path, "static-key")
                    .await
                    .status(),
                StatusCode::OK,
                "static key denied {path}"
            );
        }
    }

    #[tokio::test]
    async fn scoped_token_cannot_grant_a_route_the_namespace_lacks() {
        let body = serde_json::to_vec(&json!({ "model": "gpt-4o", "messages": [] })).expect("body");
        let response = router(test_state())
            .oneshot(
                Request::post("/v1/messages")
                    .header(
                        "authorization",
                        format!(
                            "Bearer {}",
                            scoped_token_for("test-audience", Some(vec!["messages"]),)
                        ),
                    )
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_scope_denial(response, "messages").await;
    }

    #[tokio::test]
    async fn scoped_token_on_responses_keeps_the_typed_501() {
        let response = router(test_state())
            .oneshot(
                Request::post("/v1/responses")
                    .header(
                        "authorization",
                        format!(
                            "Bearer {}",
                            scoped_token_for("test-audience", Some(vec!["chat"]))
                        ),
                    )
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"]["type"], "not_implemented");
    }

    /// Inbound auth is enforced for every configured key set: the wrong
    /// credential, and no credential at all, are both `401`.
    #[tokio::test]
    async fn a_request_without_a_valid_gateway_key_is_rejected() {
        let body =
            || Body::from(serde_json::to_vec(&json!({"model": "gpt-4o", "messages": []})).unwrap());
        for request in [
            Request::post("/v1/chat/completions")
                .header("content-type", "application/json")
                .body(body())
                .unwrap(),
            Request::post("/v1/chat/completions")
                .header("content-type", "application/json")
                .header(axum::http::header::AUTHORIZATION, "Bearer not-the-key")
                .body(body())
                .unwrap(),
            Request::post("/v1/chat/completions")
                .header("content-type", "application/json")
                .header("x-api-key", "not-the-key")
                .body(body())
                .unwrap(),
        ] {
            let resp = router(test_state()).oneshot(request).await.unwrap();
            assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        }
    }

    #[tokio::test]
    async fn every_authenticated_route_rejects_a_request_without_a_gateway_key() {
        let public_paths: Vec<_> = route_specs()
            .iter()
            .filter(|spec| spec.auth == AuthPosture::LivenessProbe)
            .map(|spec| spec.path)
            .collect();
        assert_eq!(public_paths, ["/healthz", "/readyz"]);

        for spec in route_specs()
            .into_iter()
            .filter(|spec| spec.auth == AuthPosture::Authenticated)
        {
            let mut rejected = false;
            for method in [axum::http::Method::GET, axum::http::Method::POST] {
                let request = Request::builder()
                    .method(method)
                    .uri(spec.path)
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap();
                let response = router(test_state()).oneshot(request).await.unwrap();
                if response.status() != StatusCode::METHOD_NOT_ALLOWED {
                    assert_eq!(
                        response.status(),
                        StatusCode::UNAUTHORIZED,
                        "{} must authenticate before handling the request",
                        spec.path
                    );
                    rejected = true;
                }
            }
            assert!(rejected, "{0} must handle GET or POST", spec.path);
        }
    }

    #[tokio::test]
    async fn the_responses_route_rejects_anonymous_callers_before_deferring() {
        let resp = router(test_state())
            .oneshot(
                Request::post("/v1/responses")
                    .header("content-type", "application/json")
                    .body(Body::from("{}"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    /// The configured key passes in either scheme an SDK might send it in, and
    /// the caller is attributed to the key's namespace and env-var name.
    #[tokio::test]
    async fn a_configured_gateway_key_authenticates_in_either_scheme() {
        let state = test_state();
        let snapshot = state.config();
        for headers in [
            HeaderMap::from_iter([(
                axum::http::header::AUTHORIZATION,
                format!("Bearer {CALLER_SECRET}").parse().unwrap(),
            )]),
            HeaderMap::from_iter([(
                axum::http::HeaderName::from_static("x-api-key"),
                CALLER_SECRET.parse().unwrap(),
            )]),
        ] {
            let caller = authenticate(&snapshot, &headers)
                .await
                .expect("the key is configured");
            assert_eq!(caller.namespace, "platform");
            assert_eq!(caller.subject, "AXOND_INBOUND_KEY");
            assert_eq!(caller.signer_kid, None);
        }
    }

    #[derive(Serialize)]
    struct TestTokenClaims {
        exp: u64,
        iat: u64,
        jti: &'static str,
        aud: &'static str,
        ns: &'static str,
        sub: &'static str,
        #[serde(skip_serializing_if = "Option::is_none")]
        scope: Option<Vec<&'static str>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        max_request_microdollars: Option<u64>,
    }

    #[tokio::test]
    async fn a_verified_token_usage_record_carries_its_signer_kid() {
        let (base_url, _) =
            controllable_upstream(Arc::new(AtomicBool::new(true)), StatusCode::OK).await;
        let cfg = Config::from_toml_str(&format!(
            r#"
[[namespace]]
id = "platform"
default = true

[[provider]]
id = "openai"
kind = "openai"
base_url = "{base_url}"

[[gateway_key]]
env = "STATIC_KEY"
namespace = "platform"

[gateway_token]
audience = "test-audience"

[[gateway_verifier]]
kid = "route-kid"
alg = "HS256"
env = "JWT_SECRET"
namespaces = ["platform"]
max_ttl = "15m"

[[credential]]
namespace = "platform"
provider = "openai"
env = "UPSTREAM_KEY"

[[model]]
name = "gpt-4o"
targets = [{{ provider = "openai", model = "gpt-4o", price = {{ input_microdollars_per_million = 1000000, output_microdollars_per_million = 1000000 }} }}]
"#
        ))
        .expect("token test config");
        let env = HashMap::from([
            ("STATIC_KEY".to_owned(), "static-secret".to_owned()),
            ("JWT_SECRET".to_owned(), "a".repeat(32)),
            ("UPSTREAM_KEY".to_owned(), "sk-test".to_owned()),
        ]);
        let captured = CapturingSink::default();
        let records = captured.0.clone();
        let state = AppState::new(
            cfg,
            &env,
            UsageFanout::new(vec![Box::new(captured)]),
            Box::new(NoBudget),
        )
        .expect("state");
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_secs();
        let claims = TestTokenClaims {
            exp: now + 900,
            iat: now,
            jti: "route-jti",
            aud: "test-audience",
            ns: "platform",
            sub: "token-caller",
            scope: None,
            max_request_microdollars: None,
        };
        let mut header = Header::new(Algorithm::HS256);
        header.kid = Some("route-kid".to_owned());
        let token = format!(
            "axt1.{}",
            encode(
                &header,
                &claims,
                &EncodingKey::from_secret(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            )
            .expect("token")
        );
        let request = Request::post("/v1/chat/completions")
            .header("content-type", "application/json")
            .header("authorization", format!("Bearer {token}"))
            .body(Body::from(
                serde_json::to_vec(&json!({"model": "gpt-4o", "messages": []})).expect("body"),
            ))
            .expect("request");
        let response = router(state).oneshot(request).await.expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let records = records.lock().expect("records");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].subject, "token-caller");
        assert_eq!(records[0].signer_kid.as_deref(), Some("route-kid"));
    }

    /// An epoch rejection is a typed authentication failure, so HTTP clients
    /// can distinguish it from an ordinary expired-token response.
    #[tokio::test]
    async fn an_epoch_rejected_token_returns_a_distinct_401_error_code() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_secs();
        let cfg = Config::from_toml_str(&format!(
            r#"
[[namespace]]
id = "platform"
default = true

[[gateway_key]]
env = "STATIC_KEY"
namespace = "platform"

[gateway_token]
audience = "test-audience"

[[gateway_verifier]]
kid = "route-kid"
alg = "HS256"
env = "JWT_SECRET"
namespaces = ["platform"]
max_ttl = "15m"

[[gateway_token_epoch]]
namespace = "platform"
min_iat = {}
"#,
            now + 1
        ))
        .expect("epoch test config");
        let env = HashMap::from([
            ("STATIC_KEY".to_owned(), "static-secret".to_owned()),
            ("JWT_SECRET".to_owned(), "a".repeat(32)),
        ]);
        let state = AppState::new(
            cfg,
            &env,
            UsageFanout::new(vec![Box::new(StdoutSink)]),
            Box::new(NoBudget),
        )
        .expect("state");
        let claims = TestTokenClaims {
            exp: now + 900,
            iat: now,
            jti: "route-epoch-jti",
            aud: "test-audience",
            ns: "platform",
            sub: "epoch-caller",
            scope: None,
            max_request_microdollars: None,
        };
        let mut header = Header::new(Algorithm::HS256);
        header.kid = Some("route-kid".to_owned());
        let token = format!(
            "axt1.{}",
            encode(
                &header,
                &claims,
                &EncodingKey::from_secret(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            )
            .expect("token")
        );
        let response = router(state)
            .oneshot(
                Request::post("/v1/chat/completions")
                    .header("content-type", "application/json")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::to_vec(&json!({"model": "gpt-4o", "messages": []}))
                            .expect("body"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let body: Value = serde_json::from_slice(
            &response
                .into_body()
                .collect()
                .await
                .expect("body")
                .to_bytes(),
        )
        .expect("json error body");
        assert_eq!(body["error"]["type"], "token_issued_before_epoch");
        assert_ne!(body["error"]["type"], "token_expired");
    }

    #[tokio::test]
    async fn healthz_is_ok() {
        let resp = router(test_state())
            .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    /// `/v1/models` fails closed like every other request path: no gateway key
    /// means `401`, not an open catalog (ADR 0013).
    #[tokio::test]
    async fn models_requires_a_gateway_key() {
        let resp = router(test_state())
            .oneshot(Request::get("/v1/models").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn models_lists_the_callers_aliases() {
        let resp = router(test_state())
            .oneshot(
                Request::get("/v1/models")
                    .header(
                        axum::http::header::AUTHORIZATION,
                        format!("Bearer {CALLER_SECRET}"),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let json: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["data"][0]["id"], "gpt-4o");
    }

    #[tokio::test]
    async fn models_intersect_namespace_access_with_alias_scope() {
        let state = test_state();
        let snapshot = state.config();
        let caller = InboundKey {
            namespace: "platform".to_owned(),
            subject: "restricted".to_owned(),
            signer_kid: Some("test-kid".to_owned()),
            scope: None,
            alias_scope: Some(AliasScope::parse(["gpt-4o"]).unwrap()),
            max_request_microdollars: None,
            jti: None,
        };
        let response = list_models(Extension(snapshot), Extension(caller))
            .await
            .unwrap()
            .into_response();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["data"].as_array().unwrap().len(), 1);
        assert_eq!(json["data"][0]["id"], "gpt-4o");
    }

    /// A caller sees only the aliases it could invoke: a BYOK namespace with no
    /// credential for the target's provider (and no platform fallback) gets an
    /// empty list, so it cannot enumerate aliases it is not entitled to, while
    /// the platform namespace — which does hold the credential — sees the alias.
    #[tokio::test]
    async fn models_are_scoped_to_the_callers_namespace() {
        let cfg = Config::from_toml_str(
            r#"
[[namespace]]
id = "platform"
default = true

[[namespace]]
id = "acme"

[[provider]]
id = "openai"
kind = "openai"
base_url = "https://api.openai.com/v1"

[[credential]]
namespace = "platform"
provider = "openai"
env = "K_PLATFORM"

[[gateway_key]]
env = "GK_PLATFORM"
namespace = "platform"

[[gateway_key]]
env = "GK_ACME"
namespace = "acme"

[[model]]
name = "gpt-4o"
targets = [{ provider = "openai", model = "gpt-4o", price = { input_microdollars_per_million = 1, output_microdollars_per_million = 1 } }]
"#,
        )
        .unwrap();
        let env: HashMap<String, String> = [
            ("K_PLATFORM", "sk"),
            ("GK_PLATFORM", "plat-key"),
            ("GK_ACME", "acme-key"),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect();
        let state = AppState::new(
            cfg,
            &env,
            UsageFanout::new(vec![Box::new(StdoutSink)]),
            Box::new(NoBudget),
        )
        .unwrap();

        assert_eq!(
            models_for(&state, "plat-key").await["data"][0]["id"],
            "gpt-4o"
        );
        let acme = models_for(&state, "acme-key").await;
        assert_eq!(acme["data"].as_array().unwrap().len(), 0);
    }

    /// The `/v1/models` body a caller presenting `secret` receives.
    async fn models_for(state: &AppState, secret: &str) -> Value {
        let resp = router(state.clone())
            .oneshot(
                Request::get("/v1/models")
                    .header(
                        axum::http::header::AUTHORIZATION,
                        format!("Bearer {secret}"),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[tokio::test]
    async fn unknown_model_is_typed_404_not_a_missing_route() {
        let body = serde_json::to_vec(&json!({"model": "nope", "messages": []})).unwrap();
        let resp = router(test_state())
            .oneshot(
                authorized("/v1/chat/completions")
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

    #[tokio::test]
    async fn a_disallowed_alias_is_forbidden_before_model_lookup() {
        let state = test_state();
        for alias in ["claude-3", "does-not-exist"] {
            let response = serve(
                state.clone(),
                HeaderMap::new(),
                json!({"model": alias, "messages": []}),
                Route::ChatCompletions,
                state.config(),
                InboundKey {
                    namespace: "platform".to_owned(),
                    subject: "restricted".to_owned(),
                    signer_kid: Some("test-kid".to_owned()),
                    scope: None,
                    alias_scope: Some(AliasScope::parse(["gpt-4o"]).unwrap()),
                    max_request_microdollars: None,
                    jti: None,
                },
            )
            .await
            .unwrap_err()
            .into_response();
            assert_eq!(response.status(), StatusCode::FORBIDDEN);
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let json: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(json["error"]["type"], "token_alias_not_permitted");
        }
    }

    /// Deferred past beta, but the route still answers for itself: a caller
    /// cannot tell a missing route from a misconfigured `base_url`.
    #[tokio::test]
    async fn the_responses_route_is_a_typed_501_that_names_its_deferral() {
        let resp = router(test_state())
            .oneshot(authorized("/v1/responses").body(Body::from("{}")).unwrap())
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
            .oneshot(authorized("/v1/messages").body(Body::from(body)).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let json: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["error"]["type"], "unsupported_wire");
    }

    /// The mirror of the native-route case: an Anthropic-native target cannot
    /// serve the OpenAI chat wire, so the alias is rejected up front rather than
    /// dispatched into a `/chat/completions` the provider does not expose.
    #[tokio::test]
    async fn an_anthropic_alias_on_chat_completions_is_a_typed_4xx() {
        let cfg = Config::from_toml_str(&format!(
            r#"
[[namespace]]
id = "platform"
default = true

[[provider]]
id = "anthropic"
kind = "anthropic"
base_url = "https://api.anthropic.com/v1"

{GATEWAY_KEY}

[[model]]
name = "claude"
targets = [{{ provider = "anthropic", model = "claude-sonnet-4-5", price = {{ input_microdollars_per_million = 1000000, output_microdollars_per_million = 2000000 }} }}]
"#
        ))
        .unwrap();
        let sinks: Vec<Box<dyn UsageSink>> = vec![Box::new(StdoutSink)];
        let state = AppState::new(
            cfg,
            &env_with([]),
            UsageFanout::new(sinks),
            Box::new(NoBudget),
        )
        .expect("no credentials to resolve");

        let body = serde_json::to_vec(&json!({ "model": "claude", "messages": [] })).unwrap();
        let resp = router(state)
            .oneshot(
                authorized("/v1/chat/completions")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let json: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["error"]["type"], "unsupported_wire");
        let message = json["error"]["message"].as_str().unwrap();
        assert!(message.contains("anthropic"), "{message}");
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

{GATEWAY_KEY}

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
        let env = env_with([("K1", "sk-exhausted"), ("K2", "sk-good")]);
        let captured = CapturingSink::default();
        let sinks: Vec<Box<dyn UsageSink>> = vec![Box::new(captured.clone())];
        let state = AppState::new(cfg, &env, UsageFanout::new(sinks), Box::new(NoBudget)).unwrap();

        let body = serde_json::to_vec(&json!({"model": "gpt-4o", "messages": []})).unwrap();
        let resp = router(state)
            .oneshot(
                authorized("/v1/chat/completions")
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
        assert_eq!(records[0].signer_kid, None);
        // The pool made one target attempt (credential rotation is inner).
        assert_eq!(records[0].attempts, 1);
    }

    #[tokio::test]
    async fn buffered_pool_dispatch_emits_parented_lease_spans() {
        let base_url = rate_limiting_upstream("sk-rate-limited").await;
        let cfg = Config::from_toml_str(&format!(
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
env = "K_PARKED"
id = "parked"

[[credential]]
namespace = "platform"
provider = "openai"
env = "K_RATE"
id = "rate-limited"

[[credential]]
namespace = "platform"
provider = "openai"
env = "K_SERVED"
id = "served"

[[model]]
name = "gpt-4o"
targets = [{{ provider = "openai", model = "gpt-4o", price = {{ input_microdollars_per_million = 1, output_microdollars_per_million = 1 }} }}]
"#
        ))
        .unwrap();
        let state = AppState::new(
            cfg,
            &env_with([
                ("K_PARKED", "sk-parked"),
                ("K_RATE", "sk-rate-limited"),
                ("K_SERVED", "sk-served"),
            ]),
            UsageFanout::new(vec![Box::new(StdoutSink)]),
            Box::new(NoBudget),
        )
        .unwrap();
        let snapshot = state.config();
        let parked = snapshot
            .credentials
            .plan(&snapshot.config, "platform", "openai")
            .unwrap()
            .attempts
            .into_iter()
            .find(|lease| lease.id == "parked")
            .unwrap();
        snapshot.credentials.record_failure(&parked);
        snapshot.credentials.record_failure(&parked);

        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let subscriber = tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("axond-test")));
        let body = serde_json::to_vec(&json!({"model": "gpt-4o", "messages": []})).unwrap();
        let dispatch = tracing::Dispatch::new(subscriber);
        let response = tokio::spawn(async move {
            let _default = tracing::dispatcher::set_default(&dispatch);
            router(state)
                .oneshot(
                    authorized("/v1/chat/completions")
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap()
        })
        .await
        .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        provider.force_flush().unwrap();
        let spans = exporter.get_finished_spans().unwrap();
        let attempt = spans
            .iter()
            .find(|span| span.name == "axond.upstream.attempt")
            .unwrap();
        let leases: Vec<_> = spans
            .iter()
            .filter(|span| span.name == "axond.credential.lease")
            .collect();
        assert_eq!(leases.len(), 3);
        let attribute = |span: &opentelemetry_sdk::trace::SpanData, key: &str| {
            span.attributes
                .iter()
                .find(|kv| kv.key.as_str() == key)
                .map(|kv| kv.value.to_string())
        };
        for (id, status) in [
            ("parked", "parked"),
            ("rate-limited", "rate_limited"),
            ("served", "served"),
        ] {
            let lease = leases
                .iter()
                .find(|span| attribute(span, "axond.credential.id").as_deref() == Some(id))
                .unwrap();
            assert_eq!(lease.parent_span_id, attempt.span_context.span_id());
            assert_eq!(attribute(lease, "axond.status").as_deref(), Some(status));
        }
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

{GATEWAY_KEY}

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
        let env = env_with([("KA", "ka"), ("KB", "kb")]);
        let sinks: Vec<Box<dyn UsageSink>> = vec![Box::new(captured)];
        AppState::new(cfg, &env, UsageFanout::new(sinks), Box::new(NoBudget)).unwrap()
    }

    fn chat_request() -> Request<Body> {
        let body = serde_json::to_vec(&json!({"model": "gpt-4o", "messages": []})).unwrap();
        authorized("/v1/chat/completions")
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

    struct SharedLimiter(Arc<InMemoryRateLimiter>);

    #[async_trait::async_trait]
    impl RateLimiter for SharedLimiter {
        fn name(&self) -> &'static str {
            "shared-test"
        }

        async fn acquire(
            &self,
            key: &RateLimitKey,
        ) -> Result<crate::rate_limit::RateLimitPermit, crate::rate_limit::RateLimitError> {
            self.0.acquire(key).await
        }
    }

    struct UnavailableLimiter;

    #[async_trait::async_trait]
    impl RateLimiter for UnavailableLimiter {
        fn name(&self) -> &'static str {
            "unavailable-test"
        }

        async fn acquire(
            &self,
            _key: &RateLimitKey,
        ) -> Result<crate::rate_limit::RateLimitPermit, crate::rate_limit::RateLimitError> {
            Err(crate::rate_limit::RateLimitError::StoreUnavailable)
        }
    }

    struct FakeRevocation {
        mode: FakeRevocationMode,
        calls: Arc<AtomicUsize>,
    }

    enum FakeRevocationMode {
        Revoked,
        Unavailable,
        Allow,
    }

    #[async_trait::async_trait]
    impl crate::revocation::RevocationStore for FakeRevocation {
        fn name(&self) -> &'static str {
            "fake"
        }

        async fn is_revoked(&self, _jti: &str) -> Result<bool, crate::revocation::RevocationError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            match self.mode {
                FakeRevocationMode::Revoked => Ok(true),
                FakeRevocationMode::Unavailable => {
                    Err(crate::revocation::RevocationError::Unavailable {
                        backend: "fake",
                        message: "test outage".to_owned(),
                    })
                }
                FakeRevocationMode::Allow => Ok(false),
            }
        }

        async fn revoke(
            &self,
            _jti: &str,
            _expires_at: SystemTime,
        ) -> Result<(), crate::revocation::RevocationError> {
            Ok(())
        }
    }

    struct RejectingBudget;

    #[async_trait::async_trait]
    impl crate::budget::BudgetStore for RejectingBudget {
        fn name(&self) -> &'static str {
            "rejecting"
        }

        async fn reserve(&self, _key: &BudgetKey, _estimate: u64) -> Admission {
            Admission::Denied(Denial::Exceeded)
        }

        async fn settle(&self, _key: &BudgetKey, _reservation: &Reservation, _actual: u64) {}
    }

    struct SharedBudget(Arc<crate::budget::InMemoryBudget>);

    #[async_trait::async_trait]
    impl crate::budget::BudgetStore for SharedBudget {
        fn name(&self) -> &'static str {
            "shared_test"
        }

        async fn reserve(&self, key: &BudgetKey, estimated: u64) -> Admission {
            self.0.reserve(key, estimated).await
        }

        async fn settle(&self, key: &BudgetKey, reservation: &Reservation, actual: u64) {
            self.0.settle(key, reservation, actual).await;
        }
    }

    /// One target, one credential, and the budget store under test.
    fn budgeted_state(base_url: &str, budget: Box<dyn crate::budget::BudgetStore>) -> AppState {
        budgeted_state_with_limiter(base_url, budget, Box::new(NoLimit))
    }

    fn budgeted_state_with_limiter(
        base_url: &str,
        budget: Box<dyn crate::budget::BudgetStore>,
        rate_limiter: Box<dyn RateLimiter>,
    ) -> AppState {
        let cfg = Config::from_toml_str(&format!(
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
env = "K1"

[[model]]
name = "gpt-4o"
targets = [{{ provider = "openai", model = "gpt-4o", price = {{ input_microdollars_per_million = 1000000, output_microdollars_per_million = 1000000 }} }}]
"#
        ))
        .unwrap();
        let env = env_with([("K1", "sk-test")]);
        let sinks: Vec<Box<dyn UsageSink>> = vec![Box::new(StdoutSink)];
        AppState::new_with_rate_limiter(
            cfg,
            &env,
            UsageFanout::new(sinks),
            budget,
            rate_limiter,
            Box::new(crate::revocation::NoDenylist),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn limiter_runs_before_budget_reservation() {
        let limiter = Arc::new(InMemoryRateLimiter::new(1, 10));
        let held = limiter
            .acquire(&RateLimitKey {
                namespace: "platform".to_owned(),
                subject: "AXOND_INBOUND_KEY".to_owned(),
            })
            .await
            .expect("held permit");
        let budget = RecordingBudget::default();
        let state = budgeted_state_with_limiter(
            "http://127.0.0.1:1",
            Box::new(budget.clone()),
            Box::new(SharedLimiter(Arc::clone(&limiter))),
        );
        let response = router(state).oneshot(chat_request()).await.unwrap();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        let no_retry_after = response.headers().get("retry-after").is_none();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"]["type"], "rate_limited");
        assert!(no_retry_after);
        assert!(budget.0.lock().unwrap().is_empty());
        drop(held);
    }

    #[tokio::test]
    async fn unavailable_rate_limit_store_is_a_typed_503_before_budget_reservation() {
        let budget = RecordingBudget::default();
        let state = budgeted_state_with_limiter(
            "http://127.0.0.1:1",
            Box::new(budget.clone()),
            Box::new(UnavailableLimiter),
        );

        let response = router(state).oneshot(chat_request()).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"]["type"], "rate_limit_unavailable");
        assert!(budget.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn budget_denial_does_not_leave_limiter_saturated() {
        let limiter = Arc::new(InMemoryRateLimiter::new(1, 10));
        let state = budgeted_state_with_limiter(
            "http://127.0.0.1:1",
            Box::new(RejectingBudget),
            Box::new(SharedLimiter(Arc::clone(&limiter))),
        );
        for _ in 0..2 {
            let response = router(state.clone()).oneshot(chat_request()).await.unwrap();
            assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
            let bytes = response.into_body().collect().await.unwrap().to_bytes();
            let body: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(body["error"]["type"], "budget_exceeded");
        }
    }

    #[tokio::test]
    async fn a_request_over_its_ceiling_is_rejected_before_reservation_or_dispatch() {
        let (base_url, hits) =
            controllable_upstream(Arc::new(AtomicBool::new(true)), StatusCode::OK).await;
        let budget = RecordingBudget::default();
        let state = budgeted_state(&base_url, Box::new(budget.clone()));
        let snapshot = state.config();
        let caller = InboundKey {
            namespace: "platform".to_owned(),
            subject: "ceiling-caller".to_owned(),
            signer_kid: Some("test-kid".to_owned()),
            scope: None,
            alias_scope: None,
            max_request_microdollars: Some(1),
            jti: None,
        };
        let body = json!({"model": "gpt-4o", "messages": []});

        let error = serve(
            state,
            HeaderMap::new(),
            body,
            Route::ChatCompletions,
            snapshot,
            caller,
        )
        .await
        .expect_err("the estimate exceeds the caller ceiling");

        let response = error.into_response();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["error"]["type"], "request_cost_ceiling_exceeded");
        assert_eq!(hits.load(Ordering::SeqCst), 0);
        assert!(budget.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_request_under_its_ceiling_still_reserves_and_dispatches() {
        let (base_url, hits) =
            controllable_upstream(Arc::new(AtomicBool::new(true)), StatusCode::OK).await;
        let budget = RecordingBudget::default();
        let state = budgeted_state(&base_url, Box::new(budget.clone()));
        let snapshot = state.config();
        let caller = InboundKey {
            namespace: "platform".to_owned(),
            subject: "ceiling-caller".to_owned(),
            signer_kid: Some("test-kid".to_owned()),
            scope: None,
            alias_scope: None,
            max_request_microdollars: Some(10_000),
            jti: None,
        };
        let body = json!({"model": "gpt-4o", "messages": []});

        let response = serve(
            state,
            HeaderMap::new(),
            body,
            Route::ChatCompletions,
            snapshot,
            caller,
        )
        .await
        .expect("the estimate is under the caller ceiling");

        assert_eq!(response.status(), StatusCode::OK);
        assert!(hits.load(Ordering::SeqCst) > 0);
        assert!(!budget.0.lock().unwrap().is_empty());
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

    /// A cancelled buffered handler drops its reservation guard while the
    /// dispatcher is waiting for the provider; the detached release must run
    /// before the next request can observe the ledger.
    #[tokio::test]
    async fn a_cancelled_buffered_request_releases_its_reservation() {
        let (started_tx, started_rx) = oneshot::channel();
        let started_tx = Arc::new(Mutex::new(Some(started_tx)));
        let upstream = Router::new().route(
            "/chat/completions",
            post({
                let started_tx = started_tx.clone();
                move || async move {
                    if let Some(started_tx) = started_tx.lock().unwrap().take() {
                        let _ = started_tx.send(());
                    }
                    pending::<()>().await;
                    StatusCode::OK.into_response()
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap() });

        let budget = Arc::new(crate::budget::InMemoryBudget::new(10_000));
        let state = budgeted_state(
            &format!("http://{addr}"),
            Box::new(SharedBudget(budget.clone())),
        );
        let request = tokio::spawn(router(state).oneshot(chat_request()));
        started_rx.await.unwrap();
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());

        let key = BudgetKey {
            namespace: "platform".to_owned(),
            subject: "AXOND_INBOUND_KEY".to_owned(),
        };
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let outstanding = budget.outstanding(&key);
            if outstanding == 0 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "reservation was not released before timeout; outstanding={outstanding}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
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
            authorized("/v1/chat/completions")
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

{GATEWAY_KEY}

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
        let env = env_with([("K1", "sk-test")]);
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
                authorized("/v1/messages")
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
                authorized("/v1/embeddings")
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
