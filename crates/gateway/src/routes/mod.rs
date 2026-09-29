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
//! is a native OpenAI Responses passthrough.
//!
//! An alias's `targets` are tried in configured order (ADR 0008). The failover
//! walk is the *outer* loop around credential-pool dispatch: each target has an
//! in-memory per-target circuit breaker, a retryable upstream failure advances
//! to the next target, and the walk is bounded by both a total attempt count and
//! an overall wall-clock budget. Streaming rotates credentials while opening on
//! both wires, and may rotate after an OpenAI-framed stream fails before content
//! is emitted; native streams and partially delivered streams remain terminal.

use std::sync::Arc;
use std::task::Poll;

use axum::body::Body;
use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, Extension, RawQuery, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::{Next, from_fn_with_state};
use axum::response::{IntoResponse, Response};
use axum::routing::{MethodRouter, get, post};
use axum::{Json, Router};
use futures::StreamExt;
use gateway_core::{FailoverTarget, ModelUsage, Usage};
use gateway_transport::NativeCall;
use serde_json::{Value, json};

use crate::admission::RequestKind;
use crate::budget::BudgetKey;
use crate::config::{Model, ProviderKind, ProviderWire, Target, UnpricedModels};
use crate::core_accounting::CoreAccounting;
use crate::credentials::CredentialStatusView;
use crate::error::GatewayError;
use crate::pricing::RequestPrice;
use crate::principals::Capability;
use crate::shutdown::Phase;
use crate::state::{AppState, ConfigSnapshot, InboundKey};
use crate::store::{BudgetAdmit, NamespaceRecord, StoreError};
use crate::streaming::{Framing, StreamDelivery};
use crate::telemetry;
use crate::usage::Status;
use crate::usage::identity::EventIdentity;

mod accounting;
mod auth;
mod dispatch;

use accounting::*;
use auth::*;
use dispatch::*;

pub fn router(state: AppState) -> Router {
    let specs = route_specs();
    let global = mount(
        specs
            .iter()
            .copied()
            .filter(|spec| !spec.namespace_scoped)
            .collect(),
        state.clone(),
        RouteAuthority::Global,
    );
    let canonical = mount(
        specs
            .iter()
            .copied()
            .filter(|spec| spec.namespace_scoped)
            .collect(),
        state.clone(),
        RouteAuthority::Namespaced,
    );
    let api = crate::api::router(state.clone()).layer(from_fn_with_state(
        (state.clone(), None, RouteAuthority::Global),
        authenticate_middleware,
    ));
    global
        .merge(Router::new().nest("/ns/{namespace}", canonical))
        .merge(api)
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum RouteAuthority {
    Global,
    Namespaced,
}

pub(super) fn mount(specs: Vec<RouteSpec>, state: AppState, authority: RouteAuthority) -> Router {
    // The inbound body bound is declared rather than inherited: axum's own
    // default would otherwise be the process's real memory ceiling per request.
    let max_request_bytes = state.0.admission.limits().max_request_bytes;
    specs
        .into_iter()
        .fold(Router::new(), |router, spec| {
            let route = (spec.router)().layer(DefaultBodyLimit::max(max_request_bytes));
            let route = if spec.auth.requires_a_credential() {
                route.layer(from_fn_with_state(
                    (state.clone(), spec.capability, authority),
                    authenticate_middleware,
                ))
            } else {
                route
            };
            // Admission is the outermost layer, so a request arriving after the
            // drain window is refused before it touches authentication, budgets,
            // or an upstream. The probes deliberately stay outside it: a draining
            // replica is still alive, and killing its probes early would cut the
            // very requests the drain exists to finish.
            let route = if spec.auth.takes_an_admission_slot() {
                route.layer(from_fn_with_state(state.clone(), admission_middleware))
            } else {
                route
            };
            router.route(spec.path, route)
        })
        .with_state(state)
}

/// Whether a route is one of the two unauthenticated liveness probes, or must
/// pass inbound authentication before its handler can run — and if so, whether
/// it is served work or asked about the replica serving it.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum AuthPosture {
    LivenessProbe,
    Authenticated,
}

impl AuthPosture {
    /// Whether the posture requires a credential. Both authenticated postures
    /// do, which is what keeps the sweep test's floor closed over all of them.
    fn requires_a_credential(self) -> bool {
        !matches!(self, Self::LivenessProbe)
    }

    /// Whether a request to the route is work the replica is being asked to do,
    /// rather than a question about the replica doing it.
    fn takes_an_admission_slot(self) -> bool {
        matches!(self, Self::Authenticated)
    }
}

/// A route's complete registration: adding a route requires declaring its
/// authentication posture here rather than silently omitting the layer.
#[derive(Clone, Copy)]
pub(super) struct RouteSpec {
    path: &'static str,
    namespace_scoped: bool,
    auth: AuthPosture,
    capability: Option<Capability>,
    router: fn() -> MethodRouter<AppState>,
}

/// The single route table: its posture is the source of truth for registration
/// and for the sweep test that keeps the unauthenticated set closed.
pub(super) fn route_specs() -> Vec<RouteSpec> {
    vec![
        RouteSpec {
            path: "/healthz",
            namespace_scoped: false,
            auth: AuthPosture::LivenessProbe,
            capability: None,
            router: || get(healthz),
        },
        RouteSpec {
            path: "/readyz",
            namespace_scoped: false,
            auth: AuthPosture::LivenessProbe,
            capability: None,
            router: || get(readyz),
        },
        RouteSpec {
            path: "/v1/models",
            namespace_scoped: true,
            auth: AuthPosture::Authenticated,
            capability: Some(Capability::Models),
            router: || get(list_models),
        },
        RouteSpec {
            path: "/v1/credentials",
            namespace_scoped: true,
            auth: AuthPosture::Authenticated,
            capability: Some(Capability::Credentials),
            router: || get(list_credentials),
        },
        RouteSpec {
            path: "/v1/chat/completions",
            namespace_scoped: true,
            auth: AuthPosture::Authenticated,
            capability: Some(Capability::Chat),
            router: || post(chat_completions),
        },
        RouteSpec {
            path: "/v1/messages",
            namespace_scoped: true,
            auth: AuthPosture::Authenticated,
            capability: Some(Capability::Messages),
            router: || post(native_messages),
        },
        RouteSpec {
            path: "/v1/embeddings",
            namespace_scoped: true,
            auth: AuthPosture::Authenticated,
            capability: Some(Capability::Embeddings),
            router: || post(embeddings),
        },
        RouteSpec {
            path: "/v1/responses",
            namespace_scoped: true,
            auth: AuthPosture::Authenticated,
            capability: Some(Capability::Responses),
            router: || post(responses),
        },
    ]
}

/// Refuse a request that arrives after the drain has closed admission, and
/// hold the in-flight slot for as long as the response body lives.
pub(super) async fn admission_middleware(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response, GatewayError> {
    let Some(admitted) = state.lifecycle().admit() else {
        telemetry::metrics::record_shutdown_rejection();
        return Err(GatewayError::Draining);
    };
    let lifecycle = Arc::clone(state.lifecycle());
    // A request still inside its handler has no body to end, so the deadline has
    // to reach it here: dropping the handler future cancels the upstream call at
    // its next await, and the guards it holds settle on the ordinary
    // cancellation path — the budget hold is released rather than charged,
    // because a caller that received nothing owes nothing. Without this arm such
    // a request would hold its admission slot until its own upstream budget
    // expired, spending the flush budget the usage records need.
    let response = tokio::select! {
        response = next.run(request) => response,
        () = lifecycle.abandoned() => return Err(GatewayError::Draining),
    };
    let (parts, body) = response.into_parts();
    let mut relayed = Some(body.into_data_stream());
    let mut admitted = Some(admitted);
    let mut abandoned = Box::pin(async move { lifecycle.abandoned().await });
    // The guard rides along inside the body, not this future: the response is
    // handed to hyper long before a streamed body ends, so releasing the slot
    // here would undercount every stream. Ending the body when the shutdown
    // deadline expires is also what settles an abandoned stream's spend, since
    // dropping the inner body is what cancels it upstream.
    let body = Body::from_stream(futures::stream::poll_fn(move |cx| {
        let Some(inner) = relayed.as_mut() else {
            return Poll::Ready(None);
        };
        if abandoned.as_mut().poll(cx).is_ready() {
            // Dropping the inner body cancels the stream upstream, which settles
            // the spend accrued so far; releasing the slot lets shutdown see it.
            drop(relayed.take());
            drop(admitted.take());
            // An error rather than a clean end: the caller must not read a
            // truncated stream as a complete answer.
            return Poll::Ready(Some(Err(axum::Error::new(std::io::Error::other(
                "the gateway shut down before this response finished",
            )))));
        }
        match inner.poll_next_unpin(cx) {
            Poll::Ready(None) => {
                drop(relayed.take());
                drop(admitted.take());
                Poll::Ready(None)
            }
            other => other,
        }
    }));
    Ok(Response::from_parts(parts, body))
}

pub(super) async fn healthz() -> &'static str {
    "ok"
}

/// Readiness is where a rolling deployment learns this replica is leaving: it
/// fails as soon as the drain begins, before admission closes, so a load
/// balancer can stop routing while the replica is still able to serve. Real
/// dependency readiness (config loaded, credentials present) is a follow-up.
pub(super) async fn readyz(State(state): State<AppState>) -> (StatusCode, &'static str) {
    match state.lifecycle().phase() {
        Phase::Serving => (StatusCode::OK, "ready"),
        Phase::Draining | Phase::Closing => (StatusCode::SERVICE_UNAVAILABLE, "draining"),
    }
}

/// Replica-local Tier 0 credential status. Presence is expressed by each
/// configured entry (boot resolves it or boot fails), never by an always-true
/// field. Credential ids are attribution labels only; secrets remain write-only.
pub(super) async fn list_credentials(
    Extension(snapshot): Extension<Arc<ConfigSnapshot>>,
    Extension(caller): Extension<InboundKey>,
    RawQuery(raw_query): RawQuery,
) -> Result<Json<Value>, GatewayError> {
    let namespaces = parse_credential_query(raw_query.as_deref())?;
    let view = match namespaces.as_deref() {
        None => CredentialStatusView::Namespace(&caller.namespace),
        Some("all") => {
            if !caller_holds_direct_operator_authority(&caller, &snapshot) {
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

/// Whether the caller holds the operator's own authority over the whole
/// deployment, which is what the all-namespaces credential view exposes.
///
/// This is deliberately not `caller_can_mint_capability`: that predicate asks
/// whether a caller may *delegate* a capability to a subject, while this one
/// asks whether the caller *is* the operator. The rule itself lives with
/// authentication ([`InboundKey::holds_direct_operator_authority`]), which is
/// also what decides an authenticated status caller's scope.
pub(super) fn caller_holds_direct_operator_authority(
    caller: &InboundKey,
    snapshot: &ConfigSnapshot,
) -> bool {
    caller.holds_direct_operator_authority(snapshot.config.default_namespace())
}

pub(super) fn parse_credential_query(
    raw_query: Option<&str>,
) -> Result<Option<String>, GatewayError> {
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

pub(super) fn decode_query_component(value: &str) -> Result<String, GatewayError> {
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

/// The credential-query parser, reachable from the fuzz seam only.
///
/// Query parsing is a request-path detail, so it stays private to this module.
/// `--cfg fuzzing` is set by the out-of-tree `fuzz/` project alone, so no
/// ordinary build — or `--all-features` — compiles this wrapper, and it cannot be
/// switched on by a dependant.
#[cfg(fuzzing)]
pub(crate) fn fuzz_parse_credential_query(
    raw_query: Option<&str>,
) -> Result<Option<String>, GatewayError> {
    parse_credential_query(raw_query)
}

pub(super) fn hex_digit(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// The model catalog, gated behind a gateway key and scoped to the caller's
/// namespace.
///
/// Listed from the Store's discovery cache as `provider-id/model-id`, minus the
/// effective blocklist (deployment default ∪ namespace extras). Never calls
/// upstream: a background timer in `serve` refreshes the cache.
pub(super) async fn list_models(
    State(state): State<AppState>,
    Extension(snapshot): Extension<Arc<ConfigSnapshot>>,
    Extension(_caller): Extension<InboundKey>,
    record: Option<Extension<NamespaceRecord>>,
) -> Result<Json<Value>, GatewayError> {
    let extra = record
        .as_ref()
        .and_then(|Extension(record)| record.blocklist.clone())
        .unwrap_or_default();
    let store = state.store().ok_or(GatewayError::StoreUnavailable)?;
    let cached = match store.list_provider_models().await {
        Ok(rows) => rows,
        Err(StoreError::Unavailable(_)) => return Err(GatewayError::StoreUnavailable),
        Err(err) => return Err(GatewayError::BadRequest(err.to_string())),
    };
    let mut data = Vec::new();
    for provider in &snapshot.config.provider {
        let models = cached
            .iter()
            .find(|row| row.provider == provider.id)
            .map(|row| row.data_if_source(&provider.base_url))
            .unwrap_or(&[]);
        for model in models {
            let Some(bare) = model.get("id").and_then(Value::as_str) else {
                continue;
            };
            let prefixed = format!("{}/{bare}", provider.id);
            if snapshot.config.is_blocked(&prefixed, bare, &extra) {
                continue;
            }
            data.push(json!({ "id": prefixed, "object": "model" }));
        }
    }
    Ok(Json(json!({ "object": "list", "data": data })))
}

/// The wire shape a route speaks, which is the only thing that differs between
/// the routes: the upstream path, which provider kinds can serve it, and how
/// usage is read out of the provider's answer. Everything else — aliasing,
/// failover, credential pools, budgets, usage — is shared.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Route {
    /// OpenAI-shaped chat, dispatched through the provider adapter to an
    /// OpenAI-family target's `/chat/completions`.
    ChatCompletions,
    /// Anthropic-shaped Messages, forwarded verbatim to an Anthropic target.
    NativeMessages,
    /// OpenAI-shaped embeddings, forwarded verbatim to an OpenAI-family target.
    Embeddings,
    /// OpenAI Responses, forwarded verbatim to an OpenAI-family target.
    Responses,
}

impl Route {
    fn validate_routing_controls(self, body: &Value) -> Result<(), GatewayError> {
        if body.get("stream").is_some_and(|value| !value.is_boolean()) {
            return Err(GatewayError::BadRequest(
                "`stream` must be a boolean when present".into(),
            ));
        }
        if body
            .get("previous_response_id")
            .is_some_and(|value| !value.is_null() && !value.is_string())
        {
            return Err(GatewayError::BadRequest(
                "`previous_response_id` must be a string or null when present".into(),
            ));
        }
        if body.get("stream").and_then(Value::as_bool) == Some(true) && !self.streamable() {
            return Err(GatewayError::BadRequest(format!(
                "{} does not support streaming",
                self.label()
            )));
        }
        Ok(())
    }

    /// The caller-facing path, for error messages.
    fn label(self) -> &'static str {
        match self {
            Self::ChatCompletions => "/v1/chat/completions",
            Self::NativeMessages => "/v1/messages",
            Self::Embeddings => "/v1/embeddings",
            Self::Responses => "/v1/responses",
        }
    }

    /// Path appended to the provider's `base_url`.
    fn upstream_path(self) -> &'static str {
        match self {
            Self::ChatCompletions => "/chat/completions",
            Self::NativeMessages => "/messages",
            Self::Embeddings => "/embeddings",
            Self::Responses => "/responses",
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
            Self::ChatCompletions | Self::Embeddings | Self::Responses => ProviderWire::Openai,
            Self::NativeMessages => ProviderWire::Anthropic,
        }
    }

    fn streamable(self) -> bool {
        self.stream_delivery().is_some()
    }

    /// A stored Responses id only resolves on the provider — and under the
    /// credential — that stored it, so *every* Responses request, initial ones
    /// included, uses only the first configured target and credential. That is
    /// what lets a later continuation recover the same affinity without any
    /// durable state: had the initial call failed over, its response id would
    /// live on an upstream no continuation can reach.
    fn pins_affinity(self) -> bool {
        self == Self::Responses
    }

    /// Whether this request continues a provider-stored response. Only these
    /// carry continuity that can be lost, so only these report
    /// `continuation_affinity_unavailable`; a pinned *initial* request that
    /// cannot use its target reports the ordinary routing or credential error.
    /// Null and empty values are ordinary non-continuation requests.
    fn is_continuation(self, body: &Value) -> bool {
        self.pins_affinity()
            && body
                .get("previous_response_id")
                .and_then(Value::as_str)
                .is_some_and(|id| !id.is_empty())
    }

    fn max_attempts(self, configured: u32) -> u32 {
        if self.pins_affinity() { 1 } else { configured }
    }

    fn framing(self) -> Framing {
        match self {
            Self::ChatCompletions => Framing::OpenAiSse,
            Self::NativeMessages | Self::Embeddings => Framing::Native,
            Self::Responses => Framing::Responses,
        }
    }

    /// The route's ordinary streaming posture. Returning `None` for a
    /// non-streamable route keeps an embeddings request from ever acquiring a
    /// byte-faithful delivery posture, even if a future caller accidentally
    /// asks this helper to compile one.
    fn stream_delivery(self) -> Option<StreamDelivery> {
        match self {
            Self::ChatCompletions => Some(StreamDelivery::Reemit),
            Self::NativeMessages | Self::Responses => Some(StreamDelivery::Passthrough),
            Self::Embeddings => None,
        }
    }

    /// Usage from a *native* response, mapped onto the canonical record every
    /// route produces. Wire knowledge lives in `gateway-core`.
    fn native_usage(self, response: &Value) -> ModelUsage {
        match self {
            Self::NativeMessages => gateway_core::native_message_usage(response),
            Self::Responses => gateway_core::responses_usage(response),
            // Chat never takes this path (its adapter reports usage), so the
            // OpenAI-shaped prompt-only reader is the honest default.
            Self::ChatCompletions | Self::Embeddings => gateway_core::embeddings_usage(response),
        }
    }

    /// Pre-dispatch estimate for `max_request_microdollars` and usage fallback.
    /// Embeddings produce no completion, so output is zero. Not held against
    /// the namespace cap (ADR 0064).
    fn estimate(self, body: &Value) -> Usage {
        self.measure(body).0
    }

    /// Return the usage estimate together with the serialized byte length that
    /// produced it. Keeping the two together lets the post-middleware path
    /// enforce both token and byte ceilings without serializing the body twice.
    fn measure(self, body: &Value) -> (Usage, usize) {
        let (estimate, bytes) = estimate_usage(body);
        match self {
            Self::Embeddings => (
                Usage {
                    output_tokens: 0,
                    ..estimate
                },
                bytes,
            ),
            _ => (estimate, bytes),
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

/// The delivery posture a streamed route relays with: OpenAI-shaped
/// re-emission, or the provider's own bytes.
pub(super) fn stream_delivery(route: Route) -> Result<StreamDelivery, GatewayError> {
    route.stream_delivery().ok_or_else(|| {
        GatewayError::BadRequest(format!("{} does not support streaming", route.label()))
    })
}

/// A route plus the wire headers this request carries upstream, threaded through
/// the shared failover walk so both dispatch shapes reuse one request path.
#[derive(Clone)]
pub(super) struct Wire {
    route: Route,
    headers: Vec<(&'static str, String)>,
}

impl Wire {
    fn call(&self, body: Value, provider: &'static str) -> NativeCall {
        NativeCall::new(
            provider,
            self.route.upstream_path(),
            body,
            self.headers.clone(),
        )
    }
}

pub(super) struct ServeNs {
    record: NamespaceRecord,
    admit: Option<BudgetAdmit>,
}

pub(super) fn serve_ns(
    record: Option<Extension<NamespaceRecord>>,
    admit: Option<Extension<BudgetAdmit>>,
) -> Option<ServeNs> {
    record.map(|Extension(record)| ServeNs {
        record,
        admit: admit.map(|Extension(admit)| admit),
    })
}

/// The inbound body, or a typed refusal. An oversized body is a bound the
/// gateway imposed (`413`), a wrong media type is `415` as axum's extractor
/// already answered it, and a malformed one is the caller's (`400`); no
/// response echoes the body it read.
pub(super) fn inbound_body(
    body: Result<Json<Value>, JsonRejection>,
) -> Result<Value, GatewayError> {
    match body {
        Ok(Json(body)) => Ok(body),
        Err(rejection) if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE => {
            Err(GatewayError::RequestTooLarge)
        }
        // Axum answered this arm with `415` before these rejections were mapped,
        // so it keeps that status; only the body becomes typed.
        Err(JsonRejection::MissingJsonContentType(_)) => Err(GatewayError::UnsupportedMediaType),
        Err(_) => Err(GatewayError::BadRequest(
            "request body is not valid JSON".into(),
        )),
    }
}

pub(super) async fn chat_completions(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(snapshot): Extension<Arc<ConfigSnapshot>>,
    Extension(caller): Extension<InboundKey>,
    record: Option<Extension<NamespaceRecord>>,
    admit: Option<Extension<BudgetAdmit>>,
    body: Result<Json<Value>, JsonRejection>,
) -> Result<Response, GatewayError> {
    serve(
        state,
        headers,
        inbound_body(body)?,
        Route::ChatCompletions,
        snapshot,
        caller,
        serve_ns(record, admit),
    )
    .await
}

/// Anthropic-native Messages. The caller's body already speaks the target's
/// wire, so it is forwarded to the provider's `/messages` untouched but for the
/// `model` alias — which is what keeps signed thinking and tool-use blocks
/// intact through the gateway (ADR 0012).
pub(super) async fn native_messages(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(snapshot): Extension<Arc<ConfigSnapshot>>,
    Extension(caller): Extension<InboundKey>,
    record: Option<Extension<NamespaceRecord>>,
    admit: Option<Extension<BudgetAdmit>>,
    body: Result<Json<Value>, JsonRejection>,
) -> Result<Response, GatewayError> {
    serve(
        state,
        headers,
        inbound_body(body)?,
        Route::NativeMessages,
        snapshot,
        caller,
        serve_ns(record, admit),
    )
    .await
}

pub(super) async fn embeddings(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(snapshot): Extension<Arc<ConfigSnapshot>>,
    Extension(caller): Extension<InboundKey>,
    record: Option<Extension<NamespaceRecord>>,
    admit: Option<Extension<BudgetAdmit>>,
    body: Result<Json<Value>, JsonRejection>,
) -> Result<Response, GatewayError> {
    serve(
        state,
        headers,
        inbound_body(body)?,
        Route::Embeddings,
        snapshot,
        caller,
        serve_ns(record, admit),
    )
    .await
}

pub(super) async fn responses(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(snapshot): Extension<Arc<ConfigSnapshot>>,
    Extension(caller): Extension<InboundKey>,
    record: Option<Extension<NamespaceRecord>>,
    admit: Option<Extension<BudgetAdmit>>,
    body: Result<Json<Value>, JsonRejection>,
) -> Result<Response, GatewayError> {
    serve(
        state,
        headers,
        inbound_body(body)?,
        Route::Responses,
        snapshot,
        caller,
        serve_ns(record, admit),
    )
    .await
}

/// The one request path every route shares: split `provider-id/model-id`,
/// admit if spent is under the active-period limit, dispatch (credential-pool
/// rotation, no alias failover), then charge measured spend and record exactly
/// one usage record.
/// Routes differ only in the wire they speak — where the body goes upstream and
/// how usage is read back out (see [`Route`]).
pub(super) async fn serve(
    state: AppState,
    headers: HeaderMap,
    body: Value,
    route: Route,
    snapshot: Arc<ConfigSnapshot>,
    caller: InboundKey,
    namespace: Option<ServeNs>,
) -> Result<Response, GatewayError> {
    let cfg = &snapshot.config;
    let ns_blocklist = namespace
        .as_ref()
        .and_then(|ns| ns.record.blocklist.clone())
        .unwrap_or_default();
    let preloaded_admit = namespace.as_ref().and_then(|ns| ns.admit.clone());
    let attrs = namespace.map(|ns| ns.record.attrs);

    route.validate_routing_controls(&body)?;

    let streamed = route.streamable() && body.get("stream").and_then(Value::as_bool) == Some(true);

    let alias = body
        .get("model")
        .and_then(Value::as_str)
        .ok_or_else(|| GatewayError::BadRequest("missing `model`".into()))?
        .to_string();

    let (provider_id, model_id) = split_model_id(&alias)?;
    let provider = cfg
        .provider(provider_id)
        .ok_or_else(|| GatewayError::UnknownProvider(provider_id.to_owned()))?;
    if cfg.is_blocked(&alias, model_id, &ns_blocklist) {
        return Err(GatewayError::ModelBlocked(alias));
    }
    let wire = Wire {
        route,
        headers: route.wire_headers(&headers),
    };
    if !wire.route.serves(provider.kind) {
        return Err(GatewayError::UnsupportedWire {
            route: wire.route.label(),
            alias: alias.clone(),
            provider: provider.id.clone(),
        });
    }
    let catalog_price = state.catalog_price_for(&provider.id, model_id);
    let book_price = cfg.price_for(&provider.id, model_id);
    let rates = catalog_price.or(book_price);
    let request_price = match rates {
        Some(rates) => RequestPrice::configured(rates),
        None => match provider.unpriced_models {
            UnpricedModels::Deny => return Err(GatewayError::UnpricedModel(alias)),
            UnpricedModels::Allow => RequestPrice::unpriced(),
        },
    };
    let routed = Model::single(provider.id.clone(), model_id.to_owned());
    let model = &routed;

    let stream_delivery = streamed.then(|| stream_delivery(route)).transpose()?;

    // The per-request bounds are checked before any dependency work and before
    // admission: they are pure functions of the parsed body, and a request that
    // cannot legally be served should not occupy capacity while it is refused.
    // Neither error repeats any part of the body.
    let estimate = route.estimate(&body);
    let limits = state.0.admission.limits();
    check_estimate_bounds(&body, estimate, limits)?;

    // Load shedding before any dependency work: an overloaded replica must not
    // spend a rate-limit round trip or a budget reservation on a request it is
    // about to refuse. It is also strictly after authentication, so unauthenticated
    // traffic can never occupy the process's or a tenant's capacity.
    //
    // Held for the request's lifetime the same way the rate-limit permit is:
    // dropped at scope end on a buffered request, moved into the relay's
    // accounting on a streamed one.
    let admission_permit = state
        .0
        .admission
        .admit(
            &caller.namespace,
            if streamed {
                RequestKind::Streamed
            } else {
                RequestKind::Buffered
            },
        )
        .await?;
    // The settlement this request will leave behind is admitted with it: a slot
    // in the process's bounded background-accounting capacity, not a ledger
    // write (ADR 0064). A replica whose Store has fallen that far behind refuses
    // here, before any dependency work, rather than dropping the charge of a
    // request it has already served.
    let settlement_reservation = state.0.settlements.reserve()?;

    let mut core_accounting = CoreAccounting::default();
    core_accounting.reserve_settlement(settlement_reservation);

    // Budget is denominated in micro-dollars. Admit on spent-vs-limit (no hold);
    // charge measured cost — priced at whichever target actually served — after.
    // The estimate is only for `max_request_microdollars` and usage fallback.
    let budget_key = BudgetKey {
        namespace: caller.namespace.clone(),
        subject: caller.subject.clone(),
    };
    let estimated_cost = request_price.cost_microdollars(estimate).unwrap_or(0);
    if let Some(ceiling) = caller.max_request_microdollars
        && estimated_cost > ceiling
    {
        return Err(GatewayError::RequestCostCeilingExceeded {
            alias: alias.clone(),
            estimated_microdollars: estimated_cost,
            ceiling_microdollars: ceiling,
        });
    }
    core_accounting
        .reserve_budget(
            &state,
            budget_key.clone(),
            estimated_cost,
            estimate.input_tokens,
            &alias,
            preloaded_admit,
        )
        .await?;
    let reservation = core_accounting
        .core_budget_context()
        .expect("core budget admitted")
        .1
        .clone();
    let period = reservation.period.clone();

    // The request is now admitted and will produce exactly one usage event, so
    // its identity is minted here — once, while the server span is still current
    // — and carried to whichever path settles it. A request refused above this
    // line produces no event and therefore needs no identity.
    let identity = EventIdentity::capture(&headers);

    if streamed {
        return stream_with_failover(
            &state,
            snapshot.clone(),
            &caller,
            model,
            attrs.clone(),
            StreamRequest {
                alias,
                body,
                price: request_price,
                wire: &wire,
                identity,
                core_accounting,
                delivery: stream_delivery.expect("streamed request has a delivery posture"),
                hold: BudgetHold {
                    key: budget_key,
                    reservation,
                    estimated_input_tokens: estimate.input_tokens,
                    admission: Some(admission_permit),
                },
            },
        )
        .await;
    }

    let outcome = match dispatch_with_failover(
        &state,
        &snapshot,
        &caller,
        model,
        request_price,
        &body,
        &wire,
    )
    .await
    {
        Ok(outcome) => outcome,
        Err(err) => {
            // Nothing reached a provider, so nothing was consumed: the whole
            // estimate goes back rather than lingering until it expires.
            core_accounting.release_core_budget().await;
            return Err(err);
        }
    };
    let served = &outcome.served;
    match outcome.result {
        Ok(response) => {
            let usage = to_usage(&response.usage);
            let cost = served.price.cost_microdollars(usage);
            // Provider work is already complete before response middleware
            // starts. Move the known spend and usage into a cancellation owner
            // first, so a caller disappearing during a blocking callback cannot
            // turn consumed work back into a released hold and a missing row.
            let (record, ttft_ms, attempts) = build_record(RecordArgs {
                identity: &identity,
                caller: &caller,
                alias: &alias,
                target_provider: &served.provider,
                target_model: &served.model,
                source: served.source,
                credential_id: &served.credential_id,
                status: Status::ClientCancelled,
                input_tokens: response.usage.input_tokens,
                cache_read_tokens: response.usage.cache_read_tokens,
                cache_write_tokens: response.usage.cache_write_tokens,
                output_tokens: response.usage.output_tokens,
                cost_microdollars: cost,
                latency_ms: outcome.latency_ms,
                ttft_ms: outcome.ttft_ms,
                attempts: outcome.attempts,
                attrs: attrs.clone(),
                period: period.clone(),
            });
            let settlement = core_accounting.take_settlement();
            let accounting = BufferedResponseAccounting::from_core(
                state.clone(),
                core_accounting
                    .take_core_budget()
                    .expect("core budget admitted"),
                record,
                ttft_ms,
                attempts,
                settlement,
            );
            accounting.finish(Status::Ok).await?;
            Ok(Json(response.body).into_response())
        }
        Err(err) => {
            // The charging policy is "what was actually consumed" (ADR 0010),
            // and a buffered failure reports no usage at all: providers do not
            // return a usage block with an error, and nothing was relayed to
            // measure. Spend is therefore genuinely unknowable and charged as
            // zero — the streamed path, which can measure what it relayed,
            // charges its partial spend.
            core_accounting.release_core_budget().await;
            // The upstream failure is what the caller is told about, so the
            // record is best-effort here: a `503` about the outbox would hide the
            // provider error that actually ended the request.
            record_usage_terminal(
                &state,
                core_accounting.take_settlement(),
                RecordArgs {
                    identity: &identity,
                    caller: &caller,
                    alias: &alias,
                    target_provider: &served.provider,
                    target_model: &served.model,
                    source: served.source,
                    credential_id: &served.credential_id,
                    status: Status::UpstreamError,
                    input_tokens: 0,
                    cache_read_tokens: 0,
                    cache_write_tokens: 0,
                    output_tokens: 0,
                    cost_microdollars: Some(0),
                    latency_ms: outcome.latency_ms,
                    ttft_ms: outcome.ttft_ms,
                    attempts: outcome.attempts,
                    attrs: attrs.clone(),
                    period: period.clone(),
                },
            )
            .await;
            Err(err.into())
        }
    }
}

/// The circuit-breaker key for a target: its qualified `provider/model`, so two
/// aliases pointing at the same concrete target share one breaker.
pub(crate) fn target_key(target: &Target) -> String {
    FailoverTarget::new(&target.provider, &target.model).qualified_model()
}

pub(super) fn split_model_id(model: &str) -> Result<(&str, &str), GatewayError> {
    let Some((provider, id)) = model.split_once('/') else {
        return Err(GatewayError::ModelUnprefixed(model.to_owned()));
    };
    if provider.is_empty() {
        return Err(GatewayError::UnknownProvider(provider.to_owned()));
    }
    if id.is_empty() {
        return Err(GatewayError::BadRequest(
            "model id after `/` must not be empty".into(),
        ));
    }
    Ok((provider, id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::budget::{Admission, Denial};

    use crate::budget::{NoBudget, Reservation};
    use crate::config::{Config, UndurablePolicy};
    use crate::credentials::CredentialSource;
    use crate::principals::PrincipalAuthority;
    use crate::usage::identity::RequestId;
    use crate::usage::journal::{self, UsageJournal as _};
    use crate::usage::{StdoutSink, UsageDelivery, UsageFanout, UsageRecord, UsageSink};
    use axum::body::Body;
    use axum::http::{Method, Request, StatusCode};

    use gateway_transport::{TimeoutBound, TimeoutKind, TransportError};
    use http_body_util::BodyExt;

    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};

    use std::collections::HashMap;
    use std::future::pending;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, SystemTime};
    use tokio::sync::oneshot;
    use tower::util::ServiceExt;
    use tracing_subscriber::layer::SubscriberExt;

    #[test]
    fn usage_record_copies_admission_attrs() {
        let identity = EventIdentity {
            request_id: crate::usage::identity::next_request_id(),
            trace_id: None,
        };
        let caller = InboundKey {
            namespace: "wsp_x".to_owned(),
            subject: "GW_TEST_INBOUND_KEY".to_owned(),
            authority: PrincipalAuthority::StaticKey,
            signer_kid: None,
            scope: None,
            max_request_microdollars: None,
            namespace_grant: None,
            attrs: None,
        };
        let (row, _, _) = build_record(RecordArgs {
            identity: &identity,
            caller: &caller,
            alias: "fast",
            target_provider: "openai",
            target_model: "gpt-4o",
            source: CredentialSource::Platform,
            credential_id: "openai-primary",
            status: Status::Ok,
            input_tokens: 1,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
            output_tokens: 1,
            cost_microdollars: Some(1),
            latency_ms: 1,
            ttft_ms: None,
            attempts: 1,
            attrs: Some(json!({"org": "acme"})),
            period: Some("2026-09".into()),
        });
        assert_eq!(row.attrs, Some(json!({"org": "acme"})));
        assert_eq!(row.namespace, "wsp_x");
        assert_eq!(row.period.as_deref(), Some("2026-09"));
    }

    /// An unusable spelling of the output allowance never hides a usable one, so
    /// neither the output ceiling nor the budget hold can be dodged by sending
    /// `max_tokens: null` alongside a real allowance.
    #[test]
    fn the_requested_output_allowance_takes_the_largest_usable_spelling() {
        for (body, expected) in [
            (serde_json::json!({}), None),
            (serde_json::json!({"max_tokens": 32}), Some(32)),
            (
                serde_json::json!({"max_tokens": Value::Null, "max_completion_tokens": 500_000}),
                Some(500_000),
            ),
            (
                serde_json::json!({"max_tokens": "many", "max_output_tokens": 64}),
                Some(64),
            ),
            (
                serde_json::json!({"max_tokens": 8, "max_completion_tokens": 4_096}),
                Some(4_096),
            ),
            (serde_json::json!({"max_tokens": -1}), None),
        ] {
            assert_eq!(requested_output_tokens(&body), expected, "{body}");
        }
        assert_eq!(
            estimate_usage(
                &serde_json::json!({"max_tokens": Value::Null, "max_completion_tokens": 500_000})
            )
            .0
            .output_tokens,
            500_000,
            "the estimate uses the allowance the provider will honor"
        );
    }

    #[test]
    fn estimate_usage_measures_the_final_json_including_unknown_fields() {
        let body = json!({
            "model": "upstream-model",
            "unknown_native_field": {"keep": true},
            "messages": [{"role": "user", "content": "hello"}],
            "max_tokens": 32
        });
        let expected = serialized_json_len(&body).unwrap();
        let via_string = serde_json::to_string(&body).unwrap().len();
        let (usage, bytes) = estimate_usage(&body);
        assert_eq!(bytes, expected);
        assert_eq!(bytes, via_string);
        assert_eq!(usage.input_tokens, (expected / 4) as u64);
        assert_eq!(usage.output_tokens, 32);

        let mut expanded = body.clone();
        expanded["messages"][0]["content"] = json!("hello from middleware");
        let (_, expanded_bytes) = estimate_usage(&expanded);
        assert!(expanded_bytes > bytes);
        assert_eq!(expanded_bytes, serialized_json_len(&expanded).unwrap());
    }

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

    /// A stall is the target's problem whichever bound ended the wait; only a
    /// budget spent before dispatch is the gateway's, because then no target was
    /// asked anything.
    #[test]
    fn a_stalled_target_is_recorded_even_when_the_walk_budget_ended_the_wait() {
        for kind in [
            TimeoutKind::Connect,
            TimeoutKind::ResponseHeaders,
            TimeoutKind::BufferedBody,
            TimeoutKind::StreamIdle,
        ] {
            for bound in [TimeoutBound::Phase, TimeoutBound::WalkBudget] {
                let err = TransportError::Timeout {
                    kind,
                    bound,
                    budget_ms: 100,
                };
                assert!(as_provider_error(&err).affects_provider_health());
                assert!(
                    !was_never_dispatched(&err),
                    "{} on {}",
                    kind.label(),
                    bound.label()
                );
            }
        }

        let unattempted = TransportError::Timeout {
            kind: TimeoutKind::Overall,
            bound: TimeoutBound::WalkBudget,
            budget_ms: 0,
        };
        assert!(was_never_dispatched(&unattempted));
    }

    fn ns_path(uri: &str) -> String {
        if uri.starts_with("/ns/") {
            uri.to_owned()
        } else if let Some(rest) = uri.strip_prefix("/v1/") {
            format!("/ns/platform/v1/{rest}")
        } else if let Some(rest) = uri.strip_prefix("/namespaces/") {
            format!("/ns/{rest}")
        } else {
            uri.to_owned()
        }
    }

    /// A JSON `POST` that already carries the caller's gateway key.
    fn authorized(uri: &str) -> axum::http::request::Builder {
        Request::post(ns_path(uri))
            .header("content-type", "application/json")
            .header(
                axum::http::header::AUTHORIZATION,
                format!("Bearer {CALLER_SECRET}"),
            )
    }

    fn test_state() -> AppState {
        test_state_with_base_url("https://api.openai.com/v1")
    }

    fn test_state_with_base_url(base_url: &str) -> AppState {
        let (cfg, env) = test_config_with_base_url(base_url);
        let sinks: Vec<Box<dyn UsageSink>> = vec![Box::new(StdoutSink)];
        AppState::new(cfg, &env, UsageFanout::new(sinks), Box::new(NoBudget))
            .expect("credentials resolve")
    }

    fn test_config_with_base_url(base_url: &str) -> (Config, HashMap<String, String>) {
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
env = "AXOND_PLATFORM_OPENAI"

{GATEWAY_KEY}

[[price]]
provider = "openai"
model = "*"
input_microdollars_per_million = 2500000
output_microdollars_per_million = 10000000
"#
        ))
        .unwrap();
        let env = env_with([("AXOND_PLATFORM_OPENAI", "sk-platform-test")]);
        (cfg, env)
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
        assert!(namespace_allows(
            &snapshot,
            "platform",
            Capability::Responses
        ));
        assert!(!namespace_allows(
            &snapshot,
            "platform",
            Capability::Messages
        ));
    }

    /// Inbound auth is enforced for every configured key set: the wrong
    /// credential, and no credential at all, are both `401`.
    #[tokio::test]
    async fn a_request_without_a_valid_gateway_key_is_rejected() {
        let body = || {
            Body::from(
                serde_json::to_vec(&json!({"model": "openai/gpt-4o", "messages": []})).unwrap(),
            )
        };
        for request in [
            Request::post("/ns/platform/v1/chat/completions")
                .header("content-type", "application/json")
                .body(body())
                .unwrap(),
            Request::post("/ns/platform/v1/chat/completions")
                .header("content-type", "application/json")
                .header(axum::http::header::AUTHORIZATION, "Bearer not-the-key")
                .body(body())
                .unwrap(),
            Request::post("/ns/platform/v1/chat/completions")
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
            .filter(|spec| spec.auth.requires_a_credential())
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

        for (method, path) in [
            ("GET", "/api/v1/openapi.json"),
            ("GET", "/api/v1/namespaces"),
            ("POST", "/api/v1/namespaces"),
            ("GET", "/api/v1/namespaces/platform"),
            ("PUT", "/api/v1/namespaces/platform"),
            ("DELETE", "/api/v1/namespaces/platform"),
            ("GET", "/api/v1/namespaces/platform/budgets/harness"),
            ("PUT", "/api/v1/namespaces/platform/budgets/harness"),
            ("GET", "/api/v1/namespaces/platform/usage?period=harness"),
            ("GET", "/api/v1/providers/models"),
            ("GET", "/api/v1/providers/fake-openai/models"),
        ] {
            let request = Request::builder()
                .method(method)
                .uri(path)
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap();
            let response = router(test_state()).oneshot(request).await.unwrap();
            assert_eq!(
                response.status(),
                StatusCode::UNAUTHORIZED,
                "{method} {path} must authenticate before handling the request"
            );
        }
    }

    #[tokio::test]
    async fn openapi_json_requires_the_gateway_key() {
        let unauthorized = router(test_state())
            .oneshot(
                Request::get("/api/v1/openapi.json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        let wrong = router(test_state())
            .oneshot(
                Request::get("/api/v1/openapi.json")
                    .header(axum::http::header::AUTHORIZATION, "Bearer not-the-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(wrong.status(), StatusCode::UNAUTHORIZED);

        let ok = router(test_state())
            .oneshot(
                Request::get("/api/v1/openapi.json")
                    .header(
                        axum::http::header::AUTHORIZATION,
                        format!("Bearer {CALLER_SECRET}"),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ok.status(), StatusCode::OK);
        let body = ok.into_body().collect().await.unwrap().to_bytes();
        let spec: Value = serde_json::from_slice(&body).unwrap();
        assert!(
            spec["openapi"]
                .as_str()
                .unwrap_or_default()
                .starts_with("3.1"),
            "{spec}"
        );
    }

    #[tokio::test]
    async fn usage_summary_matches_rows_for_namespace_and_period() {
        let state = test_state();
        let store = state.store().expect("store");
        for (id, ns, period, model, status, cost) in [
            (
                "req_a",
                "platform",
                "p",
                "openai/gpt-4o",
                "ok",
                Some(10_u64),
            ),
            ("req_b", "platform", "p", "openai/gpt-4o", "ok", Some(15)),
            (
                "req_c",
                "platform",
                "p",
                "openai/gpt-4o",
                "upstream_error",
                Some(1),
            ),
            (
                "req_d",
                "platform",
                "other",
                "openai/gpt-4o",
                "ok",
                Some(99),
            ),
        ] {
            store
                .append_usage(crate::store::UsageAppend {
                    request_id: id.into(),
                    namespace: ns.into(),
                    period: Some(period.into()),
                    model: model.into(),
                    status: status.into(),
                    cost_microdollars: cost,
                })
                .await
                .expect("append");
        }

        let missing_period = router(state.clone())
            .oneshot(
                Request::get("/api/v1/namespaces/platform/usage")
                    .header(
                        axum::http::header::AUTHORIZATION,
                        format!("Bearer {CALLER_SECRET}"),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing_period.status(), StatusCode::BAD_REQUEST);
        let body: Value = serde_json::from_slice(
            &missing_period
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes(),
        )
        .unwrap();
        assert_eq!(body["error"]["type"], "bad_request");

        let unknown = router(state.clone())
            .oneshot(
                Request::get("/api/v1/namespaces/ghost/usage?period=p")
                    .header(
                        axum::http::header::AUTHORIZATION,
                        format!("Bearer {CALLER_SECRET}"),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unknown.status(), StatusCode::NOT_FOUND);

        let response = router(state)
            .oneshot(
                Request::get("/api/v1/namespaces/platform/usage?period=p")
                    .header(
                        axum::http::header::AUTHORIZATION,
                        format!("Bearer {CALLER_SECRET}"),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(body["namespace"], "platform");
        assert_eq!(body["period"], "p");
        let data = body["data"].as_array().expect("data");
        assert_eq!(data.len(), 2, "{body}");
        assert_eq!(data[0]["model"], "openai/gpt-4o");
        assert_eq!(data[0]["status"], "ok");
        assert_eq!(data[0]["count"], 2);
        assert_eq!(data[0]["cost_microdollars"], 25);
        assert_eq!(data[1]["status"], "upstream_error");
        assert_eq!(data[1]["count"], 1);
        assert_eq!(data[1]["cost_microdollars"], 1);
    }

    #[tokio::test]
    async fn usage_summary_requires_period_query() {
        let response = router(test_state())
            .oneshot(
                Request::get("/api/v1/namespaces/platform/usage?period=")
                    .header(
                        axum::http::header::AUTHORIZATION,
                        format!("Bearer {CALLER_SECRET}"),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body: Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(body["error"]["type"], "bad_request");
    }

    #[tokio::test]
    async fn malformed_management_queries_are_typed_bad_request() {
        let state = test_state();
        for path in [
            "/api/v1/namespaces?limit=abc",
            "/api/v1/namespaces/platform/usage?period=bad/period",
            "/api/v1/namespaces/platform/usage?period=a&period=b",
            "/api/v1/namespaces/platform/usage",
        ] {
            let response = router(state.clone())
                .oneshot(
                    Request::get(path)
                        .header(
                            axum::http::header::AUTHORIZATION,
                            format!("Bearer {CALLER_SECRET}"),
                        )
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            let status = response.status();
            let body: Value =
                serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                    .unwrap_or_else(|_| json!({"raw": "not json"}));
            assert_eq!(status, StatusCode::BAD_REQUEST, "{path} {body}");
            assert_eq!(body["error"]["type"], "bad_request", "{path} {body}");
        }
    }

    #[test]
    fn canonical_namespace_routes_preserve_every_provider_suffix() {
        let paths: Vec<_> = route_specs()
            .into_iter()
            .filter(|spec| spec.namespace_scoped)
            .map(|spec| spec.path)
            .collect();
        assert_eq!(
            paths,
            [
                "/v1/models",
                "/v1/credentials",
                "/v1/chat/completions",
                "/v1/messages",
                "/v1/embeddings",
                "/v1/responses",
            ]
        );
        for suffix in paths {
            let canonical = format!("/namespaces/platform{suffix}");
            assert_eq!(
                canonical.strip_prefix("/namespaces/platform").unwrap(),
                suffix
            );
        }
    }

    #[tokio::test]
    async fn every_canonical_namespace_route_authenticates_first() {
        for spec in route_specs()
            .into_iter()
            .filter(|spec| spec.namespace_scoped)
        {
            let method = if matches!(spec.path, "/v1/models" | "/v1/credentials") {
                Method::GET
            } else {
                Method::POST
            };
            let response = router(test_state())
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(format!("/namespaces/ghost{}", spec.path))
                        .header("content-type", "application/json")
                        .body(Body::from("not-json"))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::UNAUTHORIZED,
                "{} disclosed namespace or body handling before authentication",
                spec.path
            );
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let body: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(body["error"]["type"], "unauthorized", "{}", spec.path);
        }
    }

    #[tokio::test]
    async fn canonical_route_uses_the_authorized_path_namespace() {
        let response = router(test_state())
            .oneshot(
                Request::get("/ns/platform/v1/models")
                    .header(
                        axum::http::header::AUTHORIZATION,
                        format!("Bearer {CALLER_SECRET}"),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn noncanonical_namespace_encoding_is_a_typed_refusal_after_authentication() {
        let response = router(test_state())
            .oneshot(
                Request::get("/ns/%70latform/v1/models")
                    .header(
                        axum::http::header::AUTHORIZATION,
                        format!("Bearer {CALLER_SECRET}"),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["error"]["type"], "invalid_namespace");
    }

    #[tokio::test]
    async fn the_responses_route_rejects_anonymous_callers_before_dispatching() {
        let resp = router(test_state())
            .oneshot(
                Request::post("/ns/platform/v1/responses")
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

    #[tokio::test]
    async fn healthz_is_ok() {
        let resp = router(test_state())
            .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    /// Draining is what a rolling deployment observes, and it must not take the
    /// liveness probe with it: a `/healthz` failure earns a `SIGKILL`, which is
    /// the one thing that would cut the requests the drain exists to finish.
    #[tokio::test]
    async fn draining_fails_readiness_while_liveness_stays_ok() {
        let state = test_state();
        let lifecycle = Arc::clone(state.lifecycle());
        let app = router(state);

        let ready = app
            .clone()
            .oneshot(Request::get("/readyz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(ready.status(), StatusCode::OK);

        lifecycle.begin_drain();
        let draining = app
            .clone()
            .oneshot(Request::get("/readyz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(draining.status(), StatusCode::SERVICE_UNAVAILABLE);
        let live = app
            .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(live.status(), StatusCode::OK);
    }

    /// The drain window keeps serving: readiness has failed, but a request that
    /// arrives before routing catches up is still answered rather than lost.
    #[tokio::test]
    async fn a_request_arriving_during_the_drain_window_is_still_served() {
        let state = test_state();
        let lifecycle = Arc::clone(state.lifecycle());
        let app = router(state);
        lifecycle.begin_drain();

        let resp = app
            .oneshot(
                Request::get("/ns/platform/v1/models")
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
    }

    #[tokio::test]
    async fn a_request_arriving_after_admission_closes_is_refused_as_draining() {
        let state = test_state();
        let lifecycle = Arc::clone(state.lifecycle());
        let app = router(state);
        lifecycle.close();

        let resp = app
            .clone()
            .oneshot(
                Request::get("/ns/platform/v1/models")
                    .header(
                        axum::http::header::AUTHORIZATION,
                        format!("Bearer {CALLER_SECRET}"),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let json: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["error"]["type"], "draining");

        // The probes stay outside admission, so an orchestrator can still tell a
        // draining replica from a dead one.
        let live = app
            .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(live.status(), StatusCode::OK);
    }

    /// Admission is released by the *response body*, not by the handler future:
    /// a streamed response is in flight for as long as its body is open, and
    /// that is precisely the work the shutdown deadline bounds.
    #[tokio::test]
    async fn a_request_counts_as_in_flight_until_its_body_is_dropped() {
        let state = test_state();
        let lifecycle = Arc::clone(state.lifecycle());
        let app = router(state);

        let resp = app
            .oneshot(
                Request::get("/ns/platform/v1/models")
                    .header(
                        axum::http::header::AUTHORIZATION,
                        format!("Bearer {CALLER_SECRET}"),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(lifecycle.in_flight(), 1, "the body is still undelivered");
        let body = resp.into_body();
        drop(body);
        assert_eq!(lifecycle.in_flight(), 0);
    }

    /// `/v1/models` fails closed like every other request path: no gateway key
    /// means `401`, not an open catalog (ADR 0013).
    #[tokio::test]
    async fn models_requires_a_gateway_key() {
        let resp = router(test_state())
            .oneshot(
                Request::get("/ns/platform/v1/models")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn models_lists_the_callers_aliases() {
        let resp = router(test_state())
            .oneshot(
                Request::get("/ns/platform/v1/models")
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
        assert_eq!(json["data"], json!([]));
    }

    #[tokio::test]
    async fn models_intersect_namespace_access_with_alias_scope() {
        let resp = router(test_state())
            .oneshot(
                Request::get("/ns/platform/v1/models")
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
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["data"], json!([]));
    }

    #[tokio::test]
    async fn unprefixed_model_is_typed_400() {
        let body = serde_json::to_vec(&json!({"model": "gpt-4o", "messages": []})).unwrap();
        let resp = router(test_state())
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
        assert_eq!(json["error"]["type"], "model_unprefixed");
    }

    #[tokio::test]
    async fn a_blocklist_glob_is_typed_400_and_not_dispatched() {
        let cfg = Config::from_toml_str(&format!(
            r#"
[[namespace]]
id = "platform"
default = true

[[provider]]
id = "openai"
kind = "openai"
base_url = "https://api.openai.com/v1"

{GATEWAY_KEY}

[blocklist]
models = ["*-preview"]

[[price]]
provider = "openai"
model = "*"
input_microdollars_per_million = 1
output_microdollars_per_million = 1
"#
        ))
        .unwrap();
        let state = AppState::new(
            cfg,
            &env_with([]),
            UsageFanout::new(vec![Box::new(StdoutSink)]),
            Box::new(NoBudget),
        )
        .unwrap();
        let body =
            serde_json::to_vec(&json!({"model": "openai/gpt-4o-preview", "messages": []})).unwrap();
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
        assert_eq!(json["error"]["type"], "model_blocked");
    }

    #[tokio::test]
    async fn a_namespace_blocklist_is_unioned_and_not_dispatched() {
        let (base_url, hits) = controllable_upstream(
            Arc::new(AtomicBool::new(false)),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
        let state = test_state_with_base_url(&base_url);
        state
            .store()
            .expect("store")
            .update_namespace("platform", json!({}), Some(vec!["secret-*".into()]))
            .await
            .expect("update")
            .expect("platform");
        let body =
            serde_json::to_vec(&json!({"model": "openai/secret-x", "messages": []})).unwrap();
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
        assert_eq!(json["error"]["type"], "model_blocked");
        assert_eq!(hits.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn unpriced_deny_is_typed_400() {
        let cfg = Config::from_toml_str(&format!(
            r#"
[[namespace]]
id = "platform"
default = true

[[provider]]
id = "openai"
kind = "openai"
base_url = "https://api.openai.com/v1"

{GATEWAY_KEY}
"#
        ))
        .unwrap();
        let state = AppState::new(
            cfg,
            &env_with([]),
            UsageFanout::new(vec![Box::new(StdoutSink)]),
            Box::new(NoBudget),
        )
        .unwrap();
        let body = serde_json::to_vec(&json!({"model": "openai/gpt-4o", "messages": []})).unwrap();
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
        assert_eq!(json["error"]["type"], "unpriced_model");
    }

    #[tokio::test]
    async fn unpriced_allow_dispatches_with_null_cost() {
        let base_url = rate_limiting_upstream("never-matches").await;
        let cfg = Config::from_toml_str(&format!(
            r#"
[[namespace]]
id = "platform"
default = true

[[provider]]
id = "openai"
kind = "openai"
base_url = "{base_url}"
unpriced_models = "allow"

[[credential]]
namespace = "platform"
provider = "openai"
env = "AXOND_PLATFORM_OPENAI"

{GATEWAY_KEY}
"#
        ))
        .unwrap();
        let captured = CapturingSink::default();
        let state = AppState::new(
            cfg,
            &env_with([("AXOND_PLATFORM_OPENAI", "sk-good")]),
            UsageFanout::new(vec![Box::new(captured.clone())]),
            Box::new(NoBudget),
        )
        .unwrap();
        let body = serde_json::to_vec(&json!({"model": "openai/gpt-4o", "messages": []})).unwrap();
        let resp = router(state)
            .oneshot(
                authorized("/v1/chat/completions")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK,);
        let records = captured.0.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].cost_microdollars, None);
        assert_eq!(records[0].target_model, "gpt-4o");
        assert_eq!(records[0].target_provider, "openai");
    }

    fn seed_catalog() -> crate::backends::catalog::CatalogSnapshot {
        crate::backends::models_dev::seed_snapshot()
    }

    fn state_with_catalog(
        cfg: Config,
        env: HashMap<String, String>,
        usage: UsageFanout,
        budget: Box<dyn crate::budget::BudgetStore>,
        snapshot: &crate::backends::catalog::CatalogSnapshot,
    ) -> AppState {
        let status = crate::backends::catalog_runtime::CatalogStatus::new();
        status.install_snapshot(snapshot);
        AppState::with_resources(
            cfg,
            &env,
            Arc::new(UsageDelivery::telemetry(usage)),
            budget,
            Some(Arc::new(status)),
        )
        .expect("credentials resolve")
    }

    #[tokio::test]
    async fn catalog_rates_charge_without_a_price_book() {
        let base_url = rate_limiting_upstream("never-matches").await;
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
env = "AXOND_PLATFORM_OPENAI"

{GATEWAY_KEY}
"#
        ))
        .unwrap();
        let captured = CapturingSink::default();
        let state = state_with_catalog(
            cfg,
            env_with([("AXOND_PLATFORM_OPENAI", "sk-good")]),
            UsageFanout::new(vec![Box::new(captured.clone())]),
            Box::new(NoBudget),
            &seed_catalog(),
        );
        let body = serde_json::to_vec(&json!({"model": "openai/gpt-4o", "messages": []})).unwrap();
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
        // seed cost: $2.5 / $10 per million; fake upstream reports 10+5 tokens.
        assert_eq!(records[0].cost_microdollars, Some(75));
        assert_eq!(records[0].target_model, "gpt-4o");
        assert_eq!(records[0].input_tokens, 10);
        assert_eq!(records[0].output_tokens, 5);
    }

    #[tokio::test]
    async fn catalog_miss_with_deny_is_typed_400() {
        let cfg = Config::from_toml_str(&format!(
            r#"
[[namespace]]
id = "platform"
default = true

[[provider]]
id = "openai"
kind = "openai"
base_url = "https://api.openai.com/v1"

{GATEWAY_KEY}
"#
        ))
        .unwrap();
        let state = state_with_catalog(
            cfg,
            env_with([]),
            UsageFanout::new(vec![Box::new(StdoutSink)]),
            Box::new(NoBudget),
            &seed_catalog(),
        );
        let body =
            serde_json::to_vec(&json!({"model": "openai/not-in-catalog", "messages": []})).unwrap();
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
        assert_eq!(json["error"]["type"], "unpriced_model");
    }

    #[tokio::test]
    async fn catalog_miss_with_allow_dispatches_with_null_cost() {
        let base_url = rate_limiting_upstream("never-matches").await;
        let cfg = Config::from_toml_str(&format!(
            r#"
[[namespace]]
id = "platform"
default = true

[[provider]]
id = "openai"
kind = "openai"
base_url = "{base_url}"
unpriced_models = "allow"

[[credential]]
namespace = "platform"
provider = "openai"
env = "AXOND_PLATFORM_OPENAI"

{GATEWAY_KEY}
"#
        ))
        .unwrap();
        let captured = CapturingSink::default();
        let state = state_with_catalog(
            cfg,
            env_with([("AXOND_PLATFORM_OPENAI", "sk-good")]),
            UsageFanout::new(vec![Box::new(captured.clone())]),
            Box::new(NoBudget),
            &seed_catalog(),
        );
        let body =
            serde_json::to_vec(&json!({"model": "openai/not-in-catalog", "messages": []})).unwrap();
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
        assert_eq!(records[0].cost_microdollars, None);
    }

    #[tokio::test]
    async fn catalog_rates_win_over_a_price_book_row() {
        let base_url = rate_limiting_upstream("never-matches").await;
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
env = "AXOND_PLATFORM_OPENAI"

{GATEWAY_KEY}

[[price]]
provider = "openai"
model = "gpt-4o"
input_microdollars_per_million = 1
output_microdollars_per_million = 1
"#
        ))
        .unwrap();
        let captured = CapturingSink::default();
        let state = state_with_catalog(
            cfg,
            env_with([("AXOND_PLATFORM_OPENAI", "sk-good")]),
            UsageFanout::new(vec![Box::new(captured.clone())]),
            Box::new(NoBudget),
            &seed_catalog(),
        );
        let body = serde_json::to_vec(&json!({"model": "openai/gpt-4o", "messages": []})).unwrap();
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
        assert_eq!(
            records[0].cost_microdollars,
            Some(75),
            "models.dev rates, not the [[price]] override"
        );
    }

    /// The seed publishes `gpt-5.5` with a `context_over_200k` tier. A flat
    /// rate cannot hold that schedule, so the catalogue does not price it and
    /// the `[[price]]` row is what the request is charged at.
    #[tokio::test]
    async fn tiered_catalog_cost_falls_back_to_the_price_book() {
        let base_url = rate_limiting_upstream("never-matches").await;
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
env = "AXOND_PLATFORM_OPENAI"

{GATEWAY_KEY}

[[price]]
provider = "openai"
model = "gpt-5.5"
input_microdollars_per_million = 10000000
output_microdollars_per_million = 45000000
"#
        ))
        .unwrap();
        let captured = CapturingSink::default();
        let state = state_with_catalog(
            cfg,
            env_with([("AXOND_PLATFORM_OPENAI", "sk-good")]),
            UsageFanout::new(vec![Box::new(captured.clone())]),
            Box::new(NoBudget),
            &seed_catalog(),
        );
        let body = serde_json::to_vec(&json!({"model": "openai/gpt-5.5", "messages": []})).unwrap();
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
        // 10 * 10_000_000 / 1e6 + 5 * 45_000_000 / 1e6 = 100 + 225, not the
        // seed's $5 / $30 base rate (200).
        assert_eq!(records[0].cost_microdollars, Some(325));
    }

    #[tokio::test]
    async fn price_book_covers_offerings_the_catalogue_does_not_price() {
        let base_url = rate_limiting_upstream("never-matches").await;
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
env = "AXOND_PLATFORM_OPENAI"

{GATEWAY_KEY}

[[price]]
provider = "openai"
model = "custom-deploy"
input_microdollars_per_million = 2500000
output_microdollars_per_million = 10000000
"#
        ))
        .unwrap();
        let captured = CapturingSink::default();
        let state = state_with_catalog(
            cfg,
            env_with([("AXOND_PLATFORM_OPENAI", "sk-good")]),
            UsageFanout::new(vec![Box::new(captured.clone())]),
            Box::new(NoBudget),
            &seed_catalog(),
        );
        let body =
            serde_json::to_vec(&json!({"model": "openai/custom-deploy", "messages": []})).unwrap();
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
        // 10 * 2_500_000 / 1e6 + 5 * 10_000_000 / 1e6 = 25 + 50
        assert_eq!(records[0].cost_microdollars, Some(75));
    }

    #[tokio::test]
    async fn unknown_provider_prefix_is_typed_400() {
        let body = serde_json::to_vec(&json!({"model": "nope/x", "messages": []})).unwrap();
        let resp = router(test_state())
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
        assert_eq!(json["error"]["type"], "unknown_provider");
    }

    #[tokio::test]
    async fn the_responses_route_dispatches_through_the_shared_path() {
        let (base_url, _) = controllable_upstream(
            Arc::new(AtomicBool::new(false)),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
        let resp = router(test_state_with_base_url(&base_url))
            .oneshot(
                authorized("/v1/responses")
                    .body(Body::from(r#"{"model":"openai/gpt-4o","input":"hello"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let json: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["error"]["type"], "provider_dependency_failed");
    }

    /// An alias whose targets cannot speak the route's wire is the caller's
    /// mistake, answered as a typed 4xx before anything is dispatched — there is
    /// no translation to fall back on for a native route.
    #[tokio::test]
    async fn an_openai_only_alias_on_the_native_route_is_a_typed_4xx() {
        let body =
            serde_json::to_vec(&json!({ "model": "openai/gpt-4o", "messages": [] })).unwrap();
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

[[price]]
provider = "anthropic"
model = "claude-sonnet-4-5"
input_microdollars_per_million = 1000000
output_microdollars_per_million = 2000000
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

        let body =
            serde_json::to_vec(&json!({ "model": "anthropic/claude-sonnet-4-5", "messages": [] }))
                .unwrap();
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

    async fn credential_probe_upstream(reject_first: bool) -> (String, Arc<Mutex<Vec<String>>>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let app_seen = seen.clone();
        let app = Router::new().route(
            "/responses",
            post(move |headers: HeaderMap| {
                let seen = app_seen.clone();
                async move {
                    let authorization = headers
                        .get(axum::http::header::AUTHORIZATION)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default()
                        .to_owned();
                    seen.lock().unwrap().push(authorization.clone());
                    if reject_first && authorization == "Bearer sk-a" {
                        (
                            StatusCode::TOO_MANY_REQUESTS,
                            Json(json!({ "error": { "message": "rate limit exceeded" } })),
                        )
                            .into_response()
                    } else {
                        Json(json!({
                            "id": "resp-1",
                            "object": "response",
                            "usage": {
                                "input_tokens": 10,
                                "output_tokens": 5
                            }
                        }))
                        .into_response()
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}"), seen)
    }

    fn two_credential_responses_state(base_url: &str, captured: CapturingSink) -> AppState {
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

[[price]]
provider = "openai"
model = "gpt-4o"
input_microdollars_per_million = 1000000
output_microdollars_per_million = 1000000
"#
        ))
        .unwrap();
        let env = env_with([("K1", "sk-a"), ("K2", "sk-b")]);
        let sinks: Vec<Box<dyn UsageSink>> = vec![Box::new(captured)];
        AppState::new(cfg, &env, UsageFanout::new(sinks), Box::new(NoBudget)).unwrap()
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

[[price]]
provider = "openai"
model = "gpt-4o"
input_microdollars_per_million = 2500000
output_microdollars_per_million = 10000000
"#
        ))
        .unwrap();
        let env = env_with([("K1", "sk-exhausted"), ("K2", "sk-good")]);
        let captured = CapturingSink::default();
        let sinks: Vec<Box<dyn UsageSink>> = vec![Box::new(captured.clone())];
        let state = AppState::new(cfg, &env, UsageFanout::new(sinks), Box::new(NoBudget)).unwrap();

        let body = serde_json::to_vec(&json!({"model": "openai/gpt-4o", "messages": []})).unwrap();
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
        use tracing::instrument::WithSubscriber as _;
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

[[price]]
provider = "openai"
model = "gpt-4o"
input_microdollars_per_million = 1
output_microdollars_per_million = 1
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

        crate::telemetry::testing::keep_callsites_answerable();
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let subscriber = tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("axond-test")));
        let body = serde_json::to_vec(&json!({"model": "openai/gpt-4o", "messages": []})).unwrap();
        let dispatch = tracing::Dispatch::new(subscriber);
        // The subscriber travels with the future, not with the thread that
        // spawned it: `set_default` is thread-local, so a task the runtime
        // resumes on another worker after an await would record nothing.
        let response = tokio::spawn(
            async move {
                router(state)
                    .oneshot(
                        authorized("/v1/chat/completions")
                            .body(Body::from(body))
                            .unwrap(),
                    )
                    .await
                    .unwrap()
            }
            .with_subscriber(dispatch),
        )
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
    #[derive(Clone)]
    struct ControllableState {
        healthy: Arc<AtomicBool>,
        hits: Arc<AtomicUsize>,
        unhealthy_status: StatusCode,
    }

    async fn controllable_handler(State(state): State<ControllableState>) -> Response {
        state.hits.fetch_add(1, Ordering::SeqCst);
        if state.healthy.load(Ordering::SeqCst) {
            Json(json!({
                "id": "resp-1",
                "object": "response",
                "choices": [],
                "usage": {
                    "input_tokens": 10,
                    "output_tokens": 5,
                    "prompt_tokens": 10,
                    "completion_tokens": 5
                }
            }))
            .into_response()
        } else {
            (
                state.unhealthy_status,
                Json(json!({ "error": { "message": "upstream is unwell" } })),
            )
                .into_response()
        }
    }

    async fn controllable_upstream(
        healthy: Arc<AtomicBool>,
        unhealthy_status: StatusCode,
    ) -> (String, Arc<AtomicUsize>) {
        let hits = Arc::new(AtomicUsize::new(0));
        let state = ControllableState {
            healthy,
            hits: hits.clone(),
            unhealthy_status,
        };
        let app = Router::new()
            .route("/chat/completions", post(controllable_handler))
            .route("/responses", post(controllable_handler))
            .with_state(state);
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
        two_target_state_with_budget(url_a, url_b, failover, captured, Box::new(NoBudget))
    }

    fn two_target_state_with_budget(
        url_a: &str,
        url_b: &str,
        failover: &str,
        captured: CapturingSink,
        budget: Box<dyn crate::budget::BudgetStore>,
    ) -> AppState {
        let cfg = Config::from_toml_str(&format!(
            r#"
[[namespace]]
id = "platform"
default = true

[[provider]]
id = "openai"
kind = "openai"
base_url = "{url_a}"

[[provider]]
id = "pb"
kind = "openai"
base_url = "{url_b}"

{GATEWAY_KEY}

[[credential]]
namespace = "platform"
provider = "openai"
env = "KA"
id = "cred-a"

[[credential]]
namespace = "platform"
provider = "pb"
env = "KB"
id = "cred-b"

{failover}

[[price]]
provider = "openai"
model = "*"
input_microdollars_per_million = 1000000
output_microdollars_per_million = 1000000
[[price]]
provider = "pb"
model = "*"
input_microdollars_per_million = 1000000
output_microdollars_per_million = 1000000
"#
        ))
        .unwrap();
        let env = env_with([("KA", "ka"), ("KB", "kb")]);
        let sinks: Vec<Box<dyn UsageSink>> = vec![Box::new(captured)];
        AppState::new(cfg, &env, UsageFanout::new(sinks), budget).unwrap()
    }

    fn chat_request() -> Request<Body> {
        let body = serde_json::to_vec(&json!({"model": "openai/gpt-4o", "messages": []})).unwrap();
        authorized("/v1/chat/completions")
            .body(Body::from(body))
            .unwrap()
    }

    fn responses_request(previous_response_id: Option<&str>) -> Request<Body> {
        let mut body = json!({"model": "openai/gpt-4o", "input": "hello"});
        if let Some(id) = previous_response_id {
            body["previous_response_id"] = json!(id);
        }
        authorized("/v1/responses")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap()
    }

    fn streaming_responses_request(previous_response_id: Option<&str>) -> Request<Body> {
        let mut body = json!({"model": "openai/gpt-4o", "input": "hello", "stream": true});
        if let Some(id) = previous_response_id {
            body["previous_response_id"] = json!(id);
        }
        authorized("/v1/responses")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap()
    }

    fn responses_request_with_null_previous_id() -> Request<Body> {
        let body = json!({
            "model": "openai/gpt-4o",
            "input": "hello",
            "previous_response_id": null
        });
        authorized("/v1/responses")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
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
                period: None,
                incarnation: None,
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

    #[derive(Clone)]
    struct BlockingSettlementBudget {
        entered: Arc<AtomicBool>,
        release: Arc<AtomicBool>,
        settlements: Arc<Mutex<Vec<u64>>>,
    }

    #[async_trait::async_trait]
    impl crate::budget::BudgetStore for BlockingSettlementBudget {
        fn name(&self) -> &'static str {
            "blocking-settlement"
        }

        async fn reserve(&self, _key: &BudgetKey, estimated_microdollars: u64) -> Admission {
            Admission::Allowed(Reservation {
                id: "blocking-settlement".to_owned(),
                estimate_microdollars: estimated_microdollars,
                period: None,
                incarnation: None,
            })
        }

        async fn settle(
            &self,
            _key: &BudgetKey,
            _reservation: &Reservation,
            actual_microdollars: u64,
        ) {
            self.settlements
                .lock()
                .expect("settlements")
                .push(actual_microdollars);
            self.entered.store(true, Ordering::Release);
            while !self.release.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        }
    }

    #[tokio::test]
    async fn cancellation_during_settlement_keeps_the_decided_outcome_once() {
        let (base_url, hits) = controllable_upstream(
            Arc::new(AtomicBool::new(true)),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
        let captured = CapturingSink::default();
        let entered = Arc::new(AtomicBool::new(false));
        let release = Arc::new(AtomicBool::new(false));
        let settlements = Arc::new(Mutex::new(Vec::new()));
        let budget = BlockingSettlementBudget {
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
            settlements: Arc::clone(&settlements),
        };
        let state = two_target_state_with_budget(
            &base_url,
            &base_url,
            "",
            captured.clone(),
            Box::new(budget),
        );

        let capacity = state.0.settlements.clone();
        let request = tokio::spawn(async move { router(state).oneshot(chat_request()).await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !entered.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("budget settlement starts after provider success");
        request.abort();
        assert!(
            request
                .await
                .expect_err("request future is cancelled")
                .is_cancelled()
        );
        release.store(true, Ordering::Release);
        capacity.await_idle(Duration::from_secs(2)).await;

        assert_eq!(hits.load(Ordering::SeqCst), 1);
        assert_eq!(
            settlements.lock().expect("settlements").as_slice(),
            &[15],
            "known provider spend is settled exactly once"
        );
        let records = captured.0.lock().expect("records");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].status, Status::Ok);
        assert_eq!(records[0].cost_microdollars, Some(15));
    }

    /// A ledger that stalls every charge until told otherwise, and counts how
    /// many charges are stalled inside it: the sustained slow-Store scenario.
    #[derive(Clone, Default)]
    struct StalledLedger {
        entered: Arc<AtomicUsize>,
        release: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl crate::budget::BudgetStore for StalledLedger {
        fn name(&self) -> &'static str {
            "stalled-ledger"
        }

        async fn reserve(&self, _key: &BudgetKey, estimated_microdollars: u64) -> Admission {
            Admission::Allowed(Reservation {
                id: "stalled".to_owned(),
                estimate_microdollars: estimated_microdollars,
                period: None,
                incarnation: None,
            })
        }

        async fn settle(&self, _key: &BudgetKey, _reservation: &Reservation, _actual: u64) {
            self.entered.fetch_add(1, Ordering::AcqRel);
            while !self.release.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        }
    }

    /// The acceptance criterion for #467: with the Store stalled, the process
    /// carries a bounded number of settlements, refuses further admissions with
    /// a typed `503` while it is at that bound, and admits again once the
    /// stalled settlements land — none of which were dropped.
    #[tokio::test]
    async fn a_stalled_store_bounds_settlements_and_sheds_new_admissions() {
        let (base_url, hits) = controllable_upstream(
            Arc::new(AtomicBool::new(true)),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
        let captured = CapturingSink::default();
        let ledger = StalledLedger::default();
        let state = two_target_state_with_budget(
            &base_url,
            &base_url,
            "[admission]\nmax_in_flight = 2\nmax_in_flight_per_tenant = 0\n\
             max_pending_settlements = 2\nmax_in_flight_settlements = 1\n",
            captured.clone(),
            Box::new(ledger.clone()),
        );
        let settlements = state.0.settlements.clone();

        // Two served requests whose charges are now stalled in the Store. The
        // callers hang up; the settlements carry on, holding the two slots.
        let mut served = Vec::new();
        for _ in 0..2 {
            let state = state.clone();
            served.push(tokio::spawn(async move {
                router(state).oneshot(chat_request()).await
            }));
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            // One execution slot: one charge is inside the ledger, the other
            // is queued behind it.
            while ledger.entered.load(Ordering::Acquire) < 1 || settlements.backlog().queued < 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("both settlements are spawned and bounded to one execution");
        for request in served {
            request.abort();
            let _ = request.await;
        }
        assert_eq!(hits.load(Ordering::SeqCst), 2);
        let backlog = settlements.backlog();
        assert_eq!(
            (backlog.executing, backlog.queued, backlog.reserved),
            (1, 1, 0),
            "at the bound: one executing, one queued, nothing more admitted"
        );

        // The bound: a third request is refused before it reaches a provider,
        // with the stable code and retry guidance, rather than admitted with a
        // charge nowhere to go.
        let refused = router(state.clone())
            .oneshot(chat_request())
            .await
            .expect("response");
        assert_eq!(refused.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            refused
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok()),
            Some("1")
        );
        let body: Value = serde_json::from_slice(
            &axum::body::to_bytes(refused.into_body(), usize::MAX)
                .await
                .expect("body"),
        )
        .expect("json");
        assert_eq!(body["error"]["type"], "settlement_capacity_exhausted");
        assert_eq!(
            hits.load(Ordering::SeqCst),
            2,
            "the refused request cost nothing upstream"
        );
        assert_eq!(
            settlements.backlog().reserved,
            0,
            "a refused request holds no capacity"
        );

        // The Store recovers: every stalled charge lands, the capacity returns,
        // and the next request is served.
        ledger.release.store(true, Ordering::Release);
        let idle = settlements.await_idle(Duration::from_secs(2)).await;
        assert_eq!(idle.unsettled(), 0);
        assert_eq!(
            ledger.entered.load(Ordering::Acquire),
            2,
            "no admitted charge was dropped"
        );
        let served = router(state.clone())
            .oneshot(chat_request())
            .await
            .expect("response");
        assert_eq!(served.status(), StatusCode::OK);
        settlements.await_idle(Duration::from_secs(2)).await;
        assert_eq!(hits.load(Ordering::SeqCst), 3);
        assert_eq!(ledger.entered.load(Ordering::Acquire), 3);
        let records = captured.0.lock().expect("records");
        assert_eq!(
            records.len(),
            3,
            "every admitted request produced its record"
        );
        assert!(
            records
                .iter()
                .all(|record| record.cost_microdollars == Some(15))
        );
    }

    #[test]
    fn malformed_routing_controls_are_rejected_before_middleware_or_dispatch() {
        for body in [
            json!({"model": "chat", "stream": "alice@example.com"}),
            json!({
                "model": "chat",
                "previous_response_id": {"value": "alice@example.com"}
            }),
        ] {
            let original = body.clone();
            assert!(matches!(
                Route::Responses.validate_routing_controls(&body),
                Err(GatewayError::BadRequest(_))
            ));
            assert_eq!(body, original);
        }

        for body in [
            json!({"model": "chat"}),
            json!({"model": "chat", "stream": false}),
            json!({"model": "chat", "previous_response_id": null}),
            json!({"model": "chat", "previous_response_id": "resp_1"}),
        ] {
            Route::Responses
                .validate_routing_controls(&body)
                .expect("valid routing controls");
        }

        let embeddings_stream = json!({"model": "embed", "stream": true, "input": "hello"});
        assert!(matches!(
            Route::Embeddings.validate_routing_controls(&embeddings_stream),
            Err(GatewayError::BadRequest(message))
                if message == "/v1/embeddings does not support streaming"
        ));
    }

    async fn redaction_responses_upstream(
        complete_token: bool,
    ) -> (String, Arc<Mutex<Vec<Value>>>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let upstream_seen = Arc::clone(&seen);
        let app = Router::new().route(
            "/responses",
            post(move |Json(body): Json<Value>| {
                let upstream_seen = Arc::clone(&upstream_seen);
                async move {
                    upstream_seen.lock().unwrap().push(body.clone());
                    let content = body["input"].as_str().expect("masked Responses input");
                    let cut = content.len() / 2;
                    let first = format!(
                        concat!(
                            "event: response.output_text.delta\n",
                            "data: {{\"type\":\"response.output_text.delta\",\"item_id\":\"item_0\",\"output_index\":0,\"content_index\":0,\"delta\":{}}}\n\n"
                        ),
                        serde_json::to_string(&content[..cut]).unwrap(),
                    );
                    let second = if complete_token {
                        format!(
                            concat!(
                            "event: response.output_text.delta\n",
                            "data: {{\"type\":\"response.output_text.delta\",\"item_id\":\"item_0\",\"output_index\":0,\"content_index\":0,\"delta\":{}}}\n\n"
                            ),
                            serde_json::to_string(&content[cut..]).unwrap(),
                        )
                    } else {
                        String::new()
                    };
                    let terminal = concat!(
                        "event: response.completed\n",
                        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"status\":\"completed\",\"usage\":{\"input_tokens\":3,\"output_tokens\":1}}}\n\n"
                    );
                    let stream = format!("{first}{second}{terminal}");
                    (
                        [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                        Body::from(stream),
                    )
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind Responses redaction fixture");
        let addr = listener.local_addr().expect("Responses redaction address");
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}"), seen)
    }

    fn responses_stream_state_with_sink(url_a: &str, url_b: &str) -> (AppState, CapturingSink) {
        let config = Config::from_toml_str(&format!(
            r#"
[[namespace]]
id = "platform"
default = true

[[provider]]
id = "openai"
kind = "openai"
base_url = "{url_a}"

[[provider]]
id = "pb"
kind = "openai"
base_url = "{url_b}"

{GATEWAY_KEY}

[[credential]]
namespace = "platform"
provider = "openai"
env = "RESPONSES_KEY_A"

[[credential]]
namespace = "platform"
provider = "pb"
env = "RESPONSES_KEY_B"

[failover]
max_attempts = 3

[[price]]
provider = "openai"
model = "*"
input_microdollars_per_million = 1
output_microdollars_per_million = 1
[[price]]
provider = "pb"
model = "*"
input_microdollars_per_million = 1
output_microdollars_per_million = 1
"#,
        ))
        .expect("Responses stream config");
        let usage = CapturingSink::default();
        let state = AppState::new(
            config,
            &env_with([
                ("RESPONSES_KEY_A", "responses-a"),
                ("RESPONSES_KEY_B", "responses-b"),
            ]),
            UsageFanout::new(vec![Box::new(usage.clone())]),
            Box::new(NoBudget),
        )
        .expect("Responses stream state");
        (state, usage)
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

{GATEWAY_KEY}

[[credential]]
namespace = "platform"
provider = "openai"
env = "K1"

[[price]]
provider = "openai"
model = "gpt-4o"
input_microdollars_per_million = 1000000
output_microdollars_per_million = 1000000
"#
        ))
        .unwrap();
        let env = env_with([("K1", "sk-test")]);
        let sinks: Vec<Box<dyn UsageSink>> = vec![Box::new(StdoutSink)];
        AppState::new(cfg, &env, UsageFanout::new(sinks), budget).unwrap()
    }

    /// One target, one credential, and an explicit `[admission]` section.
    fn admitting_state(
        base_url: &str,
        admission: &str,
        budget: Box<dyn crate::budget::BudgetStore>,
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

[[price]]
provider = "openai"
model = "gpt-4o"
input_microdollars_per_million = 1000000
output_microdollars_per_million = 1000000

[admission]
{admission}
"#
        ))
        .unwrap();
        let env = env_with([("K1", "sk-test")]);
        let sinks: Vec<Box<dyn UsageSink>> = vec![Box::new(StdoutSink)];
        AppState::new(cfg, &env, UsageFanout::new(sinks), budget).unwrap()
    }

    /// Shedding is the first thing the request path spends nothing on: a
    /// saturated replica must not pay for a rate-limit round trip, a budget
    /// reservation, or a provider call to say no.
    #[tokio::test]
    async fn a_shed_request_costs_no_budget_and_no_provider_call() {
        let (base_url, hits) =
            controllable_upstream(Arc::new(AtomicBool::new(true)), StatusCode::OK).await;
        let budget = RecordingBudget::default();
        let state = admitting_state(
            &base_url,
            "max_in_flight = 1\nmax_in_flight_streams = 1\nmax_in_flight_per_tenant = 0",
            Box::new(budget.clone()),
        );
        let held = state
            .0
            .admission
            .admit("platform", crate::admission::RequestKind::Buffered)
            .await
            .expect("the only slot");

        let response = router(state.clone()).oneshot(chat_request()).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"]["type"], "gateway_overloaded");
        assert!(budget.0.lock().unwrap().is_empty());
        assert_eq!(hits.load(Ordering::SeqCst), 0);

        // The permit the shed request never took is still the held one; giving
        // it back admits the next caller.
        drop(held);
        let served = router(state).oneshot(chat_request()).await.unwrap();
        assert_eq!(served.status(), StatusCode::OK);
        assert!(hits.load(Ordering::SeqCst) > 0);
    }

    /// A tenant's own ceiling is the caller's problem (429); the process's is
    /// the replica's (503). An operator reading either one knows which.
    #[tokio::test]
    async fn a_tenant_ceiling_sheds_as_429_and_leaves_the_replica_serving() {
        let (base_url, _) =
            controllable_upstream(Arc::new(AtomicBool::new(true)), StatusCode::OK).await;
        let state = admitting_state(
            &base_url,
            "max_in_flight = 8\nmax_in_flight_streams = 8\nmax_in_flight_per_tenant = 1",
            Box::new(NoBudget),
        );
        let held = state
            .0
            .admission
            .admit("platform", crate::admission::RequestKind::Buffered)
            .await
            .expect("the tenant's only slot");

        let response = router(state.clone()).oneshot(chat_request()).await.unwrap();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"]["type"], "tenant_concurrency_exceeded");

        // Another tenant's request is unaffected: the ceiling that fired was
        // this tenant's, not the process's.
        assert!(
            state
                .0
                .admission
                .admit("other", crate::admission::RequestKind::Buffered)
                .await
                .is_ok()
        );
        drop(held);
    }

    /// An admitted request gives its capacity back when the handler returns, so
    /// a bounded replica serves an unbounded number of sequential requests.
    #[tokio::test]
    async fn a_completed_request_releases_the_capacity_it_held() {
        let (base_url, _) =
            controllable_upstream(Arc::new(AtomicBool::new(true)), StatusCode::OK).await;
        let state = admitting_state(
            &base_url,
            "max_in_flight = 1\nmax_in_flight_streams = 1\nmax_in_flight_per_tenant = 1",
            Box::new(NoBudget),
        );
        for _ in 0..3 {
            let response = router(state.clone()).oneshot(chat_request()).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }
    }

    /// The per-request bounds are refusals, not clamps, and neither answer
    /// repeats what the caller sent.
    #[tokio::test]
    async fn per_request_bounds_are_typed_and_never_echo_the_request() {
        let (base_url, hits) =
            controllable_upstream(Arc::new(AtomicBool::new(true)), StatusCode::OK).await;
        let budget = RecordingBudget::default();
        let state = admitting_state(
            &base_url,
            "max_prompt_tokens = 64\nmax_output_tokens = 16",
            Box::new(budget.clone()),
        );

        let long_prompt = json!({
            "model": "openai/gpt-4o",
            "messages": [{"role": "user", "content": "sensitive ".repeat(32)}]
        });
        let response = router(state.clone())
            .oneshot(
                authorized("/v1/chat/completions")
                    .body(Body::from(serde_json::to_vec(&long_prompt).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"]["type"], "prompt_too_large");
        assert!(!body.to_string().contains("sensitive"), "{body}");

        let large_output = json!({
            "model": "openai/gpt-4o",
            "messages": [],
            "max_tokens": 4096
        });
        let response = router(state)
            .oneshot(
                authorized("/v1/chat/completions")
                    .body(Body::from(serde_json::to_vec(&large_output).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"]["type"], "output_limit_exceeded");

        assert!(budget.0.lock().unwrap().is_empty());
        assert_eq!(hits.load(Ordering::SeqCst), 0);
    }

    /// The router's own body limit answers before the body is buffered, so an
    /// oversized request is a typed 413 rather than a parse error.
    #[tokio::test]
    async fn an_oversized_body_is_refused_by_the_router_not_the_parser() {
        let (base_url, hits) =
            controllable_upstream(Arc::new(AtomicBool::new(true)), StatusCode::OK).await;
        let state = admitting_state(&base_url, "max_request_bytes = 512", Box::new(NoBudget));
        let response = router(state)
            .oneshot(
                authorized("/v1/chat/completions")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "model": "openai/gpt-4o",
                            "messages": [{"role": "user", "content": "x".repeat(4096)}]
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"]["type"], "request_too_large");
        assert_eq!(hits.load(Ordering::SeqCst), 0);
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

    #[tokio::test]
    async fn store_unavailable_deny_is_budget_unavailable_and_skips_upstream() {
        let (base_url, hits) =
            controllable_upstream(Arc::new(AtomicBool::new(true)), StatusCode::OK).await;
        let budget = crate::budget::StoreBudget::new(
            Arc::new(crate::store::UnavailableStore),
            crate::config::StoreUnavailable::Deny,
        );
        let resp = router(budgeted_state(&base_url, Box::new(budget)))
            .oneshot(chat_request())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body: Value =
            serde_json::from_slice(&resp.into_body().collect().await.unwrap().to_bytes()).unwrap();
        assert_eq!(body["error"]["type"], "budget_unavailable", "{body}");
        assert_eq!(hits.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn store_unavailable_allow_serves_without_a_hold() {
        let (base_url, hits) =
            controllable_upstream(Arc::new(AtomicBool::new(true)), StatusCode::OK).await;
        let budget = crate::budget::StoreBudget::new(
            Arc::new(crate::store::UnavailableStore),
            crate::config::StoreUnavailable::Allow,
        );
        let resp = router(budgeted_state(&base_url, Box::new(budget)))
            .oneshot(chat_request())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_retryable_upstream_error_does_not_fail_over_to_another_provider() {
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

        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(hits_a.load(Ordering::SeqCst), 1);
        assert_eq!(hits_b.load(Ordering::SeqCst), 0);
        let records = captured.0.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].status.as_str(), "upstream_error");
        assert_eq!(records[0].target_provider, "openai");
        assert_eq!(records[0].target_model, "gpt-4o");
        assert_eq!(records[0].credential_id, "cred-a");
        assert_eq!(records[0].attempts, 1);
    }

    /// One provider that answers, whose usage delivery is billing-grade over the
    /// given outbox. Everything else is the ordinary buffered path.
    fn billing_state(base_url: &str, journal: Arc<dyn journal::UsageJournal>) -> AppState {
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
env = "KA"
id = "cred-a"

[[price]]
provider = "openai"
model = "*"
input_microdollars_per_million = 1000000
output_microdollars_per_million = 1000000
"#
        ))
        .unwrap();
        AppState::with_resources(
            cfg,
            &env_with([("KA", "ka")]),
            Arc::new(UsageDelivery::billing(journal, UndurablePolicy::Refuse)),
            Box::new(NoBudget),
            None,
        )
        .unwrap()
    }

    /// The billing-grade promise as a caller sees it: a `200` means the event is
    /// already in the outbox, so a reader of the outbox alone can reconstruct
    /// every request that was reported as served.
    #[tokio::test]
    async fn a_billing_grade_request_is_answered_only_once_its_usage_is_durable() {
        let (url, _) = controllable_upstream(Arc::new(AtomicBool::new(true)), StatusCode::OK).await;
        let outbox = Arc::new(journal::oracle::InMemoryUsageJournal::new());
        let state = billing_state(&url, outbox.clone());

        let response = router(state).oneshot(chat_request()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let claimed = outbox
            .claim(
                &journal::ConsumerId::parse("billing").unwrap(),
                journal::Claim {
                    max_events: 8,
                    lease: Duration::from_secs(30),
                    now: SystemTime::now(),
                },
            )
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1, "the served request is journaled");
        let record = claimed[0].event.record();
        assert_eq!(record.status.as_str(), "ok");
        assert_eq!(record.target_provider, "openai");
        RequestId::parse(&record.request_id).expect("the journaled event carries its identity");
    }

    /// The refusal that makes the promise worth anything: with nowhere durable to
    /// put the event, the request is answered `503 usage_not_durable` rather than
    /// `200` for spend nothing can bill. The upstream call already happened — the
    /// gateway cannot un-spend it — so the refusal is about what it *claims*, and
    /// a `[usage_journal] on_undurable = "serve"` deployment gets the other trade.
    #[tokio::test]
    async fn a_full_outbox_refuses_the_request_rather_than_reporting_unbillable_success() {
        let (url, hits) =
            controllable_upstream(Arc::new(AtomicBool::new(true)), StatusCode::OK).await;
        let outbox = Arc::new(journal::oracle::InMemoryUsageJournal::with_capacity(
            journal::Capacity {
                max_events: 0,
                ..journal::Capacity::BILLING_GRADE
            },
        ));
        let state = billing_state(&url, outbox.clone());

        let response = router(state).oneshot(chat_request()).await.unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["error"]["type"], "usage_not_durable");
        assert_eq!(hits.load(Ordering::SeqCst), 1, "the provider did answer");
        assert!(
            outbox
                .stats(&journal::ConsumerId::parse("billing").unwrap())
                .await
                .unwrap()
                .is_drained(),
            "nothing was journaled, which is what the refusal reports"
        );
    }

    /// A journal that durably commits, then withholds the acknowledgement until
    /// the caller hangs up. This is the ambiguity an immutable usage event must
    /// handle without retrying the same identity under different content.
    struct BlockingAppend {
        inner: journal::oracle::InMemoryUsageJournal,
        entered: Arc<AtomicBool>,
        release: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl journal::UsageJournal for BlockingAppend {
        fn name(&self) -> &'static str {
            self.inner.name()
        }

        fn capacity(&self) -> journal::Capacity {
            self.inner.capacity()
        }

        fn mode(&self) -> journal::DeliveryMode {
            self.inner.mode()
        }

        async fn append(
            &self,
            event: &journal::UsageEvent,
        ) -> Result<journal::Appended, journal::JournalError> {
            let appended = self.inner.append(event).await?;
            self.entered.store(true, Ordering::Release);
            while !self.release.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
            Ok(appended)
        }

        async fn claim(
            &self,
            consumer: &journal::ConsumerId,
            claim: journal::Claim,
        ) -> Result<Vec<journal::Delivery>, journal::JournalError> {
            self.inner.claim(consumer, claim).await
        }

        async fn ack(&self, delivery: &journal::DeliveryId) -> Result<(), journal::JournalError> {
            self.inner.ack(delivery).await
        }

        async fn quarantine(
            &self,
            delivery: &journal::DeliveryId,
            reason: journal::PoisonReason,
        ) -> Result<(), journal::JournalError> {
            self.inner.quarantine(delivery, reason).await
        }

        async fn relinquish(
            &self,
            delivery: &journal::DeliveryId,
        ) -> Result<(), journal::JournalError> {
            self.inner.relinquish(delivery).await
        }

        async fn stats(
            &self,
            consumer: &journal::ConsumerId,
        ) -> Result<journal::JournalStats, journal::JournalError> {
            self.inner.stats(consumer).await
        }
    }

    /// Once provider and middleware produce a terminal outcome, accounting
    /// persists that one immutable fact in a tracked task. Losing the durable
    /// append acknowledgement cannot rewrite it as a contradictory cancellation
    /// or create a second event; `ok` means the response was eligible to return,
    /// not that the peer demonstrably received the HTTP body.
    #[tokio::test]
    async fn lost_ack_after_durable_append_keeps_one_decided_outcome() {
        let (url, _) = controllable_upstream(Arc::new(AtomicBool::new(true)), StatusCode::OK).await;
        let entered = Arc::new(AtomicBool::new(false));
        let release = Arc::new(AtomicBool::new(false));
        let outbox = Arc::new(BlockingAppend {
            inner: journal::oracle::InMemoryUsageJournal::new(),
            entered: Arc::clone(&entered),
            release: Arc::clone(&release),
        });
        let state = billing_state(&url, outbox.clone());

        let settlements = state.0.settlements.clone();
        let request = tokio::spawn(async move { router(state).oneshot(chat_request()).await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while !entered.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("durable append starts after provider success");
        request.abort();
        assert!(
            request
                .await
                .expect_err("request future is cancelled")
                .is_cancelled()
        );
        release.store(true, Ordering::Release);
        settlements.await_idle(Duration::from_secs(2)).await;

        let consumer = journal::ConsumerId::parse("billing").unwrap();
        let claimed = outbox
            .claim(
                &consumer,
                journal::Claim {
                    max_events: 8,
                    lease: Duration::from_secs(30),
                    now: SystemTime::now(),
                },
            )
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1, "the abandoned request reached the outbox");
        assert_eq!(
            claimed[0].event.record().status,
            Status::Ok,
            "the committed outcome is not contradicted after an ambiguous acknowledgement"
        );
    }

    /// A journal that refuses every append, slowly enough for the caller to
    /// hang up inside one.
    struct SlowRefusal(Duration);

    #[async_trait::async_trait]
    impl journal::UsageJournal for SlowRefusal {
        fn name(&self) -> &'static str {
            "slow-refusal"
        }

        fn capacity(&self) -> journal::Capacity {
            journal::Capacity::BILLING_GRADE
        }

        fn mode(&self) -> journal::DeliveryMode {
            journal::DeliveryMode::BillingGrade
        }

        async fn append(
            &self,
            _event: &journal::UsageEvent,
        ) -> Result<journal::Appended, journal::JournalError> {
            tokio::time::sleep(self.0).await;
            Err(journal::JournalError::Backend(
                "the outbox is unreachable".to_owned(),
            ))
        }

        async fn claim(
            &self,
            _consumer: &journal::ConsumerId,
            _claim: journal::Claim,
        ) -> Result<Vec<journal::Delivery>, journal::JournalError> {
            Ok(Vec::new())
        }

        async fn ack(&self, _delivery: &journal::DeliveryId) -> Result<(), journal::JournalError> {
            Ok(())
        }

        async fn quarantine(
            &self,
            _delivery: &journal::DeliveryId,
            _reason: journal::PoisonReason,
        ) -> Result<(), journal::JournalError> {
            Ok(())
        }

        async fn relinquish(
            &self,
            _delivery: &journal::DeliveryId,
        ) -> Result<(), journal::JournalError> {
            Ok(())
        }

        async fn stats(
            &self,
            _consumer: &journal::ConsumerId,
        ) -> Result<journal::JournalStats, journal::JournalError> {
            Ok(journal::JournalStats {
                pending: 0,
                in_flight: 0,
                quarantined: 0,
                oldest_pending_age: None,
                dropped: 0,
                capacity: journal::Capacity::BILLING_GRADE,
            })
        }
    }

    /// A refusal is only a refusal while there is somebody to refuse: the caller
    /// gets `503`, and the event it describes comes back with the retry. Once
    /// the caller has hung up, that answer reaches nobody while the spend stays
    /// settled, so the failed append is a billable fact that exists nowhere —
    /// and the one counter an operator watches for exactly that has to move.
    #[tokio::test]
    async fn an_append_that_fails_after_the_caller_hung_up_is_counted_as_lost() {
        let (url, _) = controllable_upstream(Arc::new(AtomicBool::new(true)), StatusCode::OK).await;
        let state = billing_state(&url, Arc::new(SlowRefusal(Duration::from_millis(200))));
        let usage = Arc::clone(&state.0.usage);

        let mut serving = Box::pin(router(state).oneshot(chat_request()));
        tokio::select! {
            _ = &mut serving => panic!("the request answered before the append could be cut off"),
            () = tokio::time::sleep(Duration::from_millis(50)) => {}
        }
        drop(serving);

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while usage.unheard_refusals() == 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "the refusal nobody heard was never counted as a loss"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// The other side of that rule: a caller still on the connection is told
    /// `503` and can retry, so the event is not lost and must not be counted as
    /// though it were.
    #[tokio::test]
    async fn a_refusal_the_caller_receives_is_not_counted_as_a_loss() {
        let (url, _) = controllable_upstream(Arc::new(AtomicBool::new(true)), StatusCode::OK).await;
        let state = billing_state(&url, Arc::new(SlowRefusal(Duration::ZERO)));
        let usage = Arc::clone(&state.0.usage);

        let response = router(state).oneshot(chat_request()).await.unwrap();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            usage.unheard_refusals(),
            0,
            "a refusal the caller received is a retry, not a loss"
        );
    }

    /// The identity contract as the buffered path delivers it: every settled
    /// record carries a parseable, distinct, time-ordered event id, so a reader
    /// can constrain `request_id` instead of deduplicating on
    /// `(request_id, recorded_at)` and hoping two replicas never collide.
    #[tokio::test]
    async fn buffered_records_carry_distinct_time_ordered_event_identities() {
        let (url_a, _) =
            controllable_upstream(Arc::new(AtomicBool::new(true)), StatusCode::OK).await;
        let (url_b, _) =
            controllable_upstream(Arc::new(AtomicBool::new(true)), StatusCode::OK).await;
        let captured = CapturingSink::default();
        let router = router(two_target_state(&url_a, &url_b, "", captured.clone()));

        for _ in 0..2 {
            let response = router.clone().oneshot(chat_request()).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }

        let records = captured.0.lock().unwrap();
        let ids: Vec<RequestId> = records
            .iter()
            .map(|record| {
                RequestId::parse(&record.request_id).unwrap_or_else(|e| {
                    panic!("`{}` is not an event identity: {e}", record.request_id)
                })
            })
            .collect();
        assert_eq!(ids.len(), 2);
        assert!(ids[0] < ids[1], "{ids:?} must sort in request order");
    }

    /// Affinity a continuation can recover requires the *initial* call to have
    /// used the first target too, so no Responses request fails over — not even
    /// one with no `previous_response_id` to lose.
    #[tokio::test]
    async fn every_responses_request_uses_the_first_target_without_failover() {
        let (url_a, hits_a) = controllable_upstream(
            Arc::new(AtomicBool::new(false)),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
        let (url_b, hits_b) =
            controllable_upstream(Arc::new(AtomicBool::new(true)), StatusCode::OK).await;
        let captured = CapturingSink::default();
        let state = two_target_state(
            &url_a,
            &url_b,
            "[failover]\nmax_attempts = 3\nfailure_threshold = 10",
            captured.clone(),
        );

        for (index, request) in [
            responses_request(Some("resp-from-a")),
            responses_request_with_null_previous_id(),
            responses_request(None),
            streaming_responses_request(None),
        ]
        .into_iter()
        .enumerate()
        {
            let response = router(state.clone()).oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
            let _ = response.into_body().collect().await.unwrap();
            assert_eq!(hits_a.load(Ordering::SeqCst), index + 1);
            assert_eq!(hits_b.load(Ordering::SeqCst), 0);
        }

        // Streaming settlement is detached from the response future. Wait for
        // the fourth Responses record before issuing chat so the record order
        // below cannot race the settlement task.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let count = captured.0.lock().unwrap().len();
            if count >= 4 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "streamed Responses settlement did not arrive before timeout; records={count}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        // Chat over the same provider also does not walk to pb.
        let chat = router(state).oneshot(chat_request()).await.unwrap();
        assert_eq!(chat.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(hits_b.load(Ordering::SeqCst), 0);

        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            let count = captured.0.lock().unwrap().len();
            if count >= 5 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "chat settlement did not arrive before timeout; records={count}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        let records = captured.0.lock().unwrap();
        assert_eq!(records.len(), 5);
        for record in records.iter() {
            assert_eq!(record.status.as_str(), "upstream_error");
            assert_eq!(record.attempts, 1);
            assert_eq!(record.target_provider, "openai");
        }
    }

    /// Initial and continuation requests share the pin but not its error
    /// semantics: only a request carrying a `previous_response_id` has affinity
    /// to lose, so only it reports `continuation_affinity_unavailable`. An
    /// initial request that cannot use the pinned target reports the ordinary
    /// routing error.
    #[tokio::test]
    async fn only_a_continuation_reports_lost_affinity_for_a_skipped_first_target() {
        let (url_a, hits_a) = controllable_upstream(
            Arc::new(AtomicBool::new(false)),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
        let (url_b, hits_b) =
            controllable_upstream(Arc::new(AtomicBool::new(true)), StatusCode::OK).await;
        let captured = CapturingSink::default();
        let state = two_target_state(
            &url_a,
            &url_b,
            "[failover]\nmax_attempts = 3\nfailure_threshold = 1",
            captured.clone(),
        );

        // The initial call is pinned to the failing first target, tripping its
        // breaker without ever reaching the second one.
        let first = router(state.clone())
            .oneshot(responses_request(None))
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(hits_a.load(Ordering::SeqCst), 1);
        assert_eq!(hits_b.load(Ordering::SeqCst), 0);

        let pinned = router(state.clone())
            .oneshot(responses_request(Some("resp-from-a")))
            .await
            .unwrap();
        assert_eq!(pinned.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = pinned.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["error"]["type"], "continuation_affinity_unavailable");
        assert!(
            body["error"]["message"]
                .as_str()
                .unwrap()
                .contains("continuation affinity")
        );

        let initial = router(state)
            .oneshot(responses_request(None))
            .await
            .unwrap();
        assert_eq!(initial.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = initial.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["error"]["type"], "all_provider_circuits_open");

        // Neither request that skipped the pinned target reached an upstream.
        assert_eq!(hits_a.load(Ordering::SeqCst), 1);
        assert_eq!(hits_b.load(Ordering::SeqCst), 0);
        let records = captured.0.lock().unwrap();
        assert_eq!(records.len(), 1);
    }

    /// A response created under a rotated key is one no continuation can
    /// recover, so the pooled credential is pinned for initial calls too.
    #[tokio::test]
    async fn every_responses_request_reuses_the_first_pooled_credential() {
        let (base_url, seen) = credential_probe_upstream(false).await;
        let captured = CapturingSink::default();
        let state = two_credential_responses_state(&base_url, captured);

        for _ in 0..2 {
            assert_eq!(
                router(state.clone())
                    .oneshot(responses_request(Some("resp-from-a")))
                    .await
                    .unwrap()
                    .status(),
                StatusCode::OK
            );
        }
        for _ in 0..2 {
            assert_eq!(
                router(state.clone())
                    .oneshot(responses_request(None))
                    .await
                    .unwrap()
                    .status(),
                StatusCode::OK
            );
        }
        let streamed = router(state)
            .oneshot(streaming_responses_request(None))
            .await
            .unwrap();
        let _ = streamed.into_body().collect().await.unwrap();

        assert_eq!(*seen.lock().unwrap(), ["Bearer sk-a"; 5]);
    }

    #[tokio::test]
    async fn a_pinned_responses_rate_limit_does_not_rotate_credentials() {
        let (base_url, seen) = credential_probe_upstream(true).await;
        let captured = CapturingSink::default();
        let state = two_credential_responses_state(&base_url, captured);

        let response = router(state.clone())
            .oneshot(responses_request(Some("resp-from-a")))
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(*seen.lock().unwrap(), ["Bearer sk-a"]);

        // The initial call is pinned to the same exhausted key: rotation stays
        // off, and it reports the ordinary upstream error.
        let initial = router(state)
            .oneshot(responses_request(None))
            .await
            .unwrap();
        assert_eq!(initial.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(*seen.lock().unwrap(), ["Bearer sk-a", "Bearer sk-a"]);
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
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert_eq!(hits_a.load(Ordering::SeqCst), 1);
        assert_eq!(hits_b.load(Ordering::SeqCst), 0);
        let records = captured.0.lock().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].status.as_str(), "upstream_error");
        assert_eq!(records[0].target_provider, "openai");
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

[[price]]
provider = "p"
model = "upstream-model"
input_microdollars_per_million = 1000000
output_microdollars_per_million = 1000000
"#
        ))
        .unwrap();
        let env = env_with([("K1", "sk-test")]);
        let sinks: Vec<Box<dyn UsageSink>> = vec![Box::new(captured)];
        AppState::new(cfg, &env, UsageFanout::new(sinks), Box::new(NoBudget)).unwrap()
    }

    #[tokio::test]
    async fn provider_refusals_keep_their_class_and_export_bounded_attempt_diagnostics() {
        use tracing::instrument::WithSubscriber as _;
        crate::telemetry::testing::keep_callsites_answerable();
        for (kind, path) in [
            ("anthropic", "/messages"),
            ("openai", "/chat/completions"),
            ("openai", "/responses"),
        ] {
            for stream in [false, true] {
                for (upstream_status, message, status, code) in [
                    (
                        400,
                        "input_schema does not support oneOf",
                        400,
                        "invalid_request",
                    ),
                    (422, "invalid field", 400, "invalid_request"),
                    (401, "invalid provider credentials", 502, "invalid_request"),
                    (403, "provider access denied", 502, "invalid_request"),
                    (
                        400,
                        "context window exceeded",
                        400,
                        "context_window_exceeded",
                    ),
                    (404, "requested model not found", 502, "model_unavailable"),
                    (429, "quota exhausted", 502, "provider_dependency_failed"),
                    (
                        503,
                        "provider overloaded",
                        502,
                        "provider_dependency_failed",
                    ),
                ] {
                    let diagnostic =
                        format!("{message}; rejected key sk-test; {}", "界".repeat(1800));
                    let answer = (
                        StatusCode::from_u16(upstream_status).unwrap(),
                        Json(json!({"error": {"message": diagnostic}})),
                    )
                        .into_response();
                    let (base_url, received) = native_upstream(path, answer).await;
                    let state = native_state(kind, &base_url, CapturingSink::default());
                    let exporter = InMemorySpanExporter::default();
                    let provider = SdkTracerProvider::builder()
                        .with_simple_exporter(exporter.clone())
                        .build();
                    let subscriber = tracing_subscriber::registry().with(
                        tracing_opentelemetry::layer()
                            .with_tracer(provider.tracer("upstream-errors")),
                    );
                    let request = authorized(&format!("/v1{path}"))
                        .header("anthropic-version", "2023-06-01")
                        .body(Body::from(
                            serde_json::to_vec(&json!({
                                "model": "p/upstream-model", "stream": stream, "max_tokens": 16,
                                "messages": [{"role": "user", "content": "hello"}], "input": "hello"
                            }))
                            .unwrap(),
                        ))
                        .unwrap();
                    let response = router(state)
                        .oneshot(request)
                        .with_subscriber(subscriber)
                        .await
                        .unwrap();
                    assert_eq!(
                        response.status().as_u16(),
                        status,
                        "{path} stream={stream} upstream={upstream_status}"
                    );
                    let body = response.into_body().collect().await.unwrap().to_bytes();
                    let body: Value = serde_json::from_slice(&body).unwrap();
                    assert_eq!(body["error"]["type"], code);
                    assert!(!body.to_string().contains("sk-test"));
                    assert_eq!(received.lock().unwrap().len(), 1);
                    provider.force_flush().unwrap();
                    let spans = exporter.get_finished_spans().unwrap();
                    let attempts: Vec<_> = spans
                        .iter()
                        .filter(|span| span.name == "axond.upstream.attempt")
                        .collect();
                    assert_eq!(attempts.len(), 1, "{path} stream={stream}");
                    let attempt = attempts[0];
                    let attribute = |key: &str| {
                        attempt
                            .attributes
                            .iter()
                            .find(|item| item.key.as_str() == key)
                            .map(|item| item.value.to_string())
                            .expect(key)
                    };
                    assert_eq!(
                        attribute("axond.upstream.status"),
                        upstream_status.to_string()
                    );
                    assert_eq!(attribute("axond.status"), "error");
                    assert!(matches!(
                        attempt.status,
                        opentelemetry::trace::Status::Error { .. }
                    ));
                    let recorded = attribute("axond.upstream.message");
                    assert!(recorded.starts_with(message));
                    assert!(recorded.contains("[REDACTED]"));
                    assert!(recorded.len() <= gateway_core::MAX_DIAGNOSTIC_BYTES);
                    assert!(!format!("{spans:?}").contains("sk-test"));
                }
            }
        }
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
            "model": "p/upstream-model",
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
        assert_eq!(records[0].cost_microdollars, Some(18));
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

        let sent = json!({ "model": "p/upstream-model", "input": ["one", "two"], "dimensions": 2 });
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
        assert_eq!(records[0].cost_microdollars, Some(8));
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

        // Request 1: openai fails and trips its circuit. There is no alias
        // failover onto pb.
        let resp = router(state.clone()).oneshot(chat_request()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(hits_a.load(Ordering::SeqCst), 1);
        assert_eq!(hits_b.load(Ordering::SeqCst), 0);

        // Request 2: the circuit is open, so the observed (provider, model) is
        // skipped and nothing is dispatched.
        let resp = router(state.clone()).oneshot(chat_request()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(hits_a.load(Ordering::SeqCst), 1);
        assert_eq!(hits_b.load(Ordering::SeqCst), 0);

        // After cooldown a half-open probe reaches the recovered provider.
        healthy_a.store(true, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(1_100)).await;

        let resp = router(state).oneshot(chat_request()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(hits_a.load(Ordering::SeqCst), 2);
        let records = captured.0.lock().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[1].target_provider, "openai");
        assert_eq!(records[1].attempts, 1);
    }

    // ----------------------------------------------------------------
    // `/admin/v1/status`: authorization, redaction, and revision visibility.
    //
    // The route reports on dependencies, which makes it the one surface where a
    // leak is a leak of the operator's infrastructure rather than of a request.
    // Four properties are asserted, all fail-closed: an unauthenticated caller
    // learns nothing, a token without the capability learns nothing, a tenant
    // sees its own request path with coarsened reasons, and no scope sees a
    // secret, a DSN, a raw backend error, or a revision id.

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
        #[derive(Clone, Default)]
        struct Settles(Arc<AtomicUsize>);

        #[async_trait::async_trait]
        impl crate::budget::BudgetStore for Settles {
            fn name(&self) -> &'static str {
                "settles"
            }
            async fn reserve(&self, _key: &BudgetKey, _estimated: u64) -> Admission {
                Admission::Allowed(Reservation::unheld())
            }
            async fn settle(&self, _key: &BudgetKey, _reservation: &Reservation, actual: u64) {
                assert_eq!(actual, 0, "a cancelled request consumed nothing");
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

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

        let budget = Settles::default();
        let state = budgeted_state(&format!("http://{addr}"), Box::new(budget.clone()));
        let request = tokio::spawn(router(state).oneshot(chat_request()));
        started_rx.await.unwrap();
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());

        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        while budget.0.load(Ordering::SeqCst) == 0 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "reservation was not released before timeout"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(budget.0.load(Ordering::SeqCst), 1, "released exactly once");
    }

    /// The shutdown deadline has to reach a request that has not produced a
    /// response yet. Such a request is waiting on an upstream inside its
    /// handler, bounded only by the failover budget — minutes, potentially — so
    /// if the abandonment signal stopped at the response body, the request would
    /// keep its admission slot for the whole termination and the settle wait
    /// would spend the budget the buffered records need.
    #[tokio::test]
    async fn abandonment_cancels_a_request_still_inside_its_handler() {
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
                    // Never answers: only the cancellation can end this request.
                    pending::<()>().await;
                    StatusCode::OK.into_response()
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, upstream).await.unwrap() });

        let state = budgeted_state(&format!("http://{addr}"), Box::new(NoBudget));
        let lifecycle = Arc::clone(state.lifecycle());
        let request = tokio::spawn(router(state).oneshot(chat_request()));
        // The handler is now inside the upstream call, holding its slot.
        started_rx.await.unwrap();
        assert_eq!(lifecycle.in_flight(), 1);

        lifecycle.abandon();

        let response = request.await.unwrap().unwrap();
        // Refused with the drain's own answer rather than left hanging: the
        // caller learns the replica is going away and can retry elsewhere.
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        // And the slot is back, so the flush budget is not spent waiting for it.
        assert_eq!(lifecycle.in_flight(), 0);
    }

    #[tokio::test]
    async fn admin_v1_is_unmounted() {
        let app = router(test_state());
        for path in ["/admin/v1/status", "/admin/v1/tenants"] {
            let response = app
                .clone()
                .oneshot(
                    Request::get(path)
                        .header(
                            axum::http::header::AUTHORIZATION,
                            format!("Bearer {CALLER_SECRET}"),
                        )
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::NOT_FOUND,
                "{path} must not be served (ADR 0063)"
            );
        }
    }

    #[tokio::test]
    async fn malformed_responses_controls_never_reach_the_provider() {
        const SECRET: &str = "malformed-control@example.com";
        let (url_a, seen_a) = redaction_responses_upstream(true).await;
        let (url_b, seen_b) = redaction_responses_upstream(true).await;
        let (state, usage) = responses_stream_state_with_sink(&url_a, &url_b);

        for body in [
            json!({"model": "openai/gpt-4o", "input": "ordinary", "stream": SECRET}),
            json!({
                "model": "openai/gpt-4o",
                "input": "ordinary",
                "stream": true,
                "previous_response_id": {"value": SECRET}
            }),
        ] {
            let response = router(state.clone())
                .oneshot(
                    authorized("/v1/responses")
                        .body(Body::from(serde_json::to_vec(&body).unwrap()))
                        .unwrap(),
                )
                .await
                .expect("malformed routing control response");
            assert_eq!(response.status(), StatusCode::BAD_REQUEST);
            let response = response.into_body().collect().await.unwrap().to_bytes();
            assert!(!String::from_utf8_lossy(&response).contains(SECRET));
        }

        assert!(seen_a.lock().unwrap().is_empty());
        assert!(seen_b.lock().unwrap().is_empty());
        assert!(usage.0.lock().unwrap().is_empty());
    }
}
