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

use std::collections::HashSet;
use std::sync::Arc;
use std::task::Poll;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, Extension, RawQuery, Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::middleware::{Next, from_fn_with_state};
use axum::response::{IntoResponse, Response};
use axum::routing::{MethodRouter, get, post};
use axum::{Json, Router};
use futures::StreamExt;
use gateway_core::{
    CircuitDecision, FailoverDecision, FailoverPolicy, FailoverTarget, ModelPrice, ModelUsage,
    NativeMessagesDecoder, ProviderError, ProviderRequest, ProviderResponse, ProviderStreamDecoder,
    Surface, Usage,
};
use gateway_transport::{
    AuthScheme, Deadline, NativeCall, TimeoutBound, TimeoutKind, TransportError, Upstream,
};
use secrecy::ExposeSecret;
use serde::Deserialize;
use serde_json::{Value, json};
use tracing::{Instrument, debug, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::admission::{AdmissionPermit, DiagnosticCredential, RequestKind};
use crate::aliases::AliasScope;
use crate::budget::{Admission, BudgetKey, Denial, Reservation};
use crate::config::{Model, Provider, ProviderKind, ProviderWire, Target};
use crate::credentials::{CredentialLease, CredentialPlan, CredentialSource, CredentialStatusView};
use crate::error::GatewayError;
use crate::mint::{MintRequest, mint_issued_at, mint_token_at};
use crate::principals::{Capability, Presented, PrincipalStoreError, TokenVerificationError};
use crate::rate_limit::{RateLimitKey, RateLimitPermit};
use crate::shutdown::Phase;
use crate::state::{AppState, ConfigSnapshot, InboundKey, adapter_for};
use crate::status::{StatusResponse, StatusScope};
use crate::streaming::{self, Framing, StreamContext};
use crate::telemetry;
use crate::usage::identity::EventIdentity;
use crate::usage::{Status, UsageRecord};

pub fn router(state: AppState) -> Router {
    let minting_enabled = state.config().gateway_minting.is_some();
    mount(route_specs(minting_enabled), state)
}

/// The replica diagnostics alone, for a process that serves no inference.
///
/// An unconverged replica is exactly the one an operator most needs to ask about
/// — it is refusing inference and its convergence is the reason — so the
/// diagnostic is mounted beside [`unconverged_router`] rather than being lost
/// with the inference surface it happens to be declared next to.
pub fn diagnostic_router(state: AppState) -> Router {
    let specs = route_specs(state.config().gateway_minting.is_some())
        .into_iter()
        .filter(|spec| spec.auth == AuthPosture::Diagnostic)
        .collect();
    mount(specs, state)
}

fn mount(specs: Vec<RouteSpec>, state: AppState) -> Router {
    // The inbound body bound is declared rather than inherited: axum's own
    // default would otherwise be the process's real memory ceiling per request.
    let max_request_bytes = state.0.admission.limits().max_request_bytes;
    specs
        .into_iter()
        .fold(Router::new(), |router, spec| {
            let route = (spec.router)().layer(DefaultBodyLimit::max(max_request_bytes));
            // A diagnostic takes no served-traffic slot, but it is not free
            // either: its own small ceiling bounds how many status reads run at
            // once, without letting served traffic at *its* ceiling make the
            // replica unanswerable. Applied before authentication and therefore
            // *inside* it, so a slot is spent only once a caller has proved it
            // may ask and an anonymous flood cannot hold the ceiling closed
            // against the operators it is for.
            let route = if spec.auth.takes_a_diagnostic_slot() {
                route.layer(from_fn_with_state(state.clone(), diagnostic_middleware))
            } else {
                route
            };
            let route = if spec.auth.requires_a_credential() {
                route.layer(from_fn_with_state(
                    (state.clone(), spec.capability),
                    authenticate_middleware,
                ))
            } else {
                route
            };
            // And a second, wider ceiling outside authentication, because the
            // one above cannot bound the work of authenticating. Only the
            // diagnostic needs it: every other authenticated route has
            // admission out here doing the same job.
            let route = if spec.auth.takes_a_diagnostic_slot() {
                route.layer(from_fn_with_state(
                    state.clone(),
                    diagnostic_authentication_middleware,
                ))
            } else {
                route
            };
            // Admission is the outermost layer, so a request arriving after the
            // drain window is refused before it touches authentication, budgets,
            // or an upstream. The probes and the status diagnostic deliberately
            // stay outside it: a draining replica is still alive, killing its
            // probes early would cut the very requests the drain exists to
            // finish, and a diagnostic refused by admission could neither be read
            // during the shutdown nor report that it is happening.
            let route = if spec.auth.takes_an_admission_slot() {
                route.layer(from_fn_with_state(state.clone(), admission_middleware))
            } else {
                route
            };
            router.route(spec.path, route)
        })
        .with_state(state)
}

/// The inference surface of a replica whose configuration is not a runtime
/// snapshot yet: the probes, and one refusal for everything else.
///
/// A stateful replica serves `/admin/v1` long before it can compile a published
/// revision into a snapshot, and the two must not be confused. Liveness stays
/// `200` — the process is healthy and is administrable — while readiness stays
/// `503` and every inference path answers `reason` in the gateway's own error
/// envelope. Nothing here holds an [`AppState`]: there is no configuration to
/// hold, which is the whole point. The replica diagnostic does hold one, so it
/// is mounted alongside by [`diagnostic_router`] rather than from here.
pub fn unconverged_router(reason: &'static str) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route(
            "/readyz",
            get(|| async { (StatusCode::SERVICE_UNAVAILABLE, "unconverged") }),
        )
        .fallback(move || async move {
            let body = json!({
                "error": {
                    "type": "inference_unavailable",
                    "message": reason,
                }
            });
            (StatusCode::SERVICE_UNAVAILABLE, Json(body))
        })
}

/// Whether a route is one of the two unauthenticated liveness probes, or must
/// pass inbound authentication before its handler can run — and if so, whether
/// it is served work or asked about the replica serving it.
#[derive(Clone, Copy, PartialEq, Eq)]
enum AuthPosture {
    LivenessProbe,
    Authenticated,
    Diagnostic,
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

    /// Whether the route is bounded by the separate diagnostic ceiling.
    fn takes_a_diagnostic_slot(self) -> bool {
        matches!(self, Self::Diagnostic)
    }
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
fn route_specs(minting_enabled: bool) -> Vec<RouteSpec> {
    let mut routes = vec![
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
            path: "/admin/v1/status",
            auth: AuthPosture::Diagnostic,
            capability: Some(Capability::Status),
            // The one route on this router registered under the administrative
            // prefix, and it takes that prefix's method contract with it: it
            // shadows the nested surface's own `method_not_allowed_fallback`,
            // so without this a `POST` here would be the single `/admin/v1`
            // path answering axum's empty-bodied 405 instead of a declared
            // code (`crate::admin::router::mount`).
            router: || {
                get(replica_status)
                    .fallback(|| async { crate::admin::AdminError::MethodNotAllowed })
            },
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
            capability: Some(Capability::Responses),
            router: || post(responses),
        },
    ];
    if minting_enabled {
        routes.push(RouteSpec {
            path: "/v1/tokens",
            auth: AuthPosture::Authenticated,
            capability: None,
            router: || post(mint_tokens),
        });
    }
    routes
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MintTokenRequest {
    sub: String,
    ttl_seconds: Option<u64>,
    scope: Option<Vec<String>>,
    aliases: Option<Vec<String>>,
    max_request_microdollars: Option<u64>,
}

const MAX_MINT_SUBJECT_LENGTH: usize = 128;

async fn mint_tokens(
    Extension(snapshot): Extension<Arc<ConfigSnapshot>>,
    Extension(caller): Extension<InboundKey>,
    body: Result<Json<MintTokenRequest>, axum::extract::rejection::JsonRejection>,
) -> Result<(HeaderMap, Json<Value>), GatewayError> {
    let minting = snapshot
        .gateway_minting
        .as_ref()
        .ok_or(GatewayError::MintingDisabled)?;
    if !caller.can_mint {
        return Err(GatewayError::MintNotAuthorized);
    }
    let Json(request) = body.map_err(|error| GatewayError::BadRequest(error.to_string()))?;
    if request.sub.trim().is_empty() {
        return Err(GatewayError::BadRequest("`sub` must not be empty".into()));
    }
    let subject = request.sub.trim().to_owned();
    if subject.chars().count() > MAX_MINT_SUBJECT_LENGTH
        || !subject.chars().all(|character| {
            character.is_ascii_alphanumeric() || character == '_' || matches!(character, '.' | '-')
        })
    {
        return Err(GatewayError::BadRequest(format!(
            "`sub` must be at most {MAX_MINT_SUBJECT_LENGTH} ASCII letters, digits, or `_.-`"
        )));
    }
    let epoch = snapshot.gateway_token_epoch(&caller.namespace, &subject);
    let iat = epoch
        .map(|min_iat| {
            mint_issued_at(Some(min_iat)).map_err(|_| GatewayError::MintEpochNotUsable {
                kid: minting.kid.clone(),
                min_iat,
            })
        })
        .transpose()?;
    let ttl = Duration::from_secs(request.ttl_seconds.unwrap_or(minting.max_ttl.as_secs()));
    if ttl.is_zero() || ttl > minting.max_ttl {
        return Err(GatewayError::MintClaimsNotNarrowing);
    }
    let scope = match request.scope {
        Some(values) => {
            let parsed = values
                .iter()
                .map(|value| {
                    Capability::parse(value).ok_or_else(|| {
                        GatewayError::BadRequest(format!("unknown scope capability `{value}`"))
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let requested = parsed.iter().copied().collect::<HashSet<_>>();
            if let Some(ceiling) = &minting.scope
                && !requested.is_subset(ceiling)
            {
                return Err(GatewayError::MintClaimsNotNarrowing);
            }
            Some(parsed)
        }
        None => Some(
            minting
                .scope
                .as_ref()
                .map(|values| values.iter().copied().collect())
                .unwrap_or_else(|| {
                    Capability::ALL
                        .iter()
                        .copied()
                        .filter(|capability| capability.is_granted_without_scope())
                        .collect()
                }),
        ),
    };
    if scope.as_ref().is_some_and(|capabilities| {
        capabilities
            .iter()
            .any(|capability| !caller_can_mint_capability(&caller, &snapshot, *capability))
    }) {
        return Err(GatewayError::MintClaimsNotNarrowing);
    }
    let aliases = match request.aliases {
        Some(values) => {
            let requested = AliasScope::parse(values.iter().map(String::as_str))
                .map_err(|error| GatewayError::BadRequest(error.to_string()))?;
            if let Some(ceiling) = &minting.aliases
                && !requested.is_subset_of(ceiling)
            {
                return Err(GatewayError::MintClaimsNotNarrowing);
            }
            Some(values)
        }
        None => minting
            .aliases
            .as_ref()
            .map(|aliases| aliases.patterns_for_claim()),
    };
    let max_request_microdollars = match request.max_request_microdollars {
        Some(value) if value >= 1 => {
            if minting
                .max_request_microdollars
                .is_some_and(|ceiling| value > ceiling)
            {
                return Err(GatewayError::MintClaimsNotNarrowing);
            }
            Some(value)
        }
        Some(_) => return Err(GatewayError::MintClaimsNotNarrowing),
        None => minting.max_request_microdollars,
    };
    if !snapshot
        .config
        .gateway_verifier
        .iter()
        .find(|verifier| verifier.kid == minting.kid)
        .is_some_and(|verifier| verifier.namespaces.iter().any(|ns| ns == &caller.namespace))
    {
        return Err(GatewayError::MintClaimsNotNarrowing);
    }
    let minted = mint_token_at(
        MintRequest {
            kid: &minting.kid,
            algorithm: minting.algorithm,
            key_material: minting.key_material.expose_secret(),
            namespace: &caller.namespace,
            subject: &subject,
            audience: &minting.audience,
            ttl,
            aliases,
            max_request_microdollars,
            scope,
        },
        iat,
    )
    .map_err(|error| GatewayError::BadRequest(error.to_string()))?;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs());
    let expires_in = minted.exp.saturating_sub(now);
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::CACHE_CONTROL,
        HeaderValue::from_static("no-store"),
    );
    Ok((
        headers,
        Json(json!({
            "token": minted.token,
            "exp": minted.exp,
            "expires_in": expires_in,
            "namespace": caller.namespace,
            "sub": subject,
        })),
    ))
}

fn caller_can_mint_capability(
    caller: &InboundKey,
    snapshot: &ConfigSnapshot,
    capability: Capability,
) -> bool {
    caller
        .scope
        .as_ref()
        .map_or(!capability.is_operator_only(), |scope| {
            scope.contains(&capability) && namespace_allows(snapshot, &caller.namespace, capability)
        })
}

/// Bound concurrent diagnostic reads.
///
/// A diagnostic answers from memory, so the ceiling is small, fixed, and held
/// only for the handler: there is no body to stream and nothing to settle. It
/// is deliberately *not* the served-traffic gate. Sharing `max_in_flight` would
/// let a saturated replica refuse the question "why are you saturated", and
/// taking a per-subject rate-limit permit would put the rate-limit store on the
/// path of a read whose whole purpose is to be answerable while that store is
/// down — the outage the fail-closed limiter turns into a denial.
async fn diagnostic_middleware(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response, GatewayError> {
    let _permit = state.0.admission.admit_diagnostic()?;
    Ok(next.run(request).await)
}

/// Bound the work of *authenticating* a diagnostic read.
///
/// The ceiling above is inside authentication, so it bounds the answer rather
/// than the signature verification and revocation lookup that precede it. This
/// one is outside, and is wide enough that only a flood reaches it: the two
/// together mean neither an anonymous flood can close the route to operators
/// nor a credentialled one can spend the replica's CPU and revocation store
/// without limit.
///
/// Which partition of it a request may take is decided here, from the shape of
/// the credential alone — the only thing known before the credential is spent.
/// A token's verification can block on the revocation store, so tokens are held
/// to their own share and cannot fill the share that resolves in memory: the
/// operator's static key is the credential the runbook sends through a
/// revocation outage, and a store that is slow rather than down must not be
/// able to refuse it.
async fn diagnostic_authentication_middleware(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response, GatewayError> {
    let credential =
        presented_credential(request.headers()).map_or(DiagnosticCredential::Local, |credential| {
            state
                .config()
                .diagnostic_credential(&Presented { credential })
        });
    let _permit = state
        .0
        .admission
        .admit_diagnostic_authentication(credential)?;
    Ok(next.run(request).await)
}

/// Reserve a slot for a request and hold it until the response body is fully
/// delivered, so an open SSE stream counts as in-flight for as long as it runs.
async fn admission_middleware(
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

async fn healthz() -> &'static str {
    "ok"
}

/// Readiness is where a rolling deployment learns this replica is leaving: it
/// fails as soon as the drain begins, before admission closes, so a load
/// balancer can stop routing while the replica is still able to serve. Real
/// dependency readiness (config loaded, credentials present) is a follow-up.
async fn readyz(State(state): State<AppState>) -> (StatusCode, &'static str) {
    match state.lifecycle().phase() {
        Phase::Serving => (StatusCode::OK, "ready"),
        Phase::Draining | Phase::Closing => (StatusCode::SERVICE_UNAVAILABLE, "draining"),
    }
}

/// This replica's own dependency status, projected into what the caller is
/// entitled to see (ADR 0031).
///
/// Three properties are structural rather than checked here. The read is a
/// *cache* read — [`crate::status::registry::CachedStatusRegistry::view`] is
/// synchronous, so this handler cannot probe a backend however it is edited. The
/// scope comes from the caller's authority rather than from a query parameter, so
/// a tenant cannot ask for the operator's view. And the response type has no
/// free-text field, so the probe detail behind a coarse reason code cannot be
/// projected into it.
///
/// Unlike the two probes this reports a dependency, and unlike them it is
/// authenticated: an orchestrator polling `/readyz` must never learn that the
/// budget store is down, because removing healthy replicas from service is how a
/// dependency outage becomes a fleet outage.
async fn replica_status(
    State(state): State<AppState>,
    Extension(snapshot): Extension<Arc<ConfigSnapshot>>,
    Extension(caller): Extension<InboundKey>,
) -> Json<StatusResponse> {
    let scope = StatusScope::for_operator_authority(caller_holds_direct_operator_authority(
        &caller, &snapshot,
    ));
    let view = state.status().view();
    let revision = state.revision_report();
    Json(view.project(scope, state.lifecycle().phase(), revision.as_ref()))
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
fn caller_holds_direct_operator_authority(caller: &InboundKey, snapshot: &ConfigSnapshot) -> bool {
    caller.holds_direct_operator_authority(snapshot.config.default_namespace())
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

/// The credential a request presents, before anything is known about whether it
/// is one.
///
/// It travels as `Authorization: Bearer` or, because that is what an Anthropic
/// SDK pointed at the gateway sends, as `x-api-key`. Both name the same gateway
/// key; the scheme is the client's, not a second credential space. Shared with
/// the diagnostic pre-authentication ceiling, which has to partition on the same
/// string authentication will later resolve.
fn presented_credential(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .or_else(|| headers.get("x-api-key").and_then(|v| v.to_str().ok()))
}

/// Resolve the caller's namespace + subject from the inbound key. Every request
/// must present a configured gateway key: authentication fails closed, and a
/// snapshot with no key never reaches a request (ADR 0013).
async fn authenticate(
    snapshot: &ConfigSnapshot,
    headers: &HeaderMap,
) -> Result<InboundKey, GatewayError> {
    let credential = presented_credential(headers).ok_or(GatewayError::Unauthorized)?;
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
/// before any typed handler error.
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
        Capability::Responses => Some(Route::Responses),
        Capability::Models => None,
        Capability::Credentials | Capability::CredentialsAll => None,
        // Status reports on the replica's own dependencies, so it is not
        // gated on a namespace having a servable model.
        Capability::Status => None,
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
    /// OpenAI Responses, forwarded verbatim to an OpenAI-family target.
    Responses,
}

impl Route {
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
        self != Self::Embeddings
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

/// The inbound body, or a typed refusal. An oversized body is a bound the
/// gateway imposed (`413`), a wrong media type is `415` as axum's extractor
/// already answered it, and a malformed one is the caller's (`400`); no
/// response echoes the body it read.
fn inbound_body(body: Result<Json<Value>, JsonRejection>) -> Result<Value, GatewayError> {
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

async fn chat_completions(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(snapshot): Extension<Arc<ConfigSnapshot>>,
    Extension(caller): Extension<InboundKey>,
    body: Result<Json<Value>, JsonRejection>,
) -> Result<Response, GatewayError> {
    serve(
        state,
        headers,
        inbound_body(body)?,
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
    body: Result<Json<Value>, JsonRejection>,
) -> Result<Response, GatewayError> {
    serve(
        state,
        headers,
        inbound_body(body)?,
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
    body: Result<Json<Value>, JsonRejection>,
) -> Result<Response, GatewayError> {
    serve(
        state,
        headers,
        inbound_body(body)?,
        Route::Embeddings,
        snapshot,
        caller,
    )
    .await
}

async fn responses(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(snapshot): Extension<Arc<ConfigSnapshot>>,
    Extension(caller): Extension<InboundKey>,
    body: Result<Json<Value>, JsonRejection>,
) -> Result<Response, GatewayError> {
    serve(
        state,
        headers,
        inbound_body(body)?,
        Route::Responses,
        snapshot,
        caller,
    )
    .await
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

    // The per-request bounds are checked before any dependency work and before
    // admission: they are pure functions of the parsed body, and a request that
    // cannot legally be served should not occupy capacity while it is refused.
    // Neither error repeats any part of the body.
    let estimate = route.estimate(&body);
    let limits = state.0.admission.limits();
    if let Some(limit_tokens) = limits.max_prompt_tokens
        && estimate.input_tokens > limit_tokens
    {
        return Err(GatewayError::PromptTooLarge { limit_tokens });
    }
    if let Some(limit_tokens) = limits.max_output_tokens
        && let Some(requested_tokens) = requested_output_tokens(&body)
        && requested_tokens > limit_tokens
    {
        return Err(GatewayError::OutputLimitExceeded {
            requested_tokens,
            limit_tokens,
        });
    }

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

    // The request is now admitted and will produce exactly one usage event, so
    // its identity is minted here — once, while the server span is still current
    // — and carried to whichever path settles it. A request refused above this
    // line produces no event and therefore needs no identity.
    let identity = EventIdentity::capture();

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
                identity,
                hold: BudgetHold {
                    key: budget_key,
                    reservation,
                    estimated_input_tokens: estimate.input_tokens,
                    permit: Some(rate_limit_permit),
                    admission: Some(admission_permit),
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
                    identity: &identity,
                    caller: &caller,
                    alias: &alias,
                    target_provider: &served.provider,
                    target_model: &served.model,
                    source: served.source,
                    credential_id: &served.credential_id,
                    status: Status::Ok,
                    input_tokens: response.usage.input_tokens,
                    cache_read_tokens: response.usage.cache_read_tokens,
                    cache_write_tokens: response.usage.cache_write_tokens,
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
            note_attempt_timeout(&attempt_span, target, err);
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
        identity,
        mut hold,
    } = request;
    let reservation_guard =
        BudgetReservation::new(state.clone(), hold.key.clone(), hold.reservation.clone());
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
                subject: caller.subject.clone(),
                signer_kid: caller.signer_kid.clone(),
                alias: alias.clone(),
                target_provider: target.provider.clone(),
                target_model: target.model.clone(),
                source: plan.source,
                credential_id: lease.id.clone(),
                identity: identity.clone(),
                price: target.price,
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
                    reservation_guard.disarm();
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
                                    // The rotation serves the same request, so it
                                    // carries the same event identity rather than
                                    // re-reading a span it no longer runs under.
                                    identity,
                                    price: target.price,
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
                    note_attempt_timeout(span, target, &err);
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
            reservation_guard.disarm();
            ctx.attempts = walk.attempts;
            ctx.rate_limit_permit = hold.permit.take();
            ctx.admission_permit = hold.admission.take();
            streaming::settle_upstream_error(state.clone(), ctx, started);
        } else {
            reservation_guard.release().await;
        }
        return Err(err.into());
    }
    reservation_guard.release().await;
    Err(walk.into_error())
}

/// One streamed request as the failover walk sees it: the alias it resolved,
/// the body to forward, the wire it speaks, and the budget hold it was admitted
/// under.
struct StreamRequest<'a> {
    alias: String,
    body: Value,
    wire: &'a Wire,
    /// The identity of the usage event this request will settle as, minted at
    /// admission and cloned into every stream context the walk builds — including
    /// a credential rotation's — so a stream that rotates, ends, is cancelled, or
    /// never opens all report the same event.
    identity: EventIdentity,
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
    /// The admission capacity the request was let in under. Moved into the
    /// stream context that ends up owning the relay, so an open stream keeps
    /// occupying a slot for exactly as long as it is open — and a walk that
    /// never opens one drops it here.
    admission: Option<AdmissionPermit>,
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

    fn disarm(mut self) {
        self.reservation.take();
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

/// Attribute a failed attempt's timeout class to its span and the timeout
/// counter. Only the bound is recorded — never the upstream URL, which the
/// transport has already kept out of the error.
fn note_attempt_timeout(span: &tracing::Span, target: &Target, err: &TransportError) {
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
/// without opening the target's breaker. A walk budget spent before this target
/// was ever dispatched to belongs in the same category; a target that was given
/// time and stalled does not, however short that time was.
fn record_target_failure(
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
fn as_provider_error(err: &TransportError) -> ProviderError {
    match err {
        TransportError::Provider(pe) => pe.clone(),
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
struct PooledAttempt {
    result: Result<ProviderResponse, TransportError>,
    credential_id: String,
}

/// Walk the credential pool: dispatch with the first credential, and on a
/// credential-scoped failure (rate limit / quota) park that credential and
/// retry the *same* target with the next one. Target-level failover is a
/// separate concern and is not attempted here.
#[allow(clippy::too_many_arguments)]
async fn dispatch_over_pool(
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
                            deadline,
                        )
                        .await
                }
                route => state
                    .0
                    .dispatcher
                    .send(
                        &upstream,
                        &wire.call(body.clone(), adapter.name()),
                        deadline,
                    )
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
fn was_never_dispatched(err: &TransportError) -> bool {
    err.timeout_kind() == Some(TimeoutKind::Overall)
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
    let output_tokens = requested_output_tokens(body).unwrap_or(DEFAULT_MAX_OUTPUT_TOKENS);
    Usage {
        input_tokens,
        output_tokens,
        reasoning_tokens: 0,
        cache_read_tokens: 0,
        cache_write_tokens: 0,
    }
}

/// The output allowance a request asked for, in whichever spelling its surface
/// uses. `None` when the caller left it to the provider.
///
/// A body carrying several spellings takes the largest of them, because a
/// present-but-unusable field (`null`, a string) must not hide a usable one from
/// the ceiling or from the hold: whichever field the provider honors, the answer
/// here is never below it.
fn requested_output_tokens(body: &Value) -> Option<u64> {
    ["max_tokens", "max_completion_tokens", "max_output_tokens"]
        .into_iter()
        .filter_map(|field| body.get(field).and_then(Value::as_u64))
        .max()
}

struct RecordArgs<'a> {
    /// The event identity minted when the request was accepted, so the record
    /// carries the id the rest of the request already referred to rather than
    /// one invented at settlement.
    identity: &'a EventIdentity,
    caller: &'a InboundKey,
    alias: &'a str,
    target_provider: &'a str,
    target_model: &'a str,
    source: CredentialSource,
    credential_id: &'a str,
    status: Status,
    input_tokens: u64,
    cache_read_tokens: u64,
    cache_write_tokens: u64,
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
        request_id: args.identity.request_id.to_string(),
        trace_id: args.identity.trace_id.clone(),
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
        cache_read_tokens: args.cache_read_tokens,
        cache_write_tokens: args.cache_write_tokens,
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
    use crate::convergence::status::testing::ManualClock;
    use crate::convergence::{Rejection, RevisionStatus, SnapshotSource};
    use crate::desired_state::fixtures::revision_id;
    use crate::principals::PrincipalAuthority;
    use crate::rate_limit::{InMemoryRateLimiter, NoLimit, RateLimitKey, RateLimiter};
    use crate::state::ReplicaObservability;
    use crate::status::registry::{CachedStatusRegistry, StatusRefresher, StatusSettings};
    use crate::status::{Component, ComponentObservation, ComponentState, StatusReason};
    use crate::usage::identity::RequestId;
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
    use std::time::{Duration, SystemTime, UNIX_EPOCH};
    use tokio::sync::oneshot;
    use tower::util::ServiceExt;
    use tracing_subscriber::layer::SubscriberExt;

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
            .output_tokens,
            500_000,
            "the hold prices the allowance the provider will honor"
        );
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
        test_state_with_base_url("https://api.openai.com/v1")
    }

    fn test_state_with_base_url(base_url: &str) -> AppState {
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

    fn minting_state() -> AppState {
        minting_state_with_audience_epochs("test-audience", "")
    }

    fn minting_state_with_epochs(epochs: &str) -> AppState {
        minting_state_with_audience_epochs("test-audience", epochs)
    }

    fn minting_state_with_audience_epochs(audience: &str, epochs: &str) -> AppState {
        minting_state_with_scope_audience_epochs("scope = [\"chat\", \"models\"]", audience, epochs)
    }

    fn minting_state_without_scope() -> AppState {
        minting_state_with_scope_audience_epochs("", "test-audience", "")
    }

    fn minting_state_with_scope_audience_epochs(
        scope: &str,
        audience: &str,
        epochs: &str,
    ) -> AppState {
        let cfg = Config::from_toml_str(&format!(
            r#"
[[namespace]]
id = "platform"
default = true

[[gateway_key]]
env = "MINT_KEY"
namespace = "platform"
can_mint = true

[gateway_token]
audience = "{audience}"

[[gateway_verifier]]
kid = "mint-kid"
alg = "HS256"
env = "JWT_SECRET"
namespaces = ["platform"]
max_ttl = "15m"

[gateway_minting]
kid = "mint-kid"
env = "JWT_SECRET"
{scope}
aliases = ["gpt-*"]
max_request_microdollars = 1000
{epochs}
"#,
            audience = audience,
            scope = scope,
            epochs = epochs
        ))
        .unwrap();
        let env = HashMap::from([
            ("MINT_KEY".to_owned(), "mint-key".to_owned()),
            (
                "JWT_SECRET".to_owned(),
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            ),
        ]);
        let sinks: Vec<Box<dyn UsageSink>> = vec![Box::new(StdoutSink)];
        AppState::new(cfg, &env, UsageFanout::new(sinks), Box::new(NoBudget))
            .expect("minting state")
    }

    async fn mint_request(state: AppState, body: Value) -> (StatusCode, Value) {
        mint_request_with_credential(state, "mint-key", body).await
    }

    async fn mint_request_with_credential(
        state: AppState,
        credential: &str,
        body: Value,
    ) -> (StatusCode, Value) {
        let response = router(state)
            .oneshot(
                Request::post("/v1/tokens")
                    .header(
                        axum::http::header::AUTHORIZATION,
                        format!("Bearer {credential}"),
                    )
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body = serde_json::from_slice(&body).unwrap_or(Value::Null);
        (status, body)
    }

    #[tokio::test]
    async fn minting_response_is_not_cacheable() {
        let response = router(minting_state())
            .oneshot(
                Request::post("/v1/tokens")
                    .header(axum::http::header::AUTHORIZATION, "Bearer mint-key")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"sub":"agent"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(axum::http::header::CACHE_CONTROL),
            Some(&HeaderValue::from_static("no-store"))
        );
    }

    #[tokio::test]
    async fn no_scope_ceiling_inherits_ordinary_capabilities() {
        let state = minting_state_without_scope();
        let (status, body) = mint_request(state.clone(), json!({"sub": "agent"})).await;
        assert_eq!(status, StatusCode::OK);
        let token = body["token"].as_str().unwrap();
        let principal = state
            .config()
            .resolve_principal(&Presented { credential: token })
            .await
            .expect("resolve minted token")
            .expect("minted principal");
        assert!(
            principal
                .scope
                .as_ref()
                .is_none_or(|scope| !scope.contains(&Capability::CredentialsAll))
        );
        let response =
            scoped_route_request(state.clone(), "/v1/credentials?namespaces=all", token).await;
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let response = router(state)
            .oneshot(
                Request::get("/v1/models")
                    .header(axum::http::header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    /// The two authorities stay separate in both directions: the static key can
    /// read every namespace itself, yet cannot hand that reach to a subject, and
    /// an omitted mint scope still cannot inherit it (#116).
    #[tokio::test]
    async fn minting_cannot_delegate_the_operator_view_a_static_key_holds_directly() {
        let state = minting_state_without_scope();
        let (status, body) = mint_request(
            state.clone(),
            json!({"sub": "agent", "scope": ["credentials", "credentials:all"]}),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(body["error"]["type"], "mint_claims_not_narrowing");

        let (status, body) = mint_request(state.clone(), json!({"sub": "agent"})).await;
        assert_eq!(status, StatusCode::OK);
        let token = body["token"].as_str().expect("minted token").to_owned();
        let response =
            scoped_route_request(state.clone(), "/v1/credentials?namespaces=all", &token).await;
        assert_scope_denial(response, "credentials:all").await;

        let response =
            scoped_route_request(state, "/v1/credentials?namespaces=all", "mint-key").await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    /// A mint request that names no scope inherits the caller's route
    /// capabilities, but never `status`: the dependency-status view is a grant an
    /// operator writes down, not one a subject inherits (#199). A configured
    /// ceiling is that writing down, so one naming `status` still confers it.
    #[tokio::test]
    async fn a_scope_less_mint_grants_route_capabilities_but_not_status() {
        let state = minting_state_without_scope();
        let (status, body) = mint_request(state.clone(), json!({"sub": "agent"})).await;
        assert_eq!(status, StatusCode::OK);
        let token = body["token"].as_str().expect("minted token").to_owned();
        let snapshot = state.config();
        let headers = HeaderMap::from_iter([(
            axum::http::header::AUTHORIZATION,
            format!("Bearer {token}").parse().unwrap(),
        )]);
        let caller = authenticate(&snapshot, &headers)
            .await
            .expect("the minted token authenticates");
        let scope = caller
            .scope
            .expect("an omitted scope is still written down");
        assert!(scope.contains(&Capability::Chat));
        assert!(scope.contains(&Capability::Models));
        assert!(!scope.contains(&Capability::Status));
        assert!(!scope.contains(&Capability::CredentialsAll));

        let (status, _) = mint_request(state, json!({"sub": "agent", "scope": ["status"]})).await;
        assert_eq!(status, StatusCode::OK);

        // A configured ceiling is the deployment's own written-down grant, so a
        // ceiling naming `status` still confers it: the exclusion is the default,
        // not an override of the operator's configuration.
        let state = minting_state_with_scope_audience_epochs(
            r#"scope = ["chat", "status"]"#,
            "test-audience",
            "",
        );
        let (status, body) = mint_request(state.clone(), json!({"sub": "agent"})).await;
        assert_eq!(status, StatusCode::OK);
        let token = body["token"].as_str().expect("minted token").to_owned();
        let snapshot = state.config();
        let headers = HeaderMap::from_iter([(
            axum::http::header::AUTHORIZATION,
            format!("Bearer {token}").parse().unwrap(),
        )]);
        let caller = authenticate(&snapshot, &headers)
            .await
            .expect("the minted token authenticates");
        let scope = caller.scope.expect("the ceiling is written down");
        assert!(scope.contains(&Capability::Status));
        assert!(!scope.contains(&Capability::Models));
    }

    #[tokio::test]
    async fn default_scope_minted_token_reaches_responses() {
        let state = scoped_route_state().await;
        let mut config = state.config().config.clone();
        config.gateway_key[0].can_mint = true;
        config.gateway_minting = Some(crate::config::GatewayMinting {
            kid: "scope-test-kid".to_owned(),
            env: Some("JWT_SECRET".to_owned()),
            file: None,
            max_ttl: None,
            scope: None,
            aliases: None,
            max_request_microdollars: None,
        });
        let env = HashMap::from([
            ("CHAT_KEY".to_owned(), "chat-key".to_owned()),
            ("MESSAGES_KEY".to_owned(), "messages-key".to_owned()),
            ("EMBEDDINGS_KEY".to_owned(), "embeddings-key".to_owned()),
            ("RESPONSES_KEY".to_owned(), "responses-key".to_owned()),
            ("STATIC_KEY".to_owned(), "static-key".to_owned()),
            (
                "JWT_SECRET".to_owned(),
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            ),
        ]);
        state.publish(ConfigSnapshot::build(config, &env, 0).expect("minting snapshot"));

        let (status, body) =
            mint_request_with_credential(state.clone(), "static-key", json!({"sub": "agent"}))
                .await;
        assert_eq!(status, StatusCode::OK);
        let token = body["token"].as_str().expect("minted token");
        let response = scoped_route_request(state, "/v1/responses", token).await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn padded_audience_mints_a_token_the_gateway_accepts() {
        let state = minting_state_with_audience_epochs("  test-audience  ", "");
        let response = router(state.clone())
            .oneshot(
                Request::post("/v1/tokens")
                    .header(axum::http::header::AUTHORIZATION, "Bearer mint-key")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"sub":"agent"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let token = serde_json::from_slice::<Value>(&body).unwrap()["token"]
            .as_str()
            .unwrap()
            .to_owned();

        let response = router(state)
            .oneshot(
                Request::get("/v1/models")
                    .header(axum::http::header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn future_token_epochs_block_minting_but_past_epochs_do_not() {
        let future = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 3600;
        for (epoch, subject) in [
            (
                format!("[[gateway_token_epoch]]\nnamespace = \"platform\"\nmin_iat = {future}"),
                "agent",
            ),
            (
                format!(
                    "[[gateway_token_epoch]]\nnamespace = \"platform\"\nsubject = \"agent\"\nmin_iat = {future}"
                ),
                " agent ",
            ),
        ] {
            let (status, body) =
                mint_request(minting_state_with_epochs(&epoch), json!({"sub": subject})).await;
            assert_eq!(status, StatusCode::FORBIDDEN);
            assert_eq!(body["error"]["type"], "mint_epoch_not_usable");
        }

        let near = future - 3598;
        let state = minting_state_with_epochs(&format!(
            "[[gateway_token_epoch]]\nnamespace = \"platform\"\nsubject = \"near\"\nmin_iat = {near}"
        ));
        let (status, body) = mint_request(state.clone(), json!({"sub": "near"})).await;
        assert_eq!(status, StatusCode::OK);
        let token = body["token"].as_str().expect("minted token");
        let exp = body["exp"].as_u64().expect("expiry");
        let expires_in = body["expires_in"].as_u64().expect("remaining lifetime");
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert_eq!(expires_in, exp.saturating_sub(now));
        assert!(
            state
                .config()
                .resolve_principal(&Presented { credential: token })
                .await
                .expect("resolve minted token")
                .is_some()
        );

        let past = future - 7200;
        let epoch = format!("[[gateway_token_epoch]]\nnamespace = \"platform\"\nmin_iat = {past}");
        let (status, body) =
            mint_request(minting_state_with_epochs(&epoch), json!({"sub": " agent "})).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["sub"], "agent");
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
        let (responses_url, _) = native_upstream(
            "/responses",
            Json(json!({
                "id": "resp-1",
                "usage": { "input_tokens": 1, "output_tokens": 1 }
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

[[provider]]
id = "responses"
kind = "openai"
base_url = "{responses_url}"

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

[[credential]]
namespace = "platform"
provider = "responses"
env = "RESPONSES_KEY"

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

[[model]]
name = "responses-model"
targets = [{{ provider = "responses", model = "responses-model", price = {{ input_microdollars_per_million = 1, output_microdollars_per_million = 1 }} }}]
"#
        ))
        .expect("scope test config");
        let env = HashMap::from([
            ("CHAT_KEY".to_owned(), "chat-key".to_owned()),
            ("MESSAGES_KEY".to_owned(), "messages-key".to_owned()),
            ("EMBEDDINGS_KEY".to_owned(), "embeddings-key".to_owned()),
            ("RESPONSES_KEY".to_owned(), "responses-key".to_owned()),
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
            "/v1/responses" => (
                Method::POST,
                serde_json::to_vec(&json!({
                    "model": "responses-model",
                    "input": "hello"
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
    async fn credentials_status_requires_its_scope_and_denies_every_minted_operator_view() {
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

        // A minted token cannot buy the operator view with a `credentials:all`
        // claim: delegation never confers direct operator authority, even from a
        // signer that emitted the claim outside `POST /v1/tokens`.
        let response = scoped_route_request(
            scoped_route_state().await,
            "/v1/credentials?namespaces=all",
            &scoped_token(Some(vec!["credentials", "credentials:all"])),
        )
        .await;
        assert_scope_denial(response, "credentials:all").await;

        let response = scoped_route_request(
            scoped_route_state().await,
            "/v1/credentials?namespaces=tenant",
            &scoped_token(Some(vec!["credentials"])),
        )
        .await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn credentials_status_scope_less_minted_token_keeps_own_namespace_view_only() {
        let response = scoped_route_request(
            scoped_route_state().await,
            "/v1/credentials",
            &scoped_token(None),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(body["data"][0]["namespace"], "platform");

        let response = scoped_route_request(
            scoped_route_state().await,
            "/v1/credentials?namespaces=all",
            &scoped_token(None),
        )
        .await;
        assert_scope_denial(response, "credentials:all").await;
    }

    #[tokio::test]
    async fn credentials_status_default_namespace_static_key_reaches_the_operator_view() {
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
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn credentials_status_operator_view_follows_authority_not_claims() {
        let response = scoped_route_request(
            isolated_tenant_state(),
            "/v1/credentials?namespaces=all",
            "platform-secret",
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

        // A tenant static key holds no authority beyond its own namespace.
        let response =
            scoped_route_request(isolated_tenant_state(), "/v1/credentials", "acme-static").await;
        assert_eq!(response.status(), StatusCode::OK);
        let response = scoped_route_request(
            isolated_tenant_state(),
            "/v1/credentials?namespaces=all",
            "acme-static",
        )
        .await;
        assert_scope_denial(response, "credentials:all").await;

        // Neither does a token in the default namespace that claims the
        // operator capability outright.
        for token in [
            scoped_token_for_namespace(
                "scope-tests",
                "acme",
                Some(vec!["credentials", "credentials:all"]),
            ),
            scoped_token(Some(vec!["credentials", "credentials:all"])),
        ] {
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
        }
    }

    #[tokio::test]
    async fn credentials_status_never_serializes_secret_material() {
        let response = scoped_route_request(
            scoped_route_state().await,
            "/v1/credentials?namespaces=all",
            "static-key",
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

[[gateway_key]]
env = "PLATFORM_STATIC_KEY"
namespace = "platform"

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
            ("STATIC_KEY".to_owned(), "acme-static".to_owned()),
            (
                "PLATFORM_STATIC_KEY".to_owned(),
                "platform-secret".to_owned(),
            ),
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
            "/v1/responses",
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
    async fn scoped_token_requires_the_responses_capability() {
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
        assert_scope_denial(response, "responses").await;
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
        let public_paths: Vec<_> = route_specs(false)
            .iter()
            .filter(|spec| spec.auth == AuthPosture::LivenessProbe)
            .map(|spec| spec.path)
            .collect();
        assert_eq!(public_paths, ["/healthz", "/readyz"]);

        for spec in route_specs(true)
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
                let response = if spec.path == "/v1/tokens" {
                    router(minting_state()).oneshot(request).await.unwrap()
                } else {
                    router(test_state()).oneshot(request).await.unwrap()
                };
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
    async fn minting_route_is_absent_without_boot_minting_config() {
        let response = router(test_state())
            .oneshot(Request::post("/v1/tokens").body(Body::from("{}")).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn static_minting_key_mints_but_minted_token_cannot_mint() {
        let state = minting_state();
        let response = router(state.clone())
            .oneshot(
                Request::post("/v1/tokens")
                    .header(axum::http::header::AUTHORIZATION, "Bearer mint-key")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "sub": "agent-1",
                            "ttl_seconds": 60,
                            "scope": ["chat"],
                            "aliases": ["gpt-4o"],
                            "max_request_microdollars": 500
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&body).unwrap();
        let token = body["token"].as_str().unwrap().to_owned();
        let principal = state
            .config()
            .resolve_principal(&Presented { credential: &token })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(principal.namespace, "platform");
        assert_eq!(principal.subject, "agent-1");
        assert!(!principal.can_mint);

        let response = router(state)
            .oneshot(
                Request::post("/v1/tokens")
                    .header(axum::http::header::AUTHORIZATION, format!("Bearer {token}"))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"sub":"agent-2"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn minting_rejects_unauthorized_and_malformed_requests() {
        let (status, body) = mint_request(minting_state(), json!({"sub": "agent"})).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["namespace"], "platform");

        let state = minting_state();
        let mut config = state.config().config.clone();
        config.gateway_key[0].can_mint = false;
        let env = HashMap::from([
            ("MINT_KEY".to_owned(), "mint-key".to_owned()),
            (
                "JWT_SECRET".to_owned(),
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            ),
        ]);
        let unauthorized = ConfigSnapshot::build(config, &env, 0).unwrap();
        state.publish(unauthorized);
        let (status, body) = mint_request(state, json!({"sub": "agent"})).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(body["error"]["type"], "mint_not_authorized");

        for body in [
            json!({"sub": "agent", "ns": "platform"}),
            json!({"sub": ""}),
            json!({"sub": "   "}),
            json!({"sub": "a".repeat(MAX_MINT_SUBJECT_LENGTH + 1)}),
            json!({"sub": "agent@example"}),
        ] {
            let (status, response) = mint_request(minting_state(), body).await;
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert_eq!(response["error"]["type"], "bad_request");
        }
    }

    #[tokio::test]
    async fn minting_rejects_every_claim_widening_attempt() {
        for body in [
            json!({"sub": "agent", "ttl_seconds": 901}),
            json!({"sub": "agent", "ttl_seconds": 0}),
            json!({"sub": "agent", "scope": ["embeddings"]}),
            json!({"sub": "agent", "aliases": ["*"]}),
            json!({"sub": "agent", "aliases": ["claude-*"]}),
            json!({"sub": "agent", "max_request_microdollars": 1001}),
        ] {
            let (status, body) = mint_request(minting_state(), body).await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
            assert_eq!(body["error"]["type"], "mint_claims_not_narrowing");
        }
    }

    #[tokio::test]
    async fn omitted_minting_claims_inherit_configured_ceilings() {
        let (status, body) = mint_request(minting_state(), json!({"sub": "agent"})).await;
        assert_eq!(status, StatusCode::OK);
        let token = body["token"].as_str().unwrap();
        let state = minting_state();
        let principal = state
            .config()
            .resolve_principal(&Presented { credential: token })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            principal.scope,
            Some(HashSet::from([Capability::Chat, Capability::Models]))
        );
        assert_eq!(
            principal.alias_scope,
            Some(AliasScope::parse(["gpt-*"]).unwrap())
        );
        assert_eq!(principal.max_request_microdollars, Some(1000));
    }

    #[tokio::test]
    async fn removing_minting_from_a_published_snapshot_is_fail_closed() {
        let state = minting_state();
        let app = router(state.clone());
        let mut config = state.config().config.clone();
        config.gateway_minting = None;
        config.gateway_key[0].can_mint = false;
        let env = HashMap::from([
            ("MINT_KEY".to_owned(), "mint-key".to_owned()),
            (
                "JWT_SECRET".to_owned(),
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            ),
        ]);
        state.publish(ConfigSnapshot::build(config, &env, 1).unwrap());
        let response = app
            .oneshot(
                Request::post("/v1/tokens")
                    .header(axum::http::header::AUTHORIZATION, "Bearer mint-key")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"sub":"agent"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["error"]["type"], "minting_disabled");
    }

    #[tokio::test]
    async fn the_responses_route_rejects_anonymous_callers_before_dispatching() {
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

    /// A stateful replica is administrable before it is servable. Its inference
    /// surface must say so per request rather than answer an empty configuration:
    /// an unknown-model `404` or an unauthorized `401` would read, to a caller,
    /// as a deployment that is configured and simply lacks what was asked for.
    #[tokio::test]
    async fn an_unconverged_replica_refuses_inference_without_pretending_to_be_ready() {
        let reason = crate::ops::inference_refusal(
            &crate::config::Config::from_toml_str(
                "mode = \"stateful\"\n\
                 [control_plane]\ndsn_env = \"GW_CONTROL_PLANE_DSN\"\n\
                 [secret_store]\nkek_env = \"GW_KEK\"\n\
                 [[admin_breakglass]]\nenv = \"GW_BREAKGLASS\"\n",
            )
            .expect("a valid stateful config"),
        )
        .expect("stateful inference is refused");
        let app = unconverged_router(reason);

        let live = app
            .clone()
            .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(live.status(), StatusCode::OK, "the process is healthy");

        let ready = app
            .clone()
            .oneshot(Request::get("/readyz").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(
            ready.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "but it must never be routed inference traffic"
        );

        for path in ["/v1/chat/completions", "/v1/models", "/v1/messages"] {
            let resp = app
                .clone()
                .oneshot(Request::post(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE, "{path}");
            let body: Value =
                serde_json::from_slice(&resp.into_body().collect().await.expect("body").to_bytes())
                    .expect("json error body");
            assert_eq!(body["error"]["type"], "inference_unavailable", "{path}");
            assert!(
                body["error"]["message"]
                    .as_str()
                    .is_some_and(|message| message.contains("stateful")),
                "the refusal names the mode that caused it: {body}"
            );
        }
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
            authority: PrincipalAuthority::MintedToken,
            signer_kid: Some("test-kid".to_owned()),
            scope: None,
            alias_scope: Some(AliasScope::parse(["gpt-4o"]).unwrap()),
            max_request_microdollars: None,
            can_mint: false,
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
                    authority: PrincipalAuthority::MintedToken,
                    signer_kid: Some("test-kid".to_owned()),
                    scope: None,
                    alias_scope: Some(AliasScope::parse(["gpt-4o"]).unwrap()),
                    max_request_microdollars: None,
                    can_mint: false,
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
                    .body(Body::from(r#"{"model":"gpt-4o","input":"hello"}"#))
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

[[model]]
name = "gpt-4o"
targets = [{{ provider = "openai", model = "gpt-4o", price = {{ input_microdollars_per_million = 1000000, output_microdollars_per_million = 1000000 }} }}]
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
        AppState::new(cfg, &env, UsageFanout::new(sinks), budget).unwrap()
    }

    fn chat_request() -> Request<Body> {
        let body = serde_json::to_vec(&json!({"model": "gpt-4o", "messages": []})).unwrap();
        authorized("/v1/chat/completions")
            .body(Body::from(body))
            .unwrap()
    }

    fn responses_request(previous_response_id: Option<&str>) -> Request<Body> {
        let mut body = json!({"model": "gpt-4o", "input": "hello"});
        if let Some(id) = previous_response_id {
            body["previous_response_id"] = json!(id);
        }
        authorized("/v1/responses")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap()
    }

    fn streaming_responses_request(previous_response_id: Option<&str>) -> Request<Body> {
        let mut body = json!({"model": "gpt-4o", "input": "hello", "stream": true});
        if let Some(id) = previous_response_id {
            body["previous_response_id"] = json!(id);
        }
        authorized("/v1/responses")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap()
    }

    fn responses_request_with_null_previous_id() -> Request<Body> {
        let body = json!({
            "model": "gpt-4o",
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

[[model]]
name = "gpt-4o"
targets = [{{ provider = "openai", model = "gpt-4o", price = {{ input_microdollars_per_million = 1000000, output_microdollars_per_million = 1000000 }} }}]

[admission]
{admission}
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
            Box::new(NoLimit),
            Box::new(crate::revocation::NoDenylist),
        )
        .unwrap()
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
            "model": "gpt-4o",
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
            "model": "gpt-4o",
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
                            "model": "gpt-4o",
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
            authority: PrincipalAuthority::MintedToken,
            signer_kid: Some("test-kid".to_owned()),
            scope: None,
            alias_scope: None,
            max_request_microdollars: Some(1),
            can_mint: false,
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
            authority: PrincipalAuthority::MintedToken,
            signer_kid: Some("test-kid".to_owned()),
            scope: None,
            alias_scope: None,
            max_request_microdollars: Some(10_000),
            can_mint: false,
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

        let budget = Arc::new(crate::budget::InMemoryBudget::new(10_000));
        let state = budgeted_state(
            &format!("http://{addr}"),
            Box::new(SharedBudget(budget.clone())),
        );
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

        // Chat over the same alias still fails over: pinning is scoped to the
        // Responses wire, not to the alias.
        let chat = router(state).oneshot(chat_request()).await.unwrap();
        assert_eq!(chat.status(), StatusCode::OK);
        assert_eq!(hits_b.load(Ordering::SeqCst), 1);

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
        for record in records.iter().take(4) {
            assert_eq!(record.status.as_str(), "upstream_error");
            assert_eq!(record.attempts, 1);
            assert_eq!(record.target_provider, "pa");
        }
        assert_eq!(records[4].status.as_str(), "ok");
        assert_eq!(records[4].target_provider, "pb");
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

    #[tokio::test]
    async fn a_pinned_streaming_responses_skipped_target_releases_its_budget_hold() {
        let (url_a, hits_a) = controllable_upstream(
            Arc::new(AtomicBool::new(false)),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
        let (url_b, hits_b) =
            controllable_upstream(Arc::new(AtomicBool::new(true)), StatusCode::OK).await;
        let captured = CapturingSink::default();
        let budget = Arc::new(crate::budget::InMemoryBudget::new(1_000_000));
        let state = two_target_state_with_budget(
            &url_a,
            &url_b,
            "[failover]\nmax_attempts = 3\nfailure_threshold = 1",
            captured,
            Box::new(SharedBudget(budget.clone())),
        );

        let first = router(state.clone())
            .oneshot(responses_request(None))
            .await
            .unwrap();
        assert_eq!(first.status(), StatusCode::BAD_GATEWAY);
        assert_eq!(hits_a.load(Ordering::SeqCst), 1);
        assert_eq!(hits_b.load(Ordering::SeqCst), 0);

        // Both streamed shapes skip the tripped pinned target, and neither may
        // leave its reservation outstanding.
        for request in [
            streaming_responses_request(Some("resp-from-a")),
            streaming_responses_request(None),
        ] {
            let pinned = router(state.clone()).oneshot(request).await.unwrap();
            assert_eq!(pinned.status(), StatusCode::SERVICE_UNAVAILABLE);
            let _ = pinned.into_body().collect().await.unwrap();
        }
        assert_eq!(hits_a.load(Ordering::SeqCst), 1);
        assert_eq!(hits_b.load(Ordering::SeqCst), 0);

        let key = BudgetKey {
            namespace: "platform".to_owned(),
            subject: "AXOND_INBOUND_KEY".to_owned(),
        };
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            if budget.outstanding(&key) == 0 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "a pinned Responses request leaked its budget reservation"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
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

    // ----------------------------------------------------------------
    // `/admin/v1/status`: authorization, redaction, and revision visibility.
    //
    // The route reports on dependencies, which makes it the one surface where a
    // leak is a leak of the operator's infrastructure rather than of a request.
    // Four properties are asserted, all fail-closed: an unauthenticated caller
    // learns nothing, a token without the capability learns nothing, a tenant
    // sees its own request path with coarsened reasons, and no scope sees a
    // secret, a DSN, a raw backend error, or a revision id.

    /// The status deployment: an operator's scope-less key in the default
    /// namespace, a tenant's key in another namespace, and a verifier for minted
    /// tokens.
    const STATUS_CONFIG: &str = r#"
[[namespace]]
id = "platform"
default = true

[[namespace]]
id = "tenant"

[[gateway_key]]
env = "AXOND_OPERATOR_KEY"
namespace = "platform"

[[gateway_key]]
env = "AXOND_TENANT_KEY"
namespace = "tenant"

[gateway_token]
audience = "test-audience"

[[gateway_verifier]]
kid = "scope-test-kid"
alg = "HS256"
env = "JWT_SECRET"
namespaces = ["platform"]
max_ttl = "15m"
"#;

    const OPERATOR_KEY: &str = "operator-secret";
    const TENANT_KEY: &str = "tenant-secret";

    /// What a probe of a broken Postgres would carry into the registry: a DSN
    /// with a password, and the backend's own error text. Published through the
    /// same `publish` the refresher uses, so the redaction under test is the
    /// shipped path rather than a test-only one.
    const LEAKY_DETAIL: &str = "connection to postgres://axond:s3cr3t-password@db.internal:5432/axond failed: FATAL: password authentication failed";

    fn status_state(observability: ReplicaObservability) -> AppState {
        status_state_with_revocation(observability, Box::new(crate::revocation::NoDenylist))
    }

    /// A replica whose served-traffic ceiling is small enough for a test to
    /// exhaust, so "saturated" is a real state rather than a simulated one.
    fn saturable_status_state(observability: ReplicaObservability) -> AppState {
        let config = Config::from_toml_str(&format!(
            "{STATUS_CONFIG}\n[admission]\nmax_in_flight = 1\n"
        ))
        .expect("status config");
        let env = HashMap::from([
            ("AXOND_OPERATOR_KEY".to_owned(), OPERATOR_KEY.to_owned()),
            ("AXOND_TENANT_KEY".to_owned(), TENANT_KEY.to_owned()),
            (
                "JWT_SECRET".to_owned(),
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            ),
        ]);
        let sinks: Vec<Box<dyn UsageSink>> = vec![Box::new(StdoutSink)];
        AppState::new_with_observability(
            config,
            &env,
            UsageFanout::new(sinks),
            Box::new(NoBudget),
            Box::new(NoLimit),
            Box::new(crate::revocation::NoDenylist),
            observability,
        )
        .expect("status state")
    }

    fn status_state_with_revocation(
        observability: ReplicaObservability,
        revocation: Box<dyn crate::revocation::RevocationStore>,
    ) -> AppState {
        let config = Config::from_toml_str(STATUS_CONFIG).expect("status config");
        let env = HashMap::from([
            ("AXOND_OPERATOR_KEY".to_owned(), OPERATOR_KEY.to_owned()),
            ("AXOND_TENANT_KEY".to_owned(), TENANT_KEY.to_owned()),
            (
                "JWT_SECRET".to_owned(),
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            ),
        ]);
        let sinks: Vec<Box<dyn UsageSink>> = vec![Box::new(StdoutSink)];
        AppState::new_with_observability(
            config,
            &env,
            UsageFanout::new(sinks),
            Box::new(NoBudget),
            Box::new(NoLimit),
            revocation,
            observability,
        )
        .expect("status state")
    }

    /// A stateful replica's registry: every component enabled, with the
    /// control plane refusing the replica's own credentials and the budget store
    /// unreachable. One is operator-only and one is on the tenant's request path,
    /// which is what makes the two scopes distinguishable.
    fn observed_registry() -> Arc<CachedStatusRegistry> {
        let registry = Arc::new(CachedStatusRegistry::new(
            StatusSettings {
                enabled: Component::ALL.to_vec(),
                ..StatusSettings::default()
            },
            Arc::new(crate::convergence::SystemClock),
        ));
        registry.publish(ComponentObservation::unavailable(
            Component::ControlPlane,
            StatusReason::AuthenticationRejected,
            LEAKY_DETAIL.to_owned(),
        ));
        registry.publish(ComponentObservation::unavailable(
            Component::BudgetStore,
            StatusReason::Unreachable,
            LEAKY_DETAIL.to_owned(),
        ));
        registry.publish(ComponentObservation {
            component: Component::Catalogue,
            state: ComponentState::Ok,
            reason: None,
            detail: None,
        });
        registry
    }

    async fn status_response(state: AppState, credential: Option<&str>) -> (StatusCode, Value) {
        let mut request = Request::get("/admin/v1/status");
        if let Some(credential) = credential {
            request = request.header(
                axum::http::header::AUTHORIZATION,
                format!("Bearer {credential}"),
            );
        }
        let response = router(state)
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    fn component_reason(body: &Value, component: &str) -> Option<String> {
        body["components"]
            .as_array()
            .expect("components")
            .iter()
            .find(|entry| entry["component"] == component)
            .map(|entry| entry["reason"].as_str().unwrap_or_default().to_owned())
    }

    /// Fail-closed: dependency status is not a public health endpoint. An
    /// unauthenticated poller gets `401` and no component list, because "which of
    /// the operator's backends is down" is reconnaissance.
    #[tokio::test]
    async fn status_refuses_an_unauthenticated_caller() {
        let (status, body) = status_response(
            status_state(ReplicaObservability {
                status: observed_registry(),
                revision: None,
            }),
            None,
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert!(body.get("components").is_none(), "{body}");
    }

    /// `status` is not granted to a scope-less minted token, so a token minted
    /// for inference cannot read dependency status by pointing at the route.
    #[tokio::test]
    async fn status_refuses_a_token_without_the_capability() {
        let state = status_state(ReplicaObservability {
            status: observed_registry(),
            revision: None,
        });
        let token = scoped_token_for("test-audience", Some(vec!["chat", "models"]));
        let (status, body) = status_response(state, Some(&token)).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(body["error"]["type"], "token_scope_insufficient");
        assert!(body.get("components").is_none(), "{body}");
    }

    /// The operator's own authority — a scope-less static key in the default
    /// namespace — is what deployment scope is derived from, never a request
    /// parameter.
    #[tokio::test]
    async fn status_gives_the_operator_the_deployment_view() {
        let state = status_state(ReplicaObservability {
            status: observed_registry(),
            revision: None,
        });
        let (status, body) = status_response(state, Some(OPERATOR_KEY)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["scope"], "deployment");
        assert_eq!(body["observed"], "replica");
        assert_eq!(body["phase"], "serving");
        assert_eq!(
            body["components"].as_array().expect("components").len(),
            Component::ALL.len()
        );
        // Exact reasons, including the operator-only ones.
        assert_eq!(
            component_reason(&body, "control_plane").as_deref(),
            Some("authentication_rejected")
        );
        assert_eq!(
            component_reason(&body, "budget_store").as_deref(),
            Some("unreachable")
        );
    }

    /// A tenant sees its own request path, with the operator's internals removed
    /// and the reasons behind them coarsened: it learns *that* a dependency is
    /// impaired, not that the operator's control-plane credential was rejected.
    #[tokio::test]
    async fn status_gives_a_tenant_only_its_own_request_path() {
        let state = status_state(ReplicaObservability {
            status: observed_registry(),
            revision: None,
        });
        let (status, body) = status_response(state, Some(TENANT_KEY)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["scope"], "namespace");
        let components: Vec<&str> = body["components"]
            .as_array()
            .expect("components")
            .iter()
            .map(|entry| entry["component"].as_str().expect("a component name"))
            .collect();
        assert!(!components.contains(&"control_plane"), "{body}");
        assert!(!components.contains(&"secret_store"), "{body}");
        assert!(!components.contains(&"usage_sink"), "{body}");
        assert!(components.contains(&"budget_store"), "{body}");
        // On the tenant's request path, but the reason is coarsened.
        assert_eq!(
            component_reason(&body, "budget_store").as_deref(),
            Some("unavailable")
        );
    }

    /// A minted token is not the operator however it is scoped: authority, not
    /// namespace, is what deployment scope turns on. A `status`-scoped token in
    /// the *default* namespace still gets the namespace view.
    #[tokio::test]
    async fn a_minted_status_token_is_not_the_operator() {
        let state = status_state(ReplicaObservability {
            status: observed_registry(),
            revision: None,
        });
        let token = scoped_token_for("test-audience", Some(vec!["status"]));
        let (status, body) = status_response(state, Some(&token)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["scope"], "namespace");
        assert!(body.get("revision").is_none(), "{body}");
    }

    /// The handler probes nothing, but authentication is not free for a *minted*
    /// caller: a token carries a `jti`, and checking it against the revocation
    /// store is a backend call that fails closed. So a revocation-store outage
    /// takes the minted view of status with it, while the operator's static key —
    /// which has no `jti` to check — keeps answering. That is the reason the
    /// runbook says to triage with an operator key rather than a minted one.
    #[tokio::test]
    async fn a_revocation_outage_leaves_only_the_operator_key_reading_status() {
        let unavailable = || {
            status_state_with_revocation(
                ReplicaObservability {
                    status: observed_registry(),
                    revision: None,
                },
                Box::new(FakeRevocation {
                    mode: FakeRevocationMode::Unavailable,
                    calls: Arc::new(AtomicUsize::new(0)),
                }),
            )
        };

        let (status, body) = status_response(unavailable(), Some(OPERATOR_KEY)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["scope"], "deployment", "{body}");

        let token = scoped_token_for("test-audience", Some(vec!["status"]));
        let (status, body) = status_response(unavailable(), Some(&token)).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["error"]["type"], "revocation_unavailable", "{body}");
    }

    /// The redaction assertion, made against the serialized bytes rather than
    /// against fields: a probe published a DSN, a password, and a backend error,
    /// and none of it appears in either scope's response.
    #[tokio::test]
    async fn status_never_serializes_a_secret_a_dsn_or_a_backend_error() {
        for credential in [OPERATOR_KEY, TENANT_KEY] {
            let state = status_state(ReplicaObservability {
                status: observed_registry(),
                revision: Some(lagging_replica().1),
            });
            let (status, body) = status_response(state, Some(credential)).await;
            assert_eq!(status, StatusCode::OK);
            let serialized = body.to_string();
            for leaked in [
                "s3cr3t-password",
                "postgres://",
                "db.internal",
                "password authentication failed",
                "FATAL",
                LEAKY_DETAIL,
            ] {
                assert!(
                    !serialized.contains(leaked),
                    "`{leaked}` reached a {credential} response: {serialized}"
                );
            }
            // Revision *identifiers* are absent in every scope, including the
            // operator's, because they are unbounded over a deployment's life.
            for revision in [revision_id(7), revision_id(8)] {
                assert!(!serialized.contains(&revision.to_string()), "{serialized}");
            }
        }
    }

    /// The response carries only bounded values, which is the cardinality
    /// property the metric labels depend on: every component name, state, and
    /// reason in it comes from a closed vocabulary the catalogue also names.
    #[tokio::test]
    async fn status_reports_only_bounded_vocabulary_values() {
        let state = status_state(ReplicaObservability {
            status: observed_registry(),
            revision: None,
        });
        let (_, body) = status_response(state, Some(OPERATOR_KEY)).await;
        let states: Vec<&str> = ComponentState::ALL
            .iter()
            .map(|state| state.as_str())
            .collect();
        let reasons: Vec<&str> = StatusReason::ALL
            .iter()
            .map(|reason| reason.code())
            .collect();
        for entry in body["components"].as_array().expect("components") {
            let component = entry["component"].as_str().expect("a component");
            assert!(
                crate::status::COMPONENTS.contains(&component),
                "`{component}` is outside the catalogued vocabulary"
            );
            assert!(states.contains(&entry["state"].as_str().expect("a state")));
            if let Some(reason) = entry["reason"].as_str() {
                assert!(
                    reasons.contains(&reason),
                    "`{reason}` is not a status reason"
                );
            }
            assert!(entry["observed_age_ms"].is_u64(), "{entry}");
        }
    }

    /// What a released binary answers today. `main.rs` builds its state through
    /// `new_with_rate_limiter`, which injects
    /// [`ReplicaObservability::stateless`], so no component is observed and no
    /// revision is tracked: every component is `disabled` with reason
    /// `not_configured`. This is the boundary the shipped dependency panels and
    /// their three alerts wait on, and it is documented as such in the
    /// observability runbook — a slice that injects a refresher should turn this
    /// test into its own opposite.
    #[tokio::test]
    async fn the_production_constructor_observes_nothing_yet() {
        let config = Config::from_toml_str(STATUS_CONFIG).expect("status config");
        let env = HashMap::from([
            ("AXOND_OPERATOR_KEY".to_owned(), OPERATOR_KEY.to_owned()),
            ("AXOND_TENANT_KEY".to_owned(), TENANT_KEY.to_owned()),
            (
                "JWT_SECRET".to_owned(),
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            ),
        ]);
        let sinks: Vec<Box<dyn UsageSink>> = vec![Box::new(StdoutSink)];
        let state = AppState::new_with_rate_limiter(
            config,
            &env,
            UsageFanout::new(sinks),
            Box::new(NoBudget),
            Box::new(NoLimit),
            Box::new(crate::revocation::NoDenylist),
        )
        .expect("state");
        assert!(state.revision_report().is_none());

        let (status, body) = status_response(state, Some(OPERATOR_KEY)).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["revision"].is_null(), "{body}");
        for entry in body["components"].as_array().expect("components") {
            assert_eq!(entry["state"], "disabled", "{entry}");
            assert_eq!(entry["reason"], "not_configured", "{entry}");
        }
    }

    /// Status is a diagnostic, not work. Once admission closes, a served route
    /// answers `503 draining` — but the runbook sends an operator here to
    /// diagnose a replica stuck in exactly that phase, and a route refused by
    /// admission could never report `closing` at all. So it authenticates
    /// outside admission, and takes no in-flight slot the shutdown deadline
    /// would then wait on.
    #[tokio::test]
    async fn status_still_answers_once_admission_has_closed() {
        let state = status_state(ReplicaObservability {
            status: observed_registry(),
            revision: None,
        });
        let lifecycle = Arc::clone(state.lifecycle());
        lifecycle.close();

        let served = router(state.clone())
            .oneshot(
                Request::get("/v1/models")
                    .header(
                        axum::http::header::AUTHORIZATION,
                        format!("Bearer {OPERATOR_KEY}"),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(served.status(), StatusCode::SERVICE_UNAVAILABLE);

        let (status, body) = status_response(state, Some(OPERATOR_KEY)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["phase"], "closing", "{body}");
        assert_eq!(lifecycle.in_flight(), 0, "a diagnostic held the drain open");
    }

    /// Exempt from the served-traffic gate is not exempt from every bound: the
    /// diagnostic has a small ceiling of its own, so a credential holder cannot
    /// poll it at unbounded concurrency. Held at the gate rather than the route
    /// because a status read completes too fast to keep eight of them in flight
    /// from a test.
    #[tokio::test]
    async fn diagnostic_reads_are_bounded_and_do_not_share_the_served_ceiling() {
        let state = saturable_status_state(ReplicaObservability {
            status: observed_registry(),
            revision: None,
        });
        let admission = &state.0.admission;

        let held: Vec<_> = (0..crate::admission::MAX_IN_FLIGHT_DIAGNOSTICS)
            .map(|_| admission.admit_diagnostic().expect("under the ceiling"))
            .collect();
        assert_eq!(
            admission.admit_diagnostic().err(),
            Some(crate::admission::AdmissionRejection::Diagnostics),
            "the diagnostic ceiling admitted past itself"
        );
        // Refused rather than queued, and refused as `503` with retry guidance
        // rather than as a caller error, because the process is full.
        let refused = GatewayError::Overloaded(crate::admission::AdmissionRejection::Diagnostics)
            .into_response();
        assert_eq!(refused.status(), StatusCode::SERVICE_UNAVAILABLE);
        drop(held);
        drop(admission.admit_diagnostic().expect("a slot was released"));

        // Served traffic at its own ceiling leaves the diagnostic answerable:
        // "why is this replica saturated" must not be refused by the saturation
        // it is asking about.
        let _saturated = admission
            .admit("tenant-a", crate::admission::RequestKind::Buffered)
            .await
            .expect("the only served slot");
        assert_eq!(
            admission
                .admit("tenant-b", crate::admission::RequestKind::Buffered)
                .await
                .err(),
            Some(crate::admission::AdmissionRejection::Global),
            "the served ceiling never engaged"
        );
        let (status, body) = status_response(state.clone(), Some(OPERATOR_KEY)).await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }

    /// The ceiling is inside authentication, so a slot is spent only by a caller
    /// that proved it may ask. Otherwise anyone reachable on the port could hold
    /// all eight closed against the operators the route exists for — and hold
    /// them for the length of a revocation-store round trip, during the very
    /// outage the runbook sends operators here to triage.
    #[tokio::test]
    async fn an_unauthenticated_caller_cannot_spend_a_diagnostic_slot() {
        let state = status_state(ReplicaObservability {
            status: observed_registry(),
            revision: None,
        });
        let held: Vec<_> = (0..crate::admission::MAX_IN_FLIGHT_DIAGNOSTICS)
            .map(|_| {
                state
                    .0
                    .admission
                    .admit_diagnostic()
                    .expect("under the ceiling")
            })
            .collect();

        let (status, body) = status_response(state.clone(), None).await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "an anonymous caller queued behind the ceiling: {body}"
        );
        let (status, _) = status_response(state.clone(), Some(OPERATOR_KEY)).await;
        assert_eq!(
            status,
            StatusCode::SERVICE_UNAVAILABLE,
            "the ceiling did not apply to the caller it is for"
        );
        drop(held);
        let (status, _) = status_response(state, Some(OPERATOR_KEY)).await;
        assert_eq!(status, StatusCode::OK);
    }

    /// The inner ceiling being inside authentication leaves authentication
    /// itself — a signature check, and a revocation lookup for a minted token —
    /// outside every bound, on the one authenticated route admission does not
    /// cover. The wider outer ceiling closes that: a flood of credentials that
    /// turn out to be worthless is refused before the replica spends anything
    /// verifying them, and the refusal is the same typed one.
    #[tokio::test]
    async fn a_flood_of_invalid_credentials_is_bounded_before_it_is_verified() {
        let state = status_state(ReplicaObservability {
            status: observed_registry(),
            revision: None,
        });
        let mut held = Vec::new();
        for (credential, capacity) in [
            (
                DiagnosticCredential::Minted,
                crate::admission::MAX_AUTHENTICATING_DIAGNOSTIC_TOKENS,
            ),
            (
                DiagnosticCredential::Local,
                crate::admission::MAX_AUTHENTICATING_DIAGNOSTIC_KEYS,
            ),
        ] {
            for _ in 0..capacity {
                held.push(
                    state
                        .0
                        .admission
                        .admit_diagnostic_authentication(credential)
                        .expect("under the ceiling"),
                );
            }
        }
        assert_eq!(held.len(), crate::admission::MAX_AUTHENTICATING_DIAGNOSTICS);

        // The credential is never reached, so an invalid one and the operator's
        // own are refused alike: the bound is on the verification, not the
        // verdict.
        for credential in [None, Some("not-a-key"), Some(OPERATOR_KEY)] {
            let (status, body) = status_response(state.clone(), credential).await;
            assert_eq!(
                status,
                StatusCode::SERVICE_UNAVAILABLE,
                "authentication ran outside the ceiling: {body}"
            );
            assert_eq!(body["error"]["type"], "diagnostic_concurrency_exceeded");
        }
        // The inner ceiling is untouched by that flood: it is spent on answers,
        // not on attempts.
        let answering: Vec<_> = (0..crate::admission::MAX_IN_FLIGHT_DIAGNOSTICS)
            .map(|_| {
                state
                    .0
                    .admission
                    .admit_diagnostic()
                    .expect("the flood did not spend an answering slot")
            })
            .collect();
        assert_eq!(answering.len(), crate::admission::MAX_IN_FLIGHT_DIAGNOSTICS);
        drop(answering);

        drop(held);
        let (status, _) = status_response(state, Some(OPERATOR_KEY)).await;
        assert_eq!(status, StatusCode::OK);
    }

    /// The pre-authentication ceiling is partitioned rather than pooled,
    /// because its two halves cost different things: a minted token is a
    /// revocation-store round trip, a static key a comparison in memory. Pooled,
    /// a store that is *slow* rather than down would park every permit in token
    /// verifications and refuse the operator's static key — the credential the
    /// runbook sends through exactly that outage.
    #[tokio::test]
    async fn a_token_flood_cannot_close_the_route_to_a_static_key() {
        let state = status_state(ReplicaObservability {
            status: observed_registry(),
            revision: None,
        });
        let flood: Vec<_> = (0..crate::admission::MAX_AUTHENTICATING_DIAGNOSTIC_TOKENS)
            .map(|_| {
                state
                    .0
                    .admission
                    .admit_diagnostic_authentication(DiagnosticCredential::Minted)
                    .expect("under the token share")
            })
            .collect();

        // And its refusals are counted where its capacity is held, so the
        // runbook's split of `axond_admission_rejections` distinguishes a
        // credential flood from busy readers rather than reporting both as the
        // answering ceiling.
        assert_eq!(
            state
                .0
                .admission
                .admit_diagnostic_authentication(DiagnosticCredential::Minted)
                .err(),
            Some(crate::admission::AdmissionRejection::DiagnosticsAuthenticating)
        );

        // Another token waits behind the flood rather than behind the store.
        let (status, body) = status_response(state.clone(), Some("axt1.another-one")).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["error"]["type"], "diagnostic_concurrency_exceeded");

        // The operator is not behind it: nothing about that credential can reach
        // the store the flood is parked on.
        let (status, body) = status_response(state.clone(), Some(OPERATOR_KEY)).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "a token flood held the route closed against a static key: {body}"
        );
        // Including a *wrong* static key, which costs the same comparison and so
        // shares the same share of the ceiling.
        let (status, _) = status_response(state.clone(), Some("not-a-key")).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        drop(flood);
    }

    /// `main` merges this router with the administrative surface, which nests
    /// the whole `/admin/v1` prefix and refuses it wholesale in stateless mode.
    /// The status diagnostic lives under that prefix without being part of that
    /// surface: it reports on the replica rather than administering durable
    /// state, so it must survive the merge in either mode while every
    /// administrative path keeps answering `stateful_mode_required`.
    #[tokio::test]
    async fn the_status_diagnostic_survives_the_administrative_merge() {
        let state = status_state(ReplicaObservability {
            status: observed_registry(),
            revision: None,
        });
        let app = router(state).merge(crate::admin::router::refusing_router());
        let answer = |path: String| {
            let app = app.clone();
            async move {
                let response = app
                    .oneshot(
                        Request::get(path)
                            .header(
                                axum::http::header::AUTHORIZATION,
                                format!("Bearer {OPERATOR_KEY}"),
                            )
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                let status = response.status();
                let bytes = response.into_body().collect().await.unwrap().to_bytes();
                (status, String::from_utf8_lossy(&bytes).into_owned())
            }
        };

        let (status, body) = answer("/admin/v1/status".to_owned()).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "the administrative refusal swallowed the diagnostic: {body}"
        );
        assert!(body.contains("\"object\":\"status\""), "{body}");

        let (status, body) = answer(format!("{}/tenants", crate::admin::ADMIN_PREFIX)).await;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
        assert!(body.contains("stateful_mode_required"), "{body}");
    }

    /// Registering a path under the administrative prefix means shadowing that
    /// surface's method fallback on it, so the one diagnostic living there has
    /// to carry the prefix's contract itself: a client branching on
    /// `AdminError::CODES` must never meet axum's empty-bodied 405.
    /// Both deployment postures, because both nest the prefix and both would
    /// otherwise answer this one path differently from every other one under it.
    #[tokio::test]
    async fn a_wrong_method_on_the_status_path_is_still_a_declared_refusal() {
        for (posture, surface) in [
            ("stateless", crate::admin::router::refusing_router()),
            ("stateful", stateful_admin_surface()),
        ] {
            let state = status_state(ReplicaObservability {
                status: observed_registry(),
                revision: None,
            });
            let response = router(state)
                .merge(surface)
                .oneshot(
                    Request::post("/admin/v1/status")
                        .header(
                            axum::http::header::AUTHORIZATION,
                            format!("Bearer {OPERATOR_KEY}"),
                        )
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                response.status(),
                StatusCode::METHOD_NOT_ALLOWED,
                "{posture}"
            );
            // RFC 9110 requires it on a 405, and axum sets it from the method
            // router rather than from the fallback body.
            assert!(
                response.headers().contains_key(axum::http::header::ALLOW),
                "{posture}"
            );
            let bytes = response.into_body().collect().await.unwrap().to_bytes();
            let body: Value = serde_json::from_slice(&bytes)
                .unwrap_or_else(|_| panic!("{posture}: a typed envelope, not an empty body"));
            assert_eq!(
                body["error"]["type"], "admin_method_not_allowed",
                "{posture}"
            );
        }
    }

    /// The administrative surface a stateful replica actually mounts, over an
    /// in-memory control plane: the merge is only worth testing against the real
    /// route table and its two fallbacks.
    fn stateful_admin_surface() -> Router {
        crate::admin::router::router(Arc::new(crate::admin::router::AdminApi::new(
            Arc::new(crate::admin::service::AdminService::stateful(Arc::new(
                crate::desired_state::oracle::InMemoryControlPlane::new(),
            ))),
            Arc::new(crate::admin::fakes::FakeAdminAuthenticator::new()),
            Arc::new(crate::admin::fakes::FakeAdminAuthorizer::permissive()),
        )))
    }

    /// A replica that refuses inference because it cannot compile a revision is
    /// the one an operator most needs to interrogate, and the runbook sends them
    /// to this route for exactly that incident. It serves no inference surface,
    /// so the diagnostic is mounted beside the refusal — without the refusal's
    /// fallback swallowing it, and while every other path still refuses.
    #[tokio::test]
    async fn the_status_diagnostic_answers_on_a_replica_that_refuses_inference() {
        let state = status_state(ReplicaObservability {
            status: observed_registry(),
            revision: None,
        });
        let app = unconverged_router("no snapshot yet").merge(diagnostic_router(state));
        let answer = |path: &'static str| {
            let app = app.clone();
            async move {
                let response = app
                    .oneshot(
                        Request::get(path)
                            .header(
                                axum::http::header::AUTHORIZATION,
                                format!("Bearer {OPERATOR_KEY}"),
                            )
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                let status = response.status();
                let bytes = response.into_body().collect().await.unwrap().to_bytes();
                (status, String::from_utf8_lossy(&bytes).into_owned())
            }
        };

        let (status, body) = answer("/admin/v1/status").await;
        assert_eq!(
            status,
            StatusCode::OK,
            "the inference refusal swallowed the diagnostic: {body}"
        );
        assert!(body.contains("\"object\":\"status\""), "{body}");

        let (status, body) = answer("/v1/models").await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(body.contains("inference_unavailable"), "{body}");

        // The shape `main` actually builds in stateful mode: the refusal, the
        // diagnostic, and the *real* administrative surface over the same
        // prefix. Overlapping prefixes panic in axum when they collide, so
        // composing it at all is half the assertion.
        let stateful = unconverged_router("no snapshot yet")
            .merge(diagnostic_router(status_state(ReplicaObservability {
                status: observed_registry(),
                revision: None,
            })))
            .merge(stateful_admin_surface());
        let response = stateful
            .oneshot(
                Request::get("/admin/v1/status")
                    .header(
                        axum::http::header::AUTHORIZATION,
                        format!("Bearer {OPERATOR_KEY}"),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "the administrative surface shadowed the diagnostic"
        );

        // And it is still the authenticated projection, not an open one.
        let response = unconverged_router("no snapshot yet")
            .merge(diagnostic_router(status_state(ReplicaObservability {
                status: observed_registry(),
                revision: None,
            })))
            .oneshot(
                Request::get("/admin/v1/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    /// A status read is a cache read: the handler returns the last observation
    /// and never probes, so a dependency outage cannot be turned into a status
    /// outage — or into load on a struggling backend — by polling this route.
    #[tokio::test]
    async fn status_reads_the_cache_without_probing() {
        let registry = observed_registry();
        let probe = Arc::new(CountingProbe {
            observations: Arc::new(AtomicUsize::new(0)),
        });
        let state = status_state(ReplicaObservability {
            status: Arc::clone(&registry),
            revision: None,
        });
        for _ in 0..5 {
            let (status, _) = status_response(state.clone(), Some(OPERATOR_KEY)).await;
            assert_eq!(status, StatusCode::OK);
        }
        assert_eq!(
            probe.observations.load(Ordering::SeqCst),
            0,
            "serving status observed a component"
        );
        // The refresher is the only caller that can: it observes once per round.
        StatusRefresher::new(Arc::clone(&registry), vec![probe.clone()])
            .refresh_once()
            .await;
        assert_eq!(probe.observations.load(Ordering::SeqCst), 1);
    }

    /// A probe that records being called. Reachable only from the refresher,
    /// which is what the test above turns into an assertion.
    struct CountingProbe {
        observations: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl crate::status::registry::ComponentProbe for CountingProbe {
        fn component(&self) -> Component {
            Component::BudgetStore
        }

        async fn observe(&self) -> ComponentObservation {
            self.observations.fetch_add(1, Ordering::SeqCst);
            ComponentObservation {
                component: Component::BudgetStore,
                state: ComponentState::Ok,
                reason: None,
                detail: None,
            }
        }
    }

    /// A replica that converged on the revision the control plane wants.
    fn converged_replica() -> Arc<RevisionStatus> {
        let clock = ManualClock::new();
        let status = Arc::new(RevisionStatus::new(Box::new(clock.clone())));
        let desired = revision_id(7);
        status.observe_desired(Some(desired));
        status.record_published(
            desired,
            9,
            SnapshotSource::ControlPlane,
            Duration::from_millis(120),
        );
        status.observe_desired(Some(desired));
        status
    }

    /// A replica still serving an older snapshot, with the clock moved on so the
    /// lag is an exact number rather than a race.
    fn lagging_replica() -> (ManualClock, Arc<RevisionStatus>) {
        let clock = ManualClock::new();
        let status = Arc::new(RevisionStatus::new(Box::new(clock.clone())));
        let old = revision_id(7);
        status.observe_desired(Some(old));
        status.record_published(
            old,
            9,
            SnapshotSource::ControlPlane,
            Duration::from_millis(120),
        );
        // The control plane publishes a newer revision this replica refuses.
        status.observe_desired(Some(revision_id(8)));
        status.record_rejection(
            Rejection {
                revision: Some(revision_id(8)),
                reason: "secret",
                detail: LEAKY_DETAIL.to_owned(),
            },
            3,
        );
        clock.advance(Duration::from_secs(90));
        (clock, status)
    }

    /// The fleet-convergence fixture: two replicas of the same deployment, one
    /// converged and one ninety seconds behind, each answering `/admin/v1/status`
    /// for itself.
    ///
    /// This is what makes a split fleet visible at all — a replica reports its
    /// own convergence and nothing else, so "the fleet disagrees" is a comparison
    /// an operator (or the shipped `AxondFleetRevisionSplit` rule) makes across
    /// replicas, not a claim any one replica makes.
    #[tokio::test]
    async fn two_replicas_report_their_own_revision_lag() {
        let converged = status_state(ReplicaObservability {
            status: observed_registry(),
            revision: Some(converged_replica()),
        });
        let (_clock, lagging_status) = lagging_replica();
        let lagging = status_state(ReplicaObservability {
            status: observed_registry(),
            revision: Some(lagging_status),
        });

        let (status, healthy) = status_response(converged, Some(OPERATOR_KEY)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(healthy["revision"]["converged"], true);
        assert_eq!(healthy["revision"]["lag_ms"], 0);
        assert_eq!(healthy["revision"]["consecutive_failures"], 0);
        assert_eq!(healthy["revision"]["source"], "control-plane");

        let (status, behind) = status_response(lagging, Some(OPERATOR_KEY)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(behind["revision"]["converged"], false);
        assert_eq!(behind["revision"]["lag_ms"], 90_000);
        assert_eq!(behind["revision"]["consecutive_failures"], 3);
        // The refusal reaches the response as a code, not as the rejection's own
        // detail, which named a secret.
        assert_eq!(behind["revision"]["reason"], "secret_unresolved");
        assert_eq!(behind["revision"]["generation"], 9);

        // Same deployment, same route, different answers: the lag is per replica.
        assert_ne!(healthy["revision"], behind["revision"]);
    }

    /// A tenant never learns what the fleet is converging on, in either state:
    /// convergence is the operator's business, and `lag_ms` plus a generation is
    /// enough to fingerprint a rollout.
    #[tokio::test]
    async fn a_tenant_sees_no_revision_summary_however_far_behind_the_replica_is() {
        let (_clock, lagging_status) = lagging_replica();
        let state = status_state(ReplicaObservability {
            status: observed_registry(),
            revision: Some(lagging_status),
        });
        let (status, body) = status_response(state, Some(TENANT_KEY)).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.get("revision").is_none(), "{body}");
    }
}
