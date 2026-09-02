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
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::extract::rejection::JsonRejection;
use axum::extract::{DefaultBodyLimit, Extension, OriginalUri, RawQuery, Request, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::middleware::{Next, from_fn_with_state};
use axum::response::{IntoResponse, Response};
use axum::routing::{MethodRouter, get, post};
use axum::{Json, Router};
use futures::StreamExt;
use gateway_core::{
    CircuitDecision, FailoverDecision, FailoverPolicy, FailoverTarget, MiddlewareScope,
    MiddlewareSurface, ModelPrice, ModelUsage, NativeMessagesDecoder, ProviderError,
    ProviderRequest, ProviderResponse, ProviderStreamDecoder, Surface, Usage,
};
use gateway_transport::{
    AuthScheme, Deadline, NativeCall, TimeoutBound, TimeoutKind, TransportError, Upstream,
};
use http_body::Body as HttpBody;
use secrecy::ExposeSecret;
use serde::Deserialize;
use serde_json::{Value, json};
use tracing::{Instrument, debug, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::admission::{AdmissionPermit, DiagnosticCredential, RequestKind};
use crate::aliases::AliasScope;
use crate::budget::{Admission, BudgetKey, Denial, Reservation};
use crate::config::{
    Config, CoreAccountingMode, Model, Provider, ProviderKind, ProviderWire, Target, UnpricedModels,
};
use crate::credentials::{CredentialLease, CredentialPlan, CredentialSource, CredentialStatusView};
use crate::desired_state::policy::BufferedResponseRoute;
use crate::error::GatewayError;
use crate::middleware::{CoreBudgetHold, MiddlewareChain, MiddlewareExecution};
use crate::mint::{MintRequest, mint_issued_at, mint_token_at};
use crate::namespace::NamespaceId;
use crate::pricing::{AliasPrices, Ineligible, RequestPrice};
use crate::principals::{Capability, Presented, PrincipalStoreError, TokenVerificationError};
use crate::rate_limit::{RateLimitKey, RateLimitPermit};
use crate::shutdown::Phase;
use crate::state::{AppState, ConfigSnapshot, InboundKey, adapter_for};
use crate::status::{StatusResponse, StatusScope};
use crate::store::{NamespaceRecord, StoreError};
use crate::streaming::{self, Framing, StreamContext, StreamDelivery};
use crate::telemetry;
use crate::usage::identity::EventIdentity;
use crate::usage::{Status, UsageRecord};

pub fn router(state: AppState) -> Router {
    let specs = route_specs(false);
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

/// The replica diagnostics alone, for a process that serves no inference.
///
/// An unconverged replica is exactly the one an operator most needs to ask about
/// — it is refusing inference and its convergence is the reason — so the
/// diagnostic is mounted beside [`unconverged_router`] rather than being lost
/// with the inference surface it happens to be declared next to.
#[allow(dead_code)]
pub fn diagnostic_router(state: AppState) -> Router {
    let specs = route_specs(state.config().gateway_minting.is_some())
        .into_iter()
        .filter(|spec| spec.auth == AuthPosture::Diagnostic)
        .collect();
    mount(specs, state, RouteAuthority::Global)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RouteAuthority {
    Global,
    Namespaced,
}

fn mount(specs: Vec<RouteSpec>, state: AppState, authority: RouteAuthority) -> Router {
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
            // Stateful inference is live-gated by the reconciler's active
            // revision. The gate is read per request, so a cold replica can
            // start with a refusal and begin serving immediately after a cache
            // restore or control-plane publication, without rebuilding the
            // router. Apply it before authentication so authentication remains
            // the first externally visible refusal: an anonymous caller gets
            // `401`, never a convergence-state `503`.
            let route = if spec.auth == AuthPosture::Authenticated && state.0.revision.is_some() {
                route.layer(from_fn_with_state(state.clone(), convergence_middleware))
            } else {
                route
            };
            let route = if spec.auth.requires_a_credential() {
                route.layer(from_fn_with_state(
                    (state.clone(), spec.capability, authority),
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
            // A path under the administrative prefix takes that prefix's method
            // contract with it: it shadows the nested surface's own
            // `method_not_allowed_fallback`, so without this a `POST` here
            // would be the single `/admin/v1` path answering axum's
            // empty-bodied 405 instead of a declared code
            // (`crate::admin::router::mount`). Registered outside every layer
            // above, because `MethodRouter::layer` wraps the fallback too and a
            // wrong method is answerable without an identity — the neighbouring
            // administrative paths answer it before authentication, and
            // answering `401` to a protocol mistake would send a caller looking
            // for a credential that would not have helped.
            let route = if spec.path.starts_with("/admin/v1") {
                route.fallback(|| async { crate::admin::AdminError::MethodNotAllowed })
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
#[allow(dead_code)]
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
#[derive(Clone, Copy)]
struct RouteSpec {
    path: &'static str,
    namespace_scoped: bool,
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
            path: "/admin/v1/status",
            namespace_scoped: false,
            auth: AuthPosture::Diagnostic,
            capability: Some(Capability::Status),
            router: || get(replica_status),
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
    ];
    if minting_enabled {
        routes.push(RouteSpec {
            path: "/v1/tokens",
            namespace_scoped: true,
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
/// able to refuse it. Callers presenting nothing at all are held to a third
/// share for the same reason — a flood needs no credential to mount.
async fn diagnostic_authentication_middleware(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Result<Response, GatewayError> {
    let credential = presented_credential(request.headers()).map_or(
        DiagnosticCredential::Anonymous,
        |credential| {
            state
                .config()
                .diagnostic_credential(&Presented { credential })
        },
    );
    let permit = state
        .0
        .admission
        .admit_diagnostic_authentication(credential)?;
    let mut request = request;
    request
        .extensions_mut()
        .insert(AuthenticatingPermit(Arc::new(permit)));
    Ok(next.run(request).await)
}

/// The pre-authentication permit, carried on the request so that
/// [`authenticate_middleware`] can give it back the moment the credential is
/// settled.
///
/// Holding it to the end of the response would make the share drain at the speed
/// of *answering*, not of authenticating, which is the opposite of what it is
/// for: sixteen slow readers would then close the in-memory share against the
/// static key, and the inner ceiling already bounds the answering.
#[derive(Clone)]
struct AuthenticatingPermit(Arc<crate::admission::DiagnosticPermit>);

impl AuthenticatingPermit {
    /// Give the permit back. Dropping the extension would do it too, but only
    /// once the request itself is dropped, which is the timing this exists to
    /// avoid.
    fn release(self) {
        drop(self.0);
    }
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
    if state
        .revision_report()
        .is_some_and(|report| report.active.is_none())
    {
        return (StatusCode::SERVICE_UNAVAILABLE, "unconverged");
    }
    match state.lifecycle().phase() {
        Phase::Serving => (StatusCode::OK, "ready"),
        Phase::Draining | Phase::Closing => (StatusCode::SERVICE_UNAVAILABLE, "draining"),
    }
}

fn convergence_refusal() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({
            "error": {
                "type": "inference_unavailable",
                "message": "the replica has no active projected revision",
            }
        })),
    )
        .into_response()
}

async fn convergence_middleware(
    State(state): State<AppState>,
    request: Request,
    next: Next,
) -> Response {
    if state
        .revision_report()
        .is_some_and(|report| report.active.is_none())
    {
        return convergence_refusal();
    }
    next.run(request).await
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
    // Deployment scope only, and a memory read either way: the catalogue summary
    // is what the background import last published, never a fetch or a query
    // performed for this request.
    let catalogue = state.catalogue_report();
    Json(view.project_with_catalogue(
        scope,
        state.lifecycle().phase(),
        revision.as_ref(),
        catalogue.as_ref(),
    ))
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

fn hex_digit(byte: u8) -> Option<u8> {
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
async fn list_models(
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
            .map(|row| row.data.as_slice())
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
    if credential.starts_with("axt1.") {
        return Err(GatewayError::Unauthorized);
    }
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
/// therefore cannot change what this request resolved. Invalid callers return
/// `401` first; a valid caller on a replica with no active projected revision
/// gets the typed `503` convergence refusal before the handler runs.
async fn authenticate_middleware(
    State((state, capability, authority)): State<(AppState, Option<Capability>, RouteAuthority)>,
    headers: HeaderMap,
    mut request: Request,
    next: Next,
) -> Result<Response, GatewayError> {
    let snapshot = state.config();
    let mut caller = authenticate(&snapshot, &headers).await?;
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
    // The path selects the namespace. Perform this intersection after inbound
    // authentication but before convergence disclosure or any handler
    // extractor: anonymous callers receive `401`, and an existing namespace
    // outside the grant is indistinguishable from an absent namespace.
    if authority == RouteAuthority::Namespaced {
        let path = request
            .extensions()
            .get::<OriginalUri>()
            .map_or_else(|| request.uri().path(), |original| original.path());
        let namespace = namespace_from_canonical_path(path)?;
        let grant = caller
            .namespace_grant()
            .map_err(|_| GatewayError::NamespaceNotAuthorized)?;
        let authorized = grant.permits(&namespace);
        let record = match state.store() {
            Some(store) => store
                .get_namespace(namespace.as_str())
                .await
                .map_err(GatewayError::from)?,
            None => None,
        };
        if !authorized {
            debug!(
                namespace = %namespace,
                subject = %caller.subject,
                signer_kid = ?caller.signer_kid,
                "namespace route denied"
            );
            return Err(GatewayError::NamespaceNotAuthorized);
        }
        let Some(record) = record else {
            return Err(GatewayError::UnknownNamespace);
        };

        // Downstream code reads one effective namespace from the caller
        // context. Replacing it here makes the path authoritative when a later
        // grant implementation permits a set or all namespaces. Attrs are
        // copied at admission so usage records carry the workspace metadata
        // Litvue stored (ADR 0063).
        caller.namespace = namespace.to_string();
        caller.attrs = Some(record.attrs.clone());
        request.extensions_mut().insert(namespace);
        request.extensions_mut().insert(record);
    }
    // Route capability is evaluated only after the canonical path has selected
    // the effective namespace. That ordering prevents an outside-grant path
    // from learning whether its requested wire is servable in the caller's
    // original namespace and prepares this boundary for set/all grants.
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
    // Keep the serving boundary here as well as in the route layer. The route
    // table currently adds `convergence_middleware` to every authenticated
    // inference route, but putting the invariant after successful
    // authentication means a future authenticated route cannot accidentally
    // serve the keyless stateful bootstrap by omitting that layer. Diagnostic
    // status is intentionally exempt: it is the operator's view of why the
    // replica is not ready, not inference traffic.
    if !matches!(capability, Some(Capability::Status))
        && state
            .revision_report()
            .is_some_and(|report| report.active.is_none())
    {
        request.extensions_mut().remove::<AuthenticatingPermit>();
        return Ok(convergence_refusal());
    }
    // Authentication is over, whatever it cost, so the permit that bounded it
    // goes back before the handler runs rather than after.
    if let Some(permit) = request.extensions_mut().remove::<AuthenticatingPermit>() {
        permit.release();
    }
    request.extensions_mut().insert(snapshot);
    request.extensions_mut().insert(caller);
    Ok(next.run(request).await)
}

/// Parse the raw namespace segment from the original URI. A nested axum router
/// may rewrite the active URI; accepting a decoded equivalent such as `%61cme`
/// would give one namespace several URL spellings and make routing ambiguous.
fn namespace_from_canonical_path(path: &str) -> Result<NamespaceId, GatewayError> {
    let rest = path
        .strip_prefix("/ns/")
        .or_else(|| path.strip_prefix("/namespaces/"))
        .ok_or(GatewayError::InvalidNamespace)?;
    let (namespace, suffix) = rest.split_once('/').ok_or(GatewayError::InvalidNamespace)?;
    if suffix.is_empty() {
        return Err(GatewayError::InvalidNamespace);
    }
    NamespaceId::parse(namespace).map_err(|_| GatewayError::InvalidNamespace)
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
    snapshot.config.provider.iter().any(|provider| {
        route.serves(provider.kind)
            && snapshot
                .credentials
                .is_present(&snapshot.config, namespace, &provider.id)
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
    fn middleware_surface(self) -> MiddlewareSurface {
        match self {
            Self::ChatCompletions => MiddlewareSurface::ChatCompletions,
            Self::NativeMessages => MiddlewareSurface::NativeMessages,
            Self::Embeddings => MiddlewareSurface::Embeddings,
            Self::Responses => MiddlewareSurface::Responses,
        }
    }

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

    fn buffered_response_route(self) -> Option<BufferedResponseRoute> {
        match self {
            Self::NativeMessages => Some(BufferedResponseRoute::Messages),
            Self::Responses => Some(BufferedResponseRoute::Responses),
            Self::ChatCompletions | Self::Embeddings => None,
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

/// Decide whether a streamed response is incremental or is held until the
/// upstream finishes. Any stream-event callback can refuse, so byte-faithful
/// routes need the opt-in even for validation-only middleware; mutation selects
/// reconstructed output while validation-only chains retain the original bytes.
/// Response-only middleware belongs to the non-streaming path.
fn stream_delivery(
    cfg: &Config,
    namespace: &str,
    route: Route,
    middleware: &MiddlewareChain,
) -> Result<StreamDelivery, GatewayError> {
    let ordinary = route.stream_delivery().ok_or_else(|| {
        GatewayError::BadRequest(format!("{} does not support streaming", route.label()))
    })?;
    if !middleware.has_scope(MiddlewareScope::StreamEvent) {
        return Ok(ordinary);
    }
    let mutates_response = middleware.has_response_mutator(MiddlewareScope::StreamEvent);
    let Some(policy_route) = route.buffered_response_route() else {
        // OpenAI-normalized streaming already re-emits decoded events and does
        // not need to revoke a byte-faithful contract to invoke middleware.
        return Ok(ordinary);
    };
    let enabled = cfg
        .namespace(namespace)
        .is_some_and(|namespace| namespace.buffered_response_routes().contains(&policy_route));
    if enabled {
        return Ok(if mutates_response {
            StreamDelivery::PolicyBuffered
        } else {
            StreamDelivery::PolicyValidatedPassthrough
        });
    }
    Err(GatewayError::MiddlewareResponseIncompatible {
        route: route.label(),
        framing: match route {
            Route::NativeMessages => "native",
            Route::Responses => "responses",
            Route::ChatCompletions | Route::Embeddings => "re-emitted",
        },
    })
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
    #[allow(dead_code)]
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
    record: Option<Extension<NamespaceRecord>>,
    body: Result<Json<Value>, JsonRejection>,
) -> Result<Response, GatewayError> {
    serve(
        state,
        headers,
        inbound_body(body)?,
        Route::ChatCompletions,
        snapshot,
        caller,
        record.map(|Extension(record)| record),
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
    record: Option<Extension<NamespaceRecord>>,
    body: Result<Json<Value>, JsonRejection>,
) -> Result<Response, GatewayError> {
    serve(
        state,
        headers,
        inbound_body(body)?,
        Route::NativeMessages,
        snapshot,
        caller,
        record.map(|Extension(record)| record),
    )
    .await
}

async fn embeddings(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(snapshot): Extension<Arc<ConfigSnapshot>>,
    Extension(caller): Extension<InboundKey>,
    record: Option<Extension<NamespaceRecord>>,
    body: Result<Json<Value>, JsonRejection>,
) -> Result<Response, GatewayError> {
    serve(
        state,
        headers,
        inbound_body(body)?,
        Route::Embeddings,
        snapshot,
        caller,
        record.map(|Extension(record)| record),
    )
    .await
}

async fn responses(
    State(state): State<AppState>,
    headers: HeaderMap,
    Extension(snapshot): Extension<Arc<ConfigSnapshot>>,
    Extension(caller): Extension<InboundKey>,
    record: Option<Extension<NamespaceRecord>>,
    body: Result<Json<Value>, JsonRejection>,
) -> Result<Response, GatewayError> {
    serve(
        state,
        headers,
        inbound_body(body)?,
        Route::Responses,
        snapshot,
        caller,
        record.map(|Extension(record)| record),
    )
    .await
}

/// The one request path every route shares: split `provider-id/model-id`,
/// hold a budget estimate, dispatch (credential-pool rotation, no alias
/// failover), then settle the hold and record exactly one usage record.
/// Routes differ only in the wire they speak — where the body goes upstream and
/// how usage is read back out (see [`Route`]).
async fn serve(
    state: AppState,
    headers: HeaderMap,
    body: Value,
    route: Route,
    snapshot: Arc<ConfigSnapshot>,
    caller: InboundKey,
    namespace: Option<NamespaceRecord>,
) -> Result<Response, GatewayError> {
    let cfg = &snapshot.config;
    let ns_blocklist = namespace
        .as_ref()
        .and_then(|record| record.blocklist.clone())
        .unwrap_or_default();
    let attrs = namespace.map(|record| record.attrs);

    route.validate_routing_controls(&body)?;

    let streamed = route.streamable() && body.get("stream").and_then(Value::as_bool) == Some(true);

    let alias = body
        .get("model")
        .and_then(Value::as_str)
        .ok_or_else(|| GatewayError::BadRequest("missing `model`".into()))?
        .to_string();

    if let Some(scope) = &caller.alias_scope
        && !alias_scope_permits(scope, &alias)
    {
        return Err(GatewayError::TokenForbidden(
            TokenVerificationError::AliasNotPermitted { alias },
        ));
    }

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
    let book_price = cfg.price_for(&provider.id, model_id);
    let request_price = match book_price {
        Some(rates) => RequestPrice::configured(rates),
        None => match provider.unpriced_models {
            UnpricedModels::Deny => return Err(GatewayError::UnpricedModel(alias)),
            UnpricedModels::Allow => RequestPrice::unpriced(),
        },
    };
    let routed = Model::single(
        provider.id.clone(),
        model_id.to_owned(),
        book_price.unwrap_or(UNPRICED_TARGET),
    );
    let model = &routed;

    #[cfg(not(test))]
    let middleware = snapshot.middleware(&caller.namespace);
    // Primitive tests can install a process-local override. Production has no
    // such field: its chain is always owned by the captured serving snapshot.
    #[cfg(test)]
    let middleware = if state.0.middleware.is_empty() {
        snapshot.middleware(&caller.namespace)
    } else {
        &state.0.middleware
    };
    // This is a pure snapshot decision and deliberately precedes permits,
    // reservations, usage identity, and provider dispatch. A missing opt-in is
    // a typed incompatibility, not work the gateway begins and later abandons.
    let stream_delivery = streamed
        .then(|| stream_delivery(cfg, &caller.namespace, route, middleware))
        .transpose()?;

    // The per-request bounds are checked before any dependency work and before
    // admission: they are pure functions of the parsed body, and a request that
    // cannot legally be served should not occupy capacity while it is refused.
    // Neither error repeats any part of the body.
    let estimate = route.estimate(&body);
    let limits = state.0.admission.limits();
    check_estimate_bounds(&body, estimate, limits)?;

    // What the observed target is charged at, resolved once from the snapshot
    // this request is already holding. A price book published while the request
    // is in flight replaces a later request's snapshot, never this one's.
    let prices = AliasPrices::single(request_price);
    // Responses affinity pins every request to the first target. A later priced
    // target therefore cannot make an unpriced pin chargeable, and must not be
    // used to size a budget hold for work this route can never send there.
    if wire.route.pins_affinity()
        && let Some((target, refusal)) = model.targets.first().zip(prices.ineligible(0))
    {
        tracing::warn!(
            model = %alias,
            detail = %refusal.detail(),
            "pinned target has no approved price"
        );
        if wire.route.is_continuation(&body) {
            return Err(GatewayError::ContinuationAffinityUnavailable {
                provider: target.provider.clone(),
                model: target.model.clone(),
            });
        }
        return Err(GatewayError::ModelNotPriced {
            alias,
            reason: refusal.reason().to_owned(),
        });
    }
    if let Some(refusal) = prices.refusal() {
        // The price book, its version, and its approval state answer the
        // operator's "approve what, where?" and are control-plane facts, so they
        // are logged rather than returned: the caller gets the stable reason.
        tracing::warn!(model = %alias, detail = %refusal.detail(), "alias has no approved price");
        return Err(GatewayError::ModelNotPriced {
            alias,
            reason: refusal.reason().to_owned(),
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

    // The migration gate retains the previous straight-line owner as an
    // immediate operational rollback. The default fixed-core path places the
    // permit directly in MiddlewareExecution, which follows this request into
    // buffered completion or streaming Accounting.
    let rate_limit_key = RateLimitKey {
        namespace: caller.namespace.clone(),
        subject: caller.subject.clone(),
    };
    let accounting_mode = cfg.core_middleware.accounting;
    let mut middleware_execution = middleware.execution(
        &state.0.middleware_runtime,
        Some(route.middleware_surface()),
    );
    let mut legacy_rate_limit_permit = match accounting_mode {
        CoreAccountingMode::Middleware => {
            middleware_execution
                .acquire_rate_limit(&state, &rate_limit_key)
                .await?;
            None
        }
        CoreAccountingMode::Legacy => Some(
            state
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
                })?,
        ),
    };

    // Content middleware is deliberately the first work after both permits
    // are held. It may be expensive, so running it before admission would let
    // authenticated callers turn it into an amplification surface. The
    // resulting body is the one sent to the provider and is the only body from
    // which the authoritative estimate-derived group below is computed.
    let mut middleware_request = ProviderRequest {
        model: alias.clone(),
        body,
    };
    #[cfg(not(test))]
    let middleware = snapshot.middleware(&caller.namespace);
    // Primitive tests can install a process-local override. Production has no
    // such field: its chain is always owned by the captured serving snapshot.
    #[cfg(test)]
    let middleware = if state.0.middleware.is_empty() {
        snapshot.middleware(&caller.namespace)
    } else {
        &state.0.middleware
    };
    let mut protected_values = wire
        .headers
        .iter()
        .map(|(name, value)| ((*name).to_owned(), value.clone()))
        .collect::<Vec<_>>();
    if let Some(previous_response_id) = middleware_request
        .body
        .get("previous_response_id")
        .and_then(Value::as_str)
    {
        protected_values.push((
            "previous_response_id".to_owned(),
            previous_response_id.to_owned(),
        ));
    }
    middleware_execution
        .request(&mut middleware_request, &protected_values)
        .await?;
    let body = middleware_request.body;
    // An empty chain is byte-neutral, so the pre-admission estimate is already
    // authoritative. Avoid serializing every request body a second time in the
    // default posture; only a configured chain can make the estimate differ.
    let estimate = if middleware.is_empty() {
        estimate
    } else {
        let (recomputed, body_bytes) = route.measure(&body);
        if body_bytes > limits.max_request_bytes {
            return Err(GatewayError::RequestTooLarge);
        }
        check_estimate_bounds(&body, recomputed, limits)?;
        recomputed
    };

    // Budget is denominated in micro-dollars. Hold a conservative cost estimate
    // from the post-middleware body before dispatch; settle the hold against the
    // real cost — priced at whichever target actually served — after. The first
    // estimate above remains only a cheap pre-admission fail-fast.
    let budget_key = BudgetKey {
        namespace: caller.namespace.clone(),
        subject: caller.subject.clone(),
    };
    let estimated_cost = prices
        .estimate()
        .and_then(|price| price.cost_microdollars(estimate))
        .unwrap_or(0);
    if let Some(ceiling) = caller.max_request_microdollars
        && estimated_cost > ceiling
    {
        return Err(GatewayError::RequestCostCeilingExceeded {
            alias: alias.clone(),
            estimated_microdollars: estimated_cost,
            ceiling_microdollars: ceiling,
        });
    }
    let reservation = match accounting_mode {
        CoreAccountingMode::Middleware => {
            middleware_execution
                .reserve_budget(
                    &state,
                    budget_key.clone(),
                    estimated_cost,
                    estimate.input_tokens,
                    &alias,
                )
                .await?;
            middleware_execution
                .core_budget_context()
                .expect("fixed budget middleware reserved a hold")
                .1
                .clone()
        }
        CoreAccountingMode::Legacy => {
            match state.0.budget.reserve(&budget_key, estimated_cost).await {
                Admission::Allowed(reservation) => reservation,
                Admission::Denied(Denial::Exceeded) => {
                    return Err(GatewayError::BudgetExceeded(alias));
                }
                Admission::Denied(Denial::StoreUnavailable) => {
                    return Err(GatewayError::BudgetUnavailable);
                }
            }
        }
    };
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
                prices: &prices,
                wire: &wire,
                identity,
                middleware_execution,
                delivery: stream_delivery.expect("streamed request has a delivery posture"),
                hold: BudgetHold {
                    key: budget_key,
                    reservation,
                    estimated_input_tokens: estimate.input_tokens,
                    permit: legacy_rate_limit_permit.take(),
                    admission: Some(admission_permit),
                },
            },
        )
        .await;
    }

    let mut reservation_guard = (accounting_mode == CoreAccountingMode::Legacy)
        .then(|| BudgetReservation::new(state.clone(), budget_key, reservation));
    let outcome = match dispatch_with_failover(
        &state, &snapshot, &caller, model, &prices, &body, &wire,
    )
    .await
    {
        Ok(outcome) => outcome,
        Err(err) => {
            // Nothing reached a provider, so nothing was consumed: the whole
            // estimate goes back rather than lingering until it expires.
            if !middleware_execution.release_core_budget().await {
                reservation_guard
                    .take()
                    .expect("legacy budget guard")
                    .release()
                    .await;
            }
            return Err(err);
        }
    };
    let served = &outcome.served;
    match outcome.result {
        Ok(mut response) => {
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
                price: served.price,
                latency_ms: outcome.latency_ms,
                ttft_ms: outcome.ttft_ms,
                attempts: outcome.attempts,
                attrs: attrs.clone(),
                period: period.clone(),
            });
            let accounting = match middleware_execution.take_core_budget() {
                Some(hold) => BufferedResponseAccounting::from_core(
                    state.clone(),
                    hold,
                    record,
                    ttft_ms,
                    attempts,
                ),
                None => reservation_guard
                    .take()
                    .expect("legacy budget guard")
                    .into_response_accounting(record, ttft_ms, attempts),
            };
            let mut middleware_result = middleware_execution.response(&mut response).await;
            // Upstream buffering enforced the same configured ceiling before
            // middleware ran. A response mutator can expand that bounded JSON,
            // so count the exact post-middleware serialization before any body
            // becomes caller-visible. Counting writes allocate no second body.
            if middleware_result.is_ok()
                && !json_fits_response_limit(
                    &response.body,
                    state.0.dispatcher.limits().max_response_bytes,
                )
            {
                middleware_result = Err(GatewayError::MiddlewareUnavailable);
            }
            accounting
                .finish(if middleware_result.is_ok() {
                    Status::Ok
                } else {
                    Status::Rejected
                })
                .await?;
            middleware_result?;
            Ok(attach_middleware_owner(
                Json(response.body).into_response(),
                middleware_execution,
            ))
        }
        Err(err) => {
            // The charging policy is "what was actually consumed" (ADR 0010),
            // and a buffered failure reports no usage at all: providers do not
            // return a usage block with an error, and nothing was relayed to
            // measure. Spend is therefore genuinely unknowable and charged as
            // zero — the streamed path, which can measure what it relayed,
            // charges its partial spend.
            if !middleware_execution.release_core_budget().await {
                reservation_guard
                    .take()
                    .expect("legacy budget guard")
                    .release()
                    .await;
            }
            // The upstream failure is what the caller is told about, so the
            // record is best-effort here: a `503` about the outbox would hide the
            // provider error that actually ended the request.
            record_usage_terminal(
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
                    cost_microdollars: Some(0),
                    price: served.price,
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

struct BoundedJsonCounter {
    bytes: u64,
    limit: u64,
}

impl std::io::Write for BoundedJsonCounter {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let bytes = u64::try_from(buffer.len())
            .map_err(|_| std::io::Error::other("serialized response length overflow"))?;
        let next = self
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| std::io::Error::other("serialized response length overflow"))?;
        if next > self.limit {
            return Err(std::io::Error::other(
                "serialized response exceeds configured limit",
            ));
        }
        self.bytes = next;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn json_fits_response_limit(body: &Value, limit: u64) -> bool {
    serde_json::to_writer(BoundedJsonCounter { bytes: 0, limit }, body).is_ok()
}

/// The target that produced the outcome (served it, or made the last attempt),
/// carried out of the failover walk so the caller can price and attribute it.
struct ServedTarget {
    provider: String,
    model: String,
    price: RequestPrice,
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
        .then(|| BudgetReservation::new(state.clone(), hold.key.clone(), hold.reservation.clone()));
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
struct StreamRequest<'a> {
    alias: String,
    body: Value,
    /// What each target is charged at under the snapshot the request started
    /// with, resolved before admission so the relay's settlement cannot depend on
    /// a price book published while the stream was open.
    prices: &'a AliasPrices,
    wire: &'a Wire,
    /// The identity of the usage event this request will settle as, minted at
    /// admission and cloned into every stream context the walk builds — including
    /// a credential rotation's — so a stream that rotates, ends, is cancelled, or
    /// never opens all report the same event.
    identity: EventIdentity,
    /// Pinned chain plus request-scope state, moved into the relay's
    /// response-lifetime accounting owner when a stream opens.
    middleware_execution: MiddlewareExecution,
    delivery: StreamDelivery,
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

    fn into_response_accounting(
        mut self,
        record: UsageRecord,
        ttft_ms: Option<u64>,
        attempts: u32,
    ) -> BufferedResponseAccounting {
        BufferedResponseAccounting {
            state: self.state.clone(),
            hold: Some(BufferedBudgetHold::Legacy {
                key: self.key.clone(),
                reservation: self
                    .reservation
                    .take()
                    .expect("budget reservation guard must be armed"),
            }),
            record: Some(record),
            ttft_ms,
            attempts,
        }
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

/// Owns known provider spend while buffered response middleware runs.
///
/// `client_cancelled` is recorded when this owner drops before middleware
/// produces a terminal outcome. Once middleware has returned, `finish` makes one durable
/// `ok`/`rejected` decision before any accounting await. That status describes
/// the request outcome, not an unknowable proof that the peer received the HTTP
/// response: changing it after an ambiguously acknowledged durable commit would
/// conflict with the immutable event under the same request identity.
struct BufferedResponseAccounting {
    state: AppState,
    hold: Option<BufferedBudgetHold>,
    record: Option<UsageRecord>,
    ttft_ms: Option<u64>,
    attempts: u32,
}

enum BufferedBudgetHold {
    Legacy {
        key: BudgetKey,
        reservation: Reservation,
    },
    Core(CoreBudgetHold),
}

impl BufferedResponseAccounting {
    fn from_core(
        state: AppState,
        hold: CoreBudgetHold,
        record: UsageRecord,
        ttft_ms: Option<u64>,
        attempts: u32,
    ) -> Self {
        Self {
            state,
            hold: Some(BufferedBudgetHold::Core(hold)),
            record: Some(record),
            ttft_ms,
            attempts,
        }
    }

    async fn finish(mut self, status: Status) -> Result<(), GatewayError> {
        let hold = self
            .hold
            .take()
            .expect("buffered response accounting must own its budget hold");
        let mut record = self
            .record
            .take()
            .expect("buffered response accounting must own its record");
        record.status = status;
        let decided = spawn_buffered_response_accounting(
            self.state.clone(),
            hold,
            record,
            self.ttft_ms,
            self.attempts,
        );
        match decided.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(GatewayError::UsageNotDurable {
                reason: error.reason,
            }),
            Err(_) => Err(GatewayError::UsageNotDurable {
                reason: "the durable append did not report before the runtime stopped",
            }),
        }
    }
}

impl Drop for BufferedResponseAccounting {
    fn drop(&mut self) {
        let Some(hold) = self.hold.take() else {
            return;
        };
        let mut record = self
            .record
            .take()
            .expect("armed buffered response accounting must own its record");
        record.status = Status::ClientCancelled;
        drop(spawn_buffered_response_accounting(
            self.state.clone(),
            hold,
            record,
            self.ttft_ms,
            self.attempts,
        ));
    }
}

fn spawn_buffered_response_accounting(
    state: AppState,
    hold: BufferedBudgetHold,
    record: UsageRecord,
    ttft_ms: Option<u64>,
    attempts: u32,
) -> tokio::sync::oneshot::Receiver<Result<(), crate::usage::NotDurable>> {
    let (verdict, decided) = tokio::sync::oneshot::channel();
    streaming::spawn_settlement(async move {
        match hold {
            BufferedBudgetHold::Legacy { key, reservation } => {
                state
                    .0
                    .budget
                    .settle(&key, &reservation, record.settle_cost())
                    .await;
            }
            BufferedBudgetHold::Core(hold) => {
                hold.settle(record.settle_cost()).await;
            }
        }
        telemetry::record_request(&record, ttft_ms, attempts);
        let result = state.0.usage.record(&record).await;
        if let Err(Err(unheard)) = verdict.send(result) {
            state.0.usage.count_unheard_refusal(&unheard);
        }
    });
    decided
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

/// The circuit-breaker key for a target: its qualified `provider/model`, so two
/// aliases pointing at the same concrete target share one breaker.
pub(crate) fn target_key(target: &Target) -> String {
    FailoverTarget::new(&target.provider, &target.model).qualified_model()
}

const UNPRICED_TARGET: ModelPrice = ModelPrice {
    input_microdollars_per_million: 0,
    output_microdollars_per_million: 0,
    reasoning_microdollars_per_million: None,
    cache_read_microdollars_per_million: None,
    cache_write_microdollars_per_million: None,
};

/// Split `provider-id/model-id` on the first `/`.
/// Gateway-key `alias_scope` matches the prefixed request id or the bare
/// upstream id, the same union blocklists use. A scope written as `gpt-4o` or
/// `gpt-*` still permits `openai/gpt-4o`.
fn alias_scope_permits(scope: &AliasScope, model: &str) -> bool {
    if scope.permits(model) {
        return true;
    }
    model
        .split_once('/')
        .is_some_and(|(_, bare)| !bare.is_empty() && scope.permits(bare))
}

fn split_model_id(model: &str) -> Result<(&str, &str), GatewayError> {
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

fn auth_scheme(kind: ProviderKind) -> AuthScheme {
    match kind {
        ProviderKind::Anthropic => AuthScheme::Header("x-api-key"),
        ProviderKind::Openai | ProviderKind::OpenaiCompatible => AuthScheme::Bearer,
    }
}

/// Attribute a failed attempt's timeout class to its span and the timeout
/// counter, and leave the operator the reason it failed. Only the bound reaches
/// the span — never the upstream URL, which the transport has already kept out
/// of the error.
fn note_attempt_failure(span: &tracing::Span, target: &Target, err: &TransportError) {
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
fn estimate_usage(body: &Value) -> (Usage, usize) {
    const DEFAULT_MAX_OUTPUT_TOKENS: u64 = 1_024;
    let body_bytes = serde_json::to_string(body).map(|s| s.len()).unwrap_or(0);
    let input_tokens = (body_bytes / 4) as u64;
    let output_tokens = requested_output_tokens(body).unwrap_or(DEFAULT_MAX_OUTPUT_TOKENS);
    (
        Usage {
            input_tokens,
            output_tokens,
            reasoning_tokens: 0,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
        },
        body_bytes,
    )
}

/// Apply the request-derived prompt/output ceilings to one estimate. This is
/// called twice by `serve`: once before admission as a cheap fail-fast for the
/// arriving body, and once after the middleware chain as the authoritative
/// check for the body that will actually be sent upstream.
fn check_estimate_bounds(
    body: &Value,
    estimate: Usage,
    limits: crate::admission::AdmissionLimits,
) -> Result<(), GatewayError> {
    if let Some(limit_tokens) = limits.max_prompt_tokens
        && estimate.input_tokens > limit_tokens
    {
        return Err(GatewayError::PromptTooLarge { limit_tokens });
    }
    if let Some(limit_tokens) = limits.max_output_tokens
        && let Some(requested_tokens) = requested_output_tokens(body)
        && requested_tokens > limit_tokens
    {
        return Err(GatewayError::OutputLimitExceeded {
            requested_tokens,
            limit_tokens,
        });
    }
    Ok(())
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
    cost_microdollars: Option<u64>,
    /// The pricing the cost was computed at, so the row names the immutable
    /// state it was charged against rather than "whatever is approved now".
    price: RequestPrice,
    latency_ms: u64,
    /// Time to the first token, when one was produced.
    ttft_ms: Option<u64>,
    /// Upstream attempts made; the retry count is one less.
    attempts: u32,
    attrs: Option<serde_json::Value>,
    period: Option<String>,
}

/// Record where the request is already ending for another reason, so a failure
/// to journal can only be reported and counted.
async fn record_usage_terminal(state: &AppState, args: RecordArgs<'_>) {
    let (record, ttft_ms, attempts) = build_record(args);
    telemetry::record_request(&record, ttft_ms, attempts);
    if !state.0.usage.appends() {
        state.0.usage.record_terminal(&record).await;
        return;
    }
    // Detached for the same reason [`record_usage`] is: the request this
    // describes already failed, so nothing here changes the response, but a
    // caller hanging up must not be what decides whether the attempt was
    // recorded. Awaited anyway while the handler lives, so an uncancelled
    // request still reaches its sinks before it answers.
    let (done, recorded) = tokio::sync::oneshot::channel();
    let recording = state.clone();
    crate::streaming::spawn_settlement(async move {
        recording.0.usage.record_terminal(&record).await;
        let _ = done.send(());
    });
    let _ = recorded.await;
}

fn build_record(args: RecordArgs<'_>) -> (UsageRecord, Option<u64>, u32) {
    let ttft_ms = args.ttft_ms;
    let attempts = args.attempts;
    let record = UsageRecord {
        schema_version: UsageRecord::SCHEMA_VERSION,
        request_id: args.identity.request_id.to_string(),
        trace_id: args.identity.trace_id.clone(),
        namespace: args.caller.namespace.clone(),
        attrs: args.attrs.clone().or_else(|| args.caller.attrs.clone()),
        period: args.period.clone(),
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
        catalog_version: args.price.catalog_version(),
        price_book: args.price.identity().map(|id| id.book()),
        price_book_checksum: args.price.identity().map(|id| id.checksum()),
        price_catalog: args.price.identity().map(|id| id.catalog()),
        latency_ms: args.latency_ms,
        attempts,
    };
    (record, ttft_ms, attempts)
}

/// Keep request-local content-middleware state alive until the caller finishes
/// or drops a buffered response body. The handler has already transformed the
/// JSON value, but the opaque owner follows the same response-lifetime contract
/// as streamed middleware state and admission capacity.
fn attach_middleware_owner(response: Response, owner: MiddlewareExecution) -> Response {
    let (parts, body) = response.into_parts();
    Response::from_parts(
        parts,
        Body::new(MiddlewareOwnedBody {
            inner: body,
            owner: Some(owner),
        }),
    )
}

/// Frame-transparent body ownership. In particular, wrapping middleware state
/// must not discard trailers or turn an exact-length JSON body into an
/// unknown-length stream.
struct MiddlewareOwnedBody {
    inner: Body,
    owner: Option<MiddlewareExecution>,
}

impl HttpBody for MiddlewareOwnedBody {
    type Data = bytes::Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let result = Pin::new(&mut self.inner).poll_frame(cx);
        if matches!(result, Poll::Ready(None)) {
            drop(self.owner.take());
        }
        result
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aliases::AliasScope;
    use crate::backends::catalog::ProviderId;
    use crate::budget::NoBudget;
    use crate::config::{Config, NamespacePolicy, ProjectIdentity, UndurablePolicy};
    use crate::convergence::status::testing::ManualClock;
    use crate::convergence::{Rejection, RevisionStatus, SnapshotSource};
    use crate::desired_state::fixtures::{
        approved_pricing_snapshot, policy_body, project_id, revision_id, tenant_id,
    };
    use crate::desired_state::policy::{
        ContentGuardrailRegistration, ContentMiddlewareRegistration, PolicyScope,
    };
    use crate::middleware::MiddlewareChain;
    use crate::pricing::PriceIdentity;
    use crate::principals::PrincipalAuthority;
    use crate::rate_limit::{InMemoryRateLimiter, NoLimit, RateLimitKey, RateLimiter};
    use crate::state::ReplicaObservability;
    use crate::status::registry::{CachedStatusRegistry, StatusRefresher, StatusSettings};
    use crate::status::{Component, ComponentObservation, ComponentState, StatusReason};
    use crate::usage::identity::RequestId;
    use crate::usage::journal::{self, UsageJournal as _};
    use crate::usage::{StdoutSink, UsageDelivery, UsageFanout, UsageSink};
    use axum::body::Body;
    use axum::http::{Method, Request, StatusCode};
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use gateway_core::{
        DeterministicGuardrail, GuardrailAction, GuardrailRule, Middleware, MiddlewareDeclaration,
        MiddlewareFailurePosture, MiddlewareOutcome, MiddlewarePhase, MiddlewareRefusal,
        MiddlewareResult, MiddlewareScope, MiddlewareState, MiddlewareStateBag,
        ProviderStreamEvent,
    };
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
    use tokio::sync::{Notify, oneshot};
    use tower::util::ServiceExt;
    use tracing_subscriber::layer::SubscriberExt;

    /// A usage row names the pricing it was charged against, so "what did we
    /// charge in March" is answerable from the row rather than from whatever is
    /// approved when the question is asked.
    #[test]
    fn a_usage_row_names_the_immutable_pricing_its_charge_was_computed_from() {
        let identity = EventIdentity {
            request_id: crate::usage::identity::next_request_id(),
            trace_id: None,
        };
        let caller = InboundKey {
            namespace: "platform".to_owned(),
            subject: "GW_TEST_INBOUND_KEY".to_owned(),
            authority: PrincipalAuthority::StaticKey,
            signer_kid: None,
            scope: None,
            alias_scope: None,
            max_request_microdollars: None,
            can_mint: false,
            jti: None,
            namespace_grant: None,
            attrs: None,
        };
        let args = |price| RecordArgs {
            identity: &identity,
            caller: &caller,
            alias: "fast",
            target_provider: "openai",
            target_model: "gpt-4o",
            source: CredentialSource::Platform,
            credential_id: "openai-primary",
            status: Status::Ok,
            input_tokens: 10,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
            output_tokens: 5,
            cost_microdollars: Some(40),
            price,
            latency_ms: 12,
            ttft_ms: None,
            attempts: 1,
            attrs: None,
            period: None,
        };

        let pricing = approved_pricing_snapshot();
        let approved = RequestPrice::approved(
            pricing
                .price(
                    &ProviderId::parse("openai").expect("a catalogue provider id"),
                    "gpt-4o",
                )
                .expect("the fixture book prices it"),
            PriceIdentity::of(&pricing),
        );
        let (row, _, _) = build_record(args(approved));
        assert_eq!(
            row.catalog_version,
            crate::desired_state::fixtures::catalog_version().get()
        );
        assert_ne!(row.catalog_version, pricing.book().version.get());
        assert_eq!(
            row.price_book.as_deref(),
            Some(pricing.book().to_string()).as_deref()
        );
        assert_eq!(
            row.price_book_checksum.as_deref(),
            Some(pricing.checksum().to_string()).as_deref()
        );
        assert_eq!(
            row.price_catalog.as_deref(),
            Some(pricing.catalog().to_string()).as_deref()
        );

        // A deployment priced by its file names no book, and its numeric column
        // keeps the value it has carried since the first schema.
        let (row, _, _) = build_record(args(RequestPrice::configured(
            gateway_core::catalog::ModelPrice {
                input_microdollars_per_million: 1,
                output_microdollars_per_million: 1,
                reasoning_microdollars_per_million: None,
                cache_read_microdollars_per_million: None,
                cache_write_microdollars_per_million: None,
            },
        )));
        assert_eq!(row.catalog_version, 0);
        assert_eq!(row.price_book, None);
        assert_eq!(row.price_book_checksum, None);
        assert_eq!(row.price_catalog, None);
    }

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
            alias_scope: None,
            max_request_microdollars: None,
            can_mint: false,
            jti: None,
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
            price: RequestPrice::configured(gateway_core::catalog::ModelPrice {
                input_microdollars_per_million: 1,
                output_microdollars_per_million: 1,
                reasoning_microdollars_per_million: None,
                cache_read_microdollars_per_million: None,
                cache_write_microdollars_per_million: None,
            }),
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

[gateway_token]
audience = "test-audience"

[[gateway_verifier]]
kid = "scope-test-kid"
alg = "HS256"
env = "JWT_SECRET"
namespaces = ["platform"]
max_ttl = "15m"

[[price]]
provider = "openai"
model = "*"
input_microdollars_per_million = 2500000
output_microdollars_per_million = 10000000
"#
        ))
        .unwrap();
        let mut env = env_with([("AXOND_PLATFORM_OPENAI", "sk-platform-test")]);
        env.insert(
            "JWT_SECRET".to_owned(),
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
        );
        (cfg, env)
    }

    fn stateful_test_state() -> AppState {
        let (mut config, env) = test_config_with_base_url("https://api.openai.com/v1");
        // The config was validated in its ordinary stateless source posture.
        // This test changes only the process authority after parsing so it can
        // exercise the route graph of a compiled stateful serving snapshot;
        // convergence tests own the separate bootstrap/projection validation.
        config.mode = crate::config::Mode::Stateful;
        let sinks: Vec<Box<dyn UsageSink>> = vec![Box::new(StdoutSink)];
        AppState::new_with_observability(
            config,
            &env,
            UsageFanout::new(sinks),
            Box::new(NoBudget),
            Box::new(NoLimit),
            Box::new(crate::revocation::NoDenylist),
            ReplicaObservability {
                status: observed_registry(),
                revision: Some(converged_replica()),
                catalogue: None,
            },
        )
        .expect("compiled stateful serving snapshot")
    }

    /// A deployment whose `gpt-4o` alias is bound to a catalogue offering the
    /// approved book does not price. Discovery still lists it; charging it is
    /// what the gateway refuses.
    fn state_bound_to_an_unpriced_offering() -> AppState {
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

[[price]]
provider = "openai"
model = "gpt-4o"
input_microdollars_per_million = 2500000
output_microdollars_per_million = 10000000
"#
        ))
        .expect("a catalogue binding parses");
        let sinks: Vec<Box<dyn UsageSink>> = vec![Box::new(StdoutSink)];
        let env = env_with([("AXOND_PLATFORM_OPENAI", "sk-platform-test")]);
        let state = AppState::new(
            cfg.clone(),
            &env,
            UsageFanout::new(sinks),
            Box::new(NoBudget),
        )
        .expect("credentials resolve");
        state
            .publish(
                ConfigSnapshot::build(cfg, &env, 1)
                    .expect("minting snapshot")
                    .with_pricing(approved_pricing_snapshot()),
            )
            .expect("publish");
        state
    }

    /// The acceptance criterion for unpriced models: a model the approved book
    /// does not price stays discoverable, but a request that would have to be
    /// charged for it is refused as a typed unavailability rather than served
    /// for free against an unapproved rate.
    #[tokio::test]
    #[ignore = "ADR 0063: catalogue-approved books superseded by deployment [[price]]"]
    async fn a_model_without_an_approved_price_is_discoverable_but_not_chargeable() {
        let state = state_bound_to_an_unpriced_offering();

        let listed = router(state.clone())
            .oneshot(
                Request::get("/ns/platform/v1/models")
                    .header(
                        axum::http::header::AUTHORIZATION,
                        format!("Bearer {CALLER_SECRET}"),
                    )
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("a response");
        assert_eq!(listed.status(), StatusCode::OK);
        let listed: Value = serde_json::from_slice(
            &listed
                .into_body()
                .collect()
                .await
                .expect("a body")
                .to_bytes(),
        )
        .expect("a catalogue document");
        assert_eq!(listed["data"][0]["id"], "gpt-4o");

        let refused = router(state)
            .oneshot(
                authorized("/v1/chat/completions")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "model": "openai/gpt-4o",
                            "messages": [{ "role": "user", "content": "hi" }]
                        }))
                        .expect("body"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("a response");
        assert_eq!(refused.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body: Value = serde_json::from_slice(
            &refused
                .into_body()
                .collect()
                .await
                .expect("a body")
                .to_bytes(),
        )
        .expect("an error document");
        assert_eq!(body["error"]["type"], "model_not_priced");

        // The refusal is a data-plane answer, so it says the model is not
        // chargeable and nothing about which book a deployment runs, at which
        // version, or whether that book is still a draft.
        let pricing = approved_pricing_snapshot();
        let message = body["error"]["message"]
            .as_str()
            .expect("an error message")
            .to_owned();
        assert!(message.contains("gpt-4o"), "{message}");
        for internal in [
            pricing.book().to_string(),
            pricing.book().id.to_string(),
            pricing.checksum().to_string(),
            pricing.catalog().to_string(),
            pricing.approval().state().to_owned(),
        ] {
            assert!(
                !message.contains(&internal),
                "the refusal `{message}` discloses `{internal}`"
            );
        }
    }

    /// A deployment whose alias leads with an unpriced catalogue-bound target and
    /// falls back to a file-priced one. Only the *first* target is reachable on
    /// the pinned Responses route, so the alias as a whole is chargeable while the
    /// pinned destination is not.
    fn state_pinned_to_an_unpriced_offering() -> AppState {
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

[[price]]
provider = "openai"
model = "gpt-4o"
input_microdollars_per_million = 2500000
output_microdollars_per_million = 10000000
[[price]]
provider = "openai"
model = "gpt-4o-mini"
input_microdollars_per_million = 2500000
output_microdollars_per_million = 10000000
"#
        ))
        .expect("a catalogue binding parses");
        let sinks: Vec<Box<dyn UsageSink>> = vec![Box::new(StdoutSink)];
        let env = env_with([("AXOND_PLATFORM_OPENAI", "sk-platform-test")]);
        let state = AppState::new(
            cfg.clone(),
            &env,
            UsageFanout::new(sinks),
            // The only route under test is pinned to the unpriced first target.
            // A zero budget proves the pricing refusal happens before a hold is
            // estimated from the later target that this route cannot attempt.
            Box::new(crate::budget::InMemoryBudget::new(0)),
        )
        .expect("credentials resolve");
        state
            .publish(
                ConfigSnapshot::build(cfg, &env, 1)
                    .expect("minting snapshot")
                    .with_pricing(approved_pricing_snapshot()),
            )
            .expect("publish");
        state
    }

    /// A pinned route cannot fail over past the target it is pinned to, so an
    /// unpriced pin is a refusal in its own right: it must be the documented
    /// typed `model_not_priced` unavailability, not the generic "nothing to
    /// attempt" request error a walk that dispatched nothing used to answer with,
    /// and it must disclose no more about the price book than the alias-wide
    /// refusal does.
    #[tokio::test]
    #[ignore = "ADR 0063: catalogue-approved books superseded by deployment [[price]]"]
    async fn a_pinned_target_without_an_approved_price_is_a_typed_pricing_refusal() {
        let state = state_pinned_to_an_unpriced_offering();
        let refused = router(state.clone())
            .oneshot(
                authorized("/v1/responses")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "model": "openai/gpt-4o",
                            "input": "hi"
                        }))
                        .expect("body"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("a response");

        assert_eq!(refused.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body: Value = serde_json::from_slice(
            &refused
                .into_body()
                .collect()
                .await
                .expect("a body")
                .to_bytes(),
        )
        .expect("an error document");
        assert_eq!(body["error"]["type"], "model_not_priced");

        let pricing = approved_pricing_snapshot();
        let message = body["error"]["message"]
            .as_str()
            .expect("an error message")
            .to_owned();
        assert!(message.contains("gpt-4o"), "{message}");
        for internal in [
            pricing.book().to_string(),
            pricing.book().id.to_string(),
            pricing.checksum().to_string(),
            pricing.catalog().to_string(),
            pricing.approval().state().to_owned(),
        ] {
            assert!(
                !message.contains(&internal),
                "the refusal `{message}` discloses `{internal}`"
            );
        }

        // Streaming does not get a different pricing boundary: an initial
        // request is still pinned to the unpriced first target and must be
        // refused before the stream is opened or a budget hold is taken.
        let streaming_initial = router(state.clone())
            .oneshot(
                authorized("/v1/responses")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "model": "openai/gpt-4o",
                            "input": "hi",
                            "stream": true
                        }))
                        .expect("body"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("a response");
        assert_eq!(streaming_initial.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body: Value = serde_json::from_slice(
            &streaming_initial
                .into_body()
                .collect()
                .await
                .expect("a body")
                .to_bytes(),
        )
        .expect("an error document");
        assert_eq!(body["error"]["type"], "model_not_priced");

        let continuation = router(state.clone())
            .oneshot(
                authorized("/v1/responses")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "model": "openai/gpt-4o",
                            "input": "hi",
                            "previous_response_id": "resp-from-unpriced-target"
                        }))
                        .expect("body"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("a response");
        assert_eq!(continuation.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body: Value = serde_json::from_slice(
            &continuation
                .into_body()
                .collect()
                .await
                .expect("a body")
                .to_bytes(),
        )
        .expect("an error document");
        assert_eq!(body["error"]["type"], "continuation_affinity_unavailable");

        let streaming = router(state)
            .oneshot(
                authorized("/v1/responses")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "model": "openai/gpt-4o",
                            "input": "hi",
                            "stream": true,
                            "previous_response_id": "resp-from-unpriced-target"
                        }))
                        .expect("body"),
                    ))
                    .expect("request"),
            )
            .await
            .expect("a response");
        assert_eq!(streaming.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body: Value = serde_json::from_slice(
            &streaming
                .into_body()
                .collect()
                .await
                .expect("a body")
                .to_bytes(),
        )
        .expect("an error document");
        assert_eq!(body["error"]["type"], "continuation_affinity_unavailable");
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
                Request::post("/ns/platform/v1/tokens")
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
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
    async fn minting_response_is_not_cacheable() {
        let response = router(minting_state())
            .oneshot(
                Request::post("/ns/platform/v1/tokens")
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
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
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
                Request::get("/ns/platform/v1/models")
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
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
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
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
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
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
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
        state
            .publish(ConfigSnapshot::build(config, &env, 0).expect("minting snapshot"))
            .expect("publish");

        let (status, body) =
            mint_request_with_credential(state.clone(), "static-key", json!({"sub": "agent"}))
                .await;
        assert_eq!(status, StatusCode::OK);
        let token = body["token"].as_str().expect("minted token");
        let response = scoped_route_request(state, "/v1/responses", token).await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
    async fn padded_audience_mints_a_token_the_gateway_accepts() {
        let state = minting_state_with_audience_epochs("  test-audience  ", "");
        let response = router(state.clone())
            .oneshot(
                Request::post("/ns/platform/v1/tokens")
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
                Request::get("/ns/platform/v1/models")
                    .header(axum::http::header::AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
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
        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let (status, body) = mint_request(state.clone(), json!({"sub": "near"})).await;
        let after = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert_eq!(status, StatusCode::OK);
        let token = body["token"].as_str().expect("minted token");
        let exp = body["exp"].as_u64().expect("expiry");
        let expires_in = body["expires_in"].as_u64().expect("remaining lifetime");
        let issued_at = exp.saturating_sub(expires_in);
        assert!(
            (before..=after).contains(&issued_at),
            "expires_in must describe an issuance time during the mint request: before={before}, issued_at={issued_at}, after={after}"
        );
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

[[price]]
provider = "chat"
model = "chat-model"
input_microdollars_per_million = 1
output_microdollars_per_million = 1

[[price]]
provider = "messages"
model = "messages-model"
input_microdollars_per_million = 1
output_microdollars_per_million = 1

[[price]]
provider = "embeddings"
model = "embeddings-model"
input_microdollars_per_million = 1
output_microdollars_per_million = 1

[[price]]
provider = "responses"
model = "responses-model"
input_microdollars_per_million = 1
output_microdollars_per_million = 1
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
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
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
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
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
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
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
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
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
                    .uri(ns_path(path))
                    .header("authorization", format!("Bearer {token}"))
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap()
    }

    #[tokio::test]
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
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
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
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
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
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
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
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
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
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
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
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
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
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
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
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
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
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
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
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
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
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
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
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
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
    async fn scoped_token_cannot_grant_a_route_the_namespace_lacks() {
        let body =
            serde_json::to_vec(&json!({ "model": "openai/gpt-4o", "messages": [] })).expect("body");
        let response = router(test_state())
            .oneshot(
                Request::post("/ns/platform/v1/messages")
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
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
    async fn scoped_token_requires_the_responses_capability() {
        let response = router(test_state())
            .oneshot(
                Request::post("/ns/platform/v1/responses")
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
        let paths: Vec<_> = route_specs(true)
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
                "/v1/tokens",
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
        for spec in route_specs(true)
            .into_iter()
            .filter(|spec| spec.namespace_scoped)
        {
            let method = if matches!(spec.path, "/v1/models" | "/v1/credentials") {
                Method::GET
            } else {
                Method::POST
            };
            let state = if spec.path == "/v1/tokens" {
                minting_state()
            } else {
                test_state()
            };
            let response = router(state)
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
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
    async fn stateful_serving_never_mounts_an_implicit_legacy_namespace() {
        let app = router(stateful_test_state());
        let authorized = |path: &'static str| {
            Request::get(path)
                .header(
                    axum::http::header::AUTHORIZATION,
                    format!("Bearer {CALLER_SECRET}"),
                )
                .body(Body::empty())
                .unwrap()
        };

        let legacy = app.clone().oneshot(authorized("/v1/models")).await.unwrap();
        assert_eq!(legacy.status(), StatusCode::NOT_FOUND);

        let canonical = app
            .oneshot(authorized("/namespaces/platform/v1/models"))
            .await
            .unwrap();
        assert_eq!(canonical.status(), StatusCode::OK);
    }

    #[tokio::test]
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
    async fn absent_and_outside_grant_namespaces_have_one_non_enumerating_response() {
        let app = router(status_state(ReplicaObservability {
            status: observed_registry(),
            revision: None,
            catalogue: None,
        }));
        let answer = |namespace: &'static str| {
            let app = app.clone();
            async move {
                let response = app
                    .oneshot(
                        Request::get(format!("/namespaces/{namespace}/v1/models"))
                            .header(
                                axum::http::header::AUTHORIZATION,
                                format!("Bearer {OPERATOR_KEY}"),
                            )
                            // A header cannot select or override a namespace.
                            .header("x-axond-namespace", "platform")
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                let status = response.status();
                let body = response.into_body().collect().await.unwrap().to_bytes();
                (status, body)
            }
        };

        // `tenant` exists in STATUS_CONFIG but is outside the operator key's
        // one-namespace inference grant. `ghost` does not exist at all.
        let outside_grant = answer("tenant").await;
        let absent = answer("ghost").await;
        assert_eq!(outside_grant, absent);
        assert_eq!(outside_grant.0, StatusCode::FORBIDDEN);
        assert_eq!(
            serde_json::from_slice::<Value>(&outside_grant.1).unwrap(),
            json!({
                "error": {
                    "type": "namespace_not_authorized",
                    "message": "the authenticated grant does not authorize the selected namespace"
                }
            })
        );
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
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
    async fn namespace_mismatch_precedes_body_parsing_and_convergence_disclosure() {
        let state = status_state(ReplicaObservability {
            status: observed_registry(),
            revision: Some(Arc::new(RevisionStatus::new(Box::new(
                crate::convergence::SystemClock,
            )))),
            catalogue: None,
        });
        let response = router(state)
            .oneshot(
                Request::post("/ns/tenant/v1/chat/completions")
                    .header(
                        axum::http::header::AUTHORIZATION,
                        format!("Bearer {OPERATOR_KEY}"),
                    )
                    .header("content-type", "application/json")
                    .body(Body::from("not-json"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["error"]["type"], "namespace_not_authorized");
    }

    #[tokio::test]
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
    async fn minting_route_is_absent_without_boot_minting_config() {
        for path in ["/v1/tokens", "/namespaces/platform/v1/tokens"] {
            let response = router(test_state())
                .oneshot(Request::post(path).body(Body::from("{}")).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
        }
    }

    #[tokio::test]
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
    async fn static_minting_key_mints_but_minted_token_cannot_mint() {
        let state = minting_state();
        let response = router(state.clone())
            .oneshot(
                Request::post("/ns/platform/v1/tokens")
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
                Request::post("/ns/platform/v1/tokens")
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
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
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
        state.publish(unauthorized).expect("publish");
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
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
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
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
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
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
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
        state
            .publish(ConfigSnapshot::build(config, &env, 1).unwrap())
            .expect("publish");
        let response = app
            .oneshot(
                Request::post("/ns/platform/v1/tokens")
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
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
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

[[price]]
provider = "openai"
model = "gpt-4o"
input_microdollars_per_million = 1000000
output_microdollars_per_million = 1000000
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
        let request = Request::post("/ns/platform/v1/chat/completions")
            .header("content-type", "application/json")
            .header("authorization", format!("Bearer {token}"))
            .body(Body::from(
                serde_json::to_vec(&json!({"model": "openai/gpt-4o", "messages": []}))
                    .expect("body"),
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
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
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
                Request::post("/ns/platform/v1/chat/completions")
                    .header("content-type", "application/json")
                    .header("authorization", format!("Bearer {token}"))
                    .body(Body::from(
                        serde_json::to_vec(&json!({"model": "openai/gpt-4o", "messages": []}))
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

    /// The legacy unconverged surface remains useful for a process that has no
    /// runtime snapshot at all; its response is a coarse serving refusal.
    #[tokio::test]
    async fn an_unconverged_replica_refuses_inference_without_pretending_to_be_ready() {
        let app = unconverged_router("no projected serving snapshot");

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
                    .is_some_and(|message| message.contains("projected")),
                "the refusal names the missing serving posture: {body}"
            );
        }
    }

    /// Convergence is an authenticated serving condition, not an anonymous
    /// disclosure channel. A stateful bootstrap has a revision gate but no
    /// inbound keys, so an anonymous caller must see the auth refusal first.
    #[tokio::test]
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
    async fn an_unconverged_stateful_route_authenticates_before_reporting_convergence() {
        let config = Config::from_toml_str(
            "mode = \"stateful\"\n\
             [control_plane]\ndsn_env = \"GW_CONTROL_PLANE_DSN\"\n\
             [secret_store]\nkek_env = \"GW_KEK\"\n\
             [[admin_breakglass]]\nenv = \"GW_BREAKGLASS\"\n",
        )
        .expect("valid stateful config");
        let state = AppState::new_with_observability(
            config,
            &HashMap::new(),
            UsageFanout::new(vec![Box::new(StdoutSink)]),
            Box::new(NoBudget),
            Box::new(NoLimit),
            Box::new(crate::revocation::NoDenylist),
            ReplicaObservability {
                status: observed_registry(),
                revision: Some(Arc::new(crate::convergence::RevisionStatus::new(Box::new(
                    crate::convergence::SystemClock,
                )))),
                catalogue: None,
            },
        )
        .expect("bootstrap state");
        let response = router(state)
            .oneshot(
                Request::get("/ns/platform/v1/models")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    /// The defense-in-depth gate is still typed after a valid caller has
    /// authenticated. This uses the production route table with a configured
    /// key store and no active revision, so a future route that forgets to add
    /// the separate convergence layer cannot serve around the boundary.
    #[tokio::test]
    async fn an_authenticated_unconverged_route_reports_convergence_after_authentication() {
        let state = status_state(ReplicaObservability {
            status: observed_registry(),
            revision: Some(Arc::new(crate::convergence::RevisionStatus::new(Box::new(
                crate::convergence::SystemClock,
            )))),
            catalogue: None,
        });
        let response = router(state)
            .oneshot(
                Request::get("/ns/platform/v1/models")
                    .header(
                        axum::http::header::AUTHORIZATION,
                        format!("Bearer {OPERATOR_KEY}"),
                    )
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body: Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .expect("typed convergence refusal");
        assert_eq!(body["error"]["type"], "inference_unavailable", "{body}");
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

    /// A caller sees only the aliases it could invoke: a BYOK namespace with no
    /// credential for the target's provider (and no platform fallback) gets an
    /// empty list, so it cannot enumerate aliases it is not entitled to, while
    /// the platform namespace — which does hold the credential — sees the alias.
    #[tokio::test]
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
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

[[price]]
provider = "openai"
model = "gpt-4o"
input_microdollars_per_million = 1
output_microdollars_per_million = 1
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

    /// An alias a namespace owns is that namespace's alone: another namespace
    /// neither lists it nor can invoke it, and the owner's row shadows the
    /// deployment-wide one of the same name (ADR 0058).
    ///
    /// The isolation asserted here is the catalogue and the resolution, which is
    /// where a leak would be observable: a caller that cannot name an alias cannot
    /// reach the upstream behind it, so no provider call is needed to characterise
    /// it.
    #[tokio::test]
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
    async fn an_owned_alias_is_listed_and_routable_only_by_its_namespace() {
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

[[credential]]
namespace = "acme"
provider = "openai"
env = "K_ACME"

[[gateway_key]]
env = "GK_PLATFORM"
namespace = "platform"

[[gateway_key]]
env = "GK_ACME"
namespace = "acme"

[[price]]
provider = "openai"
model = "gpt-4o"
input_microdollars_per_million = 1
output_microdollars_per_million = 1

[[price]]
provider = "openai"
model = "gpt-4o-mini"
input_microdollars_per_million = 1
output_microdollars_per_million = 1

[[price]]
provider = "openai"
model = "o3"
input_microdollars_per_million = 1
output_microdollars_per_million = 1
"#,
        )
        .unwrap();
        let env: HashMap<String, String> = [
            ("K_PLATFORM", "sk-platform"),
            ("K_ACME", "sk-acme"),
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

        let ids = |catalogue: &Value| {
            catalogue["data"]
                .as_array()
                .expect("a catalogue")
                .iter()
                .map(|entry| entry["id"].as_str().expect("an id").to_owned())
                .collect::<Vec<_>>()
        };

        // The owner lists its own aliases once each: the shadowed deployment-wide
        // `shared` is not a second entry.
        let mut acme = ids(&models_for(&state, "acme-key").await);
        acme.sort();
        assert_eq!(acme, ["private", "shared"]);

        // The other namespace sees no evidence that `private` exists.
        assert_eq!(ids(&models_for(&state, "plat-key").await), ["shared"]);

        // Nor can it name it: an alias another namespace owns is an unknown model,
        // not a forbidden one, because a refusal that distinguishes the two is a
        // way to enumerate what a tenant has enabled.
        let snapshot = state.config();
        assert!(snapshot.config.model_for("platform", "private").is_none());
        assert_eq!(
            snapshot
                .config
                .model_for("acme", "shared")
                .expect("acme's own")
                .targets[0]
                .model,
            "gpt-4o-mini"
        );

        let body = serde_json::to_vec(&json!({"model": "private", "messages": []})).unwrap();
        let resp = router(state.clone())
            .oneshot(
                Request::post("/ns/platform/v1/chat/completions")
                    .header(
                        axum::http::header::AUTHORIZATION,
                        format!("Bearer {}", "plat-key"),
                    )
                    .header(axum::http::header::CONTENT_TYPE, "application/json")
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

    /// The `/v1/models` body a caller presenting `secret` receives.
    async fn models_for(state: &AppState, secret: &str) -> Value {
        let resp = router(state.clone())
            .oneshot(
                Request::get("/ns/platform/v1/models")
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
                    namespace_grant: None,
                    attrs: None,
                },
                None,
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
    async fn alias_scope_matches_prefixed_and_bare_model_ids() {
        let (base_url, hits) = controllable_upstream(
            Arc::new(AtomicBool::new(false)),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
        let state = test_state_with_base_url(&base_url);
        let scoped = |model: &str, scope: &str| {
            let state = state.clone();
            let model = model.to_owned();
            let scope = scope.to_owned();
            async move {
                serve(
                    state.clone(),
                    HeaderMap::new(),
                    json!({"model": model, "messages": []}),
                    Route::ChatCompletions,
                    state.config(),
                    InboundKey {
                        namespace: "platform".to_owned(),
                        subject: "restricted".to_owned(),
                        authority: PrincipalAuthority::MintedToken,
                        signer_kid: Some("test-kid".to_owned()),
                        scope: None,
                        alias_scope: Some(AliasScope::parse([scope]).unwrap()),
                        max_request_microdollars: None,
                        can_mint: false,
                        jti: None,
                        namespace_grant: None,
                        attrs: None,
                    },
                    None,
                )
                .await
                .unwrap_or_else(|error| error.into_response())
            }
        };

        for scope in ["gpt-4o", "gpt-*"] {
            let response = scoped("openai/gpt-4o", scope).await;
            let bytes = response.into_body().collect().await.unwrap().to_bytes();
            let json: Value = serde_json::from_slice(&bytes).unwrap();
            assert_ne!(
                json["error"]["type"], "token_alias_not_permitted",
                "scope `{scope}` should permit openai/gpt-4o: {json}"
            );
        }
        assert!(hits.load(Ordering::SeqCst) > 0);

        let denied = scoped("openai/other", "gpt-4o").await;
        assert_eq!(denied.status(), StatusCode::FORBIDDEN);
        let bytes = denied.into_body().collect().await.unwrap().to_bytes();
        let json: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["error"]["type"], "token_alias_not_permitted");
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

    /// A provider probe whose reported input usage is derived from the body it
    /// receives. This makes the settled usage row evidence about the mutated
    /// request rather than about the pre-middleware fixture.
    async fn body_measuring_upstream() -> String {
        let app = Router::new().route(
            "/chat/completions",
            post(|axum::Json(body): axum::Json<Value>| async move {
                let input_tokens = serde_json::to_string(&body).unwrap().len() as u64 / 4;
                Json(json!({
                    "id": "body-measured",
                    "choices": [],
                    "usage": { "prompt_tokens": input_tokens, "completion_tokens": 1 }
                }))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}")
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
                generation: None,
                period: None,
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
                generation: None,
                period: None,
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

    /// A request-scope middleware used by #356 tests. It changes only content,
    /// leaving routing fields alone, and therefore gives the request path a
    /// deterministic post-middleware body whose estimate can be asserted.
    struct BodyGrowthMiddleware {
        declaration: MiddlewareDeclaration,
        padding_bytes: usize,
        output_tokens: Option<u64>,
    }

    struct BlockingResponseMiddleware {
        declaration: MiddlewareDeclaration,
        active: Arc<AtomicUsize>,
        release: Arc<AtomicBool>,
    }

    struct StreamMarkerMiddleware {
        declaration: MiddlewareDeclaration,
    }

    struct StreamValidationMiddleware {
        declaration: MiddlewareDeclaration,
        refuse_on_text: bool,
    }

    struct StreamTextObserver {
        declaration: MiddlewareDeclaration,
        observed: Arc<Mutex<Vec<String>>>,
        release_failure: Arc<Notify>,
        state_drops: Arc<AtomicUsize>,
    }

    struct StreamObserverState(Arc<AtomicUsize>);

    impl Drop for StreamObserverState {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn buffered_middleware_state_lives_until_response_body_completion_or_drop() {
        fn owned_response(drops: Arc<AtomicUsize>) -> Response {
            let mut states = MiddlewareStateBag::new(1);
            states.insert(
                0,
                MiddlewareState::new(StreamObserverState(Arc::clone(&drops))),
            );
            attach_middleware_owner(
                Json(json!({"ok": true})).into_response(),
                MiddlewareExecution::from_state_bag_for_test(states),
            )
        }

        let completed = Arc::new(AtomicUsize::new(0));
        let response = owned_response(Arc::clone(&completed));
        assert_eq!(completed.load(Ordering::SeqCst), 0);
        assert_eq!(response.body().size_hint().exact(), Some(11));
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body, bytes::Bytes::from_static(br#"{"ok":true}"#));
        assert_eq!(completed.load(Ordering::SeqCst), 1);

        let cancelled = Arc::new(AtomicUsize::new(0));
        let response = owned_response(Arc::clone(&cancelled));
        assert_eq!(cancelled.load(Ordering::SeqCst), 0);
        drop(response);
        assert_eq!(cancelled.load(Ordering::SeqCst), 1);

        let framed = Arc::new(AtomicUsize::new(0));
        let mut trailers = HeaderMap::new();
        trailers.insert("x-axond-trailer", HeaderValue::from_static("preserved"));
        let frames = futures::stream::iter([
            Ok::<_, std::convert::Infallible>(http_body::Frame::data(bytes::Bytes::from_static(
                b"payload",
            ))),
            Ok(http_body::Frame::trailers(trailers)),
        ]);
        let mut states = MiddlewareStateBag::new(1);
        states.insert(
            0,
            MiddlewareState::new(StreamObserverState(Arc::clone(&framed))),
        );
        let response = attach_middleware_owner(
            Response::new(Body::new(http_body_util::StreamBody::new(frames))),
            MiddlewareExecution::from_state_bag_for_test(states),
        );
        let mut body = response.into_body();
        let data = body
            .frame()
            .await
            .expect("data frame")
            .unwrap()
            .into_data()
            .expect("first frame is data");
        assert_eq!(data, bytes::Bytes::from_static(b"payload"));
        let trailers = body
            .frame()
            .await
            .expect("trailer frame")
            .unwrap()
            .into_trailers()
            .expect("second frame is trailers");
        assert_eq!(trailers["x-axond-trailer"], "preserved");
        assert_eq!(framed.load(Ordering::SeqCst), 0);
        assert!(body.frame().await.is_none());
        assert_eq!(framed.load(Ordering::SeqCst), 1);
    }

    impl BlockingResponseMiddleware {
        fn chain(active: Arc<AtomicUsize>, release: Arc<AtomicBool>) -> MiddlewareChain {
            let mut declaration =
                MiddlewareDeclaration::new("test.blocking-response", [MiddlewareScope::Response]);
            declaration.max_duration = Duration::from_secs(5);
            MiddlewareChain::new(vec![Arc::new(Self {
                declaration,
                active,
                release,
            })])
            .expect("blocking response chain")
        }
    }

    impl Middleware for BlockingResponseMiddleware {
        fn declaration(&self) -> &MiddlewareDeclaration {
            &self.declaration
        }

        fn apply(
            &self,
            phase: MiddlewarePhase<'_>,
            _state: Option<&mut gateway_core::MiddlewareState>,
        ) -> MiddlewareResult {
            if matches!(phase, MiddlewarePhase::Response(_)) {
                self.active.fetch_add(1, Ordering::SeqCst);
                for _ in 0..2_000 {
                    if self.release.load(Ordering::Acquire) {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(1));
                }
                self.active.fetch_sub(1, Ordering::SeqCst);
            }
            Ok(MiddlewareOutcome::continue_without_state())
        }
    }

    impl StreamMarkerMiddleware {
        fn chain() -> MiddlewareChain {
            let mut declaration =
                MiddlewareDeclaration::new("test.stream-marker", [MiddlewareScope::StreamEvent]);
            declaration.mutates_response = true;
            MiddlewareChain::new(vec![Arc::new(Self { declaration }) as Arc<dyn Middleware>])
                .expect("stream marker chain")
        }
    }

    fn guardrail(action: GuardrailAction, pattern: &str) -> Arc<dyn Middleware> {
        let mut declaration = MiddlewareDeclaration::new(
            "axond.redact",
            [
                MiddlewareScope::Request,
                MiddlewareScope::Response,
                MiddlewareScope::StreamEvent,
            ],
        );
        declaration.failure_posture = MiddlewareFailurePosture::FailClosed;
        declaration.max_duration = Duration::from_secs(1);
        declaration.mutates_response = action == GuardrailAction::Redact;
        let guardrail = DeterministicGuardrail::compile(
            declaration,
            &[7_u8; 32],
            &[GuardrailRule {
                id: "test-rule".to_owned(),
                pattern: pattern.to_owned(),
                action,
            }],
        )
        .expect("test guardrail compiles");
        Arc::new(guardrail)
    }

    fn guardrail_chain(action: GuardrailAction, pattern: &str) -> MiddlewareChain {
        MiddlewareChain::new(vec![guardrail(action, pattern)]).expect("guardrail chain")
    }

    fn observed_guardrail_chain(
        pattern: &str,
        observed: Arc<Mutex<Vec<String>>>,
        release_failure: Arc<Notify>,
        state_drops: Arc<AtomicUsize>,
    ) -> MiddlewareChain {
        let mut observer_declaration = MiddlewareDeclaration::new(
            "test.stream-text-observer",
            [MiddlewareScope::Request, MiddlewareScope::StreamEvent],
        );
        observer_declaration.max_duration = Duration::from_secs(1);
        let observer = Arc::new(StreamTextObserver {
            declaration: observer_declaration,
            observed,
            release_failure,
            state_drops,
        }) as Arc<dyn Middleware>;
        // Stream callbacks run in reverse registration order. Register the
        // observer first so it sees the event only after axond.redact has
        // consumed the generated-token prefix into request-local carry.
        MiddlewareChain::new(vec![observer, guardrail(GuardrailAction::Redact, pattern)])
            .expect("observed guardrail chain")
    }

    #[tokio::test]
    async fn cancelling_response_middleware_still_settles_provider_spend_and_usage() {
        let (base_url, hits) = controllable_upstream(
            Arc::new(AtomicBool::new(true)),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
        let captured = CapturingSink::default();
        let budget = RecordingBudget::default();
        let active = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(AtomicBool::new(false));
        let state = two_target_state_with_budget(
            &base_url,
            &base_url,
            "",
            captured.clone(),
            Box::new(budget.clone()),
        )
        .with_middleware_chain(BlockingResponseMiddleware::chain(
            Arc::clone(&active),
            Arc::clone(&release),
        ));

        let request = tokio::spawn(async move { router(state).oneshot(chat_request()).await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while active.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("response middleware starts after provider success");
        request.abort();
        assert!(
            request
                .await
                .expect_err("request future is cancelled")
                .is_cancelled()
        );
        release.store(true, Ordering::Release);
        crate::streaming::await_settlements(Duration::from_secs(2)).await;

        assert_eq!(hits.load(Ordering::SeqCst), 1);
        let records = captured.0.lock().expect("records");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].status, Status::ClientCancelled);
        assert_eq!(records[0].input_tokens, 10);
        assert_eq!(records[0].output_tokens, 5);
        assert_eq!(records[0].cost_microdollars, Some(15));
        drop(records);
        let reservations = budget.0.lock().expect("budget");
        assert_eq!(reservations.len(), 1);
        assert_eq!(reservations[0].1, 15);
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
        crate::streaming::await_settlements(Duration::from_secs(2)).await;

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

    impl Middleware for StreamMarkerMiddleware {
        fn declaration(&self) -> &MiddlewareDeclaration {
            &self.declaration
        }

        fn apply(
            &self,
            phase: MiddlewarePhase<'_>,
            _state: Option<&mut gateway_core::MiddlewareState>,
        ) -> MiddlewareResult {
            if let MiddlewarePhase::StreamEvent(ProviderStreamEvent::Data { data, .. }) = phase {
                data["middleware_marker"] = json!(true);
            }
            Ok(MiddlewareOutcome::continue_without_state())
        }
    }

    impl StreamValidationMiddleware {
        fn chain(refuse_on_text: bool) -> MiddlewareChain {
            let declaration = MiddlewareDeclaration::new(
                "test.stream-validation",
                [MiddlewareScope::StreamEvent],
            );
            MiddlewareChain::new(vec![Arc::new(Self {
                declaration,
                refuse_on_text,
            }) as Arc<dyn Middleware>])
            .expect("stream validation chain")
        }
    }

    impl Middleware for StreamValidationMiddleware {
        fn declaration(&self) -> &MiddlewareDeclaration {
            &self.declaration
        }

        fn apply(
            &self,
            phase: MiddlewarePhase<'_>,
            _state: Option<&mut gateway_core::MiddlewareState>,
        ) -> MiddlewareResult {
            if self.refuse_on_text
                && let MiddlewarePhase::StreamEvent(ProviderStreamEvent::Data { data, .. }) = phase
                && (data.pointer("/delta/text").and_then(Value::as_str) == Some("hi")
                    || data.pointer("/delta").and_then(Value::as_str) == Some("hi"))
            {
                return Ok(MiddlewareOutcome::refuse(MiddlewareRefusal::Policy));
            }
            Ok(MiddlewareOutcome::continue_without_state())
        }
    }

    impl Middleware for StreamTextObserver {
        fn declaration(&self) -> &MiddlewareDeclaration {
            &self.declaration
        }

        fn apply(
            &self,
            phase: MiddlewarePhase<'_>,
            _state: Option<&mut gateway_core::MiddlewareState>,
        ) -> MiddlewareResult {
            if matches!(&phase, MiddlewarePhase::Request(_)) {
                return Ok(MiddlewareOutcome::continue_with_state(
                    gateway_core::MiddlewareState::new(StreamObserverState(Arc::clone(
                        &self.state_drops,
                    ))),
                ));
            }
            if let MiddlewarePhase::StreamEvent(ProviderStreamEvent::Data { data, .. }) = phase
                && let Some(text) = data.pointer("/delta/text").and_then(Value::as_str)
            {
                self.observed.lock().unwrap().push(text.to_owned());
                self.release_failure.notify_one();
            }
            Ok(MiddlewareOutcome::continue_without_state())
        }
    }

    fn enable_buffered_response_routes(
        config: &mut Config,
        routes: impl IntoIterator<Item = BufferedResponseRoute>,
    ) {
        let body = policy_body(PolicyScope::Tenant(tenant_id(1)), 1)
            .with_buffered_response_routes(routes)
            .expect("valid buffered response routes");
        let generation = body.generation(revision_id(1));
        config.namespace[0].policy = Some(NamespacePolicy { body, generation });
    }

    #[test]
    fn response_mutation_selects_buffering_only_for_explicit_byte_faithful_routes() {
        let config = test_state().config().config.clone();
        let chain = StreamMarkerMiddleware::chain();

        assert!(matches!(
            stream_delivery(&config, "platform", Route::NativeMessages, &chain),
            Err(GatewayError::MiddlewareResponseIncompatible {
                route: "/v1/messages",
                framing: "native"
            })
        ));
        assert!(matches!(
            stream_delivery(&config, "platform", Route::Responses, &chain),
            Err(GatewayError::MiddlewareResponseIncompatible {
                route: "/v1/responses",
                framing: "responses"
            })
        ));
        assert_eq!(
            stream_delivery(&config, "platform", Route::ChatCompletions, &chain).unwrap(),
            StreamDelivery::Reemit
        );
        assert!(matches!(
            stream_delivery(&config, "platform", Route::Embeddings, &chain),
            Err(GatewayError::BadRequest(message))
                if message == "/v1/embeddings does not support streaming"
        ));

        let mut selected = config;
        enable_buffered_response_routes(
            &mut selected,
            [
                BufferedResponseRoute::Messages,
                BufferedResponseRoute::Responses,
            ],
        );
        assert_eq!(
            stream_delivery(&selected, "platform", Route::NativeMessages, &chain).unwrap(),
            StreamDelivery::PolicyBuffered
        );
        assert_eq!(
            stream_delivery(&selected, "platform", Route::Responses, &chain).unwrap(),
            StreamDelivery::PolicyBuffered
        );
        assert_eq!(
            stream_delivery(
                &selected,
                "platform",
                Route::NativeMessages,
                &MiddlewareChain::empty(),
            )
            .unwrap(),
            StreamDelivery::Passthrough,
            "policy permission alone must not revoke byte-faithful passthrough"
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

    #[test]
    fn post_middleware_buffered_response_uses_the_exact_serialized_byte_budget() {
        let body = json!({
            "choices": [{"message": {"content": "restored caller text"}}]
        });
        let exact = u64::try_from(serde_json::to_vec(&body).unwrap().len()).unwrap();
        assert!(json_fits_response_limit(&body, exact));
        assert!(!json_fits_response_limit(&body, exact - 1));
    }

    #[test]
    fn deterministic_guardrail_obeys_native_and_responses_buffering_opt_in() {
        let mut config = test_state().config().config.clone();
        let chain = guardrail_chain(GuardrailAction::Redact, "secret");
        let validation_only = guardrail_chain(GuardrailAction::Block, "forbidden");
        assert_eq!(
            stream_delivery(
                &config,
                "platform",
                Route::ChatCompletions,
                &validation_only,
            )
            .unwrap(),
            StreamDelivery::Reemit,
            "block-only OpenAI middleware remains incremental and nonmutating"
        );
        assert_eq!(
            stream_delivery(&config, "platform", Route::ChatCompletions, &chain).unwrap(),
            StreamDelivery::Reemit,
            "mutating OpenAI middleware re-emits decoded events incrementally"
        );
        assert!(matches!(
            stream_delivery(&config, "platform", Route::NativeMessages, &chain),
            Err(GatewayError::MiddlewareResponseIncompatible {
                route: "/v1/messages",
                framing: "native"
            })
        ));
        assert!(matches!(
            stream_delivery(&config, "platform", Route::Responses, &chain),
            Err(GatewayError::MiddlewareResponseIncompatible {
                route: "/v1/responses",
                framing: "responses"
            })
        ));
        assert!(matches!(
            stream_delivery(&config, "platform", Route::NativeMessages, &validation_only,),
            Err(GatewayError::MiddlewareResponseIncompatible {
                route: "/v1/messages",
                framing: "native"
            })
        ));
        assert!(matches!(
            stream_delivery(&config, "platform", Route::Responses, &validation_only),
            Err(GatewayError::MiddlewareResponseIncompatible {
                route: "/v1/responses",
                framing: "responses"
            })
        ));
        enable_buffered_response_routes(
            &mut config,
            [
                BufferedResponseRoute::Messages,
                BufferedResponseRoute::Responses,
            ],
        );
        assert_eq!(
            stream_delivery(&config, "platform", Route::NativeMessages, &chain).unwrap(),
            StreamDelivery::PolicyBuffered
        );
        assert_eq!(
            stream_delivery(&config, "platform", Route::Responses, &chain).unwrap(),
            StreamDelivery::PolicyBuffered
        );

        assert_eq!(
            stream_delivery(&config, "platform", Route::NativeMessages, &validation_only,).unwrap(),
            StreamDelivery::PolicyValidatedPassthrough
        );
        assert_eq!(
            stream_delivery(&config, "platform", Route::Responses, &validation_only,).unwrap(),
            StreamDelivery::PolicyValidatedPassthrough
        );
    }

    const MUTABLE_NATIVE_STREAM: &str = concat!(
        "event: message_start\n",
        "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"usage\":{\"input_tokens\":3}}}\n\n",
        "event: content_block_start\n",
        "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
        "event: content_block_delta\n",
        "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n",
        "event: content_block_stop\n",
        "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
        "event: message_delta\n",
        "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\n",
        "event: message_stop\n",
        "data: {\"type\":\"message_stop\"}\n\n",
    );

    async fn mutable_native_upstream() -> (String, Arc<AtomicUsize>) {
        let hits = Arc::new(AtomicUsize::new(0));
        let served = Arc::clone(&hits);
        let app = Router::new().route(
            "/messages",
            post(move || {
                let served = Arc::clone(&served);
                async move {
                    served.fetch_add(1, Ordering::SeqCst);
                    let split = MUTABLE_NATIVE_STREAM
                        .find("\"hi\"")
                        .expect("native fixture contains text delta")
                        + 2;
                    let bytes = MUTABLE_NATIVE_STREAM.as_bytes();
                    (
                        [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                        Body::from_stream(futures::stream::iter([
                            Ok::<_, std::convert::Infallible>(bytes::Bytes::copy_from_slice(
                                &bytes[..split],
                            )),
                            Ok(bytes::Bytes::copy_from_slice(&bytes[split..])),
                        ])),
                    )
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind native fixture");
        let addr = listener.local_addr().expect("native fixture address");
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}"), hits)
    }

    fn native_stream_state(
        base_url: &str,
        buffered: bool,
        middleware: Option<MiddlewareChain>,
    ) -> AppState {
        native_stream_state_with_sink(base_url, buffered, middleware).0
    }

    fn native_stream_state_with_sink(
        base_url: &str,
        buffered: bool,
        middleware: Option<MiddlewareChain>,
    ) -> (AppState, CapturingSink) {
        let mut config = Config::from_toml_str(&format!(
            r#"
[[namespace]]
id = "platform"
default = true

[[provider]]
id = "anthropic"
kind = "anthropic"
base_url = "{base_url}"

{GATEWAY_KEY}

[[credential]]
namespace = "platform"
provider = "anthropic"
env = "NATIVE_KEY"

[[price]]
provider = "anthropic"
model = "claude-test"
input_microdollars_per_million = 1
output_microdollars_per_million = 1
"#,
        ))
        .expect("native stream config");
        if buffered {
            enable_buffered_response_routes(&mut config, [BufferedResponseRoute::Messages]);
        }
        let usage = CapturingSink::default();
        let state = AppState::new(
            config,
            &env_with([("NATIVE_KEY", "native-secret")]),
            UsageFanout::new(vec![Box::new(usage.clone())]),
            Box::new(NoBudget),
        )
        .expect("native stream state");
        let state = match middleware {
            Some(chain) => state.with_middleware_chain(chain),
            None => state,
        };
        (state, usage)
    }

    fn native_stream_request() -> Request<Body> {
        authorized("/v1/messages")
            .body(Body::from(
                serde_json::to_vec(&json!({
                    "model": "anthropic/claude-test",
                    "stream": true,
                    "max_tokens": 8,
                    "messages": [{"role": "user", "content": "hello"}]
                }))
                .unwrap(),
            ))
            .unwrap()
    }

    #[tokio::test]
    async fn native_stream_mutation_is_refused_or_explicitly_buffered_without_implicit_reframing() {
        let (base_url, hits) = mutable_native_upstream().await;

        let denied = router(native_stream_state(
            &base_url,
            false,
            Some(StreamMarkerMiddleware::chain()),
        ))
        .oneshot(native_stream_request())
        .await
        .expect("typed incompatibility response");
        assert_eq!(denied.status(), StatusCode::BAD_REQUEST);
        let denied_body = denied.into_body().collect().await.unwrap().to_bytes();
        let denied_body: Value = serde_json::from_slice(&denied_body).unwrap();
        assert_eq!(
            denied_body["error"]["type"],
            "middleware_response_incompatible"
        );
        assert_eq!(hits.load(Ordering::SeqCst), 0);

        let passthrough = router(native_stream_state(&base_url, true, None))
            .oneshot(native_stream_request())
            .await
            .expect("policy-only passthrough response");
        assert_eq!(passthrough.status(), StatusCode::OK);
        let passthrough = passthrough.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(passthrough.as_ref(), MUTABLE_NATIVE_STREAM.as_bytes());
        assert_eq!(hits.load(Ordering::SeqCst), 1);

        let buffered = router(native_stream_state(
            &base_url,
            true,
            Some(StreamMarkerMiddleware::chain()),
        ))
        .oneshot(native_stream_request())
        .await
        .expect("explicitly buffered response");
        assert_eq!(buffered.status(), StatusCode::OK);
        let buffered = buffered.into_body().collect().await.unwrap().to_bytes();
        let buffered = String::from_utf8(buffered.to_vec()).unwrap();
        assert!(
            buffered.contains("\"middleware_marker\":true"),
            "{buffered}"
        );
        assert_ne!(buffered.as_bytes(), MUTABLE_NATIVE_STREAM.as_bytes());
        assert_eq!(hits.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn native_stream_validation_buffers_original_split_bytes_until_every_verdict() {
        let (base_url, hits) = mutable_native_upstream().await;

        let denied = router(native_stream_state(
            &base_url,
            false,
            Some(StreamValidationMiddleware::chain(false)),
        ))
        .oneshot(native_stream_request())
        .await
        .expect("typed validation incompatibility");
        assert_eq!(denied.status(), StatusCode::BAD_REQUEST);
        assert_eq!(hits.load(Ordering::SeqCst), 0);

        let validated = router(native_stream_state(
            &base_url,
            true,
            Some(StreamValidationMiddleware::chain(false)),
        ))
        .oneshot(native_stream_request())
        .await
        .expect("validated native stream");
        assert_eq!(validated.status(), StatusCode::OK);
        let validated = validated.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(validated.as_ref(), MUTABLE_NATIVE_STREAM.as_bytes());
        assert_eq!(hits.load(Ordering::SeqCst), 1);

        let refused = router(native_stream_state(
            &base_url,
            true,
            Some(StreamValidationMiddleware::chain(true)),
        ))
        .oneshot(native_stream_request())
        .await
        .expect("refused native stream");
        assert_eq!(refused.status(), StatusCode::OK);
        let refused = refused.into_body().collect().await.unwrap().to_bytes();
        let refused = String::from_utf8(refused.to_vec()).unwrap();
        assert!(refused.contains("middleware_stream_error"), "{refused}");
        assert!(
            refused.contains("request refused by middleware: policy"),
            "{refused}"
        );
        assert!(!refused.contains("\"text\":\"hi\""), "{refused}");
        assert!(!refused.contains("message_start"), "{refused}");
        assert_eq!(hits.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn native_block_only_guardrail_preserves_validated_provider_bytes_exactly() {
        let (base_url, hits) = mutable_native_upstream().await;

        let denied = router(native_stream_state(
            &base_url,
            false,
            Some(guardrail_chain(GuardrailAction::Block, "forbidden")),
        ))
        .oneshot(native_stream_request())
        .await
        .expect("block-only native stream requires validation opt-in");
        assert_eq!(denied.status(), StatusCode::BAD_REQUEST);
        assert_eq!(hits.load(Ordering::SeqCst), 0);

        let response = router(native_stream_state(
            &base_url,
            true,
            Some(guardrail_chain(GuardrailAction::Block, "forbidden")),
        ))
        .oneshot(native_stream_request())
        .await
        .expect("validated block-only native stream");

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body.as_ref(), MUTABLE_NATIVE_STREAM.as_bytes());
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    async fn redaction_native_upstream(complete_token: bool) -> (String, Arc<Mutex<Vec<Value>>>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let upstream_seen = Arc::clone(&seen);
        let app = Router::new().route(
            "/messages",
            post(move |Json(body): Json<Value>| {
                let upstream_seen = Arc::clone(&upstream_seen);
                async move {
                    upstream_seen.lock().unwrap().push(body.clone());
                    let content = body["messages"][0]["content"]
                        .as_str()
                        .expect("masked native prompt");
                    let cut = content.len() / 2;
                    let first = format!(
                        concat!(
                            "event: message_start\n",
                            "data: {{\"type\":\"message_start\",\"message\":{{\"id\":\"msg_redacted\",\"usage\":{{\"input_tokens\":3}}}}}}\n\n",
                            "event: content_block_start\n",
                            "data: {{\"type\":\"content_block_start\",\"index\":0,\"content_block\":{{\"type\":\"text\",\"text\":\"\"}}}}\n\n",
                            "event: content_block_delta\n",
                            "data: {{\"type\":\"content_block_delta\",\"index\":0,\"delta\":{{\"type\":\"text_delta\",\"text\":{}}}}}\n\n"
                        ),
                        serde_json::to_string(&content[..cut]).unwrap(),
                    );
                    let second = if complete_token {
                        format!(
                            concat!(
                            "event: content_block_delta\n",
                            "data: {{\"type\":\"content_block_delta\",\"index\":0,\"delta\":{{\"type\":\"text_delta\",\"text\":{}}}}}\n\n"
                            ),
                            serde_json::to_string(&content[cut..]).unwrap(),
                        )
                    } else {
                        String::new()
                    };
                    let terminal = concat!(
                            "event: content_block_stop\n",
                            "data: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
                            "event: message_delta\n",
                            "data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":1}}\n\n",
                            "event: message_stop\n",
                            "data: {\"type\":\"message_stop\"}\n\n"
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
            .expect("bind native redaction fixture");
        let addr = listener.local_addr().expect("native redaction address");
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}"), seen)
    }

    async fn failing_redaction_native_upstream(
        release_failure: Arc<Notify>,
    ) -> (String, Arc<Mutex<Vec<Value>>>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let upstream_seen = Arc::clone(&seen);
        let app = Router::new().route(
            "/messages",
            post(move |Json(body): Json<Value>| {
                let upstream_seen = Arc::clone(&upstream_seen);
                let release_failure = Arc::clone(&release_failure);
                async move {
                    upstream_seen.lock().unwrap().push(body.clone());
                    let content = body["messages"][0]["content"]
                        .as_str()
                        .expect("masked native prompt");
                    let cut = content.len() / 2;
                    let first = format!(
                        concat!(
                            "event: message_start\n",
                            "data: {{\"type\":\"message_start\",\"message\":{{\"id\":\"msg_failed\",\"usage\":{{\"input_tokens\":3}}}}}}\n\n",
                            "event: content_block_start\n",
                            "data: {{\"type\":\"content_block_start\",\"index\":0,\"content_block\":{{\"type\":\"text\",\"text\":\"\"}}}}\n\n",
                            "event: content_block_delta\n",
                            "data: {{\"type\":\"content_block_delta\",\"index\":0,\"delta\":{{\"type\":\"text_delta\",\"text\":{}}}}}\n\n"
                        ),
                        serde_json::to_string(&content[..cut]).unwrap(),
                    );
                    let stream = futures::stream::iter([Ok::<_, std::io::Error>(
                        bytes::Bytes::from(first),
                    )])
                    .chain(futures::stream::once(async move {
                        // Do not fail the transport until a downstream
                        // observer proves axond.redact saw the generated-token
                        // prefix and retained it as carry.
                        release_failure.notified().await;
                        Err(std::io::Error::other(
                            "redaction fixture transport failure",
                        ))
                    }));
                    (
                        [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                        Body::from_stream(stream),
                    )
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind failing native redaction fixture");
        let addr = listener
            .local_addr()
            .expect("failing native redaction address");
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}"), seen)
    }

    fn native_redaction_request(secret: &str) -> Request<Body> {
        authorized("/v1/messages")
            .body(Body::from(
                serde_json::to_vec(&json!({
                    "model": "anthropic/claude-test",
                    "stream": true,
                    "max_tokens": 8,
                    "messages": [{"role": "user", "content": secret}]
                }))
                .unwrap(),
            ))
            .unwrap()
    }

    #[tokio::test]
    async fn native_redaction_requires_opt_in_then_restores_split_output_without_leaking() {
        const SECRET: &str = "native-secret@example.com";
        let (base_url, seen) = redaction_native_upstream(true).await;

        let denied = router(native_stream_state(
            &base_url,
            false,
            Some(guardrail_chain(
                GuardrailAction::Redact,
                r"[a-z-]+@example\.com",
            )),
        ))
        .oneshot(native_redaction_request(SECRET))
        .await
        .expect("typed native redaction incompatibility");
        assert_eq!(denied.status(), StatusCode::BAD_REQUEST);
        let denied = denied.into_body().collect().await.unwrap().to_bytes();
        let denied: Value = serde_json::from_slice(&denied).unwrap();
        assert_eq!(denied["error"]["type"], "middleware_response_incompatible");
        assert!(seen.lock().unwrap().is_empty());

        let restored = router(native_stream_state(
            &base_url,
            true,
            Some(guardrail_chain(
                GuardrailAction::Redact,
                r"[a-z-]+@example\.com",
            )),
        ))
        .oneshot(native_redaction_request(SECRET))
        .await
        .expect("buffered native redaction response");
        assert_eq!(restored.status(), StatusCode::OK);
        let restored = restored.into_body().collect().await.unwrap().to_bytes();
        let restored = String::from_utf8(restored.to_vec()).unwrap();
        assert!(restored.contains(SECRET), "{restored}");
        assert!(!restored.contains("[AXOND:"), "{restored}");
        assert!(!restored.contains("middleware_stream_error"), "{restored}");
        assert!(restored.contains("event: message_stop"), "{restored}");
        assert!(
            restored.contains(r#"data: {"type":"message_stop"}"#),
            "{restored}"
        );

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        let provider_content = seen[0]["messages"][0]["content"].as_str().unwrap();
        assert!(!provider_content.contains(SECRET));
        assert!(provider_content.starts_with("[AXOND:"));
    }

    #[tokio::test]
    async fn native_incomplete_redaction_token_fails_before_any_buffered_content_is_released() {
        const SECRET: &str = "native-incomplete@example.com";
        let (base_url, seen) = redaction_native_upstream(false).await;
        let response = router(native_stream_state(
            &base_url,
            true,
            Some(guardrail_chain(
                GuardrailAction::Redact,
                r"[a-z-]+@example\.com",
            )),
        ))
        .oneshot(native_redaction_request(SECRET))
        .await
        .expect("native finalizer refusal");
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("middleware_stream_error"), "{body}");
        assert!(!body.contains(SECRET), "{body}");
        assert!(!body.contains("[AXOND:"), "{body}");
        assert!(!body.contains("message_start"), "{body}");
        assert!(!body.contains("message_stop"), "{body}");
        assert_eq!(seen.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn native_transport_failure_discards_real_redaction_carry_and_buffered_content() {
        const SECRET: &str = "native-transport@example.com";
        let release_failure = Arc::new(Notify::new());
        let observed = Arc::new(Mutex::new(Vec::new()));
        let state_drops = Arc::new(AtomicUsize::new(0));
        let (base_url, seen) =
            failing_redaction_native_upstream(Arc::clone(&release_failure)).await;
        let response = router(native_stream_state(
            &base_url,
            true,
            Some(observed_guardrail_chain(
                r"[a-z-]+@example\.com",
                Arc::clone(&observed),
                release_failure,
                Arc::clone(&state_drops),
            )),
        ))
        .oneshot(native_redaction_request(SECRET))
        .await
        .expect("native transport failure response");

        assert_eq!(response.status(), StatusCode::OK);
        let body = tokio::time::timeout(Duration::from_secs(5), response.into_body().collect())
            .await
            .expect("redaction carry was observed before transport failure")
            .unwrap()
            .to_bytes();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("upstream_stream_error"), "{body}");
        assert!(!body.contains(SECRET), "{body}");
        assert!(!body.contains("[AXOND:"), "{body}");
        assert!(!body.contains("message_start"), "{body}");
        assert!(!body.contains("message_stop"), "{body}");
        assert_eq!(
            observed.lock().unwrap().as_slice(),
            &[String::new()],
            "the guardrail must retain the partial generated token as carry"
        );
        assert_eq!(
            state_drops.load(Ordering::SeqCst),
            1,
            "transport failure drops response-lifetime middleware state once"
        );
        assert_eq!(seen.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn native_guardrail_block_refuses_before_dispatch_without_usage_or_echo() {
        const SECRET: &str = "native-blocked@example.com";
        let (base_url, seen) = redaction_native_upstream(true).await;
        let (state, usage) = native_stream_state_with_sink(
            &base_url,
            true,
            Some(guardrail_chain(
                GuardrailAction::Block,
                r"[a-z-]+@example\.com",
            )),
        );

        let response = router(state)
            .oneshot(native_redaction_request(SECRET))
            .await
            .expect("native guardrail refusal");
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(!body.contains(SECRET), "{body}");
        assert!(seen.lock().unwrap().is_empty());
        assert!(usage.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn native_guardrail_refuses_a_match_split_across_header_and_body() {
        const SECRET: &str = "native-header@example.com";
        let (base_url, seen) = redaction_native_upstream(true).await;
        let (state, usage) = native_stream_state_with_sink(
            &base_url,
            true,
            Some(guardrail_chain(
                GuardrailAction::Redact,
                r"[a-z-]+@example\.com",
            )),
        );
        let mut request = authorized("/v1/messages")
            .body(Body::from(
                serde_json::to_vec(&json!({
                    "model": "anthropic/claude-test",
                    "stream": true,
                    "max_tokens": 8,
                    "messages": [{"role": "user", "content": [
                        {"type": "text", "text": "@"},
                        {"type": "text", "text": "example.com"}
                    ]}]
                }))
                .unwrap(),
            ))
            .unwrap();
        request
            .headers_mut()
            .insert("anthropic-beta", HeaderValue::from_static("native-header"));

        let response = router(state)
            .oneshot(request)
            .await
            .expect("matched wire header refusal");
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(!body.contains(SECRET), "{body}");
        assert!(seen.lock().unwrap().is_empty());
        assert!(usage.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn native_guardrail_allows_benign_forwarded_wire_headers() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let upstream_seen = Arc::clone(&seen);
        let app = Router::new().route(
            "/messages",
            post(
                move |headers: axum::http::HeaderMap, Json(_body): Json<Value>| {
                    let upstream_seen = Arc::clone(&upstream_seen);
                    async move {
                        upstream_seen.lock().unwrap().push(
                            headers
                                .get("anthropic-beta")
                                .and_then(|value| value.to_str().ok())
                                .map(str::to_owned),
                        );
                        (
                            [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                            Body::from(MUTABLE_NATIVE_STREAM),
                        )
                    }
                },
            ),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind benign native-header fixture");
        let addr = listener
            .local_addr()
            .expect("benign native-header fixture address");
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let base_url = format!("http://{addr}");
        let state = native_stream_state(
            &base_url,
            true,
            Some(guardrail_chain(
                GuardrailAction::Redact,
                r"[a-z-]+@example\.com",
            )),
        );
        let mut request = native_redaction_request("ordinary prompt");
        request.headers_mut().insert(
            "anthropic-beta",
            HeaderValue::from_static("feature-2026-08-17"),
        );

        let response = router(state)
            .oneshot(request)
            .await
            .expect("benign wire header response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert!(String::from_utf8_lossy(&body).contains("hi"));
        assert_eq!(
            seen.lock().unwrap().as_slice(),
            [Some("feature-2026-08-17".to_owned())]
        );
    }

    const MUTABLE_RESPONSES_STREAM: &str = concat!(
        "event: response.output_text.delta\n",
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n",
        "event: response.completed\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"status\":\"completed\",\"usage\":{\"input_tokens\":3,\"output_tokens\":1}}}\n\n",
    );

    async fn mutable_responses_upstream() -> (String, Arc<AtomicUsize>) {
        let hits = Arc::new(AtomicUsize::new(0));
        let served = Arc::clone(&hits);
        let app = Router::new().route(
            "/responses",
            post(move || {
                let served = Arc::clone(&served);
                async move {
                    served.fetch_add(1, Ordering::SeqCst);
                    let split = MUTABLE_RESPONSES_STREAM
                        .find("\"hi\"")
                        .expect("Responses fixture contains text delta")
                        + 2;
                    let bytes = MUTABLE_RESPONSES_STREAM.as_bytes();
                    (
                        [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                        Body::from_stream(futures::stream::iter([
                            Ok::<_, std::convert::Infallible>(bytes::Bytes::copy_from_slice(
                                &bytes[..split],
                            )),
                            Ok(bytes::Bytes::copy_from_slice(&bytes[split..])),
                        ])),
                    )
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind Responses fixture");
        let addr = listener.local_addr().expect("Responses fixture address");
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{addr}"), hits)
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

    fn responses_redaction_request(
        secret: &str,
        previous_response_id: Option<&str>,
    ) -> Request<Body> {
        let mut body = json!({"model": "openai/gpt-4o", "input": secret, "stream": true});
        if let Some(id) = previous_response_id {
            body["previous_response_id"] = json!(id);
        }
        authorized("/v1/responses")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap()
    }

    fn responses_stream_state(
        url_a: &str,
        url_b: &str,
        buffered: bool,
        middleware: Option<MiddlewareChain>,
    ) -> AppState {
        responses_stream_state_with_sink(url_a, url_b, buffered, middleware).0
    }

    fn responses_stream_state_with_sink(
        url_a: &str,
        url_b: &str,
        buffered: bool,
        middleware: Option<MiddlewareChain>,
    ) -> (AppState, CapturingSink) {
        let mut config = Config::from_toml_str(&format!(
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
        if buffered {
            enable_buffered_response_routes(&mut config, [BufferedResponseRoute::Responses]);
        }
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
        let state = match middleware {
            Some(chain) => state.with_middleware_chain(chain),
            None => state,
        };
        (state, usage)
    }

    #[tokio::test]
    async fn responses_stream_validation_buffers_original_split_bytes_until_every_verdict() {
        let (url_a, hits_a) = mutable_responses_upstream().await;
        let (url_b, hits_b) = mutable_responses_upstream().await;

        let denied = router(responses_stream_state(
            &url_a,
            &url_b,
            false,
            Some(StreamValidationMiddleware::chain(false)),
        ))
        .oneshot(streaming_responses_request(None))
        .await
        .expect("typed Responses validation incompatibility");
        assert_eq!(denied.status(), StatusCode::BAD_REQUEST);
        assert_eq!(hits_a.load(Ordering::SeqCst), 0);

        let validated = router(responses_stream_state(
            &url_a,
            &url_b,
            true,
            Some(StreamValidationMiddleware::chain(false)),
        ))
        .oneshot(streaming_responses_request(None))
        .await
        .expect("validated Responses stream");
        assert_eq!(validated.status(), StatusCode::OK);
        let validated = validated.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(validated.as_ref(), MUTABLE_RESPONSES_STREAM.as_bytes());
        assert_eq!(hits_a.load(Ordering::SeqCst), 1);

        let refused = router(responses_stream_state(
            &url_a,
            &url_b,
            true,
            Some(StreamValidationMiddleware::chain(true)),
        ))
        .oneshot(streaming_responses_request(None))
        .await
        .expect("refused Responses stream");
        assert_eq!(refused.status(), StatusCode::OK);
        let refused = refused.into_body().collect().await.unwrap().to_bytes();
        let refused = String::from_utf8(refused.to_vec()).unwrap();
        assert!(refused.contains("middleware_stream_error"), "{refused}");
        assert!(
            refused.contains("request refused by middleware: policy"),
            "{refused}"
        );
        assert!(!refused.contains("\"delta\":\"hi\""), "{refused}");
        assert!(!refused.contains("response.completed"), "{refused}");
        assert_eq!(hits_a.load(Ordering::SeqCst), 2);
        assert_eq!(hits_b.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn responses_block_only_guardrail_preserves_validated_provider_bytes_exactly() {
        let (url_a, hits_a) = mutable_responses_upstream().await;
        let (url_b, hits_b) = mutable_responses_upstream().await;

        let denied = router(responses_stream_state(
            &url_a,
            &url_b,
            false,
            Some(guardrail_chain(GuardrailAction::Block, "forbidden")),
        ))
        .oneshot(streaming_responses_request(None))
        .await
        .expect("block-only Responses stream requires validation opt-in");
        assert_eq!(denied.status(), StatusCode::BAD_REQUEST);
        assert_eq!(hits_a.load(Ordering::SeqCst), 0);
        assert_eq!(hits_b.load(Ordering::SeqCst), 0);

        let response = router(responses_stream_state(
            &url_a,
            &url_b,
            true,
            Some(guardrail_chain(GuardrailAction::Block, "forbidden")),
        ))
        .oneshot(streaming_responses_request(None))
        .await
        .expect("validated block-only Responses stream");

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body.as_ref(), MUTABLE_RESPONSES_STREAM.as_bytes());
        assert_eq!(hits_a.load(Ordering::SeqCst), 1);
        assert_eq!(hits_b.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn responses_redaction_requires_opt_in_restores_split_output_and_keeps_affinity() {
        const SECRET: &str = "responses-secret@example.com";
        let (url_a, seen_a) = redaction_responses_upstream(true).await;
        let (url_b, seen_b) = redaction_responses_upstream(true).await;

        let denied = router(responses_stream_state(
            &url_a,
            &url_b,
            false,
            Some(guardrail_chain(
                GuardrailAction::Redact,
                r"[a-z-]+@example\.com",
            )),
        ))
        .oneshot(responses_redaction_request(SECRET, None))
        .await
        .expect("typed Responses redaction incompatibility");
        assert_eq!(denied.status(), StatusCode::BAD_REQUEST);
        let denied = denied.into_body().collect().await.unwrap().to_bytes();
        let denied: Value = serde_json::from_slice(&denied).unwrap();
        assert_eq!(denied["error"]["type"], "middleware_response_incompatible");
        assert!(seen_a.lock().unwrap().is_empty());
        assert!(seen_b.lock().unwrap().is_empty());

        let state = responses_stream_state(
            &url_a,
            &url_b,
            true,
            Some(guardrail_chain(
                GuardrailAction::Redact,
                r"[a-z-]+@example\.com",
            )),
        );
        for previous in [None, Some("resp_1")] {
            let restored = router(state.clone())
                .oneshot(responses_redaction_request(SECRET, previous))
                .await
                .expect("buffered Responses redaction response");
            assert_eq!(restored.status(), StatusCode::OK);
            let restored = restored.into_body().collect().await.unwrap().to_bytes();
            let restored = String::from_utf8(restored.to_vec()).unwrap();
            assert!(restored.contains(SECRET), "{restored}");
            assert!(!restored.contains("[AXOND:"), "{restored}");
            assert!(!restored.contains("middleware_stream_error"), "{restored}");
        }

        let seen_a = seen_a.lock().unwrap();
        assert_eq!(seen_a.len(), 2);
        assert!(
            seen_b.lock().unwrap().is_empty(),
            "affinity moved off target A"
        );
        for (index, body) in seen_a.iter().enumerate() {
            let provider_input = body["input"].as_str().unwrap();
            assert!(!provider_input.contains(SECRET));
            assert!(provider_input.starts_with("[AXOND:"));
            if index == 0 {
                assert!(body.get("previous_response_id").is_none());
            } else {
                assert_eq!(body["previous_response_id"], "resp_1");
            }
        }
    }

    #[tokio::test]
    async fn responses_incomplete_redaction_token_fails_before_original_bytes_are_released() {
        const SECRET: &str = "responses-incomplete@example.com";
        let (url_a, seen_a) = redaction_responses_upstream(false).await;
        let (url_b, seen_b) = redaction_responses_upstream(false).await;
        let response = router(responses_stream_state(
            &url_a,
            &url_b,
            true,
            Some(guardrail_chain(
                GuardrailAction::Redact,
                r"[a-z-]+@example\.com",
            )),
        ))
        .oneshot(responses_redaction_request(SECRET, None))
        .await
        .expect("Responses finalizer refusal");
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("middleware_stream_error"), "{body}");
        assert!(!body.contains(SECRET), "{body}");
        assert!(!body.contains("[AXOND:"), "{body}");
        assert!(!body.contains("response.output_text.delta"), "{body}");
        assert!(!body.contains("response.completed"), "{body}");
        assert_eq!(seen_a.lock().unwrap().len(), 1);
        assert!(seen_b.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn responses_guardrail_block_refuses_before_dispatch_without_usage_or_echo() {
        const SECRET: &str = "responses-blocked@example.com";
        let (url_a, seen_a) = redaction_responses_upstream(true).await;
        let (url_b, seen_b) = redaction_responses_upstream(true).await;
        let (state, usage) = responses_stream_state_with_sink(
            &url_a,
            &url_b,
            true,
            Some(guardrail_chain(
                GuardrailAction::Block,
                r"[a-z-]+@example\.com",
            )),
        );

        let response = router(state)
            .oneshot(responses_redaction_request(SECRET, None))
            .await
            .expect("Responses guardrail refusal");
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(!body.contains(SECRET), "{body}");
        assert!(seen_a.lock().unwrap().is_empty());
        assert!(seen_b.lock().unwrap().is_empty());
        assert!(usage.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn responses_guardrail_refuses_a_matched_continuation_id_before_dispatch() {
        const SECRET: &str = "continuation@example.com";
        let (url_a, seen_a) = redaction_responses_upstream(true).await;
        let (url_b, seen_b) = redaction_responses_upstream(true).await;
        let (state, usage) = responses_stream_state_with_sink(
            &url_a,
            &url_b,
            true,
            Some(guardrail_chain(
                GuardrailAction::Redact,
                r"[a-z-]+@example\.com",
            )),
        );

        let response = router(state)
            .oneshot(responses_redaction_request("ordinary input", Some(SECRET)))
            .await
            .expect("matched continuation id refusal");
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(!body.contains(SECRET), "{body}");
        assert!(seen_a.lock().unwrap().is_empty());
        assert!(seen_b.lock().unwrap().is_empty());
        assert!(usage.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn malformed_responses_controls_never_reach_middleware_or_provider() {
        const SECRET: &str = "malformed-control@example.com";
        let (url_a, seen_a) = redaction_responses_upstream(true).await;
        let (url_b, seen_b) = redaction_responses_upstream(true).await;
        let (state, usage) = responses_stream_state_with_sink(
            &url_a,
            &url_b,
            true,
            Some(guardrail_chain(
                GuardrailAction::Redact,
                r"[a-z-]+@example\.com",
            )),
        );

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

    #[tokio::test]
    async fn pinned_responses_initial_and_continuation_keep_affinity_in_both_buffering_modes() {
        let (url_a, hits_a) = mutable_responses_upstream().await;
        let (url_b, hits_b) = mutable_responses_upstream().await;

        let denied_state =
            responses_stream_state(&url_a, &url_b, false, Some(StreamMarkerMiddleware::chain()));
        for previous in [None, Some("resp_1")] {
            let denied = router(denied_state.clone())
                .oneshot(streaming_responses_request(previous))
                .await
                .expect("typed Responses incompatibility");
            assert_eq!(denied.status(), StatusCode::BAD_REQUEST);
            let body = denied.into_body().collect().await.unwrap().to_bytes();
            let body: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(body["error"]["type"], "middleware_response_incompatible");
        }
        assert_eq!(hits_a.load(Ordering::SeqCst), 0);
        assert_eq!(hits_b.load(Ordering::SeqCst), 0);

        let buffered_state =
            responses_stream_state(&url_a, &url_b, true, Some(StreamMarkerMiddleware::chain()));
        for previous in [None, Some("resp_1")] {
            let buffered = router(buffered_state.clone())
                .oneshot(streaming_responses_request(previous))
                .await
                .expect("buffered Responses stream");
            assert_eq!(buffered.status(), StatusCode::OK);
            let body = buffered.into_body().collect().await.unwrap().to_bytes();
            let body = String::from_utf8(body.to_vec()).unwrap();
            assert!(body.contains("\"middleware_marker\":true"), "{body}");
        }
        assert_eq!(hits_a.load(Ordering::SeqCst), 2);
        assert_eq!(hits_b.load(Ordering::SeqCst), 0);
    }

    impl BodyGrowthMiddleware {
        fn grow(body: &mut Value, padding_bytes: usize) {
            body["middleware_padding"] = Value::String("x".repeat(padding_bytes));
        }

        fn chain(padding_bytes: usize) -> MiddlewareChain {
            Self::chain_with(padding_bytes, None)
        }

        fn output_chain(output_tokens: u64) -> MiddlewareChain {
            Self::chain_with(0, Some(output_tokens))
        }

        fn chain_with(padding_bytes: usize, output_tokens: Option<u64>) -> MiddlewareChain {
            let declaration = MiddlewareDeclaration::new(
                "test.body_growth",
                [gateway_core::MiddlewareScope::Request],
            );
            MiddlewareChain::new(vec![Arc::new(Self {
                declaration,
                padding_bytes,
                output_tokens,
            }) as Arc<dyn Middleware>])
            .expect("test middleware chain")
        }
    }

    impl Middleware for BodyGrowthMiddleware {
        fn declaration(&self) -> &MiddlewareDeclaration {
            &self.declaration
        }

        fn apply(
            &self,
            phase: MiddlewarePhase<'_>,
            _state: Option<&mut gateway_core::MiddlewareState>,
        ) -> MiddlewareResult {
            if let MiddlewarePhase::Request(request) = phase {
                if self.padding_bytes > 0 {
                    Self::grow(&mut request.body, self.padding_bytes);
                }
                if let Some(output_tokens) = self.output_tokens {
                    request.body["max_tokens"] = json!(output_tokens);
                }
            }
            Ok(MiddlewareOutcome::continue_without_state())
        }
    }

    struct CountingMiddleware {
        declaration: MiddlewareDeclaration,
        calls: Arc<AtomicUsize>,
    }

    impl Middleware for CountingMiddleware {
        fn declaration(&self) -> &MiddlewareDeclaration {
            &self.declaration
        }

        fn apply(
            &self,
            phase: MiddlewarePhase<'_>,
            _state: Option<&mut gateway_core::MiddlewareState>,
        ) -> MiddlewareResult {
            if matches!(phase, MiddlewarePhase::Request(_)) {
                self.calls.fetch_add(1, Ordering::SeqCst);
            }
            Ok(MiddlewareOutcome::continue_without_state())
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
        budgeted_state_with_limiter_mode(
            base_url,
            budget,
            rate_limiter,
            CoreAccountingMode::Middleware,
        )
    }

    fn budgeted_state_with_limiter_mode(
        base_url: &str,
        budget: Box<dyn crate::budget::BudgetStore>,
        rate_limiter: Box<dyn RateLimiter>,
        accounting_mode: CoreAccountingMode,
    ) -> AppState {
        let accounting_mode = match accounting_mode {
            CoreAccountingMode::Legacy => "legacy",
            CoreAccountingMode::Middleware => "middleware",
        };
        let cfg = Config::from_toml_str(&format!(
            r#"
[core_middleware]
accounting = "{accounting_mode}"

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

    #[tokio::test]
    async fn middleware_growth_is_checked_against_the_authoritative_prompt_bound() {
        let (base_url, hits) =
            controllable_upstream(Arc::new(AtomicBool::new(true)), StatusCode::OK).await;
        let budget = RecordingBudget::default();
        let state = admitting_state(
            &base_url,
            "max_prompt_tokens = 64",
            Box::new(budget.clone()),
        )
        .with_middleware_chain(BodyGrowthMiddleware::chain(512));

        let response = router(state)
            .oneshot(chat_request())
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["error"]["type"], "prompt_too_large");
        assert!(budget.0.lock().unwrap().is_empty());
        assert_eq!(hits.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn middleware_growth_is_checked_against_the_authoritative_output_bound() {
        let (base_url, hits) =
            controllable_upstream(Arc::new(AtomicBool::new(true)), StatusCode::OK).await;
        let budget = RecordingBudget::default();
        let state = admitting_state(
            &base_url,
            "max_output_tokens = 16",
            Box::new(budget.clone()),
        )
        .with_middleware_chain(BodyGrowthMiddleware::output_chain(4_096));

        let response = router(state)
            .oneshot(chat_request())
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["error"]["type"], "output_limit_exceeded");
        assert!(budget.0.lock().unwrap().is_empty());
        assert_eq!(hits.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn middleware_does_not_run_before_the_rate_limit_permit_is_held() {
        let calls = Arc::new(AtomicUsize::new(0));
        let chain = MiddlewareChain::new(vec![Arc::new(CountingMiddleware {
            declaration: MiddlewareDeclaration::new(
                "test.counting",
                [gateway_core::MiddlewareScope::Request],
            ),
            calls: Arc::clone(&calls),
        }) as Arc<dyn Middleware>])
        .expect("test middleware chain");
        let budget = RecordingBudget::default();
        let state = budgeted_state_with_limiter(
            "http://127.0.0.1:1",
            Box::new(budget.clone()),
            Box::new(UnavailableLimiter),
        )
        .with_middleware_chain(chain);

        let response = router(state).oneshot(chat_request()).await.unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["error"]["type"], "rate_limit_unavailable");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(budget.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn middleware_growth_cannot_bypass_the_request_cost_ceiling() {
        let (base_url, hits) =
            controllable_upstream(Arc::new(AtomicBool::new(true)), StatusCode::OK).await;
        let budget = RecordingBudget::default();
        let state = budgeted_state(&base_url, Box::new(budget.clone()))
            .with_middleware_chain(BodyGrowthMiddleware::chain(512));
        let snapshot = state.config();
        let caller = InboundKey {
            namespace: "platform".to_owned(),
            subject: "ceiling-caller".to_owned(),
            authority: PrincipalAuthority::MintedToken,
            signer_kid: Some("test-kid".to_owned()),
            scope: None,
            alias_scope: None,
            max_request_microdollars: Some(50),
            can_mint: false,
            jti: None,
            namespace_grant: None,
            attrs: None,
        };
        let body = json!({
            "model": "openai/gpt-4o",
            "messages": [],
            "max_tokens": 1
        });

        let error = serve(
            state,
            HeaderMap::new(),
            body,
            Route::ChatCompletions,
            snapshot,
            caller,
            None,
        )
        .await
        .expect_err("post-middleware estimate exceeds the caller ceiling");
        assert!(matches!(
            error,
            GatewayError::RequestCostCeilingExceeded { .. }
        ));
        assert_eq!(hits.load(Ordering::SeqCst), 0);
        assert!(budget.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn middleware_body_drives_the_budget_hold_and_settled_input_usage() {
        let base_url = body_measuring_upstream().await;
        let captured = CapturingSink::default();
        let budget = RecordingBudget::default();
        let state = two_target_state_with_budget(
            &base_url,
            &base_url,
            "",
            captured.clone(),
            Box::new(budget.clone()),
        )
        .with_middleware_chain(BodyGrowthMiddleware::chain(512));

        let original = json!({
            "model": "openai/gpt-4o",
            "messages": [],
            "max_tokens": 1
        });
        let response = router(state)
            .oneshot(
                authorized("/v1/chat/completions")
                    .body(Body::from(serde_json::to_vec(&original).unwrap()))
                    .unwrap(),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);

        let mut post_middleware = original.clone();
        BodyGrowthMiddleware::grow(&mut post_middleware, 512);
        let without_middleware = Route::ChatCompletions.estimate(&original);
        let with_middleware = Route::ChatCompletions.estimate(&post_middleware);
        let (reserved, settled) = budget.0.lock().unwrap()[0];
        assert!(
            reserved > without_middleware.input_tokens + without_middleware.output_tokens,
            "the reservation must use the post-middleware body"
        );
        assert_eq!(
            reserved,
            with_middleware.input_tokens + with_middleware.output_tokens,
            "configured one-microdollar-per-token pricing makes the hold legible"
        );
        let records = captured.0.lock().unwrap();
        assert_eq!(records.len(), 1);
        let mut sent_body = post_middleware;
        sent_body["model"] = json!("m-a");
        let sent_input_tokens = serde_json::to_string(&sent_body).unwrap().len() as u64 / 4;
        assert_eq!(records[0].input_tokens, sent_input_tokens);
        assert!(records[0].input_tokens > without_middleware.input_tokens);
        assert_eq!(settled, sent_input_tokens + 1);
    }

    async fn redaction_echo_upstream() -> (String, Arc<Mutex<Vec<Value>>>) {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let upstream_seen = Arc::clone(&seen);
        let app = Router::new().route(
            "/chat/completions",
            post(move |Json(body): Json<Value>| {
                let upstream_seen = Arc::clone(&upstream_seen);
                async move {
                    upstream_seen.lock().unwrap().push(body.clone());
                    let content = body["messages"][0]["content"]
                        .as_str()
                        .expect("masked prompt")
                        .to_owned();
                    if body["stream"].as_bool() == Some(true) {
                        let cut = content.len() / 2;
                        let first = json!({
                            "id": "chatcmpl-redacted",
                            "choices": [{"index": 0, "delta": {"content": &content[..cut]}}]
                        });
                        let second = json!({
                            "id": "chatcmpl-redacted",
                            "choices": [{"index": 0, "delta": {"content": &content[cut..]}}]
                        });
                        (
                            [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                            Body::from(format!(
                                "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
                                first, second
                            )),
                        )
                            .into_response()
                    } else {
                        Json(json!({
                            "id": "chatcmpl-redacted",
                            "choices": [{
                                "index": 0,
                                "message": {"role": "assistant", "content": content}
                            }],
                            "usage": {"prompt_tokens": 4, "completion_tokens": 2}
                        }))
                        .into_response()
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind redaction fixture");
        let address = listener.local_addr().expect("redaction fixture address");
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (format!("http://{address}"), seen)
    }

    fn production_guardrail_state(base_url: &str) -> AppState {
        let mut config = Config::from_toml_str(&format!(
            r#"
[[namespace]]
id = "alpha"
default = true

[[namespace]]
id = "beta"

[[provider]]
id = "openai"
kind = "openai"
base_url = "{base_url}"

[[gateway_key]]
env = "ALPHA_INBOUND"
namespace = "alpha"

[[gateway_key]]
env = "BETA_INBOUND"
namespace = "beta"

[[credential]]
namespace = "alpha"
provider = "openai"
env = "ALPHA_PROVIDER"

[[credential]]
namespace = "beta"
provider = "openai"
env = "BETA_PROVIDER"

[[price]]
provider = "openai"
model = "gpt-4o"
input_microdollars_per_million = 1
output_microdollars_per_million = 1
"#,
        ))
        .expect("production guardrail route config");

        for (index, tenant) in [tenant_id(1), tenant_id(2)].into_iter().enumerate() {
            let registration = ContentMiddlewareRegistration::new(
                "axond.redact",
                [
                    MiddlewareScope::Request,
                    MiddlewareScope::Response,
                    MiddlewareScope::StreamEvent,
                ],
                MiddlewareFailurePosture::FailClosed,
                1_000,
            )
            .expect("valid production guardrail registration")
            .with_guardrail(
                ContentGuardrailRegistration::new(
                    "GUARDRAIL_KEY",
                    vec![GuardrailRule {
                        id: "email".to_owned(),
                        pattern: r"[a-z]+@example\.com".to_owned(),
                        action: GuardrailAction::Redact,
                    }],
                )
                .expect("valid production guardrail rules"),
            )
            .expect("guardrail configuration attaches");
            let body = policy_body(PolicyScope::Tenant(tenant), 1)
                .with_content_middleware(vec![registration])
                .expect("guardrail policy attaches");
            let generation = body.generation(revision_id(index as u64 + 1));
            config.namespace[index].project = Some(ProjectIdentity {
                tenant,
                project: project_id(index as u64 + 1),
            });
            config.namespace[index].policy = Some(NamespacePolicy { body, generation });
        }

        let env = HashMap::from([
            ("ALPHA_INBOUND".to_owned(), "alpha-inbound".to_owned()),
            ("BETA_INBOUND".to_owned(), "beta-inbound".to_owned()),
            ("ALPHA_PROVIDER".to_owned(), "sk-alpha".to_owned()),
            ("BETA_PROVIDER".to_owned(), "sk-beta".to_owned()),
            ("GUARDRAIL_KEY".to_owned(), STANDARD.encode([9_u8; 32])),
        ]);
        AppState::new(
            config,
            &env,
            UsageFanout::new(vec![Box::new(StdoutSink)]),
            Box::new(NoBudget),
        )
        .expect("production guardrail route state")
    }

    fn production_redaction_request(
        bearer: &str,
        messages: Vec<Value>,
        stream: bool,
    ) -> Request<Body> {
        Request::post("/ns/platform/v1/chat/completions")
            .header("content-type", "application/json")
            .header(
                axum::http::header::AUTHORIZATION,
                format!("Bearer {bearer}"),
            )
            .body(Body::from(
                serde_json::to_vec(&json!({
                    "model": "openai/gpt-4o",
                    "messages": messages,
                    "stream": stream,
                }))
                .expect("production redaction request body"),
            ))
            .expect("production redaction request")
    }

    #[tokio::test]
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
    async fn compiled_guardrail_policy_masks_real_routes_stably_across_turns_and_namespaces() {
        const SECRET: &str = "alice@example.com";
        let (base_url, seen) = redaction_echo_upstream().await;
        let app = router(production_guardrail_state(&base_url));

        for request in [
            production_redaction_request(
                "alpha-inbound",
                vec![json!({"role": "user", "content": SECRET})],
                false,
            ),
            production_redaction_request(
                "alpha-inbound",
                vec![
                    json!({"role": "user", "content": SECRET}),
                    json!({"role": "assistant", "content": SECRET}),
                ],
                false,
            ),
            production_redaction_request(
                "beta-inbound",
                vec![json!({"role": "user", "content": SECRET})],
                false,
            ),
        ] {
            let response = app.clone().oneshot(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let body: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(body["choices"][0]["message"]["content"], SECRET);
        }

        let streamed = app
            .clone()
            .oneshot(production_redaction_request(
                "alpha-inbound",
                vec![json!({"role": "user", "content": SECRET})],
                true,
            ))
            .await
            .unwrap();
        assert_eq!(streamed.status(), StatusCode::OK);
        let streamed = streamed.into_body().collect().await.unwrap().to_bytes();
        let streamed = String::from_utf8(streamed.to_vec()).unwrap();
        assert!(streamed.contains(SECRET), "{streamed}");
        assert!(!streamed.contains("[AXOND:"), "{streamed}");

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 4);
        let alpha_first = seen[0]["messages"][0]["content"].as_str().unwrap();
        let alpha_later_user = seen[1]["messages"][0]["content"].as_str().unwrap();
        let alpha_later_assistant = seen[1]["messages"][1]["content"].as_str().unwrap();
        let beta = seen[2]["messages"][0]["content"].as_str().unwrap();
        let alpha_streamed = seen[3]["messages"][0]["content"].as_str().unwrap();
        assert_eq!(alpha_first, alpha_later_user);
        assert_eq!(alpha_first, alpha_later_assistant);
        assert_eq!(alpha_first, alpha_streamed);
        assert_ne!(alpha_first, beta);
        for (body, provider_value) in
            seen.iter()
                .zip([alpha_first, alpha_later_user, beta, alpha_streamed])
        {
            let serialized = serde_json::to_string(body).unwrap();
            assert!(!serialized.contains(SECRET), "{serialized}");
            assert!(provider_value.starts_with("[AXOND:"), "{provider_value}");
            assert!(provider_value.ends_with(']'), "{provider_value}");
            assert_eq!(provider_value.len(), 30, "{provider_value}");
            assert!(!provider_value.contains(SECRET), "{provider_value}");
        }
    }

    #[tokio::test]
    async fn deterministic_redaction_round_trips_buffered_and_split_openai_sse_output() {
        const SECRET: &str = "alice@example.com";
        let (base_url, seen) = redaction_echo_upstream().await;
        let state = budgeted_state(&base_url, Box::new(NoBudget)).with_middleware_chain(
            guardrail_chain(GuardrailAction::Redact, r"[a-z]+@example\.com"),
        );
        let app = router(state);
        let request = |stream| {
            authorized("/v1/chat/completions")
                .body(Body::from(
                    serde_json::to_vec(&json!({
                        "model": "openai/gpt-4o",
                        "messages": [{"role": "user", "content": SECRET}],
                        "stream": stream,
                    }))
                    .unwrap(),
                ))
                .unwrap()
        };

        let buffered = app.clone().oneshot(request(false)).await.unwrap();
        assert_eq!(buffered.status(), StatusCode::OK);
        let buffered = buffered.into_body().collect().await.unwrap().to_bytes();
        let buffered: Value = serde_json::from_slice(&buffered).unwrap();
        assert_eq!(buffered["choices"][0]["message"]["content"], SECRET);

        let streamed = app.oneshot(request(true)).await.unwrap();
        assert_eq!(streamed.status(), StatusCode::OK);
        let streamed = streamed.into_body().collect().await.unwrap().to_bytes();
        let streamed = String::from_utf8(streamed.to_vec()).unwrap();
        assert!(streamed.contains(SECRET), "{streamed}");
        assert!(!streamed.contains("[AXOND:"), "{streamed}");

        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        let first = seen[0]["messages"][0]["content"].as_str().unwrap();
        let second = seen[1]["messages"][0]["content"].as_str().unwrap();
        assert_eq!(first, second, "the placeholder is stable across requests");
        assert!(!first.contains(SECRET));
        assert!(first.starts_with("[AXOND:"));
    }

    #[tokio::test]
    async fn guardrail_refusal_dispatches_nothing_records_no_usage_and_echoes_no_match() {
        const MATCHED: &str = "forbidden-secret";
        let (base_url, hits) =
            controllable_upstream(Arc::new(AtomicBool::new(true)), StatusCode::OK).await;
        let captured = CapturingSink::default();
        let state = two_target_state(&base_url, &base_url, "", captured.clone())
            .with_middleware_chain(guardrail_chain(GuardrailAction::Block, MATCHED));
        let response = router(state)
            .oneshot(
                authorized("/v1/chat/completions")
                    .body(Body::from(
                        serde_json::to_vec(&json!({
                            "model": "openai/gpt-4o",
                            "messages": [{"role": "user", "content": MATCHED}],
                        }))
                        .unwrap(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let text = String::from_utf8(body.to_vec()).unwrap();
        let body: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(body["error"]["type"], "middleware_refused");
        assert_eq!(
            body["error"]["message"],
            "request refused by middleware: policy"
        );
        assert!(!text.contains(MATCHED));
        assert_eq!(hits.load(Ordering::SeqCst), 0);
        assert!(captured.0.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn an_empty_chain_keeps_the_two_estimate_passes_identical() {
        let body = json!({
            "model": "openai/gpt-4o",
            "messages": [{"role": "user", "content": "hello"}],
            "max_tokens": 12
        });
        let mut request = ProviderRequest {
            model: "gpt-4o".to_owned(),
            body: body.clone(),
        };
        let state = MiddlewareChain::empty()
            .request_isolated(&mut request)
            .await
            .expect("empty chain");
        assert_eq!(request.body, body);
        assert_eq!(
            Route::ChatCompletions.estimate(&request.body),
            Route::ChatCompletions.estimate(&body)
        );
        assert!(state.is_empty());
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

    /// The wire body may fit while request middleware expands the body beyond
    /// the same configured ceiling. The expanded body is refused before any
    /// provider dispatch, preserving `max_request_bytes` as an outbound-memory
    /// invariant as well as an inbound parsing bound.
    #[tokio::test]
    async fn middleware_cannot_expand_a_request_past_the_byte_limit() {
        let (base_url, hits) =
            controllable_upstream(Arc::new(AtomicBool::new(true)), StatusCode::OK).await;
        let state = admitting_state(&base_url, "max_request_bytes = 512", Box::new(NoBudget))
            .with_middleware_chain(BodyGrowthMiddleware::chain(1_024));
        let response = router(state)
            .oneshot(chat_request())
            .await
            .expect("typed middleware size refusal");

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
        for mode in [CoreAccountingMode::Middleware, CoreAccountingMode::Legacy] {
            let budget = RecordingBudget::default();
            let state = budgeted_state_with_limiter_mode(
                "http://127.0.0.1:1",
                Box::new(budget.clone()),
                Box::new(UnavailableLimiter),
                mode,
            );

            let response = router(state).oneshot(chat_request()).await.unwrap();
            assert_eq!(
                response.status(),
                StatusCode::SERVICE_UNAVAILABLE,
                "{mode:?}"
            );
            let bytes = response.into_body().collect().await.unwrap().to_bytes();
            let body: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(body["error"]["type"], "rate_limit_unavailable", "{mode:?}");
            assert!(budget.0.lock().unwrap().is_empty(), "{mode:?}");
        }
    }

    #[tokio::test]
    async fn budget_denial_does_not_leave_limiter_saturated() {
        for mode in [CoreAccountingMode::Middleware, CoreAccountingMode::Legacy] {
            let limiter = Arc::new(InMemoryRateLimiter::new(1, 10));
            let state = budgeted_state_with_limiter_mode(
                "http://127.0.0.1:1",
                Box::new(RejectingBudget),
                Box::new(SharedLimiter(Arc::clone(&limiter))),
                mode,
            );
            for _ in 0..2 {
                let response = router(state.clone()).oneshot(chat_request()).await.unwrap();
                assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS, "{mode:?}");
                let bytes = response.into_body().collect().await.unwrap().to_bytes();
                let body: Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(body["error"]["type"], "budget_exceeded", "{mode:?}");
            }
        }
    }

    #[tokio::test]
    async fn middleware_owned_limiter_follows_the_buffered_response_body() {
        let (base_url, _) =
            controllable_upstream(Arc::new(AtomicBool::new(true)), StatusCode::OK).await;
        let limiter = Arc::new(InMemoryRateLimiter::new(1, 10));
        let state = budgeted_state_with_limiter_mode(
            &base_url,
            Box::new(NoBudget),
            Box::new(SharedLimiter(Arc::clone(&limiter))),
            CoreAccountingMode::Middleware,
        );
        let key = RateLimitKey {
            namespace: "platform".to_owned(),
            subject: "AXOND_INBOUND_KEY".to_owned(),
        };

        let response = router(state).oneshot(chat_request()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            matches!(
                limiter.acquire(&key).await,
                Err(crate::rate_limit::RateLimitError::Exceeded)
            ),
            "the response body must still own the request's only permit"
        );

        drop(response);
        drop(
            limiter
                .acquire(&key)
                .await
                .expect("dropping the response body released its permit"),
        );
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
            namespace_grant: None,
            attrs: None,
        };
        let body = json!({"model": "openai/gpt-4o", "messages": []});

        let error = serve(
            state,
            HeaderMap::new(),
            body,
            Route::ChatCompletions,
            snapshot,
            caller,
            None,
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
            namespace_grant: None,
            attrs: None,
        };
        let body = json!({"model": "openai/gpt-4o", "messages": []});

        let response = serve(
            state,
            HeaderMap::new(),
            body,
            Route::ChatCompletions,
            snapshot,
            caller,
            None,
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
        for mode in [CoreAccountingMode::Middleware, CoreAccountingMode::Legacy] {
            let budget = RecordingBudget::default();
            let state = budgeted_state_with_limiter_mode(
                &base_url,
                Box::new(budget.clone()),
                Box::new(NoLimit),
                mode,
            );

            let resp = router(state).oneshot(chat_request()).await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK, "{mode:?}");

            let ledger = budget.0.lock().unwrap();
            let (estimated, settled) = ledger[0];
            // 10 input + 5 output tokens at 1 µ$ each.
            assert_eq!(settled, 15, "{mode:?}");
            assert!(
                estimated > settled,
                "the estimate should be the conservative ceiling ({estimated} vs {settled}); {mode:?}"
            );
        }
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
        for mode in [CoreAccountingMode::Middleware, CoreAccountingMode::Legacy] {
            let budget = RecordingBudget::default();
            let state = budgeted_state_with_limiter_mode(
                &base_url,
                Box::new(budget.clone()),
                Box::new(NoLimit),
                mode,
            );

            let resp = router(state).oneshot(chat_request()).await.unwrap();
            assert_eq!(resp.status(), StatusCode::BAD_GATEWAY, "{mode:?}");

            let ledger = budget.0.lock().unwrap();
            assert_eq!(ledger.len(), 1, "{mode:?}");
            assert_eq!(ledger[0].1, 0, "{mode:?}");
        }
    }

    /// A cancelled buffered handler drops its reservation guard while the
    /// dispatcher is waiting for the provider; the detached release must run
    /// before the next request can observe the ledger.
    #[tokio::test]
    async fn a_cancelled_buffered_request_releases_its_reservation() {
        for mode in [CoreAccountingMode::Middleware, CoreAccountingMode::Legacy] {
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
            let limiter = Arc::new(InMemoryRateLimiter::new(1, 10));
            let state = budgeted_state_with_limiter_mode(
                &format!("http://{addr}"),
                Box::new(SharedBudget(budget.clone())),
                Box::new(SharedLimiter(Arc::clone(&limiter))),
                mode,
            );
            let request = tokio::spawn(router(state).oneshot(chat_request()));
            started_rx.await.unwrap();
            request.abort();
            assert!(request.await.unwrap_err().is_cancelled(), "{mode:?}");

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
                    "reservation was not released before timeout; {mode:?}; outstanding={outstanding}"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            drop(
                limiter
                    .acquire(&RateLimitKey {
                        namespace: "platform".to_owned(),
                        subject: "AXOND_INBOUND_KEY".to_owned(),
                    })
                    .await
                    .expect("cancelled request released its rate-limit permit"),
            );
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

    #[tokio::test]
    async fn store_unavailable_deny_is_budget_unavailable_and_skips_upstream() {
        let (base_url, hits) =
            controllable_upstream(Arc::new(AtomicBool::new(true)), StatusCode::OK).await;
        let budget = crate::budget::StoreBudget::new(
            Arc::new(crate::store::UnavailableStore),
            crate::config::StoreUnavailable::Deny,
            Duration::from_secs(30),
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
            Duration::from_secs(30),
        );
        let resp = router(budgeted_state(&base_url, Box::new(budget)))
            .oneshot(chat_request())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(hits.load(Ordering::SeqCst), 1);
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
            let body = serde_json::to_vec(
                &json!({"model": "openai/gpt-4o", "messages": [], "max_tokens": 16}),
            )
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
            Box::new(NoLimit),
            Box::new(crate::revocation::NoDenylist),
            ReplicaObservability::stateless(),
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
        crate::streaming::await_settlements(Duration::from_secs(2)).await;

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
                catalogue: None,
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
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
    async fn status_refuses_a_token_without_the_capability() {
        let state = status_state(ReplicaObservability {
            status: observed_registry(),
            revision: None,
            catalogue: None,
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
            catalogue: None,
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

    /// Catalogue freshness is observable where an operator already looks, from
    /// what the background import last published: the read is a memory read, no
    /// upstream is contacted for it, and a tenant-scoped caller learns nothing
    /// about the deployment's metadata source (#146).
    #[tokio::test]
    async fn status_reports_catalogue_freshness_to_the_operator_only() {
        let handle = crate::backends::catalog_runtime::start(
            &crate::config::CatalogConfig {
                source: crate::config::CatalogSourceBackend::Seed,
                store: crate::config::CatalogStoreBackend::InMemory,
                bootstrap: crate::config::CatalogBootstrap::Seed,
                ..crate::config::CatalogConfig::default()
            },
            None,
            &HashMap::new(),
            std::future::pending(),
        )
        .await
        .expect("an offline catalogue starts")
        .expect("an enabled catalogue yields a handle");
        let (observability, refresher) =
            ReplicaObservability::stateless_with_catalogue(Arc::clone(handle.status()));
        refresher.refresh_once().await;
        let state = status_state(observability);

        let (status, body) = status_response(state.clone(), Some(OPERATOR_KEY)).await;
        assert_eq!(status, StatusCode::OK);
        let catalogue = &body["catalogue"];
        assert!(
            catalogue.is_object(),
            "the operator sees the catalogue: {body}"
        );
        assert_eq!(catalogue["consecutive_refusals"], 0);
        assert_eq!(catalogue["persistent_refusal"], false);
        assert!(
            catalogue["active_age_ms"].is_number(),
            "freshness is what an operator acts on: {body}"
        );
        assert!(
            catalogue["content_id"].is_string(),
            "the operator learns which content is active: {body}"
        );
        let rendered = body.to_string();
        assert!(
            !rendered.contains("models.dev"),
            "the summary names no upstream URL: {rendered}"
        );
        let catalogue_component = body["components"]
            .as_array()
            .expect("components")
            .iter()
            .find(|entry| entry["component"] == "catalogue")
            .expect("catalogue component");
        assert_eq!(catalogue_component["state"], "ok", "{body}");

        let (status, body) = status_response(state, Some(TENANT_KEY)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["scope"], "namespace");
        assert!(
            body.get("catalogue").is_none_or(Value::is_null),
            "a tenant learns nothing about the deployment's catalogue: {body}"
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
            catalogue: None,
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
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
    async fn a_minted_status_token_is_not_the_operator() {
        let state = status_state(ReplicaObservability {
            status: observed_registry(),
            revision: None,
            catalogue: None,
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
    #[ignore = "ADR 0063: minted tokens / per-key namespace isolation withdrawn"]
    async fn a_revocation_outage_leaves_only_the_operator_key_reading_status() {
        let unavailable = || {
            status_state_with_revocation(
                ReplicaObservability {
                    status: observed_registry(),
                    revision: None,
                    catalogue: None,
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
                catalogue: None,
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
            catalogue: None,
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

    /// What a released binary answers in the *stateless* posture, which is the
    /// default one: no store is opened, so nothing is observed and no revision
    /// is tracked, and every component is `disabled` with reason
    /// `not_configured`. `disabled` rather than `unavailable` is the whole
    /// point — a deployment without a control plane is not a deployment with a
    /// broken one, and the shipped alerts are written not to page for it.
    #[tokio::test]
    async fn a_stateless_replica_observes_nothing_and_says_so() {
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

    /// The stateful posture, through the wiring a released binary uses:
    /// [`ReplicaObservability::observing`] over the store administration was
    /// built on. One refresh round is enough — the read is a cache read, so what
    /// the route reports is exactly what the last round published.
    ///
    /// The two halves that matter are both here: the control plane is observed
    /// *live*, and every component this deployment does not have is still
    /// `disabled` rather than being reported as broken because nobody probed it.
    #[tokio::test]
    async fn a_stateful_replica_observes_the_control_plane_it_administers() {
        let oracle = Arc::new(crate::desired_state::oracle::InMemoryControlPlane::new());
        let (observability, refresher) =
            ReplicaObservability::observing(ReplicaObservability::plan(
                Some((
                    Arc::clone(&oracle)
                        as Arc<dyn crate::backends::control_plane::ControlPlaneStore>,
                    crate::status::probes::ControlPlaneProbe::pacing(
                        &crate::backends::control_plane::postgres::ControlPlaneSettings::default(),
                    ),
                )),
                &crate::budget::NoBudget,
                &crate::rate_limit::NoLimit,
                &crate::revocation::NoDenylist,
            ));
        let refresher = refresher.expect("an observed control plane is refreshed");
        let state = status_state(observability);

        refresher.refresh_once().await;
        let (status, body) = status_response(state.clone(), Some(OPERATOR_KEY)).await;
        assert_eq!(status, StatusCode::OK);
        let component = |body: &serde_json::Value, name: &str| {
            body["components"]
                .as_array()
                .expect("components")
                .iter()
                .find(|entry| entry["component"] == name)
                .cloned()
                .unwrap_or_else(|| panic!("{name} is reported"))
        };
        assert_eq!(component(&body, "control_plane")["state"], "ok", "{body}");
        assert_eq!(component(&body, "secret_store")["state"], "disabled");
        assert_eq!(component(&body, "budget_store")["state"], "disabled");

        // An outage moves the observation, and nothing else: the replica keeps
        // answering, and the reason is the bounded code rather than the store's
        // own message.
        oracle.set_unavailable(true);
        refresher.refresh_once().await;
        let (status, body) = status_response(state, Some(OPERATOR_KEY)).await;
        assert_eq!(status, StatusCode::OK);
        let control_plane = component(&body, "control_plane");
        assert_eq!(control_plane["state"], "unavailable", "{body}");
        assert_eq!(control_plane["reason"], "unreachable", "{body}");
        assert!(
            !body
                .to_string()
                .contains("fake control plane is unavailable"),
            "the backend's own message reached the response: {body}"
        );
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
            catalogue: None,
        });
        let lifecycle = Arc::clone(state.lifecycle());
        lifecycle.close();

        let served = router(state.clone())
            .oneshot(
                Request::get("/ns/platform/v1/models")
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
            catalogue: None,
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
            catalogue: None,
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
            catalogue: None,
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
            (
                DiagnosticCredential::Anonymous,
                crate::admission::MAX_AUTHENTICATING_DIAGNOSTIC_ANONYMOUS,
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
    /// because its shares cost different things: a minted token is a
    /// revocation-store round trip, a static key a comparison in memory, an
    /// anonymous caller nothing at all. Pooled, a store that is *slow* rather
    /// than down would park every permit in token verifications and refuse the
    /// operator's static key — the credential the runbook sends through exactly
    /// that outage — and a flood needing no credential at all would do the same
    /// for free.
    #[tokio::test]
    async fn a_token_flood_cannot_close_the_route_to_a_static_key() {
        let state = status_state(ReplicaObservability {
            status: observed_registry(),
            revision: None,
            catalogue: None,
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

        // Nor can a flood that presents nothing at all, which needs no
        // credential to mount and so would otherwise be the cheapest way to
        // hold the route shut.
        let anonymous: Vec<_> = (0..crate::admission::MAX_AUTHENTICATING_DIAGNOSTIC_ANONYMOUS)
            .map(|_| {
                state
                    .0
                    .admission
                    .admit_diagnostic_authentication(DiagnosticCredential::Anonymous)
                    .expect("under the anonymous share")
            })
            .collect();
        let (status, body) = status_response(state.clone(), None).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
        let (status, body) = status_response(state.clone(), Some(OPERATOR_KEY)).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "a credential-less flood held the route closed against a static key: {body}"
        );

        drop(anonymous);
        drop(flood);
    }

    /// The outer permit bounds *authenticating*, so it is given back where
    /// authentication ends — `authenticate_middleware` takes it off the request
    /// before the handler runs — rather than where the response does. Otherwise
    /// a share would drain at the speed of answering, and a slow reader rather
    /// than an expensive credential would be what closed the route.
    #[tokio::test]
    async fn an_answered_read_does_not_keep_its_authentication_permit() {
        let state = status_state(ReplicaObservability {
            status: observed_registry(),
            revision: None,
            catalogue: None,
        });
        // One request per permit in the in-memory share, answered in sequence:
        // if the permit outlived authentication these would exhaust it, since
        // nothing here waits for a previous one to be released.
        for _ in 0..crate::admission::MAX_AUTHENTICATING_DIAGNOSTIC_KEYS + 1 {
            let (status, body) = status_response(state.clone(), Some(OPERATOR_KEY)).await;
            assert_eq!(status, StatusCode::OK, "{body}");
        }

        // Every one of them is back, not merely most of them.
        let held: Vec<_> = (0..crate::admission::MAX_AUTHENTICATING_DIAGNOSTIC_KEYS)
            .map(|_| {
                state
                    .0
                    .admission
                    .admit_diagnostic_authentication(DiagnosticCredential::Local)
                    .expect("the answered reads returned every permit they took")
            })
            .collect();
        drop(held);
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
            catalogue: None,
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
                catalogue: None,
            });
            let app = router(state).merge(surface);
            // With a credential and without one alike: a wrong method is a
            // protocol mistake, and every neighbouring administrative path
            // answers it before authentication, so answering `401` here would
            // send a caller looking for a credential that would not have
            // helped.
            for credential in [Some(format!("Bearer {OPERATOR_KEY}")), None] {
                let case = format!(
                    "{posture}/{}",
                    credential.as_ref().map_or("anonymous", |_| "operator")
                );
                let mut request = Request::post("/admin/v1/status");
                if let Some(credential) = credential {
                    request = request.header(axum::http::header::AUTHORIZATION, credential);
                }
                let response = app
                    .clone()
                    .oneshot(request.body(Body::empty()).unwrap())
                    .await
                    .unwrap();
                assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED, "{case}");
                // RFC 9110 requires it on a 405, and axum sets it from the
                // method router rather than from the fallback body.
                assert!(
                    response.headers().contains_key(axum::http::header::ALLOW),
                    "{case}"
                );
                let bytes = response.into_body().collect().await.unwrap().to_bytes();
                let body: Value = serde_json::from_slice(&bytes)
                    .unwrap_or_else(|_| panic!("{case}: a typed envelope, not an empty body"));
                assert_eq!(body["error"]["type"], "admin_method_not_allowed", "{case}");
            }
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

    /// Stateful administration and inference share an HTTP server, not a data
    /// source. The serving snapshot is the complete request-path dependency:
    /// once it has been published, an inference request must not consult the
    /// durable control plane that administration continues to read.
    #[tokio::test]
    async fn inference_reads_zero_control_plane_state_on_the_main_route_graph() {
        const ADMIN_TOKEN: &str = "stateful-admin-token";

        let control_plane = Arc::new(crate::desired_state::oracle::InMemoryControlPlane::new());
        let counting = Arc::new(crate::admin::fakes::CountingStore::new(control_plane));
        let administration =
            crate::admin::router::router(Arc::new(crate::admin::router::AdminApi::new(
                Arc::new(crate::admin::service::AdminService::stateful(
                    counting.clone(),
                )),
                Arc::new(
                    crate::admin::fakes::FakeAdminAuthenticator::new().with_human(
                        ADMIN_TOKEN,
                        "https://issuer.example",
                        "stateful-operator",
                    ),
                ),
                Arc::new(crate::admin::fakes::FakeAdminAuthorizer::permissive()),
            )));
        let (base_url, upstream_hits) =
            controllable_upstream(Arc::new(AtomicBool::new(true)), StatusCode::OK).await;

        // This is the same composition main serves: the inference router over
        // its active snapshot, merged with the stateful administrative router.
        let app = router(test_state_with_base_url(&base_url)).merge(administration);
        let admin_read = || {
            Request::get("/admin/v1/state")
                .header(
                    axum::http::header::AUTHORIZATION,
                    format!("Bearer {ADMIN_TOKEN}"),
                )
                .body(Body::empty())
                .expect("admin request")
        };

        let before_admin = counting.calls();
        let response = app
            .clone()
            .oneshot(admin_read())
            .await
            .expect("admin response");
        assert_eq!(response.status(), StatusCode::OK);
        let after_first_admin = counting.calls();
        assert!(
            after_first_admin > before_admin,
            "an authenticated administrative read did not consult the control plane"
        );

        let response = app
            .clone()
            .oneshot(chat_request())
            .await
            .expect("inference response");
        assert_eq!(response.status(), StatusCode::OK);
        let body: Value = serde_json::from_slice(
            &response
                .into_body()
                .collect()
                .await
                .expect("complete inference body")
                .to_bytes(),
        )
        .expect("inference JSON");
        assert_eq!(body["id"], "resp-1", "the serving snapshot dispatched");
        assert_eq!(upstream_hits.load(Ordering::SeqCst), 1);
        assert_eq!(
            counting.calls(),
            after_first_admin,
            "authenticated inference consulted the control plane"
        );

        let response = app
            .oneshot(admin_read())
            .await
            .expect("second admin response");
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            counting.calls() > after_first_admin,
            "the second administrative read did not consult the control plane"
        );
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
            catalogue: None,
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
                catalogue: None,
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
                catalogue: None,
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
            catalogue: None,
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
            catalogue: None,
        });
        let (_clock, lagging_status) = lagging_replica();
        let lagging = status_state(ReplicaObservability {
            status: observed_registry(),
            revision: Some(lagging_status),
            catalogue: None,
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
            catalogue: None,
        });
        let (status, body) = status_response(state, Some(TENANT_KEY)).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.get("revision").is_none(), "{body}");
    }
}
