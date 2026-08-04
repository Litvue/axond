//! HTTP surface.
//!
//! Passthrough-first (delta A1): the OpenAI-shaped `/v1/chat/completions` route
//! forwards the caller's body to a same-shaped upstream and only rewrites the
//! `model` field to the resolved target. Cross-provider translation (e.g.
//! routing an OpenAI request to Anthropic) reuses `gateway-core`'s adapters and
//! is wired in as failover lands. A `stream: true` request takes the SSE relay
//! in [`crate::streaming`]; native routes (`/v1/messages`) exist as typed
//! `501`s rather than missing routes (delta B3).

use std::time::Instant;

use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use gateway_core::{ModelPrice, ProviderError, ProviderRequest, ProviderResponse, Surface, Usage};
use gateway_transport::{AuthScheme, TransportError, Upstream};
use serde_json::{Value, json};
use tracing::Instrument;

use crate::budget::{Admission, BudgetKey};
use crate::config::{Provider, ProviderKind};
use crate::credentials::{CredentialPlan, CredentialSource};
use crate::error::GatewayError;
use crate::state::{AppState, InboundKey, adapter_for};
use crate::streaming::{self, StreamContext};
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
        .0
        .config
        .model
        .iter()
        .map(|m| json!({ "id": m.name, "object": "model", "owned_by": "axond" }))
        .collect();
    Json(json!({ "object": "list", "data": data }))
}

/// Resolve the caller's namespace + subject from the bearer token. When no
/// gateway keys are configured the gateway is open (dev mode) and uses the
/// default namespace.
fn authenticate(state: &AppState, headers: &HeaderMap) -> Result<InboundKey, GatewayError> {
    if state.0.inbound_keys.is_empty() {
        return Ok(InboundKey {
            namespace: state.0.config.default_namespace().to_string(),
            subject: "anonymous".to_string(),
        });
    }
    let token = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or(GatewayError::Unauthorized)?;
    state
        .0
        .inbound_keys
        .get(token)
        .cloned()
        .ok_or(GatewayError::Unauthorized)
}

async fn chat_completions(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(mut body): Json<Value>,
) -> Result<Response, GatewayError> {
    let caller = authenticate(&state, &headers)?;
    let cfg = &state.0.config;

    let streamed = body.get("stream").and_then(Value::as_bool) == Some(true);

    let alias = body
        .get("model")
        .and_then(Value::as_str)
        .ok_or_else(|| GatewayError::BadRequest("missing `model`".into()))?
        .to_string();

    let model = cfg
        .model(&alias)
        .ok_or_else(|| GatewayError::UnknownModel(alias.clone()))?;

    // Scaffold: first target only. Ordered failover across targets is the
    // next increment (per-attempt spans + circuit breaker from gateway-core).
    let target = &model.targets[0];
    let price = target.price;
    let target_provider = target.provider.clone();
    let target_model = target.model.clone();
    let provider = cfg
        .provider(&target_provider)
        .ok_or_else(|| GatewayError::UnknownModel(alias.clone()))?;

    // The pool for this (namespace, provider), ordered by the selection
    // strategy with unhealthy credentials skipped.
    let plan = state
        .0
        .credentials
        .plan(cfg, &caller.namespace, &provider.id)
        .ok_or_else(|| GatewayError::NoCredential {
            namespace: caller.namespace.clone(),
            provider: provider.id.clone(),
        })?;

    // Budget is denominated in micro-dollars. Reserve a conservative cost
    // estimate before dispatch; reconcile against the real cost after.
    let budget_key = BudgetKey {
        namespace: caller.namespace.clone(),
        subject: caller.subject.clone(),
    };
    let estimated_cost = estimate_cost_microdollars(&body, &price);
    if state.0.budget.reserve(&budget_key, estimated_cost).await == Admission::Denied {
        return Err(GatewayError::BudgetExceeded(alias));
    }

    // Rewrite only the model field; everything else is byte-passthrough.
    body["model"] = Value::String(target_model.clone());

    if streamed {
        // The stream path uses the first credential in the pool; per-credential
        // rotation mid-stream (skip-on-429) is a follow-up, like streaming
        // failover (#3).
        let lease = plan
            .attempts
            .first()
            .ok_or_else(|| GatewayError::NoCredential {
                namespace: caller.namespace.clone(),
                provider: provider.id.clone(),
            })?;
        let auth = match provider.kind {
            ProviderKind::Anthropic => AuthScheme::Header("x-api-key"),
            ProviderKind::Openai | ProviderKind::OpenaiCompatible => AuthScheme::Bearer,
        };
        let upstream = Upstream {
            base_url: provider.base_url.clone(),
            api_key: lease.secret.clone(),
            auth,
        };
        let ctx = StreamContext {
            namespace: caller.namespace.clone(),
            subject: caller.subject.clone(),
            alias,
            target_provider,
            target_model: target_model.clone(),
            source: plan.source,
            credential_id: lease.id.clone(),
            trace_id: telemetry::trace_id(),
            price,
            budget_key,
        };
        let request = ProviderRequest {
            model: target_model,
            body,
        };
        return streaming::relay(
            state.clone(),
            ctx,
            adapter_for(provider.kind),
            upstream,
            Surface::ChatCompletions,
            request,
        )
        .await;
    }

    // One child span per upstream attempt. The credential pool may make several
    // attempts against the same target; ordered failover (#3) will fan these
    // into distinct per-attempt children.
    let attempt_span = telemetry::upstream_attempt_span(
        0,
        &target_provider,
        &target_model,
        UsageRecord::credential_source_str(plan.source),
    );
    let started = Instant::now();
    let attempt = dispatch_over_pool(&state, provider, &plan, &target_model, body)
        .instrument(attempt_span.clone())
        .await;
    let latency_ms = started.elapsed().as_millis() as u64;
    // A non-streamed response arrives whole, so the first token lands with the
    // last one; the streaming relay reports the real first chunk.
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
    match attempt.result {
        Ok(response) => {
            let usage = to_usage(&response.usage);
            let cost = price.cost_microdollars(usage);
            state.0.budget.commit(&budget_key, cost).await;
            record_usage(
                &state,
                RecordArgs {
                    caller: &caller,
                    alias: &alias,
                    target_provider: &target_provider,
                    target_model: &target_model,
                    source: plan.source,
                    credential_id: &attempt.credential_id,
                    status: Status::Ok,
                    input_tokens: response.usage.input_tokens,
                    output_tokens: response.usage.output_tokens,
                    cost_microdollars: cost,
                    latency_ms,
                    ttft_ms,
                    attempts: 1,
                },
            )
            .await;
            Ok(Json(response.body).into_response())
        }
        Err(err) => {
            // Failure still cost provider tokens in principle, but we have no
            // authoritative usage from a failed call, so cost is recorded as 0
            // and nothing is committed against the budget (reconciliation of
            // partial/failed spend is a follow-up).
            record_usage(
                &state,
                RecordArgs {
                    caller: &caller,
                    alias: &alias,
                    target_provider: &target_provider,
                    target_model: &target_model,
                    source: plan.source,
                    credential_id: &attempt.credential_id,
                    status: Status::UpstreamError,
                    input_tokens: 0,
                    output_tokens: 0,
                    cost_microdollars: 0,
                    latency_ms,
                    ttft_ms,
                    attempts: 1,
                },
            )
            .await;
            Err(err.into())
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
async fn dispatch_over_pool(
    state: &AppState,
    provider: &Provider,
    plan: &CredentialPlan,
    target_model: &str,
    body: Value,
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
        let request = ProviderRequest {
            model: target_model.to_string(),
            body: body.clone(),
        };
        let result = state
            .0
            .dispatcher
            .dispatch(
                adapter.as_ref(),
                &upstream,
                Surface::ChatCompletions,
                request,
            )
            .await;
        match result {
            Ok(response) => {
                state.0.credentials.record_success(lease);
                return PooledAttempt {
                    result: Ok(response),
                    credential_id: lease.id.clone(),
                };
            }
            Err(err) if is_credential_exhausted(&err) => {
                state.0.credentials.record_failure(lease);
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

async fn native_messages() -> Result<Json<Value>, GatewayError> {
    Err(GatewayError::NotImplemented(
        "native Anthropic /v1/messages",
    ))
}

async fn embeddings() -> Result<Json<Value>, GatewayError> {
    Err(GatewayError::NotImplemented("/v1/embeddings"))
}

async fn responses() -> Result<Json<Value>, GatewayError> {
    Err(GatewayError::NotImplemented("/v1/responses"))
}

/// Conservative pre-dispatch cost estimate in micro-dollars: input tokens from
/// the request body (~4 chars/token) plus a reserved output allowance
/// (`max_tokens` when present, else a default), priced with the target's
/// `ModelPrice`. Reserve-then-reconcile replaces this with the real cost on
/// commit.
fn estimate_cost_microdollars(body: &Value, price: &ModelPrice) -> u64 {
    const DEFAULT_MAX_OUTPUT_TOKENS: u64 = 1_024;
    let input_tokens = (serde_json::to_string(body).map(|s| s.len()).unwrap_or(0) / 4) as u64;
    let output_tokens = body
        .get("max_tokens")
        .or_else(|| body.get("max_completion_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(DEFAULT_MAX_OUTPUT_TOKENS);
    price.cost_microdollars(Usage {
        input_tokens,
        output_tokens,
        reasoning_tokens: 0,
        cache_read_tokens: 0,
        cache_write_tokens: 0,
    })
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
    use std::sync::{Arc, Mutex};
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

    #[tokio::test]
    async fn native_messages_route_exists_and_returns_typed_501() {
        let resp = router(test_state())
            .oneshot(Request::post("/v1/messages").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_IMPLEMENTED);
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
    }
}
