//! HTTP surface.
//!
//! Passthrough-first (delta A1): the OpenAI-shaped `/v1/chat/completions` route
//! forwards the caller's body to a same-shaped upstream and only rewrites the
//! `model` field to the resolved target. Cross-provider translation (e.g.
//! routing an OpenAI request to Anthropic) reuses `gateway-core`'s adapters and
//! is wired in as failover lands. Native routes (`/v1/messages`) and streaming
//! exist as typed `501`s rather than missing routes (delta B3).

use std::time::Instant;

use axum::extract::State;
use axum::http::HeaderMap;
use axum::routing::{get, post};
use axum::{Json, Router};
use gateway_core::{ProviderRequest, Surface};
use gateway_transport::{AuthScheme, Upstream};
use serde_json::{Value, json};

use crate::config::ProviderKind;
use crate::credentials::CredentialSource;
use crate::error::GatewayError;
use crate::quota::{Admission, QuotaKey};
use crate::state::{AppState, InboundKey, adapter_for};
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
        .map(|m| json!({ "id": m.name, "object": "model", "owned_by": "tollgate" }))
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
) -> Result<Json<Value>, GatewayError> {
    let caller = authenticate(&state, &headers)?;
    let cfg = &state.0.config;

    if body.get("stream").and_then(Value::as_bool) == Some(true) {
        return Err(GatewayError::NotImplemented("streaming (SSE relay)"));
    }

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
    let provider = cfg
        .provider(&target.provider)
        .ok_or_else(|| GatewayError::UnknownModel(alias.clone()))?;

    let resolved = state
        .0
        .credentials
        .resolve(cfg, &caller.namespace, &provider.id)
        .ok_or_else(|| GatewayError::NoCredential {
            namespace: caller.namespace.clone(),
            provider: provider.id.clone(),
        })?;

    let quota_key = QuotaKey {
        namespace: caller.namespace.clone(),
        model: alias.clone(),
    };
    let estimate = estimate_tokens(&body);
    if state.0.quota.reserve(&quota_key, estimate).await == Admission::Denied {
        return Err(GatewayError::QuotaExceeded(alias));
    }

    // Rewrite only the model field; everything else is byte-passthrough.
    body["model"] = Value::String(target.model.clone());

    let auth = match provider.kind {
        ProviderKind::Anthropic => AuthScheme::Header("x-api-key"),
        ProviderKind::Openai | ProviderKind::OpenaiCompatible => AuthScheme::Bearer,
    };
    let upstream = Upstream {
        base_url: provider.base_url.clone(),
        api_key: resolved.secret,
        auth,
    };

    let adapter = adapter_for(provider.kind);
    let request = ProviderRequest {
        model: target.model.clone(),
        body,
    };

    let started = Instant::now();
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
    let latency_ms = started.elapsed().as_millis() as u64;

    match result {
        Ok(response) => {
            state
                .0
                .quota
                .commit(&quota_key, response.usage.total_tokens())
                .await;
            record_usage(
                &state,
                &caller,
                &alias,
                &target.provider,
                &target.model,
                resolved.source,
                Status::Ok,
                response.usage.input_tokens,
                response.usage.output_tokens,
                latency_ms,
            )
            .await;
            Ok(Json(response.body))
        }
        Err(err) => {
            record_usage(
                &state,
                &caller,
                &alias,
                &target.provider,
                &target.model,
                resolved.source,
                Status::UpstreamError,
                0,
                0,
                latency_ms,
            )
            .await;
            Err(err.into())
        }
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

/// Conservative pre-dispatch token estimate (~4 chars/token over the request
/// body). Reserve-then-reconcile replaces this with the real count on commit.
fn estimate_tokens(body: &Value) -> u64 {
    (serde_json::to_string(body).map(|s| s.len()).unwrap_or(0) / 4) as u64
}

/// Monotonic per-process request id. A real deploy layers this behind the
/// inbound `traceparent` so the id joins the OTel trace.
fn next_request_id() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(1);
    format!("req_{:016x}", COUNTER.fetch_add(1, Ordering::Relaxed))
}

#[allow(clippy::too_many_arguments)]
async fn record_usage(
    state: &AppState,
    caller: &InboundKey,
    alias: &str,
    target_provider: &str,
    target_model: &str,
    source: CredentialSource,
    status: Status,
    input_tokens: u64,
    output_tokens: u64,
    latency_ms: u64,
) {
    let record = UsageRecord {
        schema_version: UsageRecord::SCHEMA_VERSION,
        request_id: next_request_id(),
        namespace: caller.namespace.clone(),
        subject: caller.subject.clone(),
        model: alias.to_string(),
        target_provider: target_provider.to_string(),
        target_model: target_model.to_string(),
        credential_source: UsageRecord::credential_source_str(source),
        status,
        input_tokens,
        output_tokens,
        cost_microdollars: 0,
        catalog_version: 0,
        latency_ms,
    };
    state.0.usage.record(&record).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::quota::NoQuota;
    use crate::usage::{StdoutSink, UsageFanout, UsageSink};
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use std::collections::HashMap;
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
targets = [{ provider = "openai", model = "gpt-4o" }]
"#,
        )
        .unwrap();
        let sinks: Vec<Box<dyn UsageSink>> = vec![Box::new(StdoutSink)];
        AppState::new(
            cfg,
            &HashMap::new(),
            UsageFanout::new(sinks),
            Box::new(NoQuota),
        )
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
}
