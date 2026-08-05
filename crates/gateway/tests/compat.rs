//! Provider-SDK compatibility, black-box against a real `axond` process.
//!
//! A raw OpenAI-compatible HTTP client drives the gateway here; the same
//! surface is driven by the vendors' own Python SDKs in `tests/compat/`, which
//! runs as its own CI lane (ADR 0014).

mod support;

use serde_json::{Value, json};
use support::gateway::{ANTHROPIC_KEY, OPENAI_KEY, alias};
use support::{GATEWAY_KEY, boot, client, target, upstream};

#[tokio::test]
async fn buffered_chat_completions_round_trips() {
    let (upstream_server, gateway) = boot().await;
    let response = client()
        .post(gateway.url("/v1/chat/completions"))
        .bearer_auth(GATEWAY_KEY)
        .json(&json!({
            "model": alias::CHAT,
            "messages": [{ "role": "user", "content": "What is the capital of France?" }],
            "temperature": 0.2,
            "future_field": { "nested": [1, 2, 3] }
        }))
        .send()
        .await
        .expect("the gateway answers");

    assert_eq!(response.status(), 200);
    let body: Value = response.json().await.expect("a JSON body");
    let fixture: Value =
        serde_json::from_slice(&upstream::fixture("openai/chat_completion.json")).expect("fixture");
    assert_eq!(body, fixture);

    let recorded = upstream_server.state.last_request();
    assert_eq!(recorded.path, "/chat/completions");
    // Only `model` is rewritten; everything else the caller sent survives.
    assert_eq!(recorded.model, target::CHAT);
    assert_eq!(
        recorded.body["messages"][0]["content"],
        json!("What is the capital of France?")
    );
    assert_eq!(recorded.body["temperature"], json!(0.2));
    assert_eq!(
        recorded.body["future_field"],
        json!({ "nested": [1, 2, 3] })
    );
    // The caller's gateway key never travels upstream: the provider sees the
    // credential the pool leased.
    assert_eq!(
        recorded.authorization.as_deref(),
        Some(format!("Bearer {OPENAI_KEY}").as_str())
    );
}

#[tokio::test]
async fn buffered_native_messages_preserve_thinking_and_tool_use() {
    let (upstream_server, gateway) = boot().await;
    let response = client()
        .post(gateway.url("/v1/messages"))
        .header("x-api-key", GATEWAY_KEY)
        .header("anthropic-version", "2023-06-01")
        .header("anthropic-beta", "fixture-beta-2024-01-01")
        .json(&json!({
            "model": alias::MESSAGES,
            "max_tokens": 1024,
            "thinking": { "type": "enabled", "budget_tokens": 1024 },
            "messages": [{ "role": "user", "content": "Weather in Paris?" }]
        }))
        .send()
        .await
        .expect("the gateway answers");

    assert_eq!(response.status(), 200);
    let body: Value = response.json().await.expect("a JSON body");
    let fixture: Value = serde_json::from_slice(&upstream::fixture(
        "anthropic/message_thinking_tool_use.json",
    ))
    .expect("fixture");
    assert_eq!(body, fixture);
    assert_eq!(
        body["content"][0]["signature"],
        fixture["content"][0]["signature"]
    );
    assert_eq!(body["content"][2]["input"], fixture["content"][2]["input"]);

    let recorded = upstream_server.state.last_request();
    assert_eq!(recorded.path, "/messages");
    assert_eq!(recorded.model, target::MESSAGES);
    assert_eq!(recorded.api_key.as_deref(), Some(ANTHROPIC_KEY));
    assert_eq!(recorded.anthropic_version.as_deref(), Some("2023-06-01"));
    assert_eq!(
        recorded.anthropic_beta.as_deref(),
        Some("fixture-beta-2024-01-01")
    );
    assert_eq!(recorded.body["thinking"]["budget_tokens"], json!(1024));
}

#[tokio::test]
async fn buffered_embeddings_round_trip() {
    let (upstream_server, gateway) = boot().await;
    let response = client()
        .post(gateway.url("/v1/embeddings"))
        .bearer_auth(GATEWAY_KEY)
        .json(&json!({ "model": alias::EMBEDDINGS, "input": "hello" }))
        .send()
        .await
        .expect("the gateway answers");

    assert_eq!(response.status(), 200);
    let body: Value = response.json().await.expect("a JSON body");
    let fixture: Value =
        serde_json::from_slice(&upstream::fixture("openai/embeddings.json")).expect("fixture");
    assert_eq!(body, fixture);
    assert_eq!(upstream_server.state.last_request().path, "/embeddings");
}

#[tokio::test]
async fn streamed_chat_completions_relays_openai_framing() {
    let (_upstream, gateway) = boot().await;
    let body = stream_text(
        gateway.url("/v1/chat/completions"),
        json!({
            "model": alias::CHAT,
            "stream": true,
            "messages": [{ "role": "user", "content": "What is the capital of France?" }]
        }),
    )
    .await;

    let text: String = body
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .filter(|data| *data != "[DONE]")
        .filter_map(|data| serde_json::from_str::<Value>(data).ok())
        .filter_map(|chunk| {
            chunk
                .pointer("/choices/0/delta/content")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .collect();
    assert_eq!(text, "The capital of France is Paris.");
    assert!(body.trim_end().ends_with("data: [DONE]"), "{body}");
}

/// The promise the native route exists for: an Anthropic stream reaches the
/// caller as the exact bytes the provider sent, thinking signatures and
/// tool-use JSON deltas included.
#[tokio::test]
async fn streamed_native_messages_are_byte_faithful() {
    let (_upstream, gateway) = boot().await;
    let body = stream_text(
        gateway.url("/v1/messages"),
        json!({
            "model": alias::MESSAGES,
            "stream": true,
            "max_tokens": 1024,
            "messages": [{ "role": "user", "content": "Weather in Paris?" }]
        }),
    )
    .await;

    let fixture = upstream::fixture("anthropic/message_thinking_tool_use.sse");
    assert_eq!(body.as_bytes(), fixture.as_ref());
}

#[tokio::test]
async fn both_inbound_auth_schemes_are_accepted_and_nothing_else_is() {
    let (_upstream, gateway) = boot().await;
    let request = || {
        json!({
            "model": alias::CHAT,
            "messages": [{ "role": "user", "content": "hi" }]
        })
    };
    let post = |builder: reqwest::RequestBuilder| async {
        builder
            .json(&request())
            .send()
            .await
            .expect("the gateway answers")
            .status()
            .as_u16()
    };

    let url = gateway.url("/v1/chat/completions");
    assert_eq!(
        post(client().post(&url).bearer_auth(GATEWAY_KEY)).await,
        200
    );
    assert_eq!(
        post(client().post(&url).header("x-api-key", GATEWAY_KEY)).await,
        200
    );
    assert_eq!(
        post(client().post(&url).bearer_auth("wrong-key")).await,
        401
    );
    assert_eq!(post(client().post(&url)).await, 401);
}

async fn stream_text(url: String, body: Value) -> String {
    let response = client()
        .post(url)
        .bearer_auth(GATEWAY_KEY)
        .json(&body)
        .send()
        .await
        .expect("the gateway answers");
    assert_eq!(response.status(), 200);
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok()),
        Some("text/event-stream")
    );
    response.text().await.expect("a streamed body")
}
