//! Request bounds and load shedding, black-box against a real `axond` process
//! (ADR 0014).
//!
//! Every case here is a caller trying to take more of the replica than it is
//! allowed: a body too large to buffer, a prompt or output allowance above the
//! configured ceiling, or one more concurrent request than the process admits.
//! What is asserted is the typed verdict the caller sees, that nothing reached a
//! provider on a shed request, that no bound leaks the request or a credential,
//! and — the part a leaked permit would break — that the capacity comes back
//! when a request ends, is cancelled, or is cut off by its own bound.

mod support;

use std::time::{Duration, Instant};

use futures::StreamExt;
use serde_json::{Value, json};
use support::gateway::alias;
use support::{Axond, FakeUpstream, GATEWAY_KEY, boot_with, client};

/// One request at a time, nothing queued: the default shape, where saturation is
/// an immediate answer rather than latency the caller cannot see. The per-tenant
/// ceiling is off, so the ceiling under test is the process's own.
const ONE_AT_A_TIME: &str = r#"
[failover]
max_attempts = 1
overall_timeout_ms = 30000

[admission]
max_in_flight = 1
max_in_flight_streams = 1
max_in_flight_per_tenant = 0
queue_capacity = 0
queue_wait_ms = 0
"#;

/// One request at a time with a short bounded queue behind it, so a queued
/// caller is shed on the wait bound rather than held forever.
const SHORT_QUEUE: &str = r#"
[failover]
max_attempts = 1
overall_timeout_ms = 30000

[transport]
stream_idle_timeout_ms = 30000

[admission]
max_in_flight = 1
max_in_flight_streams = 1
max_in_flight_per_tenant = 0
queue_capacity = 4
queue_wait_ms = 300
"#;

/// Room for concurrent requests globally, but one per tenant: a tenant's own
/// ceiling is the caller's problem, not the replica's.
const ONE_PER_TENANT: &str = r#"
[failover]
max_attempts = 1
overall_timeout_ms = 30000

[admission]
max_in_flight = 8
max_in_flight_streams = 8
max_in_flight_per_tenant = 1
"#;

/// Bounds on what a single request may carry, small enough to exercise from a
/// test and far apart enough that the bound that fired is unambiguous.
const SMALL_REQUESTS: &str = r#"
[failover]
max_attempts = 1
overall_timeout_ms = 30000

[admission]
max_request_bytes = 4096
max_prompt_tokens = 64
max_output_tokens = 32
"#;

/// A stream bounded by its total lifetime, with the transport's idle bound left
/// wide so only the total duration can end a stalled stream.
const SHORT_STREAM_LIFETIME: &str = r#"
[failover]
max_attempts = 1
overall_timeout_ms = 30000

[transport]
stream_idle_timeout_ms = 30000

[admission]
max_stream_duration_ms = 700
"#;

/// A stream bounded by the bytes it may relay, well below the fixture's output.
const SMALL_STREAM_BYTES: &str = r#"
[failover]
max_attempts = 1
overall_timeout_ms = 30000

[transport]
stream_idle_timeout_ms = 30000

[admission]
max_stream_bytes = 64
"#;

/// The slowest a bounded request may take before the bound is not a bound.
const GENEROUS: Duration = Duration::from_secs(10);

#[tokio::test]
async fn an_oversized_request_body_is_refused_without_reaching_a_provider() {
    let (upstream, gateway) = boot_with(SMALL_REQUESTS).await;
    let response = client()
        .post(gateway.url("/v1/chat/completions"))
        .bearer_auth(GATEWAY_KEY)
        .json(&json!({
            "model": alias::CHAT,
            "messages": [{ "role": "user", "content": "x".repeat(256 * 1024) }]
        }))
        .send()
        .await
        .expect("the gateway answers");

    assert_eq!(response.status(), 413, "{}", gateway.output());
    let body: Value = response.json().await.expect("a JSON body");
    assert_eq!(body["error"]["type"], "request_too_large");
    assert_discreet(&body.to_string(), &upstream);
    assert!(
        upstream.state.requests().is_empty(),
        "a refused request must not reach a provider"
    );
}

#[tokio::test]
async fn a_body_without_a_json_content_type_is_still_a_415() {
    let (upstream, gateway) = boot_with(SMALL_REQUESTS).await;
    // Mapping the extractor's rejections to typed errors must not restate an
    // existing failure mode's status: axum answered this 415 before the bounds
    // existed, and the contract in docs/compatibility.md keeps it.
    let response = client()
        .post(gateway.url("/v1/chat/completions"))
        .bearer_auth(GATEWAY_KEY)
        .header("content-type", "text/plain")
        .body(r#"{"model":"x","messages":[]}"#)
        .send()
        .await
        .expect("the gateway answers");

    assert_eq!(response.status(), 415, "{}", gateway.output());
    let body: Value = response.json().await.expect("a JSON body");
    assert_eq!(body["error"]["type"], "unsupported_media_type");
    assert_discreet(&body.to_string(), &upstream);
    assert!(upstream.state.requests().is_empty());
}

#[tokio::test]
async fn a_prompt_over_the_token_bound_is_refused_and_names_only_the_bound() {
    let (upstream, gateway) = boot_with(SMALL_REQUESTS).await;
    // Under `max_request_bytes`, over `max_prompt_tokens`: the two bounds are
    // not the same bound.
    let response = client()
        .post(gateway.url("/v1/chat/completions"))
        .bearer_auth(GATEWAY_KEY)
        .json(&json!({
            "model": alias::CHAT,
            "messages": [{ "role": "user", "content": "secret-prompt ".repeat(60) }]
        }))
        .send()
        .await
        .expect("the gateway answers");

    assert_eq!(response.status(), 413, "{}", gateway.output());
    let body: Value = response.json().await.expect("a JSON body");
    assert_eq!(body["error"]["type"], "prompt_too_large");
    let message = body["error"]["message"].as_str().expect("a message");
    assert!(message.contains("64"), "the bound must be named: {body}");
    assert!(
        !message.contains("secret-prompt"),
        "the prompt must not be echoed: {body}"
    );
    assert_discreet(&body.to_string(), &upstream);
    assert!(upstream.state.requests().is_empty());
    assert!(
        !gateway.output().contains("secret-prompt"),
        "the prompt must not be logged:\n{}",
        gateway.output()
    );
}

#[tokio::test]
async fn an_output_allowance_over_the_bound_is_refused_rather_than_clamped() {
    let (upstream, gateway) = boot_with(SMALL_REQUESTS).await;
    let response = client()
        .post(gateway.url("/v1/chat/completions"))
        .bearer_auth(GATEWAY_KEY)
        .json(&json!({
            "model": alias::CHAT,
            "max_tokens": 4096,
            "messages": [{ "role": "user", "content": "hello" }]
        }))
        .send()
        .await
        .expect("the gateway answers");

    assert_eq!(response.status(), 400, "{}", gateway.output());
    let body: Value = response.json().await.expect("a JSON body");
    assert_eq!(body["error"]["type"], "output_limit_exceeded");
    assert_discreet(&body.to_string(), &upstream);
    assert!(
        upstream.state.requests().is_empty(),
        "a refused request must not reach a provider"
    );
}

#[tokio::test]
async fn a_saturated_replica_sheds_with_a_503_and_serves_again_afterwards() {
    let (upstream, gateway) = boot_with(ONE_AT_A_TIME).await;
    // The in-flight request is a slow stream, so the second request arrives
    // while the only permit is genuinely held.
    let held = chat(&gateway, alias::CHAT_SLOW, true).await;
    assert_eq!(held.status(), 200);

    let shed = chat(&gateway, alias::CHAT, false).await;
    assert_eq!(shed.status(), 503, "{}", gateway.output());
    assert_eq!(
        shed.headers()
            .get("retry-after")
            .and_then(|value| value.to_str().ok()),
        Some("1"),
        "an overloaded replica should say when to come back"
    );
    let body: Value = shed.json().await.expect("a JSON body");
    assert_eq!(body["error"]["type"], "gateway_overloaded");
    assert_discreet(&body.to_string(), &upstream);

    // The held stream runs to completion and gives its permit back.
    drain(held).await;
    await_closed_upstreams(&upstream, &gateway).await;
    let served = await_served(&gateway).await;
    assert_eq!(
        served,
        200,
        "the permit was not released:\n{}",
        gateway.output()
    );
}

#[tokio::test]
async fn a_cancelled_stream_releases_the_capacity_it_held() {
    let (upstream, gateway) = boot_with(ONE_AT_A_TIME).await;
    let held = chat(&gateway, alias::CHAT_SLOW, true).await;
    assert_eq!(held.status(), 200);
    // A caller that walks away mid-stream: the response body is dropped without
    // being drained, which is what a closed browser tab looks like.
    drop(held);
    await_closed_upstreams(&upstream, &gateway).await;

    let served = await_served(&gateway).await;
    assert_eq!(
        served,
        200,
        "a cancelled stream leaked its permit:\n{}",
        gateway.output()
    );
}

#[tokio::test]
async fn a_tenant_at_its_own_ceiling_is_a_429_not_a_503() {
    let (upstream, gateway) = boot_with(ONE_PER_TENANT).await;
    let held = chat(&gateway, alias::CHAT_SLOW, true).await;
    assert_eq!(held.status(), 200);

    // The replica has seven more global slots; this tenant has none.
    let shed = chat(&gateway, alias::CHAT, false).await;
    assert_eq!(shed.status(), 429, "{}", gateway.output());
    let body: Value = shed.json().await.expect("a JSON body");
    assert_eq!(body["error"]["type"], "tenant_concurrency_exceeded");
    assert_discreet(&body.to_string(), &upstream);

    drain(held).await;
    await_closed_upstreams(&upstream, &gateway).await;
}

#[tokio::test]
async fn a_queued_request_is_shed_on_the_wait_bound_rather_than_held() {
    let (upstream, gateway) = boot_with(SHORT_QUEUE).await;
    // A stream that stalls after its first bytes holds the only permit for
    // longer than any queue wait, so what is measured is the wait bound.
    let held = chat(&gateway, alias::CHAT_STALL_AFTER_BYTES, true).await;
    assert_eq!(held.status(), 200);

    let started = Instant::now();
    let shed = chat(&gateway, alias::CHAT, false).await;
    let waited = started.elapsed();
    assert_eq!(shed.status(), 503, "{}", gateway.output());
    let body: Value = shed.json().await.expect("a JSON body");
    assert_eq!(body["error"]["type"], "admission_queue_timeout");
    assert_discreet(&body.to_string(), &upstream);
    assert!(
        waited >= Duration::from_millis(300),
        "the request was shed before its queue wait elapsed: {waited:?}"
    );
    assert!(
        waited < GENEROUS,
        "the queue wait is not a bound: {waited:?}"
    );

    drop(held);
    await_closed_upstreams(&upstream, &gateway).await;
}

#[tokio::test]
async fn a_stream_that_never_ends_is_closed_on_its_total_duration() {
    let (upstream, gateway) = boot_with(SHORT_STREAM_LIFETIME).await;
    let started = Instant::now();
    // Bytes, then silence the transport's idle bound is too patient to notice.
    let response = chat(&gateway, alias::CHAT_STALL_AFTER_BYTES, true).await;
    assert_eq!(response.status(), 200);
    let relayed = drain(response).await;
    let elapsed = started.elapsed();

    assert!(
        elapsed >= Duration::from_millis(700) && elapsed < GENEROUS,
        "the total-duration bound did not end the stream: {elapsed:?}"
    );
    assert!(
        relayed.contains("maximum stream duration") && relayed.contains("[DONE]"),
        "the caller must be told which bound ended the stream: {relayed}"
    );
    assert_discreet(&relayed, &upstream);
    assert_settled_once(&gateway, "upstream_error").await;
    await_closed_upstreams(&upstream, &gateway).await;
}

#[tokio::test]
async fn a_stream_over_the_byte_bound_is_ended_with_what_was_committed() {
    let (upstream, gateway) = boot_with(SMALL_STREAM_BYTES).await;
    let response = chat(&gateway, alias::CHAT_LONG, true).await;
    assert_eq!(response.status(), 200);
    let relayed = drain(response).await;

    assert!(
        relayed.contains("maximum stream size") && relayed.contains("[DONE]"),
        "the byte bound must end the stream with a terminal event: {relayed}"
    );
    assert!(
        !relayed.contains("tok19"),
        "the stream should have ended before the fixture finished: {relayed}"
    );
    assert_discreet(&relayed, &upstream);
    assert_settled_once(&gateway, "upstream_error").await;
    await_closed_upstreams(&upstream, &gateway).await;
}

async fn chat(gateway: &Axond, model: &str, streamed: bool) -> reqwest::Response {
    client()
        .post(gateway.url("/v1/chat/completions"))
        .bearer_auth(GATEWAY_KEY)
        .json(&json!({
            "model": model,
            "stream": streamed,
            "messages": [{ "role": "user", "content": "hello" }]
        }))
        .send()
        .await
        .expect("the gateway answers")
}

/// The status of the next buffered request the replica accepts, once the
/// capacity a finished request held has been given back. Released permits are
/// synchronous, but the caller's own connection teardown is not, so this polls
/// rather than assuming the release already happened.
async fn await_served(gateway: &Axond) -> u16 {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let status = chat(gateway, alias::CHAT, false).await.status().as_u16();
        if status != 503 && status != 429 {
            return status;
        }
        if Instant::now() >= deadline {
            return status;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn drain(response: reqwest::Response) -> String {
    let mut stream = response.bytes_stream();
    let mut seen = String::new();
    while let Some(Ok(chunk)) = stream.next().await {
        seen.push_str(&String::from_utf8_lossy(&chunk));
    }
    seen
}

/// Exactly one usage record with the expected status: a bound that fires still
/// settles its reservation and releases its permits once.
async fn assert_settled_once(gateway: &Axond, status: &str) -> Value {
    let records = gateway.await_usage_records(1).await;
    assert_eq!(
        records.len(),
        1,
        "expected one usage record:\n{}",
        gateway.output()
    );
    let record = records.into_iter().next().expect("a usage record");
    assert_eq!(record["status"], json!(status), "usage record: {record}");
    record
}

/// Nothing caller-visible names the provider endpoint or a credential.
fn assert_discreet(text: &str, upstream: &FakeUpstream) {
    let host = upstream
        .base_url
        .strip_prefix("http://")
        .expect("a loopback base URL");
    assert!(
        !text.contains(host) && !text.contains(&upstream.base_url),
        "the provider endpoint leaked: {text}"
    );
    for secret in [
        support::gateway::OPENAI_KEY,
        support::gateway::ANTHROPIC_KEY,
        GATEWAY_KEY,
    ] {
        assert!(!text.contains(secret), "a credential leaked: {text}");
    }
}

/// A gateway that ended a stream must also let its upstream go.
async fn await_closed_upstreams(upstream: &FakeUpstream, gateway: &Axond) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let open = upstream.state.open_streams();
        if open == 0 {
            return;
        }
        if Instant::now() >= deadline {
            panic!("{open} upstream stream(s) leaked:\n{}", gateway.output());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}
