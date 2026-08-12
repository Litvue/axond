//! Transport bounds, black-box against a real `axond` process (ADR 0014).
//!
//! Every case here is an upstream that answers slowly, hugely, or not at all —
//! the shapes that used to hang a caller for as long as the provider felt like
//! it. What is asserted is the wall clock (the request ends on the gateway's
//! own bound), the typed verdict the caller sees, that no provider URL or
//! credential leaks into it, and that the accounting still settles exactly one
//! usage record with its rate-limit permit released.

mod support;

use std::time::{Duration, Instant};

use futures::StreamExt;
use serde_json::{Value, json};
use support::gateway::alias;
use support::{Axond, FakeUpstream, GATEWAY_KEY, boot_with, client};

/// The bounds under test. Small, so a stalled upstream is observed in a test's
/// lifetime; far enough apart that the phase that fired is unambiguous.
const BOUNDS: &str = r#"
[failover]
max_attempts = 1
overall_timeout_ms = 30000

[transport]
connect_timeout_ms = 2000
response_header_timeout_ms = 600
buffered_body_timeout_ms = 600
stream_idle_timeout_ms = 600
max_response_bytes = 65536
max_error_bytes = 4096
"#;

/// A failover budget shorter than every phase bound, so the overall deadline is
/// the only thing that can end an attempt.
const TIGHT_OVERALL: &str = r#"
[failover]
max_attempts = 1
overall_timeout_ms = 500

[transport]
response_header_timeout_ms = 30000
buffered_body_timeout_ms = 30000
stream_idle_timeout_ms = 30000
"#;

/// A failover budget that expires while a healthy stream is still producing:
/// once open, the stream is governed by the idle bound alone.
const TIGHT_OVERALL_PATIENT_STREAM: &str = r#"
[failover]
max_attempts = 1
overall_timeout_ms = 500

[transport]
stream_idle_timeout_ms = 5000
"#;

/// The slowest a bounded request may take before the bound is not a bound. Well
/// above the configured budgets, well below the upstream's own stall.
const GENEROUS: Duration = Duration::from_secs(10);

#[tokio::test]
async fn a_slow_completion_is_answered_rather_than_cut_off_by_the_header_bound() {
    // The shipped `[transport]` defaults, deliberately: a non-streamed provider
    // call sends no headers until the completion exists, so a header bound below
    // the walk budget would refuse answers the walk still had time for.
    let (_upstream, gateway) = boot_with(support::gateway::DEFAULT_TUNING).await;
    let (elapsed, response) = timed_chat(&gateway, alias::CHAT_LATE_HEADERS, false).await;

    assert_eq!(
        response.status(),
        200,
        "a slow completion must be served: {}",
        gateway.output()
    );
    let body: Value = response.json().await.expect("a JSON body");
    assert!(
        body["choices"][0]["message"]["content"].is_string(),
        "{body}"
    );
    assert!(
        elapsed < GENEROUS,
        "the upstream answered but the gateway took too long: {elapsed:?}"
    );

    assert_settled_once(&gateway, "ok").await;
}

#[tokio::test]
async fn an_upstream_that_never_sends_headers_ends_on_the_header_bound() {
    let (upstream, gateway) = boot_with(BOUNDS).await;
    let (elapsed, response) = timed_chat(&gateway, alias::CHAT_NO_HEADERS, false).await;

    assert_eq!(response.status(), 504);
    let body: Value = response.json().await.expect("a JSON body");
    assert_eq!(body["error"]["type"], "upstream_timeout");
    assert!(
        body["error"]["message"]
            .as_str()
            .expect("a message")
            .contains("response headers"),
        "the phase that fired must be named: {body}"
    );
    assert_discreet(&body.to_string(), &upstream);
    assert!(
        elapsed < GENEROUS,
        "the header bound did not fire: {elapsed:?}"
    );

    assert_settled_once(&gateway, "upstream_error").await;
}

#[tokio::test]
async fn an_active_attempt_cannot_outlive_the_overall_deadline() {
    // The regression: the failover loop checked the deadline before dispatching
    // and then waited on the attempt indefinitely.
    let (upstream, gateway) = boot_with(TIGHT_OVERALL).await;
    let (elapsed, response) = timed_chat(&gateway, alias::CHAT_NO_HEADERS, false).await;

    assert_eq!(response.status(), 504);
    let body: Value = response.json().await.expect("a JSON body");
    assert_eq!(body["error"]["type"], "upstream_timeout");
    let message = body["error"]["message"].as_str().expect("a message");
    assert!(
        message.contains("failover budget"),
        "the overall deadline must be the verdict: {body}"
    );
    // The phase is still named: a target that accepts a request and answers
    // nothing is the target's failure, whichever bound ran out first, and only
    // that distinction lets its circuit ever open.
    assert!(
        message.contains("response headers"),
        "the stalled phase must still be named: {body}"
    );
    assert_discreet(&body.to_string(), &upstream);
    assert!(
        elapsed < GENEROUS,
        "the overall deadline did not bound the attempt: {elapsed:?}"
    );

    assert_settled_once(&gateway, "upstream_error").await;
}

#[tokio::test]
async fn a_buffered_body_that_never_arrives_ends_on_the_body_bound() {
    let (upstream, gateway) = boot_with(BOUNDS).await;
    let (elapsed, response) = timed_chat(&gateway, alias::CHAT_SLOW_BODY, false).await;

    assert_eq!(response.status(), 504);
    let body: Value = response.json().await.expect("a JSON body");
    assert_eq!(body["error"]["type"], "upstream_timeout");
    assert!(
        body["error"]["message"]
            .as_str()
            .expect("a message")
            .contains("response body"),
        "the phase that fired must be named: {body}"
    );
    assert_discreet(&body.to_string(), &upstream);
    assert!(
        elapsed < GENEROUS,
        "the body bound did not fire: {elapsed:?}"
    );

    assert_settled_once(&gateway, "upstream_error").await;
}

#[tokio::test]
async fn an_oversized_buffered_body_is_refused_rather_than_relayed() {
    let (upstream, gateway) = boot_with(BOUNDS).await;
    let (_, response) = timed_chat(&gateway, alias::CHAT_HUGE_BODY, false).await;

    assert_eq!(response.status(), 502);
    let raw = response.text().await.expect("a body");
    let body: Value = serde_json::from_str(&raw).expect("a JSON body");
    assert_eq!(body["error"]["type"], "upstream_body_too_large");
    // The caller is told the bound, not handed the body that broke it.
    assert!(
        raw.len() < 4096,
        "the refusal relayed the body: {} bytes",
        raw.len()
    );
    assert_discreet(&raw, &upstream);

    assert_settled_once(&gateway, "upstream_error").await;
}

#[tokio::test]
async fn an_oversized_provider_error_is_truncated_not_read_whole() {
    let (upstream, gateway) = boot_with(BOUNDS).await;
    let (elapsed, response) = timed_chat(&gateway, alias::CHAT_HUGE_ERROR, false).await;

    // The provider's verdict still reaches the caller; only the diagnostic body
    // it came with is bounded.
    assert!(response.status().is_server_error());
    let raw = response.text().await.expect("a body");
    assert!(
        raw.len() < 16 * 1024,
        "the provider error body was relayed whole: {} bytes",
        raw.len()
    );
    assert_discreet(&raw, &upstream);
    assert!(
        elapsed < GENEROUS,
        "reading the error body was unbounded: {elapsed:?}"
    );

    assert_settled_once(&gateway, "upstream_error").await;
}

#[tokio::test]
async fn a_stream_that_stalls_before_any_event_ends_on_the_idle_bound() {
    let (upstream, gateway) = boot_with(BOUNDS).await;
    let started = Instant::now();
    let response = chat(&gateway, alias::CHAT_STALL, true).await;
    // The stream is open — headers arrived — so the caller gets 200 and the
    // verdict is relayed in band.
    assert_eq!(response.status(), 200);
    let relayed = drain(response).await;
    let elapsed = started.elapsed();

    assert!(
        elapsed < GENEROUS,
        "the idle bound did not fire: {elapsed:?}"
    );
    assert!(
        relayed.contains("error") && relayed.contains("[DONE]"),
        "a stalled stream must be terminated honestly: {relayed}"
    );
    assert_discreet(&relayed, &upstream);

    let record = assert_settled_once(&gateway, "upstream_error").await;
    assert_eq!(record["output_tokens"], json!(0));
    await_closed_upstreams(&upstream, &gateway).await;
}

#[tokio::test]
async fn a_stream_that_stalls_after_bytes_is_terminated_without_a_second_attempt() {
    let (upstream, gateway) = boot_with(BOUNDS).await;
    let started = Instant::now();
    let response = chat(&gateway, alias::CHAT_STALL_AFTER_BYTES, true).await;
    assert_eq!(response.status(), 200);
    let relayed = drain(response).await;
    let elapsed = started.elapsed();

    assert!(
        elapsed < GENEROUS,
        "the idle bound did not fire: {elapsed:?}"
    );
    assert!(
        relayed.contains("tok0") && relayed.contains("error") && relayed.contains("[DONE]"),
        "committed bytes must be kept and the stream closed out: {relayed}"
    );
    assert_discreet(&relayed, &upstream);
    // Bytes were already downstream, so there is nothing to retry: exactly one
    // upstream attempt, and no second completion spliced in.
    assert_eq!(
        upstream.state.requests().len(),
        1,
        "a committed stream must not be retried"
    );

    let record = assert_settled_once(&gateway, "upstream_error").await;
    assert!(
        record["output_tokens"].as_u64().expect("output tokens") > 0,
        "the relayed output must still be charged: {record}"
    );
    await_closed_upstreams(&upstream, &gateway).await;
}

#[tokio::test]
async fn a_productive_stream_outlives_the_overall_failover_deadline() {
    let (upstream, gateway) = boot_with(TIGHT_OVERALL_PATIENT_STREAM).await;
    let started = Instant::now();
    let response = chat(&gateway, alias::CHAT_LONG, true).await;
    assert_eq!(response.status(), 200);
    let relayed = drain(response).await;
    let elapsed = started.elapsed();

    // The stream is deliberately slower than the failover budget: once it is
    // open, only silence may end it.
    assert!(
        elapsed > Duration::from_millis(500),
        "the stream was not slower than the failover budget: {elapsed:?}"
    );
    assert!(
        relayed.contains("tok19") && relayed.contains("[DONE]") && !relayed.contains("\"error\""),
        "a productive stream must run to completion: {relayed}"
    );

    assert_settled_once(&gateway, "ok").await;
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

async fn timed_chat(gateway: &Axond, model: &str, streamed: bool) -> (Duration, reqwest::Response) {
    let started = Instant::now();
    let response = chat(gateway, model, streamed).await;
    (started.elapsed(), response)
}

async fn drain(response: reqwest::Response) -> String {
    let mut stream = response.bytes_stream();
    let mut seen = String::new();
    while let Some(Ok(chunk)) = stream.next().await {
        seen.push_str(&String::from_utf8_lossy(&chunk));
    }
    seen
}

/// Exactly one usage record, with the expected status and no charge left
/// unreconciled: the timeout paths settle their reservation and release their
/// rate-limit permit once, which a leaked permit or a double settle would show
/// up as a second record or a stuck follow-up request.
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

/// Nothing caller-visible names the provider endpoint or the credential the
/// pool leased.
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

/// A gateway that gave up on an upstream must also let it go.
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
