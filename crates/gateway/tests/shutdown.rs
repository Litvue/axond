//! Termination, black-box against a real `axond` process (ADR 0014).
//!
//! A rolling deployment sends `SIGTERM` and then waits a fixed number of
//! seconds. What has to hold in that window is asserted here against the shipped
//! binary: readiness fails first so the load balancer stops routing, liveness
//! keeps answering so nothing escalates to `SIGKILL`, work already admitted is
//! finished, work that cannot finish is cut on the gateway's own deadline rather
//! than the orchestrator's, and the spend accrued by both is accounted for
//! before the process is gone.

mod support;

use std::time::{Duration, Instant};

use futures::StreamExt;
use serde_json::{Value, json};
use support::gateway::alias;
use support::{Axond, GATEWAY_KEY, boot_with, client};

/// A drain window long enough to observe from a test, with a request deadline
/// well above the slowest fixture answer so nothing golden-path is cut.
const PATIENT: &str = r#"
[failover]
max_attempts = 1
overall_timeout_ms = 30000

[shutdown]
drain_grace_ms = 3000
deadline_ms = 10000
flush_timeout_ms = 2000
"#;

/// A request deadline shorter than a stream that never ends, so the deadline is
/// what terminates it. The drain window is short because no new work needs to
/// arrive; the idle bound is high so only shutdown can end the stream.
const IMPATIENT: &str = r#"
[failover]
max_attempts = 1
overall_timeout_ms = 30000

[transport]
stream_idle_timeout_ms = 30000

[shutdown]
drain_grace_ms = 300
deadline_ms = 1500
flush_timeout_ms = 2000
"#;

/// The longest a bounded shutdown may take before the bound is not a bound:
/// above drain + deadline + both flush budgets, far below the 30s
/// `terminationGracePeriodSeconds` the shipped manifest allows.
const GENEROUS: Duration = Duration::from_secs(20);

#[tokio::test]
async fn sigterm_fails_readiness_while_liveness_keeps_answering() {
    let (_upstream, mut gateway) = boot_with(PATIENT).await;
    assert_eq!(ready(&gateway).await, Some(200));

    gateway.terminate();

    assert_eq!(
        gateway.await_not_ready(Duration::from_secs(2)).await,
        Some(reqwest::StatusCode::SERVICE_UNAVAILABLE),
        "readiness must fail before the process goes away:\n{}",
        gateway.output()
    );
    // Liveness is what an orchestrator escalates on. A draining replica that
    // fails it is killed outright, cutting the requests the drain was for.
    assert_eq!(
        live(&gateway).await,
        Some(200),
        "liveness must survive the drain:\n{}",
        gateway.output()
    );

    let status = gateway
        .await_exit(GENEROUS)
        .await
        .unwrap_or_else(|| panic!("the process did not exit:\n{}", gateway.output()));
    assert!(status.success(), "shutdown was not clean: {status}");
}

#[tokio::test]
async fn a_completion_admitted_before_the_drain_is_still_answered() {
    let (_upstream, mut gateway) = boot_with(PATIENT).await;

    // The upstream withholds this answer for ~2s, so the request is unambiguously
    // in flight when the signal lands.
    let request = chat(&gateway, alias::CHAT_LATE_HEADERS, false);
    let signal = async {
        tokio::time::sleep(Duration::from_millis(300)).await;
        gateway.terminate();
    };
    let (response, ()) = tokio::join!(request, signal);

    assert_eq!(
        response.status(),
        200,
        "an admitted request must be finished:\n{}",
        gateway.output()
    );
    let body: Value = response.json().await.expect("a JSON body");
    assert!(
        body["choices"][0]["message"]["content"].is_string(),
        "{body}"
    );

    let status = gateway
        .await_exit(GENEROUS)
        .await
        .unwrap_or_else(|| panic!("the process did not exit:\n{}", gateway.output()));
    assert!(status.success(), "shutdown was not clean: {status}");
    // The completed request is charged in full, flushed before the process ends.
    assert_eq!(settled(&gateway).await["status"], json!("ok"));
}

#[tokio::test]
async fn a_stream_open_at_the_deadline_is_cut_and_still_accounted_for() {
    let (_upstream, mut gateway) = boot_with(IMPATIENT).await;

    // Relays a few events and then never sends another: without the shutdown
    // deadline this stream would hold the process until the orchestrator's
    // `SIGKILL`, which would discard the spend accrued so far.
    let response = chat(&gateway, alias::CHAT_STALL_AFTER_BYTES, true).await;
    assert_eq!(response.status(), 200);
    let mut relayed = response.bytes_stream();
    let first = relayed
        .next()
        .await
        .expect("an event")
        .expect("relayed bytes");
    assert!(!first.is_empty());

    let started = Instant::now();
    gateway.terminate();
    let status = gateway
        .await_exit(GENEROUS)
        .await
        .unwrap_or_else(|| panic!("a stalled stream held the process:\n{}", gateway.output()));
    assert!(status.success(), "shutdown was not clean: {status}");
    // The deadline is a floor as well as a ceiling: the stream is given its
    // configured window before it is abandoned.
    assert!(
        started.elapsed() >= Duration::from_millis(1_500),
        "the stream was cut before its deadline: {:?}",
        started.elapsed()
    );

    // Partial output was relayed, so the partial charge must survive the
    // shutdown — accounted as a cancellation, like any hang-up mid-stream.
    let record = settled(&gateway).await;
    assert_eq!(record["status"], json!("client_cancelled"), "{record}");
}

#[tokio::test]
async fn new_requests_stop_being_served_once_the_drain_window_closes() {
    let (_upstream, mut gateway) = boot_with(IMPATIENT).await;

    // Holds the process open past the drain window, so the refusal below is the
    // drain and not a process that has already exited.
    let response = chat(&gateway, alias::CHAT_STALL_AFTER_BYTES, true).await;
    let mut relayed = response.bytes_stream();
    relayed.next().await.expect("an event").expect("bytes");

    gateway.terminate();
    tokio::time::sleep(Duration::from_millis(900)).await;

    let refused = client()
        .post(gateway.url("/v1/chat/completions"))
        .bearer_auth(GATEWAY_KEY)
        .json(&json!({
            "model": alias::CHAT,
            "messages": [{ "role": "user", "content": "hello" }]
        }))
        .send()
        .await;
    // The listener is closed once the drain window ends, so a caller that was not
    // already connected cannot be admitted at all.
    assert!(
        refused.is_err(),
        "a draining replica accepted new work: {:?}\n{}",
        refused.map(|response| response.status()),
        gateway.output()
    );

    assert!(
        gateway.await_exit(GENEROUS).await.is_some(),
        "the process did not exit:\n{}",
        gateway.output()
    );
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

async fn ready(gateway: &Axond) -> Option<u16> {
    probe(gateway, "/readyz").await
}

async fn live(gateway: &Axond) -> Option<u16> {
    probe(gateway, "/healthz").await
}

/// The probes are unauthenticated by design, throughout the drain: an
/// orchestrator has no gateway key.
async fn probe(gateway: &Axond, path: &str) -> Option<u16> {
    client()
        .get(gateway.url(path))
        .send()
        .await
        .ok()
        .map(|response| response.status().as_u16())
}

/// Exactly one usage record, flushed before the process ended.
async fn settled(gateway: &Axond) -> Value {
    let records = gateway.await_usage_records(1).await;
    assert_eq!(
        records.len(),
        1,
        "expected one flushed usage record:\n{}",
        gateway.output()
    );
    records.into_iter().next().expect("a usage record")
}
