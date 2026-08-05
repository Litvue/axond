//! SSE soak: many concurrent long-lived streams, with client cancels and
//! upstream drops mixed in (ADR 0014).
//!
//! Three invariants are asserted, and they are the ones a gateway gets wrong
//! quietly: upstream connections balance (a cancelled client takes its upstream
//! with it), every stream settles exactly one usage record with the charge its
//! outcome earns (ADR 0010), and the process does not grow with the streams it
//! relays — nothing buffers a whole body.
//!
//! The short run is part of `cargo test`. The long run is opt-in via
//! `AXOND_SOAK=1` and lives in the `soak` workflow, off the per-PR path.

mod support;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures::StreamExt;
use serde_json::{Value, json};
use support::gateway::alias;
use support::{Axond, FakeUpstream, GATEWAY_KEY, boot, client};

/// How a soaked stream ends.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum Ending {
    /// Read to completion.
    Complete,
    /// The client hangs up after a few events.
    ClientCancel,
    /// The upstream dies mid-event once the relay has committed.
    UpstreamDrop,
}

impl Ending {
    fn alias(self, native: bool) -> &'static str {
        match (self, native) {
            (Self::UpstreamDrop, false) => alias::CHAT_DROP,
            (Self::UpstreamDrop, true) => alias::MESSAGES_DROP,
            (_, false) => alias::CHAT_SLOW,
            (_, true) => alias::MESSAGES_SLOW,
        }
    }

    /// The usage status the gateway must record for this ending.
    fn status(self) -> &'static str {
        match self {
            Self::Complete => "ok",
            Self::ClientCancel => "client_cancelled",
            // The stream was already open, so a dying upstream cannot fail
            // over: it terminates the caller and charges what it relayed.
            Self::UpstreamDrop => "upstream_error",
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sse_soak_short() {
    soak(24).await;
}

/// The heavy run: hundreds of concurrent streams. Opt-in, because it is slow.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sse_soak_long() {
    if std::env::var("AXOND_SOAK").as_deref() != Ok("1") {
        eprintln!("skipping the long soak; set AXOND_SOAK=1 to run it");
        return;
    }
    soak(600).await;
}

/// Run `per_ending * 3 * 2` concurrent streams — every ending against both wire
/// shapes — and assert the three invariants.
async fn soak(total: usize) {
    let (upstream, gateway) = boot().await;
    let endings = [Ending::Complete, Ending::ClientCancel, Ending::UpstreamDrop];
    let plan: Vec<(Ending, bool)> = (0..total)
        .map(|i| (endings[i % endings.len()], i % 2 == 0))
        .collect();

    let baseline_kib = gateway.resident_kib();
    let client = client();
    let mut running = Vec::new();
    for (ending, native) in plan.iter().copied() {
        let client = client.clone();
        let url = gateway.url(if native {
            "/v1/messages"
        } else {
            "/v1/chat/completions"
        });
        running.push(tokio::spawn(async move {
            drive(&client, url, ending, native).await
        }));
    }
    // Sample while the streams are in flight: buffering whole bodies would show
    // up here, not after everything has settled.
    let peak = Arc::new(AtomicU64::new(0));
    let sampler = tokio::spawn({
        let (pid, peak) = (gateway.pid(), peak.clone());
        async move {
            loop {
                if let Some(kib) = support::gateway::resident_kib(pid) {
                    peak.fetch_max(kib, Ordering::Relaxed);
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    });
    for task in running {
        task.await.expect("a soaked stream task does not panic");
    }
    sampler.abort();
    let peak_kib = Some(peak.load(Ordering::Relaxed)).filter(|kib| *kib > 0);

    let records = gateway.await_usage_records(plan.len()).await;
    await_balanced_upstreams(&upstream, &gateway).await;
    assert_eq!(
        upstream.state.opened_streams() as usize,
        plan.len(),
        "every request must have opened exactly one upstream stream"
    );

    let mut seen: BTreeMap<&str, usize> = BTreeMap::new();
    for record in &records {
        let status = record["status"].as_str().expect("a status").to_owned();
        *seen.entry(status_label(&status)).or_default() += 1;
        assert_charge(record, &status);
    }
    let mut expected: BTreeMap<&str, usize> = BTreeMap::new();
    for (ending, _) in &plan {
        *expected.entry(ending.status()).or_default() += 1;
    }
    assert_eq!(seen, expected, "usage records:\n{}", gateway.output());

    let settled_kib = gateway.resident_kib();
    eprintln!(
        "soak: {} streams, upstreams opened {} / open {}, outcomes {seen:?}, \
         rss {:?} -> peak {peak_kib:?} -> {settled_kib:?} KiB",
        plan.len(),
        upstream.state.opened_streams(),
        upstream.state.open_streams(),
        baseline_kib,
    );
    assert_bounded_memory(baseline_kib, peak_kib, settled_kib);
}

/// Open one stream and end it the way the plan says.
async fn drive(client: &reqwest::Client, url: String, ending: Ending, native: bool) {
    let response = client
        .post(url)
        .bearer_auth(GATEWAY_KEY)
        .json(&json!({
            "model": ending.alias(native),
            "stream": true,
            "max_tokens": 1024,
            "messages": [{ "role": "user", "content": "soak" }]
        }))
        .send()
        .await
        .expect("the gateway answers");
    assert_eq!(response.status(), 200);

    let mut stream = response.bytes_stream();
    let mut chunks = 0usize;
    while let Some(chunk) = stream.next().await {
        if chunk.is_err() {
            break;
        }
        chunks += 1;
        // Hang up mid-stream, once output has demonstrably been relayed, so the
        // partial charge is a real one rather than an empty stream.
        if ending == Ending::ClientCancel && chunks >= 4 {
            break;
        }
    }
}

/// A charge that matches the outcome: a completed stream reports the provider's
/// usage, a cancelled or broken one is charged the partial spend it measured,
/// and neither is ever charged nothing after relaying output (ADR 0010).
fn assert_charge(record: &Value, status: &str) {
    let cost = record["cost_microdollars"].as_u64().expect("a cost");
    let output = record["output_tokens"].as_u64().expect("output tokens");
    match status {
        "ok" => assert!(cost > 0 && output > 0, "a completed stream: {record}"),
        _ => {
            assert!(cost > 0, "a partial stream must still be charged: {record}");
            assert!(
                output > 0,
                "a partial stream charges the output it relayed: {record}"
            );
        }
    }
}

fn status_label(status: &str) -> &'static str {
    match status {
        "ok" => "ok",
        "client_cancelled" => "client_cancelled",
        "upstream_error" => "upstream_error",
        other => panic!("unexpected usage status `{other}`"),
    }
}

/// Every upstream the gateway opened must be closed once the clients are gone.
/// Closing is observed by the upstream dropping the response body, which can
/// trail the client's last byte by a scheduler tick.
async fn await_balanced_upstreams(upstream: &FakeUpstream, gateway: &Axond) {
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

/// Relaying is event-by-event, so the process must not grow with the number of
/// concurrent streams. The bound is deliberately loose: it catches "the whole
/// body is buffered", not allocator noise.
fn assert_bounded_memory(baseline: Option<u64>, peak: Option<u64>, settled: Option<u64>) {
    const MAX_GROWTH_KIB: u64 = 256 * 1024;
    let (Some(baseline), Some(peak), Some(settled)) = (baseline, peak, settled) else {
        // Not on a /proc platform; the other invariants still hold.
        return;
    };
    assert!(
        peak.saturating_sub(baseline) < MAX_GROWTH_KIB,
        "resident memory grew {} KiB while streaming (baseline {baseline} KiB, settled {settled} KiB)",
        peak - baseline
    );
}
