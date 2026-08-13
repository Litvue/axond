//! The driver: offers a profile's load to a real `axond` process and records
//! what happened.
//!
//! Closed-loop by construction. The driver holds `concurrency` requests in
//! flight and sends a fixed *number* of requests rather than a fixed arrival
//! rate, because a rate the machine cannot serve turns every measurement into a
//! measurement of the machine. A fixed count is reproducible: the same manifest
//! sends the same requests in the same rotation on any runner, and only the
//! timings differ.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime};

use futures::StreamExt;
use serde_json::{Value, json};
use tokio::sync::Mutex;

use super::manifest::{Profile, RESULT_SCHEMA_VERSION, Tier, Workload};
use super::probe::{ResourceReport, Sampler};
use super::result::{
    CapacityResult, Environment, Occupancy, Outcomes, Percentiles, ProfileEcho, RunMeta,
    Throughput, Upstream, UsageRecords, Verdict,
};
use crate::support::gateway::{
    ANTHROPIC_SECONDARY_ENV, Axond, GATEWAY_KEY, OPENAI_SECONDARY_ENV, alias,
};
use crate::support::upstream::FakeUpstream;

/// The bounds a capacity run is served under. The admission ceilings sit far
/// above any manifest concurrency, so what is measured is the process rather
/// than its own load shedding — shedding has its own suite
/// (`admission_bounds.rs`), and here a `503` is a finding.
/// Written out rather than defaulted
/// so the recorded config hash pins them: a later change to a shipped default
/// must not silently move a qualification result.
const TUNING: &str = r"
[failover]
max_attempts = 1
overall_timeout_ms = 60000

[transport]
connect_timeout_ms = 10000
response_header_timeout_ms = 30000
buffered_body_timeout_ms = 30000
stream_idle_timeout_ms = 30000
max_response_bytes = 33554432
max_error_bytes = 65536

[admission]
max_request_bytes = 1048576
max_in_flight = 8192
max_in_flight_streams = 8192
max_in_flight_per_tenant = 0
queue_capacity = 0
queue_wait_ms = 0
max_prompt_tokens = 0
max_output_tokens = 0
max_stream_duration_ms = 0
max_stream_bytes = 0
";

/// The admission queue the profiles are served with. Queueing is a separate
/// question from capacity — a queue converts shedding into latency the caller
/// cannot see — so it is off, and recorded as off.
const QUEUE_CAPACITY: u64 = 0;

/// A second credential per provider, so the mixed profile exercises pool
/// rotation rather than one key on every dispatch.
fn credential_pool() -> String {
    format!(
        r#"
[[credential]]
namespace = "platform"
provider = "fake-openai"
env = "{OPENAI_SECONDARY_ENV}"
id = "fake-openai-secondary"

[[credential]]
namespace = "platform"
provider = "fake-anthropic"
env = "{ANTHROPIC_SECONDARY_ENV}"
id = "fake-anthropic-secondary"
"#
    )
}

/// How long usage records may trail the last client byte before they are
/// counted as dropped. Settlement is detached from the request, so this is a
/// bound on the *sink*, not on the request path.
const SETTLE_TIMEOUT: Duration = Duration::from_secs(120);

/// How long an upstream body may stay open after every client is gone. Beyond
/// this it is a leak, which is a hard failure.
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(60);

/// One request the driver sends.
#[derive(Clone, Copy)]
struct Shape {
    route: &'static str,
    alias: &'static str,
    stream: bool,
}

impl Shape {
    const fn buffered(route: &'static str, alias: &'static str) -> Self {
        Self {
            route,
            alias,
            stream: false,
        }
    }

    const fn streamed(route: &'static str, alias: &'static str) -> Self {
        Self {
            route,
            alias,
            stream: true,
        }
    }

    fn body(self) -> Value {
        match self.route {
            "/v1/embeddings" => json!({ "model": self.alias, "input": "capacity" }),
            "/v1/responses" => {
                json!({ "model": self.alias, "stream": self.stream, "input": "capacity" })
            }
            "/v1/messages" => json!({
                "model": self.alias,
                "stream": self.stream,
                "max_tokens": 1024,
                "messages": [{ "role": "user", "content": "capacity" }],
            }),
            _ => json!({
                "model": self.alias,
                "stream": self.stream,
                "messages": [{ "role": "user", "content": "capacity" }],
            }),
        }
    }
}

const CHAT: &str = "/v1/chat/completions";
const MESSAGES: &str = "/v1/messages";
const EMBEDDINGS: &str = "/v1/embeddings";
const RESPONSES: &str = "/v1/responses";

/// Both wire families, four routes, both providers, buffered and streamed
/// interleaved. Rotation is by request index, so the mix is identical on every
/// run of the same profile.
const MIXED_ROTATION: [Shape; 6] = [
    Shape::buffered(CHAT, alias::CHAT),
    Shape::buffered(MESSAGES, alias::MESSAGES),
    Shape::buffered(EMBEDDINGS, alias::EMBEDDINGS),
    Shape::buffered(RESPONSES, alias::RESPONSES),
    Shape::streamed(CHAT, alias::CHAT_SLOW),
    Shape::streamed(MESSAGES, alias::MESSAGES_SLOW),
];

const SIZE_ROTATION: [Shape; 3] = [
    Shape::buffered(CHAT, alias::CHAT_SIZED_SMALL),
    Shape::buffered(CHAT, alias::CHAT_SIZED_MEDIUM),
    Shape::buffered(CHAT, alias::CHAT_SIZED_LARGE),
];

fn shape_for(workload: Workload, index: usize) -> Shape {
    match workload {
        Workload::Buffered => Shape::buffered(CHAT, alias::CHAT),
        Workload::Streaming | Workload::Cancellation => Shape::streamed(CHAT, alias::CHAT_SLOW),
        Workload::Mixed => MIXED_ROTATION[index % MIXED_ROTATION.len()],
        Workload::ResponseSize => SIZE_ROTATION[index % SIZE_ROTATION.len()],
    }
}

/// How one offered request ended.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Outcome {
    /// Served, and read to the end.
    Accepted,
    /// Served, and the driver hung up deliberately.
    Cancelled,
    /// Shed by admission or a tenant ceiling: `429`/`503`.
    Rejected,
    /// Answered with an error status.
    Failed,
    /// Never answered: the transport gave up.
    TransportFailure,
}

struct Attempt {
    outcome: Outcome,
    status: Option<u16>,
    error_type: Option<String>,
    latency_ms: f64,
    ttft_ms: Option<f64>,
    stream_lifetime_ms: Option<f64>,
}

/// Whether the driver hangs up on the request at `index`, given a cadence. Every
/// index divisible by the cadence over `0..requests`, which is
/// [`expected_cancellations`] of them — one more than integer division when the
/// count is not a multiple of the cadence.
pub fn cancels(index: usize, every: usize) -> bool {
    index.is_multiple_of(every)
}

/// How many of `offered` requests [`cancels`] selects at cadence `every`.
pub fn expected_cancellations(offered: u64, every: usize) -> u64 {
    offered.div_ceil(every as u64)
}

/// Only one profile offers load at a time, whatever the harness runs it from:
/// two tiers driving two gateways on one machine measure each other's
/// contention, and the artifact would still read as an envelope. The libtest
/// `--test-threads=1` in the `capacity` workflow says the same thing; this makes
/// it true rather than configured.
fn load_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(Mutex::default)
}

/// Driver-side gauges. `awaiting_first_byte` is the closest a client can get to
/// the replica's queue occupancy without trusting the replica's own telemetry.
#[derive(Default)]
pub struct Gauges {
    in_flight: AtomicU64,
    in_flight_peak: AtomicU64,
    awaiting: AtomicU64,
    awaiting_peak: AtomicU64,
}

impl Gauges {
    pub fn enter(&self) {
        let now = self.in_flight.fetch_add(1, Ordering::Relaxed) + 1;
        self.in_flight_peak.fetch_max(now, Ordering::Relaxed);
        let waiting = self.awaiting.fetch_add(1, Ordering::Relaxed) + 1;
        self.awaiting_peak.fetch_max(waiting, Ordering::Relaxed);
    }

    /// Called when the first byte of the *answer* arrives: response headers for a
    /// buffered request, the first relayed chunk for a stream. A stream's headers
    /// can precede its first token by hundreds of milliseconds, and the request is
    /// still waiting during that gap.
    pub fn first_byte(&self, waiting: &mut bool) {
        if *waiting {
            *waiting = false;
            self.awaiting.fetch_sub(1, Ordering::Relaxed);
        }
    }

    pub fn leave(&self, waiting: &mut bool) {
        self.first_byte(waiting);
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
    }

    /// The most requests that waited for a first byte at once.
    pub fn awaiting_peak(&self) -> u64 {
        self.awaiting_peak.load(Ordering::Relaxed)
    }

    /// How many are waiting for a first byte right now.
    pub fn awaiting(&self) -> u64 {
        self.awaiting.load(Ordering::Relaxed)
    }
}

/// Run `profile` at `tier` and return its result artifact.
pub async fn run(profile: &Profile, tier: Tier, manifest_text: &str) -> CapacityResult {
    let _offering = load_lock().lock().await;
    let scale = *profile.scale(tier);
    let tuning = match profile.workload {
        Workload::Mixed => format!("{TUNING}{}", credential_pool()),
        _ => TUNING.to_owned(),
    };
    let upstream = FakeUpstream::start().await;
    let gateway = Axond::start_with(&upstream.base_url, &tuning).await;
    let bind = gateway
        .base_url
        .strip_prefix("http://")
        .expect("a loopback base URL")
        .to_owned();
    let environment = Environment::collect(
        &gateway.config,
        &bind,
        &upstream.base_url,
        super::manifest::MANIFEST_RELATIVE,
        manifest_text,
    );

    let client = crate::support::client();
    let gauges = Arc::new(Gauges::default());
    let next = Arc::new(AtomicUsize::new(0));
    let sampler = Sampler::start(gateway.pid());
    let started_at = SystemTime::now();
    let started = Instant::now();

    let mut workers = Vec::with_capacity(scale.concurrency);
    for _ in 0..scale.concurrency {
        let (client, gauges, next) = (client.clone(), gauges.clone(), next.clone());
        let base_url = gateway.base_url.clone();
        let workload = profile.workload;
        let cancel_every = profile.cancel_every;
        let cancel_after = profile.cancel_after_output_chunks.unwrap_or(2);
        let total = scale.requests;
        workers.push(tokio::spawn(async move {
            let mut attempts = Vec::new();
            loop {
                let index = next.fetch_add(1, Ordering::Relaxed);
                if index >= total {
                    return attempts;
                }
                let shape = shape_for(workload, index);
                let cancel = cancel_every.is_some_and(|every| cancels(index, every));
                attempts.push(
                    attempt(
                        &client,
                        &base_url,
                        shape,
                        cancel.then_some(cancel_after),
                        &gauges,
                    )
                    .await,
                );
            }
        }));
    }

    let mut attempts = Vec::with_capacity(scale.requests);
    for worker in workers {
        attempts.extend(worker.await.expect("a capacity worker does not panic"));
    }
    let elapsed = started.elapsed();
    let resources = sampler.finish(elapsed);

    let accepted = count(&attempts, |a| {
        matches!(a.outcome, Outcome::Accepted | Outcome::Cancelled)
    });
    let observed = await_usage_records(&gateway, accepted).await;
    let leaked = await_closed_upstreams(&upstream).await;

    let latency: Vec<f64> = attempts.iter().map(|a| a.latency_ms).collect();
    let ttft: Vec<f64> = attempts.iter().filter_map(|a| a.ttft_ms).collect();
    let lifetime: Vec<f64> = attempts
        .iter()
        .filter_map(|a| a.stream_lifetime_ms)
        .collect();
    let offered = attempts.len() as u64;
    let rejected = count(&attempts, |a| a.outcome == Outcome::Rejected);
    let failed = count(&attempts, |a| a.outcome == Outcome::Failed);
    let transport_failures = count(&attempts, |a| a.outcome == Outcome::TransportFailure);
    let errors = failed + transport_failures;
    let seconds = elapsed.as_secs_f64().max(f64::EPSILON);

    let result = CapacityResult {
        schema_version: RESULT_SCHEMA_VERSION,
        profile: ProfileEcho::new(profile, tier),
        run: RunMeta::new(started_at, elapsed),
        environment,
        throughput: Throughput {
            offered,
            accepted,
            rejected,
            errors,
            elapsed_ms: elapsed.as_millis(),
            offered_rps: offered as f64 / seconds,
            accepted_rps: accepted as f64 / seconds,
            closed_loop: true,
        },
        latency_ms: Percentiles::of(&latency).expect("a profile offers at least one request"),
        ttft_ms: Percentiles::of(&ttft),
        stream_lifetime_ms: Percentiles::of(&lifetime),
        resources,
        occupancy: Occupancy {
            offered_concurrency: scale.concurrency,
            in_flight_peak: gauges.in_flight_peak.load(Ordering::Relaxed),
            awaiting_first_byte_peak: gauges.awaiting_peak.load(Ordering::Relaxed),
            admission_queue_capacity: QUEUE_CAPACITY,
        },
        outcomes: Outcomes {
            by_status: tally(attempts.iter().filter_map(|a| a.status.map(u64::from))),
            rejections_by_error_type: error_types(&attempts, Outcome::Rejected),
            errors_by_error_type: error_types(&attempts, Outcome::Failed),
            client_cancelled: count(&attempts, |a| a.outcome == Outcome::Cancelled),
            transport_failures,
        },
        usage_records: UsageRecords {
            expected: accepted,
            observed: observed.len() as u64,
            missing: accepted.saturating_sub(observed.len() as u64),
            by_status: tally(
                observed
                    .iter()
                    .map(|record| record["status"].as_str().unwrap_or("unknown").to_owned()),
            ),
        },
        upstream: Upstream {
            requests: upstream.state.received(),
            streams_opened: upstream.state.opened_streams(),
            streams_open_at_end: leaked,
        },
        verdicts: Vec::new(),
    };
    let verdicts = verdicts(&result);
    CapacityResult { verdicts, ..result }
}

/// The hard failures, evaluated against the manifest's thresholds. Every one is
/// a property the gateway either has or does not have, independently of how fast
/// the runner was.
fn verdicts(result: &CapacityResult) -> Vec<Verdict> {
    let thresholds = &result.profile.thresholds;
    let offered = result.throughput.offered.max(1) as f64;
    let mut verdicts = vec![
        Verdict::at_least(
            "min_accepted_fraction",
            result.throughput.accepted as f64 / offered,
            thresholds.min_accepted_fraction,
        ),
        Verdict::at_most(
            "max_rejections",
            result.throughput.rejected as f64,
            thresholds.max_rejections as f64,
        ),
        Verdict::at_most(
            "max_errors",
            result.throughput.errors as f64,
            thresholds.max_errors as f64,
        ),
        Verdict::at_most(
            "max_missing_usage_records",
            result.usage_records.missing as f64,
            thresholds.max_missing_usage_records as f64,
        ),
        Verdict::at_most(
            "max_leaked_upstream_streams",
            result.upstream.streams_open_at_end as f64,
            thresholds.max_leaked_upstream_streams as f64,
        ),
    ];
    verdicts.extend(memory_verdict(
        &result.resources,
        thresholds.max_rss_growth_kib,
    ));
    verdicts
}

/// What a run's resource sampling is worth as a gate.
///
/// Off a `/proc` platform there is no memory evidence, so there is nothing to
/// assert rather than a threshold that would pass vacuously. On a `/proc` host an
/// absent measurement is a different thing: the sampler lost its subject — the
/// process exited, or its `/proc` entries could not be read — and a run that
/// cannot say what memory did must not read like one that measured and passed.
/// A run whose sampler never got a turn is the same case: the baseline it seeded
/// the peak from is not a measurement of the load.
pub fn memory_verdict(resources: &ResourceReport, max_growth_kib: u64) -> Option<Verdict> {
    if resources.procfs && resources.samples == 0 {
        return Some(Verdict::at_most("resource_sampling", 1.0, 0.0));
    }
    match resources.rss_kib {
        Some(rss) => Some(Verdict::at_most(
            "max_rss_growth_kib",
            rss.growth() as f64,
            max_growth_kib as f64,
        )),
        None if resources.procfs => Some(Verdict::at_most("resource_sampling", 1.0, 0.0)),
        None => None,
    }
}

/// Offer one request and measure it.
async fn attempt(
    client: &reqwest::Client,
    base_url: &str,
    shape: Shape,
    cancel_after_output_chunks: Option<usize>,
    gauges: &Gauges,
) -> Attempt {
    let started = Instant::now();
    gauges.enter();
    // Cleared by the answer's first byte, and unconditionally when the attempt
    // ends, so an attempt that never gets one cannot pin the gauge.
    let waiting = &mut true;
    let sent = client
        .post(format!("{base_url}{}", shape.route))
        .bearer_auth(GATEWAY_KEY)
        .json(&shape.body())
        .send()
        .await;
    let response = match sent {
        Ok(response) => response,
        Err(_) => {
            gauges.leave(waiting);
            return Attempt {
                outcome: Outcome::TransportFailure,
                status: None,
                error_type: None,
                latency_ms: millis(started.elapsed()),
                ttft_ms: None,
                stream_lifetime_ms: None,
            };
        }
    };
    let status = response.status();
    let headers_at = started.elapsed();

    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        gauges.leave(waiting);
        let error_type = serde_json::from_str::<Value>(&body)
            .ok()
            .and_then(|value| value.get("error")?.get("type")?.as_str().map(str::to_owned));
        return Attempt {
            outcome: if status.as_u16() == 429 || status.as_u16() == 503 {
                Outcome::Rejected
            } else {
                Outcome::Failed
            },
            status: Some(status.as_u16()),
            error_type,
            latency_ms: millis(started.elapsed()),
            ttft_ms: None,
            stream_lifetime_ms: None,
        };
    }

    if !shape.stream {
        // A buffered answer's headers *are* its first bytes: the gateway has the
        // whole body before it answers at all.
        gauges.first_byte(waiting);
        let read = response.bytes().await;
        gauges.leave(waiting);
        return Attempt {
            outcome: if read.is_ok() {
                Outcome::Accepted
            } else {
                Outcome::TransportFailure
            },
            status: Some(status.as_u16()),
            error_type: None,
            latency_ms: millis(started.elapsed()),
            ttft_ms: None,
            stream_lifetime_ms: None,
        };
    }

    // A stream: time to first token is the first relayed byte, and its lifetime
    // is what happens after that.
    let mut stream = response.bytes_stream();
    let mut first_byte: Option<Duration> = None;
    // Relayed output *events*, not transport chunks: the two do not map
    // one-to-one, and a starved reader can take several events in one read. The
    // text is accumulated because a marker can also straddle a chunk boundary.
    let mut relayed = String::new();
    let mut cancelled = false;
    let mut torn = false;
    while let Some(chunk) = stream.next().await {
        let Ok(chunk) = chunk else {
            torn = true;
            break;
        };
        first_byte.get_or_insert_with(|| started.elapsed());
        gauges.first_byte(waiting);
        let Some(after) = cancel_after_output_chunks else {
            continue;
        };
        relayed.push_str(&String::from_utf8_lossy(&chunk));
        // Only relayed output makes a cancellation charge real, so the hang-up
        // waits for it rather than for the stream's preamble.
        if output_events(&relayed) >= after {
            cancelled = true;
            break;
        }
    }
    // Dropping the body without draining it is what a closed browser tab looks
    // like, and it is the case that leaks an upstream when it is mishandled.
    drop(stream);
    gauges.leave(waiting);
    let total = started.elapsed();
    Attempt {
        outcome: match (cancelled, torn) {
            (true, _) => Outcome::Cancelled,
            (false, true) => Outcome::TransportFailure,
            (false, false) => Outcome::Accepted,
        },
        status: Some(status.as_u16()),
        error_type: None,
        latency_ms: millis(total),
        ttft_ms: Some(millis(first_byte.unwrap_or(headers_at))),
        stream_lifetime_ms: Some(millis(
            total.saturating_sub(first_byte.unwrap_or(headers_at)),
        )),
    }
}

/// Wait for `expected` usage records, returning whatever arrived. A shortfall is
/// reported as a drop rather than panicked on: the artifact is the evidence, and
/// the threshold is what fails the run.
async fn await_usage_records(gateway: &Axond, expected: u64) -> Vec<Value> {
    let deadline = Instant::now() + SETTLE_TIMEOUT;
    loop {
        let records = gateway.usage_records();
        if records.len() as u64 >= expected || Instant::now() >= deadline {
            return records;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Upstream response bodies still open once every client is gone.
async fn await_closed_upstreams(upstream: &FakeUpstream) -> i64 {
    let deadline = Instant::now() + CLEANUP_TIMEOUT;
    loop {
        let open = upstream.state.open_streams();
        if open == 0 || Instant::now() >= deadline {
            return open;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Relayed model-output events in a stream read so far, over both wire families.
///
/// Framed per SSE event rather than counted as substrings: a stream's preamble
/// mentions `content` without relaying any (`message_start` carries an empty
/// content array), and an Anthropic delta names its type in both the event line
/// and the payload, so substring counting would both over- and under-state the
/// output. Only complete events count — a trailing partial one is counted when
/// the rest of it arrives, which is why the driver accumulates the text.
pub fn output_events(relayed: &str) -> usize {
    relayed
        .split_inclusive("\n\n")
        .filter(|event| event.ends_with("\n\n") && relays_output(event))
        .count()
}

/// Whether one SSE event carries model output, over either wire family.
fn relays_output(event: &str) -> bool {
    let named_delta = event
        .lines()
        .filter_map(|line| line.strip_prefix("event:"))
        .any(|name| name.trim() == "content_block_delta");
    if named_delta {
        return true;
    }
    event
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .filter_map(|payload| serde_json::from_str::<Value>(payload.trim()).ok())
        .any(|payload| {
            let kind = payload["type"].as_str().unwrap_or_default();
            // Anthropic without an event line, and the Responses wire.
            if kind == "content_block_delta" || kind == "response.output_text.delta" {
                return true;
            }
            // A chat chunk relays output when a choice's delta holds text.
            payload["choices"].as_array().is_some_and(|choices| {
                choices.iter().any(|choice| {
                    choice["delta"]["content"]
                        .as_str()
                        .is_some_and(|text| !text.is_empty())
                })
            })
        })
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}

fn count(attempts: &[Attempt], predicate: impl Fn(&Attempt) -> bool) -> u64 {
    attempts.iter().filter(|a| predicate(a)).count() as u64
}

fn error_types(attempts: &[Attempt], outcome: Outcome) -> BTreeMap<String, u64> {
    tally(
        attempts
            .iter()
            .filter(|a| a.outcome == outcome)
            .map(|a| a.error_type.clone().unwrap_or_else(|| "untyped".to_owned())),
    )
}

fn tally<T: ToString>(values: impl Iterator<Item = T>) -> BTreeMap<String, u64> {
    let mut counts = BTreeMap::new();
    for value in values {
        *counts.entry(value.to_string()).or_default() += 1;
    }
    counts
}
