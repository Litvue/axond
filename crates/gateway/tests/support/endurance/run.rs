//! The endurance driver: offers a profile's mixed workload to a real `axond`
//! process for as long as the manifest says, and writes down what happened
//! while it did.
//!
//! Two things make this different from the capacity driver rather than a longer
//! version of it. The run is bounded by *time* rather than by a request count,
//! because the question is what happens to a process that has been serving for
//! hours; and everything the driver keeps is bounded, because a harness that
//! accumulates one record per request for twelve hours is a memory leak
//! measuring a memory leak. Attempts are folded into segment summaries and
//! dropped, latency samples are decimated, usage records are reconciled in
//! batches and released, and the raw time series goes to a file as it is taken.

use std::collections::BTreeMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime};

use futures::StreamExt;
use serde_json::{Value, json};
use tokio::sync::Mutex;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use super::ledger::{Ledger, Tally};
use super::manifest::{Ending, Profile, RESULT_SCHEMA_VERSION, Scale, Tier};
use super::plan::{self, KEY_DIR_PLACEHOLDER, Planned, Tenant};
use super::result::{
    Distribution, EnduranceResult, Fingerprints, Occupancy, ProfileEcho, Reconciliation, Resources,
    RunMeta, Segment, Span, Throughput, Trend, Upstream, Workload,
};
use super::sampler::{Finished, Sample, Sampler, USER_HZ};
use crate::support::capacity::result::{Environment, Percentiles, Verdict};
use crate::support::gateway::Axond;
use crate::support::upstream::FakeUpstream;

/// The bounds an endurance run is served under, written out so the recorded
/// config hash pins them. The admission ceilings sit far above the manifest's
/// concurrency: shedding has its own suite, and here a `503` is a finding.
/// `max_attempts = 1` keeps the planned faults planned — a retry would turn a
/// deliberate upstream failure into a success and one accounting row into two.
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

/// The admission queue the profiles are served with, recorded as configured.
const QUEUE_CAPACITY: u64 = 0;

/// Overrides the tier's duration, for an operator dispatching a shorter or
/// longer soak than the manifest commits to. The artifact records both the
/// requested duration and the fact that it did not come from the manifest.
pub const DURATION_ENV: &str = "AXOND_ENDURANCE_DURATION_MS";

/// How long usage records may trail the last client byte before they count as
/// lost. Settlement is detached from the request, so this bounds the *sink*.
const SETTLE_TIMEOUT: Duration = Duration::from_secs(120);

/// How long the record sink must stay silent after the expected count is
/// reached before the wait accepts that everything has arrived. The count alone
/// cannot say so, because it counts duplicates the shards have not been read to
/// find yet.
const SETTLE_QUIET: Duration = Duration::from_millis(500);

/// How long an upstream body may stay open once every client is gone.
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(60);

/// How long the process is left idle before its settled resources are read.
/// Memory and descriptors are released as connections close, and reading the
/// instant the load stops would call ordinary teardown a leak.
const QUIESCE: Duration = Duration::from_secs(5);

/// How many output events a cancelled stream reads before the caller hangs up.
/// Enough that the relay has demonstrably begun, so the cancellation charges a
/// partial spend rather than racing the stream's preamble.
const CANCEL_AFTER_EVENTS: usize = 2;

/// How much wall clock a per-hour slope needs behind it before it is believed.
/// Extrapolating an hourly figure from a fifteen-second smoke would fail on
/// noise, and passing it would mean nothing either.
const MIN_TREND_HOURS: f64 = 0.5;

/// The most latency observations one distribution keeps. Beyond it the sample
/// is decimated rather than truncated, so the percentiles describe the whole
/// run instead of its first minutes.
const RETAINED_SAMPLES: usize = 100_000;

/// How often finished attempts and emitted usage records are folded into the
/// open segment and released, independently of how long a segment lasts.
const DRAIN_INTERVAL: Duration = Duration::from_millis(250);

/// How long finished work may sit in memory before it is folded into the open
/// segment and released. The drain tick, or the segment when a segment is
/// shorter than the tick — never the segment when it is longer, which is the
/// whole point: what the driver holds is bounded by a quarter of a second of
/// traffic whether the manifest segments the run every 2.5 seconds or every
/// fifteen minutes.
pub fn drain_interval(segment_ms: u64) -> Duration {
    DRAIN_INTERVAL.min(Duration::from_millis(segment_ms.max(1)))
}

/// Only one profile offers load at a time: two runs on one host measure each
/// other's contention, and the artifact would still read as an envelope.
fn load_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(Mutex::default)
}

/// How one offered request ended, judged against what the plan asked for.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Outcome {
    /// Served and read to the end, as planned.
    Completed,
    /// The caller hung up mid-answer, as planned.
    Cancelled,
    /// The upstream died mid-stream after relay began, as planned.
    Dropped,
    /// The upstream refused before a byte was relayed, as planned.
    Faulted,
    /// A planned fault answered from an open circuit rather than dispatched.
    /// The alias points at a target that refuses every request, so its breaker
    /// trips early and stays tripped: after the first few, the planned fault
    /// arrives as `all_provider_circuits_open` without an upstream attempt.
    /// That is the gateway working, and it accounts nothing because it spent
    /// nothing.
    Shed,
    /// Shed by admission: `429`/`503`. Never planned.
    Rejected,
    /// Anything else. This is what a soak exists to find.
    Unplanned,
}

impl Outcome {
    fn planned(self) -> bool {
        !matches!(self, Self::Rejected | Self::Unplanned)
    }

    /// Whether the request reached an upstream attempt, and so owes exactly one
    /// usage record. What was never dispatched has nothing to account for, and
    /// counting it as missing evidence would make the reconciliation gate fire
    /// on correct behaviour.
    fn settles(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Cancelled | Self::Dropped | Self::Faulted
        )
    }
}

/// One measured request. Folded into a segment and dropped: a twelve-hour run
/// offers too many to keep.
struct Attempt {
    plan: Planned,
    outcome: Outcome,
    status: Option<u16>,
    latency_ms: f64,
    ttft_ms: Option<f64>,
    stream_lifetime_ms: Option<f64>,
}

/// Run `profile` at `tier` and return its result artifact, taking the duration
/// override from the environment and writing under the profile's own name.
pub async fn run(profile: &Profile, tier: Tier, manifest_text: &str) -> EnduranceResult {
    let dispatched = std::env::var(DURATION_ENV).ok();
    let dispatch = Dispatch {
        duration_ms: dispatched.as_deref(),
        stem: &profile.id,
    };
    run_with(profile, tier, manifest_text, dispatch).await
}

/// How a run was dispatched. The duration override is passed rather than read
/// so a test can offer both tiers in one process without touching the
/// environment of a running program, and the stem keeps a regression run's
/// artifacts from overwriting the tier's qualifying ones.
#[derive(Clone, Copy)]
pub struct Dispatch<'a> {
    pub duration_ms: Option<&'a str>,
    pub stem: &'a str,
}

/// Run `profile` at `tier` as `dispatch` asks, and return its result artifact.
pub async fn run_with(
    profile: &Profile,
    tier: Tier,
    manifest_text: &str,
    dispatch: Dispatch<'_>,
) -> EnduranceResult {
    let _offering = load_lock().lock().await;
    let mut scale = *profile.scale(tier);
    let requested = requested_duration(&scale, tier, dispatch.duration_ms);
    // Recorded on the artifact, not just used: a dispatched run at a shorter
    // duration is segmented to match, and the echo has to say what it was. Both
    // fields move together — an echo carrying the manifest's twelve hours beside
    // a segment length sized for five describes a run that never happened.
    scale.segment_ms = segment_ms(&scale, requested);
    scale.duration_ms = requested.duration.as_millis() as u64;

    let key_dir = key_dir(profile, tier);
    let (tenants, tenant_config) = plan::tenants(&key_dir);
    let upstream = FakeUpstream::start().await;
    let gateway = Axond::start_with(&upstream.base_url, &format!("{TUNING}{tenant_config}")).await;
    let bind = gateway
        .base_url
        .strip_prefix("http://")
        .expect("a loopback base URL")
        .to_owned();
    // The key directory is per process, so it is replaced before the config is
    // hashed: an input hash that changed every run would make every artifact
    // incomparable with every other.
    let config = gateway
        .config
        .replace(&key_dir.display().to_string(), KEY_DIR_PLACEHOLDER);
    let mut environment = Environment::collect(&config, &bind, &upstream.base_url, manifest_text);
    environment.manifest.path = super::manifest::MANIFEST_RELATIVE.to_owned();

    let samples_path =
        EnduranceResult::directory(tier).join(format!("{}.samples.jsonl", dispatch.stem));
    let ledger = Ledger::create(
        &EnduranceResult::directory(tier).join(format!("{}-fingerprints", dispatch.stem)),
    );
    let sampler = Sampler::start(
        gateway.pid(),
        Duration::from_millis(scale.sample_interval_ms.max(1)),
        &samples_path,
    );

    let rotation = Arc::new(plan::rotation(&profile.mix, profile.seed));
    let gauges = Arc::new(Gauges::default());
    let next = Arc::new(AtomicUsize::new(0));
    let (tx, mut rx) = unbounded_channel();
    let started_at = SystemTime::now();
    let started = Instant::now();
    let deadline = started + requested.duration;

    // One client across the workers, as the capacity driver does: a pool per
    // worker would put the driver's own descriptors in the gateway's baseline
    // and make the two harnesses' resource shapes incomparable.
    let client = crate::support::client();
    let mut workers = Vec::with_capacity(scale.concurrency);
    for _ in 0..scale.concurrency {
        workers.push(tokio::spawn(worker(
            client.clone(),
            gateway.base_url.clone(),
            tenants.clone(),
            rotation.clone(),
            next.clone(),
            gauges.clone(),
            tx.clone(),
            deadline,
            Duration::from_millis(scale.think_time_ms),
        )));
    }
    // The driver's own handle would keep the channel open past the last worker.
    drop(tx);

    let mut aggregate = Aggregate::new(scale.segment_ms, ledger);
    // Close each segment on its boundary while the load continues, so a run
    // that is killed at hour eleven has already summarised eleven hours.
    let segment = Duration::from_millis(scale.segment_ms);
    let mut boundary = started + segment;
    // Folded on a short tick rather than on the boundary: a fifteen-minute
    // segment of finished attempts and parsed usage records waiting in memory
    // is the accumulation this harness exists to detect, in the harness.
    let drain_every = drain_interval(scale.segment_ms);
    let mut tick = started + drain_every;
    while Instant::now() < deadline {
        let until = tick.min(boundary).min(deadline);
        tokio::time::sleep_until(until.into()).await;
        aggregate.drain(&mut rx, &sampler, &gateway, &gauges);
        let now = Instant::now();
        while tick <= now {
            tick += drain_every;
        }
        if now >= boundary {
            aggregate.close_segment(started.elapsed());
            boundary += segment;
        }
    }

    for worker in workers {
        worker.await.expect("an endurance worker does not panic");
    }
    // The callers are gone, so their connections should be too. Held open, the
    // driver's idle pool would keep tens of inbound sockets alive well past the
    // quiesce, and the settled descriptor reading would be measuring the
    // harness rather than what the replica failed to give back.
    drop(client);
    let elapsed = started.elapsed();
    aggregate.drain(&mut rx, &sampler, &gateway, &gauges);
    // The tail of the run, closed while it still counts as offered load: a
    // duration that is not a whole number of segments would otherwise leave it
    // in the open segment, to be mixed with the idle reading that follows.
    aggregate.close_segment(elapsed);

    // Everything after the load: records still settling, upstream bodies still
    // closing, and the memory the process gives back once it is idle.
    let expected_records = aggregate.settling;
    await_usage_records(&gateway, &mut aggregate, expected_records).await;
    let leaked = await_closed_upstreams(&upstream).await;
    tokio::time::sleep(QUIESCE).await;
    let finished = sampler.finish();
    aggregate.absorb(&finished.pending);
    // Recorded, but not as a segment of the run: nothing was offered during it.
    aggregate.under_load = false;
    aggregate.close_segment(started.elapsed());

    let result = assemble(
        profile,
        tier,
        &scale,
        requested,
        environment,
        aggregate,
        finished,
        &samples_path,
        drain_every,
        started_at,
        elapsed,
        &gauges,
        Upstream {
            requests: upstream.state.received(),
            streams_opened: upstream.state.opened_streams(),
            streams_open_at_end: leaked,
        },
    );
    let verdicts = verdicts(&result);
    EnduranceResult { verdicts, ..result }
}

/// How long the run was asked to last, and who asked.
#[derive(Clone, Copy)]
pub struct Requested {
    pub duration: Duration,
    pub source: &'static str,
}

/// How long a segment lasts. The manifest's length, except when the run was
/// asked for a shorter duration than the manifest's: a segmentation that would
/// leave a dispatched forty-minute run with three of the eight segments it is
/// gated on would fail it for being short rather than for anything it measured.
fn segment_ms(scale: &Scale, requested: Requested) -> u64 {
    // One segment more than the gate asks for. Dividing by the gate exactly
    // would put the last boundary on the deadline, so a dispatched run would
    // fail its segment count for losing a millisecond rather than for anything
    // it measured.
    let fitting = requested.duration.as_millis() as u64 / (scale.thresholds.min_segments + 1);
    scale.segment_ms.min(fitting).max(1)
}

/// The override belongs to the soak alone. Both tiers live in one test binary,
/// so honouring it for the smoke tier would make a five-hour dispatch offer
/// five hours twice and be killed by the runner before it published anything.
pub fn requested_duration(scale: &Scale, tier: Tier, override_ms: Option<&str>) -> Requested {
    let asked = override_ms.filter(|_| tier == Tier::Soak);
    match asked.and_then(|value| value.trim().parse::<u64>().ok()) {
        Some(ms) => Requested {
            duration: Duration::from_millis(ms),
            source: "environment",
        },
        None => Requested {
            duration: Duration::from_millis(scale.duration_ms),
            source: "manifest",
        },
    }
}

/// Where this run's tenant key files live. Under `target/` rather than in a
/// temporary directory, so a soak's inputs are still readable after it ends and
/// a wiped `/tmp` cannot take a running gateway's inbound keys with it.
fn key_dir(profile: &Profile, tier: Tier) -> PathBuf {
    let dir = EnduranceResult::directory(tier).join(format!("{}-keys", profile.id));
    std::fs::create_dir_all(&dir).expect("the endurance key directory is writable");
    dir
}

/// One worker: offers requests until the deadline, pausing `think_time` between
/// them. Closed-loop, so the offered rate is a result of service time and the
/// committed think time rather than an arrival rate pushed at the replica.
#[allow(clippy::too_many_arguments)]
async fn worker(
    client: reqwest::Client,
    base_url: String,
    tenants: Vec<Tenant>,
    rotation: Arc<Vec<Ending>>,
    next: Arc<AtomicUsize>,
    gauges: Arc<Gauges>,
    tx: UnboundedSender<Attempt>,
    deadline: Instant,
    think_time: Duration,
) {
    while Instant::now() < deadline {
        let index = next.fetch_add(1, Ordering::Relaxed);
        let planned = plan::planned(index, &tenants, &rotation);
        let attempt = attempt(&client, &base_url, planned, &gauges).await;
        // A closed receiver means the driver has stopped collecting; there is
        // nothing left for this worker to contribute.
        if tx.send(attempt).is_err() {
            return;
        }
        if !think_time.is_zero() {
            tokio::time::sleep(think_time).await;
        }
    }
}

/// Offer one planned request and measure it.
async fn attempt(
    client: &reqwest::Client,
    base_url: &str,
    plan: Planned,
    gauges: &Gauges,
) -> Attempt {
    let sent_at = Instant::now();
    gauges.enter();
    let waiting = &mut true;
    let finish = |outcome, status, ttft_ms, stream_lifetime_ms, plan| Attempt {
        plan,
        outcome,
        status,
        latency_ms: millis(sent_at.elapsed()),
        ttft_ms,
        stream_lifetime_ms,
    };

    let sent = client
        .post(format!("{base_url}{}", plan.shape.route))
        .bearer_auth(&plan.tenant.key)
        .json(&body(&plan))
        .send()
        .await;
    let response = match sent {
        Ok(response) => response,
        Err(_) => {
            gauges.leave(waiting);
            return finish(Outcome::Unplanned, None, None, None, plan);
        }
    };
    let status = response.status();
    let headers_at = sent_at.elapsed();

    if !status.is_success() {
        let refusal = response.text().await.unwrap_or_default();
        gauges.leave(waiting);
        let code = status.as_u16();
        let outcome = match plan.ending {
            // The planned fault: an upstream that refused reaches the caller as
            // a `502`, and any other error status is a different failure.
            Ending::Faulted if code == 502 => Outcome::Faulted,
            // Once the fault target's breaker is open the same planned fault
            // arrives as a typed circuit refusal. The typed error is asserted
            // rather than assumed: a bare `503` here would be admission
            // shedding, which is not what this plan asked for.
            Ending::Faulted
                if code == 503 && error_type(&refusal) == Some("all_provider_circuits_open") =>
            {
                Outcome::Shed
            }
            _ if code == 429 || code == 503 => Outcome::Rejected,
            _ => Outcome::Unplanned,
        };
        return finish(outcome, Some(code), None, None, plan);
    }

    if !plan.shape.stream {
        gauges.first_byte(waiting);
        let read = response.bytes().await;
        gauges.leave(waiting);
        let outcome = match (plan.ending, read.is_ok()) {
            (Ending::Complete, true) => Outcome::Completed,
            _ => Outcome::Unplanned,
        };
        return finish(outcome, Some(status.as_u16()), None, None, plan);
    }

    let mut stream = response.bytes_stream();
    let mut first_byte: Option<Duration> = None;
    let mut relayed = String::new();
    let mut cancelled = false;
    let mut torn = false;
    while let Some(chunk) = stream.next().await {
        let Ok(chunk) = chunk else {
            torn = true;
            break;
        };
        first_byte.get_or_insert_with(|| sent_at.elapsed());
        gauges.first_byte(waiting);
        if plan.ending != Ending::Cancelled {
            continue;
        }
        relayed.push_str(&String::from_utf8_lossy(&chunk));
        // Only relayed output makes a cancellation charge real, so the hang-up
        // waits for it rather than for the stream's preamble.
        if crate::support::capacity::run::output_events(&relayed) >= CANCEL_AFTER_EVENTS {
            cancelled = true;
            break;
        }
    }
    // Dropping the body without draining it is what a closed browser tab looks
    // like, and it is the case that leaks an upstream when it is mishandled.
    drop(stream);
    gauges.leave(waiting);
    let outcome = match plan.ending {
        Ending::Complete if !torn => Outcome::Completed,
        Ending::Cancelled if cancelled => Outcome::Cancelled,
        // A dropped upstream reaches the caller either as a torn body or as a
        // terminated stream, depending on where the relay was when it died.
        // Both are the planned ending; neither is a clean answer.
        Ending::Dropped => Outcome::Dropped,
        _ => Outcome::Unplanned,
    };
    let total = sent_at.elapsed();
    let ttft = first_byte.unwrap_or(headers_at);
    finish(
        outcome,
        Some(status.as_u16()),
        Some(millis(ttft)),
        Some(millis(total.saturating_sub(ttft))),
        plan,
    )
}

/// The `error.type` of a typed refusal, if the body is one. The gateway's error
/// envelope is part of its contract, so a refusal is identified by what it says
/// rather than by its status alone.
fn error_type(body: &str) -> Option<&'static str> {
    const TYPES: [&str; 1] = ["all_provider_circuits_open"];
    let parsed: Value = serde_json::from_str(body).ok()?;
    let found = parsed["error"]["type"].as_str()?;
    TYPES.into_iter().find(|known| *known == found)
}

fn body(plan: &Planned) -> Value {
    let (alias, stream) = (plan.shape.alias, plan.shape.stream);
    match plan.shape.route {
        plan::EMBEDDINGS => json!({ "model": alias, "input": "endurance" }),
        plan::RESPONSES => json!({ "model": alias, "stream": stream, "input": "endurance" }),
        plan::MESSAGES => json!({
            "model": alias,
            "stream": stream,
            "max_tokens": 1024,
            "messages": [{ "role": "user", "content": "endurance" }],
        }),
        _ => json!({
            "model": alias,
            "stream": stream,
            "messages": [{ "role": "user", "content": "endurance" }],
        }),
    }
}

/// Driver-side gauges. `awaiting_first_byte` is the closest a client can get to
/// the replica's queue occupancy without trusting the replica's own telemetry.
/// The peaks are read *and reset* per segment, so one spike in hour one does
/// not describe hour twelve.
#[derive(Default)]
struct Gauges {
    in_flight: AtomicU64,
    in_flight_peak: AtomicU64,
    in_flight_peak_overall: AtomicU64,
    awaiting: AtomicU64,
    awaiting_peak: AtomicU64,
    awaiting_peak_overall: AtomicU64,
}

impl Gauges {
    fn enter(&self) {
        let now = self.in_flight.fetch_add(1, Ordering::Relaxed) + 1;
        self.in_flight_peak.fetch_max(now, Ordering::Relaxed);
        self.in_flight_peak_overall
            .fetch_max(now, Ordering::Relaxed);
        let waiting = self.awaiting.fetch_add(1, Ordering::Relaxed) + 1;
        self.awaiting_peak.fetch_max(waiting, Ordering::Relaxed);
        self.awaiting_peak_overall
            .fetch_max(waiting, Ordering::Relaxed);
    }

    /// Called when the first byte of the *answer* arrives: response headers for
    /// a buffered request, the first relayed chunk for a stream.
    fn first_byte(&self, waiting: &mut bool) {
        if *waiting {
            *waiting = false;
            self.awaiting.fetch_sub(1, Ordering::Relaxed);
        }
    }

    fn leave(&self, waiting: &mut bool) {
        self.first_byte(waiting);
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
    }

    /// The peaks since the last read, and reset.
    fn take_peaks(&self) -> (u64, u64) {
        // Reset to the current level rather than to zero: the requests in
        // flight at a segment boundary are in flight during the next segment
        // too, and a peak that ignored them would understate it.
        (
            self.in_flight_peak
                .swap(self.in_flight.load(Ordering::Relaxed), Ordering::Relaxed),
            self.awaiting_peak
                .swap(self.awaiting.load(Ordering::Relaxed), Ordering::Relaxed),
        )
    }
}

/// Everything the driver keeps across the whole run, all of it bounded.
struct Aggregate {
    segment_ms: u64,
    segments: Vec<Segment>,
    offered: u64,
    accepted: u64,
    planned_faults: u64,
    circuit_shed: u64,
    /// Requests that reached an upstream attempt, and so owe a usage record.
    settling: u64,
    by_response_status: BTreeMap<String, u64>,
    cancelled: u64,
    rejected: u64,
    unplanned_errors: u64,
    by_tenant: BTreeMap<String, u64>,
    by_provider: BTreeMap<String, u64>,
    by_alias: BTreeMap<String, u64>,
    by_route: BTreeMap<String, u64>,
    by_ending: BTreeMap<String, u64>,
    streamed: u64,
    buffered: u64,
    latency: Reservoir,
    ttft: Reservoir,
    lifetime: Reservoir,
    planned_status_counts: BTreeMap<String, u64>,
    /// Request-id fingerprints, spilled to disk rather than held: identity over
    /// a twelve-hour run is millions of ids, and a set of them is the growth
    /// this harness exists to detect.
    ledger: Ledger,
    records_observed: u64,
    unidentified: u64,
    unexpected_statuses: u64,
    by_status: BTreeMap<String, u64>,
    by_namespace: BTreeMap<String, u64>,
    by_credential_source: BTreeMap<String, u64>,
    /// How many times the driver folded and released what it was holding.
    /// Recorded because "bounded independently of the segment length" is a
    /// claim about this number, not about the segment count beside it.
    drains: u64,
    /// Open segment, reset at each boundary.
    open: OpenSegment,
    /// Whether the workers are still offering. Cleared once they have stopped,
    /// so the segment that spans the settle and quiesce waits is marked as the
    /// idle reading it is.
    under_load: bool,
    /// Sample extremes over the whole run, kept as the segments are closed.
    rss_peak: u64,
    fds_peak: u64,
    sockets_peak: u64,
    samples_total: u64,
}

/// The part of an aggregate that belongs to the segment being filled.
#[derive(Default)]
struct OpenSegment {
    started_ms: u128,
    offered: u64,
    accepted: u64,
    unplanned_errors: u64,
    usage_records: u64,
    latency: Vec<f64>,
    ttft: Vec<f64>,
    samples: Vec<Sample>,
    in_flight_peak: u64,
    awaiting_peak: u64,
}

impl Aggregate {
    fn new(segment_ms: u64, ledger: Ledger) -> Self {
        Self {
            segment_ms,
            segments: Vec::new(),
            offered: 0,
            accepted: 0,
            planned_faults: 0,
            circuit_shed: 0,
            settling: 0,
            by_response_status: BTreeMap::new(),
            cancelled: 0,
            rejected: 0,
            unplanned_errors: 0,
            by_tenant: BTreeMap::new(),
            by_provider: BTreeMap::new(),
            by_alias: BTreeMap::new(),
            by_route: BTreeMap::new(),
            by_ending: BTreeMap::new(),
            streamed: 0,
            buffered: 0,
            latency: Reservoir::new(),
            ttft: Reservoir::new(),
            lifetime: Reservoir::new(),
            planned_status_counts: BTreeMap::new(),
            ledger,
            records_observed: 0,
            unidentified: 0,
            unexpected_statuses: 0,
            by_status: BTreeMap::new(),
            by_namespace: BTreeMap::new(),
            by_credential_source: BTreeMap::new(),
            drains: 0,
            open: OpenSegment::default(),
            under_load: true,
            rss_peak: 0,
            fds_peak: 0,
            sockets_peak: 0,
            samples_total: 0,
        }
    }

    /// Fold everything that has arrived since the last drain into the open
    /// segment, and let it go.
    fn drain(
        &mut self,
        rx: &mut UnboundedReceiver<Attempt>,
        sampler: &Sampler,
        gateway: &Axond,
        gauges: &Gauges,
    ) {
        self.drains += 1;
        while let Ok(attempt) = rx.try_recv() {
            self.absorb_attempt(&attempt);
        }
        let samples = sampler.drain();
        self.absorb(&samples);
        for record in gateway.drain_usage_records() {
            self.absorb_record(&record);
        }
        let (in_flight, awaiting) = gauges.take_peaks();
        self.open.in_flight_peak = self.open.in_flight_peak.max(in_flight);
        self.open.awaiting_peak = self.open.awaiting_peak.max(awaiting);
    }

    fn absorb_attempt(&mut self, attempt: &Attempt) {
        self.offered += 1;
        self.open.offered += 1;
        let ending = attempt.plan.ending;
        *self
            .by_tenant
            .entry(attempt.plan.tenant.namespace.to_owned())
            .or_default() += 1;
        *self
            .by_provider
            .entry(attempt.plan.shape.provider.to_owned())
            .or_default() += 1;
        *self
            .by_alias
            .entry(attempt.plan.shape.alias.to_owned())
            .or_default() += 1;
        *self
            .by_route
            .entry(attempt.plan.shape.route.to_owned())
            .or_default() += 1;
        *self
            .by_ending
            .entry(ending.as_str().to_owned())
            .or_default() += 1;
        if attempt.plan.shape.stream {
            self.streamed += 1;
        } else {
            self.buffered += 1;
        }
        match attempt.outcome {
            Outcome::Completed => {
                self.accepted += 1;
                self.open.accepted += 1;
            }
            Outcome::Cancelled => {
                self.accepted += 1;
                self.cancelled += 1;
                self.open.accepted += 1;
            }
            // A dropped stream was served: the caller got its relay, and the
            // upstream's death after that is the planned ending, not a refusal.
            Outcome::Dropped => {
                self.accepted += 1;
                self.open.accepted += 1;
            }
            Outcome::Faulted => self.planned_faults += 1,
            Outcome::Shed => {
                self.planned_faults += 1;
                self.circuit_shed += 1;
            }
            Outcome::Rejected => self.rejected += 1,
            Outcome::Unplanned => {}
        }
        *self
            .by_response_status
            .entry(
                attempt
                    .status
                    .map_or_else(|| "transport".to_owned(), |code| code.to_string()),
            )
            .or_default() += 1;
        if attempt.outcome.settles() {
            self.settling += 1;
            *self
                .planned_status_counts
                .entry(ending.usage_status().to_owned())
                .or_default() += 1;
        }
        if !attempt.outcome.planned() {
            self.unplanned_errors += 1;
            self.open.unplanned_errors += 1;
        }
        self.latency.push(attempt.latency_ms);
        self.open.latency.push(attempt.latency_ms);
        if let Some(ttft) = attempt.ttft_ms {
            self.ttft.push(ttft);
            self.open.ttft.push(ttft);
        }
        if let Some(lifetime) = attempt.stream_lifetime_ms {
            self.lifetime.push(lifetime);
        }
    }

    fn absorb(&mut self, samples: &[Sample]) {
        for sample in samples {
            self.rss_peak = self.rss_peak.max(sample.rss_kib);
            self.fds_peak = self.fds_peak.max(sample.fds);
            self.sockets_peak = self.sockets_peak.max(sample.sockets);
        }
        self.samples_total += samples.len() as u64;
        self.open.samples.extend_from_slice(samples);
    }

    /// Reconcile one usage record. Identity is `request_id`, which is globally
    /// unique, so a repeat is a duplicate rather than a coincidence; the ids
    /// themselves are fingerprinted and spilled to the ledger, because a
    /// twelve-hour run settles more of them than the harness may hold.
    fn absorb_record(&mut self, record: &Value) {
        self.records_observed += 1;
        self.open.usage_records += 1;
        match record["request_id"].as_str() {
            Some(id) => self.ledger.record(fingerprint(id)),
            None => self.unidentified += 1,
        }
        let status = record["status"].as_str().unwrap_or("unknown").to_owned();
        if !Ending::ALL
            .iter()
            .any(|ending| ending.settles(status.as_str()))
        {
            self.unexpected_statuses += 1;
        }
        *self.by_status.entry(status).or_default() += 1;
        *self
            .by_namespace
            .entry(record["namespace"].as_str().unwrap_or("unknown").to_owned())
            .or_default() += 1;
        *self
            .by_credential_source
            .entry(
                record["credential_source"]
                    .as_str()
                    .unwrap_or("unknown")
                    .to_owned(),
            )
            .or_default() += 1;
    }

    /// Close the open segment at `now`. A segment with neither a request nor a
    /// sample is not recorded: it would fit the trend through nothing.
    fn close_segment(&mut self, now: Duration) {
        let open = std::mem::take(&mut self.open);
        let elapsed = now.as_millis().saturating_sub(open.started_ms);
        if open.offered == 0 && open.samples.is_empty() {
            self.open.started_ms = now.as_millis();
            return;
        }
        let seconds = (elapsed as f64 / 1000.0).max(f64::EPSILON);
        let cpu = cpu_delta(&open.samples);
        self.segments.push(Segment {
            index: self.segments.len(),
            under_load: self.under_load,
            started_ms: open.started_ms,
            elapsed_ms: elapsed,
            offered: open.offered,
            accepted: open.accepted,
            unplanned_errors: open.unplanned_errors,
            offered_rps: open.offered as f64 / seconds,
            latency_ms: Percentiles::of(&open.latency),
            ttft_ms: Percentiles::of(&open.ttft),
            usage_records: open.usage_records,
            samples: open.samples.len() as u64,
            rss_kib_median: median(open.samples.iter().map(|s| s.rss_kib)),
            rss_kib_peak: open.samples.iter().map(|s| s.rss_kib).max(),
            sockets_median: median(open.samples.iter().map(|s| s.sockets)),
            sockets_peak: open.samples.iter().map(|s| s.sockets).max(),
            fds_median: median(open.samples.iter().map(|s| s.fds)),
            fds_peak: open.samples.iter().map(|s| s.fds).max(),
            cpu_seconds: cpu,
            cpu_utilization: cpu.map(|seconds_of_cpu| seconds_of_cpu / seconds),
            in_flight_peak: open.in_flight_peak,
            awaiting_first_byte_peak: open.awaiting_peak,
        });
        self.open.started_ms = now.as_millis();
    }
}

/// CPU seconds a segment's samples account for: the ticks between its first and
/// last reading. A segment with one sample cannot say.
fn cpu_delta(samples: &[Sample]) -> Option<f64> {
    let (first, last) = (samples.first()?, samples.last()?);
    Some(last.cpu_ticks.saturating_sub(first.cpu_ticks) as f64 / USER_HZ)
}

fn median(values: impl Iterator<Item = u64>) -> Option<u64> {
    let mut values: Vec<u64> = values.collect();
    if values.is_empty() {
        return None;
    }
    values.sort_unstable();
    Some(values[values.len() / 2])
}

/// A 64-bit fingerprint of a request id. Duplicate detection needs identity,
/// not the id itself, and holding millions of UUIDs as strings to prove nothing
/// was duplicated would be its own unbounded growth.
fn fingerprint(id: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    id.hash(&mut hasher);
    hasher.finish()
}

/// A bounded sample of a distribution. Every value while under the cap, then
/// every second, then every fourth: the retained values stay spread over the
/// whole run instead of describing only its first minutes.
struct Reservoir {
    values: Vec<f64>,
    observed: u64,
    stride: u64,
}

impl Reservoir {
    fn new() -> Self {
        Self {
            values: Vec::new(),
            observed: 0,
            stride: 1,
        }
    }

    fn push(&mut self, value: f64) {
        if self.observed.is_multiple_of(self.stride) {
            self.values.push(value);
            if self.values.len() >= RETAINED_SAMPLES {
                let mut keep = false;
                self.values.retain(|_| {
                    keep = !keep;
                    keep
                });
                self.stride *= 2;
            }
        }
        self.observed += 1;
    }

    fn distribution(&self) -> Distribution {
        Distribution {
            observed: self.observed,
            retained: self.values.len(),
            stride: self.stride,
            percentiles: Percentiles::of(&self.values),
        }
    }
}

/// Fold the run into its artifact.
#[allow(clippy::too_many_arguments)]
fn assemble(
    profile: &Profile,
    tier: Tier,
    scale: &Scale,
    requested: Requested,
    environment: Environment,
    aggregate: Aggregate,
    finished: Finished,
    samples_path: &Path,
    drain_every: Duration,
    started_at: SystemTime,
    elapsed: Duration,
    gauges: &Gauges,
    upstream: Upstream,
) -> EnduranceResult {
    let seconds = elapsed.as_secs_f64().max(f64::EPSILON);
    let resources = resources(&aggregate, &finished, scale, elapsed);
    let trend = trend(&aggregate.segments, scale);
    let tally = aggregate.ledger.tally();
    EnduranceResult {
        schema_version: RESULT_SCHEMA_VERSION,
        profile: ProfileEcho::new(profile, tier, scale),
        run: RunMeta::new(
            started_at,
            elapsed,
            requested.duration.as_millis() as u64,
            requested.source,
            relative_to_workspace(samples_path),
            drain_every.as_millis() as u64,
            aggregate.drains,
        ),
        environment,
        workload: Workload {
            by_tenant: aggregate.by_tenant.clone(),
            by_provider: aggregate.by_provider.clone(),
            by_alias: aggregate.by_alias.clone(),
            by_route: aggregate.by_route.clone(),
            by_ending: aggregate.by_ending.clone(),
            streamed: aggregate.streamed,
            buffered: aggregate.buffered,
        },
        throughput: Throughput {
            offered: aggregate.offered,
            accepted: aggregate.accepted,
            planned_faults: aggregate.planned_faults,
            circuit_shed: aggregate.circuit_shed,
            by_response_status: aggregate.by_response_status.clone(),
            unplanned_errors: aggregate.unplanned_errors,
            cancelled: aggregate.cancelled,
            rejected: aggregate.rejected,
            elapsed_ms: elapsed.as_millis(),
            offered_rps: aggregate.offered as f64 / seconds,
            closed_loop: true,
        },
        latency_ms: aggregate.latency.distribution(),
        ttft_ms: aggregate.ttft.distribution(),
        stream_lifetime_ms: aggregate.lifetime.distribution(),
        resources,
        occupancy: Occupancy {
            offered_concurrency: scale.concurrency,
            in_flight_peak: gauges.in_flight_peak_overall.load(Ordering::Relaxed),
            awaiting_first_byte_peak: gauges.awaiting_peak_overall.load(Ordering::Relaxed),
            admission_queue_capacity: QUEUE_CAPACITY,
        },
        reconciliation: Reconciliation {
            expected: aggregate.settling,
            records_observed: aggregate.records_observed,
            distinct_request_ids: tally.distinct,
            duplicates: tally.duplicates,
            missing: aggregate.settling.saturating_sub(tally.distinct),
            unexpected_statuses: aggregate.unexpected_statuses,
            unidentified: aggregate.unidentified,
            by_status: aggregate.by_status.clone(),
            by_namespace: aggregate.by_namespace.clone(),
            by_credential_source: aggregate.by_credential_source.clone(),
            planned_status_counts: aggregate.planned_status_counts.clone(),
            fingerprints: fingerprints(&tally),
        },
        upstream,
        segments: aggregate.segments,
        trend,
        verdicts: Vec::new(),
    }
}

fn resources(
    aggregate: &Aggregate,
    finished: &Finished,
    scale: &Scale,
    elapsed: Duration,
) -> Resources {
    let (Some(baseline), Some(settled)) = (finished.baseline, finished.settled) else {
        return Resources {
            sampled: false,
            procfs: finished.procfs,
            samples: finished.taken,
            sample_interval_ms: scale.sample_interval_ms,
            rss_kib: None,
            sockets: None,
            fds: None,
            cpu_seconds: None,
            cpu_utilization: None,
            user_hz: USER_HZ,
        };
    };
    let cpu_seconds = settled.cpu_ticks.saturating_sub(baseline.cpu_ticks) as f64 / USER_HZ;
    let span = |baseline: u64, peak: u64, settled: u64| {
        Some(Span {
            baseline,
            peak: peak.max(baseline).max(settled),
            settled,
        })
    };
    Resources {
        sampled: true,
        procfs: finished.procfs,
        samples: finished.taken,
        sample_interval_ms: scale.sample_interval_ms,
        rss_kib: span(baseline.rss_kib, aggregate.rss_peak, settled.rss_kib),
        sockets: span(baseline.sockets, aggregate.sockets_peak, settled.sockets),
        fds: span(baseline.fds, aggregate.fds_peak, settled.fds),
        cpu_seconds: Some(cpu_seconds),
        cpu_utilization: Some(cpu_seconds / elapsed.as_secs_f64().max(f64::EPSILON)),
        user_hz: USER_HZ,
    }
}

/// Fit the per-segment medians. A slope needs both enough segments and enough
/// wall clock to be a slope rather than a rounding error of the first minute,
/// so `fitted` records whether the drift gates may be believed.
pub fn trend(all: &[Segment], scale: &Scale) -> Trend {
    // Only the segments that had load offered through them: the last one is
    // the settle and quiesce wait, and an idle reading at the end would pull
    // the fitted slope down by exactly as much as the process gave back.
    let segments: Vec<Segment> = all.iter().filter(|s| s.under_load).cloned().collect();
    let segments = segments.as_slice();
    let hours = |segment: &Segment| {
        (segment.started_ms as f64 + segment.elapsed_ms as f64 / 2.0) / 3_600_000.0
    };
    let slope = |value: fn(&Segment) -> Option<u64>| {
        let points: Vec<(f64, f64)> = segments
            .iter()
            .filter_map(|segment| Some((hours(segment), value(segment)? as f64)))
            .collect();
        least_squares(&points)
    };
    let quarter = segments.len() / 4;
    let quarter_median = |slice: &[Segment]| median(slice.iter().filter_map(|s| s.rss_kib_median));
    let covered_hours = segments.last().map_or(0.0, |last| {
        (last.started_ms + last.elapsed_ms) as f64 / 3_600_000.0
    });
    Trend {
        segments: segments.len(),
        fitted: segments.len() as u64 >= scale.thresholds.min_segments
            && covered_hours >= MIN_TREND_HOURS,
        rss_kib_per_hour: slope(|s| s.rss_kib_median),
        sockets_per_hour: slope(|s| s.sockets_median),
        fds_per_hour: slope(|s| s.fds_median),
        first_quarter_rss_kib: quarter_median(&segments[..quarter]),
        last_quarter_rss_kib: quarter_median(&segments[segments.len() - quarter..]),
    }
}

/// The least-squares slope through `points`, or `None` when they do not
/// describe one: fewer than two points, or every point at the same instant.
fn least_squares(points: &[(f64, f64)]) -> Option<f64> {
    if points.len() < 2 {
        return None;
    }
    let n = points.len() as f64;
    let mean_x = points.iter().map(|(x, _)| x).sum::<f64>() / n;
    let mean_y = points.iter().map(|(_, y)| y).sum::<f64>() / n;
    let covariance: f64 = points
        .iter()
        .map(|(x, y)| (x - mean_x) * (y - mean_y))
        .sum();
    let variance: f64 = points.iter().map(|(x, _)| (x - mean_x).powi(2)).sum();
    (variance > f64::EPSILON).then(|| covariance / variance)
}

/// The hard failures, evaluated against the manifest's thresholds. Every one is
/// a property the gateway either has or does not have: how fast the runner was
/// changes the throughput, not whether a descriptor came back or an accounting
/// row went missing.
fn verdicts(result: &EnduranceResult) -> Vec<Verdict> {
    let thresholds = &result.profile.thresholds;
    let planned_successes = result
        .throughput
        .offered
        .saturating_sub(result.throughput.planned_faults)
        .max(1) as f64;
    let mut verdicts = vec![
        Verdict::at_least(
            "min_accepted_fraction",
            result.throughput.accepted as f64 / planned_successes,
            thresholds.min_accepted_fraction,
        ),
        Verdict::at_most(
            "max_unplanned_errors",
            result.throughput.unplanned_errors as f64,
            thresholds.max_unplanned_errors as f64,
        ),
        Verdict::at_most(
            "max_missing_usage_records",
            result.reconciliation.missing as f64,
            thresholds.max_missing_usage_records as f64,
        ),
        Verdict::at_most(
            "max_duplicate_usage_records",
            result.reconciliation.duplicates as f64,
            thresholds.max_duplicate_usage_records as f64,
        ),
        Verdict::at_most(
            "max_unexpected_usage_statuses",
            (result.reconciliation.unexpected_statuses + result.reconciliation.unidentified) as f64,
            thresholds.max_unexpected_usage_statuses as f64,
        ),
        Verdict::at_most(
            "max_leaked_upstream_streams",
            result.upstream.streams_open_at_end as f64,
            thresholds.max_leaked_upstream_streams as f64,
        ),
        Verdict::at_least(
            "min_segments",
            result.trend.segments as f64,
            thresholds.min_segments as f64,
        ),
        // Every axis the plan mixes over has to have been offered. A run that
        // believes it covered three tenants and four endings and covered one of
        // each has measured something, but not this profile.
        Verdict::at_least("workload_coverage", coverage(result), 1.0),
    ];
    verdicts.extend(resource_verdicts(result, thresholds.max_rss_growth_kib));
    verdicts.extend(drift_verdicts(result));
    verdicts
}

/// Whether the offered traffic covered the plan: all four endings, every
/// tenant, and both buffered and streamed requests.
fn coverage(result: &EnduranceResult) -> f64 {
    let endings = Ending::ALL
        .iter()
        .all(|ending| result.workload.by_ending.contains_key(ending.as_str()));
    let tenants = result.workload.by_tenant.len() >= plan::TENANTS;
    let both = result.workload.streamed > 0 && result.workload.buffered > 0;
    f64::from(u8::from(endings && tenants && both))
}

/// What the run's sampling is worth as a gate.
///
/// Off a `/proc` platform there is no resource evidence, so there is nothing to
/// assert rather than a threshold that would pass vacuously. On a `/proc` host
/// an absent measurement is a different thing: the sampler lost its subject,
/// and a run that cannot say what memory did must not read like one that
/// measured and passed.
fn resource_verdicts(result: &EnduranceResult, max_growth_kib: u64) -> Vec<Verdict> {
    let resources = &result.resources;
    if resources.procfs && resources.samples == 0 {
        return vec![Verdict::at_most("resource_sampling", 1.0, 0.0)];
    }
    let (Some(rss), Some(sockets)) = (resources.rss_kib, resources.sockets) else {
        return if resources.procfs {
            vec![Verdict::at_most("resource_sampling", 1.0, 0.0)]
        } else {
            Vec::new()
        };
    };
    vec![
        Verdict::at_most(
            "max_rss_growth_kib",
            rss.growth() as f64,
            max_growth_kib as f64,
        ),
        // Descriptors are the balance assertion: every connection the run
        // opened has to have been given back once the callers are gone.
        Verdict::at_most(
            "max_settled_socket_excess",
            sockets.settled_excess() as f64,
            result.profile.thresholds.max_settled_socket_excess as f64,
        ),
    ]
}

/// The drift gates, which only a run long enough to have fitted a trend is
/// asked to pass. A short tier records its slopes and is judged on growth and
/// balance instead: a per-hour figure extrapolated from fifteen seconds would
/// fail on noise, and passing it would mean nothing either.
fn drift_verdicts(result: &EnduranceResult) -> Vec<Verdict> {
    let thresholds = &result.profile.thresholds;
    if !result.trend.fitted {
        return Vec::new();
    }
    [
        (
            "max_rss_drift_kib_per_hour",
            thresholds.max_rss_drift_kib_per_hour,
            result.trend.rss_kib_per_hour,
        ),
        (
            "max_socket_drift_per_hour",
            thresholds.max_socket_drift_per_hour,
            result.trend.sockets_per_hour,
        ),
        (
            "max_fd_drift_per_hour",
            thresholds.max_fd_drift_per_hour,
            result.trend.fds_per_hour,
        ),
    ]
    .into_iter()
    .filter_map(|(name, bound, value)| Some(Verdict::at_most(name, value?, bound?)))
    .collect()
}

/// Drain usage records until every offered request has settled one, or the
/// settle deadline passes. A shortfall is recorded rather than panicked on: the
/// artifact is the evidence, and the threshold is what fails the run.
async fn await_usage_records(gateway: &Axond, aggregate: &mut Aggregate, expected: u64) {
    let deadline = Instant::now() + SETTLE_TIMEOUT;
    let mut quiet_since = None;
    loop {
        let before = aggregate.ledger.recorded();
        for record in gateway.drain_usage_records() {
            aggregate.absorb_record(&record);
        }
        if aggregate.ledger.recorded() > before {
            quiet_since = None;
        }
        // The count reached, and then nothing further for a while. Only the
        // shards know how many of those records were distinct, so a run whose
        // records repeat would be given up on early if the count alone ended
        // the wait — and the records still in flight would then be reported as
        // lost, on top of the duplicate that was the real fault.
        if aggregate.ledger.recorded() >= expected
            && quiet_since.get_or_insert_with(Instant::now).elapsed() >= SETTLE_QUIET
        {
            return;
        }
        if Instant::now() >= deadline {
            return;
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

/// How duplicate detection was done, kept beside the count it produced: a
/// reconciliation that says nothing was duplicated is only worth as much as
/// the method that looked.
fn fingerprints(tally: &Tally) -> Fingerprints {
    Fingerprints {
        recorded: tally.recorded,
        shards: tally.shards,
        peak_shard_fingerprints: tally.peak_shard_fingerprints,
        exact: true,
        path: relative_to_workspace(&tally.directory),
    }
}

fn relative_to_workspace(path: &Path) -> String {
    let root = crate::support::capacity::manifest::workspace_root();
    path.strip_prefix(&root)
        .unwrap_or(path)
        .display()
        .to_string()
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}
