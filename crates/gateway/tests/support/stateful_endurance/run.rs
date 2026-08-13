//! The driver: it offers the mixed workload, changes the deployment underneath
//! it on a committed schedule, and writes down what happened.
//!
//! The shape is the stateless endurance driver's — workers offering a seeded
//! rotation, a supervising loop that drains, segments and samples on a fixed
//! tick — because that shape is what keeps a twelve-hour run's own memory
//! bounded. What is new is everything the supervising loop does *besides*
//! watching: it publishes catalogue, credential and policy revisions to the
//! live fleet and measures how long each takes to become the one serving; it
//! takes the provider and then the usage database away and gives them back; it
//! restarts the fleet one replica at a time; and it probes a tenant that must
//! never be served from another tenant's pool.
//!
//! Every one of those is measured against a bound the manifest committed to
//! before the run, and the run stops early — with the reason on the artifact —
//! when the deployment does something no amount of further measurement would
//! excuse.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime};

use futures::StreamExt;
use serde_json::{Value, json};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

use super::durable::{self, Durable, Reach};
use super::fleet::{self, Deployment, Fleet, Revision};
use super::gate::{Gate, GateCounts, Mode};
use super::manifest::{
    DURATION_ENV, Event, Injected, Profile, RESULT_SCHEMA_VERSION, Scale, Slo, Stop, Tier,
};
use super::result::*;
use crate::support::capacity::result::{Environment, Percentiles, Verdict};
use crate::support::endurance::ledger::Ledger;
use crate::support::endurance::manifest::Ending;
use crate::support::endurance::plan::{self, Planned, Tenant};
use crate::support::endurance::result::Distribution;
use crate::support::endurance::sampler::{self, Sampler};
use crate::support::upstream::FakeUpstream;

/// How often the driver drains, segments, and checks the script. Fixed rather
/// than derived from the segment length: what bounds the driver's own memory is
/// how often it lets go of what it has folded in, and that must not get slower
/// because a soak's segments got longer.
const TICK: Duration = Duration::from_millis(100);
/// How often the driver drains what the workers and the replicas have produced.
const DRAIN_EVERY: Duration = Duration::from_millis(250);
/// How often the driver asks every replica whether it is ready.
const READINESS_EVERY: Duration = Duration::from_millis(500);
/// How often the driver runs its tenant-boundary probes.
const PROBE_EVERY: Duration = Duration::from_secs(1);
/// How long a probe or convergence request may take before it counts as a
/// failure to answer.
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);
/// How long the driver waits for a replica to accept a connection. Generous
/// against a loaded runner, and finite against a replica that never will.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// How long any one request the driver makes may take. Well past the slowest
/// thing the script asks for — a stream through a gate delayed by a quarter of
/// a second — and short enough that a replica which stops answering ends the
/// request rather than the run.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
/// How long the durable table must stop growing for before it is called
/// settled.
const DURABLE_QUIET: Duration = Duration::from_secs(2);
/// Relayed events a cancelled stream waits for before hanging up.
const CANCEL_AFTER_EVENTS: usize = 2;
/// Latency samples kept. A twelve-hour run offers millions; percentiles over a
/// strided sample of them are honest, and a vector of all of them is not
/// bounded memory.
const RETAINED_SAMPLES: usize = 20_000;

/// What an operator dispatching a run may vary.
#[derive(Clone, Copy)]
pub struct Dispatch<'a> {
    /// The soak tier's duration override, read from the environment by the
    /// caller so a test can prove the tiering without setting a variable.
    pub duration_ms: Option<&'a str>,
    /// The artifact's file stem. The profile's id rather than the tier's name,
    /// which is the directory: two profiles run one after another must not
    /// write over each other's artifact, series, keys and ledger.
    pub stem: &'a str,
}

pub async fn run(
    profile: &Profile,
    tier: Tier,
    manifest_text: &str,
) -> Option<StatefulEnduranceResult> {
    let duration = std::env::var(DURATION_ENV).ok();
    run_with(
        profile,
        tier,
        manifest_text,
        Dispatch {
            duration_ms: duration.as_deref(),
            stem: &profile.id,
        },
    )
    .await
}

/// Run one tier. `None` when no PostgreSQL is configured: a stateful
/// qualification without a datastore is not a shorter one, so it is skipped
/// rather than degraded — and [`durable::dsn`] turns the skip into a panic
/// where CI requires the services.
pub async fn run_with(
    profile: &Profile,
    tier: Tier,
    manifest_text: &str,
    dispatch: Dispatch<'_>,
) -> Option<StatefulEnduranceResult> {
    let _offering = load_lock().lock().await;
    let dsn = durable::dsn()?;
    let slo = profile.slo(tier);
    let mut scale = *profile.scale(tier);
    let (duration, duration_source) = requested(tier, &scale, dispatch.duration_ms);
    // Both fields move together: an echo carrying the manifest's twelve hours
    // beside a segment length sized for one describes a run that never happened.
    scale.segment_ms = segment_ms(&scale, duration, slo.min_segments);
    scale.duration_ms = duration.as_millis() as u64;

    let upstream = FakeUpstream::start().await;
    let upstream_gate = Gate::start(authority(&upstream.base_url)).await;
    let durable = Durable::create(&dsn, dispatch.stem).await;
    let usage_gate = Gate::start(&durable.backend_authority()).await;
    let (replica_dsn, reach) = durable.replica_dsn(&usage_gate.authority());

    let deployment = Deployment {
        upstream_base_url: upstream_gate.base_url(),
        key_dir: fleet::key_dir(tier.as_str(), dispatch.stem),
        usage_table: durable.qualified_table.clone(),
    };
    let tenants = deployment.tenants();
    let probe_tenant = deployment.probe_tenant();
    let mut fleet = Fleet::start(deployment, replica_dsn, slo.replicas).await;

    let environment = {
        let replica = &fleet.replicas[0];
        let portable = fleet.deployment.portable(&replica.process.config);
        Environment::collect(
            &portable,
            replica.process.bind(),
            &upstream_gate.base_url(),
            super::manifest::MANIFEST_RELATIVE,
            manifest_text,
        )
    };

    let dir = fleet::artifact_dir(tier.as_str());
    // Which faults this run can actually cause, decided once and before anything
    // is measured: a database reached directly is never taken away, and the
    // stretch of the run the script set aside for its outage must go on being
    // judged like any other rather than excusing whatever happens in it.
    let injected = match reach {
        Reach::Gated => Injected::EveryDeclaredFault,
        Reach::Direct => Injected::UpstreamFaultsOnly,
    };
    let mut state = State::new(
        &dir,
        dispatch.stem,
        scale,
        profile.schedule,
        duration,
        injected,
        &tenants,
    );

    // One client for the workers and the driver alike, as the stateless drivers
    // do: a pool per worker would put the driver's own descriptors and sockets
    // into the picture the fleet's resources are read from. Both timeouts are
    // the run's own: a replica that accepts a connection and then answers
    // nothing is a finding this harness should measure and end on, not a
    // request that hangs the tick, the drain and the script behind it until
    // libtest gives up on the whole binary.
    let client = Arc::new(
        reqwest::Client::builder()
            .pool_max_idle_per_host(scale.concurrency.max(1))
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .build()
            .expect("the driver's HTTP client builds"),
    );
    let rotation = Arc::new(std::sync::Mutex::new(fleet.rotation()));
    let endings = Arc::new(plan::rotation(&profile.mix, profile.seed));
    let next = Arc::new(AtomicUsize::new(0));
    let stop_flag = Arc::new(AtomicBool::new(false));
    let (tx, mut rx) = unbounded_channel();

    let sample_interval = Duration::from_millis(scale.sample_interval_ms.max(1));
    let booted: Vec<(String, u32)> = fleet
        .replicas
        .iter()
        .map(|replica| (replica.id.clone(), replica.process.pid()))
        .collect();
    for (id, pid) in booted {
        state.start_sampler(&id, pid, sample_interval, &dir, dispatch.stem);
    }
    let backend_version = durable.backend_version().await;

    let started_at = SystemTime::now();
    let started = Instant::now();
    let deadline = Deadline::new(started, duration);
    let mut workers = Vec::with_capacity(scale.concurrency);
    for _ in 0..scale.concurrency {
        workers.push(tokio::spawn(worker(
            client.clone(),
            rotation.clone(),
            tenants.clone(),
            endings.clone(),
            next.clone(),
            tx.clone(),
            stop_flag.clone(),
            deadline.clone(),
            Duration::from_millis(scale.think_time_ms),
            started,
        )));
    }
    drop(tx);

    let stop = Supervisor {
        profile,
        slo,
        duration,
        started,
        deadline: deadline.clone(),
        fleet: &mut fleet,
        upstream_gate: &upstream_gate,
        usage_gate: &usage_gate,
        reach,
        client: &client,
        rotation: &rotation,
        offered: &next,
        state: &mut state,
        probe: &probe_tenant,
        sample_interval,
        dir: &dir,
        stem: dispatch.stem,
        revision: Revision::default(),
        policy_withdrawn: false,
    }
    .run(&mut rx)
    .await;

    stop_flag.store(true, Ordering::SeqCst);
    for worker in workers {
        let _ = worker.await;
    }
    // Whatever the workers sent between the last drain and their last request.
    while let Ok(attempt) = rx.try_recv() {
        state.absorb_attempt(&attempt);
    }
    let total_offered = next.load(Ordering::Relaxed) as u64;
    let elapsed = started.elapsed();
    // Flush every observation before settlement and grading. The load's tail
    // is its own segment, and a run can end while every replica is still
    // unready; neither may disappear when the driver stops sampling it.
    state.finalize(elapsed);

    let settle = Duration::from_millis(profile.termination.settle_ms);
    state.settle(&mut fleet, settle, started).await;
    state.abandon_pending_revisions();
    let settled = durable.await_settled(settle, DURABLE_QUIET).await;

    let durable_counts = durable.counts().await;
    // The outage's half of the loss only exists where the outage was applied.
    // A directly reached database was never taken away, so every row it holds
    // was settled at a moment nothing excuses and the comparison is made over
    // the whole run.
    let durable_outside = match injected {
        Injected::EveryDeclaredFault => {
            let (usage_from, usage_to) = profile.schedule.usage_outage_window(duration);
            durable
                .distinct_outside(started_at + usage_from, started_at + usage_to)
                .await
        }
        Injected::UpstreamFaultsOnly => durable_counts.distinct,
    };

    // The samplers are read for the last time before the processes stop: a
    // settled reading is what the process *kept*, and a dead process keeps
    // nothing.
    let live: Vec<String> = fleet.replicas.iter().map(|r| r.id.clone()).collect();
    for id in live {
        state.finish_sampler(&id);
    }
    fleet.shutdown();
    durable.drop_schema().await;

    let result = assemble(
        profile,
        tier,
        scale,
        duration,
        duration_source,
        environment,
        Backends {
            usage_sink: "postgres",
            usage_backend_version: backend_version,
            usage_schema: durable.schema.clone(),
            usage_reach: reach,
            upstream: "fake upstream, in process, behind a loopback fault gate",
        },
        state,
        total_offered,
        stop,
        started_at,
        elapsed,
        settle,
        DurableEvidence {
            counts: durable_counts,
            distinct_outside_window: durable_outside,
            settled,
        },
        &slo,
    );
    let path = result.write(dispatch.stem);
    eprintln!("{}\nartifact: {}", result.summary(), fleet::relative(&path));
    Some(result)
}

/// Only one tier offers load at a time, whatever the harness runs it from and
/// however many threads libtest gives it. Both tiers live in one binary beside
/// the manifest's own tests, and this lane runs the whole suite rather than
/// `--test-threads=1`: an exclusion the driver holds is true, where one the
/// invocation configures is only configured.
pub fn load_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(tokio::sync::Mutex::default)
}

/// The offered duration, and where it came from. The override is the soak
/// tier's alone: both tiers live in one binary, and a dispatched duration
/// honoured for the smoke tier would offer it twice.
pub fn requested(tier: Tier, scale: &Scale, dispatched: Option<&str>) -> (Duration, &'static str) {
    let committed = Duration::from_millis(scale.duration_ms);
    if tier != Tier::Soak {
        return (committed, "manifest");
    }
    match dispatched.and_then(|value| value.trim().parse::<u64>().ok()) {
        Some(ms) if ms > 0 => (Duration::from_millis(ms), "environment"),
        _ => (committed, "manifest"),
    }
}

/// The segment length a run of this length can actually close. A dispatched
/// short soak keeps the manifest's segment count rather than producing one
/// segment and no trend; the `+ 1` keeps the last boundary off the deadline.
pub fn segment_ms(scale: &Scale, duration: Duration, min_segments: u64) -> u64 {
    let fitting = duration.as_millis() as u64 / (min_segments + 1);
    scale.segment_ms.min(fitting.max(1))
}

/// Whether a request was in flight for any part of a declared fault. The span
/// rather than the instant it was offered: a stream that had been running for
/// two seconds when the provider was taken away failed because of the outage,
/// and charging it to the deployment because it began a moment before the
/// window opened would make the gate depend on how long requests happen to
/// take.
pub fn touched(windows: &[(Duration, Duration)], at: Duration, latency_ms: f64) -> bool {
    let ended = at + Duration::from_secs_f64((latency_ms / 1000.0).max(0.0));
    windows.iter().any(|(from, to)| ended >= *from && at < *to)
}

/// Whether a record or a drop report drained at `now` falls inside the declared
/// usage-backend outage. `None` is a run that never took the database away —
/// the harness reached it directly, so the outage was not evaluated — and a row
/// lost during a fault nobody injected is a finding rather than an excused one.
///
/// The closing edge is carried one drain interval because both records and
/// reports are stamped with the tick they were drained on rather than the
/// instant the process wrote them.
pub fn in_usage_window(window: Option<(Duration, Duration)>, now: Duration) -> bool {
    window.is_some_and(|(from, to)| now >= from && now < to + DRAIN_EVERY)
}

fn authority(base_url: &str) -> &str {
    base_url
        .strip_prefix("http://")
        .expect("a loopback base URL")
}

// ---------------------------------------------------------------------------
// The workers
// ---------------------------------------------------------------------------

/// One offered request, as the driver hears about it.
#[derive(Debug, Clone)]
struct Attempt {
    at: Duration,
    tenant: &'static str,
    ending: Ending,
    streamed: bool,
    outcome: Outcome,
    latency_ms: f64,
    ttft_ms: Option<f64>,
    /// Why an attempt ended the way it did, for the endings that are findings.
    /// A count of unexplained failures is a puzzle; a count of them by status
    /// and typed reason is a diagnosis.
    reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Completed,
    Cancelled,
    Dropped,
    Faulted,
    /// A planned fault an open circuit refused before dispatch.
    Shed,
    /// Admission refused it.
    Rejected,
    /// No replica was in rotation. What a rolling restart must not cause.
    Unavailable,
    /// Anything else.
    Unplanned,
}

impl Outcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
            Self::Dropped => "dropped",
            Self::Faulted => "faulted",
            Self::Shed => "circuit-shed",
            Self::Rejected => "rejected",
            Self::Unavailable => "unavailable",
            Self::Unplanned => "unplanned",
        }
    }

    /// Whether the request reached an upstream attempt and so owes exactly one
    /// usage record.
    fn owes_record(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Cancelled | Self::Dropped | Self::Faulted
        )
    }
}

#[allow(clippy::too_many_arguments)]
async fn worker(
    client: Arc<reqwest::Client>,
    rotation: Arc<std::sync::Mutex<Vec<String>>>,
    tenants: Vec<Tenant>,
    endings: Arc<Vec<Ending>>,
    next: Arc<AtomicUsize>,
    tx: UnboundedSender<Attempt>,
    stop: Arc<AtomicBool>,
    deadline: Deadline,
    think: Duration,
    started: Instant,
) {
    while !stop.load(Ordering::Relaxed) {
        // A rolling replacement may extend a run after its original deadline.
        // Keep workers parked rather than terminating them at the first end so
        // that the extension actually has load behind it. The supervisor owns
        // the stop flag and wakes this loop when the extended run is complete.
        if deadline.passed() {
            tokio::time::sleep(TICK).await;
            continue;
        }
        let index = next.fetch_add(1, Ordering::Relaxed);
        let planned = plan::planned(index, &tenants, &endings);
        let target = {
            let live = rotation.lock().expect("the rotation lock");
            if live.is_empty() {
                None
            } else {
                Some(live[index % live.len()].clone())
            }
        };
        let attempt = match target {
            Some(base_url) => offer(&client, &base_url, &planned, started).await,
            None => Attempt {
                at: started.elapsed(),
                tenant: planned.tenant.namespace,
                ending: planned.ending,
                streamed: planned.shape.stream,
                outcome: Outcome::Unavailable,
                latency_ms: 0.0,
                ttft_ms: None,
                reason: Some("no replica was in rotation".to_owned()),
            },
        };
        if tx.send(attempt).is_err() {
            return;
        }
        if !think.is_zero() {
            tokio::time::sleep(think).await;
        }
    }
}

async fn offer(
    client: &reqwest::Client,
    base_url: &str,
    plan: &Planned,
    started: Instant,
) -> Attempt {
    let sent = Instant::now();
    let at = started.elapsed();
    let finish = |outcome: Outcome, ttft: Option<f64>| Attempt {
        at,
        tenant: plan.tenant.namespace,
        ending: plan.ending,
        streamed: plan.shape.stream,
        outcome,
        latency_ms: sent.elapsed().as_secs_f64() * 1000.0,
        ttft_ms: ttft,
        reason: None,
    };
    let because = |outcome: Outcome, reason: String| Attempt {
        reason: Some(reason),
        ..finish(outcome, None)
    };

    let response = client
        .post(format!("{base_url}{}", plan.shape.route))
        .bearer_auth(&plan.tenant.key)
        .json(&body(plan))
        .send()
        .await;
    let response = match response {
        Ok(response) => response,
        // A refused or torn connection. Whether that is the point or a finding
        // depends on the fault windows, which the driver — not the worker —
        // knows about.
        Err(error) => return because(Outcome::Unplanned, transport_reason(&error)),
    };
    let status = response.status();
    if !status.is_success() {
        let code = status.as_u16();
        let refusal = response.text().await.unwrap_or_default();
        let outcome = match plan.ending {
            Ending::Faulted if code == 502 => Outcome::Faulted,
            Ending::Faulted
                if code == 503
                    && error_type(&refusal).as_deref() == Some("all_provider_circuits_open") =>
            {
                Outcome::Shed
            }
            _ if code == 429 || code == 503 => Outcome::Rejected,
            _ => Outcome::Unplanned,
        };
        return because(
            outcome,
            format!(
                "{} {code} {}",
                plan.shape.alias,
                error_type(&refusal).unwrap_or_else(|| "untyped".to_owned())
            ),
        );
    }

    if !plan.shape.stream {
        let read = response.bytes().await;
        return match (plan.ending, read) {
            (Ending::Complete, Ok(_)) => Attempt {
                ..finish(
                    Outcome::Completed,
                    Some(sent.elapsed().as_secs_f64() * 1000.0),
                )
            },
            (ending, Ok(_)) => because(
                Outcome::Unplanned,
                format!("a buffered {} request succeeded", ending.as_str()),
            ),
            (_, Err(error)) => because(Outcome::Unplanned, transport_reason(&error)),
        };
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
        first_byte.get_or_insert_with(|| sent.elapsed());
        if plan.ending != Ending::Cancelled {
            continue;
        }
        relayed.push_str(&String::from_utf8_lossy(&chunk));
        if crate::support::capacity::run::output_events(&relayed) >= CANCEL_AFTER_EVENTS {
            cancelled = true;
            break;
        }
    }
    // Dropping the body without draining it is what a closed tab looks like,
    // and it is the case that leaks an upstream when it is mishandled.
    drop(stream);
    let ttft = first_byte.map(|ttft| ttft.as_secs_f64() * 1000.0);
    match plan.ending {
        Ending::Complete if !torn => finish(Outcome::Completed, ttft),
        Ending::Cancelled if cancelled => finish(Outcome::Cancelled, ttft),
        Ending::Dropped => finish(Outcome::Dropped, ttft),
        Ending::Faulted if torn => finish(Outcome::Faulted, ttft),
        ending => Attempt {
            reason: Some(format!(
                "a streamed {} request ended {}after {} relayed bytes",
                ending.as_str(),
                if torn { "torn " } else { "" },
                relayed.len()
            )),
            ..finish(Outcome::Unplanned, ttft)
        },
    }
}

fn body(plan: &Planned) -> Value {
    let (alias, stream) = (plan.shape.alias, plan.shape.stream);
    match plan.shape.route {
        plan::EMBEDDINGS => json!({ "model": alias, "input": "stateful-endurance" }),
        plan::RESPONSES => {
            json!({ "model": alias, "stream": stream, "input": "stateful-endurance" })
        }
        plan::MESSAGES => json!({
            "model": alias,
            "stream": stream,
            "max_tokens": 1024,
            "messages": [{ "role": "user", "content": "stateful-endurance" }],
        }),
        _ => json!({
            "model": alias,
            "stream": stream,
            "messages": [{ "role": "user", "content": "stateful-endurance" }],
        }),
    }
}

/// Why a request never got an answer, in the few words that distinguish a
/// refused connection from a timeout or a torn one.
fn transport_reason(error: &reqwest::Error) -> String {
    if error.is_connect() {
        "the connection was refused".to_owned()
    } else if error.is_timeout() {
        "the request timed out".to_owned()
    } else if error.is_body() || error.is_decode() {
        "the response body was torn".to_owned()
    } else {
        "the request did not complete".to_owned()
    }
}

/// The `error.type` of a typed refusal. A refusal is identified by what it says
/// rather than by its status alone.
fn error_type(body: &str) -> Option<String> {
    let parsed: Value = serde_json::from_str(body).ok()?;
    Some(parsed["error"]["type"].as_str()?.to_owned())
}

/// Whether a usage record was settled by the driver's own probes rather than
/// by the workload. The boundary probe is the only caller in the probe
/// namespace and the convergence probe the only caller of the catalogue
/// revision's alias, neither of which the workload ever offers, so a record
/// carrying either was asked for by the harness.
pub fn issued_by_the_driver(record: &Value) -> bool {
    record["namespace"].as_str() == Some(fleet::PROBE)
        || record["model"].as_str() == Some(fleet::CATALOGUE_ALIAS)
}

/// Classify a gateway settlement against the committed workload plan.
///
/// `rejected` is a legitimate usage-schema settlement for a refusal. The
/// stateful plan deliberately opens refusal paths during its declared fault
/// window, and a gateway may record one of those refusals even though the
/// current driver usually sees no usage row for it. It is therefore a planned
/// refusal, not an unknown success. The response-side outcome gate still
/// catches an admission refusal outside the declared window as an unplanned
/// error. Anything absent or not produced by a planned ending remains unknown
/// and fails the qualification gate.
fn classify_usage_status(status: Option<&str>) -> Option<&'static str> {
    let status = status?;
    if status == "rejected" {
        return Some("rejected");
    }
    Ending::ALL
        .iter()
        .find_map(|ending| ending.settles(status).then_some(ending.as_str()))
}

fn fingerprint(id: &str) -> u64 {
    // FNV-1a: cheap, stable, and only ever used to tell one request id from
    // another inside one run.
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in id.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

// ---------------------------------------------------------------------------
// The supervising loop
// ---------------------------------------------------------------------------

/// Drives the script, watches the fleet, and decides when the run stops.
struct Supervisor<'a> {
    profile: &'a Profile,
    slo: Slo,
    duration: Duration,
    started: Instant,
    deadline: Deadline,
    fleet: &'a mut Fleet,
    upstream_gate: &'a Gate,
    usage_gate: &'a Gate,
    reach: Reach,
    client: &'a reqwest::Client,
    rotation: &'a Arc<std::sync::Mutex<Vec<String>>>,
    /// The workers' own live dispatch counter. The state tally is drained from
    /// a channel and can lag a restart by the whole restart.
    offered: &'a AtomicUsize,
    state: &'a mut State,
    probe: &'a Tenant,
    sample_interval: Duration,
    dir: &'a Path,
    stem: &'a str,
    revision: Revision,
    /// Whether the policy revision has been *observed* to have taken effect on
    /// every replica, rather than merely published. A probe served before that
    /// is a fleet that has not finished reloading, which the convergence bound
    /// judges; only a probe served after it is a tenant reaching into another
    /// tenant's pool.
    policy_withdrawn: bool,
}

impl Supervisor<'_> {
    async fn run(&mut self, rx: &mut UnboundedReceiver<Attempt>) -> Stop {
        let script = self.profile.schedule.resolve(self.duration);
        let mut due = script.into_iter().peekable();
        let mut last_drain = Instant::now();
        let mut last_readiness = Instant::now();
        let mut last_probe = Instant::now();

        while !self.deadline.passed() {
            tokio::time::sleep(TICK).await;
            let now = self.started.elapsed();

            if last_drain.elapsed() >= DRAIN_EVERY {
                last_drain = Instant::now();
                self.drain(rx, now);
            }
            if last_readiness.elapsed() >= READINESS_EVERY {
                last_readiness = Instant::now();
                self.readiness(now).await;
            }
            if last_probe.elapsed() >= PROBE_EVERY {
                last_probe = Instant::now();
                self.boundary_probes(now).await;
            }
            self.state.maybe_close_segment(now);

            while due.peek().is_some_and(|scheduled| scheduled.at <= now) {
                let scheduled = due.next().expect("the peeked event");
                self.event(scheduled.event, now).await;
            }

            if let Some(stop) = self.abort(now).await {
                return stop;
            }
        }
        Stop::DurationElapsed
    }

    /// Take everything the workers and the replicas have produced since the
    /// last tick. Continuously, so what the driver holds is bounded by the
    /// drain interval rather than by the run's length.
    fn drain(&mut self, rx: &mut UnboundedReceiver<Attempt>, now: Duration) {
        // Before the records of this tick are absorbed: every record drained
        // here is stamped with `now`, so a silence measured afterwards would
        // always be zero however long the fleet had been quiet.
        self.state.observe_usage_silence(now);
        while let Ok(attempt) = rx.try_recv() {
            self.state.absorb_attempt(&attempt);
        }
        for record in self.fleet.drain_usage_records() {
            self.state.absorb_record(&record, now);
        }
        for report in self.fleet.drain_usage_drops() {
            self.state.absorb_drop(&report, now);
        }
        for replica in &self.fleet.replicas {
            self.state.absorb_samples(&replica.id);
        }
    }

    /// Ask every live replica whether it is ready, and keep the longest stretch
    /// in which none of them was.
    async fn readiness(&mut self, now: Duration) {
        let mut any = false;
        for base_url in self.fleet.rotation() {
            self.state.telemetry.readiness_probes += 1;
            if fleet::ready(self.client, &base_url).await {
                any = true;
            } else {
                self.state.telemetry.readiness_failures += 1;
            }
        }
        self.state.observe_readiness(any, now);
    }

    /// The probes that decide whether a tenant ever reached past its own
    /// boundary. Cheap, and run for the whole length of the run rather than
    /// once at the start: an isolation property that only holds before the
    /// first revision is not an isolation property. Every replica is asked,
    /// because a boundary one replica still honours is not a boundary.
    async fn boundary_probes(&mut self, now: Duration) {
        let key = self.probe.key.clone();
        for base_url in self.rotation() {
            let served = self.probe_serves(&base_url, &key).await;
            self.state.observe_probe(served, self.policy_withdrawn, now);
        }
    }

    fn rotation(&self) -> Vec<String> {
        self.rotation.lock().expect("the rotation lock").clone()
    }

    /// Whether the probe tenant is being served by this replica. `Some(true)`
    /// is a served request, `Some(false)` a typed `no_credential` refusal, and
    /// `None` an answer this run cannot interpret — which is neither evidence
    /// of isolation nor of its absence.
    async fn probe_serves(&self, base_url: &str, key: &str) -> Option<bool> {
        let response = self
            .client
            .post(format!("{base_url}{}", plan::CHAT))
            .bearer_auth(key)
            .timeout(PROBE_TIMEOUT)
            .json(&json!({
                "model": crate::support::gateway::alias::CHAT,
                "messages": [{ "role": "user", "content": "boundary probe" }],
            }))
            .send()
            .await
            .ok()?;
        if response.status().is_success() {
            return Some(true);
        }
        let code = response.status().as_u16();
        let body = response.text().await.unwrap_or_default();
        match error_type(&body).as_deref() {
            Some("no_credential") => Some(false),
            // A refusal for another reason — shedding, rate limiting — says
            // nothing about the pool the caller would have reached.
            _ if code == 429 || code == 503 => None,
            _ => None,
        }
    }

    async fn event(&mut self, event: Event, now: Duration) {
        match event {
            Event::CatalogueRevision => {
                self.revision.catalogue = true;
                self.fleet.publish(self.revision);
                self.state.note(now, event.as_str(), &self.revision.label());
                let converged = self.await_alias(fleet::CATALOGUE_ALIAS).await;
                self.state.observe_revision(
                    event,
                    self.revision,
                    now,
                    converged,
                    format!("the `{}` alias began serving", fleet::CATALOGUE_ALIAS),
                );
            }
            Event::CredentialRevision => {
                self.revision.credential = true;
                self.fleet.publish(self.revision);
                self.state.note(now, event.as_str(), &self.revision.label());
                // Convergence is observed asynchronously: what proves the new
                // pool is serving is a usage record attributed to it, and that
                // arrives with the accounting rather than with the response.
                self.state.await_credential(self.revision, now);
            }
            Event::PolicyRevision => {
                self.revision.policy = true;
                self.fleet.publish(self.revision);
                self.state.note(now, event.as_str(), &self.revision.label());
                let converged = self.await_refusal().await;
                // Only once every replica has been seen to refuse: an
                // unconverged revision is a convergence failure, and calling
                // it an isolation breach would abandon the run on the wrong
                // finding.
                self.policy_withdrawn = converged.is_some();
                self.state.observe_revision(
                    event,
                    self.revision,
                    now,
                    converged,
                    format!(
                        "the `{}` namespace stopped borrowing the platform pool",
                        fleet::PROBE
                    ),
                );
            }
            Event::UpstreamLatencyBegins => {
                self.upstream_gate
                    .set(Mode::Latency(self.profile.schedule.upstream_latency_ms));
                self.state
                    .open_fault(event, now, self.upstream_gate.counts());
            }
            Event::UpstreamLatencyEnds => {
                self.upstream_gate.set(Mode::Pass);
                self.state.close_fault(
                    Event::UpstreamLatencyBegins,
                    now,
                    self.upstream_gate.counts(),
                );
            }
            Event::UpstreamOutageBegins => {
                self.upstream_gate.set(Mode::Outage);
                self.state
                    .open_fault(event, now, self.upstream_gate.counts());
            }
            Event::UpstreamOutageEnds => {
                self.upstream_gate.set(Mode::Pass);
                self.state.close_fault(
                    Event::UpstreamOutageBegins,
                    now,
                    self.upstream_gate.counts(),
                );
            }
            Event::UsageBackendOutageBegins => {
                if self.reach == Reach::Gated {
                    self.usage_gate.set(Mode::Outage);
                    self.state.open_fault(event, now, self.usage_gate.counts());
                } else {
                    self.state.note(
                        now,
                        event.as_str(),
                        "not evaluated: the configured database is not loopback, so the \
                         harness leaves its remote DSN untouched",
                    );
                }
            }
            Event::UsageBackendOutageEnds => {
                if self.reach == Reach::Gated {
                    self.usage_gate.set(Mode::Pass);
                    self.state.close_fault(
                        Event::UsageBackendOutageBegins,
                        now,
                        self.usage_gate.counts(),
                    );
                }
            }
            Event::RollingRestart => self.rolling_restart(now).await,
        }
    }

    /// Poll until every replica in rotation serves an alias, or the
    /// convergence bound passes. Every replica, because a revision one of two
    /// replicas picked up is a revision the deployment did not converge on.
    async fn await_alias(&mut self, alias: &str) -> Option<Duration> {
        let began = Instant::now();
        let bound = Duration::from_millis(self.slo.max_convergence_ms);
        while began.elapsed() <= bound {
            let rotation = self.rotation();
            let mut all = !rotation.is_empty();
            for base_url in rotation {
                all &= self.serves_alias(&base_url, alias).await;
            }
            if all {
                return Some(began.elapsed());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        None
    }

    async fn serves_alias(&self, base_url: &str, alias: &str) -> bool {
        self.client
            .post(format!("{base_url}{}", plan::CHAT))
            .bearer_auth(crate::support::gateway::GATEWAY_KEY)
            .timeout(PROBE_TIMEOUT)
            .json(&json!({
                "model": alias,
                "messages": [{ "role": "user", "content": "convergence probe" }],
            }))
            .send()
            .await
            .is_ok_and(|response| response.status().is_success())
    }

    /// Poll until every replica in rotation refuses the probe tenant, or the
    /// convergence bound passes.
    async fn await_refusal(&mut self) -> Option<Duration> {
        let began = Instant::now();
        let bound = Duration::from_millis(self.slo.max_convergence_ms);
        let key = self.probe.key.clone();
        while began.elapsed() <= bound {
            let rotation = self.rotation();
            let mut all = !rotation.is_empty();
            for base_url in rotation {
                all &= self.probe_serves(&base_url, &key).await == Some(false);
            }
            if all {
                return Some(began.elapsed());
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        None
    }

    /// Replace every replica, one at a time, never leaving the rotation empty.
    async fn rolling_restart(&mut self, now: Duration) {
        self.state
            .note(now, Event::RollingRestart.as_str(), "beginning");
        let ids: Vec<String> = self.fleet.replicas.iter().map(|r| r.id.clone()).collect();
        for id in ids {
            let began = Instant::now();
            // Out of rotation before the signal, which is the whole difference
            // between a rolling restart and an outage.
            self.take_out_of_rotation(&id);
            let retired = self
                .fleet
                .retire(&id, Duration::from_millis(RETIRE_BOUND_MS))
                .await;
            self.state.finish_sampler(&id);
            self.state.observe_retirement(&retired, now);

            let replacement = self.fleet.admit(self.revision).await;
            let pid = self
                .fleet
                .replicas
                .last()
                .expect("the replacement is in the fleet")
                .process
                .pid();
            self.state
                .start_sampler(&replacement, pid, self.sample_interval, self.dir, self.stem);
            let base_url = self
                .fleet
                .replicas
                .last()
                .expect("the replacement is in the fleet")
                .base_url()
                .to_owned();
            let ready = fleet::await_ready(
                self.client,
                &base_url,
                Duration::from_millis(self.profile.termination.abort_after_unready_ms),
            )
            .await;
            self.publish_rotation();
            self.state.observe_replacement(
                &id,
                &replacement,
                began.elapsed(),
                ready,
                self.started.elapsed(),
                self.offered.load(Ordering::Relaxed) as u64,
            );
            if !ready {
                return;
            }
        }
        // A restart is only measured by the load that follows it, and on a short
        // tier the restart can finish close enough to the end that almost none
        // does. The run offers for a little longer rather than publishing an
        // artifact whose restart nothing was asked of — the extension is on the
        // artifact, so a reader can see the run was stretched and by how much.
        let extended = self.deadline.keep_offering_for(POST_RESTART_LOAD);
        if !extended.is_zero() {
            self.state.restart.extended_for_load_ms = extended.as_millis() as u64;
            self.state.note(
                self.started.elapsed(),
                "run-extended",
                &format!(
                    "offering {}ms longer so the restart is measured under load",
                    extended.as_millis()
                ),
            );
        }
    }

    fn take_out_of_rotation(&mut self, id: &str) {
        if let Some(replica) = self.fleet.replicas.iter_mut().find(|r| r.id == id) {
            replica.in_rotation = false;
        }
        self.publish_rotation();
    }

    fn publish_rotation(&mut self) {
        *self.rotation.lock().expect("the rotation lock") = self.fleet.rotation();
    }

    /// The early aborts the manifest declares. A run that keeps measuring past
    /// one of these is measuring a deployment nobody would leave running.
    async fn abort(&mut self, now: Duration) -> Option<Stop> {
        let termination = self.profile.termination;
        if termination.abort_on_tenant_boundary_violation && self.state.tenancy.violations > 0 {
            return Some(Stop::TenantBoundaryViolation);
        }
        if self.state.replacement_never_ready {
            return Some(Stop::ReplicaNeverReady);
        }
        if self.state.consecutive_unplanned >= termination.abort_after_consecutive_unplanned_errors
        {
            return Some(Stop::UnplannedErrors);
        }
        if termination.abort_on_replica_exit {
            let departed = self.fleet.departed().await;
            if !departed.is_empty() {
                self.state.note(now, "replica-exited", &departed.join(", "));
                return Some(Stop::ReplicaExited);
            }
        }
        None
    }
}

/// How long a retiring replica has to exit. Its own config advertises an eight
/// second deadline, and this leaves room for the signal and the wait.
pub const RETIRE_BOUND_MS: u64 = 15_000;

/// How much load must follow the last replacement. A restart the workload
/// finished before proves nothing — `unavailable = 0` is satisfied by a
/// deployment nobody is asking for anything — and a restart that runs long
/// enough to eat the tail of a short tier would otherwise turn that missing
/// evidence into a failing assertion rather than into measurement.
pub const POST_RESTART_LOAD: Duration = Duration::from_secs(10);

/// When the run stops offering, shared by the workers and the supervising
/// loop so the two cannot disagree about it — and movable, because the
/// rolling restart has to be followed by load for the restart to have been
/// measured at all. It only ever moves later, and only by the shortfall.
#[derive(Clone)]
pub struct Deadline {
    started: Instant,
    ms: Arc<std::sync::atomic::AtomicU64>,
}

impl Deadline {
    pub fn new(started: Instant, duration: Duration) -> Self {
        Self {
            started,
            ms: Arc::new(std::sync::atomic::AtomicU64::new(
                duration.as_millis() as u64
            )),
        }
    }

    pub fn passed(&self) -> bool {
        self.started.elapsed().as_millis() as u64 >= self.ms.load(Ordering::Relaxed)
    }

    /// Move the end out so at least `tail` of offering is left, and answer with
    /// how much it moved by. Zero when the run already had the room.
    pub fn keep_offering_for(&self, tail: Duration) -> Duration {
        let wanted = self.started.elapsed().as_millis() as u64 + tail.as_millis() as u64;
        let was = self.ms.fetch_max(wanted, Ordering::Relaxed);
        Duration::from_millis(wanted.saturating_sub(was))
    }
}

// ---------------------------------------------------------------------------
// What the driver keeps
// ---------------------------------------------------------------------------

/// Everything the run accumulates, all of it bounded: counters, a strided
/// sample of latencies, one summary per closed segment, and one row per replica
/// incarnation. The request ids go to an external ledger rather than to a set
/// in memory, because a twelve-hour run settles more of them than a test
/// process should hold.
struct State {
    segment_ms: u64,
    segments: Vec<Segment>,
    open: OpenSegment,
    offered: u64,
    outcomes: BTreeMap<&'static str, u64>,
    by_tenant: BTreeMap<String, u64>,
    by_ending: BTreeMap<String, u64>,
    streamed: u64,
    buffered: u64,
    owed: u64,
    errors_in_fault_windows: u64,
    unplanned: u64,
    unplanned_by_reason: BTreeMap<String, u64>,
    consecutive_unplanned: u64,
    latency: Reservoir,
    ttft: Reservoir,
    ledger: Ledger,
    /// The records the driver's own probes settled, kept in their own ledger.
    /// Nothing counted them into `owed`, so leaving them in with the
    /// workload's would let one probe record stand in for a workload record
    /// the deployment lost.
    probe_ledger: Ledger,
    records_observed: u64,
    unexpected_statuses: u64,
    by_status: BTreeMap<String, u64>,
    /// Records observed outside the usage-backend outage window, which are the
    /// ones the durable sink is not allowed to lose.
    emitted_outside_usage_window: u64,
    emitted_in_usage_window: u64,
    sink_drops: SinkDrops,
    /// The declared usage-backend outage, or `None` when the run never applied
    /// one: a database this harness reaches directly cannot be taken away, so
    /// there is no window for a lost row to shelter in.
    usage_window: Option<(Duration, Duration)>,
    fault_windows: Vec<(Duration, Duration)>,
    /// When the last usage record arrived, absent until the first one does.
    last_record_at: Option<Duration>,
    samplers: Vec<(String, Sampler)>,
    /// Where each replica's series was written, keyed by replica and relative
    /// to the workspace: a reader following the artifact has to land on the
    /// file the verdicts were fitted through.
    sample_paths: BTreeMap<String, String>,
    live: BTreeMap<String, LiveResources>,
    resources: Vec<ReplicaResources>,
    revisions: Vec<RevisionObservation>,
    pending_credential: Option<(Revision, Duration, Instant)>,
    faults: Vec<FaultWindow>,
    open_faults: BTreeMap<&'static str, (Duration, GateCounts, u64)>,
    /// Closed fault windows whose first success after the fault was lifted has
    /// not arrived yet.
    awaiting_recovery: Vec<usize>,
    restart: Restart,
    /// The workers' live dispatch count when the last replacement joined. The
    /// final post-restart count uses the same counter domain; the state tally
    /// only advances on the drain tick and is not comparable to this snapshot.
    offered_at_last_replacement: u64,
    replacement_never_ready: bool,
    tenancy: Tenancy,
    telemetry: Telemetry,
    timeline: Vec<TimelineEntry>,
    unready_since: Option<Duration>,
    replicas_booted: usize,
}

#[derive(Default)]
struct OpenSegment {
    started_ms: u64,
    offered: u64,
    unplanned: u64,
    usage_records: u64,
    rss_kib: Option<u64>,
}

/// A live replica's running resource picture, folded from its samples as they
/// are taken so the samples themselves can be let go.
#[derive(Default)]
struct LiveResources {
    samples: u64,
    baseline_rss_kib: Option<u64>,
    peak_rss_kib: u64,
    last_rss_kib: Option<u64>,
    peak_fds: u64,
    last_fds: Option<u64>,
    peak_sockets: u64,
    last_sockets: Option<u64>,
    first_cpu_ticks: Option<u64>,
    last_cpu_ticks: Option<u64>,
}

impl LiveResources {
    fn absorb(&mut self, sample: &sampler::Sample) {
        self.samples += 1;
        self.baseline_rss_kib.get_or_insert(sample.rss_kib);
        self.peak_rss_kib = self.peak_rss_kib.max(sample.rss_kib);
        self.last_rss_kib = Some(sample.rss_kib);
        self.peak_fds = self.peak_fds.max(sample.fds);
        self.last_fds = Some(sample.fds);
        self.peak_sockets = self.peak_sockets.max(sample.sockets);
        self.last_sockets = Some(sample.sockets);
        self.first_cpu_ticks.get_or_insert(sample.cpu_ticks);
        self.last_cpu_ticks = Some(sample.cpu_ticks);
    }

    fn finish(&self, replica: &str) -> ReplicaResources {
        let cpu = match (self.first_cpu_ticks, self.last_cpu_ticks) {
            (Some(first), Some(last)) => Some(last.saturating_sub(first) as f64 / sampler::USER_HZ),
            _ => None,
        };
        ReplicaResources {
            replica: replica.to_owned(),
            sampled: self.samples > 0,
            samples: self.samples,
            baseline_rss_kib: self.baseline_rss_kib,
            peak_rss_kib: (self.samples > 0).then_some(self.peak_rss_kib),
            final_rss_kib: self.last_rss_kib,
            growth_kib: match (self.baseline_rss_kib, self.last_rss_kib) {
                (Some(baseline), Some(last)) => Some(last as i64 - baseline as i64),
                _ => None,
            },
            peak_open_fds: (self.samples > 0).then_some(self.peak_fds),
            final_open_fds: self.last_fds,
            peak_sockets: (self.samples > 0).then_some(self.peak_sockets),
            final_sockets: self.last_sockets,
            cpu_seconds: cpu,
        }
    }
}

/// A bounded, strided sample of a measurement. Halved rather than truncated
/// when it fills: keeping the first twenty thousand latencies of a twelve-hour
/// run would describe its first two minutes.
struct Reservoir {
    observed: u64,
    stride: u64,
    values: Vec<f64>,
}

impl Reservoir {
    fn new() -> Self {
        Self {
            observed: 0,
            stride: 1,
            values: Vec::new(),
        }
    }

    fn record(&mut self, value: f64) {
        self.observed += 1;
        if !self.observed.is_multiple_of(self.stride) {
            return;
        }
        self.values.push(value);
        if self.values.len() >= RETAINED_SAMPLES {
            let mut kept = Vec::with_capacity(self.values.len() / 2 + 1);
            for (index, value) in self.values.iter().enumerate() {
                if index % 2 == 0 {
                    kept.push(*value);
                }
            }
            self.values = kept;
            self.stride *= 2;
        }
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

impl State {
    fn new(
        dir: &Path,
        stem: &str,
        scale: Scale,
        schedule: super::manifest::Schedule,
        duration: Duration,
        injected: Injected,
        _tenants: &[Tenant],
    ) -> Self {
        Self {
            segment_ms: scale.segment_ms.max(1),
            segments: Vec::new(),
            open: OpenSegment::default(),
            offered: 0,
            outcomes: BTreeMap::new(),
            by_tenant: BTreeMap::new(),
            by_ending: BTreeMap::new(),
            streamed: 0,
            buffered: 0,
            owed: 0,
            errors_in_fault_windows: 0,
            unplanned: 0,
            unplanned_by_reason: BTreeMap::new(),
            consecutive_unplanned: 0,
            latency: Reservoir::new(),
            ttft: Reservoir::new(),
            ledger: Ledger::create(&dir.join(format!("{stem}-fingerprints"))),
            probe_ledger: Ledger::create(&dir.join(format!("{stem}-probe-fingerprints"))),
            records_observed: 0,
            unexpected_statuses: 0,
            by_status: BTreeMap::new(),
            emitted_outside_usage_window: 0,
            emitted_in_usage_window: 0,
            sink_drops: SinkDrops::default(),
            // Both come from the same decision, so an outage that is not
            // injected cannot go on excusing errors and silence through the
            // attribution windows while the row accounting has stopped
            // excusing anything.
            usage_window: match injected {
                Injected::EveryDeclaredFault => Some(schedule.usage_outage_window(duration)),
                Injected::UpstreamFaultsOnly => None,
            },
            fault_windows: schedule.attribution_windows_of(duration, injected),
            last_record_at: None,
            samplers: Vec::new(),
            sample_paths: BTreeMap::new(),
            live: BTreeMap::new(),
            resources: Vec::new(),
            revisions: Vec::new(),
            pending_credential: None,
            faults: Vec::new(),
            open_faults: BTreeMap::new(),
            awaiting_recovery: Vec::new(),
            restart: Restart {
                replicas_restarted: 0,
                unavailable: 0,
                worst_return_ms: 0,
                all_exits_bounded: true,
                all_exits_clean: true,
                flushed_on_exit: 0,
                offered_after_last_replacement: 0,
                extended_for_load_ms: 0,
            },
            offered_at_last_replacement: 0,
            replacement_never_ready: false,
            tenancy: Tenancy {
                probes: 0,
                violations: 0,
                examples: Vec::new(),
                probe_served_before_policy: 0,
                probe_refused_after_policy: 0,
                probe_served_after_policy: 0,
                misattributed_records: 0,
            },
            telemetry: Telemetry {
                readiness_probes: 0,
                readiness_failures: 0,
                worst_readiness_gap_ms: 0,
                worst_usage_silence_ms: 0,
                otlp_export_evaluated: false,
            },
            timeline: Vec::new(),
            unready_since: None,
            replicas_booted: 0,
        }
    }

    fn touched_fault_window(&self, at: Duration, latency_ms: f64) -> bool {
        touched(&self.fault_windows, at, latency_ms)
    }

    fn start_sampler(&mut self, id: &str, pid: u32, interval: Duration, dir: &Path, stem: &str) {
        let path = dir.join(format!("{stem}.{id}.samples.jsonl"));
        self.sample_paths
            .insert(id.to_owned(), fleet::relative(&path));
        self.samplers
            .push((id.to_owned(), Sampler::start(pid, interval, &path)));
        self.live.insert(id.to_owned(), LiveResources::default());
        self.replicas_booted += 1;
    }

    /// Fold a replica's samples in and let them go. The file keeps the series;
    /// the driver keeps the summary.
    fn absorb_samples(&mut self, id: &str) {
        let Some((_, sampler)) = self.samplers.iter().find(|(name, _)| name == id) else {
            return;
        };
        let taken = sampler.drain();
        let Some(live) = self.live.get_mut(id) else {
            return;
        };
        for sample in &taken {
            live.absorb(sample);
        }
        if let Some(last) = taken.last() {
            self.open.rss_kib = Some(
                self.live
                    .values()
                    .filter_map(|live| live.last_rss_kib)
                    .sum::<u64>()
                    .max(last.rss_kib),
            );
        }
    }

    /// Stop sampling a replica that is going away, and keep its row.
    fn finish_sampler(&mut self, id: &str) {
        self.absorb_samples(id);
        let Some(index) = self.samplers.iter().position(|(name, _)| name == id) else {
            return;
        };
        let (name, sampler) = self.samplers.remove(index);
        let finished = sampler.finish();
        let mut live = self.live.remove(&name).unwrap_or_default();
        for sample in &finished.pending {
            live.absorb(sample);
        }
        if let Some(settled) = finished.settled {
            live.absorb(&settled);
        }
        self.resources.push(live.finish(&name));
    }

    fn absorb_attempt(&mut self, attempt: &Attempt) {
        self.offered += 1;
        self.open.offered += 1;
        *self.outcomes.entry(attempt.outcome.as_str()).or_default() += 1;
        *self.by_tenant.entry(attempt.tenant.to_owned()).or_default() += 1;
        *self
            .by_ending
            .entry(attempt.ending.as_str().to_owned())
            .or_default() += 1;
        if attempt.streamed {
            self.streamed += 1;
        } else {
            self.buffered += 1;
        }
        if attempt.outcome.owes_record() {
            self.owed += 1;
        }
        self.latency.record(attempt.latency_ms);
        if let Some(ttft) = attempt.ttft_ms {
            self.ttft.record(ttft);
        }
        let failed = matches!(attempt.outcome, Outcome::Unplanned | Outcome::Rejected);
        if failed && self.touched_fault_window(attempt.at, attempt.latency_ms) {
            // Inside a declared window the error is the point of the window.
            self.errors_in_fault_windows += 1;
            self.consecutive_unplanned = 0;
        } else if failed {
            self.unplanned += 1;
            self.open.unplanned += 1;
            self.consecutive_unplanned += 1;
            if let Some(reason) = &attempt.reason {
                *self.unplanned_by_reason.entry(reason.clone()).or_default() += 1;
            }
        } else {
            self.consecutive_unplanned = 0;
        }
        // The first request served after a fault is lifted is what says the
        // deployment recovered. Not the gate reopening, which only says the
        // harness stopped interfering.
        if attempt.outcome.owes_record() {
            self.awaiting_recovery.retain(|&index| {
                let window = &mut self.faults[index];
                let closed = Duration::from_millis(window.closed_ms);
                if attempt.at < closed {
                    return true;
                }
                window.recovered_ms = Some((attempt.at - closed).as_millis() as u64);
                false
            });
        }
        if attempt.outcome == Outcome::Unavailable {
            self.restart.unavailable += 1;
        }
    }

    fn absorb_record(&mut self, record: &Value, now: Duration) {
        self.records_observed += 1;
        self.open.usage_records += 1;
        self.last_record_at = Some(now);
        let probe = issued_by_the_driver(record);
        match record["request_id"].as_str() {
            Some(id) if probe => self.probe_ledger.record(fingerprint(id)),
            Some(id) => self.ledger.record(fingerprint(id)),
            None => self.unexpected_statuses += 1,
        }
        let raw_status = record["status"].as_str();
        if classify_usage_status(raw_status).is_none() {
            self.unexpected_statuses += 1;
        }
        // Keep a visible bucket for malformed rows while never treating the
        // fallback as a successful settlement. The classifier above is the
        // qualification gate; this value is only diagnostic output.
        let status = raw_status.unwrap_or("unknown").to_owned();
        *self.by_status.entry(status).or_default() += 1;

        // Which side of the outage a record was settled on, as the database
        // will see it. The driver stamps a record with the tick it drained it
        // on, which is up to one drain interval after the process wrote it, so
        // the closing edge is carried that far forward: a record the database
        // records inside the window must not be counted as one this side owed
        // outside it.
        if in_usage_window(self.usage_window, now) {
            self.emitted_in_usage_window += 1;
        } else {
            self.emitted_outside_usage_window += 1;
        }

        let namespace = record["namespace"].as_str().unwrap_or("unknown");
        let credential = record["credential_id"].as_str().unwrap_or("unknown");
        if !attribution_holds(namespace, credential) {
            self.tenancy.misattributed_records += 1;
            self.tenancy.violations += 1;
            if self.tenancy.examples.len() < 8 {
                self.tenancy.examples.push(format!(
                    "namespace `{namespace}` settled against credential `{credential}`"
                ));
            }
        }

        // The credential revision is proven by attribution, not by a response:
        // what says the new pool is serving is a record naming it.
        if let Some((revision, published_at, began)) = self.pending_credential
            && (credential == fleet::OPENAI_ROTATED_ID || credential == fleet::ANTHROPIC_ROTATED_ID)
        {
            self.pending_credential = None;
            self.revisions.push(RevisionObservation {
                event: Event::CredentialRevision.as_str().to_owned(),
                revision: revision.label(),
                published_at_ms: published_at.as_millis() as u64,
                converged_ms: Some(began.elapsed().as_millis() as u64),
                observed: format!("a usage record was attributed to `{credential}`"),
            });
        }
    }

    /// Fold in one of the fleet's reports of a usage batch it dropped. The
    /// report is what attributes a missing durable row: the process names the
    /// sink, the reason, and the count, and the driver only has to say whether
    /// it happened while the backend was declared out.
    fn absorb_drop(&mut self, report: &Value, now: Duration) {
        // Normalised by the collector: whichever field the process wrote its
        // report as, `records` is what that report lost.
        let records = report["records"].as_u64().unwrap_or(1);
        let sink = report["sink"].as_str().unwrap_or("unknown").to_owned();
        let reason = report["reason"].as_str().unwrap_or("unknown").to_owned();
        self.sink_drops.reports += 1;
        self.sink_drops.records += records;
        *self.sink_drops.by_reason.entry(reason.clone()).or_default() += records;
        // Only the durable sink's losses are the database outage's. A stdout
        // sink dropping a batch while Postgres is gone is unrelated to it. The
        // closing edge is carried one drain interval, as it is for the records
        // themselves: both are stamped with the tick they were drained on, and
        // a report read a tick late must still account for what it lost.
        if sink != "stdout" && in_usage_window(self.usage_window, now) {
            self.sink_drops.records_in_usage_window += records;
            if reason == SAMPLED_DROP_REASON {
                self.sink_drops.sampled_records_in_usage_window += records;
            }
        } else {
            self.sink_drops.records_outside_windows += records;
        }
        if self.sink_drops.examples.len() < 8 {
            self.sink_drops.examples.push(SinkDrop {
                at_ms: now.as_millis() as u64,
                sink,
                reason,
                records,
            });
        }
    }

    fn maybe_close_segment(&mut self, now: Duration) {
        let elapsed = now.as_millis() as u64;
        if elapsed.saturating_sub(self.open.started_ms) >= self.segment_ms {
            self.close_segment(now);
        }
    }

    /// Close the open segment. A segment with neither a request nor a sample is
    /// not recorded: it would fit the trend through nothing.
    fn close_segment(&mut self, now: Duration) {
        let open = std::mem::take(&mut self.open);
        let ended_ms = now.as_millis() as u64;
        if open.offered == 0 && open.rss_kib.is_none() {
            self.open.started_ms = ended_ms;
            return;
        }
        self.segments.push(Segment {
            index: self.segments.len(),
            started_ms: open.started_ms,
            ended_ms,
            offered: open.offered,
            unplanned: open.unplanned,
            usage_records: open.usage_records,
            rss_kib: open.rss_kib,
        });
        self.open.started_ms = ended_ms;
    }

    /// Flush the observations that must be complete before settlement and
    /// grading. In particular, an open readiness interval must be scored at
    /// the run's end even when no later probe observes recovery.
    fn finalize(&mut self, now: Duration) {
        self.close_segment(now);
        self.close_readiness_gap(now);
    }

    /// The longest stretch, while load was being offered, in which the fleet
    /// produced no accounting at all. A gateway that keeps answering while its
    /// usage records stop is the failure this measures, and it is invisible in
    /// a total that only says how many arrived by the end.
    ///
    /// Only stretches with no excuse are measured. A stretch overlapping a
    /// declared fault or the usage-backend outage is one the run asked for, and
    /// the stretch before the first record is the fleet starting rather than
    /// the fleet falling silent.
    fn observe_usage_silence(&mut self, now: Duration) {
        let Some(last) = self.last_record_at else {
            return;
        };
        let overlaps = |from: Duration, to: Duration| now >= from && last < to;
        if self
            .usage_window
            .is_some_and(|(from, to)| overlaps(from, to))
            || self
                .fault_windows
                .iter()
                .any(|(opened, closed)| overlaps(*opened, *closed))
        {
            return;
        }
        let silence = now.saturating_sub(last).as_millis() as u64;
        self.telemetry.worst_usage_silence_ms = self.telemetry.worst_usage_silence_ms.max(silence);
    }

    fn observe_readiness(&mut self, any_ready: bool, now: Duration) {
        match (any_ready, self.unready_since) {
            (false, None) => self.unready_since = Some(now),
            (true, Some(_)) => self.close_readiness_gap(now),
            _ => {}
        }
    }

    /// Fold a readiness interval that has ended, including one that is still
    /// open when the run reaches its deadline or aborts.
    fn close_readiness_gap(&mut self, now: Duration) {
        let Some(since) = self.unready_since.take() else {
            return;
        };
        let gap = readiness_gap_ms(since, now);
        self.telemetry.worst_readiness_gap_ms = self.telemetry.worst_readiness_gap_ms.max(gap);
    }

    fn observe_probe(&mut self, served: Option<bool>, policy_applied: bool, now: Duration) {
        self.tenancy.probes += 1;
        match (served, policy_applied) {
            (Some(true), false) => self.tenancy.probe_served_before_policy += 1,
            (Some(false), false) => {}
            (Some(true), true) => {
                self.tenancy.probe_served_after_policy += 1;
                self.tenancy.violations += 1;
                if self.tenancy.examples.len() < 8 {
                    self.tenancy.examples.push(format!(
                        "the `{}` namespace was served from the platform pool {}ms into the \
                         run, after its policy revision withdrew that permission",
                        fleet::PROBE,
                        now.as_millis()
                    ));
                }
            }
            (Some(false), true) => self.tenancy.probe_refused_after_policy += 1,
            (None, _) => {}
        }
    }

    fn note(&mut self, now: Duration, event: &str, detail: &str) {
        self.timeline.push(TimelineEntry {
            at_ms: now.as_millis() as u64,
            event: event.to_owned(),
            detail: detail.to_owned(),
        });
    }

    fn observe_revision(
        &mut self,
        event: Event,
        revision: Revision,
        now: Duration,
        converged: Option<Duration>,
        observed: String,
    ) {
        self.revisions.push(RevisionObservation {
            event: event.as_str().to_owned(),
            revision: revision.label(),
            published_at_ms: now.as_millis() as u64,
            converged_ms: converged.map(|took| took.as_millis() as u64),
            observed,
        });
    }

    fn await_credential(&mut self, revision: Revision, now: Duration) {
        self.pending_credential = Some((revision, now, Instant::now()));
    }

    /// Write down a revision that was published and never observed. Left
    /// pending it would simply be absent from the artifact, and an artifact
    /// that omits the revision that failed reads exactly like one where every
    /// revision converged.
    fn abandon_pending_revisions(&mut self) {
        if let Some((revision, published_at, _)) = self.pending_credential.take() {
            self.revisions.push(RevisionObservation {
                event: Event::CredentialRevision.as_str().to_owned(),
                revision: revision.label(),
                published_at_ms: published_at.as_millis() as u64,
                converged_ms: None,
                observed: "no usage record was ever attributed to the rotated pool".to_owned(),
            });
        }
    }

    fn open_fault(&mut self, event: Event, now: Duration, counts: GateCounts) {
        self.open_faults
            .insert(event.as_str(), (now, counts, self.errors_in_fault_windows));
        self.note(now, event.as_str(), "declared");
    }

    fn close_fault(&mut self, opened_by: Event, now: Duration, counts: GateCounts) {
        let Some((opened_ms, before, errors_before)) = self.open_faults.remove(opened_by.as_str())
        else {
            return;
        };
        self.awaiting_recovery.push(self.faults.len());
        self.faults.push(FaultWindow {
            event: opened_by.as_str().to_owned(),
            opened_ms: opened_ms.as_millis() as u64,
            closed_ms: now.as_millis() as u64,
            errors_inside: self.errors_in_fault_windows - errors_before,
            recovered_ms: None,
            gate: GateCounts {
                accepted: counts.accepted.saturating_sub(before.accepted),
                refused: counts.refused.saturating_sub(before.refused),
                cut: counts.cut.saturating_sub(before.cut),
                delayed: counts.delayed.saturating_sub(before.delayed),
            },
        });
        self.note(now, opened_by.as_str(), "lifted");
    }

    fn observe_retirement(&mut self, retired: &fleet::Retired, now: Duration) {
        self.restart.flushed_on_exit += retired.flushed;
        self.restart.all_exits_clean &= retired.clean;
        self.restart.all_exits_bounded &= retired.took.is_some();
        self.note(
            now,
            "replica-retired",
            &format!(
                "{} exited {} after {}, flushing {} records",
                retired.id,
                if retired.clean {
                    "cleanly"
                } else {
                    "uncleanly"
                },
                retired
                    .took
                    .map_or_else(|| "the bound".to_owned(), |took| format!("{took:?}")),
                retired.flushed,
            ),
        );
    }

    fn observe_replacement(
        &mut self,
        retired: &str,
        replacement: &str,
        took: Duration,
        ready: bool,
        now: Duration,
        offered: u64,
    ) {
        self.restart.replicas_restarted += 1;
        self.restart.worst_return_ms = self.restart.worst_return_ms.max(took.as_millis() as u64);
        self.offered_at_last_replacement = offered;
        if !ready {
            self.replacement_never_ready = true;
        }
        self.note(
            now,
            "replica-replaced",
            &format!(
                "{replacement} took over from {retired} in {}ms and {}",
                took.as_millis(),
                if ready {
                    "answered /readyz"
                } else {
                    "never answered /readyz"
                }
            ),
        );
    }

    /// Wait for the records the fleet still owes. Bounded by the manifest's
    /// settle window and by a quiet period: a sink that is still emitting has
    /// not finished, and a count that includes duplicates cannot say whether it
    /// has.
    async fn settle(&mut self, fleet: &mut Fleet, within: Duration, started: Instant) {
        let deadline = Instant::now() + within;
        let mut quiet_since: Option<Instant> = None;
        loop {
            let before = self.records_observed;
            for record in fleet.drain_usage_records() {
                self.absorb_record(&record, started.elapsed());
            }
            for report in fleet.drain_usage_drops() {
                self.absorb_drop(&report, started.elapsed());
            }
            if self.records_observed > before {
                quiet_since = None;
            }
            let settled = self.records_observed >= self.owed
                && quiet_since.get_or_insert_with(Instant::now).elapsed() >= DURABLE_QUIET;
            if settled || Instant::now() >= deadline {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

/// Whether a usage record's credential belongs to the namespace that settled
/// it. The BYOK namespace's records must name its own credentials, and no other
/// namespace may name them: that is what a tenant boundary is, expressed in the
/// only place the run can see it after the fact.
fn attribution_holds(namespace: &str, credential: &str) -> bool {
    let byok_credential =
        credential == fleet::BYOK_OPENAI_ID || credential == fleet::BYOK_ANTHROPIC_ID;
    if namespace == fleet::BYOK {
        return byok_credential;
    }
    !byok_credential
}

// ---------------------------------------------------------------------------
// The verdict
// ---------------------------------------------------------------------------

/// The run's durable loss, split by when the records were settled rather than
/// by how much the processes reported losing.
pub struct DurableLoss {
    /// Records the run settled that the database does not hold, over the whole
    /// run.
    pub total: u64,
    /// Records settled outside the widened usage-outage window that the
    /// database does not hold outside it. Nothing excuses these.
    pub outside: u64,
    /// The remainder, which the declared outage accounts for.
    pub in_window: u64,
    /// What the comparison outside the window was made against.
    pub settled_outside: u64,
}

/// Count dispatches after the last replacement using the same live counter for
/// both values. The state tally is intentionally drained later, so subtracting
/// it from the workers' live dispatch count would mix two points in time.
pub fn offered_after_last_replacement(total_offered: u64, at_replacement: u64) -> u64 {
    total_offered.saturating_sub(at_replacement)
}

/// Split the durable loss over the usage-backend outage.
///
/// The whole-run loss is a set difference. Which half of it the outage excuses
/// is a question about *when*, so the outside half is derived from a comparison
/// of the same window on both sides — what the processes settled outside it
/// against what the database holds outside it, by the gateway's own
/// `recorded_at` — and only the remainder is charged to the outage. A drop
/// reported during the outage can no longer excuse a row lost at a safe moment.
pub fn reconcile_durable_loss(
    settled: u64,
    emitted_outside: u64,
    duplicates: u64,
    durable_distinct: u64,
    durable_outside: u64,
) -> DurableLoss {
    let total = settled.saturating_sub(durable_distinct);
    // Every duplicate is charged to the outside bucket, because nothing says
    // which side of the window a second copy arrived on. Understating what was
    // settled outside understates the loss rather than inventing one.
    let settled_outside = emitted_outside.saturating_sub(duplicates);
    let outside = settled_outside.saturating_sub(durable_outside).min(total);
    DurableLoss {
        total,
        outside,
        in_window: total - outside,
        settled_outside,
    }
}

/// The reason whose reports the gateway samples rather than writes in full:
/// the buffer-full drop is logged at the first record and then every
/// [`DROP_LOG_SAMPLE`]th, so its reports can lag what was actually lost.
/// Everything else — a rejected batch, an abandoned buffer — is reported with
/// its exact count as it happens.
pub const SAMPLED_DROP_REASON: &str = "buffer_full";
/// The gateway's sampling interval for that report
/// (`crates/gateway/src/usage/batch.rs`). Its reports carry the sink's running
/// total, so what a run can have lost beyond the last one is the tail below the
/// next boundary.
pub const DROP_LOG_SAMPLE: u64 = 1_000;

/// How much in-window durable loss the fleet's own reports account for.
///
/// The excused half of the loss is only as large as the deployment said it
/// was. The one allowance is the sampled report's tail: a run whose in-window
/// drops were reported as buffer-full may have lost up to one sampling interval
/// more than the last report named, and nothing but the next report would say
/// so. No allowance is made when nothing was reported that way, so a run that
/// lost rows in silence is a failure whatever else it dropped.
pub fn excused_in_window(drops: &SinkDrops) -> u64 {
    let tail = if drops.sampled_records_in_usage_window > 0 {
        DROP_LOG_SAMPLE - 1
    } else {
        0
    };
    drops.records_in_usage_window + tail
}

/// What the database held once everything had settled.
struct DurableEvidence {
    counts: durable::Counts,
    distinct_outside_window: u64,
    settled: durable::Settled,
}

/// Fold the run into its artifact, and judge it against the manifest.
#[allow(clippy::too_many_arguments)]
fn assemble(
    profile: &Profile,
    tier: Tier,
    scale: Scale,
    duration: Duration,
    duration_source: &'static str,
    environment: Environment,
    backends: Backends,
    state: State,
    total_offered: u64,
    stop: Stop,
    started_at: SystemTime,
    elapsed: Duration,
    settle: Duration,
    durable: DurableEvidence,
    slo: &Slo,
) -> StatefulEnduranceResult {
    let State {
        segments,
        offered: drained_offered,
        outcomes,
        by_tenant,
        by_ending,
        streamed,
        buffered,
        owed,
        errors_in_fault_windows,
        unplanned,
        unplanned_by_reason,
        latency,
        ttft,
        ledger,
        probe_ledger,
        records_observed,
        emitted_outside_usage_window,
        unexpected_statuses,
        by_status,
        sink_drops,
        sample_paths,
        resources,
        revisions,
        faults,
        mut restart,
        offered_at_last_replacement,
        tenancy,
        telemetry,
        timeline,
        ..
    } = state;
    debug_assert_eq!(
        drained_offered, total_offered,
        "all dispatched attempts should be drained before assembly"
    );
    let offered = total_offered;
    if restart.replicas_restarted > 0 {
        restart.offered_after_last_replacement =
            offered_after_last_replacement(offered, offered_at_last_replacement);
    }
    let tally = ledger.tally();
    // The driver's probes settle records of their own. They are reconciled
    // apart from the workload's — nothing owed them — and rejoined only where
    // the database is compared, because the database holds them too.
    let probes = probe_ledger.tally();
    let settled = tally.distinct + probes.distinct;
    let duplicates = records_observed.saturating_sub(settled);
    let missing = owed.saturating_sub(tally.distinct);
    let loss = reconcile_durable_loss(
        settled,
        emitted_outside_usage_window,
        duplicates,
        durable.counts.distinct,
        durable.distinct_outside_window,
    );
    let trend = trend(&segments, slo);
    let growth = resources
        .iter()
        .filter_map(|replica| replica.growth_kib)
        .max()
        .unwrap_or_default()
        .max(0) as u64;
    let convergence_worst = revisions
        .iter()
        .map(|revision| revision.converged_ms.unwrap_or(u64::MAX))
        .max()
        .unwrap_or_default();
    let unconverged = revisions
        .iter()
        .filter(|revision| revision.converged_ms.is_none())
        .count() as u64;

    let mut verdicts = vec![
        Verdict::at_least("segments", segments.len() as f64, slo.min_segments as f64),
        Verdict::at_most(
            "unplanned_errors",
            unplanned as f64,
            slo.max_unplanned_errors as f64,
        ),
        Verdict::at_most(
            "missing_usage_records",
            missing as f64,
            slo.max_missing_usage_records as f64,
        ),
        Verdict::at_most(
            "duplicate_usage_records",
            duplicates as f64,
            slo.max_duplicate_usage_records as f64,
        ),
        Verdict::at_most("unexpected_usage_statuses", unexpected_statuses as f64, 0.0),
        Verdict::at_most(
            "durable_usage_loss_outside_windows",
            loss.outside as f64,
            slo.max_durable_usage_loss_outside_windows as f64,
        ),
        // The other half of the same reconciliation: what the outage excuses,
        // bounded by what the deployment said it lost rather than merely by the
        // fact that it said anything.
        Verdict::at_most(
            "durable_usage_loss_in_window",
            loss.in_window as f64,
            excused_in_window(&sink_drops) as f64,
        ),
        Verdict::at_most(
            "durable_usage_lag_ms",
            durable.settled.lag_ms as f64,
            slo.max_durable_usage_lag_ms as f64,
        ),
        Verdict::at_most(
            "tenant_boundary_violations",
            tenancy.violations as f64,
            slo.max_tenant_boundary_violations as f64,
        ),
        Verdict::at_most(
            "restart_unavailable_requests",
            restart.unavailable as f64,
            slo.max_restart_unavailable as f64,
        ),
        // A window that never recovered is not a slow recovery: it is a
        // deployment that is still down, and infinity is the honest figure.
        Verdict::at_most(
            "recovery_ms",
            faults
                .iter()
                .map(|window| window.recovered_ms.map_or(f64::INFINITY, |ms| ms as f64))
                .fold(0.0, f64::max),
            slo.max_recovery_ms as f64,
        ),
        Verdict::at_most(
            "readiness_gap_ms",
            telemetry.worst_readiness_gap_ms as f64,
            slo.max_readiness_gap_ms as f64,
        ),
        Verdict::at_most(
            "rss_growth_kib",
            growth as f64,
            slo.max_rss_growth_kib as f64,
        ),
        // Every published revision has to become the one serving. An
        // unconverged revision is counted separately from a slow one, because
        // "never" is not a large number of milliseconds.
        Verdict::at_most("unconverged_revisions", unconverged as f64, 0.0),
        Verdict::at_most(
            "convergence_ms",
            if unconverged > 0 {
                f64::INFINITY
            } else {
                convergence_worst as f64
            },
            slo.max_convergence_ms as f64,
        ),
        // The run has to have done what it said it would: a rolling restart
        // that never happened cannot be evidence that restarts are safe.
        Verdict::at_least(
            "replicas_restarted",
            restart.replicas_restarted as f64,
            slo.replicas as f64,
        ),
        Verdict::at_least(
            "retiring_replicas_exited_cleanly",
            f64::from(restart.all_exits_clean),
            1.0,
        ),
        Verdict::at_least(
            "retiring_replicas_exited_in_bound",
            f64::from(restart.all_exits_bounded),
            1.0,
        ),
        // An abandoned run is not a shorter passing one.
        Verdict::at_least("terminated_normally", f64::from(stop.is_normal()), 1.0),
    ];
    if let Some(bound) = slo.max_rss_drift_kib_per_hour {
        verdicts.push(Verdict::at_most(
            "rss_drift_kib_per_hour",
            trend.rss_kib_per_hour.unwrap_or_default(),
            bound,
        ));
    }

    StatefulEnduranceResult {
        schema_version: RESULT_SCHEMA_VERSION,
        profile: ProfileEcho {
            id: profile.id.clone(),
            description: profile.description.clone(),
            tier: tier.as_str().to_owned(),
            seed: profile.seed,
            duration_ms: duration.as_millis() as u64,
            manifest_duration_ms: profile.scale(tier).duration_ms,
            concurrency: scale.concurrency,
            think_time_ms: scale.think_time_ms,
            sample_interval_ms: scale.sample_interval_ms,
            segment_ms: scale.segment_ms,
            mix: mix(profile),
            schedule: profile.schedule,
            slo: *slo,
            termination: profile.termination,
        },
        run: RunMeta {
            started_at_unix_ms: started_at
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|since| since.as_millis() as u64)
                .unwrap_or_default(),
            elapsed_ms: elapsed.as_millis() as u64,
            stop,
            stop_detail: (!stop.is_normal())
                .then(|| timeline.last().map(|entry| entry.detail.clone()))
                .flatten(),
            duration_source,
            settle_ms: settle.as_millis() as u64,
            replicas_booted: resources.len(),
            drain_interval_ms: DRAIN_EVERY.as_millis() as u64,
            samples_paths: resources
                .iter()
                .filter_map(|replica| sample_paths.get(&replica.replica).cloned())
                .collect(),
        },
        environment,
        backends,
        workload: Workload {
            offered,
            completed: *outcomes.get("completed").unwrap_or(&0),
            cancelled: *outcomes.get("cancelled").unwrap_or(&0),
            dropped: *outcomes.get("dropped").unwrap_or(&0),
            faulted: *outcomes.get("faulted").unwrap_or(&0),
            shed: *outcomes.get("circuit-shed").unwrap_or(&0),
            rejected: *outcomes.get("rejected").unwrap_or(&0),
            unplanned,
            unplanned_by_reason,
            errors_in_fault_windows,
            by_tenant,
            by_ending,
            streamed,
            buffered,
        },
        latency_ms: latency.distribution(),
        ttft_ms: ttft.distribution(),
        segments,
        resources,
        trend,
        revisions,
        faults,
        restart,
        tenancy,
        usage: Usage {
            owed,
            emitted: records_observed,
            distinct: tally.distinct,
            probe_distinct: probes.distinct,
            duplicates,
            missing,
            unexpected_statuses,
            by_status,
            durable: durable.counts,
            durable_lag_ms: durable.settled.lag_ms,
            durable_settled: durable.settled.within_bound,
            durable_loss_total: loss.total,
            durable_loss_outside_windows: loss.outside,
            durable_loss_in_window: loss.in_window,
            settled_outside_usage_window: loss.settled_outside,
            durable_outside_usage_window: durable.distinct_outside_window,
            durable_duplicate_rows: durable.counts.rows.saturating_sub(durable.counts.distinct),
            sink_drops,
        },
        telemetry,
        timeline,
        verdicts,
    }
}

fn mix(profile: &Profile) -> BTreeMap<String, usize> {
    Ending::ALL
        .iter()
        .map(|&ending| (ending.as_str().to_owned(), profile.mix.weight(ending)))
        .collect()
}

/// The fleet's resident memory against time, fitted through the closed
/// segments. Reported only where there are enough of them for a slope to mean
/// anything — an unevaluated trend is stated rather than silently passed.
fn trend(segments: &[Segment], slo: &Slo) -> Trend {
    let points: Vec<(f64, f64)> = segments
        .iter()
        .filter_map(|segment| {
            segment
                .rss_kib
                .map(|rss| (segment.ended_ms as f64 / 3_600_000.0, rss as f64))
        })
        .collect();
    if slo.max_rss_drift_kib_per_hour.is_none() || points.len() < 3 {
        return Trend {
            rss_kib_per_hour: None,
            evaluated: false,
            segments: points.len(),
        };
    }
    let n = points.len() as f64;
    let mean_x = points.iter().map(|(x, _)| x).sum::<f64>() / n;
    let mean_y = points.iter().map(|(_, y)| y).sum::<f64>() / n;
    let covariance: f64 = points
        .iter()
        .map(|(x, y)| (x - mean_x) * (y - mean_y))
        .sum();
    let variance: f64 = points.iter().map(|(x, _)| (x - mean_x).powi(2)).sum();
    Trend {
        rss_kib_per_hour: (variance > f64::EPSILON).then(|| covariance / variance),
        evaluated: variance > f64::EPSILON,
        segments: points.len(),
    }
}

fn readiness_gap_ms(since: Duration, ended: Duration) -> u64 {
    ended.saturating_sub(since).as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_finalizes_an_open_readiness_gap_at_run_end() {
        let run_dir = std::env::temp_dir().join(format!(
            "axond-stateful-endurance-readiness-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("the test clock is after the epoch")
                .as_nanos()
        ));
        std::fs::create_dir_all(&run_dir).expect("the readiness test directory is writable");

        let (manifest, _) = crate::support::stateful_endurance::manifest::load();
        let profile = &manifest.profiles[0];
        let mut state = State::new(
            &run_dir,
            "readiness-gap",
            profile.smoke,
            profile.schedule,
            Duration::from_millis(profile.smoke.duration_ms),
            Injected::EveryDeclaredFault,
            &[],
        );

        state.observe_readiness(false, Duration::from_millis(100));
        assert_eq!(state.telemetry.worst_readiness_gap_ms, 0);

        // This is the path run_with takes after the supervisor ends while the
        // fleet is still unready. It must count the interval even without a
        // false-to-true readiness transition to close it.
        state.finalize(Duration::from_millis(850));
        assert_eq!(state.telemetry.worst_readiness_gap_ms, 750);
        assert!(state.unready_since.is_none());

        // Finalization is idempotent: the normal recovery path must not count
        // the same interval again if it observes the closed state later.
        state.finalize(Duration::from_millis(1_000));
        assert_eq!(state.telemetry.worst_readiness_gap_ms, 750);

        drop(state);
        std::fs::remove_dir_all(&run_dir).expect("the readiness test directory is removable");
    }

    #[test]
    fn settlement_statuses_require_plan_classification() {
        assert_eq!(classify_usage_status(Some("rejected")), Some("rejected"));
        assert_eq!(classify_usage_status(Some("ok")), Some("complete"));
        assert_eq!(classify_usage_status(Some("mystery-success")), None);
        assert_eq!(classify_usage_status(None), None);
    }
}
