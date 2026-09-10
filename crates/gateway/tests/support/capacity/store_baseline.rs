//! The Store-backed latency baseline (#462).
//!
//! The capacity harness measures a replica against a fake upstream and records
//! what the whole request cost. This harness asks a narrower question with the
//! same apparatus: how much of that cost is the Store — the one namespace-plus-
//! admission join before dispatch and the charge after the response (ADR 0064),
//! the background usage index, and the management summaries that share the
//! same connection or pool with all of them — and how the two backends compare.
//!
//! Every scenario runs the same shape rotation through the gateway and, where a
//! control is possible, directly against the fake upstream, so gateway overhead
//! is a difference between two measurements on the same machine at the same
//! time rather than a number read off a different host. The process exports its
//! metrics to a loopback OTLP receiver, and the decoded `axond.store.*`
//! histograms are the phase evidence: acquire wait apart from query execution,
//! per operation kind, per backend.
//!
//! Nothing here gates latency. What is asserted is conservation — every offered
//! request accepted, shed, or failed, and every accepted one charged — and that
//! the Store instrumentation the optimisation issues will be measured with was
//! exported for the operations they are about. Shared-CI latency is recorded
//! and left informational, as [ADR 0033](../../../../../docs/adr/0033-capacity-qualification-harness.md)
//! settled; the controlled-runner procedure is in
//! `docs/operations/store-latency-baseline.md`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime};

use serde::Serialize;
use serde_json::Value;

use super::manifest::workspace_root;
use super::probe::{ResourceReport, Sampler};
use super::result::{Environment, Percentiles, RunMeta};
use super::run::{
    Attempt, CHAT, Gauges, Outcome, PLATFORM, Shape, attempt, control_attempt, count, error_types,
    load_lock, millis, tally, tuning,
};
use crate::support::fault::collector::{Collector, HistogramPoint, LabelledPoint};
use crate::support::gateway::{
    Axond, GATEWAY_KEY, HARNESS_BUDGET_PERIOD, alias, config_toml_with_storage, postgres_storage,
    sqlite_storage,
};
use crate::support::schema::Schema;
use crate::support::upstream::{FakeUpstream, target};

/// The result-artifact schema. Bumped when a field changes meaning.
pub const RESULT_SCHEMA_VERSION: u32 = 1;

/// This file, relative to the workspace root. The scenarios are code rather
/// than a manifest, so the artifact hashes the code that defined them.
pub const SOURCE_RELATIVE: &str = "crates/gateway/tests/support/capacity/store_baseline.rs";

/// The env var that opts into the full tier.
pub const FULL_TIER_ENV: &str = "AXOND_STORE_BASELINE";
/// The env var that moves the artifact directory off `target/store-baseline`.
pub const ARTIFACT_DIR_ENV: &str = "AXOND_STORE_BASELINE_DIR";
/// The env var that overrides how many measured repetitions each scenario runs.
pub const REPETITIONS_ENV: &str = "AXOND_STORE_BASELINE_REPETITIONS";
/// The env var the process reads the Postgres Store DSN from. The DSN itself
/// never enters the config or the artifact.
const STORE_DSN_ENV: &str = "GW_STORE_BASELINE_DSN";

/// How long usage records may trail the last client byte before they count as
/// dropped. A bound on the sink, not the request path.
const SETTLE_TIMEOUT: Duration = Duration::from_secs(120);
/// The pause between two waves of a burst: long enough for the Postgres pool to
/// shed its excess sessions to the idle cap, so the next wave has to reconnect.
const BURST_PAUSE: Duration = Duration::from_millis(250);

/// Which scale a run offers. The smoke tier keeps the harness honest under
/// `cargo test`; the full tier is the baseline and wants a runner to itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BaselineTier {
    Smoke,
    Full,
}

impl BaselineTier {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Smoke => "smoke",
            Self::Full => "full",
        }
    }

    /// Measured repetitions per scenario, after the warmup. Overridable so a
    /// controlled runner can ask for more without a code change.
    pub fn repetitions(self) -> usize {
        std::env::var(REPETITIONS_ENV)
            .ok()
            .and_then(|value| value.parse().ok())
            .filter(|reps| *reps > 0)
            .unwrap_or(match self {
                Self::Smoke => 1,
                Self::Full => 3,
            })
    }
}

/// The workloads the baseline compares the backends under.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Scenario {
    /// A closed loop of buffered chat completions: the inference path at a
    /// steady concurrency, one resolve+admit join and one charge per request.
    SteadyBuffered,
    /// The same loop over paced SSE streams: TTFT is the number that moves.
    SteadyStreamed,
    /// Waves of simultaneous buffered requests with a pause between them: what
    /// a pool does when demand arrives all at once and then goes away.
    Burst,
    /// The steady buffered loop while management readers summarise usage
    /// against the same Store, over an index that is still small.
    Summaries,
    /// The same, over a pre-seeded usage index large enough that each summary
    /// holds the connection for a while: the slow-Store case, without a fault
    /// injector.
    SlowStore,
    /// Buffered requests carrying a large native prompt and relaying a large
    /// answer: the payload the gateway copies, on the same Store path.
    LargePayload,
}

impl Scenario {
    pub const ALL: [Self; 6] = [
        Self::SteadyBuffered,
        Self::SteadyStreamed,
        Self::Burst,
        Self::Summaries,
        Self::SlowStore,
        Self::LargePayload,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::SteadyBuffered => "steady-buffered",
            Self::SteadyStreamed => "steady-streamed",
            Self::Burst => "burst",
            Self::Summaries => "summaries",
            Self::SlowStore => "slow-store",
            Self::LargePayload => "large-payload",
        }
    }

    pub const fn description(self) -> &'static str {
        match self {
            Self::SteadyBuffered => {
                "Closed-loop buffered chat completions: one namespace+admit join and one charge per request."
            }
            Self::SteadyStreamed => {
                "Closed-loop paced SSE streams read to completion; TTFT measured at the first relayed byte."
            }
            Self::Burst => {
                "Waves of simultaneous buffered requests separated by a pause, so the pool fills and drains each wave."
            }
            Self::Summaries => {
                "Steady buffered load while concurrent management readers summarise usage over a small index."
            }
            Self::SlowStore => {
                "Steady buffered load while concurrent management readers summarise usage over a pre-seeded large index."
            }
            Self::LargePayload => {
                "Buffered requests with a large native prompt relaying a 256 KiB answer."
            }
        }
    }

    /// The scale a tier offers this scenario at.
    pub fn scale(self, tier: BaselineTier) -> Scale {
        let smoke = tier == BaselineTier::Smoke;
        match self {
            Self::SteadyBuffered => Scale {
                arrival: Arrival::ClosedLoop,
                concurrency: if smoke { 4 } else { 32 },
                requests: if smoke { 24 } else { 3000 },
                warmup_requests: if smoke { 8 } else { 300 },
                summary_readers: 0,
                seeded_usage_rows: 0,
                payload_bytes: 0,
            },
            Self::SteadyStreamed => Scale {
                arrival: Arrival::ClosedLoop,
                concurrency: if smoke { 4 } else { 32 },
                requests: if smoke { 12 } else { 600 },
                warmup_requests: if smoke { 4 } else { 64 },
                summary_readers: 0,
                seeded_usage_rows: 0,
                payload_bytes: 0,
            },
            Self::Burst => Scale {
                arrival: Arrival::Burst {
                    wave: if smoke { 12 } else { 128 },
                },
                concurrency: if smoke { 12 } else { 128 },
                requests: if smoke { 24 } else { 1024 },
                warmup_requests: if smoke { 12 } else { 128 },
                summary_readers: 0,
                seeded_usage_rows: 0,
                payload_bytes: 0,
            },
            Self::Summaries => Scale {
                arrival: Arrival::ClosedLoop,
                concurrency: if smoke { 4 } else { 32 },
                requests: if smoke { 24 } else { 2000 },
                warmup_requests: if smoke { 8 } else { 200 },
                summary_readers: if smoke { 1 } else { 4 },
                seeded_usage_rows: 0,
                payload_bytes: 0,
            },
            Self::SlowStore => Scale {
                arrival: Arrival::ClosedLoop,
                concurrency: if smoke { 4 } else { 32 },
                requests: if smoke { 24 } else { 2000 },
                warmup_requests: if smoke { 8 } else { 200 },
                summary_readers: if smoke { 1 } else { 4 },
                seeded_usage_rows: if smoke { 2_000 } else { 200_000 },
                payload_bytes: 0,
            },
            Self::LargePayload => Scale {
                arrival: Arrival::ClosedLoop,
                concurrency: if smoke { 4 } else { 16 },
                requests: if smoke { 12 } else { 600 },
                warmup_requests: if smoke { 4 } else { 64 },
                summary_readers: 0,
                seeded_usage_rows: 0,
                payload_bytes: if smoke { 32 * 1024 } else { 256 * 1024 },
            },
        }
    }

    fn shape(self) -> Shape {
        match self {
            Self::SteadyBuffered | Self::Burst | Self::Summaries | Self::SlowStore => {
                Shape::buffered(CHAT, alias::CHAT)
            }
            Self::SteadyStreamed => Shape::streamed(CHAT, alias::CHAT_SLOW),
            Self::LargePayload => Shape::buffered(CHAT, alias::CHAT_SIZED_LARGE),
        }
    }

    /// The provider-side model the scenario's alias resolves to, for the
    /// control run that asks the fake upstream directly.
    const fn upstream_model(self) -> &'static str {
        match self {
            Self::SteadyBuffered | Self::Burst | Self::Summaries | Self::SlowStore => target::CHAT,
            Self::SteadyStreamed => target::SLOW_STREAM,
            Self::LargePayload => target::SIZED_BODY_LARGE,
        }
    }

    /// Whether a direct-upstream control is meaningful. It is not for the two
    /// scenarios whose subject is the management readers: the upstream has no
    /// summaries to serve, so a control there would measure the buffered loop
    /// twice.
    const fn has_control(self) -> bool {
        !matches!(self, Self::Summaries | Self::SlowStore)
    }
}

/// How requests arrive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum Arrival {
    /// `concurrency` workers each take the next request as soon as their last
    /// one ends.
    ClosedLoop,
    /// `wave` requests fire at once; the next wave starts when every one of the
    /// last has ended and [`BURST_PAUSE`] has passed.
    Burst { wave: usize },
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct Scale {
    pub arrival: Arrival,
    pub concurrency: usize,
    pub requests: usize,
    /// Requests offered and discarded before the first measured repetition.
    pub warmup_requests: usize,
    /// Management readers summarising usage for the whole measured loop.
    pub summary_readers: usize,
    /// Usage-index rows inserted before the process boots for the run.
    pub seeded_usage_rows: u64,
    /// Bytes of prompt padding on each request.
    pub payload_bytes: usize,
}

/// The Store a run boots against.
pub enum Backend {
    Sqlite,
    Postgres { dsn: String },
}

impl Backend {
    pub const fn label(&self) -> &'static str {
        match self {
            Self::Sqlite => "sqlite",
            Self::Postgres { .. } => "postgres",
        }
    }
}

/// Where this run keeps its Store: a file, or a schema of its own in the test
/// database that goes when the run does.
enum StoreLocation {
    Sqlite(PathBuf),
    Postgres { dsn: String, schema: Schema },
}

impl StoreLocation {
    fn storage_toml(&self) -> String {
        match self {
            Self::Sqlite(path) => sqlite_storage(path),
            Self::Postgres { .. } => postgres_storage(STORE_DSN_ENV),
        }
    }

    fn env(&self) -> Vec<(String, String)> {
        match self {
            Self::Sqlite(_) => Vec::new(),
            Self::Postgres { dsn, .. } => vec![(STORE_DSN_ENV.to_owned(), dsn.clone())],
        }
    }

    /// The per-run value in the rendered config, and what to replace it with
    /// before hashing, so two runs of one scenario hash alike.
    fn normalization(&self) -> Vec<(String, &'static str)> {
        match self {
            Self::Sqlite(path) => vec![(path.display().to_string(), "STORE_SQLITE_PATH")],
            Self::Postgres { .. } => Vec::new(),
        }
    }
}

/// Everything one run produced: every scenario, with the inputs and the host.
#[derive(Debug, Clone, Serialize)]
pub struct BaselineResult {
    pub schema_version: u32,
    pub run: RunMeta,
    pub tier: &'static str,
    pub backend: &'static str,
    pub repetitions: usize,
    pub environment: Environment,
    /// What the environment says about the machine this ran on, in words a
    /// reader will not skip: a shared runner's numbers are informational.
    pub caveat: &'static str,
    pub scenarios: Vec<ScenarioResult>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScenarioResult {
    pub id: &'static str,
    pub description: &'static str,
    pub scale: Scale,
    pub store: StoreInfo,
    pub warmup: Warmup,
    pub repetitions: Vec<Measurement>,
    /// The same load offered to the fake upstream directly, once, after its own
    /// warmup. Absent where a control would not measure the scenario's subject.
    pub control: Option<Control>,
    /// Gateway latency minus control latency at each percentile, using the
    /// median repetition's percentiles. What the gateway added, on this host.
    pub overhead_ms: Option<Overhead>,
    pub store_evidence: StoreEvidence,
}

#[derive(Debug, Clone, Serialize)]
pub struct StoreInfo {
    pub backend: &'static str,
    /// `sqlite_version()` or `version()`.
    pub version: Option<String>,
    /// Usage-index rows when the run ended: the seeded rows plus what the run
    /// appended.
    pub usage_rows: Option<u64>,
    /// The file, or the usage relation with its indexes, in bytes at the end.
    pub data_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct Warmup {
    pub offered: u64,
    pub accepted: u64,
}

/// One measured repetition.
#[derive(Debug, Clone, Serialize)]
pub struct Measurement {
    pub offered: u64,
    pub accepted: u64,
    pub rejected: u64,
    pub errors: u64,
    pub transport_failures: u64,
    /// `offered == accepted + rejected + errors`. Asserted by the suite; kept
    /// in the artifact so a reader need not recompute it.
    pub reconciled: bool,
    pub elapsed_ms: u128,
    pub accepted_rps: f64,
    pub latency_ms: Percentiles,
    pub ttft_ms: Option<Percentiles>,
    pub by_status: BTreeMap<String, u64>,
    pub rejections_by_error_type: BTreeMap<String, u64>,
    pub errors_by_error_type: BTreeMap<String, u64>,
    pub resources: ResourceReport,
    pub usage_records: UsageSettlement,
    /// The management readers that ran through this repetition, when any did.
    pub summaries: Option<SummaryReport>,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct UsageSettlement {
    pub expected: u64,
    pub observed: u64,
    pub missing: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SummaryReport {
    pub readers: usize,
    pub requests: u64,
    pub ok: u64,
    pub errors: u64,
    pub by_status: BTreeMap<String, u64>,
    pub latency_ms: Option<Percentiles>,
    pub requests_per_second: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Control {
    pub offered: u64,
    pub accepted: u64,
    pub elapsed_ms: u128,
    pub accepted_rps: f64,
    pub latency_ms: Percentiles,
    pub ttft_ms: Option<Percentiles>,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct Overhead {
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub ttft_p50: Option<f64>,
    pub ttft_p95: Option<f64>,
}

/// The decoded `axond.store.*` and `axond.usage.index.queue.*` evidence,
/// cumulative over the scenario's process: warmup included, which the artifact
/// says so a reader does not divide by the measured count.
#[derive(Debug, Clone, Serialize)]
pub struct StoreEvidence {
    pub includes_warmup: bool,
    pub backend_label: &'static str,
    pub operations: BTreeMap<String, OperationEvidence>,
    pub connections_opened: u64,
    pub index_queue: Option<IndexQueueEvidence>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OperationEvidence {
    pub calls: u64,
    pub outcomes: BTreeMap<String, u64>,
    pub acquire_wait: HistogramSummary,
    pub query_duration: Option<HistogramSummary>,
}

/// What an exported explicit-bucket histogram says. The percentiles are the
/// upper bound of the bucket the rank falls in, so they are ceilings rather
/// than values; `None` means the rank fell in the overflow bucket.
#[derive(Debug, Clone, Serialize)]
pub struct HistogramSummary {
    pub count: u64,
    pub sum_ms: f64,
    pub mean_ms: f64,
    pub min_ms: Option<f64>,
    pub max_ms: Option<f64>,
    pub p50_le_ms: Option<f64>,
    pub p95_le_ms: Option<f64>,
    pub p99_le_ms: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct IndexQueueEvidence {
    pub depth_observations: u64,
    pub max_depth: Option<f64>,
    pub wait: HistogramSummary,
}

impl HistogramSummary {
    fn of(point: &HistogramPoint) -> Self {
        let rank = |quantile: f64| -> Option<f64> {
            if point.count == 0 {
                return None;
            }
            let target = (quantile * point.count as f64).ceil().max(1.0) as u64;
            let mut seen = 0;
            for (index, bucket) in point.bucket_counts.iter().enumerate() {
                seen += bucket;
                if seen >= target {
                    return point.explicit_bounds.get(index).copied();
                }
            }
            None
        };
        let sum = point.sum.unwrap_or_default();
        Self {
            count: point.count,
            sum_ms: sum,
            mean_ms: if point.count == 0 {
                0.0
            } else {
                sum / point.count as f64
            },
            min_ms: point.min,
            max_ms: point.max,
            p50_le_ms: rank(0.50),
            p95_le_ms: rank(0.95),
            p99_le_ms: rank(0.99),
        }
    }
}

/// The Postgres DSN the full tier's Postgres arm needs, or `None` to skip it.
pub fn postgres_dsn() -> Option<String> {
    crate::support::stateful::postgres_dsn()
}

/// Whether the full tier was asked for.
pub fn full_tier_requested() -> bool {
    std::env::var(FULL_TIER_ENV).as_deref() == Ok("1")
}

/// Run every scenario against `backend` at `tier`.
pub async fn run(backend: &Backend, tier: BaselineTier) -> BaselineResult {
    let _offering = load_lock().lock().await;
    let started_at = SystemTime::now();
    let started = Instant::now();
    let upstream = FakeUpstream::start().await;
    let source_text = include_str!("store_baseline.rs");
    let mut environment = None;
    let mut scenarios = Vec::with_capacity(Scenario::ALL.len());
    for scenario in Scenario::ALL {
        let (result, config, bind, normalization) =
            run_scenario(backend, tier, scenario, &upstream).await;
        environment.get_or_insert_with(|| {
            Environment::collect_normalizing(
                &config,
                &bind,
                &upstream.base_url,
                &normalization,
                SOURCE_RELATIVE,
                source_text,
            )
        });
        eprintln!("store-baseline: {}", scenario_summary(&result));
        scenarios.push(result);
    }
    BaselineResult {
        schema_version: RESULT_SCHEMA_VERSION,
        run: RunMeta::for_harness(
            "axond store latency baseline",
            started_at,
            started.elapsed(),
        ),
        tier: tier.as_str(),
        backend: backend.label(),
        repetitions: tier.repetitions(),
        environment: environment.expect("at least one scenario ran"),
        caveat: "Latency, throughput, and CPU are informational: they measure the host as much \
                 as the gateway. Compare artifacts only when environment.hardware, \
                 environment.toolchain.cargo_profile, and environment.config.sha256 agree, and \
                 read docs/operations/store-latency-baseline.md before comparing two commits.",
        scenarios,
    }
}

async fn run_scenario(
    backend: &Backend,
    tier: BaselineTier,
    scenario: Scenario,
    upstream: &FakeUpstream,
) -> (ScenarioResult, String, String, Vec<(String, &'static str)>) {
    let scale = scenario.scale(tier);
    let location = prepare_store(backend, scenario).await;
    let render = |storage: &str, addr: std::net::SocketAddr| {
        config_toml_with_storage(addr, &upstream.base_url, tuning(), "", storage)
    };

    if scale.seeded_usage_rows > 0 {
        // The process owns the schema, so it creates it; the seed is then an
        // ordinary insert into a store that has been booted once before.
        let storage = location.storage_toml();
        let mut creator =
            Axond::start_custom(&|addr| render(&storage, addr), &location.env()).await;
        creator.shutdown();
        drop(creator);
        seed_usage_rows(&location, scale.seeded_usage_rows).await;
    }

    let collector = Collector::start().await;
    let mut env = location.env();
    env.extend([
        (
            "OTEL_EXPORTER_OTLP_ENDPOINT".to_owned(),
            collector.endpoint.clone(),
        ),
        (
            "OTEL_EXPORTER_OTLP_PROTOCOL".to_owned(),
            "http/protobuf".to_owned(),
        ),
        ("OTEL_METRIC_EXPORT_INTERVAL".to_owned(), "1000".to_owned()),
        ("OTEL_BSP_SCHEDULE_DELAY".to_owned(), "200".to_owned()),
    ]);
    let storage = location.storage_toml();
    let mut gateway = Axond::start_custom(&|addr| render(&storage, addr), &env).await;
    let bind = gateway.bind().to_owned();
    let config = gateway.config.clone();
    let client = crate::support::client();
    let shape = scenario.shape().padded(scale.payload_bytes);

    // Warmup: the same load, discarded. What it warms is the process — its
    // allocator, its connection pool, its prepared statements — and the
    // measured repetitions start from there rather than from a cold boot.
    let warmed = offer(
        &client,
        &gateway.base_url,
        shape,
        scale,
        scale.warmup_requests,
    )
    .await;
    let warmup = Warmup {
        offered: warmed.len() as u64,
        accepted: count(&warmed, |a| a.outcome == Outcome::Accepted),
    };
    let _ = await_usage_records(&gateway, warmup.accepted).await;

    let mut repetitions = Vec::with_capacity(tier.repetitions());
    for _ in 0..tier.repetitions() {
        let records_before = gateway.usage_records().len() as u64;
        let readers = (scale.summary_readers > 0)
            .then(|| SummaryReaders::start(&client, &gateway.base_url, scale.summary_readers));
        let sampler = Sampler::start(gateway.pid());
        let started = Instant::now();
        let attempts = offer(&client, &gateway.base_url, shape, scale, scale.requests).await;
        let elapsed = started.elapsed();
        let resources = sampler.finish(elapsed);
        let summaries = match readers {
            Some(readers) => Some(readers.stop(elapsed).await),
            None => None,
        };
        let accepted = count(&attempts, |a| a.outcome == Outcome::Accepted);
        let expected = records_before + accepted;
        let observed = await_usage_records(&gateway, expected).await;
        repetitions.push(measurement(
            &attempts,
            elapsed,
            resources,
            UsageSettlement {
                expected: accepted,
                observed: observed.saturating_sub(records_before),
                missing: expected.saturating_sub(observed),
            },
            summaries,
        ));
    }

    let control = if scenario.has_control() {
        Some(control_run(&client, &upstream.base_url, shape, scenario, scale).await)
    } else {
        None
    };
    let overhead = control
        .as_ref()
        .map(|control| overhead(&repetitions, control));

    // Graceful termination flushes the exporter; the decoded points are the
    // evidence, and a process killed mid-interval would lose the last of it.
    gateway.terminate();
    let exit = gateway.await_exit(Duration::from_secs(30)).await;
    assert!(
        exit.is_some_and(|status| status.success()),
        "{}: the replica did not flush telemetry and exit cleanly: {exit:?}",
        scenario.as_str()
    );
    gateway.settle_output(Duration::from_secs(2)).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let store_evidence = store_evidence(&collector, backend.label());
    let store = store_info(&location).await;
    drop(gateway);
    let normalization = location.normalization();
    drop(location);

    (
        ScenarioResult {
            id: scenario.as_str(),
            description: scenario.description(),
            scale,
            store,
            warmup,
            repetitions,
            control,
            overhead_ms: overhead,
            store_evidence,
        },
        config,
        bind,
        normalization,
    )
}

/// A Store of this scenario's own: a fresh file, or a fresh schema.
async fn prepare_store(backend: &Backend, scenario: Scenario) -> StoreLocation {
    match backend {
        Backend::Sqlite => {
            let path = std::env::temp_dir().join(format!(
                "axond-store-baseline-{}-{}.sqlite",
                std::process::id(),
                scenario.as_str()
            ));
            for suffix in ["", "-wal", "-shm"] {
                let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
            }
            StoreLocation::Sqlite(path)
        }
        Backend::Postgres { dsn } => {
            let name = format!(
                "axond_store_baseline_{}_{}",
                std::process::id(),
                scenario.as_str().replace('-', "_")
            );
            let schema = Schema::create(dsn, &name).await;
            let separator = if dsn.contains('?') { '&' } else { '?' };
            StoreLocation::Postgres {
                dsn: format!("{dsn}{separator}options=-c%20search_path%3D{name}"),
                schema,
            }
        }
    }
}

/// Insert `rows` usage-index rows under the platform namespace and the harness
/// period, so the management readers aggregate over them. Chunked, in one
/// transaction per chunk: a row at a time would make the seed the slow part
/// of the run.
async fn seed_usage_rows(location: &StoreLocation, rows: u64) {
    const CHUNK: u64 = 5_000;
    match location {
        StoreLocation::Sqlite(path) => {
            let path = path.clone();
            tokio::task::spawn_blocking(move || {
                let mut conn = rusqlite::Connection::open(&path).expect("the seeded file opens");
                let mut inserted = 0;
                while inserted < rows {
                    let tx = conn.transaction().expect("a seed transaction");
                    {
                        let mut statement = tx
                            .prepare_cached(
                                "INSERT OR IGNORE INTO axond_store_usage
                                    (request_id, namespace, period, model, status,
                                     cost_microdollars, recorded_at)
                                 VALUES (?1, ?2, ?3, ?4, 'ok', ?5, 0)",
                            )
                            .expect("the usage table the process created");
                        for row in inserted..(inserted + CHUNK).min(rows) {
                            statement
                                .execute(rusqlite::params![
                                    format!("seed-{row}"),
                                    PLATFORM.namespace,
                                    HARNESS_BUDGET_PERIOD,
                                    seeded_model(row),
                                    (row % 997) as i64,
                                ])
                                .expect("a seed row");
                        }
                    }
                    tx.commit().expect("the seed commits");
                    inserted += CHUNK;
                }
            })
            .await
            .expect("the seed thread does not panic");
        }
        StoreLocation::Postgres { dsn, .. } => {
            let client = connect_postgres(dsn).await;
            let mut inserted = 0;
            while inserted < rows {
                let end = (inserted + CHUNK).min(rows);
                let mut sql = String::from(
                    "INSERT INTO axond_store_usage
                        (request_id, namespace, period, model, status, cost_microdollars, recorded_at)
                     VALUES ",
                );
                for row in inserted..end {
                    if row > inserted {
                        sql.push(',');
                    }
                    sql.push_str(&format!(
                        "('seed-{row}', '{}', '{}', '{}', 'ok', {}, now())",
                        PLATFORM.namespace,
                        HARNESS_BUDGET_PERIOD,
                        seeded_model(row),
                        row % 997
                    ));
                }
                sql.push_str(" ON CONFLICT (request_id) DO NOTHING");
                client.batch_execute(&sql).await.expect("a seed chunk");
                inserted = end;
            }
        }
    }
}

/// A handful of models, so the summary has groups to aggregate into rather
/// than one row per seeded request.
fn seeded_model(row: u64) -> &'static str {
    const MODELS: [&str; 4] = [
        "fake-openai/fixture-chat",
        "fake-openai/slow-stream",
        "fake-anthropic/fixture-messages",
        "fake-openai/fixture-embeddings",
    ];
    MODELS[(row % MODELS.len() as u64) as usize]
}

async fn connect_postgres(dsn: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .expect("connect to the baseline schema");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// What the Store held when the run ended, read from outside the process.
async fn store_info(location: &StoreLocation) -> StoreInfo {
    match location {
        StoreLocation::Sqlite(path) => {
            let path = path.clone();
            tokio::task::spawn_blocking(move || {
                let conn = rusqlite::Connection::open(&path).ok();
                let usage_rows = conn.as_ref().and_then(|conn| {
                    conn.query_row("SELECT COUNT(*) FROM axond_store_usage", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .ok()
                    .map(|rows| rows.max(0) as u64)
                });
                let mut data_bytes = 0;
                for suffix in ["", "-wal"] {
                    if let Ok(meta) = std::fs::metadata(format!("{}{suffix}", path.display())) {
                        data_bytes += meta.len();
                    }
                }
                StoreInfo {
                    backend: "sqlite",
                    version: Some(rusqlite::version().to_owned()),
                    usage_rows,
                    data_bytes: Some(data_bytes),
                }
            })
            .await
            .expect("the store inspection does not panic")
        }
        StoreLocation::Postgres { dsn, .. } => {
            let client = connect_postgres(dsn).await;
            let version = client
                .query_one("SELECT version()", &[])
                .await
                .ok()
                .map(|row| row.get::<_, String>(0));
            let usage_rows = client
                .query_one("SELECT COUNT(*) FROM axond_store_usage", &[])
                .await
                .ok()
                .map(|row| row.get::<_, i64>(0).max(0) as u64);
            let data_bytes = client
                .query_one("SELECT pg_total_relation_size('axond_store_usage')", &[])
                .await
                .ok()
                .map(|row| row.get::<_, i64>(0).max(0) as u64);
            StoreInfo {
                backend: "postgres",
                version,
                usage_rows,
                data_bytes,
            }
        }
    }
}

/// Offer `total` requests of `shape` under the scale's arrival pattern.
async fn offer(
    client: &reqwest::Client,
    base_url: &str,
    shape: Shape,
    scale: Scale,
    total: usize,
) -> Vec<Attempt> {
    let gauges = Arc::new(Gauges::default());
    let (client, base_url) = (client.clone(), Arc::new(base_url.to_owned()));
    let make = move |_| {
        let (client, gauges, base_url) = (client.clone(), gauges.clone(), base_url.clone());
        async move { attempt(&client, &base_url, shape, None, &gauges).await }
    };
    match scale.arrival {
        Arrival::ClosedLoop => closed_loop(scale.concurrency, total, make).await,
        Arrival::Burst { wave } => waves(wave, total, make).await,
    }
}

/// `concurrency` workers taking the next index until `total` are taken.
async fn closed_loop<F, Fut>(concurrency: usize, total: usize, make: F) -> Vec<Attempt>
where
    F: Fn(usize) -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<Output = Attempt> + Send,
{
    let next = Arc::new(AtomicUsize::new(0));
    let mut workers = Vec::with_capacity(concurrency);
    for _ in 0..concurrency.max(1) {
        let (next, make) = (next.clone(), make.clone());
        workers.push(tokio::spawn(async move {
            let mut attempts = Vec::new();
            loop {
                let index = next.fetch_add(1, Ordering::Relaxed);
                if index >= total {
                    return attempts;
                }
                attempts.push(make(index).await);
            }
        }));
    }
    let mut attempts = Vec::with_capacity(total);
    for worker in workers {
        attempts.extend(worker.await.expect("a baseline worker does not panic"));
    }
    attempts
}

/// Waves of `wave` simultaneous requests until `total` are sent, pausing
/// between waves.
async fn waves<F, Fut>(wave: usize, total: usize, make: F) -> Vec<Attempt>
where
    F: Fn(usize) -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<Output = Attempt> + Send + 'static,
{
    let mut attempts = Vec::with_capacity(total);
    let mut sent = 0;
    while sent < total {
        let this_wave = wave.max(1).min(total - sent);
        let tasks: Vec<_> = (sent..sent + this_wave)
            .map(|index| tokio::spawn(make(index)))
            .collect();
        for task in tasks {
            attempts.push(task.await.expect("a burst worker does not panic"));
        }
        sent += this_wave;
        if sent < total {
            tokio::time::sleep(BURST_PAUSE).await;
        }
    }
    attempts
}

/// The same load, offered to the fake upstream directly.
async fn control_run(
    client: &reqwest::Client,
    upstream_base_url: &str,
    shape: Shape,
    scenario: Scenario,
    scale: Scale,
) -> Control {
    let model = scenario.upstream_model();
    let run = |total: usize| {
        let gauges = Arc::new(Gauges::default());
        let (client, upstream) = (client.clone(), Arc::new(upstream_base_url.to_owned()));
        async move {
            let make = move |_| {
                let (client, gauges, upstream) = (client.clone(), gauges.clone(), upstream.clone());
                async move { control_attempt(&client, &upstream, shape, model, &gauges).await }
            };
            match scale.arrival {
                Arrival::ClosedLoop => closed_loop(scale.concurrency, total, make).await,
                Arrival::Burst { wave } => waves(wave, total, make).await,
            }
        }
    };
    let _ = run(scale.warmup_requests).await;
    let started = Instant::now();
    let attempts = run(scale.requests).await;
    let elapsed = started.elapsed();
    let accepted = count(&attempts, |a| a.outcome == Outcome::Accepted);
    let latency: Vec<f64> = attempts.iter().map(|a| a.latency_ms).collect();
    let ttft: Vec<f64> = attempts.iter().filter_map(|a| a.ttft_ms).collect();
    Control {
        offered: attempts.len() as u64,
        accepted,
        elapsed_ms: elapsed.as_millis(),
        accepted_rps: accepted as f64 / elapsed.as_secs_f64().max(f64::EPSILON),
        latency_ms: Percentiles::of(&latency).expect("a control offers at least one request"),
        ttft_ms: Percentiles::of(&ttft),
    }
}

/// Gateway minus control, at the median repetition by p50. Negative values are
/// possible on a shared host and are reported as measured: they say the two
/// runs did not see the same machine, which is itself a finding.
fn overhead(repetitions: &[Measurement], control: &Control) -> Overhead {
    let median = median_repetition(repetitions);
    Overhead {
        p50: median.latency_ms.p50 - control.latency_ms.p50,
        p95: median.latency_ms.p95 - control.latency_ms.p95,
        p99: median.latency_ms.p99 - control.latency_ms.p99,
        ttft_p50: median
            .ttft_ms
            .zip(control.ttft_ms)
            .map(|(gateway, control)| gateway.p50 - control.p50),
        ttft_p95: median
            .ttft_ms
            .zip(control.ttft_ms)
            .map(|(gateway, control)| gateway.p95 - control.p95),
    }
}

/// The repetition whose p50 is the median of the repetitions' p50s: what a
/// summary quotes, so one disturbed repetition does not become the number.
pub fn median_repetition(repetitions: &[Measurement]) -> &Measurement {
    let mut ordered: Vec<&Measurement> = repetitions.iter().collect();
    ordered.sort_by(|a, b| a.latency_ms.p50.total_cmp(&b.latency_ms.p50));
    ordered[ordered.len() / 2]
}

fn measurement(
    attempts: &[Attempt],
    elapsed: Duration,
    resources: ResourceReport,
    usage_records: UsageSettlement,
    summaries: Option<SummaryReport>,
) -> Measurement {
    let offered = attempts.len() as u64;
    let accepted = count(attempts, |a| a.outcome == Outcome::Accepted);
    let rejected = count(attempts, |a| a.outcome == Outcome::Rejected);
    let failed = count(attempts, |a| a.outcome == Outcome::Failed);
    let transport_failures = count(attempts, |a| a.outcome == Outcome::TransportFailure);
    // A cancelled attempt cannot happen here — nothing hangs up — so it would
    // be a bug in the driver rather than a category; counted as an error so the
    // reconciliation notices rather than absorbs it.
    let cancelled = count(attempts, |a| a.outcome == Outcome::Cancelled);
    let errors = failed + transport_failures + cancelled;
    let latency: Vec<f64> = attempts.iter().map(|a| a.latency_ms).collect();
    let ttft: Vec<f64> = attempts.iter().filter_map(|a| a.ttft_ms).collect();
    Measurement {
        offered,
        accepted,
        rejected,
        errors,
        transport_failures,
        reconciled: offered == accepted + rejected + errors,
        elapsed_ms: elapsed.as_millis(),
        accepted_rps: accepted as f64 / elapsed.as_secs_f64().max(f64::EPSILON),
        latency_ms: Percentiles::of(&latency).expect("a repetition offers at least one request"),
        ttft_ms: Percentiles::of(&ttft),
        by_status: tally(attempts.iter().filter_map(|a| a.status.map(u64::from))),
        rejections_by_error_type: error_types(attempts, Outcome::Rejected),
        errors_by_error_type: error_types(attempts, Outcome::Failed),
        resources,
        usage_records,
        summaries,
    }
}

/// One management read as a reader saw it: the status it got, or `None` for a
/// transport failure, and how long it took.
type SummaryObservation = (Option<u16>, f64);

/// Management readers looping over `GET /api/v1/namespaces/platform/usage`
/// until told to stop.
struct SummaryReaders {
    stop: Arc<AtomicBool>,
    tasks: Vec<tokio::task::JoinHandle<Vec<SummaryObservation>>>,
    readers: usize,
}

impl SummaryReaders {
    fn start(client: &reqwest::Client, base_url: &str, readers: usize) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let url = format!(
            "{base_url}/api/v1/namespaces/{}/usage?period={HARNESS_BUDGET_PERIOD}",
            PLATFORM.namespace
        );
        let tasks = (0..readers)
            .map(|_| {
                let (client, stop, url) = (client.clone(), stop.clone(), url.clone());
                tokio::spawn(async move {
                    let mut seen = Vec::new();
                    while !stop.load(Ordering::Relaxed) {
                        let started = Instant::now();
                        let response = client.get(&url).bearer_auth(GATEWAY_KEY).send().await;
                        let status = match response {
                            Ok(response) => {
                                let status = response.status().as_u16();
                                let _ = response.bytes().await;
                                Some(status)
                            }
                            Err(_) => None,
                        };
                        seen.push((status, millis(started.elapsed())));
                    }
                    seen
                })
            })
            .collect();
        Self {
            stop,
            tasks,
            readers,
        }
    }

    async fn stop(self, elapsed: Duration) -> SummaryReport {
        self.stop.store(true, Ordering::Relaxed);
        let mut seen = Vec::new();
        for task in self.tasks {
            seen.extend(task.await.expect("a summary reader does not panic"));
        }
        let ok = seen
            .iter()
            .filter(|(status, _)| *status == Some(200))
            .count() as u64;
        let latency: Vec<f64> = seen.iter().map(|(_, latency)| *latency).collect();
        SummaryReport {
            readers: self.readers,
            requests: seen.len() as u64,
            ok,
            errors: seen.len() as u64 - ok,
            by_status: tally(seen.iter().map(|(status, _)| {
                status.map_or_else(|| "transport".to_owned(), |code| code.to_string())
            })),
            latency_ms: Percentiles::of(&latency),
            requests_per_second: seen.len() as f64 / elapsed.as_secs_f64().max(f64::EPSILON),
        }
    }
}

/// Wait for at least `expected` usage records; return how many there are.
async fn await_usage_records(gateway: &Axond, expected: u64) -> u64 {
    let deadline = Instant::now() + SETTLE_TIMEOUT;
    loop {
        let records = gateway.usage_records().len() as u64;
        if records >= expected || Instant::now() >= deadline {
            return records;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The latest cumulative point per label set, for an instrument the process
/// exported several times.
fn latest_points<T: Clone>(
    points: Vec<LabelledPoint<T>>,
    at: impl Fn(&T) -> u64,
) -> Vec<LabelledPoint<T>> {
    let mut latest: BTreeMap<Vec<(String, String)>, LabelledPoint<T>> = BTreeMap::new();
    for point in points {
        let key: Vec<(String, String)> = point
            .labels
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        match latest.get(&key) {
            Some(existing) if at(&existing.point) >= at(&point.point) => {}
            _ => {
                latest.insert(key, point);
            }
        }
    }
    latest.into_values().collect()
}

fn store_evidence(collector: &Collector, backend: &'static str) -> StoreEvidence {
    let histogram = |name: &str| -> Vec<LabelledPoint<HistogramPoint>> {
        latest_points(
            collector
                .labelled_histogram_points(name)
                .unwrap_or_else(|error| panic!("cannot decode {name}: {error}")),
            |point| point.time_unix_nano,
        )
    };
    let sums = |name: &str| {
        latest_points(
            collector
                .labelled_sum_points(name)
                .unwrap_or_else(|error| panic!("cannot decode {name}: {error}")),
            |point| point.time_unix_nano,
        )
    };
    let for_backend = |point: &LabelledPoint<HistogramPoint>| {
        point.labels.get("axond.store.backend").map(String::as_str) == Some(backend)
    };
    let operation = |point: &LabelledPoint<HistogramPoint>| {
        point
            .labels
            .get("axond.store.operation")
            .cloned()
            .unwrap_or_else(|| "unlabelled".to_owned())
    };

    let mut operations: BTreeMap<String, OperationEvidence> = BTreeMap::new();
    for point in histogram("axond.store.acquire_wait")
        .iter()
        .filter(|point| for_backend(point))
    {
        operations.insert(
            operation(point),
            OperationEvidence {
                calls: point.point.count,
                outcomes: BTreeMap::new(),
                acquire_wait: HistogramSummary::of(&point.point),
                query_duration: None,
            },
        );
    }
    for point in histogram("axond.store.query_duration")
        .iter()
        .filter(|point| for_backend(point))
    {
        if let Some(evidence) = operations.get_mut(&operation(point)) {
            evidence.query_duration = Some(HistogramSummary::of(&point.point));
        }
    }
    for point in sums("axond.store.operations") {
        if point.labels.get("axond.store.backend").map(String::as_str) != Some(backend) {
            continue;
        }
        let (Some(op), Some(outcome)) = (
            point.labels.get("axond.store.operation"),
            point.labels.get("axond.store.outcome"),
        ) else {
            continue;
        };
        if let Some(evidence) = operations.get_mut(op) {
            evidence
                .outcomes
                .insert(outcome.clone(), point.point.value as u64);
        }
    }
    let connections_opened = sums("axond.store.connections_opened")
        .iter()
        .filter(|point| {
            point.labels.get("axond.store.backend").map(String::as_str) == Some(backend)
        })
        .map(|point| point.point.value as u64)
        .sum();
    let depth = histogram("axond.usage.index.queue.depth");
    let wait = histogram("axond.usage.index.queue.wait");
    let index_queue = match (depth.first(), wait.first()) {
        (Some(depth), Some(wait)) => Some(IndexQueueEvidence {
            depth_observations: depth.point.count,
            max_depth: depth.point.max,
            wait: HistogramSummary::of(&wait.point),
        }),
        _ => None,
    };
    StoreEvidence {
        includes_warmup: true,
        backend_label: backend,
        operations,
        connections_opened,
        index_queue,
    }
}

impl BaselineResult {
    /// Where artifacts go: `$AXOND_STORE_BASELINE_DIR`, else
    /// `target/store-baseline`, then `<tier>/<backend>.{json,md}`.
    pub fn artifact_dir() -> PathBuf {
        std::env::var_os(ARTIFACT_DIR_ENV)
            .map(PathBuf::from)
            .unwrap_or_else(|| workspace_root().join("target/store-baseline"))
    }

    /// Write the JSON artifact and the human summary beside it; return both.
    pub fn write(&self) -> (PathBuf, PathBuf) {
        let dir = Self::artifact_dir().join(self.tier);
        std::fs::create_dir_all(&dir).expect("the baseline artifact directory is writable");
        let json = dir.join(format!("{}.json", self.backend));
        let text = serde_json::to_string_pretty(self).expect("the result artifact serializes");
        std::fs::write(&json, format!("{text}\n")).expect("the baseline artifact is writable");
        let markdown = dir.join(format!("{}.md", self.backend));
        std::fs::write(&markdown, self.summary_markdown()).expect("the summary is writable");
        (json, markdown)
    }

    /// The human summary: one table of the medians, one of the Store phases.
    pub fn summary_markdown(&self) -> String {
        use std::fmt::Write;
        let hardware = &self.environment.hardware;
        let mut out = String::new();
        let _ = writeln!(
            out,
            "# Store latency baseline: {} ({} tier)\n",
            self.backend, self.tier
        );
        let _ = writeln!(
            out,
            "- commit: `{}`{}",
            self.environment
                .source
                .git_commit
                .as_deref()
                .unwrap_or("unknown"),
            if self.environment.source.git_dirty == Some(true) {
                " (dirty tree)"
            } else {
                ""
            }
        );
        let _ = writeln!(
            out,
            "- build: {} profile, {}",
            self.environment.toolchain.cargo_profile,
            self.environment
                .toolchain
                .rustc
                .as_deref()
                .unwrap_or("unknown rustc")
        );
        let _ = writeln!(
            out,
            "- host: {} cpus, {}, {} KiB RAM, {} {}{}",
            hardware.cpus,
            hardware.cpu_model.as_deref().unwrap_or("unknown cpu"),
            hardware.total_memory_kib.unwrap_or_default(),
            hardware.os,
            hardware.kernel.as_deref().unwrap_or(""),
            if hardware.containerized {
                ", containerized"
            } else {
                ""
            }
        );
        let _ = writeln!(
            out,
            "- config sha256: `{}`; binary sha256: `{}`",
            self.environment.config.sha256, self.environment.binary.sha256
        );
        let _ = writeln!(out, "- repetitions per scenario: {}\n", self.repetitions);
        let _ = writeln!(out, "> {}\n", self.caveat);

        let _ = writeln!(out, "## Request path (median repetition)\n");
        let _ = writeln!(
            out,
            "| Scenario | Concurrency | Requests | Accepted req/s | p50 | p95 | p99 | TTFT p95 | Overhead p50 / p95 vs control | CPU cores | Peak RSS | Shed | Errors | Usage missing |"
        );
        let _ = writeln!(
            out,
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"
        );
        for scenario in &self.scenarios {
            let median = median_repetition(&scenario.repetitions);
            let _ = writeln!(
                out,
                "| `{}` | {} | {} | {:.0} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
                scenario.id,
                scenario.scale.concurrency,
                scenario.scale.requests,
                median.accepted_rps,
                ms(median.latency_ms.p50),
                ms(median.latency_ms.p95),
                ms(median.latency_ms.p99),
                median
                    .ttft_ms
                    .map_or_else(|| "—".to_owned(), |ttft| ms(ttft.p95)),
                scenario.overhead_ms.map_or_else(
                    || "—".to_owned(),
                    |overhead| format!("{} / {}", ms(overhead.p50), ms(overhead.p95))
                ),
                median
                    .resources
                    .cpu_utilization
                    .map_or_else(|| "—".to_owned(), |cpu| format!("{cpu:.2}")),
                median
                    .resources
                    .rss_kib
                    .map_or_else(|| "—".to_owned(), |rss| format!("{} MiB", rss.peak / 1024)),
                median.rejected,
                median.errors,
                median.usage_records.missing,
            );
        }

        let _ = writeln!(out, "\n## Management summaries (median repetition)\n");
        let _ = writeln!(
            out,
            "| Scenario | Readers | Summary req/s | Summary p50 | Summary p95 | Summary errors | Usage rows at end | Store bytes at end |"
        );
        let _ = writeln!(out, "| --- | --- | --- | --- | --- | --- | --- | --- |");
        for scenario in &self.scenarios {
            let median = median_repetition(&scenario.repetitions);
            let Some(summaries) = &median.summaries else {
                continue;
            };
            let _ = writeln!(
                out,
                "| `{}` | {} | {:.1} | {} | {} | {} | {} | {} |",
                scenario.id,
                summaries.readers,
                summaries.requests_per_second,
                summaries
                    .latency_ms
                    .map_or_else(|| "—".to_owned(), |p| ms(p.p50)),
                summaries
                    .latency_ms
                    .map_or_else(|| "—".to_owned(), |p| ms(p.p95)),
                summaries.errors,
                scenario
                    .store
                    .usage_rows
                    .map_or_else(|| "—".to_owned(), |rows| rows.to_string()),
                scenario
                    .store
                    .data_bytes
                    .map_or_else(|| "—".to_owned(), |bytes| bytes.to_string()),
            );
        }

        let _ = writeln!(
            out,
            "\n## Store phases (cumulative over the scenario's process, warmup included)\n"
        );
        let _ = writeln!(
            out,
            "| Scenario | Operation | Calls | Acquire wait mean | Acquire wait p95 ≤ | Acquire wait max | Query mean | Query p95 ≤ | Query max | Outcomes |"
        );
        let _ = writeln!(
            out,
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"
        );
        for scenario in &self.scenarios {
            for (operation, evidence) in &scenario.store_evidence.operations {
                let query = evidence.query_duration.as_ref();
                let _ = writeln!(
                    out,
                    "| `{}` | `{}` | {} | {} | {} | {} | {} | {} | {} | {} |",
                    scenario.id,
                    operation,
                    evidence.calls,
                    ms(evidence.acquire_wait.mean_ms),
                    ceiling(evidence.acquire_wait.p95_le_ms),
                    ceiling(evidence.acquire_wait.max_ms),
                    query.map_or_else(|| "—".to_owned(), |q| ms(q.mean_ms)),
                    query.map_or_else(|| "—".to_owned(), |q| ceiling(q.p95_le_ms)),
                    query.map_or_else(|| "—".to_owned(), |q| ceiling(q.max_ms)),
                    evidence
                        .outcomes
                        .iter()
                        .map(|(outcome, count)| format!("{outcome}={count}"))
                        .collect::<Vec<_>>()
                        .join(" "),
                );
            }
        }

        let _ = writeln!(out, "\n## Background index queue and connections\n");
        let _ = writeln!(
            out,
            "| Scenario | Connections opened | Index enqueues | Index queue max depth | Index wait mean | Index wait max |"
        );
        let _ = writeln!(out, "| --- | --- | --- | --- | --- | --- |");
        for scenario in &self.scenarios {
            let queue = scenario.store_evidence.index_queue.as_ref();
            let _ = writeln!(
                out,
                "| `{}` | {} | {} | {} | {} | {} |",
                scenario.id,
                scenario.store_evidence.connections_opened,
                queue.map_or_else(|| "—".to_owned(), |q| q.depth_observations.to_string()),
                queue.map_or_else(
                    || "—".to_owned(),
                    |q| q
                        .max_depth
                        .map_or_else(|| "—".to_owned(), |d| format!("{d:.0}"))
                ),
                queue.map_or_else(|| "—".to_owned(), |q| ms(q.wait.mean_ms)),
                queue.map_or_else(|| "—".to_owned(), |q| ceiling(q.wait.max_ms)),
            );
        }
        out
    }
}

fn ms(value: f64) -> String {
    if value.abs() < 1.0 {
        format!("{value:.3} ms")
    } else {
        format!("{value:.1} ms")
    }
}

fn ceiling(value: Option<f64>) -> String {
    value.map_or_else(|| "> top bucket".to_owned(), ms)
}

/// One line per scenario for a runner's log.
pub fn scenario_summary(result: &ScenarioResult) -> String {
    let median = median_repetition(&result.repetitions);
    let resolve = result
        .store_evidence
        .operations
        .get("namespace_resolve")
        .map_or_else(
            || "n/a".to_owned(),
            |op| {
                format!(
                    "resolve acquire {:.3} ms / query {:.3} ms mean",
                    op.acquire_wait.mean_ms,
                    op.query_duration.as_ref().map_or(0.0, |q| q.mean_ms)
                )
            },
        );
    format!(
        "{} [{}]: {} accepted / {} offered ({:.0} req/s), p50 {:.1} ms p95 {:.1} ms p99 {:.1} ms, \
         overhead p50 {}, {resolve}, connections {}",
        result.id,
        result.store.backend,
        median.accepted,
        median.offered,
        median.accepted_rps,
        median.latency_ms.p50,
        median.latency_ms.p95,
        median.latency_ms.p99,
        result
            .overhead_ms
            .map_or_else(|| "n/a".to_owned(), |o| format!("{:.2} ms", o.p50)),
        result.store_evidence.connections_opened,
    )
}

/// Whether `value` is a JSON artifact this harness wrote, for a reader
/// checking a retained file before comparing it.
pub fn is_baseline_artifact(value: &Value) -> bool {
    value.get("schema_version").and_then(Value::as_u64) == Some(u64::from(RESULT_SCHEMA_VERSION))
        && value
            .get("run")
            .and_then(|run| run.get("harness"))
            .and_then(Value::as_str)
            == Some("axond store latency baseline")
}

/// Whether `path` names a file inside the artifact directory: a guard for a
/// caller that cleans up.
pub fn inside_artifact_dir(path: &Path) -> bool {
    path.starts_with(BaselineResult::artifact_dir())
}
