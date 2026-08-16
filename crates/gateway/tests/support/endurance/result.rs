//! The endurance evidence schema: what a soak writes down, and what a reader
//! needs to accept or reject a comparison.
//!
//! The provenance block is the capacity harness's, unchanged and deliberately
//! so — binary, config, manifest, fixtures, hardware, toolchain, and source
//! (ADR 0033). What endurance adds is the axis capacity does not have: a
//! time series, per-segment summaries, the trend fitted through them, and a
//! reconciliation of every usage record the run should have settled against
//! every record it saw.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Serialize;

use super::manifest::{Ending, Profile, Scale, Thresholds, Tier};
use crate::support::capacity::manifest::workspace_root;
use crate::support::capacity::result::{Environment, Percentiles, Verdict};

#[derive(Debug, Clone, Serialize)]
pub struct EnduranceResult {
    pub schema_version: u32,
    pub profile: ProfileEcho,
    pub run: RunMeta,
    pub environment: Environment,
    pub workload: Workload,
    pub throughput: Throughput,
    pub latency_ms: Distribution,
    pub ttft_ms: Distribution,
    pub stream_lifetime_ms: Distribution,
    pub resources: Resources,
    pub occupancy: Occupancy,
    /// One entry per closed segment: the time series, summarised at the
    /// resolution the manifest asked for.
    pub segments: Vec<Segment>,
    pub trend: Trend,
    pub reconciliation: Reconciliation,
    pub upstream: Upstream,
    pub verdicts: Vec<Verdict>,
}

impl EnduranceResult {
    pub fn failures(&self) -> Vec<&Verdict> {
        self.verdicts.iter().filter(|v| !v.passed).collect()
    }

    /// Where the artifacts for this profile and tier live.
    pub fn directory(tier: Tier) -> PathBuf {
        workspace_root()
            .join("target/endurance")
            .join(tier.as_str())
    }

    /// Write the artifact under `target/endurance/<tier>/<profile>.json` and
    /// return where it landed. The raw samples are written separately, as they
    /// are produced (see [`super::sampler`]).
    pub fn write(&self) -> PathBuf {
        self.write_as(&self.profile.id)
    }

    /// Write the artifact under a stem of its own. A run that is not the tier's
    /// qualifying one — a regression that offers both tiers in seconds — has to
    /// leave its evidence somewhere that is not where a reader looks for the
    /// run that qualified the release.
    pub fn write_as(&self, stem: &str) -> PathBuf {
        let dir = Self::directory(self.profile.tier_enum());
        std::fs::create_dir_all(&dir).expect("the endurance artifact directory is writable");
        let path = dir.join(format!("{stem}.json"));
        let json = serde_json::to_string_pretty(self).expect("the result artifact serializes");
        std::fs::write(&path, format!("{json}\n")).expect("the endurance artifact is writable");
        path
    }

    /// A one-line human summary, for a runner's log.
    pub fn summary(&self) -> String {
        let rss = self
            .resources
            .rss_kib
            .map_or_else(|| "n/a".to_owned(), |span| format!("{} KiB", span.peak));
        format!(
            "{} [{}]: {} offered over {:.1} min ({:.1} req/s), {} segments, \
             p50 {:.1} ms p95 {:.1} ms, ttft p95 {}, peak rss {rss}, \
             rss drift {:.0} KiB/h, sockets drift {:.2}/h, \
             usage {}/{} ({} missing, {} surplus, {} duplicate, {} unexpected status)",
            self.profile.id,
            self.profile.tier,
            self.throughput.offered,
            self.run.elapsed_ms as f64 / 60_000.0,
            self.throughput.offered_rps,
            self.segments.len(),
            self.latency_ms.percentiles.map_or(f64::NAN, |p| p.p50),
            self.latency_ms.percentiles.map_or(f64::NAN, |p| p.p95),
            self.ttft_ms
                .percentiles
                .map_or_else(|| "n/a".to_owned(), |p| format!("{:.1} ms", p.p95)),
            self.trend.rss_kib_per_hour.unwrap_or_default(),
            self.trend.sockets_per_hour.unwrap_or_default(),
            self.reconciliation.records_observed,
            self.reconciliation.expected,
            self.reconciliation.missing,
            self.reconciliation.unexpected_records,
            self.reconciliation.duplicates,
            self.reconciliation.unexpected_statuses,
        )
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ProfileEcho {
    pub id: String,
    pub description: String,
    pub tier: String,
    pub seed: u64,
    /// How long this run was offered for, which is the manifest's tier unless
    /// it was dispatched shorter. The segment length beside it is fitted to the
    /// same number, so the two cannot disagree about which run this was.
    pub duration_ms: u64,
    /// What the manifest commits the tier to, kept so a dispatched artifact
    /// still says what it is a shorter run *of*.
    pub manifest_duration_ms: u64,
    pub concurrency: usize,
    pub think_time_ms: u64,
    pub sample_interval_ms: u64,
    pub segment_ms: u64,
    /// The committed mix, as offered proportions of one rotation cycle.
    pub mix: BTreeMap<String, usize>,
    pub thresholds: Thresholds,
}

impl ProfileEcho {
    pub fn new(profile: &Profile, tier: Tier, scale: &Scale) -> Self {
        Self {
            id: profile.id.clone(),
            description: profile.description.clone(),
            tier: tier.as_str().to_owned(),
            seed: profile.seed,
            duration_ms: scale.duration_ms,
            manifest_duration_ms: profile.scale(tier).duration_ms,
            concurrency: scale.concurrency,
            think_time_ms: scale.think_time_ms,
            sample_interval_ms: scale.sample_interval_ms,
            segment_ms: scale.segment_ms,
            mix: Ending::ALL
                .iter()
                .map(|&ending| (ending.as_str().to_owned(), profile.mix.weight(ending)))
                .collect(),
            thresholds: scale.thresholds,
        }
    }

    fn tier_enum(&self) -> Tier {
        if self.tier == Tier::Soak.as_str() {
            Tier::Soak
        } else {
            Tier::Smoke
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct RunMeta {
    pub started_at_unix_ms: u128,
    pub elapsed_ms: u128,
    /// What the run was asked to last, which is not always what the manifest
    /// says: an operator can dispatch a shorter or longer soak, and an artifact
    /// that hides that is not evidence of the duration it claims.
    pub requested_duration_ms: u64,
    pub duration_source: &'static str,
    pub harness: &'static str,
    pub harness_version: &'static str,
    /// Where the raw time series landed, relative to the workspace root.
    pub samples_path: String,
    /// How long a finished attempt or an emitted usage record may sit in the
    /// driver before it is folded into the open segment and released.
    pub drain_interval_ms: u64,
    /// How many times that happened. A number far larger than the segment
    /// count is the evidence that what the driver holds is bounded by the
    /// drain tick rather than by the segment length.
    pub drains: u64,
}

impl RunMeta {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        started_at: SystemTime,
        elapsed: Duration,
        requested_duration_ms: u64,
        duration_source: &'static str,
        samples_path: String,
        drain_interval_ms: u64,
        drains: u64,
    ) -> Self {
        Self {
            started_at_unix_ms: started_at
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
            elapsed_ms: elapsed.as_millis(),
            requested_duration_ms,
            duration_source,
            harness: "axond endurance harness",
            harness_version: env!("CARGO_PKG_VERSION"),
            samples_path,
            drain_interval_ms,
            drains,
        }
    }
}

/// What was actually offered, along every axis the plan mixes over. A soak that
/// believes it covered three tenants and covered one is the failure this block
/// makes visible.
#[derive(Debug, Clone, Serialize)]
pub struct Workload {
    pub by_tenant: BTreeMap<String, u64>,
    pub by_provider: BTreeMap<String, u64>,
    pub by_alias: BTreeMap<String, u64>,
    pub by_route: BTreeMap<String, u64>,
    pub by_ending: BTreeMap<String, u64>,
    pub streamed: u64,
    pub buffered: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Throughput {
    pub offered: u64,
    /// Requests whose plan says they succeed, and did.
    pub accepted: u64,
    /// Requests the plan asked to fail, and which failed as planned.
    pub planned_faults: u64,
    /// Planned faults answered from an open circuit instead of an upstream
    /// attempt. A target that fails every request trips its breaker and keeps
    /// it tripped, so most of a soak's faults are shed rather than dispatched
    /// — that is the design, and it settles no usage record because it spent
    /// nothing.
    pub circuit_shed: u64,
    /// Failures the plan did not ask for: the ones that make a run a finding.
    pub unplanned_errors: u64,
    pub cancelled: u64,
    pub rejected: u64,
    /// What the callers were answered with, by HTTP status. Streams that were
    /// torn after their headers count under the status they were opened with,
    /// and requests that never got a response are counted as `transport`.
    pub by_response_status: BTreeMap<String, u64>,
    pub elapsed_ms: u128,
    pub offered_rps: f64,
    /// Closed-loop with think time: the driver holds `concurrency` workers that
    /// pause between requests, so the rate is a result of service time and the
    /// committed think time rather than an arrival rate pushed at the replica.
    pub closed_loop: bool,
}

/// A distribution over a run long enough that keeping every observation would
/// be a memory leak in the harness. Values are retained by decimation — every
/// value while under the cap, then every second, then every fourth — so the
/// percentiles are over a uniform sample of the whole run rather than over its
/// first minutes. `observed` is exact; `retained` says how much of it the
/// percentiles were computed from.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct Distribution {
    pub observed: u64,
    pub retained: usize,
    pub stride: u64,
    pub percentiles: Option<Percentiles>,
}

/// Resident memory, descriptors, sockets, and CPU, sampled from outside the
/// process for the length of the run.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct Resources {
    pub sampled: bool,
    pub procfs: bool,
    pub samples: u64,
    pub sample_interval_ms: u64,
    pub rss_kib: Option<Span>,
    pub sockets: Option<Span>,
    pub fds: Option<Span>,
    pub cpu_seconds: Option<f64>,
    pub cpu_utilization: Option<f64>,
    pub user_hz: f64,
}

/// A quantity's baseline, peak, and settled value. Settled is sampled after the
/// load has stopped and the process has been given time to release what it
/// held: for endurance that reading, not the peak, is the interesting one.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct Span {
    pub baseline: u64,
    pub peak: u64,
    pub settled: u64,
}

impl Span {
    pub fn growth(&self) -> u64 {
        self.peak.max(self.settled).saturating_sub(self.baseline)
    }

    /// What the process kept once it was idle again, over what it started with.
    pub fn settled_excess(&self) -> u64 {
        self.settled.saturating_sub(self.baseline)
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct Occupancy {
    pub offered_concurrency: usize,
    pub in_flight_peak: u64,
    /// The driver-side view of the queue the replica is holding: requests
    /// waiting for the first byte of their *answer*.
    pub awaiting_first_byte_peak: u64,
    pub admission_queue_capacity: u64,
}

/// One closed slice of the run. Percentiles here are exact: a segment's
/// observations are bounded by its length, so nothing is decimated.
#[derive(Debug, Clone, Serialize)]
pub struct Segment {
    pub index: usize,
    /// Whether load was still being offered for the whole segment. The last
    /// segment of a run is not: it spans the settle wait, the upstream cleanup
    /// wait, and the quiesce, and is an idle reading of a process that has
    /// stopped serving. It is recorded, because what the process gives back
    /// when it goes idle is worth reading, and it is kept out of the trend and
    /// the segment count, because it is not a sample of the run's steady state.
    pub under_load: bool,
    pub started_ms: u128,
    pub elapsed_ms: u128,
    pub offered: u64,
    pub accepted: u64,
    pub unplanned_errors: u64,
    pub offered_rps: f64,
    pub latency_ms: Option<Percentiles>,
    pub ttft_ms: Option<Percentiles>,
    pub usage_records: u64,
    pub samples: u64,
    /// The medians the trend is fitted through — a median rather than a peak,
    /// because one transient spike is not drift.
    pub rss_kib_median: Option<u64>,
    pub rss_kib_peak: Option<u64>,
    pub sockets_median: Option<u64>,
    pub sockets_peak: Option<u64>,
    pub fds_median: Option<u64>,
    pub fds_peak: Option<u64>,
    pub cpu_seconds: Option<f64>,
    pub cpu_utilization: Option<f64>,
    pub in_flight_peak: u64,
    pub awaiting_first_byte_peak: u64,
}

impl Segment {
    /// A segment carrying only what the trend is fitted through, for asserting
    /// on the fit without offering hours of load to produce one.
    pub fn fitted_through(
        index: usize,
        under_load: bool,
        started_ms: u128,
        elapsed_ms: u128,
        rss_kib: u64,
    ) -> Self {
        Self {
            index,
            under_load,
            started_ms,
            elapsed_ms,
            offered: 0,
            accepted: 0,
            unplanned_errors: 0,
            offered_rps: 0.0,
            latency_ms: None,
            ttft_ms: None,
            usage_records: 0,
            samples: 1,
            rss_kib_median: Some(rss_kib),
            rss_kib_peak: Some(rss_kib),
            sockets_median: None,
            sockets_peak: None,
            fds_median: None,
            fds_peak: None,
            cpu_seconds: None,
            cpu_utilization: None,
            in_flight_peak: 0,
            awaiting_first_byte_peak: 0,
        }
    }
}

/// Least-squares slopes through the per-segment medians, in units per hour.
/// A replica that ends where it started has a slope near zero whatever its
/// peaks were, which is the property "no unbounded growth" actually names.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct Trend {
    pub segments: usize,
    /// Whether the run produced enough segments over enough time for a
    /// per-hour slope to mean anything. When false the slopes are still
    /// recorded, and the drift thresholds are not evaluated.
    pub fitted: bool,
    pub rss_kib_per_hour: Option<f64>,
    pub sockets_per_hour: Option<f64>,
    pub fds_per_hour: Option<f64>,
    /// The blunt version of the same question, for a reader: the median of the
    /// first quarter of the segments against the median of the last quarter.
    pub first_quarter_rss_kib: Option<u64>,
    pub last_quarter_rss_kib: Option<u64>,
}

/// Usage accounting, reconciled both ways. Loss is the failure a throughput
/// number hides; duplication is the failure a *count* hides, and it is only
/// visible because every record carries a globally unique `request_id`
/// (see docs/usage-schema.md).
#[derive(Debug, Clone, Serialize)]
pub struct Reconciliation {
    /// One per offered request: every terminated request settles a record,
    /// including the ones the plan faulted.
    pub expected: u64,
    pub records_observed: u64,
    pub distinct_request_ids: u64,
    /// Records sharing a `request_id` with an earlier record.
    pub duplicates: u64,
    /// Expected records that never arrived within the settle deadline.
    pub missing: u64,
    /// Distinct records beyond the requests known to owe one. This is kept
    /// separate from duplicates: an extra identity is surplus accounting, not
    /// a replay of an identity the run already observed.
    pub unexpected_records: u64,
    /// Records whose status no planned ending can produce.
    pub unexpected_statuses: u64,
    /// Records that arrived without a parseable `request_id`, which would make
    /// the duplicate count meaningless if they were ignored.
    pub unidentified: u64,
    pub by_status: BTreeMap<String, u64>,
    pub by_namespace: BTreeMap<String, u64>,
    pub by_credential_source: BTreeMap<String, u64>,
    /// What the plan said each ending would settle, and how many of each it
    /// offered: the expectation the observed statuses are read against.
    pub planned_status_counts: BTreeMap<String, u64>,
    /// How the duplicate count above was arrived at.
    pub fingerprints: Fingerprints,
}

/// The identity ledger duplicate detection was performed against. A run that
/// reports no duplicates is worth what the method that looked for them is
/// worth, so the method is part of the evidence: how many identities were
/// compared, whether the comparison was exact, and how many of them the driver
/// held in memory at once while comparing.
#[derive(Debug, Clone, Serialize)]
pub struct Fingerprints {
    /// Identified usage records, duplicates included.
    pub recorded: u64,
    /// How many files the identities were spilled across. Equal identities
    /// always share a shard, which is what keeps the sharded count exact.
    pub shards: usize,
    /// The largest shard, and so the most identities held at once: the bound a
    /// whole-run in-memory set did not have.
    pub peak_shard_fingerprints: u64,
    /// Whether every identity was compared against every identity it could
    /// equal. False would mean the count is a lower bound.
    pub exact: bool,
    /// Where the identities were spilled, relative to the workspace root.
    pub path: String,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct Upstream {
    pub requests: u64,
    pub streams_opened: u64,
    /// Upstream response bodies still open once every client is gone: a leak.
    pub streams_open_at_end: i64,
}
