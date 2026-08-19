//! The stateful endurance evidence schema: what a run writes down, and what a
//! reader needs to accept or reject it.
//!
//! The provenance block is the capacity harness's, unchanged and deliberately
//! so — binary, config, manifest, fixtures, hardware, toolchain, source
//! (ADR 0033). What this slice adds is the part of a stateful deployment that a
//! stateless soak cannot see: which revisions converged and how quickly, what
//! the durable sink actually holds, whether a tenant ever reached past its own
//! boundary, and what a rolling restart cost the callers who were mid-request
//! when it happened.
//!
//! Nothing here carries a secret. The durable backend is named by kind, version
//! and schema; credentials appear as labels, which is what they are on a usage
//! record; and the config the run was booted from is recorded with its
//! per-process paths and ports normalised out.

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::Serialize;

use super::durable::{Counts, Reach};
use super::gate::GateCounts;
use super::manifest::{Schedule, Slo, Stop, Termination, Tier};
use crate::support::capacity::result::{Environment, Verdict};
use crate::support::endurance::result::Distribution;
use crate::support::endurance::result::{CorrelationEvidence, IdentityEvidence};

#[derive(Debug, Clone, Serialize)]
pub struct StatefulEnduranceResult {
    pub schema_version: u32,
    pub profile: ProfileEcho,
    pub run: RunMeta,
    pub environment: Environment,
    pub backends: Backends,
    pub workload: Workload,
    pub latency_ms: Distribution,
    pub ttft_ms: Distribution,
    pub segments: Vec<Segment>,
    pub resources: Vec<ReplicaResources>,
    pub trend: Trend,
    pub revisions: Vec<RevisionObservation>,
    pub faults: Vec<FaultWindow>,
    pub restart: Restart,
    pub tenancy: Tenancy,
    pub usage: Usage,
    pub telemetry: Telemetry,
    pub timeline: Vec<TimelineEntry>,
    pub verdicts: Vec<Verdict>,
}

impl StatefulEnduranceResult {
    pub fn failures(&self) -> Vec<&Verdict> {
        self.verdicts
            .iter()
            .filter(|verdict| !verdict.passed)
            .collect()
    }

    pub fn write(&self, stem: &str) -> PathBuf {
        let dir = super::fleet::artifact_dir(&self.profile.tier);
        let path = dir.join(format!("{stem}.json"));
        let json = serde_json::to_string_pretty(self).expect("the result artifact serializes");
        std::fs::write(&path, format!("{json}\n"))
            .expect("the stateful endurance artifact is writable");
        path
    }

    /// A one-line human summary, for a runner's log.
    pub fn summary(&self) -> String {
        format!(
            "{} [{}]: {} offered over {:.1} min across {} replicas, stopped {:?}; \
             revisions {}/{} converged, usage {} emitted / {} durable ({} lost outside windows), \
             restarts {} ({} unavailable), boundary violations {}, unplanned errors {}",
            self.profile.id,
            self.profile.tier,
            self.workload.offered,
            self.run.elapsed_ms as f64 / 60_000.0,
            self.run.replicas_booted,
            self.run.stop,
            self.revisions
                .iter()
                .filter(|r| r.converged_ms.is_some())
                .count(),
            self.revisions.len(),
            self.usage.emitted,
            self.usage.durable.distinct,
            self.usage.durable_loss_outside_windows,
            self.restart.replicas_restarted,
            self.restart.unavailable,
            self.tenancy.violations,
            self.workload.unplanned,
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
    /// it was dispatched shorter.
    pub duration_ms: u64,
    /// What the manifest commits the tier to, so a dispatched artifact still
    /// says what it is a shorter run *of*.
    pub manifest_duration_ms: u64,
    pub concurrency: usize,
    pub think_time_ms: u64,
    pub sample_interval_ms: u64,
    pub segment_ms: u64,
    pub mix: BTreeMap<String, usize>,
    pub schedule: Schedule,
    pub slo: Slo,
    pub termination: Termination,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunMeta {
    pub started_at_unix_ms: u64,
    pub elapsed_ms: u64,
    /// Why the run stopped, and what it was: an abandoned run is not a shorter
    /// passing one.
    pub stop: Stop,
    pub stop_detail: Option<String>,
    /// `manifest` or `environment`, so a dispatched duration is never mistaken
    /// for the committed one.
    pub duration_source: &'static str,
    pub settle_ms: u64,
    pub replicas_booted: usize,
    pub drain_interval_ms: u64,
    /// Where the raw resource samples were written, relative to the workspace.
    pub samples_paths: Vec<String>,
}

/// What the run was served by. Named and versioned, never addressed.
#[derive(Debug, Clone, Serialize)]
pub struct Backends {
    pub usage_sink: &'static str,
    pub usage_backend_version: String,
    /// The schema this run created for itself, dropped when it ended.
    pub usage_schema: String,
    pub usage_reach: Reach,
    pub upstream: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct Workload {
    pub offered: u64,
    pub completed: u64,
    pub cancelled: u64,
    pub dropped: u64,
    pub faulted: u64,
    /// Planned faults an open circuit refused before dispatch. The gateway
    /// working, and owing no accounting because it spent nothing.
    pub shed: u64,
    /// Admission refusals. Never planned here.
    pub rejected: u64,
    /// Everything else, outside every declared fault window.
    pub unplanned: u64,
    /// Those failures by what they said. A count on its own says a run failed;
    /// this says what it failed with, which is what a reader of the artifact
    /// needs before they can act on it.
    pub unplanned_by_reason: BTreeMap<String, u64>,
    /// Errors inside a declared fault window, which are the point of the
    /// window rather than a finding.
    pub errors_in_fault_windows: u64,
    pub by_tenant: BTreeMap<String, u64>,
    pub by_ending: BTreeMap<String, u64>,
    pub streamed: u64,
    pub buffered: u64,
}

/// One closed segment of the run.
#[derive(Debug, Clone, Serialize)]
pub struct Segment {
    pub index: usize,
    pub started_ms: u64,
    pub ended_ms: u64,
    pub offered: u64,
    pub unplanned: u64,
    pub usage_records: u64,
    /// The fleet's resident total at the end of the segment, when procfs was
    /// available.
    pub rss_kib: Option<u64>,
}

/// One replica process' resource story. Per incarnation rather than per
/// replica: a restarted replica is a new process, and a growth figure that
/// spans a restart measures the restart.
#[derive(Debug, Clone, Serialize)]
pub struct ReplicaResources {
    pub replica: String,
    pub sampled: bool,
    pub samples: u64,
    pub baseline_rss_kib: Option<u64>,
    pub peak_rss_kib: Option<u64>,
    pub final_rss_kib: Option<u64>,
    pub growth_kib: Option<i64>,
    pub peak_open_fds: Option<u64>,
    pub final_open_fds: Option<u64>,
    pub peak_sockets: Option<u64>,
    pub final_sockets: Option<u64>,
    pub cpu_seconds: Option<f64>,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct Trend {
    /// Fitted through the per-segment fleet totals. `None` on a run too short
    /// for a per-hour slope to mean anything, which is stated rather than
    /// silently passed.
    pub rss_kib_per_hour: Option<f64>,
    pub evaluated: bool,
    pub segments: usize,
}

/// A revision published under load, and what it cost to become the one
/// serving.
#[derive(Debug, Clone, Serialize)]
pub struct RevisionObservation {
    pub event: String,
    pub revision: String,
    pub published_at_ms: u64,
    /// How long until a caller observed the new revision. `None` means it never
    /// was, which is a failure rather than a missing measurement.
    pub converged_ms: Option<u64>,
    /// What the caller observed that proved it: the alias that began serving,
    /// the credential label that began appearing, the tenant that began being
    /// refused.
    pub observed: String,
}

/// A declared fault window, and what met it.
#[derive(Debug, Clone, Serialize)]
pub struct FaultWindow {
    pub event: String,
    pub opened_ms: u64,
    pub closed_ms: u64,
    pub errors_inside: u64,
    /// How long after the window closed before a caller was served again.
    pub recovered_ms: Option<u64>,
    pub gate: GateCounts,
}

/// What the rolling restart cost.
#[derive(Debug, Clone, Serialize)]
pub struct Restart {
    pub replicas_restarted: usize,
    /// Requests the balancer could not place because no replica was in
    /// rotation. The failure a rolling restart exists to avoid.
    pub unavailable: u64,
    /// The worst time from `SIGTERM` to a replacement answering `/readyz`.
    pub worst_return_ms: u64,
    /// Whether every retiring replica exited inside the bound its own config
    /// advertises.
    pub all_exits_bounded: bool,
    pub all_exits_clean: bool,
    /// Usage records flushed by replicas on their way out. Evidence that the
    /// accounting of a replaced replica was not lost with it.
    pub flushed_on_exit: u64,
    /// Requests offered after the last replacement joined the rotation. A
    /// restart the load finished before is a restart nothing was measured
    /// across, so `unavailable = 0` would be satisfied by an idle deployment.
    pub offered_after_last_replacement: u64,
    /// How much longer than its requested duration the run offered load, so
    /// the restart had a workload behind it. Zero on a run whose schedule left
    /// the room by itself, which is every run of the soak tier.
    pub extended_for_load_ms: u64,
}

/// Whether a tenant ever reached past its own boundary.
#[derive(Debug, Clone, Serialize)]
pub struct Tenancy {
    pub probes: u64,
    pub violations: u64,
    /// What each violation was, bounded: the first few are diagnosis, and a
    /// run that produced thousands has already failed.
    pub examples: Vec<String>,
    /// The probe tenant's requests before and after its policy revision.
    pub probe_served_before_policy: u64,
    pub probe_refused_after_policy: u64,
    pub probe_served_after_policy: u64,
    /// Usage records whose namespace or credential label belonged to another
    /// tenant.
    pub misattributed_records: u64,
}

/// The accounting, reconciled against both what was dispatched and what the
/// database holds.
#[derive(Debug, Clone, Serialize)]
pub struct Usage {
    /// Requests that reached an upstream attempt and therefore owe exactly one
    /// record.
    pub owed: u64,
    pub emitted: u64,
    /// Distinct records the *workload* settled, which is what `owed` is
    /// reconciled against.
    pub distinct: u64,
    /// Distinct records the driver's own boundary and convergence probes
    /// settled. Reported so the database's row count adds up, and kept out of
    /// [`Self::distinct`] so a probe record cannot stand in for a workload
    /// record the deployment lost.
    pub probe_distinct: u64,
    /// Duplicate request IDs across all emitted rows. Taken from the single
    /// durable-expected identity set so workload/probe cross-collisions count.
    pub duplicates: u64,
    pub missing: u64,
    /// Usage rows whose trace identity was not among the exact workload or
    /// successful probe requests that owed a settlement.
    pub unexpected_records: u64,
    pub unexpected_statuses: u64,
    /// Exact code-4 expectations whose cancellation lifetime touched the raw
    /// upstream outage or its committed leading observer slack. Promotion
    /// re-derives their complete membership from retained request timings.
    pub concurrent_endings: u64,
    /// Symmetric difference between the expected correlation rows and the
    /// rows independently derived from original endings and request timings.
    pub concurrent_ending_membership_mismatches: u64,
    pub unidentified: u64,
    pub uncorrelated: u64,
    /// Refusal settlements. The qualification contract requires zero because
    /// no refusal response is owed a usage row; any such row is surplus.
    pub refusal_records: u64,
    pub by_status: BTreeMap<String, u64>,
    pub request_identities: IdentityEvidence,
    pub correlations: CorrelationEvidence,
    pub correlation_windows: IdentityEvidence,
    pub durable: Counts,
    /// How long the durable table took to stop growing after the load stopped.
    pub durable_lag_ms: u64,
    pub durable_settled: bool,
    /// Records the run emitted that never reached the database, whether or not
    /// a declared outage excuses them. Kept beside the two halves below so a
    /// reader can see what was excused rather than only what remained.
    pub durable_loss_total: u64,
    /// Records emitted outside every declared fault window that never reached
    /// the database. The gate: a sink may drop a batch while its backend is
    /// gone (ADR 0009), and may not lose a record at any other time. The
    /// exact request-ID set difference outside the widened window, so a loss is
    /// excused by *when* it happened rather than by unrelated rows preserving
    /// the same cardinality.
    pub durable_loss_outside_windows: u64,
    /// Records emitted during the usage-backend outage that never reached the
    /// database. Gated against the bounded sink-drop evidence emitted while the
    /// backend was unavailable.
    pub durable_loss_in_window: u64,
    /// Distinct records the processes settled outside the widened usage-outage
    /// window, by the driver's clock.
    pub settled_outside_usage_window: u64,
    /// Distinct rows the database holds outside that window, by the gateway's
    /// own `recorded_at`. The other side of the same comparison.
    pub durable_outside_usage_window: u64,
    /// Rows the database holds twice for one request. Documented behaviour of a
    /// retried batch, so reported rather than gated.
    pub durable_duplicate_rows: u64,
    pub durable_unexpected_rows: u64,
    pub durable_identities: DurableIdentityEvidence,
    /// Exact proof of outside-window loss. The expected side is every record
    /// emitted outside the widened outage window; the observed side is every
    /// durable row, wherever its independently stamped `recorded_at` falls.
    /// Therefore `missing` is exactly `emitted outside - durable anywhere`.
    /// `unexpected` is the durable in-window population and is diagnostic, not
    /// surplus durable data (that is gated by [`Self::durable_identities`]).
    pub durable_outside_identities: DurableIdentityEvidence,
    /// What the processes themselves said they dropped, which is how a missing
    /// row is attributed to a declared outage rather than inferred from two
    /// clocks that disagree by a drain tick.
    pub sink_drops: SinkDrops,
}

#[derive(Debug, Clone, Serialize)]
pub struct DurableIdentityEvidence {
    pub expected_rows: u64,
    pub observed_rows: u64,
    pub expected_distinct: u64,
    pub observed_distinct: u64,
    pub expected_duplicates: u64,
    pub observed_duplicates: u64,
    pub missing: u64,
    pub unexpected: u64,
    pub shards: usize,
    pub peak_shard_rows: u64,
    pub exact: bool,
    pub path: String,
}

/// The fleet's own account of usage batches that never reached a sink.
#[derive(Debug, Clone, Default, Serialize)]
pub struct SinkDrops {
    pub reports: u64,
    pub records: u64,
    pub by_reason: BTreeMap<String, u64>,
    /// Records dropped while the usage backend was declared out — the loss the
    /// contract allows (ADR 0009).
    pub records_in_usage_window: u64,
    /// How many of those records were reported by a sampled report — the
    /// buffer-full one, which the gateway writes at the first drop and then
    /// every thousandth. A run whose in-window loss was reported that way can
    /// legitimately have lost up to one interval more than it said, and the
    /// gate on the excused half allows exactly that much and no more.
    pub sampled_records_in_usage_window: u64,
    /// Records dropped at any other time, each of which is a finding.
    pub records_outside_windows: u64,
    /// The first few reports, in full, so a failing run says which sink lost
    /// what and when without needing the process's scrollback.
    pub examples: Vec<SinkDrop>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SinkDrop {
    pub at_ms: u64,
    pub sink: String,
    pub reason: String,
    pub records: u64,
}

/// What the run could observe about the deployment while it served.
#[derive(Debug, Clone, Serialize)]
pub struct Telemetry {
    pub readiness_probes: u64,
    pub readiness_failures: u64,
    /// The longest stretch in which no replica answered `/readyz`.
    pub worst_readiness_gap_ms: u64,
    /// The longest stretch in which the fleet emitted no usage record while
    /// load was being offered and no backend was out.
    pub worst_usage_silence_ms: u64,
    /// Explicitly not evaluated: a collector is a boot-time dependency this
    /// harness does not stand up, so the OTLP export path is out of scope here
    /// rather than quietly passing.
    pub otlp_export_evaluated: bool,
}

/// One thing that happened, in the order it happened.
#[derive(Debug, Clone, Serialize)]
pub struct TimelineEntry {
    pub at_ms: u64,
    pub event: String,
    pub detail: String,
}

/// The tier an artifact belongs to, as a directory name.
pub fn tier_dir(tier: Tier) -> String {
    tier.as_str().to_owned()
}
