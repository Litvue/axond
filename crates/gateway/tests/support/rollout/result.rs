//! The machine-readable rollout artifact.
//!
//! One JSON document per scenario run, carrying the traffic, the drains, the
//! loss ledger, the migration evidence, the rollback decisions, and a timeline
//! of every event in the order it happened — plus the exact inputs that produced
//! them. The provenance types are the capacity harness' (ADR 0033): a rollout
//! artifact and a capacity artifact from the same commit describe the same
//! binary, and they say so with the same hash.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Serialize;

use crate::support::capacity::manifest::{sha256_hex, workspace_root};
use crate::support::capacity::result::{
    BinaryMeta, ConfigMeta, Hardware, InputMeta, Percentiles, Source, Toolchain, Verdict,
};

use super::manifest::{self, Scenario, Thresholds, Tier};

#[derive(Debug, Clone, Serialize)]
pub struct RolloutResult {
    pub schema_version: u32,
    pub scenario: ScenarioEcho,
    pub run: RunMeta,
    pub environment: Environment,
    /// The builds that served, with the artifact identity of each.
    pub revisions: Vec<RevisionMeta>,
    pub fleet: Vec<ReplicaRecord>,
    pub traffic: Vec<PhaseTraffic>,
    pub drains: Vec<DrainRecord>,
    pub mixed_version: MixedVersion,
    pub loss: LossLedger,
    pub capacity: CapacityEnvelope,
    pub migration: MigrationEvidence,
    pub rollback: RollbackEvidence,
    pub timeline: Vec<Event>,
    pub verdicts: Vec<Verdict>,
}

impl RolloutResult {
    pub fn failures(&self) -> Vec<&Verdict> {
        self.verdicts
            .iter()
            .filter(|verdict| !verdict.passed)
            .collect()
    }

    /// Write the artifact under `target/rollout/<tier>/<scenario>.json` and
    /// return where it landed.
    pub fn write(&self) -> PathBuf {
        let dir = workspace_root()
            .join("target/rollout")
            .join(&self.scenario.tier);
        std::fs::create_dir_all(&dir).expect("the rollout artifact directory is writable");
        let path = dir.join(format!("{}.json", self.scenario.id));
        let json = serde_json::to_string_pretty(self).expect("the result artifact serializes");
        std::fs::write(&path, format!("{json}\n")).expect("the rollout artifact is writable");
        path
    }

    /// A one-line human summary, for a runner's log.
    pub fn summary(&self) -> String {
        format!(
            "{} [{}]: {} replicas, {} phases, {} offered / {} answered, {} drained \
             (readiness removed in {} ms max, exit {} ms max), mixed-version {}+{}, \
             usage {}/{}, unavailable {}, rollback {}",
            self.scenario.id,
            self.scenario.tier,
            self.scenario.replicas,
            self.traffic.len(),
            self.loss.offered,
            self.loss.answered,
            self.drains.len(),
            self.drains
                .iter()
                .filter_map(|drain| drain.readiness_removed_after_ms)
                .max()
                .unwrap_or_default(),
            self.drains
                .iter()
                .filter_map(|drain| drain.exited_after_ms)
                .max()
                .unwrap_or_default(),
            self.mixed_version.previous_requests,
            self.mixed_version.next_requests,
            self.loss.usage_records_observed,
            self.loss.usage_records_expected,
            self.loss.unavailable,
            self.rollback.compatible_patch_rollback.performed,
        )
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ScenarioEcho {
    pub id: String,
    pub description: String,
    pub tier: String,
    pub replicas: usize,
    pub workers: usize,
    pub requests_per_phase: usize,
    pub stream_every: usize,
    pub shutdown: ShutdownEcho,
    pub thresholds: Thresholds,
}

impl ScenarioEcho {
    pub fn new(scenario: &Scenario, tier: Tier) -> Self {
        let scale = scenario.scale(tier);
        Self {
            id: scenario.id.clone(),
            description: scenario.description.clone(),
            tier: tier.as_str().to_owned(),
            replicas: scenario.replicas,
            workers: scale.workers,
            requests_per_phase: scale.requests_per_phase,
            stream_every: scale.stream_every,
            shutdown: ShutdownEcho {
                drain_grace_ms: scenario.shutdown.drain_grace_ms,
                deadline_ms: scenario.shutdown.deadline_ms,
                flush_timeout_ms: scenario.shutdown.flush_timeout_ms,
                budget_ms: scenario.shutdown.budget().as_millis(),
            },
            thresholds: scenario.thresholds,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct ShutdownEcho {
    pub drain_grace_ms: u64,
    pub deadline_ms: u64,
    pub flush_timeout_ms: u64,
    /// What the process promises termination costs at most.
    pub budget_ms: u128,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunMeta {
    pub started_at_unix_ms: u128,
    pub elapsed_ms: u128,
    pub harness: &'static str,
    pub harness_version: &'static str,
}

impl RunMeta {
    pub fn new(started_at: SystemTime, elapsed: Duration) -> Self {
        Self {
            started_at_unix_ms: started_at
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
            elapsed_ms: elapsed.as_millis(),
            harness: "axond rollout harness",
            harness_version: env!("CARGO_PKG_VERSION"),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Environment {
    pub manifest: InputMeta,
    pub hardware: Hardware,
    pub toolchain: Toolchain,
    pub source: Source,
}

impl Environment {
    pub fn collect(manifest_text: &str) -> Self {
        Self {
            manifest: InputMeta {
                path: manifest::MANIFEST_RELATIVE.to_owned(),
                sha256: sha256_hex(manifest_text.as_bytes()),
            },
            hardware: Hardware::collect(),
            toolchain: Toolchain::collect(),
            source: Source::collect(),
        }
    }
}

/// What a revision *is*, in artifact terms: a binary and the config it was
/// started from, both named by hash.
#[derive(Debug, Clone, Serialize)]
pub struct RevisionMeta {
    pub label: String,
    pub binary: BinaryMeta,
    pub config: ConfigMeta,
    /// Whether the two revisions ran different binaries. A test builds one, so
    /// this is `false` and the artifact says so rather than implying a
    /// cross-build rollout it did not perform: what differs between the
    /// revisions is the served capability set, recorded below.
    pub distinct_binary: bool,
    /// Aliases this revision serves that the other does not.
    pub exclusive_aliases: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReplicaRecord {
    pub id: String,
    pub revision: String,
    /// When the balancer first routed to it, as an offset from the run's start.
    /// Timeline context, not a duration: a late replacement in a long run has a
    /// large offset and a fast admission.
    pub admitted_at_ms: Option<u128>,
    /// How long the replica took to go from added to carrying traffic. The
    /// admission the surge is judged on.
    pub admission_took_ms: Option<u128>,
    pub withdrawn_at_ms: Option<u128>,
    pub requests_served: u64,
    pub requests_after_withdrawal: u64,
    /// Requests it refused mid-drain, which the balancer retried elsewhere.
    pub refusals: u64,
    pub usage_records: u64,
    pub retired: bool,
}

/// One phase's worth of caller traffic through the balancer.
#[derive(Debug, Clone, Serialize)]
pub struct PhaseTraffic {
    pub phase: String,
    pub offered: u64,
    pub answered: u64,
    pub errors: u64,
    pub unanswered: u64,
    /// Streams whose body failed part-way: the answer started and did not
    /// finish.
    pub torn_streams: u64,
    pub streamed: u64,
    pub elapsed_ms: u128,
    pub answered_rps: f64,
    pub latency_ms: Option<Percentiles>,
    pub by_status: BTreeMap<String, u64>,
    pub by_replica: BTreeMap<String, u64>,
    pub by_revision: BTreeMap<String, u64>,
    /// Requests the balancer had to place a second time because the first
    /// replica refused them mid-drain.
    pub retried: u64,
}

/// What happened to one replica between its `SIGTERM` and its exit.
#[derive(Debug, Clone, Serialize)]
pub struct DrainRecord {
    pub replica: String,
    pub revision: String,
    pub signalled_at_ms: u128,
    /// How long after the signal the balancer stopped routing here. `None` means
    /// it never did, inside the bound.
    pub readiness_removed_after_ms: Option<u128>,
    /// How long the process took to exit, and whether it exited cleanly.
    pub exited_after_ms: Option<u128>,
    pub exit_clean: bool,
    /// The bound the process advertises: drain grace, shutdown deadline, and
    /// sink flush. The harness waits longer than this before giving up, so an
    /// exit that overruns is recorded rather than made impossible.
    pub exit_budget_ms: u128,
    /// Requests the balancer sent here after recording the withdrawal.
    pub requests_after_withdrawal: u64,
    /// The buffered request that was already in flight when the signal landed.
    pub buffered_in_flight: InFlight,
    /// The stream that was open, and could not finish, when the signal landed.
    pub stream_in_flight: StreamCut,
    /// Usage records this replica flushed before its process ended.
    pub usage_records_flushed: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct InFlight {
    pub status: Option<u16>,
    pub completed_after_signal_ms: u128,
    /// The usage status the replica settled it as.
    pub usage_status: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamCut {
    /// How long after the signal the stream ended.
    pub cut_after_signal_ms: u128,
    /// Bytes of answer the caller had received before the cut: a stream cut
    /// before it relayed anything is not the case this measures.
    pub relayed_bytes: u64,
    pub usage_status: Option<String>,
    /// Whether the cut happened inside the deadline the process declares.
    pub within_deadline: bool,
}

/// Evidence that the rollout was actually mixed-version rather than a
/// stop-the-world replacement.
#[derive(Debug, Clone, Serialize)]
pub struct MixedVersion {
    /// Requests each revision answered during the phase where both were in
    /// rotation.
    pub previous_requests: u64,
    pub next_requests: u64,
    /// The alias only the next revision serves, and what each revision did with
    /// it while both were serving.
    pub exclusive_alias: String,
    pub next_serves_exclusive_alias: bool,
    pub previous_refuses_exclusive_alias: bool,
    pub previous_status_for_exclusive_alias: Option<u16>,
}

/// The accounting that makes "nothing was lost" a measurement.
#[derive(Debug, Clone, Serialize)]
pub struct LossLedger {
    /// Requests the harness offered, through the balancer and pinned directly.
    pub offered: u64,
    /// Requests that got an answer, whichever replica gave it.
    pub answered: u64,
    /// Requests answered with an error status.
    pub errors: u64,
    /// Requests no replica answered.
    pub unanswered: u64,
    pub torn_streams: u64,
    /// Requests the balancer could not place at all.
    pub unavailable: u64,
    /// One per request that reached a replica's request path: answered requests
    /// plus the stream a deadline cut.
    pub usage_records_expected: u64,
    pub usage_records_observed: u64,
    pub usage_records_missing: u64,
    pub usage_by_status: BTreeMap<String, u64>,
    /// Upstream bodies still open once every caller is gone: a leak survives a
    /// rollout as easily as it survives a soak.
    pub upstream_streams_open_at_end: i64,
}

/// The per-phase envelope, recorded and never asserted: a fleet mid-rollout is
/// smaller than a fleet at rest, and how much that costs is the number an
/// operator sizes a surge from.
#[derive(Debug, Clone, Serialize)]
pub struct CapacityEnvelope {
    pub steady_answered_rps: f64,
    pub degraded_answered_rps: f64,
    /// `degraded / steady`. Below 1.0 is expected while a replica is draining.
    pub degraded_fraction: f64,
    pub steady_latency_p95_ms: Option<f64>,
    pub degraded_latency_p95_ms: Option<f64>,
}

/// What the operator commands said before the rollout was allowed to start.
#[derive(Debug, Clone, Serialize)]
pub struct MigrationEvidence {
    pub preflight: CommandRecord,
    pub status: CommandRecord,
    /// Whether the rollout was allowed to proceed on this evidence.
    pub gate_passed: bool,
    /// Absent Postgres, a stateless install has no schema to migrate and says so
    /// rather than skipping the check.
    pub control_plane: String,
}

/// One operator command, as an artifact reader can re-run it.
#[derive(Debug, Clone, Serialize)]
pub struct CommandRecord {
    pub argv: Vec<String>,
    pub exit_code: Option<i32>,
    pub succeeded: bool,
    /// The command's own output, which is what an operator reads.
    pub output: String,
}

/// Which rollbacks this rollout proved, and which it proved *prohibited*.
#[derive(Debug, Clone, Serialize)]
pub struct RollbackEvidence {
    pub compatible_patch_rollback: PatchRollback,
    pub migrated_layout_fence: Fence,
}

/// The rollback an operator is allowed to perform: no migration ran, so the
/// previous revision still understands the state, and it goes back the same way
/// it came — surge in, drain out.
#[derive(Debug, Clone, Serialize)]
pub struct PatchRollback {
    pub performed: bool,
    pub replica: String,
    pub answered: u64,
    pub errors: u64,
    /// Whether the rolled-back replica served the traffic the newer one had been
    /// serving.
    pub served_traffic: bool,
}

/// The rollback an operator is *not* allowed to perform. Forward-only migrations
/// mean a ledger a newer build wrote is refused rather than served, and the
/// refusal is not retryable — the operator has to decide something, not wait.
#[derive(Debug, Clone, Serialize)]
pub struct Fence {
    pub evaluated: bool,
    /// Why the fence was not evaluated, when it was not: it needs a real
    /// Postgres.
    pub skipped_reason: Option<String>,
    pub status: Option<CommandRecord>,
    pub refused: bool,
    pub refusal_names_newer_build: bool,
}

/// One thing that happened, in the order it happened. The timeline is the part
/// of the artifact a human reads first when a rollout went wrong.
#[derive(Debug, Clone, Serialize)]
pub struct Event {
    pub at_ms: u128,
    pub phase: String,
    pub kind: String,
    pub detail: String,
}
