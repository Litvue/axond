//! The rollout driver: one scenario, start to finish, with an artifact.
//!
//! The shape of a run follows the sequence `docs/operations/upgrades.md`
//! prescribes, because the point is to qualify *that* sequence rather than a
//! convenient approximation of it:
//!
//! 1. apply the retained release's migrations, publish one complete
//!    desired-state revision, and boot the retained fleet against it;
//! 2. offer traffic through that retained fleet, preflight and status the
//!    candidate, then apply its migrations to the same schema while the
//!    retained replicas keep serving;
//! 3. canary the candidate executable on the shared bootstrap and durable
//!    revision, then drain it;
//! 4. for each old replica: surge in a candidate replacement, wait for the
//!    balancer to admit it, then `SIGTERM` the old one with a buffered request
//!    and a stream already in flight on it, and keep offering traffic across the
//!    whole window;
//! 5. offer traffic to the fully replaced fleet;
//! 6. retain the real previous-to-candidate migration matrix from that same
//!    serving schema;
//! 7. either roll one replica back on an unchanged layout, or prove the retained
//!    binary refuses a candidate-migrated forward-only layout.
//!
//! Everything measured is recorded; the thresholds in the manifest decide what
//! fails. Throughput is deliberately not a gate — a shared runner moves it — but
//! the fleet properties (no caller sent to a withdrawn replica, no unanswered
//! request, no lost usage record, a termination inside the bound the process
//! advertises) do not move with the machine.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime};

use futures::StreamExt;
use serde_json::{Value, json};

use crate::support::capacity::manifest::sha256_hex;
use crate::support::capacity::result::{
    BinaryMeta, ConfigMeta, Percentiles, Verdict, binary_meta_at,
    binary_meta_at_with_version_fallback,
};
use crate::support::gateway::{self, GATEWAY_KEY, alias};

use super::fleet::{
    COMPATIBILITY, Drained, Fleet, NEXT, NEXT_ONLY_ALIAS, PREVIOUS, Revision, TraceWitnessSnapshot,
    pinned,
};
use super::ingress::{Forward, Ingress, REPLICA_HEADER, REVISION_HEADER};
use super::manifest::{RESULT_SCHEMA_VERSION, Scale, Scenario, Tier};
use super::result::{
    CapacityEnvelope, CommandRecord, DrainRecord, DrainingRefusalAttempt, Environment, Event,
    ExpectedNonUsageTraceIdentity, ExpectedUsageIdentity, FailedIngressAttempt, Fence, InFlight,
    LossLedger, MigrationEvidence, MigrationMatrix, MigrationVersion, MixedVersion,
    ObservedUsageIdentity, PatchRollback, PhaseTraffic, ReplicaRecord, ReplicaUsage,
    RetainedRelease, RevisionMeta, RollbackEvidence, RolloutResult, RunMeta, ScenarioEcho,
    StreamCut, TraceExportIdentity, UnexpectedTraceIdentity, UsageReconciliation,
};
use super::stateful::{
    BUFFERED_PROMPT, MigrationTarget as StatefulMigrationTarget, SLOW_PROMPT, STALLED_PROMPT,
};

/// How often the balancer re-probes readiness. Fast enough that the measured
/// removal is dominated by the replica's own drain rather than by the poll, and
/// slow enough to be a plausible ingress setting.
const PROBE_POLL: Duration = Duration::from_millis(50);

/// The bind address the gate's config carries. A preflight never listens, and
/// the artifact records the config it checked, so the port is fixed rather than
/// ephemeral — an artifact whose config hash changed every run would be
/// uncomparable.
const GATE_BIND: &str = "127.0.0.1:8080";

/// How long the harness waits for the fake upstream to see a pinned request
/// before it signals the replica. Being *in flight* is the precondition of the
/// whole drain measurement, so it is established rather than assumed.
const IN_FLIGHT_WAIT: Duration = Duration::from_secs(5);

/// The prompt every request carries. Fixed, so the fake upstream's answer — and
/// therefore the byte counts and the priced tokens — are the same on every run.
const PROMPT: &str = "qualify the rollout";

const PREVIOUS_BINARY_ENV: &str = "AXOND_ROLLOUT_PREVIOUS_BINARY";
const EXPECTED_PREVIOUS_VERSION_ENV: &str = "AXOND_ROLLOUT_EXPECTED_PREVIOUS_VERSION";
const EXPECTED_PREVIOUS_SHA256_ENV: &str = "AXOND_ROLLOUT_EXPECTED_PREVIOUS_SHA256";
const RETAINED_ARCHIVE_SHA256_ENV: &str = "AXOND_ROLLOUT_RETAINED_ARCHIVE_SHA256";

const EXACT_TRACE_RECONCILIATION: &str = "exact_trace";
const RETAINED_TRACE_CONTEXT: &str = "loopback_otlp_http";

/// Run-scoped caller correlation. The high word domains rollout traffic and the
/// low word is a monotonically allocated, one-based request sequence. It is a
/// qualification identity, not a production trace-id generator.
#[derive(Debug, Clone, Copy)]
struct CorrelationId(u64);

impl CorrelationId {
    const DOMAIN: u64 = 0x6178_6f6e_642d_726f;

    fn new(sequence: u64) -> Self {
        assert!(sequence > 0, "a rollout correlation sequence is nonzero");
        Self(sequence)
    }

    fn trace_id(self) -> String {
        format!("{:016x}{:016x}", Self::DOMAIN, self.0)
    }

    fn traceparent(self) -> String {
        format!("00-{}-0000000000000001-01", self.trace_id())
    }
}

struct Binaries {
    previous: PathBuf,
    candidate: PathBuf,
    promotable: bool,
    retained_release: Option<RetainedRelease>,
}

impl Binaries {
    fn resolve(tier: Tier) -> Self {
        let candidate = PathBuf::from(env!("CARGO_BIN_EXE_axond"));
        let supplied = (tier == Tier::Heavy)
            .then(|| std::env::var_os(PREVIOUS_BINARY_ENV).map(PathBuf::from))
            .flatten();
        if tier == Tier::Heavy {
            assert!(
                supplied.is_some(),
                "heavy rollout qualification requires {PREVIOUS_BINARY_ENV} to name the verified \
                 retained release executable"
            );
            assert!(
                std::env::var_os("AXOND_TEST_POSTGRES_DSN").is_some(),
                "heavy rollout qualification requires AXOND_TEST_POSTGRES_DSN"
            );
        }
        let previous = supplied.unwrap_or_else(|| candidate.clone());
        for (label, path) in [("previous", &previous), ("candidate", &candidate)] {
            assert!(
                path.is_file(),
                "the {label} rollout binary {} is not a file",
                path.display()
            );
        }
        let expected_previous_version = (tier == Tier::Heavy).then(|| {
            std::env::var(EXPECTED_PREVIOUS_VERSION_ENV).unwrap_or_else(|_| {
                panic!("heavy rollout qualification requires {EXPECTED_PREVIOUS_VERSION_ENV}")
            })
        });
        let previous_meta =
            binary_meta_at_with_version_fallback(&previous, expected_previous_version.as_deref());
        let candidate_meta = binary_meta_at(&candidate);
        let distinct = previous_meta.sha256 != candidate_meta.sha256;
        let retained_release = (tier == Tier::Heavy).then(|| {
            let required = |name: &str| {
                std::env::var(name)
                    .unwrap_or_else(|_| panic!("heavy rollout qualification requires {name}"))
            };
            let expected_version = expected_previous_version
                .clone()
                .expect("the heavy tier loaded its expected predecessor version");
            let expected_binary_sha256 = required(EXPECTED_PREVIOUS_SHA256_ENV);
            let archive_sha256 = required(RETAINED_ARCHIVE_SHA256_ENV);
            for (name, digest) in [
                (EXPECTED_PREVIOUS_SHA256_ENV, &expected_binary_sha256),
                (RETAINED_ARCHIVE_SHA256_ENV, &archive_sha256),
            ] {
                assert!(
                    digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit()),
                    "{name} must be a 64-character SHA-256 digest"
                );
            }
            assert_eq!(
                previous_meta.version, expected_version,
                "the retained executable version does not match the pinned release"
            );
            assert_eq!(
                previous_meta.sha256.to_ascii_lowercase(),
                expected_binary_sha256.to_ascii_lowercase(),
                "the retained executable digest does not match the verified archive"
            );
            RetainedRelease {
                expected_version,
                expected_binary_sha256: expected_binary_sha256.to_ascii_lowercase(),
                archive_sha256: archive_sha256.to_ascii_lowercase(),
            }
        });
        if tier == Tier::Heavy {
            assert!(
                distinct,
                "heavy rollout qualification requires distinct previous and candidate binary digests"
            );
        }
        Self {
            previous,
            candidate,
            promotable: tier == Tier::Heavy && distinct,
            retained_release,
        }
    }
}

pub async fn run(scenario: &Scenario, tier: Tier, manifest_text: &str) -> RolloutResult {
    let binaries = Binaries::resolve(tier);
    let scale = *scenario.scale(tier);
    let started_at = SystemTime::now();
    let started = Instant::now();
    let mut harness = Harness::new(scenario.clone(), scale, tier, started, &binaries).await;
    let gate = harness.prepare_gate(&binaries.previous).await;

    // Boot and admit the retained serving fleet before the candidate touches
    // the schema.
    for _ in 0..scenario.replicas {
        harness.admit(Revision::previous()).await;
    }
    harness.phase("steady-previous").await;

    // The previous processes are now demonstrably serving the projected
    // revision. Apply the candidate migration to that same schema before its
    // first process starts. Existing previous processes may drain from their
    // active immutable snapshot; a fresh previous process is admitted later
    // only when the migration ledger still permits it.
    let (migration, fence) = harness
        .complete_gate(gate, &binaries.previous, &binaries.candidate)
        .await;

    // Before replacement, prove that the candidate can boot and serve behind
    // the fleet with the same stateful bootstrap and durable revision.
    let compatibility = harness.admit(Revision::compatibility()).await;
    harness.phase("candidate-on-previous-config").await;
    let mut drains = vec![harness.drain(&compatibility, "compatibility-drain").await];

    // The rollout proper: one replacement at a time, never below the original
    // replica count, which is what makes it a rolling deployment rather than a
    // restart.
    let mut mixed = None;
    for index in 0..scenario.replicas {
        let victim = harness
            .fleet
            .oldest(Revision::previous())
            .expect("a previous-revision replica is still serving")
            .id
            .clone();
        harness.admit(Revision::next()).await;
        harness.phase(&format!("mixed-{index}")).await;
        if index == 0 {
            // Both revisions are serving right now, which is the only window in
            // which the mixed-version rule can be observed at all.
            mixed = Some(harness.mixed_version().await);
        }
        drains.push(harness.drain(&victim, &format!("drain-{index}")).await);
    }
    harness.phase("steady-next").await;

    // Roll back only when the real migration matrix proved the ledger remained
    // compatible. If the candidate added a version, the previous binary's
    // refusal is the rollback result; starting it as a replica would violate the
    // deployment rule the harness is supposed to qualify.
    let patch_rollback = if fence.expected_refused {
        PatchRollback {
            performed: false,
            skipped_reason: Some(
                "candidate added forward-only migrations; previous binary refused the layout"
                    .to_owned(),
            ),
            replica: None,
            answered: 0,
            errors: 0,
            served_traffic: false,
        }
    } else {
        let rollback_replica = harness.admit(Revision::previous()).await;
        let replaced = harness
            .fleet
            .oldest(Revision::next())
            .expect("a next-revision replica is serving")
            .id
            .clone();
        drains.push(harness.drain(&replaced, "rollback-drain").await);
        let rollback_traffic = harness.phase("rolled-back").await;
        let served = rollback_traffic
            .by_replica
            .get(&rollback_replica)
            .copied()
            .unwrap_or_default();
        PatchRollback {
            performed: true,
            skipped_reason: None,
            replica: Some(rollback_replica),
            answered: served,
            errors: rollback_traffic.errors,
            served_traffic: served > 0,
        }
    };

    // Everything is quiet now, so the accounting can settle.
    let records = await_exact_usage_records(&harness, Duration::from_secs(10)).await;
    let expected_trace_identities = harness.expected_otlp_trace_identities();
    let trace_witness = harness
        .fleet
        .settle_trace_identities(&expected_trace_identities, Duration::from_secs(5))
        .await;
    let elapsed = started.elapsed();

    let mixed = mixed.expect("the rollout has at least one mixed-version window");
    let revisions = revisions(&harness.fleet, &binaries);
    let loss = ledger(&harness, &records, &trace_witness);
    let capacity = envelope(&harness.traffic);
    let fleet_records = fleet_records(&harness, &drains);
    let rollback = RollbackEvidence {
        compatible_patch_rollback: patch_rollback,
        migrated_layout_fence: fence,
    };

    let result = RolloutResult {
        schema_version: RESULT_SCHEMA_VERSION,
        scenario: ScenarioEcho::new(scenario, tier),
        run: RunMeta::new(
            started_at,
            elapsed,
            tier,
            binaries.promotable,
            binaries.retained_release.clone(),
        ),
        environment: Environment::collect(manifest_text),
        revisions,
        fleet: fleet_records,
        traffic: harness.traffic,
        drains,
        mixed_version: mixed,
        loss,
        capacity,
        migration,
        rollback,
        timeline: harness.timeline.events,
        verdicts: Vec::new(),
    };
    let verdicts = verdicts(&result);
    RolloutResult { verdicts, ..result }
}

/// Wait for the exact expected traces, not merely the expected cardinality. An
/// unrelated early row must not stop settlement before the row it would appear
/// to substitute has had the full flush window to arrive.
async fn await_exact_usage_records(harness: &Harness, within: Duration) -> Vec<Value> {
    let deadline = Instant::now() + within;
    loop {
        let records = harness.fleet.usage_records_by_replica();
        let reconciliation = reconcile(&harness.expected_usage, &records, &BTreeMap::new());
        if (reconciliation.missing == 0 && reconciliation.status_mismatches == 0)
            || Instant::now() >= deadline
        {
            return harness.fleet.usage_records();
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The run's mutable state, in one place so the phases can be methods: a phase
/// needs the fleet, the balancer, the clock, and the ledger, and threading four
/// of those through free functions is how a driver becomes unreadable.
struct Harness {
    scenario: Scenario,
    scale: Scale,
    fleet: Fleet,
    ingress: Ingress,
    client: reqwest::Client,
    started: Instant,
    timeline: Timeline,
    traffic: Vec<PhaseTraffic>,
    /// Exact caller-side accounting rows. The denominator and every expected
    /// identity are derived from requests the driver proved reached a replica,
    /// never from records that happened to turn up later.
    expected_usage: Vec<ExpectedUsageIdentity>,
    /// Direct caller traces that deliberately do not owe usage rows. Retried
    /// drain refusals are reconstructed from the ingress attempt ledger.
    expected_non_usage_traces: Vec<ExpectedNonUsageTraceIdentity>,
    /// Monotonic run-scoped source for caller trace identities.
    next_correlation: u64,
    mixed_probe: Option<MixedVersion>,
    /// How long each replica took from being booted to carrying traffic. Kept
    /// per replica because the offset the balancer records is an offset from the
    /// run's start, which grows with the run rather than with how slowly a
    /// replacement was admitted.
    admissions: BTreeMap<String, Duration>,
}

struct PendingGate {
    config_path: String,
    migration: Option<PreparedMigration>,
}

struct PreparedMigration {
    target: StatefulMigrationTarget,
    previous_apply: CommandRecord,
    previous_status_before: CommandRecord,
    previous_versions: Vec<MigrationVersion>,
}

impl Harness {
    async fn new(
        scenario: Scenario,
        scale: Scale,
        tier: Tier,
        started: Instant,
        binaries: &Binaries,
    ) -> Self {
        let fleet = Fleet::start(
            scenario.shutdown,
            &binaries.previous,
            &binaries.candidate,
            tier == Tier::Heavy,
        )
        .await;
        let ingress = Ingress::start(PROBE_POLL, started).await;
        Self {
            scenario,
            scale,
            fleet,
            ingress,
            client: crate::support::client(),
            started,
            timeline: Timeline::new(started),
            traffic: Vec::new(),
            expected_usage: Vec::new(),
            expected_non_usage_traces: Vec::new(),
            next_correlation: 1,
            mixed_probe: None,
            admissions: BTreeMap::new(),
        }
    }

    fn reserve_correlations(&mut self, count: usize) -> u64 {
        let first = self.next_correlation;
        self.next_correlation = self
            .next_correlation
            .checked_add(u64::try_from(count).expect("a rollout phase count fits u64"))
            .expect("the rollout correlation sequence does not overflow");
        first
    }

    fn next_correlation(&mut self) -> CorrelationId {
        let sequence = self.reserve_correlations(1);
        CorrelationId::new(sequence)
    }

    fn expect_usage(&mut self, replica: &str, correlation: CorrelationId, status: &str) {
        self.expected_usage.push(ExpectedUsageIdentity {
            replica: replica.to_owned(),
            trace_id: correlation.trace_id(),
            status: status.to_owned(),
        });
    }

    fn expect_non_usage_trace(&mut self, replica: &str, correlation: CorrelationId, reason: &str) {
        self.expected_non_usage_traces
            .push(ExpectedNonUsageTraceIdentity {
                replica: replica.to_owned(),
                trace_id: correlation.trace_id(),
                reason: reason.to_owned(),
            });
    }

    fn expected_non_usage_trace_identities(&self) -> Vec<ExpectedNonUsageTraceIdentity> {
        let mut identities = self.expected_non_usage_traces.clone();
        for attempt in self.draining_refusal_attempts() {
            identities.push(ExpectedNonUsageTraceIdentity {
                replica: attempt.refused_replica,
                trace_id: attempt.trace_id,
                reason: "draining_refusal".to_owned(),
            });
        }
        identities.sort();
        identities
    }

    fn draining_refusal_attempts(&self) -> Vec<DrainingRefusalAttempt> {
        let mut attempts = Vec::new();
        for caller in self.ingress.state.callers() {
            let Some(trace_id) = caller.trace_id.as_ref() else {
                continue;
            };
            let answered = caller.answered_by();
            for refused in caller
                .attempts
                .iter()
                .filter(|attempt| attempt.refused_while_draining)
            {
                attempts.push(DrainingRefusalAttempt {
                    caller_id: caller.id,
                    trace_id: trace_id.clone(),
                    refused_replica: refused.replica.clone(),
                    accepted_replica: answered.map(|attempt| attempt.replica.clone()),
                    accepted_status: answered.and_then(|attempt| attempt.status),
                });
            }
        }
        attempts.sort();
        attempts
    }

    fn failed_ingress_attempts(&self) -> Vec<FailedIngressAttempt> {
        let mut attempts = Vec::new();
        for caller in self.ingress.state.callers() {
            let Some(trace_id) = caller.trace_id.as_ref() else {
                continue;
            };
            for attempt in &caller.attempts {
                let reason = if attempt.refused_while_draining {
                    continue;
                } else if attempt.status.is_none() {
                    "transport_failure"
                } else if attempt.status == Some(503) {
                    "untyped_503"
                } else {
                    continue;
                };
                attempts.push(FailedIngressAttempt {
                    caller_id: caller.id,
                    trace_id: trace_id.clone(),
                    replica: attempt.replica.clone(),
                    reason: reason.to_owned(),
                });
            }
        }
        attempts.sort();
        attempts
    }

    fn expected_otlp_trace_identities(&self) -> BTreeSet<(String, String)> {
        // A transport failure can leave a server span behind without a usage
        // row. Do not silently excuse it here: the exact trace mismatch and
        // exact trace verdict must fail, preserving the failed attempt's real
        // attribution instead of laundering it as successful accounting. The
        // caller may still succeed after a transport retry, so availability is
        // judged independently.
        self.expected_usage
            .iter()
            .map(|identity| (identity.replica.clone(), identity.trace_id.clone()))
            .chain(
                self.expected_non_usage_trace_identities()
                    .into_iter()
                    .map(|identity| (identity.replica, identity.trace_id)),
            )
            .collect()
    }

    /// Apply the retained layout and publish its complete desired state before
    /// any fleet replica starts. Candidate migration is intentionally deferred:
    /// the previous fleet must first prove it can serve the exact schema and
    /// revision that will be upgraded underneath it.
    async fn prepare_gate(&mut self, previous_binary: &Path) -> PendingGate {
        let dir = scratch_dir("gate");
        let bind: SocketAddr = GATE_BIND.parse().expect("the gate address parses");
        let config = self.fleet.config(bind, Revision::next());
        let path = write_config(&dir, "next.toml", &config);

        let migration = if let Some(target) = self.fleet.migration_target() {
            let previous_apply = axond_at(
                previous_binary,
                &["migrate", "apply", "--config", &path],
                &target.env,
            );
            assert!(
                previous_apply.succeeded,
                "the retained release could not apply its migrations to the serving schema:\n{}",
                previous_apply.output
            );
            let previous_status_before = axond_at(
                previous_binary,
                &["migrate", "status", "--config", &path],
                &target.env,
            );
            assert!(
                previous_status_before.succeeded,
                "the retained release did not accept its own serving layout:\n{}",
                previous_status_before.output
            );
            let client = connect(&target.dsn).await;
            let previous_versions = migration_versions(&client, &target.schema).await;
            Some(PreparedMigration {
                target,
                previous_apply,
                previous_status_before,
                previous_versions,
            })
        } else {
            None
        };
        self.fleet.prepare_stateful().await;
        self.timeline.at(
            "gate",
            "retained-layout-ready",
            "retained migrations and complete desired state are ready for the previous fleet",
        );
        PendingGate {
            config_path: path,
            migration,
        }
    }

    /// Once old replicas have served the retained revision, apply candidate
    /// migrations to that same live schema. Already-running old replicas remain
    /// eligible to drain their immutable serving snapshot; the final status
    /// probe decides whether a fresh old process is an allowed rollback.
    async fn complete_gate(
        &mut self,
        gate: PendingGate,
        previous_binary: &Path,
        candidate_binary: &Path,
    ) -> (MigrationEvidence, Fence) {
        let PendingGate {
            config_path,
            migration,
        } = gate;
        let env = migration
            .as_ref()
            .map(|migration| migration.target.env.as_slice())
            .unwrap_or(&[]);
        let preflight = axond_at(
            candidate_binary,
            &["check", "preflight", "--config", &config_path],
            env,
        );
        assert!(
            preflight.succeeded,
            "the incoming revision failed preflight against the retained fleet's live schema:\n{}",
            preflight.output
        );
        let candidate_status_before = axond_at(
            candidate_binary,
            &["migrate", "status", "--config", &config_path],
            env,
        );
        let candidate_status_acceptable = candidate_status_before.succeeded
            || (migration.is_some()
                && candidate_status_before
                    .output
                    .contains("migration(s) pending"));
        assert!(
            candidate_status_acceptable,
            "the candidate neither accepted the retained fleet's live schema nor reported expected \
             pending migrations:\n{}",
            candidate_status_before.output
        );
        self.timeline.at(
            "gate",
            "candidate-gate-on-live-retained-fleet",
            format!(
                "candidate preflight {}; pre-apply migrate status {}",
                verdict_word(preflight.succeeded),
                if candidate_status_before.succeeded {
                    "accepted"
                } else {
                    "reported pending migrations"
                }
            ),
        );
        let (status, matrix, fence, control_plane) = if let Some(prepared) = migration {
            let candidate_apply = axond_at(
                candidate_binary,
                &["migrate", "apply", "--config", &config_path],
                &prepared.target.env,
            );
            assert!(
                candidate_apply.succeeded,
                "the candidate could not migrate the schema the retained fleet is serving:\n{}",
                candidate_apply.output
            );
            let candidate_status_after = axond_at(
                candidate_binary,
                &["migrate", "status", "--config", &config_path],
                &prepared.target.env,
            );
            assert!(
                candidate_status_after.succeeded,
                "the candidate did not accept its migrated serving layout:\n{}",
                candidate_status_after.output
            );
            let client = connect(&prepared.target.dsn).await;
            let candidate_versions = migration_versions(&client, &prepared.target.schema).await;
            assert!(
                candidate_versions.starts_with(&prepared.previous_versions),
                "the candidate migration ledger changed or reordered retained rows"
            );
            let candidate_added_versions: Vec<i32> = candidate_versions
                [prepared.previous_versions.len()..]
                .iter()
                .map(|migration| migration.version)
                .collect();
            let expected_refused = !candidate_added_versions.is_empty();
            let classification = if expected_refused {
                "forward-only"
            } else {
                "unchanged"
            };
            assert_eq!(
                candidate_status_before.succeeded, !expected_refused,
                "candidate pre-apply status disagrees with the versions it added:\n{}",
                candidate_status_before.output
            );
            let previous_status_after_candidate = axond_at(
                previous_binary,
                &["migrate", "status", "--config", &config_path],
                &prepared.target.env,
            );
            let status_refused = !previous_status_after_candidate.succeeded;
            let status_names_newer = previous_status_after_candidate
                .output
                .contains("newer gateway");
            assert_eq!(
                status_refused, expected_refused,
                "migration classification {classification} disagrees with previous-binary \
                 status:\n{}",
                previous_status_after_candidate.output
            );
            if expected_refused {
                assert!(
                    status_names_newer,
                    "the previous binary refused the candidate layout without naming the newer \
                     gateway"
                );
            }
            let cold_start = self
                .fleet
                .previous_cold_start()
                .await
                .expect("a heavy stateful rollout can cold-start the retained binary");
            let refused =
                !cold_start.reached_readiness && cold_start.exit_code.is_some_and(|code| code != 0);
            let names_newer = cold_start.output.contains("newer gateway");
            assert_eq!(
                refused, expected_refused,
                "migration classification {classification} disagrees with the real retained-binary \
                 cold start (ready {}, exit {:?}):\n{}",
                cold_start.reached_readiness, cold_start.exit_code, cold_start.output
            );
            if expected_refused {
                assert!(
                    names_newer,
                    "the retained binary's cold-start refusal did not name the newer gateway:\n{}",
                    cold_start.output
                );
            } else {
                assert!(
                    cold_start.reached_readiness,
                    "the unchanged layout did not permit a retained binary to reach authenticated \
                     readiness:\n{}",
                    cold_start.output
                );
            }
            let mut cold_start_secrets: Vec<(&str, &str)> = prepared
                .target
                .env
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_str()))
                .collect();
            cold_start_secrets.push(("STATEFUL_WORKLOAD_KEY", self.fleet.caller_key()));
            let cold_start_output = redacted(&cold_start.output, &cold_start_secrets);
            self.timeline.at(
                "migration",
                "candidate-layout-active",
                format!(
                    "candidate migration was {classification}; a retained cold start was {}",
                    if refused { "refused" } else { "allowed" }
                ),
            );
            let matrix = MigrationMatrix {
                evaluated: true,
                skipped_reason: None,
                previous_apply: Some(prepared.previous_apply),
                previous_status_before: Some(prepared.previous_status_before),
                candidate_status_before: Some(candidate_status_before),
                candidate_apply: Some(candidate_apply),
                candidate_status_after: Some(candidate_status_after.clone()),
                previous_status_after_candidate: Some(previous_status_after_candidate.clone()),
                previous_versions: prepared.previous_versions,
                candidate_versions,
                candidate_added_versions,
                classification: classification.to_owned(),
            };
            let fence = Fence {
                evaluated: true,
                skipped_reason: None,
                status: Some(previous_status_after_candidate),
                cold_start_attempted: true,
                cold_start_reached_readiness: cold_start.reached_readiness,
                cold_start_exit_code: cold_start.exit_code,
                cold_start_output: Some(cold_start_output),
                refused,
                refusal_names_newer_build: names_newer,
                expected_refused,
            };
            (
                candidate_status_after,
                matrix,
                fence,
                format!(
                    "one real PostgreSQL schema ({}) supplied migrations, desired state, and the \
                     serving fleet",
                    prepared.target.schema
                ),
            )
        } else {
            let reason = "reduced diagnostic intentionally has no PostgreSQL control plane";
            (
                candidate_status_before,
                MigrationMatrix {
                    evaluated: false,
                    skipped_reason: Some(reason.to_owned()),
                    previous_apply: None,
                    previous_status_before: None,
                    candidate_status_before: None,
                    candidate_apply: None,
                    candidate_status_after: None,
                    previous_status_after_candidate: None,
                    previous_versions: Vec::new(),
                    candidate_versions: Vec::new(),
                    candidate_added_versions: Vec::new(),
                    classification: "not-evaluated".to_owned(),
                },
                Fence {
                    evaluated: false,
                    skipped_reason: Some(reason.to_owned()),
                    status: None,
                    cold_start_attempted: false,
                    cold_start_reached_readiness: false,
                    cold_start_exit_code: None,
                    cold_start_output: None,
                    refused: false,
                    refusal_names_newer_build: false,
                    expected_refused: false,
                },
                format!("not evaluated: {reason}"),
            )
        };
        let gate_passed = preflight.succeeded && status.succeeded;
        self.timeline.at(
            "gate",
            "migration-gate",
            format!(
                "preflight {} and post-apply migrate status {} for the incoming revision",
                verdict_word(preflight.succeeded),
                verdict_word(status.succeeded)
            ),
        );
        assert!(
            gate_passed,
            "the incoming revision failed its deployment gate:\npreflight:\n{}\nmigrate \
             status:\n{}",
            preflight.output, status.output
        );
        (
            MigrationEvidence {
                preflight,
                status,
                gate_passed,
                control_plane,
                matrix,
            },
            fence,
        )
    }

    /// Boot a replica, put it in rotation, and wait for the balancer to start
    /// using it. Returns its id.
    async fn admit(&mut self, revision: Revision) -> String {
        let bound = Duration::from_millis(self.scenario.thresholds.max_replacement_admission_ms);
        let booting = Instant::now();
        let replica = self.fleet.admit(revision).await;
        let (id, base_url) = (replica.id.clone(), replica.base_url().to_owned());
        self.ingress.add(&id, revision.label, &base_url);
        let admitted = self.ingress.await_admission(&id, bound).await;
        if admitted.is_some() {
            self.admissions.insert(id.clone(), booting.elapsed());
        }
        self.timeline.at(
            "admission",
            "replica-admitted",
            match admitted {
                Some(_) => format!(
                    "{id} ({}) took {} ms from boot to carrying traffic",
                    revision.label,
                    booting.elapsed().as_millis()
                ),
                None => format!(
                    "{id} ({}) was never admitted within {} ms",
                    revision.label,
                    bound.as_millis()
                ),
            },
        );
        assert!(
            admitted.is_some(),
            "{id} never became ready, so the rollout has no replacement to route to"
        );
        id
    }

    /// Offer one phase of caller traffic through the balancer.
    async fn phase(&mut self, name: &str) -> PhaseTraffic {
        let before = self.ingress.state.forwards().len();
        let requests_per_phase = self.scale.requests_per_phase;
        let first_correlation = self.reserve_correlations(requests_per_phase);
        let caller_key = self.fleet.caller_key().to_owned();
        self.timeline.at(name, "phase-start", "offering traffic");
        let (outcomes, elapsed) = offer(
            self.ingress.base_url.clone(),
            self.scale,
            self.client.clone(),
            caller_key,
            first_correlation,
        )
        .await;
        let traffic = self.settle(name, &outcomes, elapsed, before);
        self.timeline.at(name, "phase-end", summary_of(&traffic));
        traffic
    }

    /// Turn a phase's outcomes into the recorded traffic, and cross-check the
    /// driver's attribution against the balancer's own log: two witnesses to the
    /// same routing, and a disagreement is a harness bug rather than a result.
    fn settle(
        &mut self,
        name: &str,
        outcomes: &[Outcome],
        elapsed: Duration,
        forwards_before: usize,
    ) -> PhaseTraffic {
        let forwards = self.ingress.state.forwards();
        let placed = &forwards[forwards_before.min(forwards.len())..];
        let answered = outcomes.iter().filter(|o| o.ok()).count() as u64;
        let torn = outcomes.iter().filter(|o| o.torn).count() as u64;
        let mut by_status: BTreeMap<String, u64> = BTreeMap::new();
        let mut by_replica: BTreeMap<String, u64> = BTreeMap::new();
        let mut by_revision: BTreeMap<String, u64> = BTreeMap::new();
        for outcome in outcomes {
            match outcome.status {
                Some(status) => *by_status.entry(status.to_string()).or_default() += 1,
                None => *by_status.entry("transport-failure".to_owned()).or_default() += 1,
            }
            if let Some(replica) = outcome.replica.as_ref() {
                *by_replica.entry(replica.clone()).or_default() += 1;
            }
            if let Some(revision) = outcome.revision.as_ref() {
                *by_revision.entry(revision.clone()).or_default() += 1;
            }
            if outcome.ok() {
                let replica = outcome
                    .replica
                    .as_deref()
                    .expect("an answered rollout request names its replica");
                self.expect_usage(
                    replica,
                    outcome.correlation,
                    if outcome.torn {
                        "client_cancelled"
                    } else {
                        "ok"
                    },
                );
            }
        }
        let latencies: Vec<f64> = outcomes.iter().map(|o| o.latency_ms).collect();
        let traffic = PhaseTraffic {
            phase: name.to_owned(),
            offered: outcomes.len() as u64,
            answered,
            errors: outcomes
                .iter()
                .filter(|o| o.status.is_some_and(|s| !(200..300).contains(&s)))
                .count() as u64,
            unanswered: outcomes.iter().filter(|o| o.status.is_none()).count() as u64,
            torn_streams: torn,
            streamed: outcomes.iter().filter(|o| o.streamed).count() as u64,
            elapsed_ms: elapsed.as_millis(),
            answered_rps: rate(answered, elapsed),
            latency_ms: Percentiles::of(&latencies),
            by_status,
            by_replica: by_replica.clone(),
            by_revision,
            retried: placed.iter().filter(|forward| forward.retries > 0).count() as u64,
        };
        assert_eq!(
            balancer_counts(placed),
            by_replica,
            "the balancer's own log disagrees with the caller's attribution in {name}"
        );
        self.traffic.push(traffic.clone());
        traffic
    }

    /// The mixed-version window, put to the processes rather than assumed. The
    /// reduced diagnostic observes a candidate-only alias. The heavy stateful
    /// lane instead requires both binaries to serve the same immutable durable
    /// revision: desired state is global and cannot truthfully vary by replica.
    async fn mixed_version(&mut self) -> MixedVersion {
        let (previous_id, previous) = self.pinned_replica(PREVIOUS);
        let (next_id, next) = self.pinned_replica(NEXT);
        let next_correlation = self.next_correlation();
        let previous_correlation = self.next_correlation();
        let probe_alias = if self.fleet.is_stateful() {
            alias::CHAT
        } else {
            NEXT_ONLY_ALIAS
        };
        let (on_next, _) = self
            .capability(&next, probe_alias, next_correlation)
            .await
            .expect("the incoming revision answered the mixed-version probe");
        let (on_previous, previous_error_type) = self
            .capability(&previous, probe_alias, previous_correlation)
            .await
            .expect("the outgoing revision answered the mixed-version probe");
        // Every successful probe is a request like any other and belongs in the
        // exact usage ledger. The reduced previous-revision refusal never
        // reaches a provider and therefore has no usage record to expect.
        if (200..300).contains(&on_next) {
            self.expect_usage(&next_id, next_correlation, "ok");
        } else {
            self.expect_non_usage_trace(&next_id, next_correlation, "capability_refusal");
        }
        if (200..300).contains(&on_previous) {
            self.expect_usage(&previous_id, previous_correlation, "ok");
        } else {
            self.expect_non_usage_trace(&previous_id, previous_correlation, "capability_refusal");
        }
        let phase = self
            .traffic
            .last()
            .expect("a mixed-version phase has already run");
        let (mixed, kind, detail) = if self.fleet.is_stateful() {
            let revision = self
                .fleet
                .desired_state_revision()
                .expect("a heavy stateful fleet published a durable revision")
                .to_owned();
            let mixed = MixedVersion {
                previous_requests: phase.by_revision.get(PREVIOUS).copied().unwrap_or_default(),
                next_requests: phase.by_revision.get(NEXT).copied().unwrap_or_default(),
                exclusive_alias: String::new(),
                next_serves_exclusive_alias: false,
                previous_refuses_exclusive_alias: false,
                previous_status_for_exclusive_alias: None,
                previous_error_type_for_exclusive_alias: None,
                shared_stateful_revision: Some(revision.clone()),
                shared_alias: Some(probe_alias.to_owned()),
                previous_serves_shared_alias: (200..300).contains(&on_previous),
                next_serves_shared_alias: (200..300).contains(&on_next),
            };
            let detail = format!(
                "durable revision {revision} alias `{probe_alias}` answered {on_previous} on the \
                 retained revision and {on_next} on the candidate, with {} and {} exact \
                 by-revision requests served in the window",
                mixed.previous_requests, mixed.next_requests
            );
            (mixed, "shared-revision-probe", detail)
        } else {
            let mixed = MixedVersion {
                previous_requests: phase.by_revision.get(PREVIOUS).copied().unwrap_or_default(),
                next_requests: phase.by_revision.get(NEXT).copied().unwrap_or_default(),
                exclusive_alias: NEXT_ONLY_ALIAS.to_owned(),
                next_serves_exclusive_alias: (200..300).contains(&on_next),
                previous_refuses_exclusive_alias: on_previous == 404
                    && previous_error_type.as_deref() == Some("unknown_model"),
                previous_status_for_exclusive_alias: Some(on_previous),
                previous_error_type_for_exclusive_alias: previous_error_type,
                shared_stateful_revision: None,
                shared_alias: None,
                previous_serves_shared_alias: false,
                next_serves_shared_alias: false,
            };
            let detail = format!(
                "`{NEXT_ONLY_ALIAS}` answered {on_next} on the incoming revision and \
                 {on_previous} on the outgoing one, with {} and {} requests served in the window",
                mixed.next_requests, mixed.previous_requests
            );
            (mixed, "capability-probe", detail)
        };
        self.timeline.at("mixed-version", kind, detail);
        self.mixed_probe = Some(mixed.clone());
        mixed
    }

    /// Ask one exact replica for the mixed-version contract's selected alias.
    async fn capability(
        &self,
        base_url: &str,
        alias: &str,
        correlation: CorrelationId,
    ) -> Option<(u16, Option<String>)> {
        let response = self
            .client
            .post(format!("{base_url}/v1/chat/completions"))
            .bearer_auth(self.fleet.caller_key())
            .header("traceparent", correlation.traceparent())
            .json(&body(alias, false))
            .send()
            .await
            .ok()?;
        let status = response.status().as_u16();
        let body = response.text().await.ok()?;
        let error_type = serde_json::from_str::<Value>(&body)
            .ok()
            .and_then(|value| value["error"]["type"].as_str().map(ToOwned::to_owned));
        Some((status, error_type))
    }

    /// A live replica at `revision`, as the pair a pinned request needs: the id
    /// its records will be accounted under, and the address to send to.
    fn pinned_replica(&self, revision: &str) -> (String, String) {
        let replica = self
            .fleet
            .replicas()
            .iter()
            .find(|replica| replica.revision.label == revision)
            .unwrap_or_else(|| panic!("a {revision}-revision replica is serving"));
        (replica.id.clone(), replica.base_url().to_owned())
    }

    /// Take one replica out of the rollout: a buffered request and a stream are
    /// pinned to it and confirmed in flight, the balancer keeps offering traffic
    /// across the whole window, and the process is held to the bound it
    /// advertises.
    async fn drain(&mut self, id: &str, phase: &str) -> DrainRecord {
        let base_url = self.fleet.replica(id).base_url().to_owned();
        let revision = self.fleet.replica(id).revision.label.to_owned();

        let buffered_correlation = self.next_correlation();
        let stream_correlation = self.next_correlation();
        let buffered = self
            .pin(&base_url, pinned::BUFFERED, false, buffered_correlation)
            .await;
        let stream = self
            .pin(&base_url, pinned::STREAM, true, stream_correlation)
            .await;
        // Both are through the replica's request path and at the upstream, so
        // both will settle a usage record however the drain ends them. The
        // stream is the one the deadline cuts, so its record is a cancellation.
        self.expect_usage(id, buffered_correlation, "ok");
        self.expect_usage(id, stream_correlation, "client_cancelled");

        let forwards_before = self.ingress.state.forwards().len();
        let requests_per_phase = self.scale.requests_per_phase;
        let first_correlation = self.reserve_correlations(requests_per_phase);
        let caller_key = self.fleet.caller_key().to_owned();
        let traffic = tokio::spawn(offer(
            self.ingress.base_url.clone(),
            self.scale,
            self.client.clone(),
            caller_key,
            first_correlation,
        ));

        let signalled = Instant::now();
        self.fleet.signal(id);
        self.timeline.at(
            phase,
            "sigterm",
            format!("{id} ({revision}) was signalled with two requests in flight on it"),
        );

        let thresholds = self.scenario.thresholds;
        let drain_grace = Duration::from_millis(self.scenario.shutdown.drain_grace_ms);
        let removal_bound = Duration::from_millis(thresholds.max_readiness_removal_ms.max(1) * 4)
            .max(Duration::from_secs(2));
        let slack = Duration::from_millis(thresholds.max_drain_exit_slack_ms);
        let (withdrawn, drained) = tokio::join!(
            self.ingress.await_withdrawal(id, signalled, removal_bound),
            self.fleet.retire(id, signalled, slack),
        );
        let (outcomes, elapsed) = traffic.await.expect("the drain-window traffic completes");
        self.settle(phase, &outcomes, elapsed, forwards_before);

        let buffered = buffered.settle().await;
        let stream = stream.settle().await;
        let member = self
            .ingress
            .state
            .member(id)
            .expect("the drained replica is a balancer member");

        self.timeline.at(
            phase,
            "readiness-removed",
            match withdrawn {
                Some(after) => format!(
                    "the balancer stopped routing to {id} {} ms after the signal",
                    after.as_millis()
                ),
                None => format!("the balancer never stopped routing to {id}"),
            },
        );
        self.timeline.at(
            phase,
            "stream-cut",
            format!(
                "the pinned stream ended {} ms after the signal with {} bytes relayed",
                stream.ended_after(signalled).as_millis(),
                stream.bytes
            ),
        );
        self.timeline.at(
            phase,
            "buffered-completed",
            format!(
                "the pinned buffered request answered {:?} {} ms after the signal",
                buffered.status,
                buffered.ended_after(signalled).as_millis()
            ),
        );
        self.timeline.at(
            phase,
            "exited",
            match drained.took {
                Some(took) => format!(
                    "{id} exited {} after {} ms, having flushed {} usage records",
                    if drained.clean { "cleanly" } else { "non-zero" },
                    took.as_millis(),
                    drained.usage_records.len()
                ),
                None => format!(
                    "{id} outlived the {} ms bound it advertises:\n{}",
                    drained.budget.as_millis(),
                    drained.output
                ),
            },
        );

        DrainRecord {
            replica: id.to_owned(),
            revision,
            signalled_at_ms: signalled.duration_since(self.started).as_millis(),
            readiness_removed_after_ms: withdrawn.map(|after| after.as_millis()),
            exited_after_ms: drained.took.map(|took| took.as_millis()),
            exit_clean: drained.clean,
            exit_budget_ms: drained.budget.as_millis(),
            requests_after_withdrawal: member.forwards_after_withdrawal(),
            // Recomputed from the recorded dispatch instants against the
            // recorded withdrawal instant, rather than from the flag the
            // selection carried: two witnesses to the same boundary, so the gate
            // survives the selection stopping to enforce it.
            dispatches_after_withdrawal: member
                .withdrawn_at()
                .map_or(0, |at| member.dispatches_after(at)),
            // Only dispatches past the replica's own grace window are a defect:
            // inside it the replica is still admitting, so a hand-over the
            // scheduler delayed across the withdrawal instant is served exactly
            // as it would be in production.
            dispatches_beyond_drain_grace: member
                .withdrawn_at()
                .map_or(0, |at| member.dispatches_beyond(at, drain_grace)),
            worst_dispatch_lag_ms: member
                .withdrawn_at()
                .and_then(|at| member.worst_dispatch_lag(at))
                .map(|lag| lag.as_millis()),
            drain_grace_ms: self.scenario.shutdown.drain_grace_ms,
            buffered_in_flight: InFlight {
                status: buffered.status,
                completed_after_signal_ms: buffered.ended_after(signalled).as_millis(),
                usage_status: usage_status(&drained, pinned::BUFFERED),
            },
            stream_in_flight: StreamCut {
                cut_after_signal_ms: stream.ended_after(signalled).as_millis(),
                relayed_bytes: stream.bytes,
                usage_status: usage_status(&drained, pinned::STREAM),
                within_deadline: stream.ended_after(signalled)
                    <= self.scenario.shutdown.stream_budget()
                        + Duration::from_millis(thresholds.max_stream_cut_observation_slack_ms),
            },
            usage_records_flushed: drained.usage_records.len() as u64,
        }
    }

    /// Start a request pinned to one replica — past the balancer, so the drain
    /// cannot route it away — and return once the upstream has seen it, which is
    /// what makes "in flight" a fact rather than a hope.
    async fn pin(
        &self,
        base_url: &str,
        alias: &str,
        stream: bool,
        correlation: CorrelationId,
    ) -> Pinned {
        // The exact arrival count, not the retained-request list: that list is
        // capped, so at heavy scale its length stops growing and a wait on it
        // could never be satisfied.
        let seen = self.fleet.upstream.state.received();
        let client = self.client.clone();
        let caller_key = self.fleet.caller_key().to_owned();
        let url = format!("{base_url}/v1/chat/completions");
        let payload = body(alias, stream);
        let handle = tokio::spawn(async move {
            let response = client
                .post(url)
                .bearer_auth(caller_key)
                .header("traceparent", correlation.traceparent())
                .json(&payload)
                .send()
                .await;
            let Ok(response) = response else {
                return (None, 0, Instant::now());
            };
            let status = response.status().as_u16();
            let mut body = response.bytes_stream();
            let mut bytes = 0u64;
            while let Some(chunk) = body.next().await {
                match chunk {
                    Ok(chunk) => bytes += chunk.len() as u64,
                    Err(_) => break,
                }
            }
            (Some(status), bytes, Instant::now())
        });
        let deadline = Instant::now() + IN_FLIGHT_WAIT;
        while self.fleet.upstream.state.received() <= seen {
            assert!(
                Instant::now() < deadline,
                "the pinned `{alias}` request never reached the upstream, so the drain would not \
                 have found it in flight"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Pinned { handle }
    }
}

/// A request the harness holds against one replica across its drain.
struct Pinned {
    handle: tokio::task::JoinHandle<(Option<u16>, u64, Instant)>,
}

impl Pinned {
    async fn settle(self) -> Settled {
        let (status, bytes, ended) = self.handle.await.expect("the pinned request settles");
        Settled {
            status,
            bytes,
            ended,
        }
    }
}

struct Settled {
    status: Option<u16>,
    bytes: u64,
    ended: Instant,
}

impl Settled {
    fn ended_after(&self, signalled: Instant) -> Duration {
        self.ended.saturating_duration_since(signalled)
    }
}

/// One caller request as the driver saw it.
struct Outcome {
    correlation: CorrelationId,
    status: Option<u16>,
    replica: Option<String>,
    revision: Option<String>,
    latency_ms: f64,
    streamed: bool,
    /// A stream whose body failed part-way: the answer started and did not
    /// finish.
    torn: bool,
}

impl Outcome {
    fn ok(&self) -> bool {
        self.status
            .is_some_and(|status| (200..300).contains(&status))
    }
}

/// Offer one phase of load through the balancer, closed-loop over `workers`.
///
/// Owned arguments only, so a phase can be spawned to run *across* a drain
/// rather than before or after it.
async fn offer(
    base_url: String,
    scale: Scale,
    client: reqwest::Client,
    caller_key: String,
    first_correlation: u64,
) -> (Vec<Outcome>, Duration) {
    let next = Arc::new(AtomicUsize::new(0));
    let started = Instant::now();
    let workers = (0..scale.workers).map(|_| {
        let (client, base_url, caller_key, next) = (
            client.clone(),
            base_url.clone(),
            caller_key.clone(),
            next.clone(),
        );
        tokio::spawn(async move {
            let mut mine = Vec::new();
            loop {
                let index = next.fetch_add(1, Ordering::Relaxed);
                if index >= scale.requests_per_phase {
                    return mine;
                }
                let sequence = first_correlation
                    .checked_add(u64::try_from(index).expect("a request index fits u64"))
                    .expect("the rollout correlation sequence does not overflow");
                mine.push(
                    one(
                        &client,
                        &base_url,
                        &caller_key,
                        scale.streams(index),
                        CorrelationId::new(sequence),
                    )
                    .await,
                );
            }
        })
    });
    let outcomes = futures::future::join_all(workers)
        .await
        .into_iter()
        .flat_map(|worker| worker.expect("a traffic worker completes"))
        .collect();
    (outcomes, started.elapsed())
}

/// One request through the balancer, read to the last byte.
async fn one(
    client: &reqwest::Client,
    base_url: &str,
    caller_key: &str,
    streamed: bool,
    correlation: CorrelationId,
) -> Outcome {
    let alias = if streamed {
        alias::CHAT_SLOW
    } else {
        alias::CHAT
    };
    let at = Instant::now();
    let sent = client
        .post(format!("{base_url}/v1/chat/completions"))
        .bearer_auth(caller_key)
        .header("traceparent", correlation.traceparent())
        .json(&body(alias, streamed))
        .send()
        .await;
    let Ok(response) = sent else {
        return Outcome {
            correlation,
            status: None,
            replica: None,
            revision: None,
            latency_ms: at.elapsed().as_secs_f64() * 1000.0,
            streamed,
            torn: false,
        };
    };
    let status = response.status().as_u16();
    let header = |name: &str| {
        response
            .headers()
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned)
    };
    let (replica, revision) = (header(REPLICA_HEADER), header(REVISION_HEADER));
    let mut body = response.bytes_stream();
    let mut torn = false;
    while let Some(chunk) = body.next().await {
        if chunk.is_err() {
            torn = true;
            break;
        }
    }
    Outcome {
        correlation,
        status: Some(status),
        replica,
        revision,
        latency_ms: at.elapsed().as_secs_f64() * 1000.0,
        streamed,
        torn,
    }
}

fn body(alias: &str, stream: bool) -> Value {
    let prompt = match alias {
        alias::CHAT_SLOW => SLOW_PROMPT,
        alias::CHAT_LATE_HEADERS => BUFFERED_PROMPT,
        alias::CHAT_STALL_AFTER_BYTES => STALLED_PROMPT,
        _ => PROMPT,
    };
    json!({
        "model": alias,
        "messages": [{"role": "user", "content": prompt}],
        "stream": stream,
    })
}

async fn migration_versions(
    client: &tokio_postgres::Client,
    schema: &str,
) -> Vec<MigrationVersion> {
    client
        .query(
            &format!(
                "SELECT version, name, checksum FROM {schema}.axond_cp_schema_migration \
                 ORDER BY version"
            ),
            &[],
        )
        .await
        .expect("the migration ledger is readable")
        .into_iter()
        .map(|row| MigrationVersion {
            version: row.get(0),
            name: row.get(1),
            checksum: row.get(2),
        })
        .collect()
}

async fn connect(dsn: &str) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
        .await
        .expect("the fence connects to PostgreSQL");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// Run one operator command against the named binary and keep what an operator
/// would read.
fn axond_at(binary: &Path, args: &[&str], env: &[(String, String)]) -> CommandRecord {
    let mut command = Command::new(binary);
    let mut secrets = env.to_vec();
    if env.is_empty() {
        // Only the reduced stateless config references generated fixture
        // credentials. Heavy commands receive exclusively the same stateful
        // deployment environment as their serving fleet.
        secrets.extend(
            [
                ("GW_INBOUND_KEY", GATEWAY_KEY),
                (gateway::BOOT_KEY_ENV, "gate-boot-key"),
                ("GW_FAKE_OPENAI_KEY", gateway::OPENAI_KEY),
                ("GW_FAKE_ANTHROPIC_KEY", gateway::ANTHROPIC_KEY),
                (gateway::OPENAI_SECONDARY_ENV, gateway::OPENAI_KEY_SECONDARY),
                (
                    gateway::ANTHROPIC_SECONDARY_ENV,
                    gateway::ANTHROPIC_KEY_SECONDARY,
                ),
            ]
            .into_iter()
            .map(|(name, value)| (name.to_owned(), value.to_owned())),
        );
    }
    command.args(args).env("RUST_LOG", "warn");
    for (name, value) in &secrets {
        command.env(name, value);
    }
    let output = command.output().expect("the axond binary runs");
    let secret_refs: Vec<(&str, &str)> = secrets
        .iter()
        .map(|(name, value)| (name.as_str(), value.as_str()))
        .collect();
    CommandRecord {
        argv: std::iter::once(binary.display().to_string())
            .chain(args.iter().map(ToString::to_string))
            .collect(),
        exit_code: output.status.code(),
        succeeded: output.status.success(),
        output: redacted(
            &format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            ),
            &secret_refs,
        ),
    }
    // `args` are not part of the environment, so they are recorded verbatim:
    // the harness never passes a credential on the command line.
}

/// Command output goes into an uploaded artifact, so it may only carry what an
/// operator could paste into a ticket. Every value the command was given is
/// treated as a credential and replaced by the name it came from, and any
/// database URL is dropped whole — a failure path that echoes its environment
/// must not turn the artifact into a secret.
pub fn redacted(text: &str, secrets: &[(&str, &str)]) -> String {
    let mut out = text.to_owned();
    for (name, value) in secrets {
        // Short values would match unrelated text; nothing this harness passes
        // as a credential is that short.
        if value.len() >= 8 {
            out = out.replace(value, &format!("${{{name}}}"));
        }
    }
    scrub_urls(&out)
}

/// Replace every `scheme://…` run with a placeholder. Coarse on purpose: a DSN
/// the harness never learned (one the binary composed, say) is still a DSN.
fn scrub_urls(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(at) = rest.find("://") {
        // Resume after the delimiter itself, which is one `char` rather than one
        // byte: output carrying a non-ASCII glyph before a URL must be redacted,
        // not panicked on.
        let scheme_start = rest[..at]
            .char_indices()
            .rev()
            .find(|(_, c)| !c.is_ascii_alphanumeric() && *c != '+' && *c != '-' && *c != '.')
            .map_or(0, |(index, c)| index + c.len_utf8());
        let end = rest[at..]
            .find(|c: char| c.is_whitespace() || matches!(c, '"' | '\'' | ',' | ')'))
            .map_or(rest.len(), |offset| at + offset);
        out.push_str(&rest[..scheme_start]);
        out.push_str("${redacted-url}");
        rest = &rest[end..];
    }
    out.push_str(rest);
    out
}

/// Every executable/config pair used by the rollout, named by byte digest.
fn revisions(fleet: &Fleet, binaries: &Binaries) -> Vec<RevisionMeta> {
    let bind: SocketAddr = GATE_BIND.parse().expect("the gate address parses");
    let previous_binary = binary_meta_at_with_version_fallback(
        &binaries.previous,
        binaries
            .retained_release
            .as_ref()
            .map(|release| release.expected_version.as_str()),
    );
    let candidate_binary = binary_meta_at(&binaries.candidate);
    let desired_state_revision = fleet.desired_state_revision().map(ToOwned::to_owned);
    [
        (Revision::previous(), previous_binary.clone()),
        (Revision::compatibility(), candidate_binary.clone()),
        (Revision::next(), candidate_binary),
    ]
    .into_iter()
    .map(|(revision, binary)| {
        let mut config = fleet.config(bind, revision);
        if !fleet.is_stateful() {
            config = config.replace(&fleet.upstream.base_url, "http://127.0.0.1:UPSTREAM_PORT");
        }
        RevisionMeta {
            label: revision.label.to_owned(),
            distinct_binary: binary.sha256 != previous_binary.sha256,
            binary,
            config: ConfigMeta {
                sha256: sha256_hex(config.as_bytes()),
                normalized_toml: config,
            },
            desired_state_revision: desired_state_revision.clone(),
            exclusive_aliases: match (fleet.is_stateful(), revision.label) {
                (true, _) => Vec::new(),
                (false, NEXT) => vec![NEXT_ONLY_ALIAS.to_owned()],
                _ => Vec::new(),
            },
        }
    })
    .collect()
}

/// Everything the balancer and the fleet know about each replica, live or gone.
fn fleet_records(harness: &Harness, drains: &[DrainRecord]) -> Vec<ReplicaRecord> {
    let usage: BTreeMap<&str, u64> = drains
        .iter()
        .map(|drain| (drain.replica.as_str(), drain.usage_records_flushed))
        .collect();
    harness
        .ingress
        .state
        .members()
        .iter()
        .map(|member| {
            let live = harness
                .fleet
                .replicas()
                .iter()
                .find(|replica| replica.id == member.id);
            ReplicaRecord {
                id: member.id.clone(),
                revision: member.revision.clone(),
                admitted_at_ms: member.admitted_at().map(|at| at.as_millis()),
                admission_took_ms: harness
                    .admissions
                    .get(&member.id)
                    .map(|took| took.as_millis()),
                withdrawn_at_ms: member.withdrawn_at().map(|at| at.as_millis()),
                requests_served: member.forwards(),
                requests_after_withdrawal: member.forwards_after_withdrawal(),
                refusals: member.refusals(),
                usage_records: live.map_or_else(
                    || usage.get(member.id.as_str()).copied().unwrap_or_default(),
                    |replica| replica.process.usage_records().len() as u64,
                ),
                retired: live.is_none(),
            }
        })
        .collect()
}

fn ledger(
    harness: &Harness,
    records: &[Value],
    trace_witness: &TraceWitnessSnapshot,
) -> LossLedger {
    let mut by_status: BTreeMap<String, u64> = BTreeMap::new();
    for record in records {
        let status = record["status"].as_str().unwrap_or("unknown").to_owned();
        *by_status.entry(status).or_default() += 1;
    }
    let callers = harness.ingress.state.callers();
    let mut refusals: BTreeMap<String, u64> = BTreeMap::new();
    for caller in &callers {
        for replica in caller.draining_refusals() {
            *refusals.entry(replica.to_owned()).or_default() += 1;
        }
    }
    let reconciliation = reconcile(
        &harness.expected_usage,
        &harness.fleet.usage_records_by_replica(),
        &refusals,
    );
    let trace_exports = trace_witness.exports;
    let trace_identities = trace_witness.identities.iter().cloned().collect::<Vec<_>>();
    let trace_export_replicas = trace_identities
        .iter()
        .map(|(replica, _)| replica.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let otlp_trace_identities = trace_identities
        .into_iter()
        .map(|(replica, trace_id)| TraceExportIdentity { replica, trace_id })
        .collect();
    let expected_non_usage_trace_identities = harness.expected_non_usage_trace_identities();
    let draining_refusal_attempts = harness.draining_refusal_attempts();
    let failed_ingress_attempts = harness.failed_ingress_attempts();
    let expected_trace_identities = reconciliation
        .expected
        .iter()
        .map(|identity| (identity.replica.clone(), identity.trace_id.clone()))
        .chain(
            expected_non_usage_trace_identities
                .iter()
                .map(|identity| (identity.replica.clone(), identity.trace_id.clone())),
        )
        .collect();
    let observed_trace_identities = trace_witness.identities.clone();
    let unexpected_otlp_trace_identities = classify_unexpected_trace_identities(
        &expected_trace_identities,
        &observed_trace_identities,
        &failed_ingress_attempts,
    );
    let usage_reconciliation = UsageReconciliation {
        mode: EXACT_TRACE_RECONCILIATION.to_owned(),
        exact_trace_replicas: reconciliation.exact_trace_replicas.clone(),
        retained_trace_context: RETAINED_TRACE_CONTEXT.to_owned(),
        otlp_trace_exports: trace_exports,
        otlp_trace_export_replicas: trace_export_replicas,
        expected_non_usage_trace_identities,
        otlp_trace_identities,
        unexpected_otlp_trace_identities,
        otlp_trace_collection_errors: trace_witness.collection_errors.clone(),
    };
    LossLedger {
        caller_requests: callers.len() as u64,
        usage_records_missing: reconciliation.missing,
        usage_records_surplus: reconciliation.unexpected,
        usage_identity_duplicates: reconciliation.identity_duplicates,
        usage_record_id_duplicates: reconciliation.request_id_duplicates,
        usage_status_mismatches: reconciliation.status_mismatches,
        usage_records_unidentified: reconciliation.unidentified,
        expected_usage_identities: reconciliation.expected,
        observed_usage_identities: reconciliation.observed,
        per_replica: reconciliation.per_replica,
        offered: harness.traffic.iter().map(|phase| phase.offered).sum(),
        answered: harness.traffic.iter().map(|phase| phase.answered).sum(),
        errors: harness.traffic.iter().map(|phase| phase.errors).sum(),
        unanswered: harness.traffic.iter().map(|phase| phase.unanswered).sum(),
        torn_streams: harness.traffic.iter().map(|phase| phase.torn_streams).sum(),
        unavailable: harness.ingress.state.unavailable(),
        usage_records_expected: harness.expected_usage.len() as u64,
        usage_records_observed: records.len() as u64,
        usage_records_distinct: reconciliation.request_ids_distinct,
        usage_reconciliation,
        draining_refusal_attempts,
        failed_ingress_attempts,
        // A typed drain refusal happens outside the accepted request path and
        // owes no usage row. It remains a routing diagnostic, never a credit
        // that can excuse an otherwise unexpected record.
        usage_records_retry_duplicates: 0,
        refusals_retried: refusals.values().sum(),
        usage_by_status: by_status,
        upstream_streams_open_at_end: harness.fleet.upstream.state.open_streams(),
    }
}

pub fn classify_unexpected_trace_identities(
    expected: &BTreeSet<(String, String)>,
    observed: &BTreeSet<(String, String)>,
    failed_attempts: &[FailedIngressAttempt],
) -> Vec<UnexpectedTraceIdentity> {
    let reasons = failed_attempts
        .iter()
        .map(|attempt| {
            (
                (attempt.replica.clone(), attempt.trace_id.clone()),
                attempt.reason.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    observed
        .difference(expected)
        .map(|(replica, trace_id)| UnexpectedTraceIdentity {
            replica: replica.clone(),
            trace_id: trace_id.clone(),
            reason: reasons
                .get(&(replica.clone(), trace_id.clone()))
                .cloned()
                .unwrap_or_else(|| "unattributed".to_owned()),
        })
        .collect()
}

#[derive(Debug)]
pub struct ReconciledUsage {
    pub expected: Vec<ExpectedUsageIdentity>,
    pub observed: Vec<ObservedUsageIdentity>,
    pub per_replica: Vec<ReplicaUsage>,
    pub missing: u64,
    pub unexpected: u64,
    pub identity_duplicates: u64,
    pub request_id_duplicates: u64,
    pub request_ids_distinct: u64,
    pub status_mismatches: u64,
    pub unidentified: u64,
    pub exact_trace_replicas: Vec<String>,
}

#[derive(Default)]
struct ReplicaReconciliation {
    expected: u64,
    observed: u64,
    missing: u64,
    unexpected: u64,
    identity_duplicates: u64,
    status_mismatches: u64,
    unidentified: u64,
}

/// Reconcile exact caller traces against exact usage rows.
///
/// The identity is `(replica, trace_id)`, and status is compared separately so
/// a terminal-status rewrite is reported as such rather than hidden as a
/// missing row plus a surplus row. Reconciliation is a multiset operation: an
/// extra row with the right trace is still a duplicate, and a different trace
/// on the same replica can never fill a missing expected trace.
pub fn reconcile(
    expected: &[ExpectedUsageIdentity],
    records: &BTreeMap<String, Vec<Value>>,
    refusals: &BTreeMap<String, u64>,
) -> ReconciledUsage {
    let mut expected = expected.to_vec();
    expected.sort();
    let mut observed: Vec<ObservedUsageIdentity> = records
        .iter()
        .flat_map(|(replica, rows)| {
            rows.iter().map(|record| ObservedUsageIdentity {
                replica: replica.clone(),
                trace_id: record["trace_id"].as_str().map(ToOwned::to_owned),
                status: record["status"].as_str().map(ToOwned::to_owned),
                request_id: record["request_id"].as_str().map(ToOwned::to_owned),
            })
        })
        .collect();
    observed.sort();

    // The map itself is the fleet scope and deliberately includes replicas
    // with zero rows. Seeding it first prevents an idle process from vanishing
    // from the exact-trace disclosure and OTLP identity gate.
    let mut replicas: BTreeMap<String, ReplicaReconciliation> = records
        .keys()
        .chain(refusals.keys())
        .cloned()
        .map(|replica| (replica, ReplicaReconciliation::default()))
        .collect();
    let mut expected_by_identity: BTreeMap<(String, String), String> = BTreeMap::new();
    for row in &expected {
        let metrics = replicas.entry(row.replica.clone()).or_default();
        metrics.expected += 1;
        let identity = (row.replica.clone(), row.trace_id.clone());
        if expected_by_identity
            .insert(identity, row.status.clone())
            .is_some()
        {
            metrics.identity_duplicates += 1;
        }
    }

    let mut observed_by_identity: BTreeMap<(String, String), Vec<&ObservedUsageIdentity>> =
        BTreeMap::new();
    let mut request_ids: BTreeMap<&str, u64> = BTreeMap::new();
    for row in &observed {
        let metrics = replicas.entry(row.replica.clone()).or_default();
        metrics.observed += 1;
        let trace_and_status_identified = row.trace_id.as_deref().is_some_and(canonical_trace_id)
            && row.status.as_deref().is_some_and(canonical_usage_status);
        let request_id_identified = row.request_id.as_deref().is_some_and(canonical_request_id);
        if let Some(request_id) = row.request_id.as_deref().filter(|_| request_id_identified) {
            *request_ids.entry(request_id).or_default() += 1;
        }
        if !request_id_identified || !trace_and_status_identified {
            metrics.unidentified += 1;
        }
        match (row.trace_id.as_deref(), row.status.as_deref()) {
            (Some(trace_id), Some(status))
                if canonical_trace_id(trace_id) && canonical_usage_status(status) =>
            {
                observed_by_identity
                    .entry((row.replica.clone(), trace_id.to_owned()))
                    .or_default()
                    .push(row);
            }
            _ => {
                metrics.unexpected += 1;
            }
        }
    }

    for ((replica, trace_id), expected_status) in &expected_by_identity {
        let metrics = replicas.entry(replica.clone()).or_default();
        let Some(rows) = observed_by_identity.get(&(replica.clone(), trace_id.clone())) else {
            metrics.missing += 1;
            continue;
        };
        if !rows
            .iter()
            .any(|row| row.status.as_deref() == Some(expected_status.as_str()))
        {
            metrics.status_mismatches += 1;
        }
        let extras = rows.len().saturating_sub(1) as u64;
        metrics.identity_duplicates += extras;
        metrics.unexpected += extras;
    }
    for ((replica, trace_id), rows) in &observed_by_identity {
        if expected_by_identity.contains_key(&(replica.clone(), trace_id.clone())) {
            continue;
        }
        let metrics = replicas.entry(replica.clone()).or_default();
        metrics.unexpected += rows.len() as u64;
        metrics.identity_duplicates += rows.len().saturating_sub(1) as u64;
    }

    let request_id_duplicates = request_ids
        .values()
        .map(|count| count.saturating_sub(1))
        .sum();
    let per_replica = replicas
        .into_iter()
        .map(|(replica, metrics)| ReplicaUsage {
            reconciliation: EXACT_TRACE_RECONCILIATION.to_owned(),
            caller_requests_answered: metrics.expected,
            usage_records: metrics.observed,
            caller_requests_refused_while_draining: refusals
                .get(&replica)
                .copied()
                .unwrap_or_default(),
            retry_duplicates: 0,
            missing: metrics.missing,
            unexplained_surplus: metrics.unexpected,
            identity_duplicates: metrics.identity_duplicates,
            status_mismatches: metrics.status_mismatches,
            unidentified: metrics.unidentified,
            replica,
        })
        .collect::<Vec<_>>();
    ReconciledUsage {
        missing: per_replica.iter().map(|row| row.missing).sum(),
        unexpected: per_replica.iter().map(|row| row.unexplained_surplus).sum(),
        identity_duplicates: per_replica.iter().map(|row| row.identity_duplicates).sum(),
        status_mismatches: per_replica.iter().map(|row| row.status_mismatches).sum(),
        unidentified: per_replica.iter().map(|row| row.unidentified).sum(),
        request_id_duplicates,
        request_ids_distinct: request_ids.len() as u64,
        exact_trace_replicas: per_replica
            .iter()
            .filter(|row| row.reconciliation == EXACT_TRACE_RECONCILIATION)
            .map(|row| row.replica.clone())
            .collect(),
        expected,
        observed,
        per_replica,
    }
}

fn canonical_trace_id(trace_id: &str) -> bool {
    trace_id.len() == 32
        && trace_id != "00000000000000000000000000000000"
        && trace_id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn canonical_usage_status(status: &str) -> bool {
    matches!(
        status,
        "ok" | "upstream_error" | "client_cancelled" | "partial" | "rejected"
    )
}

fn canonical_request_id(request_id: &str) -> bool {
    let Some(uuid) = request_id.strip_prefix("req_") else {
        return false;
    };
    let bytes = uuid.as_bytes();
    bytes.len() == 36
        && [8, 13, 18, 23]
            .into_iter()
            .all(|index| bytes[index] == b'-')
        && bytes[14] == b'7'
        && matches!(bytes[19], b'8' | b'9' | b'a' | b'b')
        && bytes.iter().enumerate().all(|(index, byte)| {
            [8, 13, 18, 23].contains(&index)
                || byte.is_ascii_digit()
                || (b'a'..=b'f').contains(byte)
        })
}

/// What the rollout cost in throughput while the fleet was short a replica.
/// Recorded, never asserted: it is the number a surge is sized from.
fn envelope(traffic: &[PhaseTraffic]) -> CapacityEnvelope {
    let mean = |select: fn(&PhaseTraffic) -> bool| {
        let phases: Vec<&PhaseTraffic> = traffic.iter().filter(|phase| select(phase)).collect();
        if phases.is_empty() {
            return (0.0, None);
        }
        let rps = phases.iter().map(|phase| phase.answered_rps).sum::<f64>() / phases.len() as f64;
        let p95 = phases
            .iter()
            .filter_map(|phase| phase.latency_ms.map(|latency| latency.p95))
            .fold(None::<f64>, |worst, p95| {
                Some(worst.map_or(p95, |w| w.max(p95)))
            });
        (rps, p95)
    };
    let (steady, steady_p95) = mean(|phase| phase.phase.starts_with("steady"));
    // `contains`, not `starts_with`: the rollback's drain is a replica short of
    // the fleet too, and leaving `rollback-drain` out would average the cost of
    // a rollout over two of the three windows that have it.
    let (degraded, degraded_p95) = mean(|phase| phase.phase.contains("drain"));
    CapacityEnvelope {
        steady_answered_rps: steady,
        degraded_answered_rps: degraded,
        degraded_fraction: if steady > 0.0 { degraded / steady } else { 0.0 },
        steady_latency_p95_ms: steady_p95,
        degraded_latency_p95_ms: degraded_p95,
    }
}

/// The manifest's thresholds, applied to what was measured.
fn verdicts(result: &RolloutResult) -> Vec<Verdict> {
    let thresholds = &result.scenario.thresholds;
    let drains = &result.drains;
    let worst = |select: fn(&DrainRecord) -> Option<u128>| {
        drains.iter().filter_map(select).max().unwrap_or_default() as f64
    };
    let mut verdicts = vec![
        Verdict::at_most(
            "max_requests_to_drained_replica",
            drains
                .iter()
                .map(|drain| {
                    drain
                        .requests_after_withdrawal
                        .max(drain.dispatches_beyond_drain_grace)
                })
                .max()
                .unwrap_or_default() as f64,
            thresholds.max_requests_to_drained_replica as f64,
        ),
        Verdict::at_most(
            "max_request_loss",
            (result.loss.unanswered + result.loss.errors + result.loss.torn_streams) as f64,
            thresholds.max_request_loss as f64,
        ),
        Verdict::at_most(
            "max_unavailable_responses",
            result.loss.unavailable as f64,
            thresholds.max_unavailable_responses as f64,
        ),
        Verdict::at_most(
            "max_usage_record_loss",
            result.loss.usage_records_missing as f64,
            thresholds.max_usage_record_loss as f64,
        ),
        // Retry duplicates are already discounted from the count loss is
        // measured against, so anything still in surplus is double accounting.
        Verdict::at_most(
            "unexplained_usage_record_surplus",
            result.loss.usage_records_surplus as f64,
            0.0,
        ),
        // The trace joins the exact caller event to the exact replica. A second
        // row under that identity is double accounting even if it minted a new
        // billing id.
        Verdict::at_most(
            "duplicate_usage_trace_identities",
            result.loss.usage_identity_duplicates as f64,
            0.0,
        ),
        Verdict::at_most(
            "usage_status_mismatches",
            result.loss.usage_status_mismatches as f64,
            0.0,
        ),
        Verdict::at_most(
            "unidentified_usage_records",
            result.loss.usage_records_unidentified as f64,
            0.0,
        ),
        // `request_id` is the independent billing event identity. Reusing it
        // remains a failure even when the trace ledger otherwise reconciles.
        Verdict::at_most(
            "duplicate_usage_record_ids",
            result.loss.usage_record_id_duplicates as f64,
            0.0,
        ),
        Verdict::at_least(
            "otlp_trace_context_exported",
            result
                .loss
                .usage_reconciliation
                .otlp_trace_export_replicas
                .len() as f64,
            result.loss.usage_reconciliation.exact_trace_replicas.len() as f64,
        ),
        Verdict::at_most(
            "otlp_trace_export_identity_mismatches",
            (result
                .loss
                .expected_usage_identities
                .iter()
                .map(|identity| (&identity.replica, &identity.trace_id))
                .chain(
                    result
                        .loss
                        .usage_reconciliation
                        .expected_non_usage_trace_identities
                        .iter()
                        .map(|identity| (&identity.replica, &identity.trace_id)),
                )
                .collect::<BTreeSet<_>>()
                .symmetric_difference(
                    &result
                        .loss
                        .usage_reconciliation
                        .otlp_trace_identities
                        .iter()
                        .map(|identity| (&identity.replica, &identity.trace_id))
                        .collect::<BTreeSet<_>>(),
                )
                .count()
                + result
                    .loss
                    .usage_reconciliation
                    .otlp_trace_collection_errors
                    .len()) as f64,
            0.0,
        ),
        // Every drain must have been *observed* to leave rotation. A drain with
        // no removal time is a balancer that never noticed, which a maximum over
        // the ones that did notice would hide.
        Verdict::at_most(
            "readiness_removal_observed",
            drains
                .iter()
                .filter(|drain| drain.readiness_removed_after_ms.is_none())
                .count() as f64,
            0.0,
        ),
        Verdict::at_most(
            "max_readiness_removal_ms",
            worst(|drain| drain.readiness_removed_after_ms),
            thresholds.max_readiness_removal_ms as f64,
        ),
        // How long a replacement took to start carrying traffic, not when in the
        // run it did so: the offset grows with every phase, the admission does
        // not.
        Verdict::at_most(
            "max_replacement_admission_ms",
            result
                .fleet
                .iter()
                .filter_map(|replica| replica.admission_took_ms)
                .max()
                .unwrap_or_default() as f64,
            thresholds.max_replacement_admission_ms as f64,
        ),
        Verdict::at_most(
            "bounded_termination",
            drains
                .iter()
                .filter(|drain| drain.exited_after_ms.is_none())
                .count() as f64,
            0.0,
        ),
        Verdict::at_most(
            "max_drain_exit_slack_ms",
            drains
                .iter()
                .filter_map(|drain| {
                    Some(drain.exited_after_ms?.saturating_sub(drain.exit_budget_ms))
                })
                .max()
                .unwrap_or_default() as f64,
            thresholds.max_drain_exit_slack_ms as f64,
        ),
        Verdict::at_least(
            "min_mixed_version_requests",
            result
                .mixed_version
                .previous_requests
                .min(result.mixed_version.next_requests) as f64,
            thresholds.min_mixed_version_requests as f64,
        ),
        // A buffered request the replica admitted before the signal is finished
        // rather than dropped.
        Verdict::at_most(
            "buffered_requests_completed_during_drain",
            drains
                .iter()
                .filter(|drain| {
                    !drain
                        .buffered_in_flight
                        .status
                        .is_some_and(|status| (200..300).contains(&status))
                })
                .count() as f64,
            0.0,
        ),
        // A stream the upstream never ends is ended by the deadline, and only
        // after it had relayed something: a stream cut before any byte is a
        // different failure with the same shape.
        Verdict::at_most(
            "streams_cut_within_deadline",
            drains
                .iter()
                .filter(|drain| {
                    !drain.stream_in_flight.within_deadline
                        || drain.stream_in_flight.relayed_bytes == 0
                })
                .count() as f64,
            0.0,
        ),
        // The partial stream is accounted for, and accounted for as partial.
        Verdict::at_most(
            "partial_streams_accounted",
            drains
                .iter()
                .filter(|drain| {
                    drain.stream_in_flight.usage_status.as_deref() != Some("client_cancelled")
                })
                .count() as f64,
            0.0,
        ),
        Verdict::at_most(
            "upstream_streams_open_at_end",
            result.loss.upstream_streams_open_at_end as f64,
            0.0,
        ),
        Verdict::at_most(
            "migration_gate_passed",
            f64::from(u8::from(!result.migration.gate_passed)),
            0.0,
        ),
        Verdict::at_most(
            "rollback_matches_migration_classification",
            f64::from(u8::from(
                if result.rollback.migrated_layout_fence.expected_refused {
                    result.rollback.compatible_patch_rollback.performed
                        || !result.rollback.migrated_layout_fence.refused
                } else {
                    !result.rollback.compatible_patch_rollback.performed
                        || !result.rollback.compatible_patch_rollback.served_traffic
                        || result.rollback.migrated_layout_fence.refused
                },
            )),
            0.0,
        ),
    ];
    if result.scenario.tier == "heavy" {
        let mixed = &result.mixed_version;
        let projected_revision = result
            .revisions
            .iter()
            .find(|revision| revision.label == PREVIOUS)
            .and_then(|revision| revision.desired_state_revision.as_deref());
        verdicts.push(Verdict::at_most(
            "mixed_version_shared_stateful_serving",
            f64::from(u8::from(
                mixed.shared_stateful_revision.is_none()
                    || mixed.shared_stateful_revision.as_deref() != projected_revision
                    || mixed.shared_alias.as_deref() != Some(alias::CHAT)
                    || !mixed.previous_serves_shared_alias
                    || !mixed.next_serves_shared_alias
                    || mixed.previous_requests == 0
                    || mixed.next_requests == 0,
            )),
            0.0,
        ));
    } else {
        // Reduced diagnostics keep the observable candidate-only capability
        // split. Desired state is global in heavy stateful mode, so applying
        // this contract there would fabricate per-replica configuration.
        verdicts.push(Verdict::at_most(
            "mixed_version_shared_stateful_serving",
            f64::from(u8::from(
                !(result.mixed_version.next_serves_exclusive_alias
                    && result.mixed_version.previous_refuses_exclusive_alias),
            )),
            0.0,
        ));
    }
    // The fence is a gate only where it could be evaluated; an artifact from a
    // runner with no PostgreSQL says it was skipped rather than passing it.
    if result.rollback.migrated_layout_fence.evaluated {
        let fence = &result.rollback.migrated_layout_fence;
        verdicts.push(Verdict::at_most(
            "migration_fence_matches_classification",
            f64::from(u8::from(if fence.expected_refused {
                !(fence.cold_start_attempted
                    && !fence.cold_start_reached_readiness
                    && fence.cold_start_exit_code.is_some_and(|code| code != 0)
                    && fence.refused
                    && fence.refusal_names_newer_build)
            } else {
                !(fence.cold_start_attempted
                    && fence.cold_start_reached_readiness
                    && !fence.refused)
            })),
            0.0,
        ));
    }
    if result.scenario.tier == "heavy" {
        let binary_digests: BTreeSet<&str> = result
            .revisions
            .iter()
            .map(|revision| revision.binary.sha256.as_str())
            .collect();
        let previous = result
            .revisions
            .iter()
            .find(|revision| revision.label == PREVIOUS);
        let compatibility = result
            .revisions
            .iter()
            .find(|revision| revision.label == COMPATIBILITY);
        let next = result
            .revisions
            .iter()
            .find(|revision| revision.label == NEXT);
        let compatibility_served = result
            .traffic
            .iter()
            .find(|phase| phase.phase == "candidate-on-previous-config")
            .and_then(|phase| phase.by_revision.get(COMPATIBILITY))
            .is_some_and(|requests| *requests > 0);
        let exact_phases = previous.zip(compatibility).zip(next).is_some_and(
            |((previous, compatibility), next)| {
                let shared_revision = previous.desired_state_revision.as_deref();
                previous.config.sha256 == compatibility.config.sha256
                    && compatibility.config.sha256 == next.config.sha256
                    && previous.binary.sha256 != compatibility.binary.sha256
                    && compatibility.binary.sha256 == next.binary.sha256
                    && shared_revision.is_some()
                    && compatibility.desired_state_revision.as_deref() == shared_revision
                    && next.desired_state_revision.as_deref() == shared_revision
                    && compatibility_served
            },
        );
        verdicts.extend([
            Verdict::at_most(
                "heavy_rollout_is_promotable",
                f64::from(u8::from(!result.run.promotable)),
                0.0,
            ),
            Verdict::at_most(
                "heavy_rollout_uses_two_binary_digests",
                f64::from(u8::from(binary_digests.len() != 2)),
                0.0,
            ),
            Verdict::at_most(
                "candidate_serves_shared_stateful_revision",
                f64::from(u8::from(!exact_phases)),
                0.0,
            ),
            Verdict::at_most(
                "migration_matrix_evaluated",
                f64::from(u8::from(!result.migration.matrix.evaluated)),
                0.0,
            ),
        ]);
    }
    verdicts
}

/// The usage status the replica settled a pinned alias' request as.
fn usage_status(drained: &Drained, alias: &str) -> Option<String> {
    drained
        .usage_records
        .iter()
        .find(|record| record["model"] == alias)
        .and_then(|record| record["status"].as_str())
        .map(ToOwned::to_owned)
}

fn balancer_counts(forwards: &[Forward]) -> BTreeMap<String, u64> {
    let mut counts: BTreeMap<String, u64> = BTreeMap::new();
    for forward in forwards {
        *counts.entry(forward.replica.clone()).or_default() += 1;
    }
    counts
}

fn summary_of(traffic: &PhaseTraffic) -> String {
    format!(
        "{} offered, {} answered, {} errors, {} unanswered, {} retried, across {:?}",
        traffic.offered,
        traffic.answered,
        traffic.errors,
        traffic.unanswered,
        traffic.retried,
        traffic.by_replica,
    )
}

fn rate(count: u64, elapsed: Duration) -> f64 {
    let seconds = elapsed.as_secs_f64();
    if seconds > 0.0 {
        count as f64 / seconds
    } else {
        0.0
    }
}

fn verdict_word(passed: bool) -> &'static str {
    if passed { "passed" } else { "FAILED" }
}

/// A directory of this run's own, under the target directory rather than the
/// system temp: an operator reading a failed run wants the config it checked.
fn scratch_dir(name: &str) -> PathBuf {
    let dir = crate::support::capacity::manifest::workspace_root()
        .join("target/rollout/scratch")
        .join(format!("{name}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("the scratch directory is writable");
    dir
}

/// Write a config the operator commands will read. Mode 0600, because
/// `axond check preflight` fails a config another account can rewrite — the
/// harness has to satisfy the gate it is qualifying.
fn write_config(dir: &Path, name: &str, text: &str) -> String {
    let path = dir.join(name);
    std::fs::write(&path, text).expect("the config is written");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            .expect("the config is owner-only");
    }
    path.display().to_string()
}

/// The run's chronological record.
struct Timeline {
    started: Instant,
    events: Vec<Event>,
}

impl Timeline {
    fn new(started: Instant) -> Self {
        Self {
            started,
            events: Vec::new(),
        }
    }

    fn at(&mut self, phase: &str, kind: &str, detail: impl Into<String>) {
        self.events.push(Event {
            at_ms: self.started.elapsed().as_millis(),
            phase: phase.to_owned(),
            kind: kind.to_owned(),
            detail: detail.into(),
        });
    }
}
