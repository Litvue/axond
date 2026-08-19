//! The fleet: several real `axond` processes, at two revisions, sharing one
//! deterministic fake upstream.
//!
//! Two things here are worth being explicit about.
//!
//! **What a "revision" is.** A rollout is only interesting if the two builds
//! differ in a way a caller can observe. Heavy qualification supplies separate
//! retained and candidate executables but gives both the same stateful bootstrap
//! and immutable desired-state revision; exact response attribution proves both
//! serve it. The reduced diagnostic keeps a candidate-only alias as its cheap
//! observable split. Every binary, config, and durable revision hash is
//! recorded, so an artifact never claims two builds when it ran one.
//!
//! **Why usage records are harvested at exit.** A drained replica's process is
//! gone by the time the run ends, and its records are the ones most likely to be
//! lost. They are taken from the process' stdout the moment it exits and kept
//! with the fleet, so the loss ledger covers replicas that no longer exist.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::support::fault::collector::Collector;
use crate::support::gateway::{Axond, alias};
use crate::support::upstream::{FakeUpstream, target};

use super::manifest::ShutdownBounds;
use super::stateful::{
    ColdStartAttempt, Deployment as StatefulDeployment, MigrationTarget, Process as StatefulProcess,
};

/// The revision labels an artifact reports.
pub const PREVIOUS: &str = "previous";
pub const COMPATIBILITY: &str = "candidate-previous-config";
pub const NEXT: &str = "next";

/// The reduced stateless diagnostic's next-only alias. Stateful desired state
/// is global, so the heavy lane never publishes or probes this split.
pub const NEXT_ONLY_ALIAS: &str = "chat-next-only";

/// How long a retiring replica's output pipe is given to reach EOF once the
/// process is gone. Generous: it is only ever waited out when a reader thread
/// is starved, and the alternative is a phantom lost usage record.
const OUTPUT_SETTLE: Duration = Duration::from_secs(5);
/// Five batch-processor intervals after the last rollout trace. The harness
/// configures a 200 ms OTLP schedule, so no caller-domain span activity for
/// this window is a settled exporter snapshot rather than the first expected
/// subset. Duplicate caller spans reset the window; readiness spans do not.
const TRACE_QUIESCENCE: Duration = Duration::from_secs(1);
const ROLLOUT_TRACE_DOMAIN_PREFIX: &str = "61786f6e642d726f";

/// Stateless failover bounds. Stateful mode rejects failover settings in its
/// bootstrap TOML and the current desired-state schema has no deployment-wide
/// failover-policy resource, so stateful replicas use the shipped defaults.
/// Keep the stateless fixture's historical bounds explicit and separate.
const STATELESS_FAILOVER_TUNING: &str = r"
[failover]
max_attempts = 1
overall_timeout_ms = 60000
";

/// The transport bounds every replica is booted with. Written out rather than
/// defaulted so the recorded config hash pins them: a later change to a shipped
/// default must not silently move a qualification result.
const TRANSPORT_TUNING: &str = r"
[transport]
connect_timeout_ms = 10000
response_header_timeout_ms = 30000
buffered_body_timeout_ms = 30000
stream_idle_timeout_ms = 30000
";

/// Which build a replica is running.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Revision {
    pub label: &'static str,
}

impl Revision {
    pub const fn previous() -> Self {
        Self { label: PREVIOUS }
    }

    pub const fn next() -> Self {
        Self { label: NEXT }
    }

    /// The candidate executable reading the same bootstrap and desired revision
    /// as the retained fleet before it enters the replacement sequence.
    pub const fn compatibility() -> Self {
        Self {
            label: COMPATIBILITY,
        }
    }

    /// The stateless diagnostic config this revision adds on top of the shared
    /// bounds. Stateful deployment bypasses this candidate-only model.
    pub fn stateless_tuning(self, shutdown: ShutdownBounds) -> String {
        let mut tuning = format!(
            "{STATELESS_FAILOVER_TUNING}{TRANSPORT_TUNING}{}",
            shutdown.toml()
        );
        if self.label == NEXT {
            tuning.push_str(&format!(
                "\n[[model]]\nname = \"{NEXT_ONLY_ALIAS}\"\ntargets = [ {{ provider = \
                 \"fake-openai\", model = \"{}\", price = {{ \
                 input_microdollars_per_million = {}, output_microdollars_per_million = {} }} }} \
                 ]\n",
                target::CHAT,
                crate::support::gateway::INPUT_PRICE,
                crate::support::gateway::OUTPUT_PRICE,
            ));
        }
        tuning
    }

    /// Bootstrap-only tuning for a stateful replica. Serving resources come
    /// from the durable revision; failover stays absent because stateful
    /// bootstrap rejects it and therefore uses the shipped defaults.
    pub fn stateful_tuning(shutdown: ShutdownBounds) -> String {
        format!("{TRANSPORT_TUNING}{}", shutdown.toml())
    }
}

/// The two launch paths share one fleet contract. Reduced diagnostics keep the
/// established generated stateless config; heavy qualification uses a config
/// containing only bootstrap references and projects all serving state from
/// Postgres.
pub enum Process {
    Stateless(Axond),
    Stateful(StatefulProcess),
}

impl Process {
    fn base_url(&self) -> &str {
        match self {
            Self::Stateless(process) => &process.base_url,
            Self::Stateful(process) => &process.base_url,
        }
    }

    pub fn usage_records(&self) -> Vec<Value> {
        match self {
            Self::Stateless(process) => process.usage_records(),
            Self::Stateful(process) => process.usage_records(),
        }
    }

    fn terminate(&self) {
        match self {
            Self::Stateless(process) => process.terminate(),
            Self::Stateful(process) => process.terminate(),
        }
    }

    async fn await_exit(&mut self, within: Duration) -> Option<std::process::ExitStatus> {
        match self {
            Self::Stateless(process) => process.await_exit(within).await,
            Self::Stateful(process) => process.await_exit(within).await,
        }
    }

    async fn settle_output(&self, within: Duration) {
        match self {
            Self::Stateless(process) => process.settle_output(within).await,
            Self::Stateful(process) => process.settle_output(within).await,
        }
    }

    fn output(&self) -> String {
        match self {
            Self::Stateless(process) => process.output(),
            Self::Stateful(process) => process.output(),
        }
    }
}

/// One running replica.
pub struct Replica {
    pub id: String,
    pub revision: Revision,
    pub process: Process,
    /// Dedicated to this process, so no other replica can satisfy its trace
    /// witness through a shared receiver.
    pub collector: Collector,
}

impl Replica {
    pub fn base_url(&self) -> &str {
        self.process.base_url()
    }
}

/// A replica that has been drained, with the accounting it flushed on its way
/// out.
pub struct Retired {
    pub id: String,
    pub revision: Revision,
    pub usage_records: Vec<Value>,
    pub collector: Collector,
}

pub struct Fleet {
    pub upstream: FakeUpstream,
    shutdown: ShutdownBounds,
    previous_binary: PathBuf,
    candidate_binary: PathBuf,
    replicas: Vec<Replica>,
    retired: Vec<Retired>,
    stateful: Option<StatefulDeployment>,
    started: usize,
}

pub struct TraceWitnessSnapshot {
    pub exports: u64,
    pub identities: BTreeSet<(String, String)>,
}

#[derive(Clone, Default)]
struct TraceIdentityDelta {
    identities: BTreeSet<(String, String)>,
    caller_spans_seen: u64,
}

impl Fleet {
    pub async fn start(
        shutdown: ShutdownBounds,
        previous_binary: &Path,
        candidate_binary: &Path,
        stateful: bool,
    ) -> Self {
        let upstream = FakeUpstream::start().await;
        let stateful = if stateful {
            Some(StatefulDeployment::create(&upstream.base_url).await)
        } else {
            None
        };
        Self {
            upstream,
            shutdown,
            previous_binary: previous_binary.to_owned(),
            candidate_binary: candidate_binary.to_owned(),
            replicas: Vec::new(),
            retired: Vec::new(),
            stateful,
            started: 0,
        }
    }

    pub fn is_stateful(&self) -> bool {
        self.stateful.is_some()
    }

    pub fn config(&self, bind: std::net::SocketAddr, revision: Revision) -> String {
        match &self.stateful {
            Some(deployment) => deployment.config(bind, self.shutdown),
            None => crate::support::gateway::config_toml(
                bind,
                &self.upstream.base_url,
                &revision.stateless_tuning(self.shutdown),
                "",
            ),
        }
    }

    pub fn migration_target(&self) -> Option<MigrationTarget> {
        self.stateful
            .as_ref()
            .map(StatefulDeployment::migration_target)
    }

    pub async fn prepare_stateful(&mut self) {
        if let Some(deployment) = self.stateful.as_mut() {
            deployment
                .prepare(&self.previous_binary, self.shutdown)
                .await;
        }
    }

    pub fn desired_state_revision(&self) -> Option<&str> {
        self.stateful
            .as_ref()
            .and_then(StatefulDeployment::revision)
    }

    /// The token every caller-side request must use. Stateful replicas only
    /// accept the durable principal projected into their shared revision;
    /// reduced stateless replicas retain the generated fixture key.
    pub fn caller_key(&self) -> &str {
        self.stateful.as_ref().map_or(
            crate::support::gateway::GATEWAY_KEY,
            StatefulDeployment::workload_key,
        )
    }

    pub async fn previous_cold_start(&self) -> Option<ColdStartAttempt> {
        match &self.stateful {
            Some(deployment) => Some(
                deployment
                    .cold_start_attempt(&self.previous_binary, self.shutdown)
                    .await,
            ),
            None => None,
        }
    }

    /// Boot one more replica at `revision`. The id counts booted processes
    /// rather than live ones, so a replacement is never confused with the
    /// replica it replaced.
    pub async fn admit(&mut self, revision: Revision) -> &Replica {
        let id = format!("{}-{}", revision.label, self.started);
        self.started += 1;
        let binary = if revision.label == PREVIOUS {
            &self.previous_binary
        } else {
            &self.candidate_binary
        };
        let collector = Collector::start().await;
        let telemetry_env = vec![
            (
                "OTEL_EXPORTER_OTLP_ENDPOINT".to_owned(),
                collector.endpoint.clone(),
            ),
            (
                "OTEL_EXPORTER_OTLP_PROTOCOL".to_owned(),
                "http/protobuf".to_owned(),
            ),
            ("OTEL_BSP_SCHEDULE_DELAY".to_owned(), "200".to_owned()),
            ("OTEL_METRIC_EXPORT_INTERVAL".to_owned(), "1000".to_owned()),
            ("RUST_LOG".to_owned(), "warn,axond=info".to_owned()),
            ("AXOND_INSTANCE_ID".to_owned(), id.clone()),
        ];
        let process = match &self.stateful {
            Some(deployment) => Process::Stateful(
                deployment
                    .start_replica(binary, self.shutdown, &telemetry_env)
                    .await,
            ),
            None => {
                let env = telemetry_env
                    .iter()
                    .map(|(name, value)| (name.as_str(), value.as_str()))
                    .collect::<Vec<_>>();
                Process::Stateless(
                    Axond::start_with_binary_and_env(
                        &self.upstream.base_url,
                        &revision.stateless_tuning(self.shutdown),
                        binary,
                        &env,
                    )
                    .await,
                )
            }
        };
        self.replicas.push(Replica {
            id,
            revision,
            process,
            collector,
        });
        self.replicas.last().expect("the replica was pushed")
    }

    pub fn replicas(&self) -> &[Replica] {
        &self.replicas
    }

    pub fn replica(&self, id: &str) -> &Replica {
        self.replicas
            .iter()
            .find(|replica| replica.id == id)
            .unwrap_or_else(|| panic!("{id} is not a live replica"))
    }

    /// The first live replica at `revision`, which is the one a rolling
    /// deployment takes next.
    pub fn oldest(&self, revision: Revision) -> Option<&Replica> {
        self.replicas
            .iter()
            .find(|replica| replica.revision == revision)
    }

    /// `SIGTERM` one replica, exactly as an orchestrator does when it takes it
    /// out of rotation. Separate from [`Fleet::retire`] so a caller can watch
    /// the balancer and the process over the same window: the interesting part
    /// of a drain is what happens between the signal and the exit.
    pub fn signal(&self, id: &str) {
        self.replica(id).process.terminate();
    }

    /// Wait for a signalled replica to go, bounded by what the process itself
    /// promises, and keep what it flushed. `took` is `None` when the bound was
    /// exceeded, which is the failure a bounded shutdown exists to prevent.
    pub async fn retire(&mut self, id: &str, signalled: Instant, slack: Duration) -> Drained {
        let index = self
            .replicas
            .iter()
            .position(|replica| replica.id == id)
            .unwrap_or_else(|| panic!("{id} is not a live replica"));
        // Two different bounds: the one the process advertises, which is what a
        // drain is judged against, and the longer one the harness is willing to
        // wait before calling the termination unbounded. Folding the slack into
        // the reported budget would make the overrun zero by construction.
        let budget = self.shutdown.budget();
        let mut replica = self.replicas.remove(index);
        let status = replica
            .process
            .await_exit((budget + slack).saturating_sub(signalled.elapsed()))
            .await;
        let took = status.map(|_| signalled.elapsed());
        // The records a replica flushes on its way out are written just before
        // it exits, so the pipe is drained before the buffer is read.
        replica.process.settle_output(OUTPUT_SETTLE).await;
        let output = replica.process.output();
        let usage_records = replica.process.usage_records();
        self.retired.push(Retired {
            id: replica.id.clone(),
            revision: replica.revision,
            usage_records: usage_records.clone(),
            collector: replica.collector,
        });
        Drained {
            id: replica.id,
            revision: replica.revision,
            took,
            clean: status.is_some_and(|status| status.success()),
            budget,
            usage_records,
            output,
        }
    }

    /// Every usage record the fleet has emitted, live replicas and retired ones
    /// together. The denominator of the loss ledger.
    pub fn usage_records(&self) -> Vec<Value> {
        self.replicas
            .iter()
            .flat_map(|replica| replica.process.usage_records())
            .chain(
                self.retired
                    .iter()
                    .flat_map(|retired| retired.usage_records.clone()),
            )
            .collect()
    }

    /// The same records, kept under the replica that wrote them. Replica is one
    /// component of the exact correlation identity, so neither a row from
    /// another replica nor an unrelated row on this replica can fill a hole.
    pub fn usage_records_by_replica(&self) -> BTreeMap<String, Vec<Value>> {
        self.replicas
            .iter()
            .map(|replica| (replica.id.clone(), replica.process.usage_records()))
            .chain(
                self.retired
                    .iter()
                    .map(|retired| (retired.id.clone(), retired.usage_records.clone())),
            )
            .collect()
    }

    pub fn retired(&self) -> &[Retired] {
        &self.retired
    }

    /// Total trace batches received across the replica-dedicated collectors.
    pub fn trace_exports(&self) -> u64 {
        self.collectors()
            .map(|(_, collector)| {
                collector
                    .counts()
                    .get("traces")
                    .copied()
                    .unwrap_or_default()
            })
            .sum()
    }

    fn trace_identity_delta(&self) -> Result<TraceIdentityDelta, String> {
        let mut identities = BTreeSet::new();
        let mut caller_spans_seen = 0_u64;
        for (replica, collector) in self.collectors() {
            let observed = collector.trace_identity_delta(replica)?;
            for (trace_id, occurrences) in observed
                .occurrences
                .into_iter()
                .filter(|(trace_id, _)| trace_id.starts_with(ROLLOUT_TRACE_DOMAIN_PREFIX))
            {
                caller_spans_seen = caller_spans_seen
                    .checked_add(occurrences)
                    .ok_or_else(|| "rollout caller-span count overflowed u64".to_owned())?;
                identities.insert((replica.to_owned(), trace_id));
            }
        }
        Ok(TraceIdentityDelta {
            identities,
            caller_spans_seen,
        })
    }

    /// Settle the complete caller-domain trace set. Once every expected trace
    /// has arrived, the set must remain unchanged for several configured batch
    /// intervals; the returned snapshot is the one serialized and judged.
    /// Unrelated readiness and startup spans neither satisfy nor delay it.
    pub async fn settle_trace_identities(
        &self,
        expected: &BTreeSet<(String, String)>,
        within: Duration,
    ) -> Result<TraceWitnessSnapshot, String> {
        let mut snapshot = settle_identity_set(expected, within, TRACE_QUIESCENCE, || {
            self.trace_identity_delta()
        })
        .await?;
        snapshot.exports = self.trace_exports();
        Ok(snapshot)
    }

    fn collectors(&self) -> impl Iterator<Item = (&str, &Collector)> {
        self.replicas
            .iter()
            .map(|replica| (replica.id.as_str(), &replica.collector))
            .chain(
                self.retired
                    .iter()
                    .map(|replica| (replica.id.as_str(), &replica.collector)),
            )
    }
}

async fn settle_identity_set<F>(
    expected: &BTreeSet<(String, String)>,
    within: Duration,
    quiescence: Duration,
    mut observe: F,
) -> Result<TraceWitnessSnapshot, String>
where
    F: FnMut() -> Result<TraceIdentityDelta, String>,
{
    let deadline = Instant::now() + within;
    let mut identities = BTreeSet::new();
    let mut unchanged_since = Instant::now();
    loop {
        let now = Instant::now();
        let delta = observe()?;
        if delta.caller_spans_seen > 0 {
            unchanged_since = now;
        }
        identities.extend(delta.identities);
        if (expected.is_subset(&identities) && now.duration_since(unchanged_since) >= quiescence)
            || now >= deadline
        {
            return Ok(TraceWitnessSnapshot {
                exports: 0,
                identities,
            });
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// What one replica's termination cost, and what it flushed on the way out.
pub struct Drained {
    pub id: String,
    pub revision: Revision,
    /// How long the process took to exit; `None` if it outlived `budget` plus
    /// the harness' slack.
    pub took: Option<Duration>,
    pub clean: bool,
    /// The bound the process itself advertises: drain grace, shutdown deadline,
    /// and sink flush. Exclusive of the harness' slack, so an overrun is
    /// measurable.
    pub budget: Duration,
    pub usage_records: Vec<Value>,
    /// Everything the process logged, kept for a failure message.
    pub output: String,
}

/// A caller-visible request the harness pins to one replica, bypassing the
/// balancer: the in-flight work a drain has to finish or cut.
pub mod pinned {
    /// A buffered completion whose upstream withholds the answer for ~2s, so the
    /// request is unambiguously in flight when the signal lands.
    pub const BUFFERED: &str = super::alias::CHAT_LATE_HEADERS;
    /// A stream whose upstream never ends it, so only the shutdown deadline can.
    pub const STREAM: &str = super::alias::CHAT_STALL_AFTER_BYTES;
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;

    const SHUTDOWN: ShutdownBounds = ShutdownBounds {
        drain_grace_ms: 100,
        deadline_ms: 200,
        flush_timeout_ms: 300,
    };

    #[test]
    fn stateful_tuning_contains_only_bootstrap_owned_sections() {
        let tuning = Revision::stateful_tuning(SHUTDOWN);

        assert!(tuning.contains("[transport]"));
        assert!(tuning.contains("[shutdown]"));
        assert!(!tuning.contains("[failover]"));
        assert!(!tuning.contains("[[model]]"));
    }

    #[test]
    fn stateless_tuning_retains_the_previous_config_contract() {
        let previous = Revision::previous().stateless_tuning(SHUTDOWN);
        let compatibility = Revision::compatibility().stateless_tuning(SHUTDOWN);
        let next = Revision::next().stateless_tuning(SHUTDOWN);
        let expected = r"
[failover]
max_attempts = 1
overall_timeout_ms = 60000

[transport]
connect_timeout_ms = 10000
response_header_timeout_ms = 30000
buffered_body_timeout_ms = 30000
stream_idle_timeout_ms = 30000

[shutdown]
drain_grace_ms = 100
deadline_ms = 200
flush_timeout_ms = 300
";

        assert_eq!(previous, expected);
        assert_eq!(compatibility, expected);
        assert_eq!(
            next,
            format!(
                "{expected}\n[[model]]\nname = \"chat-next-only\"\ntargets = [ {{ provider = \
                 \"fake-openai\", model = \"fixture-chat\", price = {{ \
                 input_microdollars_per_million = 2500000, \
                 output_microdollars_per_million = 10000000 }} }} ]\n"
            )
        );
    }

    #[tokio::test]
    async fn trace_settlement_retains_an_extra_identity_that_arrives_late() {
        let expected_identity = (
            "previous-0".to_owned(),
            format!("{ROLLOUT_TRACE_DOMAIN_PREFIX}1"),
        );
        let extra_identity = (
            "previous-0".to_owned(),
            format!("{ROLLOUT_TRACE_DOMAIN_PREFIX}2"),
        );
        let expected = [expected_identity.clone()].into_iter().collect();
        let observed = Arc::new(Mutex::new(TraceIdentityDelta {
            identities: [expected_identity].into_iter().collect(),
            caller_spans_seen: 1,
        }));
        let delayed = observed.clone();
        let expected_extra = extra_identity.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(30)).await;
            let mut delta = delayed.lock().expect("trace test lock");
            delta.identities.insert(expected_extra);
            delta.caller_spans_seen += 1;
        });

        let snapshot = settle_identity_set(
            &expected,
            Duration::from_millis(250),
            Duration::from_millis(80),
            || {
                Ok(std::mem::take(
                    &mut *observed.lock().expect("trace test lock"),
                ))
            },
        )
        .await
        .expect("the synthetic collector settles");

        assert_eq!(
            snapshot.identities,
            [
                (
                    "previous-0".to_owned(),
                    format!("{ROLLOUT_TRACE_DOMAIN_PREFIX}1")
                ),
                extra_identity,
            ]
            .into_iter()
            .collect()
        );
    }

    #[tokio::test]
    async fn trace_settlement_resets_for_duplicate_caller_activity_before_a_late_extra() {
        let expected_identity = (
            "previous-0".to_owned(),
            format!("{ROLLOUT_TRACE_DOMAIN_PREFIX}1"),
        );
        let extra_identity = (
            "previous-0".to_owned(),
            format!("{ROLLOUT_TRACE_DOMAIN_PREFIX}2"),
        );
        let expected = [expected_identity.clone()].into_iter().collect();
        let observed = Arc::new(Mutex::new(TraceIdentityDelta {
            identities: [expected_identity].into_iter().collect(),
            caller_spans_seen: 1,
        }));
        let delayed = observed.clone();
        let expected_extra = extra_identity.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(40)).await;
            delayed.lock().expect("trace test lock").caller_spans_seen += 1;
            tokio::time::sleep(Duration::from_millis(40)).await;
            let mut observed = delayed.lock().expect("trace test lock");
            observed.identities.insert(expected_extra);
            observed.caller_spans_seen += 1;
        });

        let snapshot = settle_identity_set(
            &expected,
            Duration::from_millis(250),
            Duration::from_millis(70),
            || {
                Ok(std::mem::take(
                    &mut *observed.lock().expect("trace test lock"),
                ))
            },
        )
        .await
        .expect("caller activity keeps the synthetic collector open");

        assert!(snapshot.identities.contains(&extra_identity));
    }
}
