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

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use serde_json::Value;

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

/// Stateless failover bounds. Stateful mode projects failover policy from the
/// control plane, so these must never appear in its bootstrap TOML.
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

    /// Bootstrap-only tuning for a stateful replica. Failover and serving
    /// policy are deliberately absent because the durable revision owns them.
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
        let process = match &self.stateful {
            Some(deployment) => {
                Process::Stateful(deployment.start_replica(binary, self.shutdown).await)
            }
            None => Process::Stateless(
                Axond::start_with_binary(
                    &self.upstream.base_url,
                    &revision.stateless_tuning(self.shutdown),
                    binary,
                )
                .await,
            ),
        };
        self.replicas.push(Replica {
            id,
            revision,
            process,
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
}
