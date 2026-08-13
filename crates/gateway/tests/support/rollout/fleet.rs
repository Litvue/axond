//! The fleet: several real `axond` processes, at two revisions, sharing one
//! deterministic fake upstream.
//!
//! Two things here are worth being explicit about.
//!
//! **What a "revision" is.** A rollout is only interesting if the two builds
//! differ in a way a caller can observe, and a test cannot build a second binary
//! from a second commit. So a revision is the pair (binary, config) the process
//! was started from, and the *next* revision differs by a capability the
//! previous one does not have: an alias only it serves. That is exactly the
//! shape of the mixed-version rule Axond documents — during a rollout a caller
//! may only rely on what both revisions have — and it makes "which revision
//! answered?" a question the harness can put to the process rather than to its
//! own bookkeeping. Both revisions' binary and config hashes are recorded, so an
//! artifact never claims two builds when it ran one.
//!
//! **Why usage records are harvested at exit.** A drained replica's process is
//! gone by the time the run ends, and its records are the ones most likely to be
//! lost. They are taken from the process' stdout the moment it exits and kept
//! with the fleet, so the loss ledger covers replicas that no longer exist.

use std::time::{Duration, Instant};

use serde_json::Value;

use crate::support::gateway::{Axond, alias};
use crate::support::upstream::{FakeUpstream, target};

use super::manifest::ShutdownBounds;

/// The revision labels an artifact reports.
pub const PREVIOUS: &str = "previous";
pub const NEXT: &str = "next";

/// An alias only the next revision serves. A caller that uses it during a
/// mixed-version window gets an error from whichever replica has not been
/// replaced yet, which is the documented rule made observable.
pub const NEXT_ONLY_ALIAS: &str = "chat-next-only";

/// The transport bounds every replica is booted with. Written out rather than
/// defaulted so the recorded config hash pins them: a later change to a shipped
/// default must not silently move a qualification result.
const TUNING: &str = r"
[failover]
max_attempts = 1
overall_timeout_ms = 60000

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

    /// The config this revision adds on top of the shared bounds.
    pub fn tuning(self, shutdown: ShutdownBounds) -> String {
        let mut tuning = format!("{TUNING}{}", shutdown.toml());
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
}

/// One running replica.
pub struct Replica {
    pub id: String,
    pub revision: Revision,
    pub process: Axond,
}

impl Replica {
    pub fn base_url(&self) -> &str {
        &self.process.base_url
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
    replicas: Vec<Replica>,
    retired: Vec<Retired>,
    started: usize,
}

impl Fleet {
    pub async fn start(shutdown: ShutdownBounds) -> Self {
        Self {
            upstream: FakeUpstream::start().await,
            shutdown,
            replicas: Vec::new(),
            retired: Vec::new(),
            started: 0,
        }
    }

    /// Boot one more replica at `revision`. The id counts booted processes
    /// rather than live ones, so a replacement is never confused with the
    /// replica it replaced.
    pub async fn admit(&mut self, revision: Revision) -> &Replica {
        let id = format!("{}-{}", revision.label, self.started);
        self.started += 1;
        let process =
            Axond::start_with(&self.upstream.base_url, &revision.tuning(self.shutdown)).await;
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
        let budget = self.shutdown.budget() + slack;
        let mut replica = self.replicas.remove(index);
        let status = replica
            .process
            .await_exit(budget.saturating_sub(signalled.elapsed()))
            .await;
        let took = status.map(|_| signalled.elapsed());
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

    /// Wait until the fleet has flushed at least `count` usage records, or the
    /// deadline passes. Settlement is detached from the request, so a record can
    /// land just after the caller's last byte.
    pub async fn await_usage_records(&self, count: usize, within: Duration) -> Vec<Value> {
        let deadline = std::time::Instant::now() + within;
        loop {
            let records = self.usage_records();
            if records.len() >= count || std::time::Instant::now() >= deadline {
                return records;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    pub fn retired(&self) -> &[Retired] {
        &self.retired
    }
}

/// What one replica's termination cost, and what it flushed on the way out.
pub struct Drained {
    pub id: String,
    pub revision: Revision,
    /// How long the process took to exit; `None` if it outlived `budget`.
    pub took: Option<Duration>,
    pub clean: bool,
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
