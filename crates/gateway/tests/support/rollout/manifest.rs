//! The committed rollout manifest: every input a rollout run reads.
//!
//! Data rather than code, for the reason the capacity manifest is
//! (ADR 0033): a result artifact names the exact inputs that produced it, and
//! the manifest's own hash is recorded alongside the binary's.

use std::path::PathBuf;
use std::time::Duration;

use figment::Figment;
use figment::providers::{Format, Toml};
use serde::{Deserialize, Serialize};

use crate::support::capacity::manifest::workspace_root;

/// The manifest, relative to the workspace root.
pub const MANIFEST_RELATIVE: &str = "qualification/rollout/manifest.toml";

/// The result-artifact schema version. Bumped when a field changes meaning, so
/// a stored artifact is never reinterpreted under a newer contract.
pub const RESULT_SCHEMA_VERSION: u32 = 1;

/// The manifest schema this harness understands.
pub const MANIFEST_SCHEMA_VERSION: u32 = 1;

/// The fewest replicas a rollout scenario can be written with: with one, a
/// drain has nowhere to route and the harness would qualify a restart.
pub const MIN_REPLICAS: usize = 2;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Manifest {
    pub schema_version: u32,
    #[serde(rename = "scenario")]
    pub scenarios: Vec<Scenario>,
}

/// One rollout, at two scales, with the thresholds that make it a gate.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Scenario {
    pub id: String,
    pub description: String,
    pub replicas: usize,
    pub reduced: Scale,
    pub heavy: Scale,
    pub shutdown: ShutdownBounds,
    pub thresholds: Thresholds,
}

impl Scenario {
    pub fn scale(&self, tier: Tier) -> &Scale {
        match tier {
            Tier::Reduced => &self.reduced,
            Tier::Heavy => &self.heavy,
        }
    }
}

/// How much traffic each phase of the rollout carries.
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct Scale {
    pub workers: usize,
    pub requests_per_phase: usize,
    pub stream_every: usize,
}

impl Scale {
    /// Whether the request at `index` is streamed. Rotation is by index, so the
    /// mix is identical on every run of the same scale.
    pub fn streams(&self, index: usize) -> bool {
        self.stream_every > 0 && index.is_multiple_of(self.stream_every)
    }
}

/// The `[shutdown]` section every replica in the scenario is booted with. The
/// harness holds the process to their sum rather than to a wall-clock guess.
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct ShutdownBounds {
    pub drain_grace_ms: u64,
    pub deadline_ms: u64,
    pub flush_timeout_ms: u64,
}

impl ShutdownBounds {
    /// The `[shutdown]` TOML a replica is booted with.
    pub fn toml(&self) -> String {
        format!(
            "\n[shutdown]\ndrain_grace_ms = {}\ndeadline_ms = {}\nflush_timeout_ms = {}\n",
            self.drain_grace_ms, self.deadline_ms, self.flush_timeout_ms
        )
    }

    /// What the process promises termination costs at most: the window it stops
    /// admitting in, the deadline it cuts surviving work at, and the budget it
    /// flushes accounting within. An orchestrator's grace period is set from
    /// this sum, so exceeding it is a `SIGKILL` in production.
    pub fn budget(&self) -> Duration {
        Duration::from_millis(self.drain_grace_ms + self.deadline_ms + self.flush_timeout_ms)
    }
}

/// The hard failures. Every one is a property of the *fleet* rather than of the
/// machine it ran on: a slow runner changes throughput, but it does not change
/// whether the load balancer kept routing to a replica it had seen drain.
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct Thresholds {
    pub max_requests_to_drained_replica: u64,
    pub max_request_loss: u64,
    pub max_unavailable_responses: u64,
    pub max_usage_record_loss: u64,
    pub max_readiness_removal_ms: u64,
    pub max_replacement_admission_ms: u64,
    pub max_drain_exit_slack_ms: u64,
    pub min_mixed_version_requests: u64,
}

/// Which scale to run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    Reduced,
    Heavy,
}

impl Tier {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Reduced => "reduced",
            Self::Heavy => "heavy",
        }
    }
}

pub fn manifest_path() -> PathBuf {
    workspace_root().join(MANIFEST_RELATIVE)
}

/// Load the manifest, refusing a schema this harness does not understand: a
/// silently misread scenario would still produce an artifact, and the artifact
/// would look like evidence.
pub fn load() -> (Manifest, String) {
    let path = manifest_path();
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("{} is unreadable: {e}", path.display()));
    let manifest: Manifest = Figment::from(Toml::file(&path))
        .extract()
        .unwrap_or_else(|e| panic!("{} is not a valid rollout manifest: {e}", path.display()));
    assert_eq!(
        manifest.schema_version, MANIFEST_SCHEMA_VERSION,
        "unsupported rollout manifest schema"
    );
    assert!(
        !manifest.scenarios.is_empty(),
        "the rollout manifest declares no scenarios"
    );
    (manifest, text)
}
