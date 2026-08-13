//! The committed recovery manifest: every scenario the recovery harness runs.
//!
//! Scenarios are data for the same reason capacity profiles are (ADR 0033): the
//! run has to be reproducible from the repository. What is different here is
//! that a scenario becomes executable in halves — the control-plane half runs
//! against a real Postgres today, the serving half waits on a projection a
//! replica can serve — so a scenario is a set of [`Stage`]s, each with its own
//! status, its own evidence, and its own blockers. This module is what keeps
//! that map honest rather than aspirational.
//!
//! The types below are the contract. A scenario the driver has no
//! [`Capability`] for cannot be written, an evidence field outside [`Evidence`]
//! cannot be retained, and a scenario is executable exactly when every stage of
//! it is. Which *stages* the driver runs is asserted where the driver lives, in
//! the gateway's own `qualification::recovery` tests: that check needs the
//! registry and the manifest in one place, and the registry is not reachable
//! from an integration test.

use std::collections::BTreeSet;
use std::path::PathBuf;

use figment::Figment;
use figment::providers::{Format, Toml};
use serde::{Deserialize, Serialize};

/// The manifest, relative to the workspace root.
pub const MANIFEST_RELATIVE: &str = "qualification/recovery/manifest.toml";

/// The operator- and contributor-facing contract, relative to the workspace
/// root. It states the same scenarios in prose, so the two are checked against
/// each other.
pub const CONTRACT_RELATIVE: &str = "docs/operations/recovery-qualification.md";

/// The manifest schema this harness understands.
pub const MANIFEST_SCHEMA_VERSION: u32 = 3;

/// The slices axond #219 waits on. A stage may only be blocked on one of
/// these, and between them the stages must account for all of them: an issue
/// that no stage names is a dependency nobody is waiting for, and a dependency
/// nobody is waiting for is a sign the stage that needed it was dropped.
///
/// #159 was on this list for the hardened lane a recovery scenario needs — a
/// database running with WAL archiving, and the evidence published as an
/// artifact. That lane exists: `ops/restore-drill.sh` runs it, and
/// `ops/check-recovery-evidence.py` fails the lane when an executable stage
/// leaves no artifact. The rest of #159 — disclosure, fuzzing, SDK
/// compatibility — blocks no stage here, and keeping it listed would have
/// meant inventing a stage to wait on it.
pub const BLOCKING_ISSUES: [u32; 9] = [144, 145, 146, 147, 148, 149, 150, 155, 158];

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub schema_version: u32,
    #[serde(rename = "scenario")]
    pub scenarios: Vec<Scenario>,
}

/// One recovery scenario: what happens, what makes it fail, and the stages it
/// is assembled from.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    pub id: String,
    pub capability: Capability,
    pub description: String,
    pub gate: Gate,
    #[serde(rename = "stage")]
    pub stages: Vec<Stage>,
}

impl Scenario {
    /// Executable exactly when every stage is. Derived rather than declared:
    /// a scenario-level status could be optimistic about a stage that is not.
    pub fn status(&self) -> Status {
        if self
            .stages
            .iter()
            .all(|stage| stage.status == Status::Executable)
        {
            Status::Executable
        } else {
            Status::Blocked
        }
    }

    /// Every evidence class the scenario's stages retain between them.
    pub fn evidence(&self) -> BTreeSet<Evidence> {
        self.stages
            .iter()
            .flat_map(|stage| stage.evidence.iter().copied())
            .collect()
    }
}

/// One half of a scenario: a piece of it that is either driven today or waiting
/// on a named slice.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Stage {
    pub id: String,
    pub status: Status,
    /// Which lane runs it. Present exactly when the stage is executable: a
    /// blocked stage has no runner, and an executable one that named none
    /// could not be checked for the artifact it is supposed to leave.
    #[serde(default)]
    pub runner: Option<Runner>,
    /// What the stage observes, in prose, for the reader who is deciding
    /// whether the evidence answers their question.
    pub covers: String,
    pub evidence: Vec<Evidence>,
    #[serde(default)]
    pub blocked_on: Vec<Dependency>,
}

/// Whether the driver runs this stage today.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Executable,
    Blocked,
}

/// Which lane executes a stage. Two, because severing a link and promoting a
/// recovered cluster need different machinery: the in-process driver cannot
/// take a base backup, and a shell drill cannot reach into a reconciler.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Runner {
    /// `qualification::recovery`, in the stateful test lane.
    StatefulTests,
    /// `ops/restore-drill.sh`, in the restore and PITR drill lane.
    RestoreDrill,
}

impl Runner {
    pub const ALL: [Self; 2] = [Self::StatefulTests, Self::RestoreDrill];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::StatefulTests => "stateful-tests",
            Self::RestoreDrill => "restore-drill",
        }
    }
}

/// A scenario the driver implements, or will. One variant per scenario axond
/// #219 names, so a scenario cannot be quietly dropped from the manifest: the
/// contract test requires every variant to appear.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    ControlPlaneOutage,
    ColdBootValidCache,
    ColdBootNoCache,
    ColdBootInvalidCache,
    RecoveryConvergence,
    SecretRotation,
    BackupRestore,
    PointInTimeRecovery,
}

impl Capability {
    pub const ALL: [Self; 8] = [
        Self::ControlPlaneOutage,
        Self::ColdBootValidCache,
        Self::ColdBootNoCache,
        Self::ColdBootInvalidCache,
        Self::RecoveryConvergence,
        Self::SecretRotation,
        Self::BackupRestore,
        Self::PointInTimeRecovery,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ControlPlaneOutage => "control_plane_outage",
            Self::ColdBootValidCache => "cold_boot_valid_cache",
            Self::ColdBootNoCache => "cold_boot_no_cache",
            Self::ColdBootInvalidCache => "cold_boot_invalid_cache",
            Self::RecoveryConvergence => "recovery_convergence",
            Self::SecretRotation => "secret_rotation",
            Self::BackupRestore => "backup_restore",
            Self::PointInTimeRecovery => "point_in_time_recovery",
        }
    }
}

/// What a run retains. These are the evidence classes axond #219 requires, and
/// the union over the manifest has to cover all of them — a scenario set that
/// retains no data-loss boundary is not evidence about recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Evidence {
    /// When the control plane went away, when it returned, and what the replica
    /// did at each transition.
    OutageTimeline,
    /// What happened to inference: offered, answered, refused, and with what.
    ServingBehavior,
    /// Desired, loaded, and active revision per replica.
    Revisions,
    /// How far behind desired state a replica was, sampled over the window.
    ConvergenceLag,
    /// What a replica booting into the window did: restored, refused, or served.
    ColdStart,
    /// How long the restore took, wall clock, recorded and never asserted.
    RestoreDuration,
    /// What durable state did not survive, named rather than counted.
    DataLossBoundary,
    /// Which dependencies failed open and which failed closed.
    FailOpenClosed,
    /// Administrative authentication and audit outcomes across the window.
    AuditAuth,
}

impl Evidence {
    pub const ALL: [Self; 9] = [
        Self::OutageTimeline,
        Self::ServingBehavior,
        Self::Revisions,
        Self::ConvergenceLag,
        Self::ColdStart,
        Self::RestoreDuration,
        Self::DataLossBoundary,
        Self::FailOpenClosed,
        Self::AuditAuth,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OutageTimeline => "outage_timeline",
            Self::ServingBehavior => "serving_behavior",
            Self::Revisions => "revisions",
            Self::ConvergenceLag => "convergence_lag",
            Self::ColdStart => "cold_start",
            Self::RestoreDuration => "restore_duration",
            Self::DataLossBoundary => "data_loss_boundary",
            Self::FailOpenClosed => "fail_open_closed",
            Self::AuditAuth => "audit_auth",
        }
    }
}

/// The hard failures. Every field is a property of the deployment rather than
/// of the machine it ran on, for the same reason the capacity thresholds are:
/// a shared runner moves durations, and a gate that flakes is a gate that gets
/// switched off.
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Gate {
    /// Of the requests offered during the scenario's window, the fraction that
    /// may fail. Recovery is not an excuse for a 5xx: an outage degrades
    /// change, not serving.
    pub max_serving_error_fraction: f64,
    /// How long after the control plane returns a replica may still be behind
    /// desired state.
    pub max_convergence_lag_seconds: u64,
    /// Revisions committed before the recovery target that the recovered
    /// database no longer holds.
    pub max_data_loss_revisions: u64,
    /// What readiness must say once the window closes.
    pub readiness: Readiness,
    /// What an administrative write does during the window.
    pub admin_writes: AdminWrites,
    /// Unauthenticated administrative calls that succeeded. Zero, always: an
    /// outage may refuse a change, and may never admit a caller.
    pub max_unauthenticated_admin_successes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Readiness {
    /// Ready, and answering inference.
    Serves,
    /// Not ready, and saying why. Never ready while serving nothing.
    Refuses,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AdminWrites {
    Accepted,
    /// Refused with a retryable unavailable error, writing nothing.
    Unavailable,
}

/// One slice a blocked stage waits on, and what the stage needs from it.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Dependency {
    pub issue: u32,
    pub needs: String,
}

/// The workspace root, resolved from this crate rather than from whatever
/// working directory the test runner happens to have.
pub fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

pub fn manifest_path() -> PathBuf {
    workspace_root().join(MANIFEST_RELATIVE)
}

pub fn contract_path() -> PathBuf {
    workspace_root().join(CONTRACT_RELATIVE)
}

/// Load the manifest, refusing a schema this harness does not understand: a
/// silently misread scenario would qualify something other than what is
/// committed.
pub fn load() -> Manifest {
    let path = manifest_path();
    let manifest: Manifest = Figment::from(Toml::file(&path))
        .extract()
        .unwrap_or_else(|e| panic!("{} is not a valid recovery manifest: {e}", path.display()));
    assert_eq!(
        manifest.schema_version, MANIFEST_SCHEMA_VERSION,
        "unsupported recovery manifest schema"
    );
    assert!(
        !manifest.scenarios.is_empty(),
        "the recovery manifest declares no scenarios"
    );
    manifest
}

/// The prose contract, read as text so the manifest can be checked against it.
pub fn contract_text() -> String {
    let path = contract_path();
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("{} is unreadable: {e}", path.display()))
}
