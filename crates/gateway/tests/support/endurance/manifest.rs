//! The committed endurance manifest: every input a soak reads.
//!
//! Data rather than code, for the same reason the capacity manifest is: a
//! result artifact names the exact inputs that produced it, and the manifest's
//! own hash is recorded alongside the binary's and the fixtures' (ADR 0033).

use std::path::PathBuf;

use figment::Figment;
use figment::providers::{Format, Toml};
use serde::{Deserialize, Serialize};

use crate::support::capacity::manifest::workspace_root;

/// The manifest, relative to the workspace root.
pub const MANIFEST_RELATIVE: &str = "qualification/endurance/manifest.toml";

/// The result-artifact schema version. Bumped when a field changes meaning, so
/// a stored artifact is never reinterpreted under a newer contract.
///
/// 4: reconciliation pairs each planned request with its usage record by a
/// full 128-bit W3C trace identity, validates the status for that request, and
/// observes the complete settlement window before a terminal drain.
///
/// 3: reconciliation records and gates distinct usage identities beyond the
/// requests known to owe a row, so a watchdog expiry cannot hide surplus
/// accounting behind a saturating missing-record subtraction.
///
/// 2: `profile.duration_ms` is the duration the run was offered rather than the
/// one the manifest commits, which moved to `profile.manifest_duration_ms`. A
/// version-1 artifact of a dispatched run states the manifest's duration
/// beside that run's numbers, so the two may not be read the same way.
pub const RESULT_SCHEMA_VERSION: u32 = 4;

/// The manifest schema this harness understands.
pub const MANIFEST_SCHEMA_VERSION: u32 = 2;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Manifest {
    pub schema_version: u32,
    #[serde(rename = "profile")]
    pub profiles: Vec<Profile>,
}

/// One endurance workload, at two tiers.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Profile {
    pub id: String,
    pub description: String,
    /// Seeds the ending rotation, so the mix is offered in the same order on
    /// every host.
    pub seed: u64,
    pub mix: Mix,
    pub smoke: Scale,
    pub soak: Scale,
}

impl Profile {
    pub fn scale(&self, tier: Tier) -> &Scale {
        match tier {
            Tier::Smoke => &self.smoke,
            Tier::Soak => &self.soak,
        }
    }
}

/// How many requests of each ending are in one cycle of the rotation.
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct Mix {
    pub complete: usize,
    pub cancelled: usize,
    pub dropped: usize,
    pub faulted: usize,
}

impl Mix {
    pub fn cycle_len(&self) -> usize {
        self.complete + self.cancelled + self.dropped + self.faulted
    }

    pub fn weight(&self, ending: Ending) -> usize {
        match ending {
            Ending::Complete => self.complete,
            Ending::Cancelled => self.cancelled,
            Ending::Dropped => self.dropped,
            Ending::Faulted => self.faulted,
        }
    }
}

/// How one tier is offered, and what it is gated on.
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct Scale {
    pub duration_ms: u64,
    pub concurrency: usize,
    /// Per worker, between requests: endurance holds a modest rate for a long
    /// time rather than saturating the host, which is capacity's question.
    pub think_time_ms: u64,
    pub sample_interval_ms: u64,
    pub segment_ms: u64,
    pub thresholds: Thresholds,
}

/// The hard failures. Every one is a property of the *gateway* rather than of
/// the machine: how fast the runner was changes throughput, not whether a
/// descriptor came back or an accounting row went missing.
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct Thresholds {
    /// Of the requests whose plan says they succeed — a deliberately faulted
    /// request is not a failure of the run.
    pub min_accepted_fraction: f64,
    /// Errors beyond the ones the plan asked for.
    pub max_unplanned_errors: u64,
    pub max_missing_usage_records: u64,
    /// Distinct usage identities beyond the requests known to owe a record.
    pub max_unexpected_usage_records: u64,
    pub max_duplicate_usage_records: u64,
    /// Usage records whose status is not one the plan can produce.
    pub max_unexpected_usage_statuses: u64,
    pub max_leaked_upstream_streams: i64,
    /// Sockets still held once the load has stopped and the process has
    /// settled, over the baseline it started from.
    pub max_settled_socket_excess: u64,
    pub max_rss_growth_kib: u64,
    /// Drift gates, in units per hour, fitted through the per-segment medians.
    /// Absent on a tier too short for a per-hour slope to mean anything.
    #[serde(default)]
    pub max_rss_drift_kib_per_hour: Option<f64>,
    #[serde(default)]
    pub max_socket_drift_per_hour: Option<f64>,
    #[serde(default)]
    pub max_fd_drift_per_hour: Option<f64>,
    /// A trend needs segments to be a trend: a run that produced fewer than
    /// this many says nothing about drift, and that is a failure rather than a
    /// pass.
    pub min_segments: u64,
}

/// How a planned request ends. The four shapes a long-lived relay has to
/// survive: the answer that arrives, the caller who leaves, the upstream that
/// dies mid-stream, and the upstream that refuses before a byte is relayed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Ending {
    Complete,
    Cancelled,
    Dropped,
    Faulted,
}

impl Ending {
    pub const ALL: [Self; 4] = [
        Self::Complete,
        Self::Cancelled,
        Self::Dropped,
        Self::Faulted,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Cancelled => "cancelled",
            Self::Dropped => "dropped",
            Self::Faulted => "faulted",
        }
    }

    /// The usage status this ending settles. A record with any other status for
    /// this ending is an accounting bug, which is why the plan states it up
    /// front rather than reading it back off the run.
    pub fn usage_status(self) -> &'static str {
        match self {
            Self::Complete => "ok",
            Self::Cancelled => "client_cancelled",
            // Both reach the caller as an upstream failure: a stream that dies
            // after relay has begun cannot fail over, and a target that refuses
            // before any byte has one attempt in this configuration.
            Self::Dropped | Self::Faulted => "upstream_error",
        }
    }

    /// Whether the plan expects the caller to see a successful response. A
    /// faulted request is planned to fail, so it is not counted against the
    /// acceptance threshold.
    pub fn planned_success(self) -> bool {
        !matches!(self, Self::Faulted)
    }

    /// Whether `status` is one this ending can settle. A cancelled stream may
    /// be charged either way round — `client_cancelled` when the hang-up is
    /// what ended it, `partial` when the relay had already finished — and both
    /// are accounted spend, so both are the plan rather than a defect.
    pub fn settles(self, status: &str) -> bool {
        match self {
            Self::Complete => status == "ok",
            Self::Cancelled => matches!(status, "client_cancelled" | "partial"),
            Self::Dropped => matches!(status, "upstream_error" | "partial"),
            Self::Faulted => status == "upstream_error",
        }
    }
}

/// Which tier to run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    Smoke,
    Soak,
}

impl Tier {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Smoke => "smoke",
            Self::Soak => "soak",
        }
    }
}

pub fn manifest_path() -> PathBuf {
    workspace_root().join(MANIFEST_RELATIVE)
}

/// Load the manifest, refusing a schema this harness does not understand: a
/// silently misread profile would qualify something other than what is
/// committed.
pub fn load() -> (Manifest, String) {
    let path = manifest_path();
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("{} is unreadable: {e}", path.display()));
    let manifest: Manifest = Figment::from(Toml::file(&path))
        .extract()
        .unwrap_or_else(|e| panic!("{} is not a valid endurance manifest: {e}", path.display()));
    assert_eq!(
        manifest.schema_version, MANIFEST_SCHEMA_VERSION,
        "unsupported endurance manifest schema"
    );
    assert!(
        !manifest.profiles.is_empty(),
        "the endurance manifest declares no profiles"
    );
    (manifest, text)
}
