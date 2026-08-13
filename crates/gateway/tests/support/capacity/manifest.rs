//! The committed capacity manifest: every input a run reads.
//!
//! Profiles are data rather than code so a result artifact can name the exact
//! inputs that produced it — the manifest's own hash is recorded alongside the
//! binary's and the fixtures' (ADR 0033).

use std::path::{Path, PathBuf};

use figment::Figment;
use figment::providers::{Format, Toml};
use serde::{Deserialize, Serialize};

/// The manifest, relative to the workspace root.
pub const MANIFEST_RELATIVE: &str = "qualification/capacity/manifest.toml";

/// The result-artifact schema version. Bumped when a field changes meaning, so
/// a stored artifact is never reinterpreted under a newer contract.
pub const RESULT_SCHEMA_VERSION: u32 = 1;

/// The manifest schema this harness understands.
pub const MANIFEST_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Manifest {
    pub schema_version: u32,
    #[serde(rename = "profile")]
    pub profiles: Vec<Profile>,
}

/// One workload, at two scales, with the thresholds that make it a gate.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Profile {
    pub id: String,
    pub workload: Workload,
    pub description: String,
    /// Cancellation only: every `cancel_every`-th caller hangs up.
    #[serde(default)]
    pub cancel_every: Option<usize>,
    /// Cancellation only: how many chunks carrying relayed output a caller waits
    /// for before hanging up, so the partial charge is a real one.
    #[serde(default)]
    pub cancel_after_output_chunks: Option<usize>,
    /// Shedding only: the concurrency ceiling the profile boots the replica
    /// with. Written here rather than left to a shipped default so the ceiling
    /// a run was measured against is in the manifest and in the record.
    #[serde(default)]
    pub max_in_flight: Option<u64>,
    /// Backend limits only: the transport bound the profile boots the replica
    /// with, and therefore the wall clock every request must end inside.
    #[serde(default)]
    pub upstream_timeout_ms: Option<u64>,
    pub reduced: Scale,
    pub heavy: Scale,
    pub thresholds: Thresholds,
}

impl Profile {
    pub fn scale(&self, tier: Tier) -> &Scale {
        match tier {
            Tier::Reduced => &self.reduced,
            Tier::Heavy => &self.heavy,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct Scale {
    pub concurrency: usize,
    pub requests: usize,
}

/// What a profile sends. The driver owns the shape rotation for each one, so a
/// manifest cannot describe a workload the harness does not implement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Workload {
    Buffered,
    Streaming,
    Mixed,
    ResponseSize,
    Cancellation,
    Tenants,
    Shedding,
    BackendLimits,
}

impl Workload {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Buffered => "buffered",
            Self::Streaming => "streaming",
            Self::Mixed => "mixed",
            Self::ResponseSize => "response_size",
            Self::Cancellation => "cancellation",
            Self::Tenants => "tenants",
            Self::Shedding => "shedding",
            Self::BackendLimits => "backend_limits",
        }
    }

    /// Every workload the driver implements. The manifest coverage test reads
    /// this, so adding a variant without a committed profile fails rather than
    /// leaving the new shape unqualified.
    pub const ALL: [Self; 8] = [
        Self::Buffered,
        Self::Streaming,
        Self::Mixed,
        Self::ResponseSize,
        Self::Cancellation,
        Self::Tenants,
        Self::Shedding,
        Self::BackendLimits,
    ];
}

/// The hard failures. Every one of these is a property of the *gateway* rather
/// than of the machine it ran on: a shared runner changes throughput and
/// latency, but it does not change whether a stream leaked a socket or whether
/// every accepted request settled a usage record.
///
/// The absolute rejection and error counts are optional because two profiles
/// exist to provoke exactly those outcomes: shedding is the point of the
/// `shedding` profile and a bounded upstream failure is the point of
/// `backend-limits`. A profile that omits them has to bound the same outcome as
/// a fraction instead — `the_committed_manifest_covers_every_workload_with_thresholds`
/// refuses one that bounds it neither way, so "optional" never means "ungated".
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct Thresholds {
    pub min_accepted_fraction: f64,
    #[serde(default)]
    pub max_rejections: Option<u64>,
    #[serde(default)]
    pub max_errors: Option<u64>,
    pub max_missing_usage_records: u64,
    pub max_leaked_upstream_streams: i64,
    pub max_rss_growth_kib: u64,
    /// Shedding: the ceiling has to bite, and it has to stop biting once the
    /// offered load is inside it. A run that shed nothing measured a replica
    /// that was never full; one that shed everything measured a replica that
    /// stopped serving.
    #[serde(default)]
    pub min_rejected_fraction: Option<f64>,
    #[serde(default)]
    pub max_rejected_fraction: Option<f64>,
    /// Backend limits: a bounded share of requests may fail, and every failure
    /// has to carry a typed body rather than whatever the upstream leaked.
    #[serde(default)]
    pub max_error_fraction: Option<f64>,
    #[serde(default)]
    pub max_untyped_errors: Option<u64>,
    /// Backend limits: requests that outlived the bound the replica itself
    /// declares. This is the gateway's own promise, not a property of the
    /// runner, so it is asserted with generous slack (see `DEADLINE_SLACK`).
    #[serde(default)]
    pub max_over_deadline: Option<u64>,
    /// Tenants: upstream calls that carried a credential belonging to a
    /// namespace other than the caller's.
    #[serde(default)]
    pub max_foreign_credential_uses: Option<u64>,
    /// Tenants: usage rows attributed to a namespace that did not send them.
    #[serde(default)]
    pub max_misattributed_usage_records: Option<u64>,
    /// Shedding and backend limits: a request offered after the load stops must
    /// be served, or the permits the run consumed never came back.
    #[serde(default)]
    pub max_unserved_after_load: Option<u64>,
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

/// The workspace root, resolved from this crate rather than from whatever
/// working directory the test runner happens to have.
pub fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

pub fn manifest_path() -> PathBuf {
    workspace_root().join(MANIFEST_RELATIVE)
}

/// Load the manifest, refusing a schema this harness does not understand: a
/// silently misread profile would qualify something other than what is
/// committed.
pub fn load() -> (Manifest, String) {
    let path = manifest_path();
    let text = read(&path);
    let manifest: Manifest = Figment::from(Toml::file(&path))
        .extract()
        .unwrap_or_else(|e| panic!("{} is not a valid capacity manifest: {e}", path.display()));
    assert_eq!(
        manifest.schema_version, MANIFEST_SCHEMA_VERSION,
        "unsupported capacity manifest schema"
    );
    assert!(
        !manifest.profiles.is_empty(),
        "the capacity manifest declares no profiles"
    );
    (manifest, text)
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("{} is unreadable: {e}", path.display()))
}

/// Lowercase hex SHA-256, the identity every recorded input is named by.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let digest = ring::digest::digest(&ring::digest::SHA256, bytes);
    digest.as_ref().iter().fold(String::new(), |mut hex, byte| {
        use std::fmt::Write;
        let _ = write!(hex, "{byte:02x}");
        hex
    })
}

pub fn sha256_file(path: &Path) -> String {
    let bytes =
        std::fs::read(path).unwrap_or_else(|e| panic!("{} is unreadable: {e}", path.display()));
    sha256_hex(&bytes)
}
