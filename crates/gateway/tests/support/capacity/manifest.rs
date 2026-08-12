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
}

impl Workload {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Buffered => "buffered",
            Self::Streaming => "streaming",
            Self::Mixed => "mixed",
            Self::ResponseSize => "response_size",
            Self::Cancellation => "cancellation",
        }
    }
}

/// The hard failures. Every one of these is a property of the *gateway* rather
/// than of the machine it ran on: a shared runner changes throughput and
/// latency, but it does not change whether a stream leaked a socket or whether
/// every accepted request settled a usage record.
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub struct Thresholds {
    pub min_accepted_fraction: f64,
    pub max_rejections: u64,
    pub max_errors: u64,
    pub max_missing_usage_records: u64,
    pub max_leaked_upstream_streams: i64,
    pub max_rss_growth_kib: u64,
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
