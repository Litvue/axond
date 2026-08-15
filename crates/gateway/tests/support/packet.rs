//! The committed qualification packet: how far axond #156 actually got.
//!
//! Its children merge one at a time, and each one leaves the epic in a
//! different state — a harness with runs behind it, and harnesses with none.
//! `qualification/packet.toml`
//! is where that state is written down, and these types are what stop it from
//! being written down optimistically: a [`Status`] is checked against the paths
//! and the retained records the slice actually names, so a slice cannot say
//! `evidenced` while retaining nothing.
//!
//! Modelled on [`super::recovery`], for the same reason: a claim about
//! qualification is only worth as much as the file that can contradict it.

use std::path::PathBuf;

use figment::Figment;
use figment::providers::{Format, Toml};
use serde::{Deserialize, Serialize};

/// The packet, relative to the workspace root.
pub const MANIFEST_RELATIVE: &str = "qualification/packet.toml";

/// The operator-facing page that states the same packet in prose.
pub const CONTRACT_RELATIVE: &str = "docs/operations/qualification.md";

/// The schema this loader understands.
pub const MANIFEST_SCHEMA_VERSION: u32 = 1;

/// The epic the packet reports on.
pub const EPIC_ISSUE: u32 = 156;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Packet {
    pub schema_version: u32,
    #[serde(rename = "slice")]
    pub slices: Vec<Slice>,
    pub closure: Closure,
}

impl Packet {
    pub fn slice(&self, id: SliceId) -> &Slice {
        self.slices
            .iter()
            .find(|slice| slice.id == id)
            .unwrap_or_else(|| panic!("the packet is missing the {} slice", id.as_str()))
    }
}

/// One slice of #156: a question, whatever answers it so far, and what is left.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Slice {
    pub id: SliceId,
    /// The child issue that owns this slice.
    pub issue: u32,
    pub status: Status,
    pub question: String,
    /// The scenarios #156 names that this slice answers.
    pub covers: Vec<Scenario>,
    /// The committed inputs, present once the slice has a manifest.
    #[serde(default)]
    pub manifest: Option<String>,
    #[serde(default)]
    pub driver: Option<String>,
    /// The test that keeps a committed contract honest while its driver does
    /// not exist. It measures nothing, which is why it is not a driver.
    #[serde(default)]
    pub contract_test: Option<String>,
    #[serde(default)]
    pub contract: Option<String>,
    #[serde(default)]
    pub adr: Option<String>,
    /// Where the deterministic tier runs on every change, if it does.
    #[serde(default)]
    pub reduced_lane: Option<String>,
    /// The workflow that runs the heavy tier, if one exists.
    #[serde(default)]
    pub heavy_lane: Option<String>,
    /// What the slice's own manifest calls its heavy tier — `heavy` for
    /// capacity, `soak` for endurance, and `serving` for recovery. A record from any other tier is a
    /// correctness run, so the rung above `harnessed` is defined against this
    /// name rather than against a shared one.
    #[serde(default)]
    pub heavy_tier: Option<String>,
    /// Retained runs, as committed evidence records.
    #[serde(default)]
    pub retained: Vec<String>,
    /// What the slice still owes #156. Required unless it owes nothing.
    #[serde(default)]
    pub outstanding: Option<String>,
    /// Slices this one waits on, by issue.
    #[serde(default)]
    pub blocked_on: Vec<u32>,
}

impl Slice {
    /// Every repository path the slice names, whatever the field.
    pub fn paths(&self) -> Vec<&str> {
        self.manifest
            .iter()
            .chain(&self.driver)
            .chain(&self.contract_test)
            .chain(&self.contract)
            .chain(&self.adr)
            .chain(&self.heavy_lane)
            .map(String::as_str)
            .chain(self.retained.iter().map(String::as_str))
            .collect()
    }
}

/// How far a slice got. The order is the order of the ladder, and the contract
/// test derives each rung from the slice's own fields rather than trusting it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    /// No manifest and no driver: the question is written down and nothing runs.
    Unbuilt,
    /// A committed manifest and a `contract_test` that keeps it honest, but no
    /// driver — the scenarios are declared, never measured.
    Declared,
    /// A driver that runs, with no retained run of its heavy tier behind it.
    Harnessed,
    /// A driver, and retained evidence from a run of it.
    Evidenced,
}

impl Status {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unbuilt => "unbuilt",
            Self::Declared => "declared",
            Self::Harnessed => "harnessed",
            Self::Evidenced => "evidenced",
        }
    }
}

/// The slices #156 decomposes into. One variant each, so a slice cannot be
/// dropped from the packet without the contract test noticing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SliceId {
    Capacity,
    Endurance,
    Recovery,
    Fault,
    Rollout,
}

impl SliceId {
    pub const ALL: [Self; 5] = [
        Self::Capacity,
        Self::Endurance,
        Self::Recovery,
        Self::Fault,
        Self::Rollout,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Capacity => "capacity",
            Self::Endurance => "endurance",
            Self::Recovery => "recovery",
            Self::Fault => "fault",
            Self::Rollout => "rollout",
        }
    }
}

/// The scenarios axond #156 lists. Every one has to be some slice's
/// responsibility: a scenario no slice covers is one the epic would close over.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Scenario {
    /// Buffered and streaming throughput with latency, TTFT, and resources.
    ThroughputAndLatency,
    /// Mixed tenants, aliases, providers, and credentials.
    MixedWorkload,
    /// Response sizes, from a kilobyte to a quarter of a megabyte.
    ResponseSizes,
    /// Provider 429 and 5xx, DNS/TLS/connect stalls, truncation, idle streams.
    ProviderFaults,
    /// Redis and Postgres latency, outage, recovery, and fail-open/fail-closed.
    BackendOutage,
    /// Control-plane outage and revision convergence.
    ControlPlaneOutage,
    /// SIGTERM during buffered and streaming traffic.
    SigtermDrain,
    /// Rolling patch upgrades and rollback.
    RollingUpgrade,
    /// The 12–24 hour sustained mixed-workload soak.
    LongSoak,
}

impl Scenario {
    pub const ALL: [Self; 9] = [
        Self::ThroughputAndLatency,
        Self::MixedWorkload,
        Self::ResponseSizes,
        Self::ProviderFaults,
        Self::BackendOutage,
        Self::ControlPlaneOutage,
        Self::SigtermDrain,
        Self::RollingUpgrade,
        Self::LongSoak,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ThroughputAndLatency => "throughput_and_latency",
            Self::MixedWorkload => "mixed_workload",
            Self::ResponseSizes => "response_sizes",
            Self::ProviderFaults => "provider_faults",
            Self::BackendOutage => "backend_outage",
            Self::ControlPlaneOutage => "control_plane_outage",
            Self::SigtermDrain => "sigterm_drain",
            Self::RollingUpgrade => "rolling_upgrade",
            Self::LongSoak => "long_soak",
        }
    }
}

/// What closing #156 takes. `satisfied` is derived by the contract test from
/// the slices, so the packet cannot declare the epic done while a slice is
/// still owed a run.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Closure {
    pub issue: u32,
    pub satisfied: bool,
    pub requirement: Vec<String>,
}

/// One retained run: the summary of a result artifact, and the provenance that
/// says what it may be compared with. Written by
/// `ops/qualification-evidence.py`.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Record {
    pub schema_version: u32,
    #[serde(rename = "slice")]
    pub slice_id: SliceId,
    pub tier: String,
    pub runner: Runner,
    pub runner_note: String,
    pub source: RecordSource,
    pub binary: RecordBinary,
    pub inputs: RecordInputs,
    pub hardware: RecordHardware,
    #[serde(rename = "profile")]
    #[serde(default)]
    pub profiles: Vec<RecordProfile>,
    /// Non-capacity slices retain one observation per manifest workload. The
    /// raw JSON remains in the workflow artifact; this compact row carries the
    /// identity and verdict needed to decide whether it is promotable evidence.
    #[serde(rename = "observation", default)]
    pub observations: Vec<RecordObservation>,
    /// Recovery records retain one row for every executable scenario stage.
    #[serde(rename = "stage", default)]
    pub stages: Vec<RecordStage>,
}

/// Where a run happened, which is what bounds who may compare it with what.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Runner {
    /// A contributor's machine. Honest about itself, and not a fleet baseline.
    Local,
    /// A workflow run, on a runner whose shape is at least known.
    GithubActions,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecordSource {
    pub git_commit: String,
    pub git_dirty: bool,
    pub crate_version: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecordBinary {
    pub sha256: String,
    pub version: String,
    pub cargo_profile: String,
    pub rustc: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecordInputs {
    pub manifest: String,
    pub manifest_sha256: String,
    pub fixtures: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecordHardware {
    pub os: String,
    pub arch: String,
    pub kernel: String,
    pub cpu_model: String,
    pub cpus: u32,
    pub total_memory_kib: u64,
    pub containerized: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecordProfile {
    pub id: String,
    pub concurrency: u32,
    /// The config the process booted for this profile, which is per profile
    /// rather than per run: `mixed` boots a second credential per provider.
    pub config_sha256: String,
    pub requests: u64,
    pub elapsed_ms: u64,
    pub offered: u64,
    pub accepted: u64,
    pub rejected: u64,
    pub errors: u64,
    pub accepted_rps: f64,
    pub latency_p50_ms: f64,
    pub latency_p95_ms: f64,
    pub latency_p99_ms: f64,
    #[serde(default)]
    pub ttft_p95_ms: Option<f64>,
    /// The concurrency ceiling the profile booted, when it booted one below the
    /// load it offered. Present only on a profile built to be shed by.
    #[serde(default)]
    pub admission_max_in_flight: Option<u64>,
    /// The namespaces a multi-tenant profile served at once, and what crossed
    /// between them. `None` means the profile served one namespace, which is
    /// not the same claim as having served several and found nothing crossed.
    #[serde(default)]
    pub tenants: Option<u32>,
    #[serde(default)]
    pub foreign_credential_uses: Option<u64>,
    #[serde(default)]
    pub misattributed_usage_records: Option<u64>,
    /// The bound the replica held its upstreams to, and how the run met it.
    #[serde(default)]
    pub upstream_bound_ms: Option<u64>,
    #[serde(default)]
    pub over_bound: Option<u64>,
    #[serde(default)]
    pub max_latency_ms: Option<f64>,
    /// Whether the replica served one more request after the load stopped.
    #[serde(default)]
    pub served_after_load: Option<bool>,
    pub peak_rss_kib: u64,
    pub rss_growth_kib: u64,
    pub peak_sockets: u64,
    pub cpu_seconds: f64,
    pub missing_usage_records: u64,
    pub leaked_upstream_streams: u64,
    /// How many thresholds the run was judged against. A record with none is a
    /// measurement, not evidence.
    pub verdicts: u32,
    pub passed: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecordObservation {
    pub id: String,
    pub artifact_sha256: String,
    pub elapsed_ms: u64,
    pub verdicts: u32,
    pub passed: bool,
    /// Endurance records carry both the duration offered and the duration the
    /// committed tier requires. Other slices do not have a duration claim.
    #[serde(default)]
    pub duration_ms: Option<u64>,
    #[serde(default)]
    pub manifest_duration_ms: Option<u64>,
    #[serde(default)]
    pub requested_duration_ms: Option<u64>,
    #[serde(default)]
    pub duration_source: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecordStage {
    /// The fully-qualified `scenario/stage` key from the recovery manifest.
    pub id: String,
    pub runner: String,
    pub artifact_sha256: String,
    pub elapsed_ms: u64,
    pub verdicts: u32,
    pub passed: bool,
}

/// The workspace root, resolved from this crate rather than from the runner's
/// working directory.
pub fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

pub fn manifest_path() -> PathBuf {
    workspace_root().join(MANIFEST_RELATIVE)
}

pub fn contract_path() -> PathBuf {
    workspace_root().join(CONTRACT_RELATIVE)
}

/// Load the packet, refusing a schema this loader does not understand.
pub fn load() -> Packet {
    let path = manifest_path();
    let packet: Packet = Figment::from(Toml::file(&path))
        .extract()
        .unwrap_or_else(|e| panic!("{} is not a valid packet: {e}", path.display()));
    assert_eq!(
        packet.schema_version, MANIFEST_SCHEMA_VERSION,
        "unsupported qualification packet schema"
    );
    packet
}

/// A slice's own manifest, read for the two things the packet checks a record
/// against: the workloads it declares, and the digest of the bytes on disk.
///
/// A workload is a `[[profile]]` for the load-shaped slices and a
/// `[[scenario]]` for the sequence-shaped ones (rollout, recovery). Both are
/// read, because a record is held to the manifest it names whatever that
/// manifest calls its unit of work.
#[derive(Debug, Clone, Deserialize)]
pub struct SliceManifest {
    #[serde(rename = "profile", default)]
    pub profiles: Vec<SliceManifestWorkload>,
    #[serde(rename = "scenario", default)]
    pub scenarios: Vec<SliceManifestWorkload>,
}

impl SliceManifest {
    /// Every workload the manifest declares, whatever it calls them.
    pub fn workloads(&self) -> impl Iterator<Item = &SliceManifestWorkload> {
        self.profiles.iter().chain(&self.scenarios)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SliceManifestWorkload {
    pub id: String,
    #[serde(default)]
    pub soak: Option<SliceManifestTier>,
    #[serde(rename = "stage", default)]
    pub stages: Vec<SliceManifestStage>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SliceManifestTier {
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SliceManifestStage {
    pub id: String,
    pub status: String,
    #[serde(default)]
    pub runner: Option<String>,
}

/// Load whichever manifest a slice names, with the digest a run of it records
/// — so a record taken before an edit to that manifest stops matching it.
pub fn load_slice_manifest(relative: &str) -> (SliceManifest, String) {
    let path = workspace_root().join(relative);
    let bytes =
        std::fs::read(&path).unwrap_or_else(|e| panic!("{} is unreadable: {e}", path.display()));
    let manifest: SliceManifest = Figment::from(Toml::file(&path))
        .extract()
        .unwrap_or_else(|e| panic!("{} declares no readable workloads: {e}", path.display()));
    assert!(
        manifest.workloads().next().is_some(),
        "{} declares neither a profile nor a scenario, so a record naming it \
         could not be checked against anything",
        path.display()
    );
    (manifest, super::capacity::manifest::sha256_hex(&bytes))
}

/// Load one retained evidence record, by its path relative to the workspace.
pub fn load_record(relative: &str) -> Record {
    let path = workspace_root().join(relative);
    let record: Record = Figment::from(Toml::file(&path))
        .extract()
        .unwrap_or_else(|e| panic!("{} is not a valid evidence record: {e}", path.display()));
    assert_eq!(
        record.schema_version,
        MANIFEST_SCHEMA_VERSION,
        "{}: unsupported evidence record schema",
        path.display()
    );
    record
}

/// The prose packet, read as text so the two can be checked against each other.
pub fn contract_text() -> String {
    let path = contract_path();
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("{} is unreadable: {e}", path.display()))
}
