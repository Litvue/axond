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
//! A claim about qualification is only worth as much as the file that can
//! contradict it.

use std::path::PathBuf;

use figment::Figment;
use figment::providers::{Format, Toml};
use serde::{Deserialize, Serialize};

/// The packet, relative to the workspace root.
pub const MANIFEST_RELATIVE: &str = "qualification/packet.toml";

/// The operator-facing page that states the same packet in prose.
pub const CONTRACT_RELATIVE: &str = "docs/operations/qualification.md";

/// The schema this loader understands. Schema 2 adds the release-candidate
/// cohort that the remaining request-path heavy records must share before
/// closure. Recovery, rollout, and stateful-endurance slices were retired with
/// the tier matrix (ADR 0063 / #427).
pub const MANIFEST_SCHEMA_VERSION: u32 = 2;
pub const GENERIC_RECORD_SCHEMA_VERSION: u32 = 1;
pub const CAPACITY_RECORD_SCHEMA_VERSION: u32 = 2;

pub const QUALIFICATION_CANDIDATE_VERSION: &str = "0.4.0";
pub const PENDING_SOURCE_COMMIT: &str = "pending";

/// The epic the packet reports on.
pub const EPIC_ISSUE: u32 = 156;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Packet {
    pub schema_version: u32,
    pub cohort: QualificationCohort,
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

/// The immutable candidate identity shared by the remaining request-path
/// slices. While no candidate is frozen, `source_commit` is explicitly
/// `pending`; a closed packet requires an exact hexadecimal Git object id.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QualificationCohort {
    pub id: String,
    pub candidate_version: String,
    pub source_commit: String,
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
    /// What the slice's own manifest calls its evidencing tier — `heavy` for
    /// capacity, `smoke` for endurance, and `full` for fault. The 12-hour soak
    /// is a scheduled observational lane, not this field. A record from any
    /// other tier is not the ship gate, so the rung above `harnessed` is
    /// defined against this name.
    #[serde(default)]
    pub heavy_tier: Option<String>,
    /// Retained runs, as committed evidence records.
    #[serde(default)]
    pub retained: Vec<String>,
    /// Superseded records kept for audit history. They remain indexed and must
    /// exist, but they cannot promote a slice or contribute to closure.
    #[serde(default)]
    pub historical: Vec<String>,
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
            .chain(self.historical.iter().map(String::as_str))
            .collect()
    }
}

pub fn is_exact_git_commit(value: &str) -> bool {
    matches!(value.len(), 40 | 64) && value.bytes().all(|byte| byte.is_ascii_hexdigit())
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

/// The request-path slices that remain after ADR 0063 retired the tier matrix.
/// One variant each, so a slice cannot be dropped from the packet without the
/// contract test noticing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SliceId {
    Capacity,
    Endurance,
    Fault,
}

impl SliceId {
    pub const ALL: [Self; 3] = [Self::Capacity, Self::Endurance, Self::Fault];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Capacity => "capacity",
            Self::Endurance => "endurance",
            Self::Fault => "fault",
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
    /// Store latency, outage, recovery, and fail-open/fail-closed. Redis budget
    /// and rate-limit rows are skipped (ADR 0063); Postgres HA rows skip unless
    /// a DSN is supplied.
    BackendOutage,
    /// The 12–24 hour sustained mixed-workload soak.
    LongSoak,
}

impl Scenario {
    pub const ALL: [Self; 6] = [
        Self::ThroughputAndLatency,
        Self::MixedWorkload,
        Self::ResponseSizes,
        Self::ProviderFaults,
        Self::BackendOutage,
        Self::LongSoak,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ThroughputAndLatency => "throughput_and_latency",
            Self::MixedWorkload => "mixed_workload",
            Self::ResponseSizes => "response_sizes",
            Self::ProviderFaults => "provider_faults",
            Self::BackendOutage => "backend_outage",
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
    /// Compact capacity schema 2 binds every profile to its raw result.
    #[serde(default)]
    pub artifact_schema_version: Option<u32>,
    #[serde(default)]
    pub artifact_sha256: Option<String>,
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
    /// Exact queue-depth evidence emitted only by queue-enabled profiles.
    #[serde(default)]
    pub queue_observations: Option<u64>,
    #[serde(default)]
    pub queue_min_depth: Option<u64>,
    #[serde(default)]
    pub queue_max_depth: Option<u64>,
    #[serde(default)]
    pub queue_attributes: Option<u64>,
    #[serde(default)]
    pub queue_exact: Option<bool>,
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
    /// The schema of the raw artifact summarized by this row. Endurance and
    /// rollout observations require it because their artifact contracts evolve
    /// independently of this compact packet schema.
    #[serde(default)]
    pub artifact_schema_version: Option<u32>,
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
    /// Both endurance rows bind their exact request and correlation ledgers in
    /// addition to the raw JSON artifact. Promotion re-hashes these claims;
    /// the packet retains them so exactness cannot be reduced to an unverified
    /// path label after promotion.
    #[serde(default)]
    pub request_identities_sha256: Option<String>,
    #[serde(default)]
    pub request_identities_files: Option<u32>,
    #[serde(default)]
    pub request_identities_bytes: Option<u64>,
    #[serde(default)]
    pub correlations_sha256: Option<String>,
    #[serde(default)]
    pub correlations_files: Option<u32>,
    #[serde(default)]
    pub correlations_bytes: Option<u64>,
    /// Endurance binds one resource-sample JSONL; stateful endurance binds a
    /// non-empty set with one file per replica incarnation. Trend and bound
    /// verdicts are reconstructed from these retained samples.
    #[serde(default)]
    pub samples_sha256: Option<String>,
    #[serde(default)]
    pub samples_files: Option<u32>,
    #[serde(default)]
    pub samples_bytes: Option<u64>,
    /// Stateful endurance additionally binds the original request intervals
    /// used for exact fault-window classification and durable usage identities
    /// inside and outside the fault windows.
    #[serde(default)]
    pub correlation_windows_sha256: Option<String>,
    #[serde(default)]
    pub correlation_windows_files: Option<u32>,
    #[serde(default)]
    pub correlation_windows_bytes: Option<u64>,
    #[serde(default)]
    pub durable_identities_sha256: Option<String>,
    #[serde(default)]
    pub durable_identities_files: Option<u32>,
    #[serde(default)]
    pub durable_identities_bytes: Option<u64>,
    #[serde(default)]
    pub durable_outside_identities_sha256: Option<String>,
    #[serde(default)]
    pub durable_outside_identities_files: Option<u32>,
    #[serde(default)]
    pub durable_outside_identities_bytes: Option<u64>,
    /// Compact rollout record schema 4 preserves both serving executable
    /// identities, the checksum-pinned retained archive, and the shared durable
    /// serving proof after disposable raw artifacts expire.
    #[serde(default)]
    pub rollout_previous_version: Option<String>,
    #[serde(default)]
    pub rollout_previous_binary_sha256: Option<String>,
    #[serde(default)]
    pub rollout_candidate_version: Option<String>,
    #[serde(default)]
    pub rollout_candidate_binary_sha256: Option<String>,
    #[serde(default)]
    pub rollout_retained_archive_sha256: Option<String>,
    #[serde(default)]
    pub rollout_shared_stateful_revision: Option<String>,
    #[serde(default)]
    pub rollout_shared_alias: Option<String>,
    #[serde(default)]
    pub rollout_previous_serves_shared_alias: Option<bool>,
    #[serde(default)]
    pub rollout_candidate_serves_shared_alias: Option<bool>,
    /// The exact usage-reconciliation instrumentation retained after the raw
    /// rollout artifact expires.
    #[serde(default)]
    pub rollout_usage_reconciliation: Option<String>,
    #[serde(default)]
    pub rollout_exact_trace_replicas: Option<u32>,
    #[serde(default)]
    pub rollout_retained_trace_context: Option<String>,
    #[serde(default)]
    pub rollout_otlp_trace_exports: Option<u64>,
    #[serde(default)]
    pub rollout_otlp_trace_export_replicas: Option<u32>,
    #[serde(default)]
    pub rollout_otlp_trace_identities: Option<u64>,
    #[serde(default)]
    pub rollout_otlp_trace_identities_sha256: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecordStage {
    /// The fully-qualified `scenario/stage` key from the recovery manifest.
    pub id: String,
    pub runner: String,
    /// The manifest driver that produced the raw artifact. Optional only for
    /// superseded historical records; active schema-2 evidence requires it.
    #[serde(default)]
    pub driver: Option<String>,
    /// Raw recovery artifacts are schema 2. These fields are optional so the
    /// superseded v0.3.39 history remains readable; active evidence and closure
    /// require both claims.
    #[serde(default)]
    pub artifact_schema_version: Option<u32>,
    pub artifact_sha256: String,
    #[serde(default)]
    pub binary_sha256: Option<String>,
    /// Process-backed stages bind the exact executable they launched. Shell
    /// restore stages execute the record binary too, but retain that identity
    /// through their lane-level executable digest instead of these fields.
    #[serde(default)]
    pub executed_binary_sha256: Option<String>,
    #[serde(default)]
    pub execution_bound: Option<bool>,
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
    #[serde(rename = "row", default)]
    pub rows: Vec<SliceManifestWorkload>,
}

impl SliceManifest {
    /// Every workload the manifest declares, whatever it calls them.
    pub fn workloads(&self) -> impl Iterator<Item = &SliceManifestWorkload> {
        self.profiles
            .iter()
            .chain(&self.scenarios)
            .chain(&self.rows)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct SliceManifestWorkload {
    pub id: String,
    #[serde(default)]
    pub smoke: Option<SliceManifestTier>,
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
    #[serde(default)]
    pub driver: Option<String>,
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
    let expected = match record.slice_id {
        SliceId::Capacity => CAPACITY_RECORD_SCHEMA_VERSION,
        _ => GENERIC_RECORD_SCHEMA_VERSION,
    };
    assert_eq!(
        record.schema_version,
        expected,
        "{}: unsupported {} evidence record schema",
        path.display(),
        record.slice_id.as_str()
    );
    record
}

/// The prose packet, read as text so the two can be checked against each other.
pub fn contract_text() -> String {
    let path = contract_path();
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("{} is unreadable: {e}", path.display()))
}
