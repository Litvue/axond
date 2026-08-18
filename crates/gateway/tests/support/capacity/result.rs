//! The machine-readable result artifact.
//!
//! One JSON document per profile run, carrying both the measurements and the
//! exact inputs that produced them: binary, config, fixtures, manifest, and
//! hardware. A number without its provenance cannot be compared with a number
//! from another machine, and comparing them anyway is how a capacity claim
//! becomes folklore.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::Serialize;

use super::manifest::{self, Profile, RESULT_SCHEMA_VERSION, Thresholds, Tier};
use super::probe::ResourceReport;

#[derive(Debug, Clone, Serialize)]
pub struct CapacityResult {
    pub schema_version: u32,
    pub profile: ProfileEcho,
    pub run: RunMeta,
    pub environment: Environment,
    pub throughput: Throughput,
    /// End-to-end request latency: headers to last byte included.
    pub latency_ms: Percentiles,
    /// Time to first token, for streamed requests only.
    pub ttft_ms: Option<Percentiles>,
    /// First byte to last byte of a stream.
    pub stream_lifetime_ms: Option<Percentiles>,
    pub resources: ResourceReport,
    pub occupancy: Occupancy,
    /// Decoded OTLP evidence for the queueing profile. Absent on profiles that
    /// intentionally boot with no queue.
    pub queue: Option<QueueEvidence>,
    pub outcomes: Outcomes,
    pub usage_records: UsageRecords,
    pub upstream: Upstream,
    /// Per-namespace accounting, for the profiles that serve more than one.
    /// Absent — rather than empty — on a single-tenant profile, so a reader
    /// cannot mistake "not measured" for "measured and nothing crossed".
    pub tenancy: Option<Tenancy>,
    /// How the run held to the bound the replica declares, for the profiles
    /// that boot one an upstream will breach.
    pub deadlines: Option<Deadlines>,
    /// Whether the replica still served after the load stopped.
    pub recovery: Option<Recovery>,
    pub verdicts: Vec<Verdict>,
}

impl CapacityResult {
    pub fn failures(&self) -> Vec<&Verdict> {
        self.verdicts.iter().filter(|v| !v.passed).collect()
    }

    /// Write the artifact under `target/capacity/<tier>/<profile>.json` and
    /// return where it landed.
    pub fn write(&self) -> PathBuf {
        let dir = manifest::workspace_root()
            .join("target/capacity")
            .join(&self.profile.tier);
        std::fs::create_dir_all(&dir).expect("the capacity artifact directory is writable");
        let path = dir.join(format!("{}.json", self.profile.id));
        let json = serde_json::to_string_pretty(self).expect("the result artifact serializes");
        std::fs::write(&path, format!("{json}\n")).expect("the capacity artifact is writable");
        path
    }

    /// A one-line human summary, for a runner's log.
    pub fn summary(&self) -> String {
        let rss = self
            .resources
            .rss_kib
            .map_or_else(|| "n/a".to_owned(), |span| format!("{} KiB", span.peak));
        format!(
            "{} [{}]: {} accepted / {} offered in {} ms ({:.0} req/s), \
             p50 {:.1} ms p95 {:.1} ms p99 {:.1} ms, ttft p95 {}, peak rss {rss}, \
             sockets peak {}, cpu {:.2} s, usage {}/{}",
            self.profile.id,
            self.profile.tier,
            self.throughput.accepted,
            self.throughput.offered,
            self.throughput.elapsed_ms,
            self.throughput.accepted_rps,
            self.latency_ms.p50,
            self.latency_ms.p95,
            self.latency_ms.p99,
            self.ttft_ms
                .as_ref()
                .map_or_else(|| "n/a".to_owned(), |p| format!("{:.1} ms", p.p95)),
            self.resources
                .sockets
                .map_or_else(|| "n/a".to_owned(), |span| span.peak.to_string()),
            self.resources.cpu_seconds.unwrap_or_default(),
            self.usage_records.observed,
            self.usage_records.expected,
        )
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ProfileEcho {
    pub id: String,
    pub workload: String,
    pub description: String,
    pub tier: String,
    pub concurrency: usize,
    pub requests: usize,
    pub max_in_flight: Option<u64>,
    pub queue_capacity: Option<u64>,
    pub queue_wait_ms: Option<u64>,
    pub thresholds: Thresholds,
}

impl ProfileEcho {
    pub fn new(profile: &Profile, tier: Tier) -> Self {
        let scale = profile.scale(tier);
        Self {
            id: profile.id.clone(),
            workload: profile.workload.as_str().to_owned(),
            description: profile.description.clone(),
            tier: tier.as_str().to_owned(),
            concurrency: scale.concurrency,
            requests: scale.requests,
            max_in_flight: profile.max_in_flight,
            queue_capacity: profile.queue_capacity,
            queue_wait_ms: profile.queue_wait_ms,
            thresholds: profile.thresholds,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct RunMeta {
    pub started_at_unix_ms: u128,
    /// Whole milliseconds, rounding a positive sub-millisecond run up to one
    /// so a retained record cannot confuse a real run with missing duration.
    pub elapsed_ms: u128,
    /// The harness, so an artifact from an older driver is recognisable.
    pub harness: &'static str,
    pub harness_version: &'static str,
}

impl RunMeta {
    pub fn new(started_at: SystemTime, elapsed: Duration) -> Self {
        Self::for_harness("axond capacity harness", started_at, elapsed)
    }

    /// The same provenance for a sibling harness. The name is part of the
    /// artifact because a fault result and a capacity result answer different
    /// questions and must never be read as one another's.
    pub fn for_harness(harness: &'static str, started_at: SystemTime, elapsed: Duration) -> Self {
        Self {
            started_at_unix_ms: started_at
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
            elapsed_ms: duration_millis_ceil(elapsed),
            harness,
            harness_version: env!("CARGO_PKG_VERSION"),
        }
    }
}

/// Preserve a positive measured duration when an artifact's unit is whole
/// milliseconds. `Duration::as_millis` truncates, which would make a real
/// sub-millisecond run indistinguishable from a run with no elapsed time.
pub(crate) fn duration_millis_ceil(duration: Duration) -> u128 {
    duration.as_nanos().div_ceil(1_000_000)
}

#[cfg(test)]
mod run_meta_tests {
    use std::time::{Duration, UNIX_EPOCH};

    use super::{RunMeta, duration_millis_ceil};

    #[test]
    fn whole_millisecond_artifacts_retain_positive_sub_millisecond_durations() {
        assert_eq!(duration_millis_ceil(Duration::ZERO), 0);
        assert_eq!(duration_millis_ceil(Duration::from_nanos(1)), 1);
        assert_eq!(duration_millis_ceil(Duration::from_millis(1)), 1);
        assert_eq!(
            duration_millis_ceil(Duration::from_millis(1) + Duration::from_nanos(1)),
            2
        );
        assert_eq!(
            RunMeta::for_harness("test harness", UNIX_EPOCH, Duration::from_nanos(1)).elapsed_ms,
            1
        );
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Environment {
    pub binary: BinaryMeta,
    pub config: ConfigMeta,
    pub manifest: InputMeta,
    pub fixtures: Vec<InputMeta>,
    pub hardware: Hardware,
    pub toolchain: Toolchain,
    pub source: Source,
}

impl Environment {
    /// Collect everything a reader needs to reproduce or reject a comparison.
    /// `config` is the generated gateway config; the addresses in it are
    /// ephemeral, so it is normalised before it is hashed. `manifest_relative`
    /// names which committed manifest `manifest_text` came from, so an artifact
    /// from one harness is not read as another's.
    pub fn collect(
        config: &str,
        bind: &str,
        upstream: &str,
        manifest_relative: &str,
        manifest_text: &str,
    ) -> Self {
        Self::collect_normalizing(
            config,
            bind,
            upstream,
            &[],
            manifest_relative,
            manifest_text,
        )
    }

    /// As [`Environment::collect`], for a harness whose config carries further
    /// per-run values — an injector's port, a run-scoped key prefix. Each is
    /// replaced by its placeholder before hashing, because a config hash that
    /// changes every run makes two results of the same row incomparable, which
    /// is the one thing the hash exists to decide.
    pub fn collect_normalizing(
        config: &str,
        bind: &str,
        upstream: &str,
        also: &[(String, &str)],
        manifest_relative: &str,
        manifest_text: &str,
    ) -> Self {
        let mut normalized = config
            .replace(bind, "127.0.0.1:GATEWAY_PORT")
            .replace(upstream, "http://127.0.0.1:UPSTREAM_PORT");
        for (value, placeholder) in also {
            normalized = normalized.replace(value.as_str(), placeholder);
        }
        let binary = PathBuf::from(env!("CARGO_BIN_EXE_axond"));
        Self {
            binary: BinaryMeta {
                path: binary.display().to_string(),
                sha256: manifest::sha256_file(&binary),
                size_bytes: std::fs::metadata(&binary)
                    .map(|m| m.len())
                    .unwrap_or_default(),
                version: env!("CARGO_PKG_VERSION").to_owned(),
            },
            config: ConfigMeta {
                sha256: manifest::sha256_hex(normalized.as_bytes()),
                normalized_toml: normalized,
            },
            manifest: InputMeta {
                path: manifest_relative.to_owned(),
                sha256: manifest::sha256_hex(manifest_text.as_bytes()),
            },
            fixtures: fixtures(),
            hardware: Hardware::collect(),
            toolchain: Toolchain::collect(),
            source: Source::collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct BinaryMeta {
    pub path: String,
    pub sha256: String,
    pub size_bytes: u64,
    pub version: String,
}

/// The binary under test, named by hash. Shared with the rollout harness so two
/// artifacts from the same commit describe the same build and say so with the
/// same digest.
pub fn binary_meta() -> BinaryMeta {
    let binary = PathBuf::from(env!("CARGO_BIN_EXE_axond"));
    binary_meta_at(&binary)
}

/// Identify an arbitrary `axond` executable by its own bytes and reported
/// version. A rollout may not infer the retained release's version from the
/// candidate crate that compiled the harness.
pub fn binary_meta_at(binary: &std::path::Path) -> BinaryMeta {
    binary_meta_at_with_version_fallback(binary, None)
}

/// Identify an arbitrary executable, accepting a pinned external version only
/// when a legacy binary cannot self-report it.
///
/// The fallback is safe only when the caller separately authenticates the
/// bytes. The heavy rollout does that with the published archive checksum and
/// then compares this function's binary digest with the extracted executable.
/// Candidate binaries never receive a fallback.
pub fn binary_meta_at_with_version_fallback(
    binary: &std::path::Path,
    pinned_legacy_version: Option<&str>,
) -> BinaryMeta {
    let version_output = std::process::Command::new(binary)
        .arg("--version")
        .output()
        .unwrap_or_else(|error| panic!("{} --version failed to run: {error}", binary.display()));
    let version = if version_output.status.success() {
        let reported = String::from_utf8_lossy(&version_output.stdout);
        reported
            .split_whitespace()
            .last()
            .unwrap_or_else(|| panic!("{} --version produced no version", binary.display()))
            .to_owned()
    } else if let Some(version) = pinned_legacy_version {
        version.to_owned()
    } else {
        panic!(
            "{} --version failed: {}{}",
            binary.display(),
            String::from_utf8_lossy(&version_output.stdout),
            String::from_utf8_lossy(&version_output.stderr),
        );
    };
    BinaryMeta {
        path: binary.display().to_string(),
        sha256: manifest::sha256_file(binary),
        size_bytes: std::fs::metadata(binary)
            .map(|meta| meta.len())
            .unwrap_or_default(),
        version,
    }
}

#[cfg(all(test, unix))]
mod binary_meta_tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;

    #[test]
    fn a_checksum_pinned_legacy_binary_may_lack_version_self_report() {
        let directory =
            std::env::temp_dir().join(format!("axond-legacy-binary-meta-{}", std::process::id()));
        std::fs::create_dir_all(&directory).expect("the test directory is writable");
        let binary = directory.join("axond-v0.3.40");
        std::fs::write(&binary, "#!/bin/sh\nexit 2\n").expect("the test executable is written");
        let mut permissions = std::fs::metadata(&binary)
            .expect("the test executable exists")
            .permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&binary, permissions).expect("the test executable is runnable");

        let metadata = binary_meta_at_with_version_fallback(&binary, Some("0.3.40"));
        assert_eq!(metadata.version, "0.3.40");
        assert_eq!(metadata.path, binary.display().to_string());
        assert_eq!(metadata.sha256.len(), 64);

        std::fs::remove_dir_all(directory).expect("the test directory is removable");
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ConfigMeta {
    pub sha256: String,
    /// The config the process was booted with, with the ephemeral ports
    /// replaced: it is a fixture, and it names environment variables rather
    /// than carrying any credential.
    pub normalized_toml: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct InputMeta {
    pub path: String,
    pub sha256: String,
}

/// Every committed fixture the fake upstream can serve, hashed: a re-recorded
/// fixture changes the answer sizes and therefore the numbers.
fn fixtures() -> Vec<InputMeta> {
    let root = manifest::workspace_root().join("tests/fixtures");
    let mut found = Vec::new();
    collect_files(&root, &root, &mut found);
    found.sort_by(|a, b| a.path.cmp(&b.path));
    found
}

fn collect_files(root: &Path, dir: &Path, into: &mut Vec<InputMeta>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_files(root, &path, into);
        } else {
            let relative = path.strip_prefix(root).unwrap_or(&path);
            into.push(InputMeta {
                path: format!("tests/fixtures/{}", relative.display()),
                sha256: manifest::sha256_file(&path),
            });
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Hardware {
    pub os: &'static str,
    pub arch: &'static str,
    pub kernel: Option<String>,
    pub cpus: usize,
    pub cpu_model: Option<String>,
    pub total_memory_kib: Option<u64>,
    /// Whether the run happened inside a container, where the CPU and memory
    /// the kernel reports may not be the CPU and memory the process may use.
    pub containerized: bool,
}

impl Hardware {
    pub fn collect() -> Self {
        Self {
            os: std::env::consts::OS,
            arch: std::env::consts::ARCH,
            kernel: read_trimmed("/proc/sys/kernel/osrelease")
                .or_else(|| command_output("uname", &["-r"])),
            cpus: std::thread::available_parallelism().map_or(0, Into::into),
            cpu_model: cpu_model(),
            total_memory_kib: total_memory_kib(),
            containerized: Path::new("/.dockerenv").exists()
                || std::env::var_os("GITHUB_ACTIONS").is_some(),
        }
    }
}

fn cpu_model() -> Option<String> {
    std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|info| cpu_model_from(&info))
        .or_else(|| command_output("sysctl", &["-n", "machdep.cpu.brand_string"]))
}

fn cpu_model_from(info: &str) -> Option<String> {
    let fields: Vec<(&str, &str)> = info
        .lines()
        .filter_map(|line| line.split_once(':'))
        .map(|(key, value)| (key.trim(), value.trim()))
        .filter(|(_, value)| !value.is_empty())
        .collect();

    for preferred in ["model name", "Hardware"] {
        if let Some((_, model)) = fields.iter().find(|(key, _)| *key == preferred) {
            return Some((*model).to_owned());
        }
    }

    // AArch64 kernels commonly omit both x86's `model name` and the older
    // ARM `Hardware` field, especially inside virtualized Docker runtimes.
    // Each implementer/part pair is a kernel-provided CPU identity in that
    // format. Keep every distinct literal pair: translating values to a
    // marketing name would make the evidence depend on an incomplete lookup
    // table, while retaining only the first pair would hide heterogeneous ARM
    // systems.
    let identities = arm_cpu_identities_from(info)?;
    Some(
        identities
            .into_iter()
            .map(|(implementer, part)| format!("CPU implementer {implementer}, part {part}"))
            .collect::<Vec<_>>()
            .join("; "),
    )
}

fn arm_cpu_identities_from(info: &str) -> Option<BTreeSet<(String, String)>> {
    fn finish_record(
        implementers: &mut Vec<String>,
        parts: &mut Vec<String>,
        identities: &mut BTreeSet<(String, String)>,
    ) -> Option<()> {
        if implementers.is_empty() && parts.is_empty() {
            return Some(());
        }
        if implementers.len() != 1
            || parts.len() != 1
            || implementers[0].is_empty()
            || parts[0].is_empty()
        {
            return None;
        }
        identities.insert((implementers.pop()?, parts.pop()?));
        Some(())
    }

    let mut identities = BTreeSet::new();
    let mut implementers = Vec::new();
    let mut parts = Vec::new();

    for line in info.lines() {
        if line.trim().is_empty() {
            finish_record(&mut implementers, &mut parts, &mut identities)?;
            continue;
        }
        let Some((key, value)) = line.split_once(':') else {
            continue;
        };
        let key = key.trim();
        let value = value.trim();
        if key == "processor" {
            finish_record(&mut implementers, &mut parts, &mut identities)?;
        }
        match key {
            "CPU implementer" => implementers.push(value.to_owned()),
            "CPU part" => parts.push(value.to_owned()),
            _ => {}
        }
    }
    finish_record(&mut implementers, &mut parts, &mut identities)?;
    (!identities.is_empty()).then_some(identities)
}

#[cfg(test)]
mod cpu_model_tests {
    use super::cpu_model_from;

    #[test]
    fn prefers_a_human_model_name() {
        let info = concat!(
            "Hardware : Example Board\n",
            "model name : Example CPU\n",
            "CPU implementer : 0x61\n",
            "CPU part : 0x000\n",
        );
        assert_eq!(cpu_model_from(info).as_deref(), Some("Example CPU"));
    }

    #[test]
    fn accepts_the_older_arm_hardware_field() {
        let info = "Hardware : Example Board\nCPU implementer : 0x41\nCPU part : 0xd03\n";
        assert_eq!(cpu_model_from(info).as_deref(), Some("Example Board"));
    }

    #[test]
    fn records_aarch64_implementer_and_part_when_no_model_is_exposed() {
        let info = "processor : 0\nCPU implementer : 0x61\nCPU part : 0x000\n";
        assert_eq!(
            cpu_model_from(info).as_deref(),
            Some("CPU implementer 0x61, part 0x000")
        );
    }

    #[test]
    fn refuses_an_incomplete_cpu_identity() {
        assert_eq!(cpu_model_from("CPU implementer : 0x61\n"), None);
    }

    #[test]
    fn records_all_distinct_aarch64_identities_in_lexical_order() {
        let info = concat!(
            "processor : 7\n",
            "CPU implementer : 0x61\n",
            "CPU part : 0x000\n",
            "\n",
            "processor : 3\n",
            "CPU implementer : 0x41\n",
            "CPU part : 0xD03\n",
        );
        assert_eq!(
            cpu_model_from(info).as_deref(),
            Some("CPU implementer 0x41, part 0xD03; CPU implementer 0x61, part 0x000")
        );
    }

    #[test]
    fn deduplicates_homogeneous_aarch64_identities_without_blank_lines() {
        let info = concat!(
            "processor : 0\n",
            "CPU implementer : 0x61\n",
            "CPU part : 0x000\n",
            "processor : 1\n",
            "CPU implementer : 0x61\n",
            "CPU part : 0x000\n",
        );
        assert_eq!(
            cpu_model_from(info).as_deref(),
            Some("CPU implementer 0x61, part 0x000")
        );
    }

    #[test]
    fn refuses_any_incomplete_aarch64_processor_identity() {
        let info = concat!(
            "processor : 0\n",
            "CPU implementer : 0x61\n",
            "CPU part : 0x000\n",
            "\n",
            "processor : 1\n",
            "CPU implementer : 0x41\n",
        );
        assert_eq!(cpu_model_from(info), None);
    }

    #[test]
    fn refuses_ambiguous_aarch64_processor_identity() {
        let info = concat!(
            "processor : 0\n",
            "CPU implementer : 0x61\n",
            "CPU implementer : 0x41\n",
            "CPU part : 0x000\n",
        );
        assert_eq!(cpu_model_from(info), None);
    }
}

fn total_memory_kib() -> Option<u64> {
    std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|info| {
            info.lines().find_map(|line| {
                line.strip_prefix("MemTotal:")?
                    .split_whitespace()
                    .next()?
                    .parse()
                    .ok()
            })
        })
        .or_else(|| {
            command_output("sysctl", &["-n", "hw.memsize"])
                .and_then(|bytes| bytes.parse::<u64>().ok())
                .map(|bytes| bytes / 1024)
        })
}

fn read_trimmed(path: &str) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()
        .map(|text| text.trim().to_owned())
}

#[derive(Debug, Clone, Serialize)]
pub struct Toolchain {
    pub rustc: Option<String>,
    /// `debug` unless the harness was run against a release build.
    pub cargo_profile: &'static str,
}

impl Toolchain {
    pub fn collect() -> Self {
        Self {
            rustc: command_output("rustc", &["--version"]),
            cargo_profile: if cfg!(debug_assertions) {
                "debug"
            } else {
                "release"
            },
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Source {
    pub crate_version: &'static str,
    pub git_commit: Option<String>,
    /// `true` when the working tree had uncommitted changes, which makes the
    /// commit alone insufficient to reproduce the run.
    pub git_dirty: Option<bool>,
}

impl Source {
    pub fn collect() -> Self {
        Self {
            crate_version: env!("CARGO_PKG_VERSION"),
            git_commit: command_output("git", &["rev-parse", "HEAD"]),
            git_dirty: command_output("git", &["status", "--porcelain"])
                .map(|status| !status.is_empty()),
        }
    }
}

fn command_output(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program)
        .args(args)
        .current_dir(manifest::workspace_root())
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

#[derive(Debug, Clone, Serialize)]
pub struct Throughput {
    pub offered: u64,
    pub accepted: u64,
    pub rejected: u64,
    pub errors: u64,
    pub elapsed_ms: u128,
    pub offered_rps: f64,
    pub accepted_rps: f64,
    /// The driver is closed-loop: it holds `concurrency` requests in flight
    /// rather than pushing a fixed arrival rate, so the offered rate is a
    /// *result* of the service time, not an input.
    pub closed_loop: bool,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct Percentiles {
    pub count: usize,
    pub min: f64,
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub max: f64,
    pub mean: f64,
}

impl Percentiles {
    /// Nearest-rank percentiles over `values` in milliseconds. `None` when
    /// nothing was measured, so an absent measurement is absent rather than
    /// zero.
    pub fn of(values: &[f64]) -> Option<Self> {
        if values.is_empty() {
            return None;
        }
        let mut sorted = values.to_vec();
        sorted.sort_by(f64::total_cmp);
        let at = |q: f64| {
            let rank = (q * sorted.len() as f64).ceil() as usize;
            sorted[rank.clamp(1, sorted.len()) - 1]
        };
        Some(Self {
            count: sorted.len(),
            min: sorted[0],
            p50: at(0.50),
            p95: at(0.95),
            p99: at(0.99),
            max: sorted[sorted.len() - 1],
            mean: sorted.iter().sum::<f64>() / sorted.len() as f64,
        })
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct Occupancy {
    pub offered_concurrency: usize,
    /// The most requests the driver had in flight at once.
    pub in_flight_peak: u64,
    /// The most requests waiting for the first byte of their *answer* at once —
    /// response headers for a buffered request, the first relayed chunk for a
    /// stream. The driver-side view of the queue the replica is holding, which is
    /// what an operator sees as
    /// `axond.admission.in_flight{axond.admission.resource="queue"}` from the
    /// inside.
    pub awaiting_first_byte_peak: u64,
    /// The admission queue the process was configured with, for context.
    pub admission_queue_capacity: u64,
    /// The concurrency ceiling the profile booted the replica with, when it
    /// sets one. Absent means the shipped default was left far above the
    /// offered load, so the run measured the process rather than its own
    /// shedding.
    pub admission_max_in_flight: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueueEvidence {
    pub instrument: &'static str,
    pub points_exported: usize,
    pub observations: u64,
    pub min_depth: Option<u64>,
    pub max_depth: Option<u64>,
    pub explicit_bounds: Vec<u64>,
    pub bucket_counts: Vec<u64>,
    /// Must be zero: queue depth is deliberately label-free.
    pub attributes: usize,
    pub exact: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct Outcomes {
    pub by_status: BTreeMap<String, u64>,
    /// Typed error bodies of shed requests, keyed by `error.type`.
    pub rejections_by_error_type: BTreeMap<String, u64>,
    pub errors_by_error_type: BTreeMap<String, u64>,
    /// Requests the driver deliberately hung up on.
    pub client_cancelled: u64,
    /// Requests that failed at the transport rather than with a status.
    pub transport_failures: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct UsageRecords {
    /// One per admitted request: a shed request never reaches accounting.
    pub expected: u64,
    pub observed: u64,
    /// Records that never arrived within the settle deadline. A drop is the
    /// accounting failure a throughput number hides.
    pub missing: u64,
    pub by_status: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct Upstream {
    pub requests: u64,
    pub streams_opened: u64,
    /// Upstream response bodies still open once every client is gone: a leak.
    pub streams_open_at_end: i64,
}

/// What each namespace offered, was served, and was charged for.
#[derive(Debug, Clone, Serialize)]
pub struct Tenancy {
    pub by_namespace: BTreeMap<String, TenantCounts>,
    /// Upstream calls that cannot be accounted for by their owner being
    /// served: a tenant answered with a credential it does not own, or with
    /// the platform pool it did not opt into.
    pub foreign_credential_uses: u64,
    /// Usage rows filed against a namespace that did not send them.
    pub misattributed_usage_records: u64,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct TenantCounts {
    pub offered: u64,
    pub accepted: u64,
    /// Everything the replica took on rather than shed, whatever became of it:
    /// the denominator the isolation counts are measured against, because a
    /// request that failed after dispatch still spent its owner's credential.
    pub dispatched: u64,
    pub rejected: u64,
    pub usage_records: u64,
    /// Requests the upstream saw bearing this namespace's own credential. The
    /// credential itself is never recorded — only whose it was, and how often.
    pub upstream_calls: u64,
}

/// The bound the replica declares for an upstream that stops answering, and
/// what the run measured against it.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct Deadlines {
    pub bound_ms: u64,
    /// How far past the bound a request may still end before it counts as
    /// having outlived it, so a loaded runner does not fail a working bound.
    pub slack_multiple: u32,
    pub over_bound: u64,
    pub max_latency_ms: f64,
}

/// One request offered after the load stopped. A ceiling that keeps a permit,
/// or a bound that keeps a slot, is invisible in a throughput number and
/// obvious here.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct Recovery {
    pub served: bool,
    pub status: Option<u16>,
    pub latency_ms: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct Verdict {
    pub threshold: String,
    pub comparison: &'static str,
    pub value: f64,
    pub bound: f64,
    pub passed: bool,
}

impl Verdict {
    pub fn at_most(threshold: &str, value: f64, bound: f64) -> Self {
        Self {
            threshold: threshold.to_owned(),
            comparison: "<=",
            value,
            bound,
            passed: value <= bound,
        }
    }

    pub fn at_least(threshold: &str, value: f64, bound: f64) -> Self {
        Self {
            threshold: threshold.to_owned(),
            comparison: ">=",
            value,
            bound,
            passed: value >= bound,
        }
    }
}
