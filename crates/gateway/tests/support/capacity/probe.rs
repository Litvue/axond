//! What the gateway process cost while it served: resident memory, CPU, and
//! open sockets, sampled from `/proc` while the load is in flight.
//!
//! Sampling the process from outside is deliberate. The alternative is to
//! believe the gateway's own telemetry about its own saturation, which is the
//! one witness that cannot be trusted when the question is whether the process
//! is holding more than it should.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::Serialize;

/// Ticks per second the kernel reports CPU time in. `USER_HZ` is 100 on every
/// Linux configuration Axond ships for; the harness records the assumption
/// rather than linking libc for `sysconf(_SC_CLK_TCK)`.
const USER_HZ: f64 = 100.0;

/// How often the process is sampled. Frequent enough that a transient RSS peak
/// while streaming is seen, cheap enough that the sampler is not the load.
const INTERVAL: Duration = Duration::from_millis(20);

/// One observation of the process.
#[derive(Debug, Clone, Copy)]
pub struct ProcessSample {
    pub rss_kib: u64,
    pub cpu_ticks: u64,
    pub sockets: u64,
}

/// Whether this host has a `/proc` to sample at all. Distinguishing the platform
/// from a failed read matters: absent resource fields on a `/proc` host mean the
/// sampler lost its subject, not that the measurement was never possible, and
/// the difference is what keeps the memory gate from passing vacuously.
pub fn procfs() -> bool {
    std::path::Path::new("/proc/self/status").exists()
}

/// Sample a process, or `None` off a `/proc` platform — or when the process is
/// gone.
pub fn sample(pid: u32) -> Option<ProcessSample> {
    Some(ProcessSample {
        rss_kib: rss_kib(pid)?,
        cpu_ticks: cpu_ticks(pid)?,
        sockets: sockets(pid)?,
    })
}

fn rss_kib(pid: u32) -> Option<u64> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    status.lines().find_map(|line| {
        line.strip_prefix("VmRSS:")?
            .split_whitespace()
            .next()?
            .parse()
            .ok()
    })
}

/// `utime + stime` from `/proc/<pid>/stat`. The process name can contain spaces
/// and parentheses, so the fields are counted from the closing parenthesis.
fn cpu_ticks(pid: u32) -> Option<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let rest = stat.rsplit_once(')')?.1;
    let fields: Vec<&str> = rest.split_whitespace().collect();
    // `rest` starts at field 3 (`state`), so `utime` and `stime` — fields 14 and
    // 15 — are offsets 11 and 12.
    let utime: u64 = fields.get(11)?.parse().ok()?;
    let stime: u64 = fields.get(12)?.parse().ok()?;
    Some(utime + stime)
}

/// Open socket descriptors: the resource a leaked upstream connection or an
/// unclosed client stream consumes.
fn sockets(pid: u32) -> Option<u64> {
    let entries = std::fs::read_dir(format!("/proc/{pid}/fd")).ok()?;
    let mut count = 0;
    for entry in entries.flatten() {
        if let Ok(target) = std::fs::read_link(entry.path())
            && target.to_string_lossy().starts_with("socket:")
        {
            count += 1;
        }
    }
    Some(count)
}

/// The resource story of one run, all of it observed from outside the process.
#[derive(Debug, Clone, Serialize)]
pub struct ResourceReport {
    /// Whether the measurements below were taken. Off a `/proc` platform the run
    /// still qualifies every other property rather than being skipped; on one, a
    /// `false` here is itself a hard failure (`resource_sampling`), because a
    /// missing measurement and a passing one must not read alike.
    pub sampled: bool,
    /// Whether this host could have been sampled at all.
    pub procfs: bool,
    pub samples: u64,
    pub rss_kib: Option<Span>,
    pub sockets: Option<Span>,
    pub cpu_seconds: Option<f64>,
    /// CPU seconds per wall-clock second — above 1.0 on a multi-core runner.
    pub cpu_utilization: Option<f64>,
    pub user_hz: f64,
}

/// A quantity's baseline, peak, and settled value: growth that does not come
/// back is the interesting one.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct Span {
    pub baseline: u64,
    pub peak: u64,
    pub settled: u64,
}

impl Span {
    pub fn growth(&self) -> u64 {
        self.peak.saturating_sub(self.baseline)
    }
}

/// Samples a process in the background for the length of a run.
pub struct Sampler {
    shared: Arc<Shared>,
    task: tokio::task::JoinHandle<()>,
    baseline: Option<ProcessSample>,
    pid: u32,
}

#[derive(Default)]
struct Shared {
    samples: AtomicU64,
    peak_rss: AtomicU64,
    peak_sockets: AtomicU64,
}

impl Sampler {
    /// Take a baseline and start sampling. The baseline is taken before any load
    /// is offered, so growth is attributable to the run.
    pub fn start(pid: u32) -> Self {
        let baseline = sample(pid);
        let shared = Arc::new(Shared::default());
        if let Some(baseline) = baseline {
            shared.peak_rss.store(baseline.rss_kib, Ordering::Relaxed);
            shared
                .peak_sockets
                .store(baseline.sockets, Ordering::Relaxed);
        }
        let task = tokio::spawn({
            let shared = shared.clone();
            async move {
                loop {
                    if let Some(current) = sample(pid) {
                        shared.samples.fetch_add(1, Ordering::Relaxed);
                        shared
                            .peak_rss
                            .fetch_max(current.rss_kib, Ordering::Relaxed);
                        shared
                            .peak_sockets
                            .fetch_max(current.sockets, Ordering::Relaxed);
                    }
                    tokio::time::sleep(INTERVAL).await;
                }
            }
        });
        Self {
            shared,
            task,
            baseline,
            pid,
        }
    }

    /// Stop sampling and report, with `elapsed` the wall clock the load took.
    pub fn finish(self, elapsed: Duration) -> ResourceReport {
        self.task.abort();
        let settled = sample(self.pid);
        let samples = self.shared.samples.load(Ordering::Relaxed);
        let (Some(baseline), Some(settled)) = (self.baseline, settled) else {
            return ResourceReport {
                sampled: false,
                procfs: procfs(),
                samples,
                rss_kib: None,
                sockets: None,
                cpu_seconds: None,
                cpu_utilization: None,
                user_hz: USER_HZ,
            };
        };
        let ticks = settled.cpu_ticks.saturating_sub(baseline.cpu_ticks);
        let cpu_seconds = ticks as f64 / USER_HZ;
        ResourceReport {
            sampled: true,
            procfs: procfs(),
            samples,
            rss_kib: Some(Span {
                baseline: baseline.rss_kib,
                peak: self.shared.peak_rss.load(Ordering::Relaxed),
                settled: settled.rss_kib,
            }),
            sockets: Some(Span {
                baseline: baseline.sockets,
                peak: self.shared.peak_sockets.load(Ordering::Relaxed),
                settled: settled.sockets,
            }),
            cpu_seconds: Some(cpu_seconds),
            cpu_utilization: Some(cpu_seconds / elapsed.as_secs_f64().max(f64::EPSILON)),
            user_hz: USER_HZ,
        }
    }
}
