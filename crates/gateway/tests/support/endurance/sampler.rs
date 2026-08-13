//! The time series: what the gateway process held, sampled from outside it for
//! the length of the run.
//!
//! Capacity's probe keeps a baseline, a peak, and a settled reading, which is
//! all a two-minute run can support. Endurance needs the shape in between — a
//! peak says nothing about whether memory came back, and "came back" is the
//! whole question — so every sample is retained: appended to a JSONL file as it
//! is taken, and handed to the driver in batches so each segment can be
//! summarised while the run continues.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Serialize;

use crate::support::capacity::probe::procfs;

/// Ticks per second the kernel reports CPU time in, as the capacity probe
/// records it: `USER_HZ` is 100 on every Linux configuration Axond ships for.
pub const USER_HZ: f64 = 100.0;

/// One observation of the process.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct Sample {
    /// Milliseconds since the load started.
    pub at_ms: u128,
    pub rss_kib: u64,
    pub cpu_ticks: u64,
    /// Open descriptors of every kind: sockets, files, pipes, timers.
    pub fds: u64,
    pub sockets: u64,
}

/// Read the process once, or `None` off a `/proc` platform — or when the
/// process is gone.
pub fn sample(pid: u32, at: Duration) -> Option<Sample> {
    let (fds, sockets) = descriptors(pid)?;
    Some(Sample {
        at_ms: at.as_millis(),
        rss_kib: rss_kib(pid)?,
        cpu_ticks: cpu_ticks(pid)?,
        fds,
        sockets,
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
    let utime: u64 = fields.get(11)?.parse().ok()?;
    let stime: u64 = fields.get(12)?.parse().ok()?;
    Some(utime + stime)
}

/// Descriptors and, of those, sockets. Counted in one directory walk: the two
/// numbers answer different questions — a leaked connection shows in both, a
/// leaked file or timer only in the first — and reading them separately would
/// let them disagree about a moment.
fn descriptors(pid: u32) -> Option<(u64, u64)> {
    let entries = std::fs::read_dir(format!("/proc/{pid}/fd")).ok()?;
    let (mut fds, mut sockets) = (0, 0);
    for entry in entries.flatten() {
        fds += 1;
        if let Ok(target) = std::fs::read_link(entry.path())
            && target.to_string_lossy().starts_with("socket:")
        {
            sockets += 1;
        }
    }
    Some((fds, sockets))
}

/// Samples a process for the length of a run, retaining every sample.
pub struct Sampler {
    shared: Arc<Shared>,
    task: tokio::task::JoinHandle<()>,
    baseline: Option<Sample>,
    pid: u32,
    started: Instant,
    path: PathBuf,
}

#[derive(Default)]
struct Shared {
    /// Samples the driver has not folded into a segment yet.
    pending: Mutex<Vec<Sample>>,
    taken: AtomicU64,
}

impl Sampler {
    /// Take a baseline, start sampling every `interval`, and write the series
    /// to `path` as it is taken. Writing during the run rather than at the end
    /// is deliberate: a soak that is killed at hour eleven should still leave
    /// eleven hours of evidence behind.
    pub fn start(pid: u32, interval: Duration, path: &Path) -> Self {
        let started = Instant::now();
        let baseline = sample(pid, Duration::ZERO);
        let shared = Arc::new(Shared::default());
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("the endurance sample directory is writable");
        }
        let file = File::create(path).expect("the endurance sample file is writable");
        let task = tokio::spawn({
            let shared = shared.clone();
            // `baseline` is `Copy`, so the task takes its own.
            async move {
                let mut writer = BufWriter::new(file);
                if let Some(baseline) = baseline {
                    write_sample(&mut writer, &baseline);
                }
                loop {
                    tokio::time::sleep(interval).await;
                    let Some(current) = sample(pid, started.elapsed()) else {
                        continue;
                    };
                    write_sample(&mut writer, &current);
                    // Flushed every sample: the file is the evidence, and a
                    // buffered tail lost to a kill is evidence that was never
                    // written down.
                    let _ = writer.flush();
                    shared.taken.fetch_add(1, Ordering::Relaxed);
                    shared.pending.lock().expect("sample lock").push(current);
                }
            }
        });
        Self {
            shared,
            task,
            baseline,
            pid,
            started,
            path: path.to_owned(),
        }
    }

    /// The samples taken since the last drain.
    pub fn drain(&self) -> Vec<Sample> {
        std::mem::take(&mut *self.shared.pending.lock().expect("sample lock"))
    }

    pub fn baseline(&self) -> Option<Sample> {
        self.baseline
    }

    pub fn taken(&self) -> u64 {
        self.shared.taken.load(Ordering::Relaxed)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Stop sampling and read the process one last time — after the load has
    /// stopped, so what it reports is what the gateway *kept*.
    pub fn finish(self) -> Finished {
        self.task.abort();
        Finished {
            baseline: self.baseline,
            settled: sample(self.pid, self.started.elapsed()),
            pending: self.drain(),
            taken: self.taken(),
            procfs: procfs(),
        }
    }
}

fn write_sample(writer: &mut BufWriter<File>, sample: &Sample) {
    let line = serde_json::to_string(sample).expect("a sample serializes");
    let _ = writeln!(writer, "{line}");
}

pub struct Finished {
    pub baseline: Option<Sample>,
    pub settled: Option<Sample>,
    /// Samples taken since the driver's last segment boundary.
    pub pending: Vec<Sample>,
    pub taken: u64,
    pub procfs: bool,
}
