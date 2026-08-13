//! Exact duplicate detection over a run that settles more usage records than
//! the driver may hold.
//!
//! Duplicate accounting is only detectable by identity, and identity over a
//! twelve-hour soak is millions of `request_id`s. Keeping them in a set is the
//! unbounded growth this harness exists to find, in the harness — so the
//! fingerprints are written out instead, sharded by their own low bits, and
//! counted at the end one shard at a time. Detection stays *exact*: every
//! fingerprint is compared against every other one that could equal it,
//! because equal fingerprints always land in the same shard.
//!
//! The memory this costs is one shard, not one run: the driver holds a write
//! buffer per shard while the run is offered, and the largest shard while it is
//! tallied. A twelve-hour run at a thousand records a second spills 43 million
//! fingerprints — 350 MiB on disk under `target/`, and a few megabytes of
//! resident memory to count them.

use std::collections::{HashSet, VecDeque};
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};

/// How many shards the fingerprints are spilled across. Sized so a soak's
/// largest shard is megabytes rather than gigabytes when it is read back.
pub const SHARDS: usize = 64;

/// Bytes per fingerprint on disk: one little-endian `u64`, so a shard's length
/// is its count and a partial write is visible as a ragged tail.
const WIDTH: usize = std::mem::size_of::<u64>();

/// How many of the most recent fingerprints are also kept in memory. The final
/// count does not need them — that is the shards' job — but the settle wait
/// does: it has to know how many *distinct* records have arrived before the
/// shards can be read, and a run whose records repeat would otherwise be given
/// up on early and reported as having lost the ones still in flight. A
/// duplicate the gateway emits arrives near its original, so a window catches
/// it; one that does not is only ever *under*-counted, which makes the wait
/// patient rather than hasty.
const WINDOW: usize = 1 << 16;

/// An append-only, sharded record of every identified usage record's
/// fingerprint.
pub struct Ledger {
    dir: PathBuf,
    shards: Vec<BufWriter<File>>,
    recorded: u64,
    window: VecDeque<u64>,
    resident: HashSet<u64>,
    near_duplicates: u64,
}

impl Ledger {
    /// Create a ledger under `dir`, replacing whatever a previous run left
    /// there: a stale shard would report the last run's records as this run's
    /// duplicates.
    pub fn create(dir: &Path) -> Self {
        if dir.exists() {
            std::fs::remove_dir_all(dir).expect("the endurance fingerprint directory is writable");
        }
        std::fs::create_dir_all(dir).expect("the endurance fingerprint directory is writable");
        let shards = (0..SHARDS)
            .map(|shard| {
                let file = File::create(dir.join(format!("shard-{shard:02}.bin")))
                    .expect("an endurance fingerprint shard is writable");
                BufWriter::new(file)
            })
            .collect();
        Self {
            dir: dir.to_owned(),
            shards,
            recorded: 0,
            window: VecDeque::with_capacity(WINDOW),
            resident: HashSet::with_capacity(WINDOW),
            near_duplicates: 0,
        }
    }

    /// Record one fingerprint. Sharded on the *low* bits and written whole, so
    /// the shard a fingerprint lands in is a function of the fingerprint alone.
    pub fn record(&mut self, fingerprint: u64) {
        let shard = (fingerprint as usize) % SHARDS;
        self.shards[shard]
            .write_all(&fingerprint.to_le_bytes())
            .expect("an endurance fingerprint shard accepts a write");
        self.recorded += 1;
        if self.resident.insert(fingerprint) {
            self.window.push_back(fingerprint);
            if self.window.len() > WINDOW
                && let Some(evicted) = self.window.pop_front()
            {
                self.resident.remove(&evicted);
            }
        } else {
            self.near_duplicates += 1;
        }
    }

    /// How many fingerprints have been recorded, duplicates included. Echoed on
    /// the artifact beside the tally, so a reader can see what was counted.
    pub fn recorded(&self) -> u64 {
        self.recorded
    }

    /// A lower bound on how many *distinct* fingerprints have been recorded,
    /// available without reading the shards. Never above the true count, so a
    /// caller waiting on it waits at least as long as it should.
    pub fn distinct_at_least(&self) -> u64 {
        self.recorded - self.near_duplicates
    }

    /// Count the run, one shard at a time.
    pub fn tally(mut self) -> Tally {
        for shard in &mut self.shards {
            shard
                .flush()
                .expect("an endurance fingerprint shard flushes");
        }
        drop(self.shards);
        let (mut distinct, mut duplicates, mut peak) = (0, 0, 0);
        for shard in 0..SHARDS {
            let fingerprints = read_shard(&self.dir.join(format!("shard-{shard:02}.bin")));
            peak = peak.max(fingerprints.len() as u64);
            let mut seen = HashSet::with_capacity(fingerprints.len());
            for fingerprint in fingerprints {
                if seen.insert(fingerprint) {
                    distinct += 1;
                } else {
                    duplicates += 1;
                }
            }
        }
        Tally {
            recorded: self.recorded,
            distinct,
            duplicates,
            shards: SHARDS,
            peak_shard_fingerprints: peak,
            directory: self.dir,
        }
    }
}

/// What a run's fingerprints came to, and how they were counted.
#[derive(Debug, Clone)]
pub struct Tally {
    pub recorded: u64,
    pub distinct: u64,
    pub duplicates: u64,
    pub shards: usize,
    /// The largest number of fingerprints held in memory at once while
    /// counting: the bound the whole-run set used to have no answer for.
    pub peak_shard_fingerprints: u64,
    pub directory: PathBuf,
}

fn read_shard(path: &Path) -> Vec<u64> {
    let mut bytes = Vec::new();
    File::open(path)
        .expect("an endurance fingerprint shard is readable")
        .read_to_end(&mut bytes)
        .expect("an endurance fingerprint shard is readable");
    bytes
        .chunks_exact(WIDTH)
        .map(|chunk| u64::from_le_bytes(chunk.try_into().expect("a fingerprint is eight bytes")))
        .collect()
}
