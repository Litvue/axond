//! Exact, bounded identity reconciliation for endurance qualification.
//!
//! Both ledgers spill fixed-width rows across deterministic shards and retain
//! only one shard (or one expected/observed shard pair) while tallying. Full
//! 128-bit identities are retained: a reduced hash is not exact evidence.

use std::collections::VecDeque;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use super::manifest::Ending;

/// Shard count chosen so a soak's largest in-memory tally stays small.
pub const SHARDS: usize = 64;

/// Fixed width of every request and trace identity stored by these ledgers.
pub const ID_WIDTH: usize = 16;
const CORRELATION_WIDTH: usize = ID_WIDTH + 1;
const REQUEST_PREFIX: &str = "req_";
/// Hard ceiling for one in-memory sort. With 64 deterministic shards this
/// admits 96 million rows per ledger while preventing a corrupt or clustered
/// artifact from turning terminal reconciliation into an unbounded allocation.
pub const MAX_SHARD_ROWS: usize = 1_500_000;
/// Per-shard write buffer. All shard files are pre-created, and a flush opens
/// only one of them at a time, keeping descriptors bounded independently of
/// how many exact ledgers a stateful run owns.
const SHARD_BUFFER_BYTES: usize = 64 * 1024;

/// Why text could not be accepted as an exact binary identity.
///
/// Input text is deliberately not retained because malformed fields can hold
/// material that must not be copied into qualification logs or artifacts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityError {
    MissingRequestPrefix,
    InvalidShape,
    InvalidHex,
    NotUuidV7,
    InvalidUuidVariant,
    ZeroTraceId,
}

impl fmt::Display for IdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::MissingRequestPrefix => "request identity is missing the req_ prefix",
            Self::InvalidShape => "identity does not have its canonical text shape",
            Self::InvalidHex => "identity contains a non-lowercase-hex digit",
            Self::NotUuidV7 => "request identity is not a UUIDv7",
            Self::InvalidUuidVariant => "request identity does not use the RFC 9562 variant",
            Self::ZeroTraceId => "trace identity is all zeroes",
        })
    }
}

impl std::error::Error for IdentityError {}

/// Parse canonical `req_<lowercase-hyphenated-UUIDv7>` text into all 128 bits.
pub fn parse_request_id(text: &str) -> Result<[u8; ID_WIDTH], IdentityError> {
    let uuid = text
        .strip_prefix(REQUEST_PREFIX)
        .ok_or(IdentityError::MissingRequestPrefix)?;
    if uuid.len() != 36
        || uuid.as_bytes().get(8) != Some(&b'-')
        || uuid.as_bytes().get(13) != Some(&b'-')
        || uuid.as_bytes().get(18) != Some(&b'-')
        || uuid.as_bytes().get(23) != Some(&b'-')
    {
        return Err(IdentityError::InvalidShape);
    }
    let mut compact = [0_u8; 32];
    let mut cursor = 0;
    for byte in uuid.bytes() {
        if byte == b'-' {
            continue;
        }
        let Some(slot) = compact.get_mut(cursor) else {
            return Err(IdentityError::InvalidShape);
        };
        *slot = byte;
        cursor += 1;
    }
    if cursor != compact.len() {
        return Err(IdentityError::InvalidShape);
    }
    let identity = parse_lower_hex(&compact)?;
    if identity[6] >> 4 != 7 {
        return Err(IdentityError::NotUuidV7);
    }
    if identity[8] >> 6 != 0b10 {
        return Err(IdentityError::InvalidUuidVariant);
    }
    Ok(identity)
}

/// Parse a canonical W3C trace ID: 32 lowercase hex digits and not all zeroes.
pub fn parse_trace_id(text: &str) -> Result<[u8; ID_WIDTH], IdentityError> {
    let compact: &[u8; 32] = text
        .as_bytes()
        .try_into()
        .map_err(|_| IdentityError::InvalidShape)?;
    validate_trace_bytes(parse_lower_hex(compact)?)
}

fn parse_lower_hex(text: &[u8; 32]) -> Result<[u8; ID_WIDTH], IdentityError> {
    let mut identity = [0_u8; ID_WIDTH];
    for (output, pair) in identity.iter_mut().zip(text.chunks_exact(2)) {
        let high = hex_nibble(pair[0]).ok_or(IdentityError::InvalidHex)?;
        let low = hex_nibble(pair[1]).ok_or(IdentityError::InvalidHex)?;
        *output = high << 4 | low;
    }
    Ok(identity)
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

fn validate_trace_bytes(identity: [u8; ID_WIDTH]) -> Result<[u8; ID_WIDTH], IdentityError> {
    if identity == [0; ID_WIDTH] {
        Err(IdentityError::ZeroTraceId)
    } else {
        Ok(identity)
    }
}

/// A malformed or unreadable fixed-width spill shard.
#[derive(Debug)]
pub enum ShardError {
    Io {
        path: PathBuf,
        source: io::Error,
    },
    Ragged {
        path: PathBuf,
        width: usize,
        bytes: usize,
    },
    TooLarge {
        path: PathBuf,
        rows: usize,
        maximum: usize,
    },
    InvalidRow {
        path: PathBuf,
        row: usize,
        field: &'static str,
        value: u8,
    },
}

impl fmt::Display for ShardError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { path, source } => {
                write!(formatter, "cannot read {}: {source}", path.display())
            }
            Self::Ragged { path, width, bytes } => write!(
                formatter,
                "{} has {bytes} bytes, not a multiple of its {width}-byte row width",
                path.display()
            ),
            Self::TooLarge {
                path,
                rows,
                maximum,
            } => write!(
                formatter,
                "{} contains {rows} rows, above the bounded reconciliation ceiling {maximum}",
                path.display()
            ),
            Self::InvalidRow {
                path,
                row,
                field,
                value,
            } => write!(
                formatter,
                "{} row {row} has invalid {field} code {value}",
                path.display()
            ),
        }
    }
}

impl std::error::Error for ShardError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Ragged { .. } | Self::TooLarge { .. } | Self::InvalidRow { .. } => None,
        }
    }
}

/// Append-only, sharded storage of canonical request identities.
pub struct Ledger {
    dir: PathBuf,
    shards: ShardWriters,
    recorded: u64,
}

impl Ledger {
    pub fn create(dir: &Path) -> Self {
        recreate_directory(dir);
        Self {
            dir: dir.to_owned(),
            shards: create_shards(dir, "request"),
            recorded: 0,
        }
    }

    /// Parse and record one canonical request ID.
    pub fn record(&mut self, request_id: &str) -> Result<(), IdentityError> {
        let identity = parse_request_id(request_id)?;
        write_identity(&mut self.shards, identity);
        self.recorded += 1;
        Ok(())
    }

    pub fn recorded(&self) -> u64 {
        self.recorded
    }

    pub fn tally(mut self) -> Result<Tally, ShardError> {
        self.shards.flush();
        drop(self.shards);
        let (mut distinct, mut duplicates, mut peak) = (0, 0, 0);
        for shard in 0..SHARDS {
            let path = shard_path(&self.dir, "request", shard);
            let mut identities = read_fixed_rows::<ID_WIDTH>(&path)?;
            peak = peak.max(identities.len() as u64);
            identities.sort_unstable();
            let mut previous = None;
            for identity in identities {
                if previous == Some(identity) {
                    duplicates += 1;
                } else {
                    distinct += 1;
                    previous = Some(identity);
                }
            }
        }
        Ok(Tally {
            recorded: self.recorded,
            distinct,
            duplicates,
            shards: SHARDS,
            peak_shard_rows: peak,
            exact: true,
            directory: self.dir,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Tally {
    pub recorded: u64,
    pub distinct: u64,
    pub duplicates: u64,
    pub shards: usize,
    pub peak_shard_rows: u64,
    pub exact: bool,
    pub directory: PathBuf,
}

/// Two exact request-ID sets, kept in bounded spill shards and merged only when
/// the run ends. Stateful endurance uses this for emitted rows versus durable
/// PostgreSQL rows; cardinality equality cannot prove they are the same rows.
pub struct IdentityPairLedger {
    dir: PathBuf,
    expected_shards: ShardWriters,
    observed_shards: ShardWriters,
    expected: u64,
    observed: u64,
}

impl IdentityPairLedger {
    pub fn create(dir: &Path) -> Self {
        recreate_directory(dir);
        Self {
            dir: dir.to_owned(),
            expected_shards: create_shards(dir, "expected-request"),
            observed_shards: create_shards(dir, "observed-request"),
            expected: 0,
            observed: 0,
        }
    }

    pub fn record_expected(&mut self, request_id: &str) -> Result<(), IdentityError> {
        let identity = parse_request_id(request_id)?;
        write_identity(&mut self.expected_shards, identity);
        self.expected += 1;
        Ok(())
    }

    pub fn record_observed(&mut self, request_id: &str) -> Result<(), IdentityError> {
        let identity = parse_request_id(request_id)?;
        write_identity(&mut self.observed_shards, identity);
        self.observed += 1;
        Ok(())
    }

    pub fn tally(mut self) -> Result<IdentityPairTally, ShardError> {
        self.expected_shards.flush();
        self.observed_shards.flush();
        drop(self.expected_shards);
        drop(self.observed_shards);
        let mut tally = IdentityPairTally {
            expected_rows: self.expected,
            observed_rows: self.observed,
            expected_distinct: 0,
            observed_distinct: 0,
            expected_duplicates: 0,
            observed_duplicates: 0,
            missing: 0,
            unexpected: 0,
            shards: SHARDS,
            peak_shard_rows: 0,
            exact: true,
            directory: self.dir.clone(),
        };
        for shard in 0..SHARDS {
            let mut expected =
                read_fixed_rows::<ID_WIDTH>(&shard_path(&self.dir, "expected-request", shard))?;
            let mut observed =
                read_fixed_rows::<ID_WIDTH>(&shard_path(&self.dir, "observed-request", shard))?;
            tally.peak_shard_rows = tally
                .peak_shard_rows
                .max((expected.len() + observed.len()) as u64);
            expected.sort_unstable();
            observed.sort_unstable();
            let expected_rows = expected.len() as u64;
            let observed_rows = observed.len() as u64;
            expected.dedup();
            observed.dedup();
            tally.expected_distinct += expected.len() as u64;
            tally.observed_distinct += observed.len() as u64;
            tally.expected_duplicates += expected_rows - expected.len() as u64;
            tally.observed_duplicates += observed_rows - observed.len() as u64;
            reconcile_identity_sets(
                &expected,
                &observed,
                &mut tally.missing,
                &mut tally.unexpected,
            );
        }
        Ok(tally)
    }
}

#[derive(Debug, Clone)]
pub struct IdentityPairTally {
    pub expected_rows: u64,
    pub observed_rows: u64,
    pub expected_distinct: u64,
    pub observed_distinct: u64,
    pub expected_duplicates: u64,
    pub observed_duplicates: u64,
    pub missing: u64,
    pub unexpected: u64,
    pub shards: usize,
    pub peak_shard_rows: u64,
    pub exact: bool,
    pub directory: PathBuf,
}

fn reconcile_identity_sets(
    expected: &[[u8; ID_WIDTH]],
    observed: &[[u8; ID_WIDTH]],
    missing: &mut u64,
    unexpected: &mut u64,
) {
    let (mut left, mut right) = (0, 0);
    while left < expected.len() && right < observed.len() {
        match expected[left].cmp(&observed[right]) {
            std::cmp::Ordering::Less => {
                *missing += 1;
                left += 1;
            }
            std::cmp::Ordering::Greater => {
                *unexpected += 1;
                right += 1;
            }
            std::cmp::Ordering::Equal => {
                left += 1;
                right += 1;
            }
        }
    }
    *missing += (expected.len() - left) as u64;
    *unexpected += (observed.len() - right) as u64;
}

/// Why an observed usage row could not enter correlation reconciliation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservedCorrelationError {
    Identity(IdentityError),
    UnknownStatus,
}

impl fmt::Display for ObservedCorrelationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identity(error) => fmt::Display::fmt(error, formatter),
            Self::UnknownStatus => formatter.write_str("usage status is not in the schema"),
        }
    }
}

impl std::error::Error for ObservedCorrelationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Identity(error) => Some(error),
            Self::UnknownStatus => None,
        }
    }
}

/// Paired exact reconciliation of planned requests and observed usage rows.
pub struct CorrelationLedger {
    dir: PathBuf,
    expected_shards: ShardWriters,
    observed_shards: ShardWriters,
    expected: u64,
    observed: u64,
}

impl CorrelationLedger {
    pub fn create(dir: &Path) -> Self {
        recreate_directory(dir);
        Self {
            dir: dir.to_owned(),
            expected_shards: create_shards(dir, "expected"),
            observed_shards: create_shards(dir, "observed"),
            expected: 0,
            observed: 0,
        }
    }

    /// Record a settling request's full planned trace identity and ending.
    pub fn record_expected(
        &mut self,
        trace_id: [u8; ID_WIDTH],
        ending: Ending,
    ) -> Result<(), IdentityError> {
        let trace_id = validate_trace_bytes(trace_id)?;
        write_correlation_row(&mut self.expected_shards, trace_id, ending_code(ending));
        self.expected += 1;
        Ok(())
    }

    /// Parse and record a usage row's canonical 32-hex trace ID and status.
    pub fn record_observed(
        &mut self,
        trace_id: &str,
        status: &str,
    ) -> Result<(), ObservedCorrelationError> {
        let trace_id = parse_trace_id(trace_id).map_err(ObservedCorrelationError::Identity)?;
        let status =
            ObservedStatus::parse(status).ok_or(ObservedCorrelationError::UnknownStatus)?;
        write_correlation_row(&mut self.observed_shards, trace_id, status.code());
        self.observed += 1;
        Ok(())
    }

    pub fn expected(&self) -> u64 {
        self.expected
    }

    pub fn observed(&self) -> u64 {
        self.observed
    }

    /// Sort and merge only one expected/observed shard pair at a time.
    pub fn tally(mut self) -> Result<CorrelationTally, ShardError> {
        self.expected_shards.flush();
        self.observed_shards.flush();
        drop(self.expected_shards);
        drop(self.observed_shards);
        let (
            mut expected_count,
            mut observed_count,
            mut missing,
            mut unexpected,
            mut status_mismatches,
            mut peak,
        ) = (0, 0, 0, 0, 0, 0);
        for shard in 0..SHARDS {
            let mut expected = read_expected_rows(&shard_path(&self.dir, "expected", shard))?;
            let mut observed = read_observed_rows(&shard_path(&self.dir, "observed", shard))?;
            expected_count += expected.len() as u64;
            observed_count += observed.len() as u64;
            peak = peak.max((expected.len() + observed.len()) as u64);
            expected.sort_unstable();
            observed.sort_unstable();
            reconcile_shard(
                &expected,
                &observed,
                &mut missing,
                &mut unexpected,
                &mut status_mismatches,
            );
        }
        Ok(CorrelationTally {
            expected: expected_count,
            observed: observed_count,
            missing,
            unexpected,
            status_mismatches,
            shards: SHARDS,
            peak_shard_rows: peak,
            exact: true,
            directory: self.dir,
        })
    }
}

/// Exact caller-to-usage reconciliation. Uncorrelated rows stay in a separate
/// caller-owned counter because they never enter this ledger.
#[derive(Debug, Clone)]
pub struct CorrelationTally {
    pub expected: u64,
    pub observed: u64,
    pub missing: u64,
    pub unexpected: u64,
    pub status_mismatches: u64,
    pub shards: usize,
    pub peak_shard_rows: u64,
    pub exact: bool,
    pub directory: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ExpectedRow {
    identity: [u8; ID_WIDTH],
    ending: Ending,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct ObservedRow {
    identity: [u8; ID_WIDTH],
    status: ObservedStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ObservedStatus {
    Ok,
    UpstreamError,
    ClientCancelled,
    Partial,
    Rejected,
}

impl ObservedStatus {
    const ALL: [Self; 5] = [
        Self::Ok,
        Self::UpstreamError,
        Self::ClientCancelled,
        Self::Partial,
        Self::Rejected,
    ];

    fn parse(status: &str) -> Option<Self> {
        match status {
            "ok" => Some(Self::Ok),
            "upstream_error" => Some(Self::UpstreamError),
            "client_cancelled" => Some(Self::ClientCancelled),
            "partial" => Some(Self::Partial),
            "rejected" => Some(Self::Rejected),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::UpstreamError => "upstream_error",
            Self::ClientCancelled => "client_cancelled",
            Self::Partial => "partial",
            Self::Rejected => "rejected",
        }
    }

    const fn code(self) -> u8 {
        self as u8
    }

    fn from_code(code: u8) -> Option<Self> {
        Self::ALL.get(usize::from(code)).copied()
    }
}

fn ending_code(ending: Ending) -> u8 {
    match ending {
        Ending::Complete => 0,
        Ending::Cancelled => 1,
        Ending::Dropped => 2,
        Ending::Faulted => 3,
    }
}

fn ending_from_code(code: u8) -> Option<Ending> {
    Ending::ALL.get(usize::from(code)).copied()
}

fn recreate_directory(dir: &Path) {
    if dir.exists() {
        std::fs::remove_dir_all(dir).expect("the endurance ledger directory is writable");
    }
    std::fs::create_dir_all(dir).expect("the endurance ledger directory is writable");
}

struct ShardWriters {
    dir: PathBuf,
    stem: &'static str,
    buffers: Vec<Vec<u8>>,
}

impl ShardWriters {
    fn write(&mut self, shard: usize, bytes: &[u8]) {
        self.buffers[shard].extend_from_slice(bytes);
        if self.buffers[shard].len() >= SHARD_BUFFER_BYTES {
            self.flush_one(shard);
        }
    }

    fn flush_one(&mut self, shard: usize) {
        if self.buffers[shard].is_empty() {
            return;
        }
        let path = shard_path(&self.dir, self.stem, shard);
        let mut file = OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap_or_else(|error| panic!("cannot append {}: {error}", path.display()));
        file.write_all(&self.buffers[shard])
            .unwrap_or_else(|error| panic!("cannot append {}: {error}", path.display()));
        self.buffers[shard].clear();
    }

    fn flush(&mut self) {
        for shard in 0..SHARDS {
            self.flush_one(shard);
        }
    }
}

fn create_shards(dir: &Path, stem: &'static str) -> ShardWriters {
    for shard in 0..SHARDS {
        File::create(shard_path(dir, stem, shard)).expect("an endurance ledger shard is writable");
    }
    ShardWriters {
        dir: dir.to_owned(),
        stem,
        buffers: (0..SHARDS).map(|_| Vec::new()).collect(),
    }
}

fn shard_path(dir: &Path, stem: &str, shard: usize) -> PathBuf {
    dir.join(format!("{stem}-shard-{shard:02}.bin"))
}

fn shard_for(identity: &[u8; ID_WIDTH]) -> usize {
    usize::from(identity[ID_WIDTH - 1]) % SHARDS
}

fn write_identity(shards: &mut ShardWriters, identity: [u8; ID_WIDTH]) {
    shards.write(shard_for(&identity), &identity);
}

fn write_correlation_row(shards: &mut ShardWriters, identity: [u8; ID_WIDTH], code: u8) {
    let shard = shard_for(&identity);
    let mut row = [0_u8; CORRELATION_WIDTH];
    row[..ID_WIDTH].copy_from_slice(&identity);
    row[ID_WIDTH] = code;
    shards.write(shard, &row);
}

fn read_fixed_rows<const WIDTH: usize>(path: &Path) -> Result<Vec<[u8; WIDTH]>, ShardError> {
    let bytes_on_disk = std::fs::metadata(path)
        .map_err(|source| ShardError::Io {
            path: path.to_owned(),
            source,
        })?
        .len();
    if bytes_on_disk % WIDTH as u64 != 0 {
        return Err(ShardError::Ragged {
            path: path.to_owned(),
            width: WIDTH,
            bytes: usize::try_from(bytes_on_disk).unwrap_or(usize::MAX),
        });
    }
    let rows = bytes_on_disk / WIDTH as u64;
    if rows > MAX_SHARD_ROWS as u64 {
        return Err(ShardError::TooLarge {
            path: path.to_owned(),
            rows: usize::try_from(rows).unwrap_or(usize::MAX),
            maximum: MAX_SHARD_ROWS,
        });
    }
    let bytes_on_disk = usize::try_from(bytes_on_disk)
        .expect("a shard below the row ceiling fits the address space");
    let mut bytes = Vec::new();
    File::open(path)
        .map_err(|source| ShardError::Io {
            path: path.to_owned(),
            source,
        })?
        .read_to_end(&mut bytes)
        .map_err(|source| ShardError::Io {
            path: path.to_owned(),
            source,
        })?;
    debug_assert_eq!(bytes.len(), bytes_on_disk);
    Ok(bytes
        .chunks_exact(WIDTH)
        .map(|chunk| chunk.try_into().expect("the row width was checked"))
        .collect())
}

fn read_expected_rows(path: &Path) -> Result<Vec<ExpectedRow>, ShardError> {
    read_fixed_rows::<CORRELATION_WIDTH>(path)?
        .into_iter()
        .enumerate()
        .map(|(row, bytes)| {
            let identity = bytes[..ID_WIDTH].try_into().expect("16 identity bytes");
            let value = bytes[ID_WIDTH];
            let ending = ending_from_code(value).ok_or_else(|| ShardError::InvalidRow {
                path: path.to_owned(),
                row,
                field: "ending",
                value,
            })?;
            Ok(ExpectedRow { identity, ending })
        })
        .collect()
}

fn read_observed_rows(path: &Path) -> Result<Vec<ObservedRow>, ShardError> {
    read_fixed_rows::<CORRELATION_WIDTH>(path)?
        .into_iter()
        .enumerate()
        .map(|(row, bytes)| {
            let identity = bytes[..ID_WIDTH].try_into().expect("16 identity bytes");
            let value = bytes[ID_WIDTH];
            let status =
                ObservedStatus::from_code(value).ok_or_else(|| ShardError::InvalidRow {
                    path: path.to_owned(),
                    row,
                    field: "status",
                    value,
                })?;
            Ok(ObservedRow { identity, status })
        })
        .collect()
}

fn reconcile_shard(
    expected: &[ExpectedRow],
    observed: &[ObservedRow],
    missing: &mut u64,
    unexpected: &mut u64,
    status_mismatches: &mut u64,
) {
    let (mut expected_at, mut observed_at) = (0, 0);
    while expected_at < expected.len() || observed_at < observed.len() {
        match (expected.get(expected_at), observed.get(observed_at)) {
            (Some(expected_row), Some(observed_row))
                if expected_row.identity == observed_row.identity =>
            {
                let expected_end = group_end_expected(expected, expected_at);
                let observed_end = group_end_observed(observed, observed_at);
                let expected_group = &expected[expected_at..expected_end];
                let observed_group = &observed[observed_at..observed_end];
                let paired = expected_group.len().min(observed_group.len()) as u64;
                *missing += expected_group.len().saturating_sub(observed_group.len()) as u64;
                *unexpected += observed_group.len().saturating_sub(expected_group.len()) as u64;
                *status_mismatches += paired - compatible_pairs(expected_group, observed_group);
                expected_at = expected_end;
                observed_at = observed_end;
            }
            (Some(expected_row), Some(observed_row))
                if expected_row.identity < observed_row.identity =>
            {
                let end = group_end_expected(expected, expected_at);
                *missing += (end - expected_at) as u64;
                expected_at = end;
            }
            (Some(_), Some(_)) => {
                let end = group_end_observed(observed, observed_at);
                *unexpected += (end - observed_at) as u64;
                observed_at = end;
            }
            (Some(_), None) => {
                *missing += (expected.len() - expected_at) as u64;
                break;
            }
            (None, Some(_)) => {
                *unexpected += (observed.len() - observed_at) as u64;
                break;
            }
            (None, None) => break,
        }
    }
}

fn group_end_expected(rows: &[ExpectedRow], start: usize) -> usize {
    rows[start + 1..]
        .iter()
        .position(|row| row.identity != rows[start].identity)
        .map_or(rows.len(), |offset| start + 1 + offset)
}

fn group_end_observed(rows: &[ObservedRow], start: usize) -> usize {
    rows[start + 1..]
        .iter()
        .position(|row| row.identity != rows[start].identity)
        .map_or(rows.len(), |offset| start + 1 + offset)
}

/// Maximum compatible pairing for one identity. Expected correlations should
/// be unique, but this tiny flow keeps repeated malformed plans exact too.
fn compatible_pairs(expected: &[ExpectedRow], observed: &[ObservedRow]) -> u64 {
    const SOURCE: usize = 0;
    const ENDING_START: usize = 1;
    const STATUS_START: usize = ENDING_START + 4;
    const SINK: usize = STATUS_START + 5;
    const NODES: usize = SINK + 1;
    let mut endings = [0_u64; 4];
    let mut statuses = [0_u64; 5];
    for row in expected {
        endings[usize::from(ending_code(row.ending))] += 1;
    }
    for row in observed {
        statuses[usize::from(row.status.code())] += 1;
    }
    let mut capacity = [[0_u64; NODES]; NODES];
    for (index, count) in endings.into_iter().enumerate() {
        capacity[SOURCE][ENDING_START + index] = count;
    }
    for (index, count) in statuses.into_iter().enumerate() {
        capacity[STATUS_START + index][SINK] = count;
    }
    let edge_capacity = expected.len().min(observed.len()) as u64;
    for (ending_index, ending) in Ending::ALL.into_iter().enumerate() {
        for (status_index, status) in ObservedStatus::ALL.into_iter().enumerate() {
            if ending.settles(status.as_str()) {
                capacity[ENDING_START + ending_index][STATUS_START + status_index] = edge_capacity;
            }
        }
    }
    max_flow(&mut capacity, SOURCE, SINK)
}

fn max_flow<const NODES: usize>(
    capacity: &mut [[u64; NODES]; NODES],
    source: usize,
    sink: usize,
) -> u64 {
    let mut total = 0;
    loop {
        let mut parent = [usize::MAX; NODES];
        parent[source] = source;
        let mut queue = VecDeque::from([source]);
        while let Some(from) = queue.pop_front() {
            for to in 0..NODES {
                if parent[to] == usize::MAX && capacity[from][to] > 0 {
                    parent[to] = from;
                    queue.push_back(to);
                }
            }
        }
        if parent[sink] == usize::MAX {
            return total;
        }
        let mut increment = u64::MAX;
        let mut node = sink;
        while node != source {
            let from = parent[node];
            increment = increment.min(capacity[from][node]);
            node = from;
        }
        node = sink;
        while node != source {
            let from = parent[node];
            capacity[from][node] -= increment;
            capacity[node][from] += increment;
            node = from;
        }
        total += increment;
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

    fn test_dir(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "axond-endurance-{label}-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn trace(last: u8) -> [u8; ID_WIDTH] {
        let mut identity = [0_u8; ID_WIDTH];
        identity[0] = 1;
        identity[ID_WIDTH - 1] = last;
        identity
    }

    fn trace_text(identity: [u8; ID_WIDTH]) -> String {
        identity
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<Vec<_>>()
            .join("")
    }

    #[test]
    fn parses_only_canonical_req_uuid_v7_text() {
        let parsed = parse_request_id("req_0192f5e1-2b3c-7def-8123-456789abcdef").unwrap();
        assert_eq!(
            (parsed[6] >> 4, parsed[8] >> 6, parsed[15]),
            (7, 0b10, 0xef)
        );
        assert_eq!(
            parse_request_id("0192f5e1-2b3c-7def-8123-456789abcdef"),
            Err(IdentityError::MissingRequestPrefix)
        );
        assert_eq!(
            parse_request_id("req_0192F5E1-2b3c-7def-8123-456789abcdef"),
            Err(IdentityError::InvalidHex)
        );
        assert_eq!(
            parse_request_id("req_0192f5e1-2b3c-4def-8123-456789abcdef"),
            Err(IdentityError::NotUuidV7)
        );
        assert_eq!(
            parse_request_id("req_0192f5e1-2b3c-7def-4123-456789abcdef"),
            Err(IdentityError::InvalidUuidVariant)
        );
        assert_eq!(
            parse_trace_id("00000000000000000000000000000000"),
            Err(IdentityError::ZeroTraceId)
        );
        assert_eq!(
            parse_trace_id("0192F5E12B3C7DEF8123456789ABCDEF"),
            Err(IdentityError::InvalidHex)
        );
    }

    #[test]
    fn request_ledger_counts_full_identities_and_rejects_malformed_text() {
        let dir = test_dir("request-ledger");
        let mut ledger = Ledger::create(&dir);
        let first = "req_0192f5e1-2b3c-7000-8000-000000000001";
        let second = "req_0192f5e1-2b3c-7000-8000-000000000002";
        ledger.record(first).unwrap();
        ledger.record(second).unwrap();
        ledger.record(first).unwrap();
        assert!(ledger.record("req_not-a-uuid").is_err());
        let tally = ledger.tally().unwrap();
        assert_eq!(
            (tally.recorded, tally.distinct, tally.duplicates),
            (3, 2, 1)
        );
        assert!(tally.exact);
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn exact_identity_sets_do_not_let_unrelated_rows_hide_missing_rows() {
        let dir = test_dir("identity-pair");
        let mut ledger = IdentityPairLedger::create(&dir);
        let a = "req_0192f5e1-2b3c-7000-8000-000000000001";
        let b = "req_0192f5e1-2b3c-7000-8000-000000000002";
        let c = "req_0192f5e1-2b3c-7000-8000-000000000003";
        ledger.record_expected(a).unwrap();
        ledger.record_expected(b).unwrap();
        ledger.record_expected(b).unwrap();
        ledger.record_observed(b).unwrap();
        ledger.record_observed(c).unwrap();
        ledger.record_observed(c).unwrap();
        let tally = ledger.tally().unwrap();
        assert_eq!((tally.expected_rows, tally.observed_rows), (3, 3));
        assert_eq!((tally.expected_distinct, tally.observed_distinct), (2, 2));
        assert_eq!(
            (tally.expected_duplicates, tally.observed_duplicates),
            (1, 1)
        );
        assert_eq!((tally.missing, tally.unexpected), (1, 1));
        assert!(tally.exact);
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn ragged_and_malformed_shard_rows_fail() {
        let ragged_dir = test_dir("ragged");
        let ragged = ragged_dir.join("rows.bin");
        fs::create_dir_all(&ragged_dir).unwrap();
        fs::write(&ragged, [0_u8; ID_WIDTH + 1]).unwrap();
        assert!(matches!(
            read_fixed_rows::<ID_WIDTH>(&ragged),
            Err(ShardError::Ragged { .. })
        ));

        let malformed_dir = test_dir("malformed");
        let malformed = malformed_dir.join("rows.bin");
        fs::create_dir_all(&malformed_dir).unwrap();
        let mut row = [0_u8; CORRELATION_WIDTH];
        row[0] = 1;
        row[ID_WIDTH] = u8::MAX;
        fs::write(&malformed, row).unwrap();
        assert!(matches!(
            read_observed_rows(&malformed),
            Err(ShardError::InvalidRow {
                field: "status",
                ..
            })
        ));
        fs::remove_dir_all(ragged_dir).ok();
        fs::remove_dir_all(malformed_dir).ok();
    }

    #[test]
    fn an_oversized_shard_is_refused_before_it_is_read() {
        let dir = test_dir("oversized");
        let path = dir.join("rows.bin");
        fs::create_dir_all(&dir).unwrap();
        File::create(&path)
            .unwrap()
            .set_len(((MAX_SHARD_ROWS + 1) * ID_WIDTH) as u64)
            .unwrap();
        assert!(matches!(
            read_fixed_rows::<ID_WIDTH>(&path),
            Err(ShardError::TooLarge { .. })
        ));
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn missing_and_unrelated_surplus_do_not_cancel() {
        let dir = test_dir("missing-surplus");
        let mut ledger = CorrelationLedger::create(&dir);
        let (a, b, c) = (trace(1), trace(2), trace(3));
        ledger.record_expected(a, Ending::Complete).unwrap();
        ledger.record_expected(b, Ending::Complete).unwrap();
        ledger.record_observed(&trace_text(b), "ok").unwrap();
        ledger.record_observed(&trace_text(c), "ok").unwrap();
        let tally = ledger.tally().unwrap();
        assert_eq!((tally.expected, tally.observed), (2, 2));
        assert_eq!((tally.missing, tally.unexpected), (1, 1));
        assert_eq!(tally.status_mismatches, 0);
        assert!(tally.exact);
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn duplicate_observed_correlation_is_surplus() {
        let dir = test_dir("observed-duplicate");
        let mut ledger = CorrelationLedger::create(&dir);
        let a = trace(4);
        ledger.record_expected(a, Ending::Complete).unwrap();
        ledger.record_observed(&trace_text(a), "ok").unwrap();
        ledger.record_observed(&trace_text(a), "ok").unwrap();
        let tally = ledger.tally().unwrap();
        assert_eq!((tally.missing, tally.unexpected), (0, 1));
        assert_eq!(tally.status_mismatches, 0);
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn status_is_checked_against_the_specific_ending() {
        let dir = test_dir("status-mismatch");
        let mut ledger = CorrelationLedger::create(&dir);
        let a = trace(5);
        ledger.record_expected(a, Ending::Complete).unwrap();
        ledger
            .record_observed(&trace_text(a), "upstream_error")
            .unwrap();
        let tally = ledger.tally().unwrap();
        assert_eq!(
            (tally.missing, tally.unexpected, tally.status_mismatches),
            (0, 0, 1)
        );
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn cancelled_and_dropped_accept_both_valid_statuses() {
        let dir = test_dir("alternate-statuses");
        let mut ledger = CorrelationLedger::create(&dir);
        let identities = [trace(6), trace(7), trace(8), trace(9)];
        ledger
            .record_expected(identities[0], Ending::Cancelled)
            .unwrap();
        ledger
            .record_expected(identities[1], Ending::Cancelled)
            .unwrap();
        ledger
            .record_expected(identities[2], Ending::Dropped)
            .unwrap();
        ledger
            .record_expected(identities[3], Ending::Dropped)
            .unwrap();
        ledger
            .record_observed(&trace_text(identities[0]), "client_cancelled")
            .unwrap();
        ledger
            .record_observed(&trace_text(identities[1]), "partial")
            .unwrap();
        ledger
            .record_observed(&trace_text(identities[2]), "upstream_error")
            .unwrap();
        ledger
            .record_observed(&trace_text(identities[3]), "partial")
            .unwrap();
        let tally = ledger.tally().unwrap();
        assert_eq!(
            (tally.missing, tally.unexpected, tally.status_mismatches),
            (0, 0, 0)
        );
        fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn tally_holds_only_one_expected_observed_shard_pair() {
        let dir = test_dir("bounded-pair");
        let mut ledger = CorrelationLedger::create(&dir);
        let rows = 4_096_u64;
        for index in 0..rows {
            let mut identity = [0_u8; ID_WIDTH];
            identity[..8].copy_from_slice(&(index + 1).to_be_bytes());
            identity[ID_WIDTH - 1] = index as u8;
            ledger.record_expected(identity, Ending::Complete).unwrap();
            ledger.record_observed(&trace_text(identity), "ok").unwrap();
        }
        let tally = ledger.tally().unwrap();
        assert_eq!((tally.expected, tally.observed), (rows, rows));
        assert_eq!((tally.missing, tally.unexpected), (0, 0));
        assert!(tally.peak_shard_rows < tally.expected + tally.observed);
        assert!(tally.peak_shard_rows <= (rows * 2 / SHARDS as u64) + 2);
        fs::remove_dir_all(dir).ok();
    }
}
