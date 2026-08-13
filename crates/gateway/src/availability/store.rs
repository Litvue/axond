//! Durable discovery evidence: what a replica remembers having looked at.
//!
//! Availability is derived from a revision, but the *evidence* half of it is
//! learned rather than published — a look at a provider, taken with one tenant's
//! credentials, off the request path. A replica that kept that only in memory
//! would restart into "I have never looked" and report `unknown` for every
//! target until discovery came round again, which is honest but needlessly
//! blind: the last complete listing is still the best thing anyone knows.
//!
//! Three rules the shape here exists to keep:
//!
//! - **Evidence is not desired state.** Nothing in this module is published, and
//!   losing the whole table costs freshness and nothing else. Restoring is
//!   folded through the same ordering and retention path a live observation
//!   takes ([`AvailabilityIndexBuilder::record`](super::AvailabilityIndexBuilder::record)), so stored evidence cannot
//!   resurrect a target a later definitive look dropped and cannot rewind a
//!   newer look this process already holds.
//! - **Two slots, and which is which is durable.** A record holds the look it is
//!   deciding on and the newest look that found the target; collapsing them on
//!   the way to storage would turn a discovery outage into either a refusal or a
//!   forever-fresh `available`.
//! - **No operator detail is durable.** A failed probe's
//!   [`detail`](DiscoveryObservation::detail) can carry an upstream error body —
//!   a URL bearing a key, an account name — so it is dropped at the storage
//!   boundary rather than filtered on the way out. What survives a restart is
//!   the bounded vocabulary a verdict may state.

use std::collections::BTreeMap;
use std::time::SystemTime;

use async_trait::async_trait;

use super::discovery::{
    DiscoveryCompleteness, DiscoveryObservation, DiscoveryResult, DiscoverySource,
};
use super::index::{AvailabilityIndex, AvailabilityRecord};
use super::refs::{AvailabilityKey, ScopeRef};
use crate::backends::control_plane::ControlPlaneError;

/// Which of a record's two looks a stored row is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ObservationSlot {
    /// The look the record is currently deciding on.
    Current,
    /// The newest look that found the target, kept for the outage that follows.
    LastKnownGood,
}

impl ObservationSlot {
    pub const ALL: &'static [Self] = &[Self::Current, Self::LastKnownGood];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Current => "current",
            Self::LastKnownGood => "last_known_good",
        }
    }

    /// The slot a stored identifier names, or `None` for text no release wrote.
    pub fn parse(input: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|slot| slot.as_str() == input)
    }
}

/// One durable look: which slot of which record, and the conclusion the record
/// had reached when it was written.
///
/// The watermark travels with the row rather than being recomputed on load,
/// because it is the part that cannot be recovered from the looks themselves: a
/// complete listing that dropped a target may be long gone from both slots, and
/// without its instant a stored positive would come back and resurrect the
/// target the listing removed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredObservation {
    pub key: AvailabilityKey,
    pub slot: ObservationSlot,
    pub observation: DiscoveryObservation,
    pub definitive_at: Option<SystemTime>,
}

impl StoredObservation {
    /// The rows that describe an index's evidence, in key order.
    ///
    /// Records carrying no evidence produce no rows: a dimension a revision
    /// states is re-derived from the revision at every projection, and storing it
    /// here would be a second, staler copy of desired state.
    pub fn of_index(index: &AvailabilityIndex) -> Vec<Self> {
        let mut rows = Vec::new();
        for (key, record) in index.records() {
            for (slot, held) in [
                (ObservationSlot::Current, &record.discovery),
                (ObservationSlot::LastKnownGood, &record.last_known_good),
            ] {
                if let Some(observation) = held {
                    rows.push(Self {
                        key: key.clone(),
                        slot,
                        observation: observation.clone().without_detail(),
                        definitive_at: record.definitive_at,
                    });
                }
            }
        }
        rows
    }
}

/// One write of a replica's evidence: the looks it holds, and the keys it holds
/// none for.
///
/// Both halves, because a row set alone cannot say that a key *stopped* having
/// evidence. A record whose looks were all discredited emits no row, and a write
/// that only replaced the keys it mentions would leave the discredited rows in
/// place for the next restart to believe — the evidence a complete listing
/// removed would come back every time the process did.
///
/// [`of_index`](Self::of_index) derives both from one index, so the answer to
/// "which keys were cleared" is a fact of the index rather than bookkeeping a
/// caller has to keep: a key the replica knows about and holds no look for must
/// not have a stored look.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EvidenceWrite {
    rows: Vec<StoredObservation>,
    cleared: Vec<AvailabilityKey>,
}

impl EvidenceWrite {
    /// Everything an index says about durable evidence: its looks, and the keys
    /// it describes without any.
    pub fn of_index(index: &AvailabilityIndex) -> Self {
        Self {
            rows: StoredObservation::of_index(index),
            cleared: index
                .records()
                // Holding *a look* rather than holding evidence: a record whose
                // looks a later definitive conclusion discredited keeps its
                // watermark and emits no row, so asking the broader question
                // would leave exactly the discredited rows this half exists to
                // remove.
                .filter(|(_, record)| {
                    record.discovery.is_none() && record.last_known_good.is_none()
                })
                .map(|(key, _)| key.clone())
                .collect(),
        }
    }

    /// A write of rows alone, for a caller that is adding evidence and asserting
    /// nothing about keys it did not mention.
    pub fn of_rows(rows: Vec<StoredObservation>) -> Self {
        Self {
            rows,
            cleared: Vec::new(),
        }
    }

    /// Also clear these keys, whatever the store holds for them.
    #[must_use]
    pub fn clearing(mut self, keys: impl IntoIterator<Item = AvailabilityKey>) -> Self {
        self.cleared.extend(keys);
        self
    }

    pub fn rows(&self) -> &[StoredObservation] {
        &self.rows
    }

    /// The keys whose stored evidence must not survive this write.
    pub fn cleared(&self) -> &[AvailabilityKey] {
        &self.cleared
    }

    /// Whether this write would change nothing, which a store may answer without
    /// opening a transaction.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty() && self.cleared.is_empty()
    }
}

/// Where discovery evidence is kept between restarts.
///
/// Read once per boot and written by whatever takes the looks — never on the
/// request path, and never by publication. A store that is unreachable costs the
/// replica its last-known-good state and nothing more: the projection then
/// reports `unknown`, which is what a replica that has not looked honestly
/// knows.
#[async_trait]
pub trait ObservationStore: Send + Sync {
    /// Every stored look, or only one scope's.
    ///
    /// `None` is the whole deployment, which is what a replica restoring its own
    /// state asks for; a scope is what an operator-facing read asks for. It is
    /// never a wildcard reachable from a tenant-scoped caller — the caller
    /// supplies the scope, and the row filter is by tenant.
    async fn load(
        &self,
        scope: Option<ScopeRef>,
    ) -> Result<Vec<StoredObservation>, ControlPlaneError>;

    /// Replace the stored evidence for every key the write names — the keys its
    /// rows mention, and the keys it clears.
    ///
    /// Per key rather than per row, so a record whose retained look was
    /// discredited stops having one: an upsert alone would leave the discredited
    /// row behind for the next restart to believe. A key whose looks were *all*
    /// discredited emits no row at all, which is why the write carries
    /// [`EvidenceWrite::cleared`] beside them — otherwise the one case where
    /// stored evidence must disappear is the one case a row set cannot express.
    ///
    /// Atomic across both halves: a write that deleted the cleared keys and then
    /// failed to insert the rows would leave a replica remembering less than it
    /// knows, and one that inserted first would leave it remembering something a
    /// listing removed.
    async fn save(&self, write: &EvidenceWrite) -> Result<(), ControlPlaneError>;
}

/// Rows reassembled into the records they were written from.
///
/// Per key rather than per row, because a record is what the ordering rules are
/// written against: restoring the two slots as two separate declarations would
/// have the record's own retained positive judged against its own current look
/// and refused as an out-of-order arrival.
///
/// The dimensions a restored record carries are the fail-closed defaults, and
/// the next projection replaces them with what the revision actually says.
/// Storing the dimensions too would mean a replica could serve availability
/// derived from a revision it is not running.
pub(super) fn restored_records(
    rows: impl IntoIterator<Item = StoredObservation>,
) -> BTreeMap<AvailabilityKey, AvailabilityRecord> {
    let mut records: BTreeMap<AvailabilityKey, AvailabilityRecord> = BTreeMap::new();
    for row in rows {
        let record = records.entry(row.key).or_default();
        match row.slot {
            ObservationSlot::Current => record.discovery = Some(row.observation),
            ObservationSlot::LastKnownGood => record.last_known_good = Some(row.observation),
        }
        record.definitive_at = match (record.definitive_at, row.definitive_at) {
            (Some(held), Some(stored)) => Some(held.max(stored)),
            (held, stored) => held.or(stored),
        };
    }
    records
}

/// The bounded vocabulary a row is written and read as.
///
/// Text a release never wrote is [`ControlPlaneError::CorruptStorage`], not a
/// silently dropped row: a newer replica's vocabulary read by an older build is
/// intact storage this build cannot interpret, and quietly treating it as "no
/// evidence" would report a target as unknown rather than saying the build is
/// too old.
pub(crate) fn parse_result(text: &str) -> Option<DiscoveryResult> {
    DiscoveryResult::ALL
        .iter()
        .copied()
        .find(|value| value.as_str() == text)
}

pub(crate) fn parse_completeness(text: &str) -> Option<DiscoveryCompleteness> {
    DiscoveryCompleteness::ALL
        .iter()
        .copied()
        .find(|value| value.as_str() == text)
}

pub(crate) fn parse_source(text: &str) -> Option<DiscoverySource> {
    DiscoverySource::ALL
        .iter()
        .copied()
        .find(|value| value.as_str() == text)
}
