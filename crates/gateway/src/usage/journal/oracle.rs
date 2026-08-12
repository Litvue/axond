//! The in-memory contract oracle: what every [`UsageJournal`] must do.
//!
//! This exists to *define* behaviour, not to be deployed — an in-memory journal
//! would promise durability it cannot provide, which is the exact failure this
//! contract exists to prevent — so it is test-only, which also keeps the Tier 0
//! hermetic gate hermetic (ADR 0018).
//!
//! What it is precise about is the part the Postgres outbox worker has to
//! reproduce in SQL:
//!
//! - **Appends are keyed, not counted.** The idempotency key is a unique index;
//!   re-appending identical content recognises the existing row
//!   ([`Appended::AlreadyPresent`]) and re-appending different content under the
//!   same key is refused ([`JournalError::Conflict`]) rather than updating it.
//! - **Delivery state is per consumer, and durable.** Acknowledgements,
//!   quarantines, and attempt counts survive a restart; that is what makes a
//!   crash between a destination write and an acknowledgement recoverable by
//!   repeating the acknowledgement.
//! - **A claim is a lease, not a lock.** Nothing has to be released. A claimant
//!   that disappears stops renewing, the lease expires, and the event becomes
//!   claimable again — so a restart replays without an unlock step to lose.
//! - **Ordering is per key, and enforced by the claim.** At most one event per
//!   [`OrderingKey`] is in flight, so a second concurrent consumer of the same
//!   journal cannot reorder one caller's events.
//! - **Capacity is a decision, not an accident.** A full journal either refuses
//!   the append or drops its oldest unacknowledged event, and the dropped count
//!   is reported rather than inferred.
//! - **Retention is what bounds a drained journal.** An event every consumer
//!   acknowledged is pruned once [`Capacity::retain_acknowledged`] has passed
//!   since it was observed, so storage does not grow without limit just because
//!   delivery kept up. A quarantined event is not pruned.
//!
//! The mutex is this fake's transaction; the Postgres implementation's is a
//! transaction.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;

use super::{
    Appended, Capacity, CapacityPolicy, Claim, ConsumerId, Delivery, DeliveryId, DeliveryMode,
    IdempotencyKey, JournalError, JournalStats, OrderingKey, PoisonReason, UsageEvent,
    UsageJournal,
};

/// One journaled event at its position.
#[derive(Debug, Clone)]
struct Entry {
    position: u64,
    event: UsageEvent,
}

/// One consumer's delivery state. Durable: it outlives the process that wrote
/// it, so a restart resumes rather than redelivering everything.
#[derive(Debug, Default)]
struct ConsumerState {
    acked: BTreeSet<u64>,
    quarantined: BTreeMap<u64, PoisonReason>,
    /// Delivery attempts handed out per position, which is what makes a poison
    /// event detectable rather than eternal.
    attempts: BTreeMap<u64, u32>,
    /// Unexpired leases, by position. Durable like the rest — a durable store
    /// keeps its lease column across a restart and waits for expiry.
    leases: BTreeMap<u64, SystemTime>,
}

/// The journal's storage. Shared behind an `Arc` so a "restart" is a new journal
/// over the same bytes.
#[derive(Debug, Default)]
struct Storage {
    entries: Vec<Entry>,
    /// The unique index on the idempotency key.
    positions: BTreeMap<IdempotencyKey, u64>,
    consumers: BTreeMap<ConsumerId, ConsumerState>,
    next_position: u64,
    dropped: u64,
}

impl Storage {
    fn entry(&self, position: u64) -> Option<&Entry> {
        self.entries.iter().find(|entry| entry.position == position)
    }

    /// Positions no consumer has finished with, which is what capacity bounds.
    /// With no consumer registered every entry counts: a journal nobody reads
    /// still fills up.
    fn unacknowledged(&self) -> Vec<u64> {
        self.entries
            .iter()
            .map(|entry| entry.position)
            .filter(|position| {
                self.consumers.is_empty()
                    || self.consumers.values().any(|state| {
                        !state.acked.contains(position) && !state.quarantined.contains_key(position)
                    })
            })
            .collect()
    }

    /// Drop events every registered consumer has acknowledged and whose retention
    /// window has passed, measured from the event's own observation time — the
    /// `recorded_at` a store already has, so this is one `DELETE ... WHERE` and
    /// not a second timestamp column.
    ///
    /// Quarantined events stay: they are waiting for an operator. So does
    /// everything, if no consumer is registered — a journal nobody reads has
    /// acknowledged nothing.
    fn prune_acknowledged(&mut self, retain: Duration, now: SystemTime) {
        if self.consumers.is_empty() {
            return;
        }
        let prunable: Vec<u64> = self
            .entries
            .iter()
            .filter(|entry| entry.event.observed_at() + retain <= now)
            .filter(|entry| {
                self.consumers.values().all(|state| {
                    state.acked.contains(&entry.position)
                        && !state.quarantined.contains_key(&entry.position)
                })
            })
            .map(|entry| entry.position)
            .collect();
        for position in prunable {
            let Some(index) = self
                .entries
                .iter()
                .position(|entry| entry.position == position)
            else {
                continue;
            };
            let pruned = self.entries.remove(index);
            // The unique index goes with the row, which is why the window has to
            // outlive any retry: past it, the same event appends as a new one.
            self.positions.remove(pruned.event.idempotency_key());
            for state in self.consumers.values_mut() {
                state.acked.remove(&position);
                state.attempts.remove(&position);
                state.leases.remove(&position);
            }
        }
    }
}

/// A `UsageJournal` whose transaction is a mutex.
pub(crate) struct InMemoryUsageJournal {
    storage: Arc<Mutex<Storage>>,
    capacity: Capacity,
}

impl InMemoryUsageJournal {
    pub(crate) fn new() -> Self {
        Self::with_capacity(Capacity::BILLING_GRADE)
    }

    pub(crate) fn with_capacity(capacity: Capacity) -> Self {
        Self {
            storage: Arc::new(Mutex::new(Storage::default())),
            capacity,
        }
    }

    /// A second journal over the same storage: what a process restart looks like
    /// to a durable store. Delivery state carries over; nothing in flight is
    /// silently re-handed out, because the leases are still there until they
    /// expire.
    pub(crate) fn restart(&self) -> Self {
        Self {
            storage: Arc::clone(&self.storage),
            capacity: self.capacity,
        }
    }

    fn locked(&self) -> MutexGuard<'_, Storage> {
        self.storage.lock().expect("journal mutex is not poisoned")
    }

    /// Events held, whatever their delivery state — the storage footprint, which
    /// is the thing retention has to bound.
    pub(crate) fn stored_events(&self) -> usize {
        self.locked().entries.len()
    }
}

#[async_trait]
impl UsageJournal for InMemoryUsageJournal {
    fn name(&self) -> &'static str {
        "memory"
    }

    fn capacity(&self) -> Capacity {
        self.capacity
    }

    /// The honest answer: nothing here survives the process, so this journal
    /// must not claim the billing-grade guarantee even though it implements every
    /// operation of it.
    fn mode(&self) -> DeliveryMode {
        DeliveryMode::TelemetryGrade
    }

    async fn append(&self, event: &UsageEvent) -> Result<Appended, JournalError> {
        let mut storage = self.locked();
        storage.prune_acknowledged(self.capacity.retain_acknowledged, SystemTime::now());
        if let Some(position) = storage.positions.get(event.idempotency_key()).copied() {
            let stored = storage.entry(position).expect("indexed position exists");
            if stored.event.is_same_fact_as(event) {
                return Ok(Appended::AlreadyPresent { position });
            }
            return Err(JournalError::Conflict {
                key: event.idempotency_key().clone(),
            });
        }
        let pending = storage.unacknowledged();
        if pending.len() as u64 >= self.capacity.max_events {
            match self.capacity.policy {
                CapacityPolicy::Refuse => {
                    return Err(JournalError::AtCapacity {
                        pending: pending.len() as u64,
                        capacity: self.capacity,
                    });
                }
                CapacityPolicy::DropOldest => {
                    let oldest = *pending.first().expect("capacity is at least one event");
                    let dropped = storage
                        .entries
                        .iter()
                        .position(|entry| entry.position == oldest)
                        .map(|index| storage.entries.remove(index))
                        .expect("pending position is stored");
                    storage.positions.remove(dropped.event.idempotency_key());
                    for state in storage.consumers.values_mut() {
                        state.leases.remove(&oldest);
                        state.attempts.remove(&oldest);
                    }
                    storage.dropped += 1;
                }
            }
        }
        let position = storage.next_position;
        storage.next_position += 1;
        storage
            .positions
            .insert(event.idempotency_key().clone(), position);
        storage.entries.push(Entry {
            position,
            event: event.clone(),
        });
        Ok(Appended::Accepted { position })
    }

    async fn claim(
        &self,
        consumer: &ConsumerId,
        claim: Claim,
    ) -> Result<Vec<Delivery>, JournalError> {
        let mut storage = self.locked();
        let capacity = self.capacity;
        let mut poisoned: Vec<(u64, PoisonReason)> = Vec::new();
        let mut claimed: Vec<Delivery> = Vec::new();
        // Ordering keys that already have an event in flight or claimed in this
        // pass. At most one per key, so a consumer sees one caller's events in
        // append order however many claimants there are.
        let mut busy: HashSet<OrderingKey> = HashSet::new();
        let entries: Vec<Entry> = storage.entries.clone();
        let state = storage.consumers.entry(consumer.clone()).or_default();
        for entry in &entries {
            if state.acked.contains(&entry.position)
                || state.quarantined.contains_key(&entry.position)
            {
                continue;
            }
            let key = entry.event.ordering_key().clone();
            if let Some(expiry) = state.leases.get(&entry.position).copied() {
                if expiry > claim.now {
                    busy.insert(key);
                    continue;
                }
                state.leases.remove(&entry.position);
            }
            if busy.contains(&key) {
                continue;
            }
            let attempt = state.attempts.get(&entry.position).copied().unwrap_or(0) + 1;
            if attempt > capacity.max_delivery_attempts {
                poisoned.push((entry.position, PoisonReason::AttemptsExhausted));
                continue;
            }
            state.attempts.insert(entry.position, attempt);
            let expiry = claim.now + claim.lease;
            state.leases.insert(entry.position, expiry);
            busy.insert(key);
            claimed.push(Delivery {
                id: DeliveryId {
                    consumer: consumer.clone(),
                    event: entry.event.id(),
                    attempt,
                },
                event: entry.event.clone(),
                lease_expires_at: expiry,
            });
            if claimed.len() >= claim.max_events {
                break;
            }
        }
        // An event whose attempts ran out leaves the delivery path here rather
        // than being handed out again, so it stops blocking its ordering key.
        for (position, reason) in poisoned {
            state.quarantined.insert(position, reason);
            state.leases.remove(&position);
        }
        Ok(claimed)
    }

    async fn ack(&self, delivery: &DeliveryId) -> Result<(), JournalError> {
        let mut storage = self.locked();
        let Some(position) = storage
            .positions
            .get(&IdempotencyKey::from(delivery.event))
            .copied()
        else {
            return Err(JournalError::NotOutstanding {
                delivery: delivery.clone(),
            });
        };
        let state = storage
            .consumers
            .entry(delivery.consumer.clone())
            .or_default();
        // Idempotent, and deliberately not conditional on the attempt number: a
        // consumer that crashed after writing its destination row repeats the
        // acknowledgement, and a store that insisted on the attempt it last
        // handed out would refuse exactly the retry that makes recovery work.
        if state.acked.contains(&position) {
            return Ok(());
        }
        // Quarantine is terminal until an operator intervenes. Letting an
        // acknowledgement through here would erase the poison count an operator is
        // watching, and make the event prunable — losing the one copy of a record
        // somebody was asked to look at.
        if state.quarantined.contains_key(&position) {
            return Err(JournalError::Quarantined {
                delivery: delivery.clone(),
            });
        }
        if !state.attempts.contains_key(&position) {
            return Err(JournalError::NotOutstanding {
                delivery: delivery.clone(),
            });
        }
        state.acked.insert(position);
        state.leases.remove(&position);
        Ok(())
    }

    async fn quarantine(
        &self,
        delivery: &DeliveryId,
        reason: PoisonReason,
    ) -> Result<(), JournalError> {
        let mut storage = self.locked();
        let Some(position) = storage
            .positions
            .get(&IdempotencyKey::from(delivery.event))
            .copied()
        else {
            return Err(JournalError::NotOutstanding {
                delivery: delivery.clone(),
            });
        };
        let state = storage
            .consumers
            .entry(delivery.consumer.clone())
            .or_default();
        // Idempotent, and gated on the same "was it ever handed out?" test as
        // `ack`: quarantining is a verdict on a delivery this consumer attempted,
        // not a way to remove an event it never saw.
        if state.quarantined.contains_key(&position) {
            return Ok(());
        }
        if !state.attempts.contains_key(&position) {
            return Err(JournalError::NotOutstanding {
                delivery: delivery.clone(),
            });
        }
        state.quarantined.insert(position, reason);
        state.leases.remove(&position);
        Ok(())
    }

    async fn stats(&self, consumer: &ConsumerId) -> Result<JournalStats, JournalError> {
        let now = SystemTime::now();
        let storage = self.locked();
        let state = storage.consumers.get(consumer);
        let mut stats = JournalStats {
            pending: 0,
            in_flight: 0,
            quarantined: 0,
            oldest_pending_age: None,
            dropped: storage.dropped,
            capacity: self.capacity,
        };
        let mut oldest: Option<SystemTime> = None;
        for entry in &storage.entries {
            let (acked, quarantined, leased) = state.map_or((false, false, None), |state| {
                (
                    state.acked.contains(&entry.position),
                    state.quarantined.contains_key(&entry.position),
                    state.leases.get(&entry.position).copied(),
                )
            });
            // Quarantine first: it is the state an operator is looking for, and an
            // event cannot be both (`ack` refuses a quarantined delivery).
            if quarantined {
                stats.quarantined += 1;
                continue;
            }
            if acked {
                continue;
            }
            if leased.is_some_and(|expiry| expiry > now) {
                stats.in_flight += 1;
                continue;
            }
            stats.pending += 1;
            let observed = entry.event.observed_at();
            if oldest.is_none_or(|current| observed < current) {
                oldest = Some(observed);
            }
        }
        stats.oldest_pending_age =
            oldest.map(|observed| now.duration_since(observed).unwrap_or(Duration::ZERO));
        Ok(stats)
    }
}
