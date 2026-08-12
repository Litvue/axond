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
//! - **A consumer is registered by claiming.** Nothing else creates its delivery
//!   state, so a stray acknowledgement from a consumer that never read the journal
//!   cannot add a row that retention then waits on forever.
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
//! - **Capacity bounds everything stored.** Undelivered events, quarantined ones,
//!   and delivered ones still inside their retention window all occupy it, so the
//!   limit is true of the journal's footprint. A full journal gives up delivered
//!   events first (a re-acknowledgement is cheaper than a refusal), and only then
//!   refuses the append or drops its oldest *non-quarantined* event — with the
//!   dropped count reported rather than inferred.
//! - **Retention is a maximum, not a promise.** An event every consumer
//!   acknowledged is pruned once [`Capacity::retain_acknowledged`] has passed since
//!   it was observed, or earlier if the journal needs the room, so storage does not
//!   grow without limit just because delivery kept up. A quarantined event is not
//!   pruned.
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

    /// Positions every registered consumer has acknowledged, in append order.
    /// Quarantine excludes an event, and so does having no consumer at all: a
    /// journal nobody reads has acknowledged nothing.
    ///
    /// These are the events retention is holding as a courtesy to a consumer that
    /// may re-acknowledge after a restart — the first space a full journal reclaims,
    /// because they have already been delivered.
    fn delivered(&self) -> Vec<u64> {
        if self.consumers.is_empty() {
            return Vec::new();
        }
        self.entries
            .iter()
            .map(|entry| entry.position)
            .filter(|position| !self.is_quarantined(*position))
            .filter(|position| {
                self.consumers
                    .values()
                    .all(|state| state.acked.contains(position))
            })
            .collect()
    }

    /// Remove one event and every trace of it, including its idempotency-key index
    /// entry — which is why a retention window has to outlive any retry: past it,
    /// the same event appends as a new one.
    fn forget(&mut self, position: u64) {
        let Some(index) = self
            .entries
            .iter()
            .position(|entry| entry.position == position)
        else {
            return;
        };
        let removed = self.entries.remove(index);
        self.positions.remove(removed.event.idempotency_key());
        for state in self.consumers.values_mut() {
            state.acked.remove(&position);
            state.attempts.remove(&position);
            state.leases.remove(&position);
        }
    }

    /// Make room by forgetting already-delivered events ahead of their retention
    /// window, oldest first, until the journal is back inside `max_events`.
    ///
    /// This is what keeps the capacity bound true of *everything* stored while a
    /// journal that is keeping up still accepts appends: the retention window is a
    /// courtesy to a re-acknowledging consumer, and a courtesy is the first thing to
    /// give up when the alternative is refusing an event that has not been delivered
    /// at all. Returns how many were forgotten — they are not losses, so the caller
    /// does not count them as dropped.
    fn reclaim_delivered(&mut self, max_events: u64) -> usize {
        let mut reclaimed = 0;
        for position in self.delivered() {
            if (self.entries.len() as u64) < max_events {
                break;
            }
            self.forget(position);
            reclaimed += 1;
        }
        reclaimed
    }

    /// Whether any consumer has this position set aside as poison. Such an event
    /// is evidence somebody was asked to look at, so it is exempt from both
    /// retention pruning and a capacity drop.
    fn is_quarantined(&self, position: u64) -> bool {
        self.consumers
            .values()
            .any(|state| state.quarantined.contains_key(&position))
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
        let expired: Vec<u64> = self
            .delivered()
            .into_iter()
            .filter(|position| {
                self.entry(*position)
                    .is_some_and(|entry| entry.event.observed_at() + retain <= now)
            })
            .collect();
        for position in expired {
            self.forget(position);
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
        // Capacity is measured on everything stored, so the limit is true of the
        // journal's footprint rather than of one class of event inside it. Delivered
        // events still inside their retention window are given up first: they cost
        // nothing but a re-acknowledgement, which is a better trade than refusing an
        // event nobody has delivered.
        if storage.entries.len() as u64 >= self.capacity.max_events {
            storage.reclaim_delivered(self.capacity.max_events);
        }
        if storage.entries.len() as u64 >= self.capacity.max_events {
            let retained = storage.entries.len() as u64;
            match self.capacity.policy {
                CapacityPolicy::Refuse => {
                    return Err(JournalError::AtCapacity {
                        pending: retained,
                        capacity: self.capacity,
                    });
                }
                CapacityPolicy::DropOldest => {
                    // A quarantined event is not a drop candidate: it is waiting
                    // for an operator, and deleting it to make room would destroy
                    // the record they were asked to look at. A journal whose whole
                    // backlog is poison therefore refuses — the honest answer, since
                    // the only room left to make is somebody else's evidence.
                    let Some(oldest) = storage
                        .entries
                        .iter()
                        .map(|entry| entry.position)
                        .find(|position| !storage.is_quarantined(*position))
                    else {
                        return Err(JournalError::AtCapacity {
                            pending: retained,
                            capacity: self.capacity,
                        });
                    };
                    storage.forget(oldest);
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
        // Read-only: a consumer is registered by claiming, not by talking about a
        // delivery. Creating its row here would let one spurious acknowledgement
        // register a phantom consumer, and since only an event *every* registered
        // consumer acked is prunable, that phantom would freeze retention for good.
        let Some(state) = storage.consumers.get_mut(&delivery.consumer) else {
            return Err(JournalError::NotOutstanding {
                delivery: delivery.clone(),
            });
        };
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
        // Read-only for the same reason as [`ack`]: a verdict from a consumer that
        // never claimed anything must not register it.
        let Some(state) = storage.consumers.get_mut(&delivery.consumer) else {
            return Err(JournalError::NotOutstanding {
                delivery: delivery.clone(),
            });
        };
        // Idempotent, and gated on the same "was it ever handed out?" test as
        // `ack`: quarantining is a verdict on a delivery this consumer attempted,
        // not a way to remove an event it never saw.
        if state.quarantined.contains_key(&position) {
            return Ok(());
        }
        // The two verdicts are exclusive in both directions. A late quarantine that
        // overrode an acknowledgement would put a successfully delivered event on
        // the poison count and, since quarantine is exempt from pruning, keep it
        // there forever.
        if state.acked.contains(&position) {
            return Err(JournalError::AlreadyAcknowledged {
                delivery: delivery.clone(),
            });
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
