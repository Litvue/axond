//! Bounded capacity for background accounting ("settlement").
//!
//! A request's spend is charged *after* its response (ADR 0064): the charge
//! and the usage append run in a detached task so a caller hanging up cannot
//! cancel them. Detached work is only safe when it is bounded, and a slow or
//! unavailable Store is exactly the condition under which it stops being so:
//! responses keep being served at upstream speed while their settlements pile
//! up behind the Store, each one a task, a record, and an unrecorded charge.
//!
//! This module bounds that work with capacity the runtime owns:
//!
//! * **Reserve at admission, without a write.** Every request takes one unit of
//!   settlement capacity when it is admitted — a semaphore permit in this
//!   process, not a hold on the ledger — and hands it to the settlement it
//!   later spawns. A request that cannot reserve is refused with
//!   `503 settlement_capacity_exhausted` before it reaches a provider, so
//!   saturation pushes back on *new* admissions and never drops the charge of a
//!   request that was already admitted. The reservation is what makes the
//!   settlement's admission unconditional.
//! * **Queue wait and execution deadline are separate.** A spawned settlement
//!   waits for one of a bounded number of execution slots for at most the
//!   configured queue wait, then runs under its own execution deadline. A
//!   settlement that misses the queue wait is counted and never started. A
//!   settlement that misses the execution deadline is counted the same way,
//!   but tracking and the execution slot stay held until the future — including
//!   non-cancellable `spawn_blocking` Store work — actually ends, so a late
//!   charge cannot outrun the bound or land without its usage append. Neither
//!   miss is retried: `charge_budget` is not idempotent.
//! * **Release exactly once, on every exit.** Tracking is a guard released on
//!   `Drop`, so normal completion, an execution timeout, a panic inside the
//!   settlement, and an aborted task all return capacity the same way.
//!
//! Shutdown waits for the spawned settlements within the settle share of the
//! flush budget and reports what was still queued, executing, or reserved when
//! that share ran out.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use futures::FutureExt;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio::time::Instant;

use crate::admission::{AdmissionRejection, RESOURCE_SETTLEMENT};
use crate::config::AdmissionConfig;
use crate::telemetry::metrics;

/// The stage labels on `axond.settlement.in_flight`. A closed vocabulary.
pub const STAGE_RESERVED: &str = "reserved";
pub const STAGE_QUEUED: &str = "queued";
pub const STAGE_EXECUTING: &str = "executing";

/// The reason labels on `axond.settlement.failures`. A closed vocabulary.
pub const FAILURE_QUEUE_TIMEOUT: &str = "queue_timeout";
pub const FAILURE_EXECUTION_TIMEOUT: &str = "execution_timeout";
pub const FAILURE_PANICKED: &str = "panicked";
pub const FAILURE_CANCELLED: &str = "cancelled";
pub const FAILURE_REFUSED: &str = "refused";

/// Every failure reason a settlement can record, so the metric catalogue can
/// assert its vocabulary is complete.
#[cfg(test)]
pub const FAILURE_REASONS: [&str; 5] = [
    FAILURE_QUEUE_TIMEOUT,
    FAILURE_EXECUTION_TIMEOUT,
    FAILURE_PANICKED,
    FAILURE_CANCELLED,
    FAILURE_REFUSED,
];

/// The resolved bounds. `None` is "off", matching the `0` the operator wrote.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SettlementLimits {
    /// Settlements this process will carry at once, in any stage: reserved by
    /// an admitted request, queued for an execution slot, or executing.
    pub max_pending: Option<usize>,
    /// Settlements executing against the Store concurrently.
    pub max_in_flight: Option<usize>,
    /// How long a spawned settlement waits for an execution slot.
    pub queue_wait: Option<Duration>,
    /// How long one settlement may execute once it has a slot.
    pub execution_timeout: Option<Duration>,
}

impl From<&AdmissionConfig> for SettlementLimits {
    fn from(config: &AdmissionConfig) -> Self {
        let bound = |value: usize| (value > 0).then_some(value);
        let millis = |value: u64| (value > 0).then(|| Duration::from_millis(value));
        Self {
            max_pending: bound(config.max_pending_settlements),
            max_in_flight: bound(config.max_in_flight_settlements),
            queue_wait: millis(config.settlement_queue_wait_ms),
            execution_timeout: millis(config.settlement_timeout_ms),
        }
    }
}

impl SettlementLimits {
    /// Nothing bounded: the posture of a test that only wants the tracking.
    #[cfg(test)]
    pub fn unbounded() -> Self {
        Self {
            max_pending: None,
            max_in_flight: None,
            queue_wait: None,
            execution_timeout: None,
        }
    }
}

/// The process's settlement capacity. Built once at boot and shared by every
/// request; the bounds are fixed for the process lifetime, so a reloaded
/// `[admission]` section is validated but applied on restart.
#[derive(Clone)]
pub struct Settlements {
    shared: Arc<Shared>,
}

struct Shared {
    limits: SettlementLimits,
    pending: Option<Arc<Semaphore>>,
    executing: Option<Arc<Semaphore>>,
    reserved: AtomicU64,
    queued: AtomicU64,
    running: AtomicU64,
    /// Spawned settlements that have not yet finished. Stays nonzero across
    /// the queued-to-executing cutover, where `queued` is decremented before
    /// `running` is incremented; [`Settlements::await_idle`] watches this
    /// rather than the sum of the stage counters.
    spawned: AtomicU64,
    /// When each spawned settlement was enqueued, keyed by its sequence number,
    /// so the age of the oldest one is a lookup rather than a scan.
    backlog: Mutex<BTreeMap<u64, Instant>>,
    sequence: AtomicU64,
    /// Bumped whenever a spawned settlement finishes. A `watch` rather than a
    /// `Notify` because a `Notify` waiter only enqueues on its first poll, so a
    /// wake-up racing the count check would be lost and the shutdown wait
    /// would sleep out its whole budget.
    finished: watch::Sender<u64>,
    /// Immediate `execution_timeout` signals, recorded when the deadline fires
    /// rather than when blocking work later ends.
    #[cfg(test)]
    deadline_misses: AtomicU64,
}

/// What is outstanding right now, for the shutdown report and for tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Backlog {
    /// Admitted requests holding capacity whose settlement has not been spawned.
    pub reserved: u64,
    /// Spawned settlements waiting for an execution slot.
    pub queued: u64,
    /// Settlements executing against the Store.
    pub executing: u64,
    /// Spawned settlements not yet finished. Equal to `queued + executing`
    /// except during the queued-to-executing cutover, which this count covers.
    pub spawned: u64,
    /// Age of the oldest spawned settlement, queued or executing.
    pub oldest_age: Option<Duration>,
}

impl Backlog {
    /// Spawned settlements not yet finished: the work shutdown waits for.
    pub fn unsettled(self) -> u64 {
        self.spawned
    }
}

impl Settlements {
    pub fn new(limits: SettlementLimits) -> Self {
        let settlements = Self {
            shared: Arc::new(Shared {
                pending: limits.max_pending.map(|n| Arc::new(Semaphore::new(n))),
                executing: limits.max_in_flight.map(|n| Arc::new(Semaphore::new(n))),
                reserved: AtomicU64::new(0),
                queued: AtomicU64::new(0),
                running: AtomicU64::new(0),
                spawned: AtomicU64::new(0),
                backlog: Mutex::new(BTreeMap::new()),
                sequence: AtomicU64::new(0),
                finished: watch::Sender::new(0),
                limits,
                #[cfg(test)]
                deadline_misses: AtomicU64::new(0),
            }),
        };
        register_age_source(&settlements.shared);
        settlements
    }

    pub fn from_config(config: &AdmissionConfig) -> Self {
        Self::new(SettlementLimits::from(config))
    }

    #[cfg(test)]
    fn deadline_misses(&self) -> u64 {
        self.shared.deadline_misses.load(Ordering::Acquire)
    }

    /// Reserve settlement capacity for one request being admitted, or shed the
    /// request. No ledger write happens here: the reservation is a slot in this
    /// process, which is what lets the settlement it becomes run unconditionally.
    pub fn reserve(&self) -> Result<SettlementReservation, AdmissionRejection> {
        self.try_reserve().ok_or_else(|| {
            metrics::record_admission_rejection(
                RESOURCE_SETTLEMENT,
                AdmissionRejection::Settlement.code(),
            );
            AdmissionRejection::Settlement
        })
    }

    fn try_reserve(&self) -> Option<SettlementReservation> {
        let permit = match &self.shared.pending {
            Some(pending) => Some(Arc::clone(pending).try_acquire_owned().ok()?),
            None => None,
        };
        self.shared.reserved.fetch_add(1, Ordering::AcqRel);
        metrics::record_settlement_stage_entered(STAGE_RESERVED);
        Some(SettlementReservation {
            shared: Arc::clone(&self.shared),
            permit,
            consumed: false,
        })
    }

    /// Run one settlement under capacity reserved at admission. Never refuses:
    /// the reservation is the admission.
    ///
    /// Outside a runtime (process teardown) there is nothing left to settle
    /// onto, and the reservation is released with the future.
    pub fn spawn<F>(&self, reservation: SettlementReservation, future: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let guard = Guard::enqueue(reservation);
        let shared = Arc::clone(&self.shared);
        handle.spawn(async move {
            let mut guard = guard;
            let _slot = match shared.acquire_execution_slot().await {
                Ok(slot) => slot,
                Err(()) => {
                    guard.outcome = Some(Outcome::QueueTimeout);
                    return;
                }
            };
            guard.start_executing();
            // Caught here rather than left to the task harness so the reason is
            // known when the guard reports: tokio drops a panicked future after
            // the unwind, where `thread::panicking()` is already false.
            //
            // The execution deadline is a missed-deadline signal, not a
            // cancellation. SQLite Store calls run in `spawn_blocking`; dropping
            // this future would release capacity while that closure kept
            // running, so a late `charge_budget` could land without its usage
            // append. After the deadline we keep the join (and the slot) until
            // the future actually ends.
            let mut run = std::pin::pin!(std::panic::AssertUnwindSafe(future).catch_unwind());
            let outcome = match shared.limits.execution_timeout {
                Some(deadline) => match tokio::time::timeout(deadline, run.as_mut()).await {
                    Ok(Ok(())) => Outcome::Completed,
                    Ok(Err(_panic)) => Outcome::Panicked,
                    Err(_) => {
                        // Report the miss now: a stalled Store must increment
                        // the timeout counter even if the join never returns,
                        // and a later panic must not replace that signal.
                        guard.note_execution_timeout();
                        let _ = run.await;
                        Outcome::ExecutionTimeout
                    }
                },
                None => match run.await {
                    Ok(()) => Outcome::Completed,
                    Err(_panic) => Outcome::Panicked,
                },
            };
            if guard.outcome.is_none() {
                guard.outcome = Some(outcome);
            }
        });
    }

    /// Run one settlement that no admitted request reserved capacity for,
    /// taking the capacity now. Refused — the future handed back — when the
    /// process is at `max_pending_settlements`, so the caller decides what the
    /// work was worth: a zero-charge release can be dropped, a record can be
    /// awaited inline.
    pub fn try_spawn<F>(&self, future: F) -> Result<(), F>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        match self.try_reserve() {
            Some(reservation) => {
                self.spawn(reservation, future);
                Ok(())
            }
            None => Err(future),
        }
    }

    /// Spawn `future` under capacity this request already reserved, so
    /// cancellation accounting cannot be refused because that same slot still
    /// occupies the ceiling. Without a reservation this is [`Self::try_spawn`],
    /// and a refusal is loud: an admitted charge is never dropped in silence.
    pub fn spawn_reserved<F>(
        &self,
        reservation: Option<SettlementReservation>,
        future: F,
        what: &'static str,
    ) where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        match reservation {
            Some(reserved) => self.spawn(reserved, future),
            None => {
                if self.try_spawn(future).is_err() {
                    self.refuse(what);
                }
            }
        }
    }

    /// A settlement refused by [`Self::try_spawn`] whose work was a charge or a
    /// record: counted and logged, because it is spend this process will not
    /// account for.
    pub fn refuse(&self, what: &'static str) {
        metrics::record_settlement_failure(FAILURE_REFUSED);
        tracing::error!(
            what,
            max_pending = ?self.shared.limits.max_pending,
            "settlement refused: the process is at its settlement capacity and the work \
             carried no admission reservation"
        );
    }

    /// What is outstanding right now.
    pub fn backlog(&self) -> Backlog {
        let shared = &self.shared;
        Backlog {
            reserved: shared.reserved.load(Ordering::Acquire),
            queued: shared.queued.load(Ordering::Acquire),
            executing: shared.running.load(Ordering::Acquire),
            spawned: shared.spawned.load(Ordering::Acquire),
            oldest_age: shared.oldest_age(),
        }
    }

    /// Wait up to `bound` for every spawned settlement to finish, and report
    /// what is still outstanding. Called once on the shutdown path, before the
    /// sinks are flushed; the bound is the caller's, never extended here.
    pub async fn await_idle(&self, bound: Duration) -> Backlog {
        let shared = &self.shared;
        let _ = tokio::time::timeout(bound, async {
            // Subscribed before the counts are read, so a settlement finishing
            // in between bumps a version this receiver has not seen and
            // `changed()` returns at once. The waiter watches `spawned`, not
            // `queued + running`: those two stage counters are both briefly
            // zero while `start_executing` cuts over.
            let mut finished = shared.finished.subscribe();
            while shared.spawned.load(Ordering::Acquire) != 0 {
                if finished.changed().await.is_err() {
                    return;
                }
            }
        })
        .await;
        self.backlog()
    }
}

/// The process's live settlement capacity, so
/// `axond.settlement.oldest_pending_age` can be observed at collection time
/// rather than frozen at the last enqueue or completion.
static AGE_SOURCE: OnceLock<Mutex<Weak<Shared>>> = OnceLock::new();

fn age_source() -> &'static Mutex<Weak<Shared>> {
    AGE_SOURCE.get_or_init(|| Mutex::new(Weak::new()))
}

fn register_age_source(shared: &Arc<Shared>) {
    *age_source().lock().expect("settlement age source") = Arc::downgrade(shared);
}

/// Age of the oldest spawned settlement, in milliseconds, or `0` when the
/// backlog is empty. The oldest-pending-age gauge observes this at collection
/// so a stalled settlement keeps climbing.
pub(crate) fn oldest_pending_age_ms() -> u64 {
    age_source()
        .lock()
        .expect("settlement age source")
        .upgrade()
        .and_then(|shared| shared.oldest_age())
        .map(|age| age.as_millis() as u64)
        .unwrap_or(0)
}

impl Shared {
    /// One execution slot, within the queue wait. `Err` is the queue wait
    /// expiring — or the semaphore closing, which never happens while the
    /// process serves and is treated the same way rather than admitting past
    /// the ceiling.
    async fn acquire_execution_slot(&self) -> Result<Option<OwnedSemaphorePermit>, ()> {
        let Some(executing) = &self.executing else {
            return Ok(None);
        };
        let acquire = Arc::clone(executing).acquire_owned();
        let acquired = match self.limits.queue_wait {
            Some(wait) => tokio::time::timeout(wait, acquire).await.map_err(|_| ())?,
            None => acquire.await,
        };
        acquired.map(Some).map_err(|_| ())
    }

    fn oldest_age(&self) -> Option<Duration> {
        self.backlog
            .lock()
            .expect("settlement backlog")
            .first_key_value()
            .map(|(_, enqueued)| enqueued.elapsed())
    }
}

/// The one admission-reserved settlement slot a request may spend. Middleware
/// and legacy cancellation accounting take from the same slot so the capacity
/// is transferred exactly once — never released and then `try_spawn`ed, which
/// refuses when that same slot still occupies the ceiling.
#[derive(Clone)]
pub(crate) struct SettlementSlot {
    inner: Arc<Mutex<Option<SettlementReservation>>>,
}

impl Default for SettlementSlot {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(None)),
        }
    }
}

impl SettlementSlot {
    pub fn insert(&self, reservation: SettlementReservation) {
        *self.inner.lock().expect("settlement slot") = Some(reservation);
    }

    pub fn take(&self) -> Option<SettlementReservation> {
        self.inner.lock().expect("settlement slot").take()
    }
}

/// Settlement capacity held by one admitted request until its settlement is
/// spawned. Dropping it unspawned — a request refused after admission, or one
/// whose accounting ran inline — returns the capacity.
pub struct SettlementReservation {
    shared: Arc<Shared>,
    permit: Option<OwnedSemaphorePermit>,
    consumed: bool,
}

impl SettlementReservation {
    /// Hand the capacity to a spawned settlement. The reservation stage ends
    /// here; the guard that receives the permit owns the count from now on.
    fn consume(mut self) -> (Arc<Shared>, Option<OwnedSemaphorePermit>) {
        self.consumed = true;
        self.shared.reserved.fetch_sub(1, Ordering::AcqRel);
        metrics::record_settlement_stage_left(STAGE_RESERVED);
        (Arc::clone(&self.shared), self.permit.take())
    }
}

impl Drop for SettlementReservation {
    fn drop(&mut self) {
        if self.consumed {
            return;
        }
        self.shared.reserved.fetch_sub(1, Ordering::AcqRel);
        metrics::record_settlement_stage_left(STAGE_RESERVED);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Completed,
    QueueTimeout,
    ExecutionTimeout,
    Panicked,
    /// The task was dropped without running to an outcome: aborted, or the
    /// runtime shut down under it.
    Cancelled,
}

impl Outcome {
    fn failure_reason(self) -> Option<&'static str> {
        match self {
            Self::Completed => None,
            Self::QueueTimeout => Some(FAILURE_QUEUE_TIMEOUT),
            Self::ExecutionTimeout => Some(FAILURE_EXECUTION_TIMEOUT),
            Self::Panicked => Some(FAILURE_PANICKED),
            Self::Cancelled => Some(FAILURE_CANCELLED),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stage {
    Queued,
    Executing,
}

/// One spawned settlement's place in the accounting. Lives inside the task, so
/// whichever way the task ends — completion, timeout, panic, abort — the
/// counts, the backlog entry, and the capacity permit are released exactly
/// once, by `Drop`.
struct Guard {
    shared: Arc<Shared>,
    _permit: Option<OwnedSemaphorePermit>,
    sequence: u64,
    enqueued: Instant,
    stage: Stage,
    outcome: Option<Outcome>,
    /// True once a failure metric has been emitted, so Drop does not count
    /// the same settlement twice after an immediate timeout report.
    failure_recorded: bool,
}

impl Guard {
    fn enqueue(reservation: SettlementReservation) -> Self {
        let (shared, permit) = reservation.consume();
        let sequence = shared.sequence.fetch_add(1, Ordering::AcqRel);
        let enqueued = Instant::now();
        shared
            .backlog
            .lock()
            .expect("settlement backlog")
            .insert(sequence, enqueued);
        shared.queued.fetch_add(1, Ordering::AcqRel);
        shared.spawned.fetch_add(1, Ordering::AcqRel);
        metrics::record_settlement_stage_entered(STAGE_QUEUED);
        Self {
            shared,
            _permit: permit,
            sequence,
            enqueued,
            stage: Stage::Queued,
            outcome: None,
            failure_recorded: false,
        }
    }

    fn start_executing(&mut self) {
        self.shared.queued.fetch_sub(1, Ordering::AcqRel);
        self.shared.running.fetch_add(1, Ordering::AcqRel);
        metrics::record_settlement_stage_left(STAGE_QUEUED);
        metrics::record_settlement_stage_entered(STAGE_EXECUTING);
        metrics::record_settlement_queue_wait(self.enqueued.elapsed().as_secs_f64() * 1_000.0);
        self.stage = Stage::Executing;
    }

    fn note_execution_timeout(&mut self) {
        self.outcome = Some(Outcome::ExecutionTimeout);
        if self.failure_recorded {
            return;
        }
        metrics::record_settlement_failure(FAILURE_EXECUTION_TIMEOUT);
        tracing::error!(
            reason = FAILURE_EXECUTION_TIMEOUT,
            waited_ms = self.enqueued.elapsed().as_millis() as u64,
            "settlement missed its execution deadline; the slot is held until Store work ends"
        );
        self.failure_recorded = true;
        #[cfg(test)]
        self.shared.deadline_misses.fetch_add(1, Ordering::AcqRel);
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        let outcome = self.outcome.unwrap_or(Outcome::Cancelled);
        match self.stage {
            Stage::Queued => {
                self.shared.queued.fetch_sub(1, Ordering::AcqRel);
                metrics::record_settlement_stage_left(STAGE_QUEUED);
            }
            Stage::Executing => {
                self.shared.running.fetch_sub(1, Ordering::AcqRel);
                metrics::record_settlement_stage_left(STAGE_EXECUTING);
            }
        }
        self.shared.spawned.fetch_sub(1, Ordering::AcqRel);
        self.shared
            .backlog
            .lock()
            .expect("settlement backlog")
            .remove(&self.sequence);
        if let Some(reason) = outcome.failure_reason() {
            if !self.failure_recorded {
                metrics::record_settlement_failure(reason);
                tracing::error!(
                    reason,
                    waited_ms = self.enqueued.elapsed().as_millis() as u64,
                    "settlement did not complete; its charge is not retried and may be unrecorded"
                );
            }
        }
        // After the counts, so a waiter woken by this sees them already
        // decremented. The permit drops with `self`, after this runs, which is
        // fine: the waiter watches the counts, not the semaphore.
        self.shared.finished.send_modify(|version| *version += 1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    fn limits(max_pending: usize, max_in_flight: usize) -> SettlementLimits {
        SettlementLimits {
            max_pending: Some(max_pending),
            max_in_flight: Some(max_in_flight),
            queue_wait: Some(Duration::from_millis(200)),
            execution_timeout: Some(Duration::from_millis(200)),
        }
    }

    async fn settle_soon(settlements: &Settlements) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while settlements.backlog().unsettled() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("settlements finish");
    }

    #[test]
    fn zero_means_unbounded_when_limits_are_resolved() {
        let config = AdmissionConfig {
            max_pending_settlements: 0,
            max_in_flight_settlements: 0,
            settlement_queue_wait_ms: 0,
            settlement_timeout_ms: 0,
            ..AdmissionConfig::default()
        };
        assert_eq!(
            SettlementLimits::from(&config),
            SettlementLimits::unbounded()
        );
        let shipped = SettlementLimits::from(&AdmissionConfig::default());
        assert_eq!(shipped.max_pending, Some(4_096));
        assert_eq!(shipped.max_in_flight, Some(64));
        assert_eq!(shipped.queue_wait, Some(Duration::from_secs(10)));
        assert_eq!(shipped.execution_timeout, Some(Duration::from_secs(10)));
    }

    #[tokio::test]
    async fn a_reservation_is_capacity_until_it_is_spawned_or_dropped() {
        let settlements = Settlements::new(limits(1, 1));
        let reserved = settlements.reserve().expect("under the ceiling");
        assert_eq!(settlements.backlog().reserved, 1);
        assert_eq!(
            settlements.reserve().err(),
            Some(AdmissionRejection::Settlement)
        );
        drop(reserved);
        assert_eq!(settlements.backlog().reserved, 0);
        let reserved = settlements.reserve().expect("capacity returned");
        settlements.spawn(reserved, async {});
        settle_soon(&settlements).await;
        assert_eq!(settlements.backlog(), Backlog::default());
        settlements
            .reserve()
            .expect("capacity returned after settling");
    }

    #[tokio::test]
    async fn completion_cancellation_panic_and_timeout_each_release_once() {
        let settlements = Settlements::new(limits(4, 4));
        let (block_tx, block_rx) = tokio::sync::oneshot::channel::<()>();
        let (never_tx, never_rx) = tokio::sync::oneshot::channel::<()>();

        settlements.spawn(settlements.reserve().unwrap(), async {});
        settlements.spawn(settlements.reserve().unwrap(), async {
            panic!("a settlement that panics");
        });
        settlements.spawn(settlements.reserve().unwrap(), async {
            let _ = block_rx.await;
        });
        settlements.spawn(settlements.reserve().unwrap(), async {
            // Held past the execution deadline and never released.
            let _ = never_rx.await;
        });
        assert_eq!(
            settlements.reserve().err(),
            Some(AdmissionRejection::Settlement),
            "four settlements hold the whole capacity"
        );
        drop(block_tx);
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert_eq!(
            settlements.backlog().executing,
            1,
            "a missed execution deadline keeps the slot until the future ends"
        );
        drop(never_tx);
        settle_soon(&settlements).await;
        assert_eq!(settlements.backlog(), Backlog::default());
        for _ in 0..4 {
            settlements
                .reserve()
                .expect("every exit path released once");
        }
    }

    /// A settlement is aborted when the runtime that owns it drops its tasks;
    /// the guard inside the task is dropped with it, without any outcome having
    /// been decided, and must still give the capacity back.
    #[test]
    fn an_aborted_settlement_releases_its_capacity() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let settlements = Settlements::new(SettlementLimits {
            max_pending: Some(1),
            max_in_flight: Some(1),
            queue_wait: None,
            execution_timeout: None,
        });
        let for_task = settlements.clone();
        runtime.block_on(async move {
            for_task.spawn(for_task.reserve().unwrap(), async {
                std::future::pending::<()>().await;
            });
            tokio::task::yield_now().await;
            assert_eq!(for_task.backlog().executing, 1);
            assert_eq!(
                for_task.reserve().err(),
                Some(AdmissionRejection::Settlement)
            );
        });
        runtime.shutdown_timeout(Duration::from_secs(1));
        assert_eq!(settlements.backlog(), Backlog::default());
        settlements
            .reserve()
            .expect("the aborted settlement released its slot");
    }

    #[tokio::test]
    async fn execution_is_bounded_and_the_queue_wait_is_separate() {
        let settlements = Settlements::new(SettlementLimits {
            max_pending: Some(8),
            max_in_flight: Some(1),
            queue_wait: Some(Duration::from_millis(50)),
            execution_timeout: None,
        });
        let running = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let (release_tx, release_rx) = tokio::sync::watch::channel(false);
        for _ in 0..3 {
            let running = Arc::clone(&running);
            let peak = Arc::clone(&peak);
            let mut release = release_rx.clone();
            settlements.spawn(settlements.reserve().unwrap(), async move {
                let now = running.fetch_add(1, Ordering::AcqRel) + 1;
                peak.fetch_max(now, Ordering::AcqRel);
                let _ = release.wait_for(|released| *released).await;
                running.fetch_sub(1, Ordering::AcqRel);
            });
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        let backlog = settlements.backlog();
        assert_eq!(backlog.executing, 1, "one execution slot");
        assert_eq!(backlog.queued, 2, "the rest wait");
        assert!(
            backlog
                .oldest_age
                .is_some_and(|age| age >= Duration::from_millis(15))
        );
        // The queue wait expires for the two that never got a slot; the one
        // executing is untouched by it.
        tokio::time::sleep(Duration::from_millis(80)).await;
        let backlog = settlements.backlog();
        assert_eq!((backlog.executing, backlog.queued), (1, 0));
        release_tx.send(true).unwrap();
        settle_soon(&settlements).await;
        assert_eq!(peak.load(Ordering::Acquire), 1);
        assert_eq!(settlements.backlog(), Backlog::default());
    }

    #[tokio::test]
    async fn try_spawn_hands_the_work_back_when_saturated() {
        let settlements = Settlements::new(limits(1, 1));
        let held = settlements.reserve().expect("the one slot");
        let refused = settlements.try_spawn(async {});
        assert!(refused.is_err(), "the future comes back to its caller");
        drop(held);
        settlements
            .try_spawn(async {})
            .ok()
            .expect("capacity returned");
        settle_soon(&settlements).await;
    }

    #[tokio::test]
    async fn await_idle_returns_within_its_bound_and_reports_leftovers() {
        let settlements = Settlements::new(SettlementLimits::unbounded());
        let _reserved = settlements.reserve().unwrap();
        let (hold_tx, hold_rx) = tokio::sync::oneshot::channel::<()>();
        settlements.spawn(settlements.reserve().unwrap(), async {
            let _ = hold_rx.await;
        });
        let started = Instant::now();
        let leftovers = settlements.await_idle(Duration::from_millis(50)).await;
        assert!(started.elapsed() < Duration::from_secs(1));
        assert_eq!((leftovers.reserved, leftovers.executing), (1, 1));
        drop(hold_tx);
        let idle = settlements.await_idle(Duration::from_secs(2)).await;
        assert_eq!(idle.unsettled(), 0);
        assert_eq!(
            idle.reserved, 1,
            "an unspawned reservation is reported, not waited for"
        );
    }

    #[test]
    fn spawned_count_covers_the_queued_to_executing_stage_gap() {
        let settlements = Settlements::new(limits(1, 1));
        let guard = Guard::enqueue(settlements.reserve().unwrap());
        assert_eq!(settlements.backlog().spawned, 1);
        assert_eq!(settlements.backlog().queued, 1);
        // The cutover decrements `queued` before incrementing `running`. A
        // waiter that summed the stage counters would see idle here and flush
        // sinks before the settlement had written.
        guard.shared.queued.fetch_sub(1, Ordering::AcqRel);
        let stages = guard.shared.queued.load(Ordering::Acquire)
            + guard.shared.running.load(Ordering::Acquire);
        assert_eq!(stages, 0, "the stage counters have a visible gap");
        assert_eq!(
            guard.shared.spawned.load(Ordering::Acquire),
            1,
            "spawned stays nonzero across the cutover await_idle watches"
        );
        guard.shared.queued.fetch_add(1, Ordering::AcqRel);
        drop(guard);
        assert_eq!(settlements.backlog().spawned, 0);
    }

    #[tokio::test]
    async fn await_idle_waits_for_spawned_work_not_the_sum_of_stages() {
        let settlements = Settlements::new(SettlementLimits {
            max_pending: Some(1),
            max_in_flight: Some(1),
            queue_wait: None,
            execution_timeout: None,
        });
        let (hold_tx, hold_rx) = tokio::sync::oneshot::channel::<()>();
        settlements.spawn(settlements.reserve().unwrap(), async {
            let _ = hold_rx.await;
        });
        tokio::task::yield_now().await;
        let waiter = {
            let settlements = settlements.clone();
            tokio::spawn(async move { settlements.await_idle(Duration::from_millis(40)).await })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !waiter.is_finished(),
            "await_idle must not return while a settlement is still spawned"
        );
        drop(hold_tx);
        let leftovers = waiter.await.expect("waiter");
        assert_eq!(leftovers.unsettled(), 0);
    }

    #[tokio::test]
    async fn spawn_reserved_consumes_the_request_slot_at_capacity() {
        let settlements = Settlements::new(limits(1, 1));
        let reserved = settlements.reserve().expect("the one slot");
        assert!(
            settlements.try_spawn(async {}).is_err(),
            "try_spawn cannot take a second slot the request still holds"
        );
        let ran = Arc::new(AtomicUsize::new(0));
        let ran_for_task = Arc::clone(&ran);
        settlements.spawn_reserved(
            Some(reserved),
            async move {
                ran_for_task.fetch_add(1, Ordering::AcqRel);
            },
            "zero-charge budget release",
        );
        settle_soon(&settlements).await;
        assert_eq!(ran.load(Ordering::Acquire), 1);
        settlements
            .reserve()
            .expect("the consumed slot was released after the reserved spawn");
    }

    #[tokio::test]
    async fn oldest_pending_age_advances_without_enqueue_or_completion() {
        let settlements = Settlements::new(limits(1, 1));
        let (hold_tx, hold_rx) = tokio::sync::oneshot::channel::<()>();
        settlements.spawn(settlements.reserve().unwrap(), async {
            let _ = hold_rx.await;
        });
        tokio::task::yield_now().await;
        let first = settlements
            .backlog()
            .oldest_age
            .expect("a spawned settlement has an age");
        tokio::time::sleep(Duration::from_millis(30)).await;
        let later = settlements.backlog().oldest_age.expect("still spawned");
        assert!(
            later > first,
            "age is computed from the enqueue Instant, so a stall climbs"
        );
        drop(hold_tx);
        settle_soon(&settlements).await;
    }

    #[tokio::test]
    async fn execution_timeout_keeps_capacity_until_blocking_work_finishes() {
        let settlements = Settlements::new(SettlementLimits {
            max_pending: Some(1),
            max_in_flight: Some(1),
            queue_wait: None,
            execution_timeout: Some(Duration::from_millis(50)),
        });
        let (started_tx, started_rx) = tokio::sync::oneshot::channel::<()>();
        let (finish_tx, finish_rx) = tokio::sync::oneshot::channel::<()>();
        settlements.spawn(settlements.reserve().unwrap(), async move {
            tokio::task::spawn_blocking(move || {
                let _ = started_tx.send(());
                let _ = finish_rx.blocking_recv();
            })
            .await
            .expect("blocking settlement work");
        });
        started_rx.await.expect("the blocking closure started");
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(
            settlements.deadline_misses(),
            1,
            "the timeout is counted when the deadline fires, not when blocking work ends"
        );
        assert_eq!(
            settlements.backlog().executing,
            1,
            "the deadline counted a timeout without releasing the slot"
        );
        assert_eq!(
            settlements.reserve().err(),
            Some(AdmissionRejection::Settlement),
            "a late spawn_blocking charge cannot outrun max_in_flight"
        );
        drop(finish_tx);
        settle_soon(&settlements).await;
        settlements
            .reserve()
            .expect("capacity returns only after the blocking work ends");
        assert_eq!(
            settlements.deadline_misses(),
            1,
            "draining the join must not record the timeout a second time"
        );
    }
}
