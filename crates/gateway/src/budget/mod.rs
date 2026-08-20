//! Spend-budget enforcement — the read path.
//!
//! Budgets are denominated in **micro-dollars** (integer; no float drift), not
//! tokens: a `gpt-4o` token and a `claude-haiku` token cost wildly different
//! amounts, so a token cap is not a spend cap. Cost is derived from the model's
//! `price` (§catalog `ModelPrice`) applied to actual usage.
//!
//! Deliberately a *separate* trait from [`crate::usage::UsageSink`] (§5.2):
//! budget checks are on the request path (fast, fresh), records are off it
//! (slow, batched). A Tinybird sink is fine; a Tinybird budget store is not.
//!
//! Actual cost is unknown until a response completes, so enforcement is
//! **reserve → compute-actual → settle**: [`BudgetStore::reserve`] holds a
//! conservative estimate before dispatch, and [`BudgetStore::settle`] converts
//! that hold into the measured spend afterwards — or releases it entirely when
//! nothing was consumed. A reservation is *held*, so concurrent in-flight
//! requests cannot collectively overshoot the cap (ADR 0010).
//!
//! Three backends ship. [`NoBudget`] is the default and touches no datastore
//! (ADR 0002). [`InMemoryBudget`] holds reservations per replica. The shared
//! backends — [`redis::RedisBudget`] and [`postgres::PostgresBudget`] — hold
//! them in one datastore, so a replica set enforces a single cap. When a shared
//! store is unreachable the default stance is **fail-closed**: admission is
//! denied rather than silently unenforced.
//!
//! One of the seven responsibility-specific backends catalogued in
//! [`crate::backends`], and deliberately its own seam: this is a *request-path*
//! contract with a millisecond latency budget and its own `on_unavailable`
//! policy, which is why it shares no trait with the control-plane contracts even
//! when both happen to be pointed at Postgres.

mod postgres;
mod redis;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;

use crate::backends::health::BackendHealth;
use crate::config::{BudgetBackend, BudgetConfig, StoreUnavailable};
use crate::desired_state::policy::PolicyGeneration;
use crate::policy::{BudgetCaps, Ceilings, PolicyHold, Unenforceable, denied};
use crate::telemetry::metrics;

pub use postgres::PostgresBudget;
pub use redis::{MigrationReport, RedisBudget};

/// The dimension a budget is scoped to. Neutral vocabulary, like usage records.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BudgetKey {
    pub namespace: String,
    pub subject: String,
}

/// A held estimate: the outcome of an admitted [`BudgetStore::reserve`], and the
/// handle that [`BudgetStore::settle`] converts into measured spend. Cheap to
/// clone so the streaming relay can carry it to a detached settlement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reservation {
    /// Unique per reservation, so a settlement releases exactly the hold it
    /// belongs to even when a key has many in flight.
    pub id: String,
    pub estimate_microdollars: u64,
    /// The published policy generation this hold was admitted under, or `None`
    /// when the bootstrap file's caps admitted it (#150).
    ///
    /// Carried on the hold rather than looked up at settlement, which is what
    /// makes a publication bind from the next admission: a request admitted under
    /// the previous document settles against the generation that granted it, and
    /// a drain is finished when no hold names the superseded generation any more.
    pub generation: Option<PolicyGeneration>,
}

impl Reservation {
    /// The reservation a store that holds nothing hands back.
    pub fn unheld() -> Self {
        Self {
            id: String::new(),
            estimate_microdollars: 0,
            generation: None,
        }
    }

    /// A process-unique id. A stale hold is reclaimed by its own expiry rather
    /// than by id collision, so a monotonic counter with the process's start
    /// time is enough — and, unlike a UUID, needs no dependency.
    fn next_id() -> String {
        static COUNTER: AtomicU64 = AtomicU64::new(1);
        static EPOCH: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
        let epoch = *EPOCH.get_or_init(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_micros() as u64)
                .unwrap_or_default()
        });
        format!("{epoch:x}-{:x}", COUNTER.fetch_add(1, Ordering::Relaxed))
    }
}

/// Why a request was not admitted. Distinct arms because they are distinct
/// answers to the caller: over-cap is the caller's problem (`429`), an
/// unreachable store is the gateway's (`503`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Denial {
    Exceeded,
    StoreUnavailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Admission {
    Allowed(Reservation),
    Denied(Denial),
}

impl Admission {
    #[cfg(test)]
    fn reservation(&self) -> &Reservation {
        match self {
            Self::Allowed(reservation) => reservation,
            Self::Denied(reason) => panic!("expected an admitted request, got {reason:?}"),
        }
    }
}

#[async_trait]
pub trait BudgetStore: Send + Sync {
    fn name(&self) -> &'static str;
    /// Pre-dispatch check that *holds* the estimate, in micro-dollars, so the
    /// requests already in flight count against the cap.
    async fn reserve(&self, key: &BudgetKey, estimated_microdollars: u64) -> Admission;
    /// Release the reservation and record the measured spend, in micro-dollars.
    ///
    /// **Exactly once per admitted request**, whatever its outcome — completion,
    /// upstream failure, client cancellation, or a dropped handler. The route
    /// guarantees it: the guard holding the reservation is disarmed before the
    /// call, so a settlement and its drop-path fallback cannot both run, and no
    /// caller retries a settlement. So an implementation must charge and release
    /// in one atomic step (neither alone is recoverable afterwards), and it must
    /// not assume a second chance: a settlement that fails at the store leaves the
    /// hold to expire with its TTL, which the reserve path reclaims. That is the
    /// bound on how long a failed settlement can hold a budget, and it is why the
    /// TTL should exceed the longest expected request rather than be generous.
    async fn settle(&self, key: &BudgetKey, reservation: &Reservation, actual_microdollars: u64);
    /// Drop a reservation that consumed nothing. Settling zero is the same
    /// operation, and every backend implements it that way.
    async fn release(&self, key: &BudgetKey, reservation: &Reservation) {
        self.settle(key, reservation, 0).await;
    }

    /// This store's reachability, for the status refresher only.
    ///
    /// `None` for a store with no remote dependency — `none` and `in-memory`
    /// cannot be unreachable, and their component reports `disabled` rather than
    /// a state invented for them. Never called from the request path: the handle
    /// goes to a [`ComponentProbe`], which only the refresher holds.
    ///
    /// [`ComponentProbe`]: crate::status::registry::ComponentProbe
    fn health(&self) -> Option<Arc<dyn BackendHealth>> {
        None
    }
}

/// Always-allow. The default posture when no budget is configured.
pub struct NoBudget;

#[async_trait]
impl BudgetStore for NoBudget {
    fn name(&self) -> &'static str {
        "none"
    }
    async fn reserve(&self, _key: &BudgetKey, _estimated_microdollars: u64) -> Admission {
        Admission::Allowed(Reservation::unheld())
    }
    async fn settle(
        &self,
        _key: &BudgetKey,
        _reservation: &Reservation,
        _actual_microdollars: u64,
    ) {
    }
}

/// Per-replica in-memory spend counter (micro-dollars). No datastore, so a
/// fleet enforces per-replica ceilings — documented, not hidden. Reservations
/// are held, so concurrent requests on *this* replica cannot overshoot.
pub struct InMemoryBudget {
    limit_microdollars: u64,
    idle_ttl: Duration,
    reservation_ttl: Duration,
    max_subjects: usize,
    namespace_count: usize,
    floor: usize,
    unavailable: UnavailablePolicy,
    ledger_state: Mutex<LedgerState>,
}

#[derive(Default)]
struct LedgerState {
    ledgers: HashMap<BudgetKey, Ledger>,
    namespace_counts: HashMap<String, usize>,
}

struct Ledger {
    spent: u64,
    held: HashMap<String, HeldReservation>,
    last_touched: Instant,
}

struct HeldReservation {
    amount_microdollars: u64,
    expires_at: Instant,
}

impl Default for Ledger {
    fn default() -> Self {
        Self {
            spent: 0,
            held: HashMap::new(),
            last_touched: Instant::now(),
        }
    }
}

impl Ledger {
    fn outstanding(&self) -> u64 {
        self.held
            .values()
            .fold(0, |sum, held| sum.saturating_add(held.amount_microdollars))
    }

    fn reclaim_expired(&mut self, now: Instant) {
        self.held.retain(|_, held| held.expires_at > now);
    }
}

impl InMemoryBudget {
    #[cfg(test)]
    pub fn new(limit_microdollars: u64) -> Self {
        Self::with_settings(
            limit_microdollars,
            Duration::from_secs(60 * 60),
            Duration::from_secs(300),
            10_000,
        )
    }

    #[cfg(test)]
    fn with_limits(limit_microdollars: u64, idle_ttl: Duration, max_subjects: usize) -> Self {
        Self::with_settings(
            limit_microdollars,
            idle_ttl,
            Duration::from_secs(300),
            max_subjects,
        )
    }

    #[cfg(test)]
    fn with_settings(
        limit_microdollars: u64,
        idle_ttl: Duration,
        reservation_ttl: Duration,
        max_subjects: usize,
    ) -> Self {
        Self::with_settings_and_policy(
            limit_microdollars,
            idle_ttl,
            reservation_ttl,
            max_subjects,
            UnavailablePolicy::Deny,
            1,
        )
    }

    fn with_settings_and_policy(
        limit_microdollars: u64,
        idle_ttl: Duration,
        reservation_ttl: Duration,
        max_subjects: usize,
        unavailable: UnavailablePolicy,
        namespace_count: usize,
    ) -> Self {
        let floor = if namespace_count == 0 || max_subjects < namespace_count {
            if max_subjects < namespace_count && namespace_count != 0 {
                tracing::warn!(
                    max_subjects,
                    configured_namespace_count = namespace_count,
                    "per-namespace ledger floors are disabled because max_subjects is below the configured namespace count"
                );
            }
            0
        } else {
            (max_subjects / namespace_count).max(1)
        };
        Self {
            limit_microdollars,
            idle_ttl,
            reservation_ttl,
            max_subjects,
            namespace_count,
            floor,
            unavailable,
            ledger_state: Mutex::new(LedgerState::default()),
        }
    }

    #[cfg(test)]
    fn with_namespace_count(
        limit_microdollars: u64,
        idle_ttl: Duration,
        reservation_ttl: Duration,
        max_subjects: usize,
        namespace_count: usize,
    ) -> Self {
        Self::with_settings_and_policy(
            limit_microdollars,
            idle_ttl,
            reservation_ttl,
            max_subjects,
            UnavailablePolicy::Deny,
            namespace_count,
        )
    }

    fn prune_idle(&self, state: &mut LedgerState) {
        let now = Instant::now();
        let removed = state
            .ledgers
            .iter_mut()
            .filter_map(|(key, ledger)| {
                ledger.reclaim_expired(now);
                (ledger.held.is_empty()
                    && now.saturating_duration_since(ledger.last_touched) > self.idle_ttl)
                    .then_some(key.clone())
            })
            .collect::<Vec<_>>();
        for key in removed {
            state.remove(&key);
        }
    }

    fn prune_namespace(&self, namespace: &str, state: &mut LedgerState) {
        let now = Instant::now();
        let removed = state
            .ledgers
            .iter_mut()
            .filter_map(|(key, ledger)| {
                if key.namespace != namespace {
                    return None;
                }
                ledger.reclaim_expired(now);
                (ledger.held.is_empty()
                    && now.saturating_duration_since(ledger.last_touched) > self.idle_ttl)
                    .then_some(key.clone())
            })
            .collect::<Vec<_>>();
        for key in removed {
            state.remove(&key);
        }
    }

    fn reserved_for_others(&self, namespace: &str, state: &LedgerState) -> usize {
        let present_other_shortfall = state
            .namespace_counts
            .iter()
            .filter(|(present_namespace, _)| present_namespace.as_str() != namespace)
            .map(|(_, retained)| self.floor.saturating_sub(*retained))
            .sum::<usize>();
        let requesting_present = state.namespace_counts.contains_key(namespace);
        let absent_other_count = self
            .namespace_count
            .saturating_sub(state.namespace_counts.len())
            .saturating_sub(usize::from(!requesting_present));
        let reservation =
            present_other_shortfall.saturating_add(absent_other_count.saturating_mul(self.floor));
        let free = self.max_subjects.saturating_sub(state.ledgers.len());
        let requesting_shortfall = self
            .floor
            .saturating_sub(state.namespace_counts.get(namespace).copied().unwrap_or(0));
        reservation.min(free.saturating_sub(requesting_shortfall))
    }
}

impl LedgerState {
    fn entry_or_default(&mut self, key: &BudgetKey) -> &mut Ledger {
        if !self.ledgers.contains_key(key) {
            *self
                .namespace_counts
                .entry(key.namespace.clone())
                .or_default() += 1;
        }
        self.ledgers.entry(key.clone()).or_default()
    }

    fn remove(&mut self, key: &BudgetKey) -> Option<Ledger> {
        let removed = self.ledgers.remove(key);
        if removed.is_some() {
            self.decrement_namespace_count(&key.namespace);
        }
        removed
    }

    fn decrement_namespace_count(&mut self, namespace: &str) {
        if let Some(count) = self.namespace_counts.get_mut(namespace) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.namespace_counts.remove(namespace);
            }
        }
    }
}

impl InMemoryBudget {
    #[cfg(test)]
    pub fn outstanding(&self, key: &BudgetKey) -> u64 {
        self.ledger_state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .ledgers
            .get(key)
            .map_or(0, Ledger::outstanding)
    }
}

#[async_trait]
impl BudgetStore for InMemoryBudget {
    fn name(&self) -> &'static str {
        "in_memory"
    }

    async fn reserve(&self, key: &BudgetKey, estimated_microdollars: u64) -> Admission {
        let mut state = self.ledger_state.lock().unwrap_or_else(|e| e.into_inner());
        if !state.ledgers.contains_key(key) {
            let free = self.max_subjects.saturating_sub(state.ledgers.len());
            let reserved_for_others = self.reserved_for_others(&key.namespace, &state);
            if free <= reserved_for_others {
                if self.floor == 0 {
                    self.prune_idle(&mut state);
                } else {
                    self.prune_namespace(&key.namespace, &mut state);
                }
                metrics::record_budget_retained_subjects(state.ledgers.len());
                let free_after_prune = self.max_subjects.saturating_sub(state.ledgers.len());
                let reserved_after_prune = self.reserved_for_others(&key.namespace, &state);
                if free_after_prune <= reserved_after_prune {
                    let reason = if self.floor == 0 || free_after_prune == 0 {
                        "ledger capacity reached"
                    } else {
                        "ledger capacity is reserved for other namespaces"
                    };
                    tracing::warn!(
                        namespace = %key.namespace,
                        namespace_retained = state.namespace_counts.get(&key.namespace).copied().unwrap_or(0),
                        floor = self.floor,
                        global_retained = state.ledgers.len(),
                        max_subjects = self.max_subjects,
                        reason,
                        "budget ledger capacity denied"
                    );
                    // Nothing was written, so nothing is uncertain: this denial
                    // is the store refusing a ledger, not a lost answer.
                    let admission = self.unavailable.admission("in_memory", &reason, None);
                    if matches!(&admission, Admission::Denied(Denial::StoreUnavailable)) {
                        metrics::record_budget_capacity_denial();
                    }
                    return admission;
                }
            }
        }
        let ledger = state.entry_or_default(key);
        let now = Instant::now();
        ledger.reclaim_expired(now);
        ledger.last_touched = now;
        let committed = ledger.spent.saturating_add(ledger.outstanding());
        if committed.saturating_add(estimated_microdollars) > self.limit_microdollars {
            return Admission::Denied(Denial::Exceeded);
        }
        // Per-replica, so no published policy governs it: a fleet-wide document
        // is refused on this backend at activation rather than approximated here.
        let reservation = Reservation {
            id: Reservation::next_id(),
            estimate_microdollars: estimated_microdollars,
            generation: None,
        };
        ledger.held.insert(
            reservation.id.clone(),
            HeldReservation {
                amount_microdollars: estimated_microdollars,
                expires_at: now.checked_add(self.reservation_ttl).unwrap_or(now),
            },
        );
        Admission::Allowed(reservation)
    }

    async fn settle(&self, key: &BudgetKey, reservation: &Reservation, actual_microdollars: u64) {
        if reservation.id.is_empty() {
            return;
        }
        let mut state = self.ledger_state.lock().unwrap_or_else(|e| e.into_inner());
        let ledger = state.entry_or_default(key);
        let now = Instant::now();
        ledger.reclaim_expired(now);
        ledger.held.remove(&reservation.id);
        ledger.last_touched = now;
        ledger.spent = ledger.spent.saturating_add(actual_microdollars);
    }
}

/// What a shared store does when it cannot be reached. The default is
/// fail-closed: an unenforceable cap denies rather than silently admitting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnavailablePolicy {
    Deny,
    Allow,
}

impl From<StoreUnavailable> for UnavailablePolicy {
    fn from(value: StoreUnavailable) -> Self {
        match value {
            StoreUnavailable::Deny => Self::Deny,
            StoreUnavailable::Allow => Self::Allow,
        }
    }
}

/// A reserve whose outcome the caller never learned, and the hold it took
/// before making it.
///
/// A lost response is not a lost side effect: the Lua script may have written
/// the reservation, or the transaction may have committed, before the
/// connection broke. The id is gone with the answer, so nothing will ever
/// settle that entry — it is reclaimed by the reservation TTL it was written
/// with, and until then the generation that priced it is still represented in
/// the store. So the hold outlives the request by exactly that long, and a
/// drain keeps meaning what the runbook says: nothing the generation admitted
/// is left in the store.
pub(crate) struct Uncertain {
    pub(crate) hold: PolicyHold,
    /// The TTL the reservation would have been written with.
    pub(crate) reservation_ttl: Duration,
}

impl UnavailablePolicy {
    /// The admission for a reservation the store could not answer.
    ///
    /// `uncertain` carries the caller's hold when the failed call may have left
    /// a reservation behind; the hold is then kept for that reservation's whole
    /// TTL rather than dropped, on both stances — a fail-closed denial does not
    /// un-write what the store may have committed. It is `None` only where the
    /// failure provably precedes any side effect.
    fn admission(
        self,
        backend: &'static str,
        error: &dyn std::fmt::Display,
        uncertain: Option<Uncertain>,
    ) -> Admission {
        if let Some(Uncertain {
            hold,
            reservation_ttl,
        }) = uncertain
        {
            hold.linger(reservation_ttl);
        }
        match self {
            Self::Deny => {
                tracing::error!(
                    backend,
                    error = %error,
                    "budget cap is unenforceable; denying (fail-closed)"
                );
                Admission::Denied(Denial::StoreUnavailable)
            }
            Self::Allow => {
                tracing::warn!(
                    backend,
                    error = %error,
                    "budget cap is unenforceable; admitting unenforced (fail-open)"
                );
                Admission::Allowed(Reservation::unheld())
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BudgetError {
    #[error("budget backend `{backend}`: {message}")]
    Invalid {
        backend: &'static str,
        message: String,
    },
    #[error("redis budget backend: {0}")]
    Redis(#[from] ::redis::RedisError),
    #[error("postgres budget backend: {0}")]
    Postgres(#[from] tokio_postgres::Error),
}

impl BudgetError {
    fn invalid(backend: &'static str, message: impl Into<String>) -> Self {
        Self::Invalid {
            backend,
            message: message.into(),
        }
    }
}

/// What a durable backend needs beyond its connection: where it reads the caps
/// it enforces, which key layout it was built on, and the stance to take when the
/// store cannot be reached.
///
/// The split is #150's: the *values* are read per request through [`Ceilings`],
/// because a publication changes them without a restart; the *layout* and the
/// unavailability stance are fixed for the life of the process, because changing
/// either under outstanding holds is a migration, not a setting.
#[derive(Debug, Clone)]
pub struct SharedSettings {
    pub ceilings: Ceilings,
    /// Whether this store's keys carry a namespace-wide ledger. `true` turns
    /// every reserve and settle into a composite `(subject, namespace)` operation
    /// on the same logical reservation; `false` leaves the subject-only behavior
    /// untouched.
    pub namespace_scope: bool,
    pub unavailable: UnavailablePolicy,
}

impl SharedSettings {
    /// The settings a deployment whose caps never change runs with. Test-only:
    /// a serving process reads them through the published runtime.
    #[cfg(test)]
    pub fn fixed(caps: BudgetCaps, unavailable: UnavailablePolicy) -> Self {
        Self {
            namespace_scope: caps.namespace_microdollars.is_some(),
            ceilings: Ceilings::fixed(crate::policy::ActivePolicy {
                budget: Some(caps),
                concurrency: None,
                generation: None,
                static_only: false,
            }),
            unavailable,
        }
    }

    pub const fn enforces_namespace_cap(&self) -> bool {
        self.namespace_scope
    }

    /// The caps governing `namespace` and the generation that stated them.
    /// `Some` with `caps = None` is an intentional flat-v2 static-only
    /// namespace, which bypasses exact budget enforcement. `None` still means
    /// that a projected namespace has no policy document and must be denied.
    ///
    /// A store that cannot answer "what is the cap here" must not admit: an
    /// unenforced cap and an infinite one are indistinguishable to a caller, and
    /// only one of them is what an operator published. The layout is checked with
    /// it, so a view whose scope-wide cap disagrees with the keys this process
    /// booted on denies rather than enforcing half of each.
    ///
    /// Both come out of *one* read of the published view: a hold stamped with a
    /// generation whose caps were never applied to it would let that
    /// generation's drain finish while a request granted under it is still in
    /// flight.
    /// `store` is the `axond.policy.store` this store denies under: the
    /// responsibility and the backend, because the concurrency store is commonly
    /// the same backend and the two denials are different operator problems.
    pub(crate) fn caps(&self, store: &'static str, namespace: &str) -> Option<Governing> {
        let policy = self.ceilings.active(namespace);
        if policy.is_static_only() {
            return Some(Governing {
                caps: None,
                generation: None,
            });
        }
        let Some(caps) = policy.budget else {
            // Every one of these denials is counted; the explanation is sampled,
            // because the condition belongs to the published view and repeating
            // it per request scales the log with traffic rather than with the
            // problem.
            if denied(Unenforceable::Ungoverned, store, namespace) {
                tracing::warn!(
                    store,
                    namespace,
                    "no policy governs this namespace, so its spend cap cannot be enforced; \
                     denying every request for it until one is published"
                );
            }
            return None;
        };
        if caps.namespace_microdollars.is_some() != self.namespace_scope {
            if denied(Unenforceable::Layout, store, namespace) {
                tracing::error!(
                    store,
                    namespace,
                    namespace_scope = self.namespace_scope,
                    "the active policy's scope-wide cap disagrees with the key layout this \
                     process booted on; denying rather than enforcing against the wrong ledgers"
                );
            }
            return None;
        }
        Some(Governing {
            caps: Some(caps),
            generation: policy.generation,
        })
    }
}

/// The caps one admission is checked against, and the generation that granted
/// them — read together, so a hold cannot be accounted against a document whose
/// caps never bound it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Governing {
    pub(crate) caps: Option<BudgetCaps>,
    pub(crate) generation: Option<PolicyGeneration>,
}

/// Which cap a composite reserve ran out of. Both answer the caller with the
/// same `429 budget_exceeded`; they differ only in what an operator should do,
/// so the scope is logged and counted rather than exposed on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExceededScope {
    Subject,
    Namespace,
}

/// Build the configured backend. Connecting here means a misconfigured budget
/// store refuses to boot rather than failing (or silently not enforcing) at
/// request time — the same posture as usage sinks.
pub async fn build(
    config: &BudgetConfig,
    env: &HashMap<String, String>,
    namespace_count: usize,
    ceilings: Ceilings,
) -> Result<Box<dyn BudgetStore>, BudgetError> {
    let settings = SharedSettings {
        ceilings,
        namespace_scope: config.enforces_namespace_scope(),
        unavailable: config.on_unavailable.into(),
    };
    match config.backend {
        // `validate_budget` already refuses a namespace cap on these two, so
        // reaching them here means no namespace cap was asked for.
        BudgetBackend::None => Ok(Box::new(NoBudget)),
        BudgetBackend::InMemory => Ok(Box::new(InMemoryBudget::with_settings_and_policy(
            config.limit_microdollars,
            Duration::from_secs(config.idle_ttl_seconds),
            Duration::from_secs(config.reservation_ttl_seconds),
            config.max_subjects,
            config.on_unavailable.into(),
            namespace_count,
        ))),
        BudgetBackend::Redis => {
            let url = dsn(config, "redis", env)?;
            Ok(Box::new(
                RedisBudget::connect(url, config.key_prefix(), settings).await?,
            ))
        }
        BudgetBackend::Postgres => {
            let dsn = dsn(config, "postgres", env)?;
            Ok(Box::new(
                PostgresBudget::connect(
                    dsn,
                    postgres::PostgresBudgetSettings {
                        table: config.table(),
                        create_table: config.create_table,
                        shared: settings,
                    },
                )
                .await?,
            ))
        }
    }
}

/// Move Redis budget state into the layout the namespace cap needs, carrying
/// accumulated spend forward. Run with every replica stopped; the gateway
/// refuses to boot with a namespace cap until this has been done, and refuses to
/// boot without one afterwards, so neither direction silently resets a ledger.
///
/// `namespaces` is the namespace id list the v1 keys are attributed against:
/// they carry an unescaped `{namespace|subject}` tag, so they are resolved
/// against real namespace ids rather than split at a delimiter that may appear
/// in either half. Stateless deployments take it from `[[namespace]]`; stateful
/// ones, whose bootstrap file cannot declare a namespace, pass the projected ids
/// on the command line. An empty list is refused before anything is scanned.
pub async fn migrate_redis(
    config: &BudgetConfig,
    namespaces: &[String],
    env: &HashMap<String, String>,
) -> Result<MigrationReport, BudgetError> {
    if config.backend != BudgetBackend::Redis {
        return Err(BudgetError::invalid(
            "redis",
            format!(
                "the Redis budget migration needs `[budget] backend = \"redis\"`, not `{}`",
                config.backend.as_str()
            ),
        ));
    }
    // The migration is one-way, and only a gateway with the cap set can serve what
    // it produces. Run against a config that has none, it would take the fleet
    // down: every replica would refuse to boot on a layout its configuration
    // cannot read, and the documented way back is a spend reset. So the command
    // requires the configuration it is a migration *to*.
    if !config.enforces_namespace_scope() {
        return Err(BudgetError::invalid(
            "redis",
            "the Redis budget migration moves this `key_prefix` to the v2 layout, which only a \
             gateway laid out for a scope-wide cap can serve. Declare it under `[budget]` first — \
             `namespace_limit_microdollars` in stateless mode, `namespace_scope = true` in \
             stateful mode, where the cap itself is published — then migrate, then start the \
             fleet on that same configuration.",
        ));
    }
    // A v1 key names `{namespace|subject}` as one tag, so a namespace list is
    // what makes it attributable at all. In stateful mode the bootstrap file
    // cannot declare a namespace, so an empty list is the ordinary case there and
    // not an operator's slip: refused here, with the flag named, rather than
    // half-way through a scan that would abort on the first key it met.
    if namespaces.is_empty() {
        return Err(BudgetError::invalid(
            "redis",
            "the Redis budget migration attributes each v1 key to a namespace, and none were \
             given. In stateless mode declare them under `[[namespace]]`; in stateful mode, where \
             namespaces belong to the control plane, pass the projected ids with `--namespace` \
             (repeat the flag), listing every namespace the fleet has served under this \
             `key_prefix`.",
        ));
    }
    let url = dsn(config, "redis", env)?;
    redis::migrate_v1_to_v2(url, &config.key_prefix(), namespaces).await
}

/// The connection string, resolved from the environment. Like every other
/// secret it is referenced by env-var name, never inlined in config.
fn dsn<'a>(
    config: &BudgetConfig,
    backend: &'static str,
    env: &'a HashMap<String, String>,
) -> Result<&'a str, BudgetError> {
    let name = config.dsn_env.as_deref().unwrap_or_default();
    env.get(name)
        .map(String::as_str)
        .filter(|dsn| !dsn.trim().is_empty())
        .ok_or_else(|| {
            BudgetError::invalid(
                backend,
                format!("`{name}` is unset or empty in the environment"),
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The ceilings a build test needs: none of these backends is reached, and
    /// what governs a namespace is decided in `crate::policy`, not here.
    fn ungoverned() -> Ceilings {
        Ceilings::fixed(crate::policy::ActivePolicy::default())
    }

    pub(super) fn key() -> BudgetKey {
        BudgetKey {
            namespace: "acme".into(),
            subject: "GW_INBOUND_ACME_KEY".into(),
        }
    }

    #[tokio::test]
    async fn admits_until_settled_spend_would_exceed_the_cap() {
        let budget = InMemoryBudget::new(1_000); // 1,000 micro-dollars
        let k = key();

        let first = budget.reserve(&k, 400).await;
        budget.settle(&k, first.reservation(), 400).await;
        let second = budget.reserve(&k, 400).await;
        budget.settle(&k, second.reservation(), 400).await; // 800 spent

        // A request estimated to cost 300 would push spend to 1,100 > 1,000.
        assert_eq!(
            budget.reserve(&k, 300).await,
            Admission::Denied(Denial::Exceeded)
        );
        // A cheaper request still fits.
        assert!(matches!(
            budget.reserve(&k, 100).await,
            Admission::Allowed(_)
        ));
    }

    /// The point of holding a reservation: two concurrent requests cannot both
    /// be admitted against a cap that only covers one.
    #[tokio::test]
    async fn an_outstanding_reservation_counts_against_the_cap() {
        let budget = InMemoryBudget::new(1_000);
        let k = key();

        let held = budget.reserve(&k, 700).await;
        assert_eq!(
            budget.reserve(&k, 700).await,
            Admission::Denied(Denial::Exceeded)
        );

        // The first request turned out to be cheap, which frees the rest.
        budget.settle(&k, held.reservation(), 100).await;
        assert!(matches!(
            budget.reserve(&k, 700).await,
            Admission::Allowed(_)
        ));
    }

    /// A request that consumed nothing must not leave its estimate held.
    #[tokio::test]
    async fn releasing_a_reservation_frees_the_whole_estimate() {
        let budget = InMemoryBudget::new(1_000);
        let k = key();

        let held = budget.reserve(&k, 1_000).await;
        budget.release(&k, held.reservation()).await;

        assert!(matches!(
            budget.reserve(&k, 1_000).await,
            Admission::Allowed(_)
        ));
    }

    #[tokio::test]
    async fn no_budget_always_admits() {
        let budget = NoBudget;
        assert!(matches!(
            budget.reserve(&key(), u64::MAX).await,
            Admission::Allowed(_)
        ));
    }

    #[test]
    fn reservation_ids_are_unique() {
        let ids: std::collections::HashSet<String> =
            (0..1_000).map(|_| Reservation::next_id()).collect();
        assert_eq!(ids.len(), 1_000);
    }

    #[tokio::test]
    async fn an_unreachable_store_denies_by_default_and_admits_when_told_to() {
        let error = "connection refused";
        assert_eq!(
            UnavailablePolicy::Deny.admission("redis", &error, None),
            Admission::Denied(Denial::StoreUnavailable)
        );
        assert!(matches!(
            UnavailablePolicy::Allow.admission("redis", &error, None),
            Admission::Allowed(_)
        ));
    }

    #[tokio::test]
    async fn a_shared_backend_without_its_dsn_env_fails_at_boot() {
        let config = BudgetConfig {
            backend: BudgetBackend::Redis,
            limit_microdollars: 1_000,
            dsn_env: Some("AXOND_TEST_MISSING_BUDGET_URL".to_owned()),
            ..BudgetConfig::default()
        };
        let err = build(&config, &HashMap::new(), 0, ungoverned())
            .await
            .err()
            .expect("a missing dsn must fail at boot");
        assert!(matches!(err, BudgetError::Invalid { .. }), "{err:?}");
    }

    /// The migration is one-way and only the cap-enabled configuration can serve
    /// what it produces, so running it from a config without the cap would leave
    /// the whole fleet unable to boot. It refuses before touching Redis.
    #[tokio::test]
    async fn the_redis_migration_needs_the_cap_it_migrates_to() {
        let config = BudgetConfig {
            backend: BudgetBackend::Redis,
            limit_microdollars: 1_000,
            dsn_env: Some("AXOND_TEST_MISSING_BUDGET_URL".to_owned()),
            ..BudgetConfig::default()
        };
        let err = migrate_redis(&config, &[], &HashMap::new())
            .await
            .expect_err("migrating without the cap must fail");
        assert!(
            format!("{err}").contains("namespace_limit_microdollars"),
            "the error must name the missing setting: {err}"
        );

        // Stateful deployments declare the same layout with `namespace_scope`,
        // because the cap itself is published rather than configured. The
        // migration has to accept the declaration the fleet will boot on, or the
        // documented procedure has no performable step.
        let stateful = BudgetConfig {
            namespace_scope: true,
            dsn_env: Some("AXOND_TEST_MISSING_BUDGET_URL".to_owned()),
            ..config
        };
        let err = migrate_redis(&stateful, &[], &HashMap::new())
            .await
            .expect_err("a stateful config declares no namespace to attribute keys to");
        assert!(
            !format!("{err}").contains("Declare it under"),
            "the layout is declared, so the refusal must not be about the cap: {err}"
        );
        assert!(
            format!("{err}").contains("--namespace"),
            "the refusal must name the flag that makes the procedure performable: {err}"
        );

        // With the list given, the layout gate is behind it and the next thing
        // that can fail is the connection — nothing was scanned in between.
        let err = migrate_redis(&stateful, &["acme".to_owned()], &HashMap::new())
            .await
            .expect_err("no DSN is set");
        assert!(
            format!("{err}").contains("AXOND_TEST_MISSING_BUDGET_URL"),
            "the refusal must be about the DSN: {err}"
        );
    }

    #[tokio::test]
    async fn the_default_backend_holds_nothing() {
        let store = build(&BudgetConfig::default(), &HashMap::new(), 0, ungoverned())
            .await
            .expect("the default backend needs no datastore");
        assert_eq!(store.name(), "none");
    }

    /// A shared store reads its caps per reserve, so a publication moves them
    /// without a restart — and the connection it reads them for is untouched.
    #[test]
    fn shared_settings_read_the_caps_the_runtime_is_publishing_now() {
        use std::sync::Arc;

        use crate::config::NamespacePolicy;
        use crate::desired_state::fixtures::{project_id, tenant_id};
        use crate::desired_state::policy::PolicyScope;
        use crate::policy::fixtures::{detailed, generation};
        use crate::policy::view::tests::governed;
        use crate::policy::{Ceilings, PolicyRuntime, PolicyView};

        let scope = PolicyScope::Project {
            tenant: tenant_id(1),
            project: project_id(1),
        };
        let published_body =
            |subject_limit, epoch| detailed(scope, epoch, subject_limit, None, 300, 8, 60, 0);
        let published = |subject_limit, epoch| {
            let body = published_body(subject_limit, epoch);
            governed(
                "acme/core",
                NamespacePolicy {
                    body: body.clone(),
                    generation: generation(&body, epoch),
                },
            )
        };
        let runtime = Arc::new(PolicyRuntime::bootstrap(&published(1_000, 1)));
        let settings = SharedSettings {
            ceilings: Ceilings::published(&runtime),
            namespace_scope: false,
            unavailable: UnavailablePolicy::Deny,
        };

        let first = settings
            .caps(crate::policy::ungoverned::BUDGET_REDIS, "acme/core")
            .expect("the namespace is governed");
        assert_eq!(first.caps.expect("governed").subject_microdollars, 1_000);
        assert_eq!(
            first.generation,
            Some(generation(&published_body(1_000, 1), 1))
        );
        runtime.install(PolicyView::of(&published(9_000, 2)));
        let second = settings
            .caps(crate::policy::ungoverned::BUDGET_REDIS, "acme/core")
            .expect("the namespace is governed");
        // The caps and the generation stamped on the hold come from one read, so
        // they always name each other.
        assert_eq!(second.caps.expect("governed").subject_microdollars, 9_000);
        assert_eq!(
            second.generation,
            Some(generation(&published_body(9_000, 2), 2))
        );

        // A namespace no document governs has no enforceable cap, and an
        // unenforced finite cap must never be served as an infinite one.
        assert!(
            settings
                .caps(crate::policy::ungoverned::BUDGET_REDIS, "unpublished")
                .is_none()
        );

        // Deliberately not `on_unavailable`'s decision: that stance answers
        // "the store is unreachable, admit anyway?", and here the store is fine
        // and the *limit* is what is missing.
        let fail_open = SharedSettings {
            unavailable: UnavailablePolicy::Allow,
            ..settings.clone()
        };
        assert!(
            fail_open
                .caps(crate::policy::ungoverned::BUDGET_REDIS, "unpublished")
                .is_none(),
            "a fail-open deployment still cannot admit against a cap nobody published"
        );

        // Nor may a document's scope-wide cap be enforced against keys that were
        // not laid out for one: that is a migration, not a value.
        let mismatched = SharedSettings {
            namespace_scope: true,
            ..settings
        };
        assert!(
            mismatched
                .caps(crate::policy::ungoverned::BUDGET_REDIS, "acme/core")
                .is_none()
        );

        let static_only = crate::policy::PolicyView::of(&crate::config::Config {
            namespace: vec![crate::config::Namespace {
                id: "static-only".to_owned(),
                default: true,
                allow_platform_fallback: false,
                project: None,
                policy: None,
                static_policy: Some(crate::config::NamespaceStaticPolicy::default()),
            }],
            ..crate::policy::view::tests::stateful_config()
        });
        let settings = SharedSettings {
            ceilings: Ceilings::fixed(static_only.policy("static-only")),
            namespace_scope: false,
            unavailable: UnavailablePolicy::Deny,
        };
        let governing = settings
            .caps(crate::policy::ungoverned::BUDGET_REDIS, "static-only")
            .expect("static-only explicitly bypasses exact budget enforcement");
        assert!(governing.caps.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn an_ambiguous_store_error_keeps_the_generation_for_the_reservation_ttl() {
        use std::sync::Arc;

        use crate::config::NamespacePolicy;
        use crate::desired_state::fixtures::{project_id, tenant_id};
        use crate::desired_state::policy::PolicyScope;
        use crate::policy::PolicyRuntime;
        use crate::policy::fixtures::{detailed, generation};
        use crate::policy::view::tests::governed;

        let scope = PolicyScope::Project {
            tenant: tenant_id(1),
            project: project_id(1),
        };
        let ttl_seconds = 300;
        let body = detailed(scope, 1, 1_000, None, ttl_seconds, 8, 60, 0);
        let held = generation(&body, 1);
        let runtime = Arc::new(PolicyRuntime::bootstrap(&governed(
            "acme/core",
            NamespacePolicy {
                body,
                generation: held,
            },
        )));
        let ceilings = Ceilings::published(&runtime);
        let reservation_ttl = Duration::from_secs(ttl_seconds);

        // A reserve whose answer was lost may have written its reservation
        // anyway, and no settlement can ever remove it: the id went with the
        // answer. Both stances therefore keep the generation counted for as
        // long as the store can hold that entry, rather than reporting a drain
        // complete while spend priced by the superseded document survives.
        for stance in [UnavailablePolicy::Deny, UnavailablePolicy::Allow] {
            let hold = PolicyHold::take(&ceilings, Some(held));
            assert_eq!(runtime.outstanding(held), 1);
            let admission = stance.admission(
                "redis",
                &"connection reset by peer",
                Some(Uncertain {
                    hold,
                    reservation_ttl,
                }),
            );
            match (&stance, &admission) {
                // Fail-open admits the request unenforced, so nothing downstream
                // will settle against the generation — the linger is the only
                // accounting left.
                (UnavailablePolicy::Allow, Admission::Allowed(reservation)) => {
                    assert!(reservation.id.is_empty());
                    assert_eq!(reservation.generation, None);
                }
                (UnavailablePolicy::Deny, Admission::Denied(Denial::StoreUnavailable)) => {}
                other => panic!("unexpected admission: {other:?}"),
            }

            // A bootstrap admission names no generation, so it is released here
            // rather than by a task that would sleep out the TTL to account for
            // nothing — which during an outage is one task per failed request.
            PolicyHold::take(&ceilings, None).linger(reservation_ttl);

            tokio::time::sleep(reservation_ttl - Duration::from_secs(1)).await;
            assert_eq!(
                runtime.outstanding(held),
                1,
                "{stance:?}: the generation stays counted while the store may still hold the \
                 reservation"
            );
            tokio::time::sleep(Duration::from_secs(2)).await;
            assert_eq!(
                runtime.outstanding(held),
                0,
                "{stance:?}: and is released once that reservation can only have expired"
            );
        }
    }

    #[tokio::test]
    async fn an_idle_unheld_ledger_is_evicted_at_capacity() {
        let budget = InMemoryBudget::with_limits(1_000, Duration::from_millis(1), 1);
        let first = key();
        let second = BudgetKey {
            namespace: "acme".into(),
            subject: "second".into(),
        };

        let reservation = budget.reserve(&first, 100).await;
        budget.settle(&first, reservation.reservation(), 100).await;
        tokio::time::sleep(Duration::from_millis(2)).await;

        assert!(matches!(
            budget.reserve(&second, 100).await,
            Admission::Allowed(_)
        ));
        assert_eq!(budget.ledger_state.lock().unwrap().ledgers.len(), 1);
        assert!(
            !budget
                .ledger_state
                .lock()
                .unwrap()
                .ledgers
                .contains_key(&first)
        );
    }

    #[tokio::test]
    async fn an_outstanding_reservation_survives_pruning_and_settlement() {
        let budget = InMemoryBudget::with_limits(1_000, Duration::from_millis(1), 1);
        let first = key();
        let second = BudgetKey {
            namespace: "acme".into(),
            subject: "second".into(),
        };

        let held = budget.reserve(&first, 500).await;
        tokio::time::sleep(Duration::from_millis(2)).await;
        assert_eq!(
            budget.reserve(&second, 100).await,
            Admission::Denied(Denial::StoreUnavailable)
        );

        budget.settle(&first, held.reservation(), 600).await;
        assert_eq!(
            budget.reserve(&first, 401).await,
            Admission::Denied(Denial::Exceeded)
        );
    }

    #[tokio::test]
    async fn an_expired_hold_is_reclaimed_and_its_ledger_can_be_evicted() {
        let budget = InMemoryBudget::with_settings(
            1_000,
            Duration::from_millis(1),
            Duration::from_millis(1),
            1,
        );
        let first = key();
        let second = BudgetKey {
            namespace: "acme".into(),
            subject: "second".into(),
        };

        budget.reserve(&first, 500).await;
        tokio::time::sleep(Duration::from_millis(2)).await;
        assert!(matches!(
            budget.reserve(&second, 100).await,
            Admission::Allowed(_)
        ));
        assert!(
            !budget
                .ledger_state
                .lock()
                .unwrap()
                .ledgers
                .contains_key(&first)
        );
    }

    #[tokio::test]
    async fn a_late_settlement_records_spend_after_ledger_eviction() {
        let budget = InMemoryBudget::with_settings(
            1_000,
            Duration::from_millis(1),
            Duration::from_millis(1),
            1,
        );
        let first = key();
        let second = BudgetKey {
            namespace: "acme".into(),
            subject: "second".into(),
        };

        let held = budget.reserve(&first, 100).await;
        tokio::time::sleep(Duration::from_millis(2)).await;
        let second_held = budget.reserve(&second, 100).await;
        budget.settle(&first, held.reservation(), 900).await;
        assert_eq!(
            budget.reserve(&first, 101).await,
            Admission::Denied(Denial::Exceeded)
        );
        let first_followup = budget.reserve(&first, 100).await;
        budget.release(&first, first_followup.reservation()).await;
        budget.release(&second, second_held.reservation()).await;
    }

    #[tokio::test]
    async fn an_active_ledger_keeps_its_spend_under_the_idle_ttl() {
        let budget = InMemoryBudget::with_limits(1_000, Duration::from_secs(60), 1);
        let first = key();
        let second = BudgetKey {
            namespace: "acme".into(),
            subject: "second".into(),
        };

        let reservation = budget.reserve(&first, 600).await;
        budget.settle(&first, reservation.reservation(), 600).await;
        assert_eq!(
            budget.reserve(&second, 100).await,
            Admission::Denied(Denial::StoreUnavailable)
        );
        assert_eq!(
            budget.reserve(&first, 401).await,
            Admission::Denied(Denial::Exceeded)
        );
    }

    #[tokio::test]
    async fn capacity_with_only_held_ledgers_denies_fail_closed() {
        let budget = InMemoryBudget::with_settings_and_policy(
            1_000,
            Duration::from_millis(1),
            Duration::from_secs(300),
            2,
            UnavailablePolicy::Deny,
            1,
        );
        let first = key();
        let second = BudgetKey {
            namespace: "acme".into(),
            subject: "second".into(),
        };
        let third = BudgetKey {
            namespace: "acme".into(),
            subject: "third".into(),
        };

        let first_hold = budget.reserve(&first, 100).await;
        let second_hold = budget.reserve(&second, 100).await;
        tokio::time::sleep(Duration::from_millis(2)).await;
        assert_eq!(
            budget.reserve(&third, 100).await,
            Admission::Denied(Denial::StoreUnavailable)
        );
        budget.release(&first, first_hold.reservation()).await;
        budget.release(&second, second_hold.reservation()).await;
    }

    #[tokio::test]
    async fn capacity_with_only_held_ledgers_can_fail_open_without_charging() {
        let budget = InMemoryBudget::with_settings_and_policy(
            1_000,
            Duration::from_millis(1),
            Duration::from_secs(300),
            1,
            UnavailablePolicy::Allow,
            1,
        );
        let first = key();
        let second = BudgetKey {
            namespace: "acme".into(),
            subject: "second".into(),
        };

        let first_hold = budget.reserve(&first, 100).await;
        let second_admission = budget.reserve(&second, 100).await;
        let second_hold = second_admission.reservation();
        assert!(second_hold.id.is_empty());
        budget.settle(&second, second_hold, 900).await;
        budget.release(&first, first_hold.reservation()).await;
        assert!(matches!(
            budget.reserve(&second, 1_000).await,
            Admission::Allowed(_)
        ));
    }

    #[tokio::test]
    async fn one_namespace_cannot_consume_another_namespaces_floor() {
        let budget = InMemoryBudget::with_namespace_count(
            1_000,
            Duration::from_secs(60),
            Duration::from_secs(300),
            4,
            2,
        );
        for subject in ["a-1", "a-2"] {
            let key = BudgetKey {
                namespace: "a".into(),
                subject: subject.into(),
            };
            let reservation = budget.reserve(&key, 1).await;
            budget.settle(&key, reservation.reservation(), 1).await;
        }

        let b = BudgetKey {
            namespace: "b".into(),
            subject: "never-seen".into(),
        };
        assert!(matches!(budget.reserve(&b, 1).await, Admission::Allowed(_)));
    }

    #[tokio::test]
    async fn post_reload_namespace_growth_does_not_lock_out_free_capacity() {
        let budget = InMemoryBudget::with_namespace_count(
            1_000,
            Duration::from_secs(60),
            Duration::from_secs(300),
            10,
            2,
        );
        {
            let mut state = budget.ledger_state.lock().unwrap();
            for namespace in ["a", "b", "c", "d"] {
                for subject in ["1", "2"] {
                    state.entry_or_default(&BudgetKey {
                        namespace: namespace.into(),
                        subject: subject.into(),
                    });
                }
            }
        }

        let admission = budget
            .reserve(
                &BudgetKey {
                    namespace: "a".into(),
                    subject: "new".into(),
                },
                1,
            )
            .await;
        assert!(matches!(admission, Admission::Allowed(_)));
        assert!(budget.ledger_state.lock().unwrap().ledgers.len() <= budget.max_subjects);
    }

    #[tokio::test]
    async fn nominal_namespace_count_preserves_full_capacity_isolation() {
        let budget = InMemoryBudget::with_namespace_count(
            1_000,
            Duration::from_secs(60),
            Duration::from_secs(300),
            4,
            2,
        );
        {
            let mut state = budget.ledger_state.lock().unwrap();
            for namespace in ["a", "b"] {
                for subject in ["1", "2"] {
                    state.entry_or_default(&BudgetKey {
                        namespace: namespace.into(),
                        subject: subject.into(),
                    });
                }
            }
        }

        assert_eq!(
            budget
                .reserve(
                    &BudgetKey {
                        namespace: "a".into(),
                        subject: "new".into(),
                    },
                    1,
                )
                .await,
            Admission::Denied(Denial::StoreUnavailable)
        );
    }

    #[tokio::test]
    async fn a_namespace_can_burst_into_unused_headroom() {
        let budget = InMemoryBudget::with_namespace_count(
            1_000,
            Duration::from_secs(60),
            Duration::from_secs(300),
            7,
            2,
        );
        for subject in ["a-1", "a-2", "a-3", "a-4"] {
            let key = BudgetKey {
                namespace: "a".into(),
                subject: subject.into(),
            };
            let reservation = budget.reserve(&key, 1).await;
            budget.settle(&key, reservation.reservation(), 1).await;
        }
        assert_eq!(budget.ledger_state.lock().unwrap().ledgers.len(), 4);
    }

    #[tokio::test]
    async fn full_capacity_evicts_only_the_requesting_namespace() {
        let budget = InMemoryBudget::with_namespace_count(
            1_000,
            Duration::from_millis(1),
            Duration::from_secs(300),
            4,
            2,
        );
        let first = BudgetKey {
            namespace: "a".into(),
            subject: "a-1".into(),
        };
        let reservation = budget.reserve(&first, 1).await;
        budget.settle(&first, reservation.reservation(), 1).await;
        tokio::time::sleep(Duration::from_millis(2)).await;

        for (namespace, subject) in [("a", "a-2"), ("b", "b-1"), ("b", "b-2")] {
            let key = BudgetKey {
                namespace: namespace.into(),
                subject: subject.into(),
            };
            let reservation = budget.reserve(&key, 1).await;
            budget.settle(&key, reservation.reservation(), 1).await;
        }

        let replacement = BudgetKey {
            namespace: "a".into(),
            subject: "a-new".into(),
        };
        assert!(matches!(
            budget.reserve(&replacement, 1).await,
            Admission::Allowed(_)
        ));
        let state = budget.ledger_state.lock().unwrap();
        assert_eq!(state.ledgers.len(), 4);
        assert_eq!(
            state.namespace_counts.get("a").copied().unwrap_or_default(),
            2
        );
        assert_eq!(
            state.namespace_counts.get("b").copied().unwrap_or_default(),
            2
        );
        assert!(state.ledgers.contains_key(&BudgetKey {
            namespace: "b".into(),
            subject: "b-1".into(),
        }));
        assert!(state.ledgers.contains_key(&BudgetKey {
            namespace: "b".into(),
            subject: "b-2".into(),
        }));
        assert!(!state.ledgers.contains_key(&BudgetKey {
            namespace: "a".into(),
            subject: "a-1".into(),
        }));
        assert!(state.ledgers.contains_key(&BudgetKey {
            namespace: "a".into(),
            subject: "a-2".into(),
        }));
    }

    #[tokio::test]
    async fn a_live_hold_is_never_evicted() {
        let budget = InMemoryBudget::with_namespace_count(
            1_000,
            Duration::from_millis(1),
            Duration::from_secs(300),
            2,
            2,
        );
        let first = BudgetKey {
            namespace: "a".into(),
            subject: "a-1".into(),
        };
        let second = BudgetKey {
            namespace: "b".into(),
            subject: "b-1".into(),
        };
        let first_hold = budget.reserve(&first, 1).await;
        let second_hold = budget.reserve(&second, 1).await;
        tokio::time::sleep(Duration::from_millis(2)).await;
        let replacement = BudgetKey {
            namespace: "a".into(),
            subject: "a-2".into(),
        };
        assert_eq!(
            budget.reserve(&replacement, 1).await,
            Admission::Denied(Denial::StoreUnavailable)
        );
        budget.release(&first, first_hold.reservation()).await;
        budget.release(&second, second_hold.reservation()).await;
    }

    #[tokio::test]
    async fn expired_holds_are_reclaimed_before_idle_eviction() {
        let budget = InMemoryBudget::with_namespace_count(
            1_000,
            Duration::from_millis(1),
            Duration::from_millis(1),
            2,
            2,
        );
        let first = BudgetKey {
            namespace: "a".into(),
            subject: "a-1".into(),
        };
        let second = BudgetKey {
            namespace: "b".into(),
            subject: "b-1".into(),
        };
        budget.reserve(&first, 1).await;
        let second_hold = budget.reserve(&second, 1).await;
        tokio::time::sleep(Duration::from_millis(3)).await;
        let replacement = BudgetKey {
            namespace: "a".into(),
            subject: "a-2".into(),
        };
        assert!(matches!(
            budget.reserve(&replacement, 1).await,
            Admission::Allowed(_)
        ));
        assert!(
            !budget
                .ledger_state
                .lock()
                .unwrap()
                .ledgers
                .contains_key(&first)
        );
        budget.release(&second, second_hold.reservation()).await;
    }

    #[tokio::test]
    async fn global_bound_holds_under_namespace_churn() {
        let max_subjects = 16;
        let budget = InMemoryBudget::with_namespace_count(
            1_000_000,
            Duration::from_nanos(1),
            Duration::from_secs(300),
            max_subjects,
            2,
        );
        for index in 0..1_000 {
            let key = BudgetKey {
                namespace: if index % 2 == 0 { "a" } else { "b" }.into(),
                subject: format!("subject-{index}"),
            };
            let admission = budget.reserve(&key, 1).await;
            if let Admission::Allowed(reservation) = admission {
                budget.settle(&key, &reservation, 1).await;
            }
            assert!(budget.ledger_state.lock().unwrap().ledgers.len() <= max_subjects);
        }
    }

    #[tokio::test]
    async fn zero_namespaces_keep_global_capacity_behavior() {
        let budget = InMemoryBudget::with_namespace_count(
            1_000,
            Duration::from_secs(60),
            Duration::from_secs(300),
            1,
            0,
        );
        let first = BudgetKey {
            namespace: "a".into(),
            subject: "a-1".into(),
        };
        let second = BudgetKey {
            namespace: "b".into(),
            subject: "b-1".into(),
        };
        let hold = budget.reserve(&first, 1).await;
        assert_eq!(
            budget.reserve(&second, 1).await,
            Admission::Denied(Denial::StoreUnavailable)
        );
        budget.release(&first, hold.reservation()).await;
    }

    #[tokio::test]
    async fn too_few_subjects_for_namespaces_keep_global_capacity_behavior() {
        let budget = InMemoryBudget::with_namespace_count(
            1_000,
            Duration::from_secs(60),
            Duration::from_secs(300),
            1,
            2,
        );
        let first = BudgetKey {
            namespace: "a".into(),
            subject: "a-1".into(),
        };
        let second = BudgetKey {
            namespace: "b".into(),
            subject: "b-1".into(),
        };
        let hold = budget.reserve(&first, 1).await;
        assert_eq!(
            budget.reserve(&second, 1).await,
            Admission::Denied(Denial::StoreUnavailable)
        );
        budget.release(&first, hold.reservation()).await;
    }

    #[tokio::test]
    async fn many_subjects_keep_the_in_memory_ledger_bounded() {
        let max_subjects = 64;
        let budget = InMemoryBudget::with_limits(1_000_000, Duration::from_nanos(1), max_subjects);

        for index in 0..5_000 {
            if index >= max_subjects {
                std::thread::sleep(Duration::from_micros(1));
            }
            let key = BudgetKey {
                namespace: "acme".into(),
                subject: format!("subject-{index}"),
            };
            let reservation = budget.reserve(&key, 1).await.reservation().clone();
            budget.settle(&key, &reservation, 1).await;
        }

        assert!(budget.ledger_state.lock().unwrap().ledgers.len() <= max_subjects);
    }
}
