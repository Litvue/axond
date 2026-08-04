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

mod postgres;
mod redis;

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;

use crate::config::{BudgetBackend, BudgetConfig, StoreUnavailable};

pub use postgres::PostgresBudget;
pub use redis::RedisBudget;

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
}

impl Reservation {
    /// The reservation a store that holds nothing hands back.
    pub fn unheld() -> Self {
        Self {
            id: String::new(),
            estimate_microdollars: 0,
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
    /// Called exactly once per admitted request, whatever its outcome.
    async fn settle(&self, key: &BudgetKey, reservation: &Reservation, actual_microdollars: u64);
    /// Drop a reservation that consumed nothing. Settling zero is the same
    /// operation, and every backend implements it that way.
    async fn release(&self, key: &BudgetKey, reservation: &Reservation) {
        self.settle(key, reservation, 0).await;
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
    ledgers: Mutex<HashMap<BudgetKey, Ledger>>,
}

#[derive(Default)]
struct Ledger {
    spent: u64,
    held: HashMap<String, u64>,
}

impl Ledger {
    fn outstanding(&self) -> u64 {
        self.held
            .values()
            .fold(0, |sum, held| sum.saturating_add(*held))
    }
}

impl InMemoryBudget {
    pub fn new(limit_microdollars: u64) -> Self {
        Self {
            limit_microdollars,
            ledgers: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl BudgetStore for InMemoryBudget {
    fn name(&self) -> &'static str {
        "in_memory"
    }

    async fn reserve(&self, key: &BudgetKey, estimated_microdollars: u64) -> Admission {
        let mut ledgers = self.ledgers.lock().unwrap_or_else(|e| e.into_inner());
        let ledger = ledgers.entry(key.clone()).or_default();
        let committed = ledger.spent.saturating_add(ledger.outstanding());
        if committed.saturating_add(estimated_microdollars) > self.limit_microdollars {
            return Admission::Denied(Denial::Exceeded);
        }
        let reservation = Reservation {
            id: Reservation::next_id(),
            estimate_microdollars: estimated_microdollars,
        };
        ledger
            .held
            .insert(reservation.id.clone(), estimated_microdollars);
        Admission::Allowed(reservation)
    }

    async fn settle(&self, key: &BudgetKey, reservation: &Reservation, actual_microdollars: u64) {
        let mut ledgers = self.ledgers.lock().unwrap_or_else(|e| e.into_inner());
        let ledger = ledgers.entry(key.clone()).or_default();
        ledger.held.remove(&reservation.id);
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

impl UnavailablePolicy {
    /// The admission for a reservation the store could not answer.
    fn admission(self, backend: &'static str, error: &dyn std::fmt::Display) -> Admission {
        match self {
            Self::Deny => {
                tracing::error!(
                    backend,
                    error = %error,
                    "budget store is unreachable; denying (fail-closed)"
                );
                Admission::Denied(Denial::StoreUnavailable)
            }
            Self::Allow => {
                tracing::warn!(
                    backend,
                    error = %error,
                    "budget store is unreachable; admitting unenforced (fail-open)"
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

/// Settings shared by both durable backends: the cap they enforce, how long a
/// hold survives a replica that dies mid-request, and the stance to take when
/// the store cannot be reached.
#[derive(Debug, Clone, Copy)]
pub struct SharedSettings {
    pub limit_microdollars: u64,
    pub reservation_ttl: Duration,
    pub unavailable: UnavailablePolicy,
}

/// Build the configured backend. Connecting here means a misconfigured budget
/// store refuses to boot rather than failing (or silently not enforcing) at
/// request time — the same posture as usage sinks.
pub async fn build(
    config: &BudgetConfig,
    env: &HashMap<String, String>,
) -> Result<Box<dyn BudgetStore>, BudgetError> {
    let settings = SharedSettings {
        limit_microdollars: config.limit_microdollars,
        reservation_ttl: Duration::from_secs(config.reservation_ttl_seconds),
        unavailable: config.on_unavailable.into(),
    };
    match config.backend {
        BudgetBackend::None => Ok(Box::new(NoBudget)),
        BudgetBackend::InMemory => Ok(Box::new(InMemoryBudget::new(config.limit_microdollars))),
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
            UnavailablePolicy::Deny.admission("redis", &error),
            Admission::Denied(Denial::StoreUnavailable)
        );
        assert!(matches!(
            UnavailablePolicy::Allow.admission("redis", &error),
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
        let err = build(&config, &HashMap::new())
            .await
            .err()
            .expect("a missing dsn must fail at boot");
        assert!(matches!(err, BudgetError::Invalid { .. }), "{err:?}");
    }

    #[tokio::test]
    async fn the_default_backend_holds_nothing() {
        let store = build(&BudgetConfig::default(), &HashMap::new())
            .await
            .expect("the default backend needs no datastore");
        assert_eq!(store.name(), "none");
    }
}
