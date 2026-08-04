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
//! **reserve-then-reconcile**: `reserve` a conservative cost estimate
//! pre-dispatch, `commit` the real cost after. Concurrent in-flight requests
//! can overshoot, so caps are *soft* unless a backend implements hard
//! reservation.
//!
//! Default backend is in-memory (no datastore). Redis/Postgres backends for
//! cross-replica exact accounting are opt-in follow-ups.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;

/// The dimension a budget is scoped to. Neutral vocabulary, like usage records.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BudgetKey {
    pub namespace: String,
    pub subject: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    Allowed,
    Denied,
}

#[async_trait]
pub trait BudgetStore: Send + Sync {
    #[allow(dead_code)] // surfaced in logs/metrics once telemetry lands
    fn name(&self) -> &'static str;
    /// Pre-dispatch check + conservative reservation, in micro-dollars.
    async fn reserve(&self, key: &BudgetKey, estimated_microdollars: u64) -> Admission;
    /// Post-response reconciliation with the actual cost, in micro-dollars.
    async fn commit(&self, key: &BudgetKey, actual_microdollars: u64);
}

/// Always-allow. The default posture when no budget is configured.
pub struct NoBudget;

#[async_trait]
impl BudgetStore for NoBudget {
    fn name(&self) -> &'static str {
        "none"
    }
    async fn reserve(&self, _key: &BudgetKey, _estimated_microdollars: u64) -> Admission {
        Admission::Allowed
    }
    async fn commit(&self, _key: &BudgetKey, _actual_microdollars: u64) {}
}

/// Per-replica in-memory spend counter (micro-dollars). No datastore; not
/// shared across replicas (a fleet enforces per-replica ceilings — documented,
/// not hidden). Not yet wired into `main` (config-selected backend is a
/// follow-up).
#[allow(dead_code)]
pub struct InMemoryBudget {
    limit_microdollars: u64,
    spent: Mutex<HashMap<BudgetKey, u64>>,
}

#[allow(dead_code)]
impl InMemoryBudget {
    pub fn new(limit_microdollars: u64) -> Self {
        Self {
            limit_microdollars,
            spent: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl BudgetStore for InMemoryBudget {
    fn name(&self) -> &'static str {
        "in_memory"
    }

    async fn reserve(&self, key: &BudgetKey, estimated_microdollars: u64) -> Admission {
        // Peek only: admitted spend accrues on `commit` with the real cost.
        // Nothing is held between reserve and commit, so concurrent in-flight
        // requests can overshoot — caps are soft, as documented.
        let spent = self.spent.lock().unwrap_or_else(|e| e.into_inner());
        let current = *spent.get(key).unwrap_or(&0);
        if current.saturating_add(estimated_microdollars) > self.limit_microdollars {
            Admission::Denied
        } else {
            Admission::Allowed
        }
    }

    async fn commit(&self, key: &BudgetKey, actual_microdollars: u64) {
        let mut spent = self.spent.lock().unwrap_or_else(|e| e.into_inner());
        let entry = spent.entry(key.clone()).or_insert(0);
        *entry = entry.saturating_add(actual_microdollars);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> BudgetKey {
        BudgetKey {
            namespace: "acme".into(),
            subject: "GW_INBOUND_ACME_KEY".into(),
        }
    }

    #[tokio::test]
    async fn admits_until_committed_spend_would_exceed_the_cap() {
        let budget = InMemoryBudget::new(1_000); // 1,000 micro-dollars
        let k = key();

        assert_eq!(budget.reserve(&k, 400).await, Admission::Allowed);
        budget.commit(&k, 400).await;
        assert_eq!(budget.reserve(&k, 400).await, Admission::Allowed);
        budget.commit(&k, 400).await; // 800 spent

        // A request estimated to cost 300 would push spend to 1,100 > 1,000.
        assert_eq!(budget.reserve(&k, 300).await, Admission::Denied);
        // A cheaper request still fits.
        assert_eq!(budget.reserve(&k, 100).await, Admission::Allowed);
    }

    #[tokio::test]
    async fn no_budget_always_admits() {
        let budget = NoBudget;
        assert_eq!(budget.reserve(&key(), u64::MAX).await, Admission::Allowed);
    }
}
