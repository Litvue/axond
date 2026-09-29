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
//! Actual cost is unknown until a response completes. The Store ledger (ADR
//! 0064) **does not hold an estimate** before dispatch: [`BudgetStore::reserve`]
//! is a spent-vs-limit read, and [`BudgetStore::settle`] adds measured spend
//! afterwards. Concurrent in-flight requests can overshoot the cap.
//!
//! One backend ships: [`StoreBudget`], the ledger in the required Store (ADR
//! 0063). When the Store is unreachable the default stance is **fail-closed**:
//! admission is denied rather than silently unenforced.

mod store_budget;

use std::sync::Arc;

use async_trait::async_trait;

use crate::config::StoreUnavailable;
use crate::store::BudgetAdmit;

pub use store_budget::StoreBudget;

/// The dimension a budget is scoped to. Neutral vocabulary, like usage records.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BudgetKey {
    pub namespace: String,
    pub subject: String,
}

/// Admit outcome: the handle [`BudgetStore::settle`] uses to charge measured
/// spend. Cheap to clone so the streaming relay can carry it to a detached
/// settlement. The Store path does not write a hold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reservation {
    /// Unique per admit on backends that still hold an estimate. Unused on
    /// the Store path (charge keys off period + incarnation).
    pub id: String,
    pub estimate_microdollars: u64,
    /// Active period captured at admit. Set by the Store ledger; absent on
    /// unheld / legacy backends.
    pub period: Option<String>,
    /// Namespace incarnation captured at admit. Charge is a no-op if it no
    /// longer matches (delete + recreate).
    pub incarnation: Option<i64>,
}

impl Reservation {
    /// The reservation a store that holds nothing hands back.
    pub fn unheld() -> Self {
        Self {
            id: String::new(),
            estimate_microdollars: 0,
            period: None,
            incarnation: None,
        }
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

impl Admission {}

/// Map a Store spent-vs-limit read onto the request-path admission type.
pub(crate) fn admission_from_store(admit: BudgetAdmit) -> Admission {
    match admit {
        BudgetAdmit::Allowed {
            period,
            incarnation,
        } => Admission::Allowed(Reservation {
            period: Some(period),
            incarnation: Some(incarnation),
            ..Reservation::unheld()
        }),
        BudgetAdmit::Exceeded => Admission::Denied(Denial::Exceeded),
    }
}

#[async_trait]
pub trait BudgetStore: Send + Sync {
    fn name(&self) -> &'static str;
    /// Pre-dispatch spent-vs-limit check. The Store path does not write a hold.
    async fn reserve(&self, key: &BudgetKey, estimated_microdollars: u64) -> Admission;
    /// Charge measured spend, in micro-dollars.
    ///
    /// **Exactly once per admitted request**, whatever its outcome — completion,
    /// upstream failure, client cancellation, or a dropped handler. The route
    /// guarantees it: the guard is disarmed before the call, so a settlement and
    /// its drop-path fallback cannot both run, and no caller retries. The Store
    /// path adds `actual` to `spent` when period and incarnation still match. A
    /// charge the store rejects is not retried and under-records that request.
    async fn settle(&self, key: &BudgetKey, reservation: &Reservation, actual_microdollars: u64);
    /// Drop a reservation that consumed nothing. Settling zero is the same
    /// operation, and every backend implements it that way.
    async fn release(&self, key: &BudgetKey, reservation: &Reservation) {
        self.settle(key, reservation, 0).await;
    }

    /// Same `Store` the namespace lookup uses, when this ledger is that Store.
    /// The request path then reuses a preloaded admit instead of a second RTT.
    fn store_ledger(&self) -> Option<&Arc<dyn crate::store::Store>> {
        None
    }
}

/// Always-allow, for tests that exercise nothing budget-related.
#[cfg(test)]
pub struct NoBudget;

#[cfg(test)]
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
    pub(crate) fn admission(
        self,
        backend: &'static str,
        error: &dyn std::fmt::Display,
    ) -> Admission {
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
