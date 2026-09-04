//! Budget ledger on the required [`Store`] (ADR 0063 / ADR 0064).
//!
//! Caps are per `(namespace, period)`. Admission is a spent-vs-limit read;
//! actuals are charged after the response. Unavailability follows
//! `[storage].on_unavailable`.

use std::sync::Arc;

use async_trait::async_trait;

use super::{Admission, BudgetKey, BudgetStore, Denial, Reservation, UnavailablePolicy};
use crate::backends::health::BackendHealth;
use crate::config::StoreUnavailable;
use crate::store::{BudgetAdmit, Store, StoreError};

pub struct StoreBudget {
    store: Arc<dyn Store>,
    unavailable: UnavailablePolicy,
}

impl StoreBudget {
    pub fn new(store: Arc<dyn Store>, unavailable: StoreUnavailable) -> Self {
        Self {
            store,
            unavailable: unavailable.into(),
        }
    }

    fn on_unavailable(&self, error: &StoreError) -> Admission {
        self.unavailable.admission("store", error, None)
    }
}

#[async_trait]
impl BudgetStore for StoreBudget {
    fn name(&self) -> &'static str {
        "store"
    }

    fn health(&self) -> Option<Arc<dyn BackendHealth>> {
        self.store.health()
    }

    async fn reserve(&self, key: &BudgetKey, _estimated_microdollars: u64) -> Admission {
        match self.store.admit_budget(&key.namespace).await {
            Ok(BudgetAdmit::Allowed {
                period,
                incarnation,
            }) => Admission::Allowed(Reservation {
                id: Reservation::next_id(),
                estimate_microdollars: 0,
                generation: None,
                period: Some(period),
                incarnation: Some(incarnation),
            }),
            Ok(BudgetAdmit::Exceeded) => Admission::Denied(Denial::Exceeded),
            Err(StoreError::Invalid(_)) => Admission::Denied(Denial::Exceeded),
            Err(error) => self.on_unavailable(&error),
        }
    }

    async fn settle(&self, key: &BudgetKey, reservation: &Reservation, actual_microdollars: u64) {
        let (Some(period), Some(incarnation)) =
            (reservation.period.as_deref(), reservation.incarnation)
        else {
            return;
        };
        if let Err(error) = self
            .store
            .charge_budget(&key.namespace, period, incarnation, actual_microdollars)
            .await
        {
            tracing::error!(
                backend = "store",
                namespace = %key.namespace,
                period,
                error = %error,
                "budget charge failed; spend is not recorded for this request"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::UnavailableStore;

    fn key() -> BudgetKey {
        BudgetKey {
            namespace: "wsp_x".into(),
            subject: "GW_INBOUND_KEY".into(),
        }
    }

    #[tokio::test]
    async fn deny_is_budget_unavailable() {
        let budget = StoreBudget::new(Arc::new(UnavailableStore), StoreUnavailable::Deny);
        assert!(matches!(
            budget.reserve(&key(), 1).await,
            Admission::Denied(Denial::StoreUnavailable)
        ));
    }

    #[tokio::test]
    async fn zero_limit_is_exceeded_even_when_unavailable_is_allow() {
        let sqlite = crate::store::SqliteStore::open(":memory:").expect("sqlite");
        sqlite
            .put_namespace(crate::store::NamespaceRecord {
                id: "wsp_x".into(),
                attrs: serde_json::json!({}),
                blocklist: None,
            })
            .await
            .expect("ns");
        sqlite.put_budget("wsp_x", "p", 0).await.expect("budget");
        let store: Arc<dyn Store> = Arc::new(sqlite);
        for stance in [StoreUnavailable::Allow, StoreUnavailable::Deny] {
            let budget = StoreBudget::new(Arc::clone(&store), stance);
            assert!(
                matches!(
                    budget.reserve(&key(), 1).await,
                    Admission::Denied(Denial::Exceeded)
                ),
                "{stance:?}"
            );
        }
    }

    #[tokio::test]
    async fn allow_serves_without_a_hold() {
        let budget = StoreBudget::new(Arc::new(UnavailableStore), StoreUnavailable::Allow);
        let admission = budget.reserve(&key(), 1).await;
        match admission {
            Admission::Allowed(reservation) => {
                assert!(reservation.id.is_empty());
                assert!(reservation.period.is_none());
            }
            other => panic!("{other:?}"),
        }
    }
}
