//! Budget ledger on the required [`Store`] (ADR 0063).
//!
//! Caps are per `(namespace, period)`. The active period is the last successful
//! PUT; inference does not carry a period. Unavailability follows
//! `[storage].on_unavailable`.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use super::{Admission, BudgetKey, BudgetStore, Denial, Reservation, UnavailablePolicy};
use crate::config::StoreUnavailable;
use crate::store::{BudgetReserve, Store, StoreError};

pub struct StoreBudget {
    store: Arc<dyn Store>,
    unavailable: UnavailablePolicy,
    reservation_ttl: Duration,
}

impl StoreBudget {
    pub fn new(
        store: Arc<dyn Store>,
        unavailable: StoreUnavailable,
        reservation_ttl: Duration,
    ) -> Self {
        Self {
            store,
            unavailable: unavailable.into(),
            reservation_ttl: reservation_ttl.max(Duration::from_secs(1)),
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

    async fn reserve(&self, key: &BudgetKey, estimated_microdollars: u64) -> Admission {
        let id = Reservation::next_id();
        match self
            .store
            .reserve_budget(
                &key.namespace,
                estimated_microdollars,
                self.reservation_ttl,
                &id,
            )
            .await
        {
            Ok(BudgetReserve::Allowed { period }) => Admission::Allowed(Reservation {
                id,
                estimate_microdollars: estimated_microdollars,
                generation: None,
                period: Some(period),
            }),
            Ok(BudgetReserve::Exceeded) => Admission::Denied(Denial::Exceeded),
            Err(error) => self.on_unavailable(&error),
        }
    }

    async fn settle(&self, key: &BudgetKey, reservation: &Reservation, actual_microdollars: u64) {
        if reservation.id.is_empty() {
            return;
        }
        let Some(period) = reservation.period.as_deref() else {
            return;
        };
        if let Err(error) = self
            .store
            .settle_budget(&key.namespace, period, &reservation.id, actual_microdollars)
            .await
        {
            tracing::error!(
                backend = "store",
                namespace = %key.namespace,
                period,
                error = %error,
                "budget settlement failed; leaving the hold to expire"
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
        let budget = StoreBudget::new(
            Arc::new(UnavailableStore),
            StoreUnavailable::Deny,
            Duration::from_secs(30),
        );
        assert!(matches!(
            budget.reserve(&key(), 1).await,
            Admission::Denied(Denial::StoreUnavailable)
        ));
    }

    #[tokio::test]
    async fn allow_serves_without_a_hold() {
        let budget = StoreBudget::new(
            Arc::new(UnavailableStore),
            StoreUnavailable::Allow,
            Duration::from_secs(30),
        );
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
