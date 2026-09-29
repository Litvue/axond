//! The request's core accounting owner: its budget hold and the settlement
//! capacity it reserved at admission.
//!
//! One value follows a request from admission into buffered completion or
//! streaming accounting, wherever its single settlement is spawned from. The
//! Store ledger admits on spent-vs-limit and charges measured spend after the
//! response (ADR 0064).

use crate::budget::{Admission, BudgetKey, Denial, Reservation, admission_from_store};
use crate::error::GatewayError;
use crate::settlement::{SettlementReservation, SettlementSlot};
use crate::state::AppState;
use crate::store::BudgetAdmit;

#[derive(Default)]
pub struct CoreAccounting {
    core_budget: Option<CoreBudgetHold>,
    /// The settlement capacity the request reserved at admission. Shared with
    /// [`CoreBudgetHold`] so cancellation consumes this slot instead of
    /// `try_spawn`ing a second one.
    settlement: SettlementSlot,
}

impl CoreAccounting {
    /// Hold the settlement capacity admission reserved for this request until
    /// the path that settles it takes it back with [`Self::take_settlement`].
    pub(crate) fn reserve_settlement(&mut self, reservation: SettlementReservation) {
        self.settlement.insert(reservation);
    }

    /// The reserved settlement capacity, for the one settlement this request
    /// spawns. `None` once taken, or when nothing was reserved.
    pub(crate) fn take_settlement(&mut self) -> Option<SettlementReservation> {
        self.settlement.take()
    }

    /// Admit on spent-vs-limit. An armed hold charges on drop unless an outcome
    /// settles it first.
    pub(crate) async fn reserve_budget(
        &mut self,
        state: &AppState,
        key: BudgetKey,
        estimated_microdollars: u64,
        estimated_input_tokens: u64,
        alias: &str,
        preloaded: Option<BudgetAdmit>,
    ) -> Result<(), GatewayError> {
        debug_assert!(self.core_budget.is_none());
        let admission = match preloaded {
            Some(admit) => admission_from_store(admit),
            None => state.0.budget.reserve(&key, estimated_microdollars).await,
        };
        let reservation = match admission {
            Admission::Allowed(reservation) => reservation,
            Admission::Denied(Denial::Exceeded) => {
                return Err(GatewayError::BudgetExceeded(alias.to_owned()));
            }
            Admission::Denied(Denial::StoreUnavailable) => {
                return Err(GatewayError::BudgetUnavailable);
            }
        };
        self.core_budget = Some(CoreBudgetHold {
            state: state.clone(),
            key,
            reservation: Some(reservation),
            estimated_input_tokens,
            settlement: self.settlement.clone(),
        });
        Ok(())
    }

    pub(crate) fn core_budget_context(&self) -> Option<(&BudgetKey, &Reservation, u64)> {
        self.core_budget.as_ref().map(|hold| {
            (
                &hold.key,
                hold.reservation
                    .as_ref()
                    .expect("core budget hold is armed"),
                hold.estimated_input_tokens,
            )
        })
    }

    pub(crate) fn take_core_budget(&mut self) -> Option<CoreBudgetHold> {
        self.core_budget.take()
    }

    pub(crate) async fn release_core_budget(&mut self) -> bool {
        let Some(hold) = self.core_budget.take() else {
            return false;
        };
        hold.release().await;
        true
    }
}

/// The admitted budget hold. An armed hold charges zero on drop unless an
/// outcome settles it first, so a cancelled request still releases its hold.
pub(crate) struct CoreBudgetHold {
    state: AppState,
    key: BudgetKey,
    reservation: Option<Reservation>,
    estimated_input_tokens: u64,
    /// Same slot as [`CoreAccounting::settlement`]: Drop consumes it so a
    /// zero-charge release is spawned under the request's own reservation.
    settlement: SettlementSlot,
}

impl CoreBudgetHold {
    pub(crate) async fn settle(mut self, actual_microdollars: u64) {
        let reservation = self
            .reservation
            .take()
            .expect("core budget hold must be armed");
        self.state
            .0
            .budget
            .settle(&self.key, &reservation, actual_microdollars)
            .await;
    }

    pub(crate) async fn release(mut self) {
        let reservation = self
            .reservation
            .take()
            .expect("core budget hold must be armed");
        self.state.0.budget.release(&self.key, &reservation).await;
    }
}

impl Drop for CoreBudgetHold {
    fn drop(&mut self) {
        let Some(reservation) = self.reservation.take() else {
            return;
        };
        let state = self.state.clone();
        let key = self.key.clone();
        let settlements = self.state.0.settlements.clone();
        // Consume the request's admission reservation rather than try_spawn:
        // at capacity that slot is why a second reserve would refuse, and a
        // skipped release would leave the hold until TTL.
        settlements.spawn_reserved(
            self.settlement.take(),
            async move {
                state.0.budget.release(&key, &reservation).await;
            },
            "zero-charge budget release",
        );
    }
}
