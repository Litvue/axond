//! Scheduling for effective-dated pricing snapshots.
//!
//! A compiled [`PricingSnapshot`](crate::desired_state::pricing::PricingSnapshot)
//! already records the first instant at which its resolution may change. This
//! module turns that value into a control-plane timer. It deliberately does not
//! resolve prices: the reconciler wakes at the boundary and runs the ordinary
//! compile/admit/publish path, so requests continue to read one immutable
//! snapshot and never poll a clock or a store.

use std::time::{Duration, SystemTime};

use crate::desired_state::pricing::{EffectiveInstant, InvalidInstant};

/// The next effective-dating boundary a reconciler must wake for.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PricingSchedule {
    boundary: Option<EffectiveInstant>,
}

impl PricingSchedule {
    /// A schedule with no future pricing boundary.
    pub const fn empty() -> Self {
        Self { boundary: None }
    }

    /// Record the boundary for the snapshot just published.
    pub const fn set(&mut self, boundary: Option<EffectiveInstant>) {
        self.boundary = boundary;
    }

    /// The boundary currently being waited for, if any.
    pub const fn boundary(self) -> Option<EffectiveInstant> {
        self.boundary
    }

    /// Whether the wall clock has reached the boundary.
    pub fn due_at(self, now: SystemTime) -> Result<bool, PricingScheduleError> {
        let Some(boundary) = self.boundary else {
            return Ok(false);
        };
        Ok(EffectiveInstant::of(now)? >= boundary)
    }

    /// How long a monotonic timer should wait before checking the boundary.
    ///
    /// The timer is only a wake-up hint. The reconciler calls [`Self::due_at`]
    /// after it wakes as well, which makes a wall clock moving backwards safe:
    /// a timer that was calculated before the move cannot activate a rate early.
    /// A clock moving forwards makes the boundary immediately due, and the
    /// compiler resolves the book against that current instant.
    pub fn delay_at(self, now: SystemTime) -> Result<Option<Duration>, PricingScheduleError> {
        let Some(boundary) = self.boundary else {
            return Ok(None);
        };
        let now = EffectiveInstant::of(now)?;
        let boundary = boundary
            .to_system_time()
            .ok_or(PricingScheduleError::BoundaryUnrepresentable { boundary })?;
        Ok(Some(
            boundary
                .duration_since(
                    now.to_system_time()
                        .ok_or(PricingScheduleError::ClockUnrepresentable { now })?,
                )
                .unwrap_or(Duration::ZERO),
        ))
    }
}

/// Why an effective-dating timer could not be derived from the host clock.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PricingScheduleError {
    #[error("the host clock cannot be represented on the effective-dating timeline: {0}")]
    Clock(#[from] InvalidInstant),
    #[error("the scheduled pricing boundary {boundary} cannot be represented as a wall-clock time")]
    BoundaryUnrepresentable { boundary: EffectiveInstant },
    #[error("the current wall-clock instant {now} cannot be represented as a wall-clock time")]
    ClockUnrepresentable { now: EffectiveInstant },
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, UNIX_EPOCH};

    fn at(millis: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_millis(millis)
    }

    #[test]
    fn a_boundary_is_due_at_exactly_its_instant() {
        let schedule = PricingSchedule {
            boundary: Some(EffectiveInstant::from_millis(1_000)),
        };
        assert!(!schedule.due_at(at(999)).expect("clock is valid"));
        assert!(schedule.due_at(at(1_000)).expect("clock is valid"));
        assert!(schedule.due_at(at(1_001)).expect("clock is valid"));
        assert_eq!(
            schedule.delay_at(at(999)),
            Ok(Some(Duration::from_millis(1)))
        );
        assert_eq!(schedule.delay_at(at(1_000)), Ok(Some(Duration::ZERO)));
    }

    #[test]
    fn a_clock_moving_backwards_never_activates_a_boundary_early() {
        let schedule = PricingSchedule {
            boundary: Some(EffectiveInstant::from_millis(1_000)),
        };
        assert!(!schedule.due_at(at(900)).expect("clock is valid"));
        assert_eq!(
            schedule.delay_at(at(900)),
            Ok(Some(Duration::from_millis(100)))
        );
    }

    #[test]
    fn a_clock_before_the_epoch_is_reported_not_clamped() {
        let schedule = PricingSchedule {
            boundary: Some(EffectiveInstant::from_millis(1_000)),
        };
        let error = schedule
            .due_at(UNIX_EPOCH - Duration::from_millis(1))
            .expect_err("a pre-epoch clock is not a valid pricing instant");
        assert!(matches!(error, PricingScheduleError::Clock(_)));
    }

    #[test]
    fn an_empty_schedule_has_no_timer() {
        let schedule = PricingSchedule::empty();
        assert_eq!(schedule.delay_at(at(1_000)), Ok(None));
        assert!(!schedule.due_at(at(1_000)).expect("empty is valid"));
    }
}
