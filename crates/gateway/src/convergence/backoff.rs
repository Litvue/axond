//! Bounded retry pacing for convergence.
//!
//! A reconciler that retried immediately would turn a control-plane outage into a
//! second incident: N replicas polling a struggling Postgres as fast as it can
//! refuse them. A reconciler that retried forever at a fixed long interval would
//! make a transient blip cost a full interval of staleness. So the delay is
//! exponential from a short first retry up to a hard ceiling, and it is reset the
//! moment an attempt succeeds.
//!
//! Two deliberate omissions:
//!
//! - **No jitter.** The delay sequence is a pure function of the failure count,
//!   which is what lets a test assert the sequence exactly instead of asserting a
//!   range. Herd avoidance comes from replicas polling on independent schedules
//!   (they boot at different times), not from randomizing a retry.
//! - **No attempt limit.** Convergence has nowhere to give up *to*: a replica
//!   that stopped retrying would serve its old snapshot forever while reporting
//!   nothing wrong. Instead the delay saturates at [`BackoffPolicy::max`], the
//!   failure count keeps rising, and the failure count is what gets reported
//!   (see [`super::status`]).

use std::time::Duration;

/// How fast retries back off, and how far.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackoffPolicy {
    /// The delay after the first consecutive failure.
    pub initial: Duration,
    /// The ceiling the delay saturates at, however long the outage lasts.
    pub max: Duration,
    /// The factor applied per consecutive failure.
    pub multiplier: u32,
}

impl Default for BackoffPolicy {
    /// Fast enough that a blip costs a fraction of a second, slow enough that a
    /// long outage settles into one attempt per ceiling per replica.
    fn default() -> Self {
        Self {
            initial: Duration::from_millis(500),
            max: Duration::from_secs(30),
            multiplier: 2,
        }
    }
}

/// Why a backoff policy cannot be used.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidBackoff {
    #[error("backoff.initial must be greater than zero")]
    ZeroInitial,
    #[error("backoff.multiplier must be at least 2; a lower factor never backs off")]
    Multiplier { multiplier: u32 },
    #[error("backoff.max ({max:?}) must be at least backoff.initial ({initial:?})")]
    Ceiling { initial: Duration, max: Duration },
}

impl BackoffPolicy {
    pub const fn validate(&self) -> Result<(), InvalidBackoff> {
        if self.initial.is_zero() {
            return Err(InvalidBackoff::ZeroInitial);
        }
        if self.multiplier < 2 {
            return Err(InvalidBackoff::Multiplier {
                multiplier: self.multiplier,
            });
        }
        // `Duration` comparison is not const, so compare the parts that matter.
        if self.max.as_nanos() < self.initial.as_nanos() {
            return Err(InvalidBackoff::Ceiling {
                initial: self.initial,
                max: self.max,
            });
        }
        Ok(())
    }

    /// The delay owed after `failures` consecutive failures.
    ///
    /// `0` failures is no delay: a succeeding reconciler waits for its poll
    /// interval, not for a retry. Saturating arithmetic throughout, so a replica
    /// that has been failing for a week computes the ceiling rather than
    /// overflowing.
    pub fn delay(&self, failures: u32) -> Duration {
        let Some(exponent) = failures.checked_sub(1) else {
            return Duration::ZERO;
        };
        let factor = u64::from(self.multiplier)
            .checked_pow(exponent.min(u32::from(u8::MAX)))
            .unwrap_or(u64::MAX);
        self.initial
            .checked_mul(u32::try_from(factor).unwrap_or(u32::MAX))
            .unwrap_or(self.max)
            .min(self.max)
    }
}

/// The retry state of one reconciler: how many consecutive failures, and what
/// they currently cost.
#[derive(Debug, Clone, Copy)]
pub struct Backoff {
    policy: BackoffPolicy,
    failures: u32,
}

impl Backoff {
    pub const fn new(policy: BackoffPolicy) -> Self {
        Self {
            policy,
            failures: 0,
        }
    }

    /// Record a failed attempt and return how long to wait before the next one.
    pub fn fail(&mut self) -> Duration {
        self.failures = self.failures.saturating_add(1);
        self.policy.delay(self.failures)
    }

    /// Forget the outage. Called on every successful attempt, including one that
    /// found nothing to do: reaching the control plane is the success being
    /// tracked here.
    pub const fn succeed(&mut self) {
        self.failures = 0;
    }

    pub const fn failures(&self) -> u32 {
        self.failures
    }

    /// The delay currently owed, without recording another failure.
    pub fn delay(&self) -> Duration {
        self.policy.delay(self.failures)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy() -> BackoffPolicy {
        BackoffPolicy {
            initial: Duration::from_millis(100),
            max: Duration::from_secs(1),
            multiplier: 2,
        }
    }

    /// The whole sequence, exactly: doubling from the first retry, saturating at
    /// the ceiling, and never growing past it however long the outage runs.
    #[test]
    fn consecutive_failures_double_the_delay_up_to_the_ceiling_and_stop_there() {
        let mut backoff = Backoff::new(policy());
        assert_eq!(backoff.delay(), Duration::ZERO);
        let observed: Vec<Duration> = (0..6).map(|_| backoff.fail()).collect();
        assert_eq!(
            observed,
            vec![
                Duration::from_millis(100),
                Duration::from_millis(200),
                Duration::from_millis(400),
                Duration::from_millis(800),
                Duration::from_secs(1),
                Duration::from_secs(1),
            ]
        );
        assert_eq!(backoff.failures(), 6);
    }

    /// A week-long outage must compute a delay, not overflow into a panic or a
    /// zero-length sleep that would hot-loop.
    #[test]
    fn an_unbounded_failure_count_saturates_at_the_ceiling() {
        let policy = policy();
        for failures in [32u32, 1_000, u32::MAX] {
            assert_eq!(policy.delay(failures), policy.max, "{failures} failures");
        }
    }

    #[test]
    fn one_success_clears_the_outage() {
        let mut backoff = Backoff::new(policy());
        backoff.fail();
        backoff.fail();
        backoff.succeed();
        assert_eq!(backoff.failures(), 0);
        assert_eq!(backoff.delay(), Duration::ZERO);
        assert_eq!(backoff.fail(), policy().initial);
    }

    #[test]
    fn a_policy_that_would_hot_loop_or_never_back_off_is_refused() {
        assert_eq!(
            BackoffPolicy {
                initial: Duration::ZERO,
                ..policy()
            }
            .validate(),
            Err(InvalidBackoff::ZeroInitial)
        );
        assert_eq!(
            BackoffPolicy {
                multiplier: 1,
                ..policy()
            }
            .validate(),
            Err(InvalidBackoff::Multiplier { multiplier: 1 })
        );
        assert_eq!(
            BackoffPolicy {
                initial: Duration::from_secs(5),
                max: Duration::from_secs(1),
                ..policy()
            }
            .validate(),
            Err(InvalidBackoff::Ceiling {
                initial: Duration::from_secs(5),
                max: Duration::from_secs(1),
            })
        );
        assert_eq!(BackoffPolicy::default().validate(), Ok(()));
    }
}
