//! Convergence pacing, and the bounds that keep it bounded.
//!
//! Four numbers, each of which is a promise to an operator:
//!
//! - `poll_interval` — the worst-case detection delay when notifications are off
//!   or lost. This is the number a "how fast do changes roll out?" answer is
//!   built from.
//! - `target` — how long divergence is *allowed* to last before it is an
//!   incident. Nothing enforces it (a replica cannot make Postgres reachable);
//!   it is the threshold reported alongside lag so an alert has a documented
//!   value to compare against instead of one invented in a dashboard.
//! - `backoff` — how a failing replica paces retries, so an outage costs one
//!   attempt per ceiling per replica rather than a hot loop.
//!
//! Defaults assume the ordinary deployment: a handful of replicas, an
//! administrator publishing occasional changes, and a Postgres that is normally
//! healthy. They are deliberately unaggressive — the poll exists to catch a lost
//! notification, not to be the primary path.

use std::time::Duration;

use super::backoff::{BackoffPolicy, InvalidBackoff};

/// How a replica paces convergence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConvergenceSettings {
    /// How often desired state is read when nothing has failed.
    pub poll_interval: Duration,
    /// The convergence target reported for alerting: divergence lasting longer
    /// than this is an incident rather than convergence in progress.
    pub target: Duration,
    /// Retry pacing for failed attempts.
    pub backoff: BackoffPolicy,
}

impl Default for ConvergenceSettings {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(5),
            target: Duration::from_secs(30),
            backoff: BackoffPolicy::default(),
        }
    }
}

/// Why convergence settings cannot be used.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum InvalidSettings {
    #[error("convergence.poll_interval must be greater than zero; a zero interval hot-loops")]
    ZeroPollInterval,
    #[error(
        "convergence.target ({target:?}) must be at least convergence.poll_interval \
         ({poll_interval:?}); a target shorter than the detection delay can never be met"
    )]
    UnreachableTarget {
        poll_interval: Duration,
        target: Duration,
    },
    #[error(transparent)]
    Backoff(#[from] InvalidBackoff),
}

impl ConvergenceSettings {
    /// Refuse settings that cannot do what they claim.
    ///
    /// The target check is the interesting one: a target below the poll interval
    /// is not a strict SLO, it is an alert that fires on healthy convergence, so
    /// it is rejected rather than accepted and quietly missed.
    pub fn validate(&self) -> Result<(), InvalidSettings> {
        if self.poll_interval.is_zero() {
            return Err(InvalidSettings::ZeroPollInterval);
        }
        if self.target < self.poll_interval {
            return Err(InvalidSettings::UnreachableTarget {
                poll_interval: self.poll_interval,
                target: self.target,
            });
        }
        self.backoff.validate()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_defaults_are_usable_and_state_a_reachable_target() {
        let settings = ConvergenceSettings::default();
        assert_eq!(settings.validate(), Ok(()));
        assert!(settings.target >= settings.poll_interval);
        // A lost notification costs one poll interval, which must stay well
        // inside the target an operator alerts on.
        assert!(settings.poll_interval * 2 <= settings.target);
    }

    #[test]
    fn settings_that_cannot_do_what_they_claim_are_refused() {
        assert_eq!(
            ConvergenceSettings {
                poll_interval: Duration::ZERO,
                ..Default::default()
            }
            .validate(),
            Err(InvalidSettings::ZeroPollInterval)
        );
        assert_eq!(
            ConvergenceSettings {
                poll_interval: Duration::from_secs(10),
                target: Duration::from_secs(1),
                ..Default::default()
            }
            .validate(),
            Err(InvalidSettings::UnreachableTarget {
                poll_interval: Duration::from_secs(10),
                target: Duration::from_secs(1),
            })
        );
        assert!(matches!(
            ConvergenceSettings {
                backoff: BackoffPolicy {
                    initial: Duration::ZERO,
                    ..BackoffPolicy::default()
                },
                ..Default::default()
            }
            .validate(),
            Err(InvalidSettings::Backoff(_))
        ));
    }
}
