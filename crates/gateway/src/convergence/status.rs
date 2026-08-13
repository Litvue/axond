//! What a replica reports about its own convergence.
//!
//! Three revision ids answer three different operator questions, and collapsing
//! them into one "current revision" gauge is how a fleet hides a stuck replica:
//!
//! - **desired** — what the control plane says should be serving. Shared by every
//!   replica.
//! - **loaded** — the newest revision this replica successfully hydrated *and*
//!   compiled. Equal to desired once a candidate has been accepted; behind it
//!   while a candidate is being refused.
//! - **active** — what requests are actually being served from. This is the only
//!   one that describes behaviour, and it changes only when a snapshot is
//!   published.
//!
//! Between them, `desired != active` is the alertable condition and *lag* — how
//! long that has been true — is the number to alert on, because a revision that
//! is one second behind is convergence working and one that is ten minutes behind
//! is an incident. The rejection reason attached to the report is what turns that
//! alert into an action: an operator who sees `lag = 9m, reason = validation`
//! knows to look at what was published, not at Postgres.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::desired_state::RevisionId;

/// Where the active snapshot came from.
///
/// Reported because "serving revision R" means something different depending on
/// whether the control plane confirmed R a second ago or a cached copy of R was
/// all this replica could find at boot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotSource {
    /// Hydrated from the control plane.
    ControlPlane,
    /// Restored from this replica's signed last-known-good cache, because the
    /// control plane was unreachable at boot.
    LastKnownGood,
}

impl SnapshotSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ControlPlane => "control-plane",
            Self::LastKnownGood => "last-known-good",
        }
    }
}

/// The clock convergence measures lag and convergence time against.
///
/// Injected rather than called directly so the lag and timing assertions are
/// deterministic instead of sleeping. Production passes [`SystemClock`].
pub trait Clock: Send + Sync {
    fn now(&self) -> Instant;
}

/// The monotonic system clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// The most recent refusal, kept so a lagging replica can say *why*.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rejection {
    /// The revision that was refused, when the failure was specific to one.
    pub revision: Option<RevisionId>,
    /// A stable low-cardinality label: `unavailable`, `corrupt`, `projection`,
    /// `validation`, `secret`, `snapshot`.
    pub reason: &'static str,
    /// The operator-facing detail. Carries references, never secret material.
    pub detail: String,
}

/// An immutable read of one replica's convergence state.
///
/// The default is the honest report of a replica that has converged onto
/// nothing — no desired revision observed, none loaded, none active — which is
/// exactly the state of a replica with no reconciler running.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RevisionReport {
    pub desired: Option<RevisionId>,
    pub loaded: Option<RevisionId>,
    pub active: Option<RevisionId>,
    pub source: Option<SnapshotSource>,
    /// The generation of the active snapshot, which increments on every
    /// publication and is what request logs correlate against.
    pub generation: u64,
    /// How long desired has differed from active. Zero when converged.
    pub lag: Duration,
    /// How long the last accepted candidate took from observation to
    /// publication.
    pub last_convergence: Option<Duration>,
    pub consecutive_failures: u32,
    pub last_rejection: Option<Rejection>,
}

impl RevisionReport {
    /// Whether this replica is serving what the control plane wants.
    ///
    /// `None == None` counts as converged: a deployment with nothing published
    /// is not lagging.
    pub fn converged(&self) -> bool {
        self.desired == self.active
    }
}

#[derive(Debug, Default)]
struct State {
    desired: Option<RevisionId>,
    /// When the current desired revision was first observed while not active.
    /// `None` while converged.
    diverged_since: Option<Instant>,
    loaded: Option<RevisionId>,
    active: Option<RevisionId>,
    source: Option<SnapshotSource>,
    generation: u64,
    last_convergence: Option<Duration>,
    consecutive_failures: u32,
    last_rejection: Option<Rejection>,
}

/// The shared, lock-guarded convergence state of one replica.
///
/// A `Mutex` is the right primitive precisely because nothing on the request path
/// touches it: it is written by the single reconciler task and read by
/// administrative and telemetry callers.
pub struct RevisionStatus {
    state: Mutex<State>,
    clock: Box<dyn Clock>,
}

impl RevisionStatus {
    pub fn new(clock: Box<dyn Clock>) -> Self {
        Self {
            state: Mutex::new(State::default()),
            clock,
        }
    }

    fn locked(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .expect("convergence status is not poisoned")
    }

    /// Record what the control plane says is desired.
    ///
    /// Divergence is timestamped on the transition, not on every poll, so lag
    /// measures "how long has this replica been behind" rather than "how long
    /// since the last poll".
    pub fn observe_desired(&self, desired: Option<RevisionId>) {
        let mut state = self.locked();
        if state.desired != desired {
            state.desired = desired;
            state.diverged_since = None;
        }
        if state.desired == state.active {
            state.diverged_since = None;
        } else if state.diverged_since.is_none() {
            state.diverged_since = Some(self.clock.now());
        }
    }

    /// Record that a revision hydrated and compiled, whether or not its snapshot
    /// has been published yet.
    pub fn observe_loaded(&self, loaded: RevisionId) {
        self.locked().loaded = Some(loaded);
    }

    /// Record a published snapshot: this is the only call that changes what is
    /// being served.
    pub fn record_published(
        &self,
        revision: RevisionId,
        generation: u64,
        source: SnapshotSource,
        took: Duration,
    ) {
        let mut state = self.locked();
        state.active = Some(revision);
        state.loaded = Some(revision);
        state.source = Some(source);
        state.generation = generation;
        state.last_convergence = Some(took);
        state.consecutive_failures = 0;
        state.last_rejection = None;
        if state.desired == state.active {
            state.diverged_since = None;
        }
    }

    /// Record a refusal. The active snapshot is untouched by construction — this
    /// type holds no snapshot — which is the point: reporting a failure cannot
    /// change what is serving.
    pub fn record_rejection(&self, rejection: Rejection, consecutive_failures: u32) {
        let mut state = self.locked();
        state.consecutive_failures = consecutive_failures;
        state.last_rejection = Some(rejection);
    }

    pub fn report(&self) -> RevisionReport {
        let state = self.locked();
        let lag = match state.diverged_since {
            Some(since) => self.clock.now().saturating_duration_since(since),
            None => Duration::ZERO,
        };
        RevisionReport {
            desired: state.desired,
            loaded: state.loaded,
            active: state.active,
            source: state.source,
            generation: state.generation,
            lag,
            last_convergence: state.last_convergence,
            consecutive_failures: state.consecutive_failures,
            last_rejection: state.last_rejection.clone(),
        }
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use std::sync::Arc;

    /// A clock that only moves when a test moves it.
    #[derive(Debug, Clone)]
    pub(crate) struct ManualClock {
        base: Instant,
        offset: Arc<Mutex<Duration>>,
    }

    impl ManualClock {
        pub(crate) fn new() -> Self {
            Self {
                base: Instant::now(),
                offset: Arc::new(Mutex::new(Duration::ZERO)),
            }
        }

        pub(crate) fn advance(&self, by: Duration) {
            *self.offset.lock().expect("not poisoned") += by;
        }
    }

    impl Clock for ManualClock {
        fn now(&self) -> Instant {
            self.base + *self.offset.lock().expect("not poisoned")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testing::ManualClock;
    use super::*;
    use crate::desired_state::fixtures::revision_id;

    fn status(clock: &ManualClock) -> RevisionStatus {
        RevisionStatus::new(Box::new(clock.clone()))
    }

    #[test]
    fn a_replica_with_nothing_published_is_converged_and_not_lagging() {
        let clock = ManualClock::new();
        let status = status(&clock);
        status.observe_desired(None);
        clock.advance(Duration::from_secs(60));
        let report = status.report();
        assert!(report.converged());
        assert_eq!(report.lag, Duration::ZERO);
        assert_eq!(report.active, None);
    }

    /// Lag is measured from the moment the replica fell behind, and stops the
    /// moment it catches up — not from the last poll, and not from process start.
    #[test]
    fn lag_measures_how_long_desired_has_differed_from_active() {
        let clock = ManualClock::new();
        let status = status(&clock);
        let first = revision_id(1);

        status.observe_desired(Some(first));
        clock.advance(Duration::from_secs(3));
        let lagging = status.report();
        assert!(!lagging.converged());
        assert_eq!(lagging.lag, Duration::from_secs(3));
        assert_eq!(lagging.active, None);

        status.record_published(
            first,
            1,
            SnapshotSource::ControlPlane,
            Duration::from_millis(120),
        );
        clock.advance(Duration::from_secs(30));
        let converged = status.report();
        assert!(converged.converged());
        assert_eq!(converged.lag, Duration::ZERO);
        assert_eq!(converged.generation, 1);
        assert_eq!(converged.last_convergence, Some(Duration::from_millis(120)));
        assert_eq!(converged.source, Some(SnapshotSource::ControlPlane));
    }

    /// The report an operator reads during an incident: still serving the old
    /// revision, loaded is behind desired, and the reason says which stage
    /// refused.
    #[test]
    fn a_refused_candidate_reports_the_reason_while_the_old_revision_stays_active() {
        let clock = ManualClock::new();
        let status = status(&clock);
        let first = revision_id(1);
        let second = revision_id(2);
        status.observe_desired(Some(first));
        status.record_published(first, 1, SnapshotSource::ControlPlane, Duration::ZERO);

        status.observe_desired(Some(second));
        clock.advance(Duration::from_secs(5));
        status.record_rejection(
            Rejection {
                revision: Some(second),
                reason: "validation",
                detail: "model `fast` targets undefined provider `gone`".to_owned(),
            },
            3,
        );

        let report = status.report();
        assert_eq!(report.desired, Some(second));
        assert_eq!(report.loaded, Some(first));
        assert_eq!(report.active, Some(first));
        assert_eq!(report.generation, 1);
        assert_eq!(report.lag, Duration::from_secs(5));
        assert_eq!(report.consecutive_failures, 3);
        assert_eq!(
            report
                .last_rejection
                .as_ref()
                .map(|rejection| rejection.reason),
            Some("validation")
        );
        assert!(!report.converged());
    }

    /// A newer desired revision restarts the lag measurement rather than
    /// inheriting the previous one's clock, so lag always answers "how stale is
    /// what I am serving *now*".
    #[test]
    fn a_new_desired_revision_restarts_the_lag_measurement() {
        let clock = ManualClock::new();
        let status = status(&clock);
        status.observe_desired(Some(revision_id(1)));
        clock.advance(Duration::from_secs(10));
        status.observe_desired(Some(revision_id(2)));
        clock.advance(Duration::from_secs(2));
        assert_eq!(status.report().lag, Duration::from_secs(2));
    }

    #[test]
    fn a_publication_from_the_cache_reports_the_cache_as_its_source() {
        let clock = ManualClock::new();
        let status = status(&clock);
        let first = revision_id(1);
        status.record_published(first, 1, SnapshotSource::LastKnownGood, Duration::ZERO);
        let report = status.report();
        assert_eq!(report.source, Some(SnapshotSource::LastKnownGood));
        assert_eq!(report.active, Some(first));
        // Nothing has been observed from the control plane, so the replica is
        // serving a revision it cannot yet confirm is desired.
        assert_eq!(report.desired, None);
        assert!(!report.converged());
    }
}
