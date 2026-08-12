//! Process lifecycle: readiness drain, bounded request completion, and a
//! bounded flush of the buffered sinks.
//!
//! Termination is a sequence, not an instant, because a rolling deployment
//! removes a replica from service by *observing* it, not by being told:
//!
//! 1. `SIGTERM`/`SIGINT` arrives. The lifecycle leaves [`Phase::Serving`], so
//!    `/readyz` answers `503` while the replica keeps serving everything it is
//!    already admitting. This is the window the load balancer needs to notice.
//!    Liveness (`/healthz`) stays `200` for the whole sequence: a draining
//!    replica is not a wedged one, and failing liveness would only earn it a
//!    `SIGKILL`.
//! 2. After `shutdown.drain_grace_ms` the listener stops accepting and every
//!    request that arrives anyway is refused with a typed `503`
//!    ([`crate::error::GatewayError::Draining`]).
//! 3. Requests admitted before that point have `shutdown.deadline_ms` to
//!    finish. Whatever is still in flight when the deadline expires — a long
//!    stream, most likely — is abandoned: its response body ends in an error,
//!    which drops the upstream stream and settles it through the usual
//!    cancellation path, so the usage record is written as `client_cancelled`
//!    with the spend measured up to the last relayed token.
//! 4. Within one `shutdown.flush_timeout_ms` budget: the abandoned responses are
//!    given a moment to end, their settlements are awaited, the buffered usage
//!    sinks are flushed — anything that cannot be written is counted as a
//!    `shutdown` drop rather than silently lost — and the telemetry providers
//!    flush with whatever remains.
//!
//! Every wait above is bounded, so the worst case is
//! `drain_grace_ms + deadline_ms + flush_timeout_ms`: keep
//! `terminationGracePeriodSeconds` above that sum and `SIGKILL` never arrives
//! mid-flush.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::Notify;

use crate::config::Shutdown as ShutdownConfig;
use crate::telemetry::metrics;

/// Where the process is in the termination sequence. Monotonic: a phase is
/// never left in the direction of serving again, so a drained replica cannot be
/// talked back into service by anything short of a restart.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Phase {
    /// Normal operation: readiness passes and new work is admitted.
    Serving,
    /// Termination has begun. Readiness fails so the load balancer can remove
    /// the replica, but new work is still admitted — a request that arrives
    /// before routing catches up is served rather than lost.
    Draining,
    /// Admission is closed. New requests are refused; admitted ones finish
    /// within the deadline.
    Closing,
}

impl Phase {
    /// Stable, low-cardinality label for logs and metrics.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Serving => "serving",
            Self::Draining => "draining",
            Self::Closing => "closing",
        }
    }

    fn from_code(code: u8) -> Self {
        match code {
            0 => Self::Serving,
            1 => Self::Draining,
            _ => Self::Closing,
        }
    }

    fn code(self) -> u8 {
        match self {
            Self::Serving => 0,
            Self::Draining => 1,
            Self::Closing => 2,
        }
    }
}

/// The process-wide drain state, shared with the router through
/// [`crate::state::AppState`].
///
/// The phase is an atomic rather than a lock because every request reads it:
/// admission must cost an atomic load, not a shared-lock acquisition.
pub struct Lifecycle {
    phase: AtomicU8,
    in_flight: AtomicU64,
    /// Woken once, when admission closes, so the bounded completion wait can
    /// start its clock at that moment rather than at boot.
    closed: Notify,
    /// Set when the deadline expires: in-flight responses end themselves rather
    /// than being torn down by the runtime, which is what lets their spend
    /// settle at all (see [`Lifecycle::abandon`]).
    abandoning: AtomicBool,
    abandon: Notify,
    /// Woken when the last in-flight request finishes.
    idle: Notify,
}

impl Default for Lifecycle {
    fn default() -> Self {
        Self::new()
    }
}

impl Lifecycle {
    pub fn new() -> Self {
        Self {
            phase: AtomicU8::new(Phase::Serving.code()),
            in_flight: AtomicU64::new(0),
            closed: Notify::new(),
            abandoning: AtomicBool::new(false),
            abandon: Notify::new(),
            idle: Notify::new(),
        }
    }

    pub fn phase(&self) -> Phase {
        Phase::from_code(self.phase.load(Ordering::Acquire))
    }

    /// Requests admitted and not yet finished. Observed at the deadline to say
    /// how much work was abandoned.
    pub fn in_flight(&self) -> u64 {
        self.in_flight.load(Ordering::Relaxed)
    }

    /// Take an admission slot, or refuse because admission is closed. The
    /// returned guard releases the slot when the response (including a streamed
    /// body) is dropped.
    pub fn admit(self: &Arc<Self>) -> Option<Admitted> {
        if self.phase() == Phase::Closing {
            return None;
        }
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        Some(Admitted {
            lifecycle: Arc::clone(self),
        })
    }

    /// Fail readiness while continuing to serve. Idempotent, so a repeated
    /// signal cannot walk the sequence backwards.
    pub fn begin_drain(&self) {
        let _ = self.phase.compare_exchange(
            Phase::Serving.code(),
            Phase::Draining.code(),
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    /// Close admission and release the bounded completion wait.
    pub fn close(&self) {
        self.phase.store(Phase::Closing.code(), Ordering::Release);
        self.closed.notify_waiters();
    }

    /// Resolves once admission has closed. Registering the waiter before
    /// re-reading the phase is what makes a concurrent [`Lifecycle::close`]
    /// impossible to miss.
    pub async fn closed(&self) {
        let notified = self.closed.notified();
        if self.phase() == Phase::Closing {
            return;
        }
        notified.await;
    }

    /// Tell every still-open response to end.
    ///
    /// Dropping the server future would not do this: `hyper` serves each
    /// connection on its own task, so the connections outlive the future and are
    /// torn down only when the runtime is, by which point there is nothing left
    /// to settle spend onto. Ending the bodies here instead keeps the
    /// cancellation on the normal accounting path.
    pub fn abandon(&self) {
        self.abandoning.store(true, Ordering::Release);
        self.abandon.notify_waiters();
    }

    /// Resolves once [`Lifecycle::abandon`] has been called.
    pub async fn abandoned(&self) {
        let notified = self.abandon.notified();
        if self.abandoning.load(Ordering::Acquire) {
            return;
        }
        notified.await;
    }

    /// Wait up to `bound` for in-flight requests to finish, and report how many
    /// are left.
    pub async fn quiesce(&self, bound: Duration) -> u64 {
        let _ = tokio::time::timeout(bound, async {
            loop {
                let idle = self.idle.notified();
                if self.in_flight() == 0 {
                    return;
                }
                idle.await;
            }
        })
        .await;
        self.in_flight()
    }
}

/// An admitted request. Dropping it releases the slot, so cancellation counts
/// exactly like completion.
pub struct Admitted {
    lifecycle: Arc<Lifecycle>,
}

impl Drop for Admitted {
    fn drop(&mut self) {
        if self.lifecycle.in_flight.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.lifecycle.idle.notify_waiters();
        }
    }
}

/// The bounds one termination runs under, resolved from `[shutdown]` when the
/// signal arrives (so a reload of the section applies without a restart).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Plan {
    /// How long readiness fails before admission closes.
    pub drain_grace: Duration,
    /// How long admitted requests have to finish once admission is closed.
    pub deadline: Duration,
    /// The bound on flushing the buffered usage sinks and the telemetry
    /// exporters.
    pub flush_timeout: Duration,
}

impl From<&ShutdownConfig> for Plan {
    fn from(config: &ShutdownConfig) -> Self {
        Self {
            drain_grace: Duration::from_millis(config.drain_grace_ms),
            deadline: Duration::from_millis(config.deadline_ms),
            flush_timeout: Duration::from_millis(config.flush_timeout_ms),
        }
    }
}

/// How the serving loop ended. `Abandoned` is the documented lossy case, and
/// the only one that reports a count.
#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    /// Every admitted request finished, or the process was never asked to stop.
    Completed,
    /// The deadline expired with work still in flight; those connections were
    /// dropped.
    Abandoned { in_flight: u64 },
    /// The server itself failed.
    Failed(String),
}

impl Outcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Abandoned { .. } => "abandoned",
            Self::Failed(_) => "failed",
        }
    }
}

/// Drive the drain sequence and resolve once admission is closed. This is the
/// future `axum::serve(..).with_graceful_shutdown(..)` waits on, so returning
/// from it is what stops the listener.
pub async fn drain(lifecycle: Arc<Lifecycle>, signals: Signals, plan: impl Fn() -> Plan) {
    let mut signals = signals;
    let signal = signals.recv().await;
    let plan = plan();
    lifecycle.begin_drain();
    metrics::record_shutdown_phase(Phase::Draining);
    tracing::warn!(
        signal,
        drain_grace_ms = plan.drain_grace.as_millis() as u64,
        deadline_ms = plan.deadline.as_millis() as u64,
        in_flight = lifecycle.in_flight(),
        "shutdown requested: readiness now fails while admitted requests keep being served"
    );
    // A second signal means the operator (or the runtime) is no longer willing
    // to wait for routing to catch up, so the grace window is cut short.
    if !plan.drain_grace.is_zero() {
        tokio::select! {
            () = tokio::time::sleep(plan.drain_grace) => {}
            second = signals.recv() => {
                tracing::warn!(signal = second, "second termination signal: closing admission now");
            }
        }
    }
    lifecycle.close();
    metrics::record_shutdown_phase(Phase::Closing);
    tracing::info!(
        in_flight = lifecycle.in_flight(),
        deadline_ms = plan.deadline.as_millis() as u64,
        "admission closed: new requests are refused with `draining`"
    );
}

/// Await the server, bounding the wait for in-flight work by `deadline` once
/// admission has closed.
///
/// `axum`'s graceful shutdown waits for every connection with no bound of its
/// own, which is exactly the unbounded wait a termination grace period would
/// resolve with `SIGKILL` — after the flush never ran. The deadline starts when
/// admission closes, so time spent failing readiness is not charged to
/// in-flight requests.
pub async fn serve_bounded<S, E>(served: S, lifecycle: &Lifecycle, plan: Plan) -> Outcome
where
    S: std::future::IntoFuture<Output = Result<(), E>>,
    E: std::fmt::Display,
{
    let mut served = std::pin::pin!(served.into_future());
    {
        let closed = std::pin::pin!(lifecycle.closed());
        tokio::select! {
            result = &mut served => return finish(result),
            () = closed => {}
        }
    }
    match tokio::time::timeout(plan.deadline, served).await {
        Ok(result) => finish(result),
        Err(_) => {
            let in_flight = lifecycle.in_flight();
            metrics::record_shutdown_abandoned(in_flight);
            tracing::warn!(
                in_flight,
                deadline_ms = plan.deadline.as_millis() as u64,
                "shutdown deadline expired: ending still-open responses; streamed spend is \
                 settled as `client_cancelled` up to the last relayed token"
            );
            // The caller waits for them to end — bounded — as the first step of
            // the flush sequence, so both share one budget.
            lifecycle.abandon();
            Outcome::Abandoned { in_flight }
        }
    }
}

fn finish<E: std::fmt::Display>(result: Result<(), E>) -> Outcome {
    match result {
        Ok(()) => Outcome::Completed,
        Err(error) => Outcome::Failed(error.to_string()),
    }
}

/// The termination signals, installed before serving so a platform that refuses
/// a handler fails at boot rather than at shutdown.
pub struct Signals {
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
}

impl Signals {
    #[cfg(unix)]
    pub fn install() -> std::io::Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};
        Ok(Self {
            terminate: signal(SignalKind::terminate())?,
            interrupt: signal(SignalKind::interrupt())?,
        })
    }

    #[cfg(not(unix))]
    pub fn install() -> std::io::Result<Self> {
        Ok(Self {})
    }

    /// The next termination request, named for the log line.
    #[cfg(unix)]
    pub async fn recv(&mut self) -> &'static str {
        tokio::select! {
            _ = self.terminate.recv() => "SIGTERM",
            _ = self.interrupt.recv() => "SIGINT",
        }
    }

    #[cfg(not(unix))]
    pub async fn recv(&mut self) -> &'static str {
        let _ = tokio::signal::ctrl_c().await;
        "ctrl-c"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn draining_fails_readiness_but_still_admits() {
        let lifecycle = Arc::new(Lifecycle::new());
        assert_eq!(lifecycle.phase(), Phase::Serving);
        lifecycle.begin_drain();
        // What `/readyz` reads: anything past `Serving` answers `503`.
        assert_eq!(lifecycle.phase(), Phase::Draining);
        let admitted = lifecycle.admit().expect("draining still admits");
        assert_eq!(lifecycle.in_flight(), 1);
        drop(admitted);
        assert_eq!(lifecycle.in_flight(), 0);
    }

    #[test]
    fn closing_refuses_new_work_and_never_reopens() {
        let lifecycle = Arc::new(Lifecycle::new());
        let admitted = lifecycle.admit().expect("serving admits");
        lifecycle.close();
        assert!(lifecycle.admit().is_none(), "admission must be closed");
        // An admitted request is unaffected by the close, and a stale drain
        // cannot walk the phase back to `Draining`.
        assert_eq!(lifecycle.in_flight(), 1);
        lifecycle.begin_drain();
        assert_eq!(lifecycle.phase(), Phase::Closing);
        drop(admitted);
        assert_eq!(lifecycle.in_flight(), 0);
    }

    #[tokio::test]
    async fn closed_resolves_for_a_waiter_that_arrives_late() {
        let lifecycle = Arc::new(Lifecycle::new());
        lifecycle.close();
        // No waiter existed at `close`, so this must observe the phase itself
        // rather than wait for a notification that will never come again.
        tokio::time::timeout(Duration::from_secs(1), lifecycle.closed())
            .await
            .expect("a late waiter still resolves");
    }

    /// Bounds small enough that a test observes them, in the same order of
    /// magnitude as the shipped defaults.
    fn plan() -> Plan {
        Plan {
            drain_grace: Duration::from_millis(10),
            deadline: Duration::from_millis(50),
            flush_timeout: Duration::from_millis(200),
        }
    }

    #[tokio::test]
    async fn a_finished_server_completes_without_waiting_for_the_deadline() {
        let lifecycle = Lifecycle::new();
        let outcome =
            serve_bounded(async { Ok::<(), std::io::Error>(()) }, &lifecycle, plan()).await;
        assert_eq!(outcome, Outcome::Completed);
    }

    /// The deadline both bounds the wait and tells the still-open responses to
    /// end: without that they would be torn down with the runtime, too late to
    /// settle what they had already spent.
    #[tokio::test]
    async fn work_still_in_flight_at_the_deadline_is_told_to_end() {
        let lifecycle = Arc::new(Lifecycle::new());
        let admitted = lifecycle.admit().expect("serving admits");
        lifecycle.close();
        let body = {
            let lifecycle = Arc::clone(&lifecycle);
            tokio::spawn(async move {
                lifecycle.abandoned().await;
                drop(admitted);
            })
        };
        let outcome = serve_bounded(
            std::future::pending::<Result<(), std::io::Error>>(),
            &lifecycle,
            plan(),
        )
        .await;
        assert_eq!(outcome, Outcome::Abandoned { in_flight: 1 });
        body.await.expect("the response ended");
        assert_eq!(
            lifecycle.in_flight(),
            0,
            "the abandoned response must have released its slot"
        );
    }

    #[tokio::test]
    async fn a_server_error_is_reported_rather_than_swallowed() {
        let lifecycle = Lifecycle::new();
        let outcome = serve_bounded(
            async { Err::<(), std::io::Error>(std::io::Error::other("listener failed")) },
            &lifecycle,
            plan(),
        )
        .await;
        assert!(matches!(outcome, Outcome::Failed(message) if message.contains("listener failed")));
    }

    #[tokio::test]
    async fn quiesce_returns_what_is_still_in_flight_when_the_bound_expires() {
        let lifecycle = Arc::new(Lifecycle::new());
        let _admitted = lifecycle.admit().expect("serving admits");
        assert_eq!(lifecycle.quiesce(Duration::from_millis(20)).await, 1);
    }

    #[test]
    fn the_plan_comes_from_the_shutdown_section() {
        let plan = Plan::from(&ShutdownConfig::default());
        assert!(
            !plan.deadline.is_zero(),
            "waits must be bounded, not absent"
        );
        assert!(!plan.flush_timeout.is_zero());
    }
}
