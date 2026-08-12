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
//!    finish. Whatever is still in flight when the deadline expires is
//!    abandoned. A response that has begun ends in an error, which drops the
//!    upstream stream and settles it through the usual cancellation path, so the
//!    usage record is written as `client_cancelled` with the spend measured up to
//!    the last relayed token; a request still inside its handler has no body to
//!    end, so the same signal is taken at the handler boundary, cancelling the
//!    upstream call and answering the caller `503 draining`.
//! 4. Within one `shutdown.flush_timeout_ms` budget: the abandoned responses are
//!    given a moment to end and their settlements are awaited — for at most half
//!    the budget ([`Plan::settle_share`]), so one request that cannot end cannot
//!    starve the step that writes the records — then the buffered usage sinks are
//!    flushed (anything that cannot be written is counted as a `shutdown` drop
//!    rather than silently lost) and the telemetry providers flush with whatever
//!    remains.
//!
//! Every wait above is bounded, so the worst case is
//! `drain_grace_ms + deadline_ms + flush_timeout_ms`: keep
//! `terminationGracePeriodSeconds` above that sum and `SIGKILL` never arrives
//! mid-flush.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::watch;

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
    /// Set when the deadline expires: in-flight responses end themselves rather
    /// than being torn down by the runtime, which is what lets their spend
    /// settle at all (see [`Lifecycle::abandon`]).
    abandoning: AtomicBool,
    /// Bumped whenever any of the above changes, so a waiter can sleep until
    /// something it cares about happened.
    ///
    /// A `watch` channel rather than a `Notify`: a `Notify` waiter is only
    /// enqueued when its future is first polled, so a `notify_waiters` landing
    /// between the state check and that first poll wakes nobody and the waiter
    /// sleeps until its timeout — burning the very budget the flush needs. A
    /// `watch` receiver records its version at `subscribe`, so a change racing
    /// the check is still observed.
    changes: watch::Sender<u64>,
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
            abandoning: AtomicBool::new(false),
            changes: watch::Sender::new(0),
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
        self.announce();
    }

    /// Resolves once admission has closed.
    pub async fn closed(&self) {
        self.wait_for(|| self.phase() == Phase::Closing).await;
    }

    /// Publish that something a waiter might be sleeping on has changed. The
    /// state itself stays in the atomics; the version is only a wake-up.
    fn announce(&self) {
        self.changes.send_modify(|version| *version += 1);
    }

    /// Sleep until `reached` holds, waking on every announced change.
    ///
    /// Subscribing *before* the first check is the whole point: a change that
    /// lands between the check and the sleep bumps a version this receiver has
    /// not seen, so `changed()` returns immediately instead of waiting for a
    /// notification that already happened.
    async fn wait_for(&self, mut reached: impl FnMut() -> bool) {
        let mut changes = self.changes.subscribe();
        while !reached() {
            if changes.changed().await.is_err() {
                // The sender lives in `self`, so this is unreachable while the
                // borrow is held; treat it as "nothing will change again".
                return;
            }
        }
    }

    /// Tell every still-open request to end: responses by ending their bodies,
    /// handlers by dropping the future that is still awaiting an upstream.
    ///
    /// Dropping the server future would not do this: `hyper` serves each
    /// connection on its own task, so the connections outlive the future and are
    /// torn down only when the runtime is, by which point there is nothing left
    /// to settle spend onto. Ending the bodies here instead keeps the
    /// cancellation on the normal accounting path.
    pub fn abandon(&self) {
        self.abandoning.store(true, Ordering::Release);
        self.announce();
    }

    /// Resolves once [`Lifecycle::abandon`] has been called.
    pub async fn abandoned(&self) {
        self.wait_for(|| self.abandoning.load(Ordering::Acquire))
            .await;
    }

    /// Wait up to `bound` for in-flight requests to finish, and report how many
    /// are left.
    pub async fn quiesce(&self, bound: Duration) -> u64 {
        let _ = tokio::time::timeout(bound, self.wait_for(|| self.in_flight() == 0)).await;
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
            self.lifecycle.announce();
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

impl Plan {
    /// The most of `flush_timeout` the waits for in-flight work may spend,
    /// leaving the rest reserved for the flush itself.
    ///
    /// The waits come first because a settlement that lands after the flush is
    /// a record nobody writes — but they must not be able to consume the whole
    /// budget: one request that cannot end would then leave `Duration::ZERO`
    /// for the sinks, and every record already accepted would be dropped as
    /// `shutdown`. Losing the whole buffer to save one settlement is the wrong
    /// trade, so the write step keeps a reserve. Half is a deliberate split
    /// rather than a tuned one: both steps matter, and the total the operator
    /// was promised does not change.
    pub fn settle_share(self) -> Duration {
        self.flush_timeout / 2
    }
}

/// The signal-time [`Plan`], published by [`drain`] for the steps that run
/// after serving.
///
/// Without this the deadline and the flush budget would be whatever `[shutdown]`
/// said at boot, while the drain logged — and the documentation promised — the
/// reloaded values: an operator would see the new bounds and get the old ones.
#[derive(Clone, Default)]
pub struct ResolvedPlan(Arc<std::sync::OnceLock<Plan>>);

impl ResolvedPlan {
    pub fn new() -> Self {
        Self::default()
    }

    /// The plan resolved when the signal arrived, or `fallback` if the server
    /// ended on its own and no signal was ever handled.
    pub fn or(&self, fallback: Plan) -> Plan {
        self.0.get().copied().unwrap_or(fallback)
    }

    /// Set once, by the drain: a second signal re-uses the first plan so the
    /// bounds cannot change midway through one termination.
    fn publish(&self, plan: Plan) {
        let _ = self.0.set(plan);
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
pub async fn drain(
    lifecycle: Arc<Lifecycle>,
    signals: Signals,
    plan: impl Fn() -> Plan,
    resolved: ResolvedPlan,
) {
    let mut signals = signals;
    let signal = signals.recv().await;
    let plan = plan();
    // Published before anything else waits on it, so the deadline and flush
    // budget enforced later are the ones logged below.
    resolved.publish(plan);
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
    // to wait for routing to catch up, so the grace window is cut short. With
    // `drain_grace_ms = 0` there is no window to cut short: admission closes on
    // the first signal, and the arm below is never reached.
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
    // Handlers stay installed for the rest of the sequence rather than being
    // dropped here: a signal arriving during the deadline or the flush would
    // otherwise hit the default disposition and kill the process mid-flush,
    // discarding exactly the records this sequence exists to write. Past this
    // point the remaining bounds are what shortens termination, so further
    // signals are logged and otherwise ignored.
    tokio::spawn(async move {
        loop {
            let signal = signals.recv().await;
            tracing::warn!(
                signal,
                "termination signal ignored: admission is already closed and the remaining \
                 waits are bounded"
            );
        }
    });
}

/// Await the server, bounding the wait for in-flight work by `deadline` once
/// admission has closed.
///
/// `axum`'s graceful shutdown waits for every connection with no bound of its
/// own, which is exactly the unbounded wait a termination grace period would
/// resolve with `SIGKILL` — after the flush never ran. The deadline starts when
/// admission closes, so time spent failing readiness is not charged to
/// in-flight requests.
///
/// The deadline comes from `resolved` — the plan the drain read when the signal
/// arrived — and only falls back to `boot` when the server ended without a
/// signal, so a reloaded `[shutdown]` is what is actually enforced.
pub async fn serve_bounded<S, E>(
    served: S,
    lifecycle: &Lifecycle,
    resolved: &ResolvedPlan,
    boot: Plan,
) -> Outcome
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
    // Admission has closed, so the drain has published its plan.
    let plan = resolved.or(boot);
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
pub struct Signals(Source);

enum Source {
    #[cfg(unix)]
    Os {
        terminate: tokio::signal::unix::Signal,
        interrupt: tokio::signal::unix::Signal,
    },
    #[cfg(not(unix))]
    CtrlC,
    /// Signals a test delivers itself, so the drain can be driven without
    /// touching process-wide handlers. The sequence ends by never resolving
    /// again, exactly like a process that receives one `SIGTERM`.
    #[cfg(test)]
    Scripted(tokio::sync::mpsc::UnboundedReceiver<&'static str>),
}

impl Signals {
    #[cfg(unix)]
    pub fn install() -> std::io::Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};
        Ok(Self(Source::Os {
            terminate: signal(SignalKind::terminate())?,
            interrupt: signal(SignalKind::interrupt())?,
        }))
    }

    #[cfg(not(unix))]
    pub fn install() -> std::io::Result<Self> {
        Ok(Self(Source::CtrlC))
    }

    /// One `SIGTERM` and nothing after it.
    #[cfg(test)]
    fn once() -> Self {
        let (deliver, signals) = Self::scripted();
        deliver("SIGTERM");
        signals
    }

    /// A sender for the test to deliver signals through, and the source the
    /// drain reads them from.
    #[cfg(test)]
    fn scripted() -> (impl Fn(&'static str), Self) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        (
            move |signal| {
                let _ = tx.send(signal);
            },
            Self(Source::Scripted(rx)),
        )
    }

    /// The next termination request, named for the log line.
    pub async fn recv(&mut self) -> &'static str {
        match &mut self.0 {
            #[cfg(unix)]
            Source::Os {
                terminate,
                interrupt,
            } => {
                tokio::select! {
                    _ = terminate.recv() => "SIGTERM",
                    _ = interrupt.recv() => "SIGINT",
                }
            }
            #[cfg(not(unix))]
            Source::CtrlC => {
                let _ = tokio::signal::ctrl_c().await;
                "ctrl-c"
            }
            #[cfg(test)]
            Source::Scripted(signals) => match signals.recv().await {
                Some(signal) => signal,
                None => std::future::pending().await,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The waits for in-flight work may never leave the flush without a budget:
    /// records already accepted are lost if the write step gets nothing.
    #[test]
    fn the_settle_waits_can_never_take_the_whole_flush_budget() {
        for flush_timeout_ms in [0, 1, 500, 5_000, 60_000] {
            let plan = Plan::from(&ShutdownConfig {
                flush_timeout_ms,
                ..ShutdownConfig::default()
            });
            let reserved = plan.flush_timeout - plan.settle_share();
            assert!(
                plan.settle_share() <= plan.flush_timeout / 2,
                "settle share must be capped at half of {flush_timeout_ms}ms"
            );
            assert!(
                reserved >= plan.settle_share(),
                "the flush must keep at least the share the waits get"
            );
        }
    }

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
        let outcome = serve_bounded(
            async { Ok::<(), std::io::Error>(()) },
            &lifecycle,
            &ResolvedPlan::new(),
            plan(),
        )
        .await;
        assert_eq!(outcome, Outcome::Completed);
    }

    /// A reload between boot and the signal has to change the bound that is
    /// enforced, not just the one that is logged.
    #[tokio::test]
    async fn the_deadline_enforced_is_the_one_read_when_the_signal_arrived() {
        let lifecycle = Arc::new(Lifecycle::new());
        let _admitted = lifecycle.admit().expect("serving admits");
        let resolved = ResolvedPlan::new();

        let reloaded = Plan {
            drain_grace: Duration::ZERO,
            // Long enough that enforcing the boot plan's 50ms instead would
            // abandon the request and fail the assertion below.
            deadline: Duration::from_secs(30),
            flush_timeout: Duration::from_millis(200),
        };
        let draining = {
            let lifecycle = Arc::clone(&lifecycle);
            let resolved = resolved.clone();
            tokio::spawn(async move {
                drain(
                    lifecycle,
                    Signals::once(),
                    move || reloaded,
                    resolved.clone(),
                )
                .await;
            })
        };

        let served = {
            let lifecycle = Arc::clone(&lifecycle);
            async move {
                lifecycle.closed().await;
                // Past the boot deadline, inside the reloaded one.
                tokio::time::sleep(Duration::from_millis(150)).await;
                Ok::<(), std::io::Error>(())
            }
        };
        let outcome = serve_bounded(served, &lifecycle, &resolved, plan()).await;

        assert_eq!(
            outcome,
            Outcome::Completed,
            "the reloaded deadline must be the one enforced"
        );
        assert_eq!(resolved.or(plan()).deadline, reloaded.deadline);
        assert_eq!(
            resolved.or(plan()).flush_timeout,
            reloaded.flush_timeout,
            "the flush budget must come from the same snapshot as the deadline"
        );
        draining.await.expect("the drain finished");
    }

    /// Without a signal there is nothing to publish, so the boot values stand.
    #[test]
    fn the_boot_plan_stands_when_no_signal_was_ever_handled() {
        assert_eq!(ResolvedPlan::new().or(plan()), plan());
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
            &ResolvedPlan::new(),
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
            &ResolvedPlan::new(),
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

    /// The property the waits rely on, asserted directly: a change published
    /// after a receiver exists is observable by that receiver, whether or not it
    /// has been polled. This is what a `Notify` does not give — its waiter is
    /// enqueued only on first poll, so a wake-up in that window is dropped and
    /// the waiter sleeps out its bound instead of seeing work already finished.
    #[test]
    fn a_change_published_before_the_first_poll_is_still_observable() {
        let lifecycle = Arc::new(Lifecycle::new());
        let changes = lifecycle.changes.subscribe();
        assert!(!changes.has_changed().expect("the sender is alive"));

        let admitted = lifecycle.admit().expect("serving admits");
        lifecycle.close();
        drop(admitted);

        assert!(
            changes.has_changed().expect("the sender is alive"),
            "a waiter armed before the change must not have to be woken again"
        );
    }

    /// The same window, exercised through the public waits: the state reaches
    /// its terminal value before the future is ever polled.
    #[tokio::test]
    async fn a_wait_created_before_the_change_resolves_without_a_wake_up() {
        let lifecycle = Arc::new(Lifecycle::new());
        let admitted = lifecycle.admit().expect("serving admits");
        let abandoned = lifecycle.abandoned();
        let quiesced = lifecycle.quiesce(Duration::from_secs(30));

        lifecycle.abandon();
        drop(admitted);

        tokio::time::timeout(Duration::from_secs(1), abandoned)
            .await
            .expect("`abandoned` must not wait for a notification that already fired");
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), quiesced)
                .await
                .expect("`quiesce` must not sleep out its bound once nothing is in flight"),
            0
        );
    }

    /// The costly case: the last guard drops on another thread while `quiesce`
    /// is arming. A lost wake-up here does not fail loudly — it spends the whole
    /// flush budget, so the usage records are dropped as `shutdown` instead of
    /// written. The bound is far larger than the work so that a miss shows up as
    /// a stall rather than a flake.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn quiesce_sees_a_guard_released_while_it_is_arming() {
        let started = std::time::Instant::now();
        for _ in 0..1_000 {
            let lifecycle = Arc::new(Lifecycle::new());
            let admitted = lifecycle.admit().expect("serving admits");
            let releasing = tokio::task::spawn_blocking(move || drop(admitted));

            assert_eq!(lifecycle.quiesce(Duration::from_secs(30)).await, 0);
            releasing.await.expect("the guard was released");
            assert!(
                started.elapsed() < Duration::from_secs(20),
                "quiesce stalled: a release was missed and the bound is being slept out"
            );
        }
    }

    /// The escape hatch an operator reaches for when routing has already caught
    /// up: the second signal closes admission instead of waiting out a grace
    /// window sized for the slowest load balancer.
    #[tokio::test]
    async fn a_second_signal_cuts_the_grace_window_short() {
        let lifecycle = Arc::new(Lifecycle::new());
        let (deliver, signals) = Signals::scripted();
        let long_grace = Plan {
            // Far longer than the test's patience: only the second signal can
            // close admission this quickly.
            drain_grace: Duration::from_secs(30),
            ..plan()
        };
        let draining = {
            let lifecycle = Arc::clone(&lifecycle);
            tokio::spawn(async move {
                drain(lifecycle, signals, move || long_grace, ResolvedPlan::new()).await;
            })
        };

        deliver("SIGTERM");
        // The drain has to be inside the grace window for the second signal to
        // be the thing that ends it.
        tokio::time::timeout(Duration::from_secs(5), async {
            while lifecycle.phase() != Phase::Draining {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the first signal begins the drain");
        assert!(
            lifecycle.admit().is_some(),
            "the grace window keeps admitting"
        );

        deliver("SIGINT");
        tokio::time::timeout(Duration::from_secs(5), draining)
            .await
            .expect("the second signal must not wait out the grace window")
            .expect("the drain finished");
        assert_eq!(lifecycle.phase(), Phase::Closing);
    }

    /// With no window there is nothing to cut short, so the first signal closes
    /// admission and no second one is required to make progress.
    #[tokio::test]
    async fn a_zero_grace_window_closes_admission_on_the_first_signal() {
        let lifecycle = Arc::new(Lifecycle::new());
        let zero_grace = Plan {
            drain_grace: Duration::ZERO,
            ..plan()
        };
        tokio::time::timeout(
            Duration::from_secs(5),
            drain(
                Arc::clone(&lifecycle),
                Signals::once(),
                move || zero_grace,
                ResolvedPlan::new(),
            ),
        )
        .await
        .expect("a zero grace window needs no second signal");
        assert_eq!(lifecycle.phase(), Phase::Closing);
        assert!(lifecycle.admit().is_none());
    }

    /// Past the close the remaining bounds are what shortens termination, so a
    /// further signal is logged and the phase stays where it is — it must not
    /// re-enter the sequence or take the process down mid-flush.
    #[tokio::test]
    async fn a_signal_after_the_close_leaves_the_sequence_alone() {
        let lifecycle = Arc::new(Lifecycle::new());
        let (deliver, signals) = Signals::scripted();
        let zero_grace = Plan {
            drain_grace: Duration::ZERO,
            ..plan()
        };
        deliver("SIGTERM");
        drain(
            Arc::clone(&lifecycle),
            signals,
            move || zero_grace,
            ResolvedPlan::new(),
        )
        .await;

        deliver("SIGTERM");
        // Let the installed consumer take it.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(lifecycle.phase(), Phase::Closing);
        assert!(lifecycle.admit().is_none());
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
