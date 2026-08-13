//! What convergence promises a fleet, asserted end to end.
//!
//! These tests drive the real [`Reconciler`] against the domain's contract
//! oracle ([`InMemoryControlPlane`]) and the real publication seam
//! ([`AppState`]), because the properties being tested are properties of that
//! composition: a mock sink could show that `publish` was called, but only the
//! actual `ArcSwap` shows that an in-flight request keeps the snapshot it
//! started with.
//!
//! Determinism comes from three injections rather than from sleeping: the
//! control plane is in-memory, the clock is manual, and the loop's timers run on
//! Tokio's paused clock. No test here is timing-sensitive.

use std::sync::Arc;
use std::time::Duration;

use super::compile::testing::{AliasProjection, bootstrap, env};
use super::lkg::testing::{KEY, cache_path};
use super::status::testing::ManualClock;
use super::*;
use crate::backends::control_plane::{ControlPlaneError, ControlPlaneStore};
use crate::budget::NoBudget;
use crate::desired_state::oracle::InMemoryControlPlane;
use crate::desired_state::{DesiredState, ExpectedRevision, RevisionId, fixtures};
use crate::state::AppState;
use crate::telemetry;
use crate::usage::{UsageFanout, UsageSink};

/// One replica: a control plane, a compiler, the `ArcSwap` it publishes into,
/// and a clock the test owns.
struct Replica {
    store: Arc<InMemoryControlPlane>,
    state: AppState,
    clock: ManualClock,
    reconciler: Arc<Reconciler>,
    /// The ledger the replica's materialization registers unwrapped material in:
    /// what a test reads to see which versions this process is holding, without
    /// being able to read the material itself.
    ledger: Arc<MaterialLedger>,
}

impl Replica {
    /// A replica whose projection targets the bootstrap's `openai` provider, so
    /// every published revision compiles.
    fn serving(store: &Arc<InMemoryControlPlane>) -> Self {
        Self::build(store, "openai", None)
    }

    /// A replica whose projection targets a provider nobody defines, so every
    /// candidate is refused by the same graph gate boot applies.
    fn refusing(store: &Arc<InMemoryControlPlane>) -> Self {
        Self::build(store, "nonexistent", None)
    }

    fn with_cache(store: &Arc<InMemoryControlPlane>, cache: LastKnownGood) -> Self {
        Self::build(store, "openai", Some(cache))
    }

    /// A replica whose projection is fine and whose secret store is down: the one
    /// way a candidate fails on *material* rather than on its graph.
    fn with_unresolvable_secrets(store: &Arc<InMemoryControlPlane>) -> Self {
        Self::assembled(
            store,
            "openai",
            None,
            super::secrets::testing::unavailable(),
        )
    }

    fn build(
        store: &Arc<InMemoryControlPlane>,
        provider: &'static str,
        cache: Option<LastKnownGood>,
    ) -> Self {
        Self::assembled(
            store,
            provider,
            cache,
            super::secrets::testing::permissive(),
        )
    }

    fn assembled(
        store: &Arc<InMemoryControlPlane>,
        provider: &'static str,
        cache: Option<LastKnownGood>,
        secrets: Arc<SecretMaterialization>,
    ) -> Self {
        let sinks: Vec<Box<dyn UsageSink>> = Vec::new();
        // The boot snapshot: generation 0, serving whatever the file said. Every
        // convergence assertion is relative to this.
        let state = AppState::new(
            bootstrap(),
            &env(),
            UsageFanout::new(sinks),
            Box::new(NoBudget),
        )
        .expect("the bootstrap config is servable");
        let clock = ManualClock::new();
        let ledger = Arc::clone(secrets.ledger());
        let reconciler = Arc::new(Reconciler::new(
            Arc::clone(store) as Arc<dyn ControlPlaneStore>,
            Arc::new(RevisionCompiler::with_secrets(
                bootstrap(),
                env(),
                AliasProjection { provider },
                Arc::clone(&secrets),
            )),
            Arc::new(state.clone()),
            settings(),
            cache,
            Arc::new(clock.clone()),
        ));
        Self {
            store: Arc::clone(store),
            state,
            clock,
            reconciler,
            ledger,
        }
    }

    fn report(&self) -> RevisionReport {
        self.reconciler.report()
    }

    /// The aliases this replica is serving right now.
    fn served_aliases(&self) -> Vec<String> {
        self.state
            .config()
            .config
            .model
            .iter()
            .map(|model| model.name.clone())
            .collect()
    }

    fn generation(&self) -> u64 {
        self.state.config().generation
    }
}

/// Tight but valid pacing, so a paused-clock test advances milliseconds rather
/// than seconds.
fn settings() -> ConvergenceSettings {
    ConvergenceSettings {
        poll_interval: Duration::from_millis(100),
        target: Duration::from_secs(1),
        backoff: BackoffPolicy {
            initial: Duration::from_millis(100),
            max: Duration::from_secs(4),
            multiplier: 2,
        },
    }
}

fn control_plane() -> Arc<InMemoryControlPlane> {
    Arc::new(InMemoryControlPlane::new())
}

/// Publish `state` as the newest revision, the way an administrator would.
async fn publish(
    store: &InMemoryControlPlane,
    key: &str,
    expected: ExpectedRevision,
    state: DesiredState,
) -> RevisionId {
    store
        .publish_revision(fixtures::candidate(expected, key, state))
        .await
        .expect("the candidate is valid")
        .id
}

/// The base case: a replica that starts behind catches up, and what it serves
/// afterwards is what desired state says.
#[tokio::test]
async fn a_replica_converges_to_the_desired_revision_and_serves_it() {
    let store = control_plane();
    let published = publish(&store, "first", ExpectedRevision::Empty, fixtures::state()).await;
    let replica = Replica::serving(&store);

    assert_eq!(replica.generation(), 0, "the boot snapshot is generation 0");
    let outcome = replica
        .reconciler
        .converge_once(telemetry::CONVERGENCE_POLLED)
        .await;

    assert!(
        matches!(
            outcome,
            Outcome::Published { revision, generation, .. }
                if revision == published && generation == 1
        ),
        "{outcome:?}"
    );
    let report = replica.report();
    assert!(report.converged());
    assert_eq!(report.desired, Some(published));
    assert_eq!(report.loaded, Some(published));
    assert_eq!(report.active, Some(published));
    assert_eq!(report.source, Some(SnapshotSource::ControlPlane));
    assert_eq!(report.lag, Duration::ZERO);
    assert_eq!(report.consecutive_failures, 0);
    assert!(report.last_rejection.is_none());
    // The published revision is what is being *served*, not merely recorded.
    assert!(replica.served_aliases().contains(&"fast".to_owned()));
    assert_eq!(replica.generation(), 1);
}

/// A converged replica still reads desired state — that read is how it detects
/// the next change and how it proves the control plane is reachable — but it does
/// not republish, because a republication would bump the generation and reset
/// per-target circuits for no reason.
#[tokio::test]
async fn a_converged_replica_does_not_republish_what_it_is_already_serving() {
    let store = control_plane();
    publish(&store, "first", ExpectedRevision::Empty, fixtures::state()).await;
    let replica = Replica::serving(&store);
    replica
        .reconciler
        .converge_once(telemetry::CONVERGENCE_POLLED)
        .await;

    for _ in 0..5 {
        let outcome = replica
            .reconciler
            .converge_once(telemetry::CONVERGENCE_POLLED)
            .await;
        assert!(
            matches!(outcome, Outcome::AlreadyConverged { .. }),
            "{outcome:?}"
        );
    }
    assert_eq!(replica.generation(), 1, "no spurious republication");
}

/// A second revision converges on top of the first, and the replica ends up
/// serving the newer alias set rather than a union of both.
#[tokio::test]
async fn a_newer_revision_replaces_the_previous_one_wholesale() {
    let store = control_plane();
    let first = publish(&store, "first", ExpectedRevision::Empty, fixtures::state()).await;
    let replica = Replica::serving(&store);
    replica
        .reconciler
        .converge_once(telemetry::CONVERGENCE_POLLED)
        .await;
    assert!(replica.served_aliases().contains(&"fast".to_owned()));

    let second = publish(
        &store,
        "second",
        ExpectedRevision::Exactly(first),
        fixtures::state_with_renamed_alias(),
    )
    .await;
    replica
        .reconciler
        .converge_once(telemetry::CONVERGENCE_POLLED)
        .await;

    assert_eq!(replica.report().active, Some(second));
    assert_eq!(replica.generation(), 2);
    let aliases = replica.served_aliases();
    assert!(aliases.contains(&"quick".to_owned()), "{aliases:?}");
    assert!(
        !aliases.contains(&"fast".to_owned()),
        "the previous revision's alias is gone, not merged: {aliases:?}"
    );
}

/// The reason polling is the correctness mechanism: nothing notifies this
/// replica, and it converges anyway once its poll interval elapses.
#[tokio::test(start_paused = true)]
async fn a_missed_notification_is_recovered_by_the_poll() {
    let store = control_plane();
    let replica = Replica::serving(&store);
    let signal = Arc::new(ChangeSignal::new());
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let loop_reconciler = Arc::clone(&replica.reconciler);
    let task = tokio::spawn(async move {
        loop_reconciler
            .run(Arc::new(ChangeSignal::new()), async {
                let _ = stopped.await;
            })
            .await;
    });

    // Published *without* notifying anyone, which is what a dropped `NOTIFY`
    // looks like from a replica's point of view.
    let published = publish(&store, "first", ExpectedRevision::Empty, fixtures::state()).await;
    assert_eq!(replica.report().active, None, "not converged yet");

    advance_until(&replica, |report| report.active == Some(published)).await;
    assert!(replica.served_aliases().contains(&"fast".to_owned()));

    drop(signal);
    let _ = stop.send(());
    task.await.expect("the loop stops when shutdown completes");
}

/// And the reason notifications are worth having: the same convergence happens
/// without waiting for the poll interval at all.
#[tokio::test(start_paused = true)]
async fn a_notification_converges_before_the_next_poll() {
    let store = control_plane();
    let replica = Replica::serving(&store);
    let signal = Arc::new(ChangeSignal::new());
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let loop_reconciler = Arc::clone(&replica.reconciler);
    let listener = Arc::clone(&signal);
    let task = tokio::spawn(async move {
        loop_reconciler
            .run(listener, async {
                let _ = stopped.await;
            })
            .await;
    });

    let published = publish(&store, "first", ExpectedRevision::Empty, fixtures::state()).await;
    signal.notify();
    for _ in 0..32 {
        if replica.report().active == Some(published) {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(
        replica.report().active,
        Some(published),
        "a notification converges without the clock advancing"
    );

    let _ = stop.send(());
    task.await.expect("the loop stops");
}

/// Malformed desired state — here, a revision that does not project onto a
/// servable graph — is refused with *nothing* published: same generation, same
/// aliases, and a report that says which stage refused it.
#[tokio::test]
async fn a_revision_that_fails_the_boot_gate_publishes_nothing() {
    let store = control_plane();
    let published = publish(&store, "first", ExpectedRevision::Empty, fixtures::state()).await;
    let replica = Replica::refusing(&store);
    let before = replica.served_aliases();

    let outcome = replica
        .reconciler
        .converge_once(telemetry::CONVERGENCE_POLLED)
        .await;

    assert!(
        matches!(
            outcome,
            Outcome::Rejected { revision, reason }
                if revision == Some(published) && reason == "validation"
        ),
        "{outcome:?}"
    );
    let report = replica.report();
    assert!(!report.converged());
    assert_eq!(report.desired, Some(published));
    assert_eq!(report.loaded, None, "a refused candidate never loaded");
    assert_eq!(report.active, None);
    assert_eq!(report.generation, 0);
    assert_eq!(report.consecutive_failures, 1);
    let rejection = report.last_rejection.expect("a reason is reported");
    assert_eq!(rejection.reason, "validation");
    assert!(
        rejection.detail.contains("undefined provider"),
        "{}",
        rejection.detail
    );
    // Nothing about what is being served changed.
    assert_eq!(replica.generation(), 0);
    assert_eq!(replica.served_aliases(), before);
}

/// A revision this build cannot read — here a tenant body written before tenancy
/// bodies were typed — is refused as an *incompatibility*, under its own reason,
/// while the replica keeps serving the revision it already converged onto.
///
/// The distinction is operational: `corrupt` sends someone to repair storage,
/// which is exactly the wrong response to a fleet mid-upgrade.
#[tokio::test]
async fn a_revision_this_build_cannot_read_is_refused_as_an_incompatibility() {
    let store = control_plane();
    let first = publish(&store, "first", ExpectedRevision::Empty, fixtures::state()).await;
    let replica = Replica::serving(&store);
    replica
        .reconciler
        .converge_once(telemetry::CONVERGENCE_POLLED)
        .await;
    let serving = replica.served_aliases();

    let second = publish(
        &store,
        "second",
        ExpectedRevision::Exactly(first),
        fixtures::state_with_renamed_alias(),
    )
    .await;
    // The retained tenant row, as an older writer left it.
    store.rewrite_version(fixtures::legacy_tenant(1, "acme"));

    let outcome = replica
        .reconciler
        .converge_once(telemetry::CONVERGENCE_POLLED)
        .await;
    assert!(
        matches!(
            outcome,
            Outcome::Rejected { revision, reason }
                if revision == Some(second) && reason == "incompatible"
        ),
        "{outcome:?}"
    );
    let report = replica.report();
    let rejection = report.last_rejection.expect("a reason is reported");
    assert_eq!(rejection.reason, "incompatible");
    assert!(
        rejection.detail.contains("not compatible with this build"),
        "{}",
        rejection.detail
    );

    // Last known good is retained in the only sense that matters: the replica is
    // still serving the revision it converged onto, at the same generation.
    assert_eq!(report.active, Some(first));
    assert_eq!(report.desired, Some(second));
    assert_eq!(replica.generation(), 1);
    assert_eq!(replica.served_aliases(), serving);
}

/// A control-plane outage degrades to staleness: the replica keeps serving the
/// revision it already has, reports growing lag, and converges when Postgres
/// comes back.
#[tokio::test]
async fn an_outage_keeps_the_previous_revision_serving_and_reports_the_lag() {
    let store = control_plane();
    let first = publish(&store, "first", ExpectedRevision::Empty, fixtures::state()).await;
    let replica = Replica::serving(&store);
    replica
        .reconciler
        .converge_once(telemetry::CONVERGENCE_POLLED)
        .await;
    let serving = replica.served_aliases();

    // A newer revision exists, and then the control plane goes away before this
    // replica ever sees it.
    let second = publish(
        &store,
        "second",
        ExpectedRevision::Exactly(first),
        fixtures::state_with_renamed_alias(),
    )
    .await;
    store.set_unavailable(true);

    for attempt in 1..=3 {
        let outcome = replica
            .reconciler
            .converge_once(telemetry::CONVERGENCE_POLLED)
            .await;
        assert!(
            matches!(
                outcome,
                Outcome::Rejected {
                    reason: "unavailable",
                    ..
                }
            ),
            "{outcome:?}"
        );
        assert_eq!(replica.report().consecutive_failures, attempt);
        replica.clock.advance(Duration::from_secs(2));
    }

    let lagging = replica.report();
    assert_eq!(
        lagging.active,
        Some(first),
        "still serving the old revision"
    );
    assert_eq!(
        lagging.desired,
        Some(first),
        "desired is unreadable, not lost"
    );
    assert_eq!(replica.generation(), 1);
    assert_eq!(replica.served_aliases(), serving);
    assert_eq!(
        lagging.last_rejection.map(|rejection| rejection.reason),
        Some("unavailable")
    );

    store.set_unavailable(false);
    let outcome = replica
        .reconciler
        .converge_once(telemetry::CONVERGENCE_POLLED)
        .await;
    assert!(
        matches!(outcome, Outcome::Published { revision, .. } if revision == second),
        "{outcome:?}"
    );
    let recovered = replica.report();
    assert!(recovered.converged());
    assert_eq!(recovered.consecutive_failures, 0);
    assert!(
        recovered.last_rejection.is_none(),
        "a success clears the reported failure"
    );
    assert_eq!(recovered.lag, Duration::ZERO);
}

/// Lag is what an operator alerts on, so it has to grow while a replica is stuck
/// and be measured from the moment it fell behind.
#[tokio::test]
async fn lag_grows_while_a_replica_cannot_reach_the_desired_revision() {
    let store = control_plane();
    let first = publish(&store, "first", ExpectedRevision::Empty, fixtures::state()).await;
    let replica = Replica::refusing(&store);
    replica
        .reconciler
        .converge_once(telemetry::CONVERGENCE_POLLED)
        .await;

    assert_eq!(
        replica.report().lag,
        Duration::ZERO,
        "measured, not guessed"
    );
    replica.clock.advance(Duration::from_secs(45));
    let report = replica.report();
    assert_eq!(report.desired, Some(first));
    assert_eq!(report.lag, Duration::from_secs(45));
    assert!(
        report.lag > settings().target,
        "past the documented convergence target, which is what an alert fires on"
    );
}

/// Backoff is bounded and monotone: repeated failures widen the retry delay up to
/// the ceiling and stay there, so an outage costs a bounded number of attempts
/// per replica instead of a hot loop.
#[tokio::test]
async fn repeated_failures_widen_the_retry_delay_up_to_the_ceiling() {
    let store = control_plane();
    publish(&store, "first", ExpectedRevision::Empty, fixtures::state()).await;
    store.set_unavailable(true);
    let replica = Replica::serving(&store);

    let mut delays = Vec::new();
    let mut backoff = Backoff::new(settings().backoff);
    for _ in 0..8 {
        replica
            .reconciler
            .converge_once(telemetry::CONVERGENCE_POLLED)
            .await;
        delays.push(backoff.fail());
    }
    assert_eq!(
        delays,
        vec![
            Duration::from_millis(100),
            Duration::from_millis(200),
            Duration::from_millis(400),
            Duration::from_millis(800),
            Duration::from_millis(1_600),
            Duration::from_millis(3_200),
            Duration::from_secs(4),
            Duration::from_secs(4),
        ],
        "exponential, then saturated at the ceiling"
    );
    assert_eq!(replica.report().consecutive_failures, 8);
}

/// The loop-level version of the same property: over a fixed window of paused
/// time, a replica whose control plane is down makes a number of attempts bounded
/// by its backoff rather than spinning.
#[tokio::test(start_paused = true)]
async fn a_replica_whose_control_plane_is_down_does_not_hot_loop() {
    let store = control_plane();
    publish(&store, "first", ExpectedRevision::Empty, fixtures::state()).await;
    store.set_unavailable(true);
    let replica = Replica::serving(&store);
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let loop_reconciler = Arc::clone(&replica.reconciler);
    let task = tokio::spawn(async move {
        loop_reconciler
            .run(Arc::new(ChangeSignal::new()), async {
                let _ = stopped.await;
            })
            .await;
    });

    // Thirty seconds of outage. With a 100ms initial delay doubling to a 4s
    // ceiling, an unbounded loop would attempt hundreds of times.
    for _ in 0..30 {
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
    }
    let failures = replica.report().consecutive_failures;
    assert!(
        (2..=16).contains(&failures),
        "bounded retries over 30s of outage, got {failures}"
    );
    assert_eq!(replica.generation(), 0, "nothing was published");

    let _ = stop.send(());
    task.await.expect("the loop stops");
}

/// A replica that boots while the control plane is unreachable serves its signed
/// cache rather than failing to start — the case that keeps an outage from also
/// freezing fleet size.
#[tokio::test]
async fn a_cold_boot_during_an_outage_restores_the_signed_snapshot() {
    let store = control_plane();
    let published = publish(&store, "first", ExpectedRevision::Empty, fixtures::state()).await;
    let path = cache_path("cold-boot");

    // The replica that was running before the outage exports what it served.
    let warm = Replica::with_cache(
        &store,
        LastKnownGood::new(&path, KEY).expect("a long enough key"),
    );
    warm.reconciler
        .converge_once(telemetry::CONVERGENCE_POLLED)
        .await;
    assert!(path.exists(), "converging exported the cache");

    // A fresh replica starts with the control plane down.
    store.set_unavailable(true);
    let cold = Replica::with_cache(
        &store,
        LastKnownGood::new(&path, KEY).expect("a long enough key"),
    );
    let restored = cold.reconciler.bootstrap().await.expect("the cache serves");

    assert_eq!(restored, published);
    let report = cold.report();
    assert_eq!(report.active, Some(published));
    assert_eq!(report.source, Some(SnapshotSource::LastKnownGood));
    assert!(cold.served_aliases().contains(&"fast".to_owned()));
    assert_eq!(cold.generation(), 1);

    // And once the control plane returns, the same replica converges normally and
    // stops reporting the cache as its source.
    store.set_unavailable(false);
    let second = publish(
        &store,
        "second",
        ExpectedRevision::Exactly(published),
        fixtures::state_with_renamed_alias(),
    )
    .await;
    cold.reconciler
        .converge_once(telemetry::CONVERGENCE_POLLED)
        .await;
    let converged = cold.report();
    assert_eq!(converged.active, Some(second));
    assert_eq!(converged.source, Some(SnapshotSource::ControlPlane));

    let _ = std::fs::remove_file(&path);
}

/// A replica added to a fleet mid-rollout meets a desired revision its build
/// cannot read, and starts from its signed cache rather than refusing to boot.
///
/// Storage is intact here, so there is nothing for an operator to repair, and a
/// replica that would not start would withdraw capacity during exactly the
/// rollout or rollback that needs it added. The cache was written by this build,
/// so it is readable by definition.
#[tokio::test]
async fn a_cold_boot_onto_an_unreadable_revision_restores_the_signed_snapshot() {
    let store = control_plane();
    let published = publish(&store, "first", ExpectedRevision::Empty, fixtures::state()).await;
    let path = cache_path("cold-boot-incompatible");

    let warm = Replica::with_cache(
        &store,
        LastKnownGood::new(&path, KEY).expect("a long enough key"),
    );
    warm.reconciler
        .converge_once(telemetry::CONVERGENCE_POLLED)
        .await;
    assert!(path.exists(), "converging exported the cache");

    // Desired state moves on, and its retained tenant row is one an older writer
    // left behind: this build cannot read the revision it must converge onto.
    let second = publish(
        &store,
        "second",
        ExpectedRevision::Exactly(published),
        fixtures::state_with_renamed_alias(),
    )
    .await;
    store.rewrite_version(fixtures::legacy_tenant(1, "acme"));

    let cold = Replica::with_cache(
        &store,
        LastKnownGood::new(&path, KEY).expect("a long enough key"),
    );
    let restored = cold
        .reconciler
        .bootstrap()
        .await
        .expect("an unreadable revision is not a reason to refuse to start");

    assert_eq!(restored, published);
    let report = cold.report();
    assert_eq!(report.active, Some(published));
    assert_eq!(report.source, Some(SnapshotSource::LastKnownGood));
    assert_eq!(report.generation, 1);
    assert!(cold.served_aliases().contains(&"fast".to_owned()));

    // The cache is a fallback, not a way to stop reporting refusals: converging
    // still refuses the revision under its own reason, and keeps serving.
    let outcome = cold
        .reconciler
        .converge_once(telemetry::CONVERGENCE_POLLED)
        .await;
    assert!(
        matches!(
            outcome,
            Outcome::Rejected { revision, reason }
                if revision == Some(second) && reason == "incompatible"
        ),
        "{outcome:?}"
    );
    assert_eq!(cold.report().active, Some(published));

    let _ = std::fs::remove_file(&path);
}

/// With no cache to restore, the same unreadable revision refuses the boot: there
/// is nothing to serve, and serving nothing while reporting healthy is worse.
#[tokio::test]
async fn a_cold_boot_onto_an_unreadable_revision_without_a_cache_refuses_to_start() {
    let store = control_plane();
    publish(&store, "first", ExpectedRevision::Empty, fixtures::state()).await;
    store.rewrite_version(fixtures::legacy_tenant(1, "acme"));

    let error = Replica::serving(&store)
        .reconciler
        .bootstrap()
        .await
        .expect_err("there is nothing to serve");
    assert!(
        matches!(
            error,
            BootstrapError::Store {
                source: ControlPlaneError::Incompatible { .. }
            }
        ),
        "an unreadable revision is named as such, not as an outage: {error}"
    );
}

/// A rollback that reuses the volume: neither desired state nor the cache the
/// newer build left behind is readable here. The refusal names the version skew
/// rather than blaming a cache file that is authentic and intact.
#[tokio::test]
async fn a_cold_boot_whose_cache_a_newer_build_wrote_is_refused_as_a_skew() {
    let store = control_plane();
    let published = publish(&store, "first", ExpectedRevision::Empty, fixtures::state()).await;
    let path = cache_path("cold-boot-newer-cache");

    let warm = Replica::with_cache(
        &store,
        LastKnownGood::new(&path, KEY).expect("a long enough key"),
    );
    warm.reconciler
        .converge_once(telemetry::CONVERGENCE_POLLED)
        .await;
    let cached = LastKnownGood::new(&path, KEY).expect("a long enough key");
    let readable = cached.load().expect("reads back").expect("a cache exists");
    cached
        .export_unassembled(readable.manifest(), &fixtures::state_with_legacy_tenant())
        .expect("a newer build's export");
    assert_eq!(readable.manifest().id, published);

    // And desired state is what the newer build published, which this build also
    // does not read.
    store.rewrite_version(fixtures::legacy_tenant(1, "acme"));

    let error = Replica::with_cache(
        &store,
        LastKnownGood::new(&path, KEY).expect("a long enough key"),
    )
    .reconciler
    .bootstrap()
    .await
    .expect_err("there is nothing this build can serve");
    assert!(
        matches!(
            error,
            BootstrapError::Store {
                source: ControlPlaneError::Incompatible { .. }
            }
        ),
        "the skew is named, not the cache that faithfully recorded it: {error}"
    );

    let _ = std::fs::remove_file(&path);
}

/// Without a cache, a cold boot during an outage refuses to start rather than
/// serving an empty configuration while reporting itself healthy.
#[tokio::test]
async fn a_cold_boot_during_an_outage_without_a_cache_refuses_to_start() {
    let store = control_plane();
    publish(&store, "first", ExpectedRevision::Empty, fixtures::state()).await;
    store.set_unavailable(true);
    let replica = Replica::serving(&store);

    let error = replica
        .reconciler
        .bootstrap()
        .await
        .expect_err("there is nothing to serve");
    assert!(
        matches!(error, BootstrapError::Unavailable { .. }),
        "{error}"
    );
    assert_eq!(replica.generation(), 0);
}

/// A reachable control plane with nothing published is also a refusal to start:
/// an empty stateful deployment has no aliases and no keys, and starting anyway
/// would report a healthy replica that answers nothing.
#[tokio::test]
async fn a_cold_boot_against_an_empty_control_plane_refuses_to_start() {
    let store = control_plane();
    let replica = Replica::serving(&store);
    let error = replica
        .reconciler
        .bootstrap()
        .await
        .expect_err("nothing has been published");
    assert!(matches!(error, BootstrapError::Empty), "{error}");
}

/// A revision that cannot be compiled is fatal at boot, and specifically *not*
/// answered from the cache: booting an older cached revision would silently serve
/// state an operator already replaced.
#[tokio::test]
async fn a_boot_revision_that_does_not_compile_is_fatal_even_with_a_cache() {
    let store = control_plane();
    publish(&store, "first", ExpectedRevision::Empty, fixtures::state()).await;
    let path = cache_path("boot-rejected");
    let replica = Replica::build(
        &store,
        "nonexistent",
        Some(LastKnownGood::new(&path, KEY).expect("a long enough key")),
    );

    let error = replica
        .reconciler
        .bootstrap()
        .await
        .expect_err("the desired revision is unservable");
    assert!(matches!(error, BootstrapError::Rejected { .. }), "{error}");
    assert!(!path.exists(), "nothing unservable was ever cached");
}

/// The one-snapshot-per-request guarantee, asserted against the real `ArcSwap`:
/// a request that took its snapshot under revision *N* keeps resolving against
/// *N* — same generation, same aliases — after *N+1* becomes active. This covers
/// buffered and streaming requests alike, because both hold exactly this `Arc`
/// for their lifetime (a stream holds it across every relayed chunk).
#[tokio::test]
async fn an_in_flight_request_keeps_the_revision_it_started_under() {
    let store = control_plane();
    let first = publish(&store, "first", ExpectedRevision::Empty, fixtures::state()).await;
    let replica = Replica::serving(&store);
    replica
        .reconciler
        .converge_once(telemetry::CONVERGENCE_POLLED)
        .await;

    // A request starts: exactly what the request path does, once, at the top.
    let in_flight = replica.state.config();
    assert_eq!(in_flight.generation, 1);
    assert!(
        in_flight
            .config
            .model
            .iter()
            .any(|model| model.name == "fast")
    );

    // A newer revision becomes active mid-request.
    publish(
        &store,
        "second",
        ExpectedRevision::Exactly(first),
        fixtures::state_with_renamed_alias(),
    )
    .await;
    replica
        .reconciler
        .converge_once(telemetry::CONVERGENCE_POLLED)
        .await;
    assert_eq!(replica.generation(), 2);
    assert!(replica.served_aliases().contains(&"quick".to_owned()));

    // The in-flight request's view is unchanged: it finishes, or relays its
    // stream to completion, against the revision it started under.
    assert_eq!(in_flight.generation, 1);
    assert!(
        in_flight
            .config
            .model
            .iter()
            .any(|model| model.name == "fast"),
        "the alias the request resolved is still resolvable"
    );
    assert!(
        !in_flight
            .config
            .model
            .iter()
            .any(|model| model.name == "quick"),
        "and the newer revision has not leaked into it"
    );

    // Requests that start now see the new revision — that is the difference
    // between pinning a request and pinning a replica.
    assert_eq!(replica.state.config().generation, 2);
}

/// Advance the paused clock until `predicate` holds, so a loop test asserts on a
/// condition rather than on a sleep duration.
async fn advance_until(replica: &Replica, predicate: impl Fn(&RevisionReport) -> bool) {
    for _ in 0..64 {
        if predicate(&replica.reconciler.report()) {
            return;
        }
        tokio::time::advance(Duration::from_millis(50)).await;
        tokio::task::yield_now().await;
    }
    panic!(
        "the replica never reached the expected state: {:?}",
        replica.reconciler.report()
    );
}

/// A published revision's material is held by the snapshot serving it, so it is
/// live exactly while that snapshot is.
#[tokio::test]
async fn a_published_revision_holds_the_material_it_was_compiled_against() {
    let store = control_plane();
    publish(&store, "first", ExpectedRevision::Empty, fixtures::state()).await;
    let replica = Replica::serving(&store);
    assert!(
        replica.ledger.is_empty(),
        "the boot snapshot has no typed credentials"
    );

    replica
        .reconciler
        .converge_once(telemetry::CONVERGENCE_POLLED)
        .await;

    let held = replica.ledger.retained();
    let expected = required_secrets(&fixtures::state());
    assert_eq!(held, expected, "the published revision's versions are held");
    assert_eq!(
        replica.state.config().secrets().references(),
        expected,
        "and the snapshot serving requests is what holds them"
    );
}

/// A rotation is two versions at once, and the old one is released by the last
/// request that was still using it — not by the rotation.
#[tokio::test]
async fn a_rotation_overlaps_versions_until_the_previous_snapshot_is_gone() {
    let store = control_plane();
    let first = publish(&store, "first", ExpectedRevision::Empty, fixtures::state()).await;
    let replica = Replica::serving(&store);
    replica
        .reconciler
        .converge_once(telemetry::CONVERGENCE_POLLED)
        .await;
    let old = required_secrets(&fixtures::state());

    // A request starts under the pre-rotation revision, and keeps its snapshot.
    let in_flight = replica.state.config();

    publish(
        &store,
        "rotated",
        ExpectedRevision::Exactly(first),
        fixtures::state_with_rotated_credential(),
    )
    .await;
    replica
        .reconciler
        .converge_once(telemetry::CONVERGENCE_POLLED)
        .await;

    let new = required_secrets(&fixtures::state_with_rotated_credential());
    assert_ne!(new, old, "a rotation pins a different exact version");
    assert_eq!(
        replica.state.config().secrets().references(),
        new,
        "new requests authenticate with the rotated version"
    );
    for reference in &old {
        assert!(
            replica.ledger.holds(*reference),
            "the in-flight request's version is still live: {reference}"
        );
        assert!(in_flight.secrets().get(*reference).is_some());
    }

    // The request finishes. Nothing references the old version, so it is gone.
    drop(in_flight);
    for reference in &old {
        assert!(
            !replica.ledger.holds(*reference),
            "the superseded version is zeroized once nothing serves it: {reference}"
        );
    }
    assert_eq!(replica.ledger.retained(), new);
}

/// The last-known-good property, for secrets: a candidate whose material will
/// not resolve is refused, the previous revision keeps serving with the material
/// it already holds, and the failed candidate leaves nothing behind.
#[tokio::test]
async fn a_candidate_whose_material_does_not_resolve_leaves_the_previous_revision_serving() {
    let store = control_plane();
    let first = publish(&store, "first", ExpectedRevision::Empty, fixtures::state()).await;
    let replica = Replica::serving(&store);
    replica
        .reconciler
        .converge_once(telemetry::CONVERGENCE_POLLED)
        .await;
    let serving = required_secrets(&fixtures::state());
    assert_eq!(replica.ledger.retained(), serving);

    // The store goes down between revisions, which is the only way a candidate
    // can fail on material it would otherwise resolve.
    let unavailable = Replica::with_unresolvable_secrets(&store);
    let rotated = publish(
        &store,
        "rotated",
        ExpectedRevision::Exactly(first),
        fixtures::state_with_rotated_credential(),
    )
    .await;
    let outcome = unavailable
        .reconciler
        .converge_once(telemetry::CONVERGENCE_POLLED)
        .await;

    assert!(matches!(outcome, Outcome::Rejected { .. }), "{outcome:?}");
    let report = unavailable.report();
    assert_eq!(report.desired, Some(rotated));
    assert_ne!(report.active, Some(rotated), "the candidate is not active");
    let rejection = report.last_rejection.expect("a recorded rejection");
    assert_eq!(rejection.reason, "secret");
    assert!(
        !rejection.detail.contains(super::secrets::testing::MATERIAL),
        "the rejection names references, not material: {}",
        rejection.detail
    );
    assert!(
        unavailable.ledger.is_empty(),
        "a refused candidate retains nothing"
    );

    // The replica that *was* serving is untouched: same generation, same aliases,
    // same material.
    assert_eq!(replica.generation(), 1);
    assert!(replica.served_aliases().contains(&"fast".to_owned()));
    assert_eq!(replica.ledger.retained(), serving);
}

/// Every exact version a revision's resolvable credentials pin, ordered.
fn required_secrets(state: &DesiredState) -> Vec<crate::desired_state::secrets::SecretRef> {
    let mut references: Vec<_> = crate::desired_state::credentials::Credentials::of(state)
        .expect("readable fixture credentials")
        .required_secrets()
        .map(|(_, reference)| reference)
        .collect();
    references.sort_unstable();
    references
}
