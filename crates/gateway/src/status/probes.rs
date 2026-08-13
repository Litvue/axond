//! The probes that fill the registry, and the only place a backend is asked
//! how it is.
//!
//! A probe is deliberately thin: it calls the one reachability method its
//! backend already offers for diagnostics, and turns whatever comes back into a
//! [`ComponentObservation`] — a state, a code from the closed
//! [`StatusReason`] vocabulary, and an operator-facing detail that is logged and
//! never projected into a response. Nothing here retries, caches, or interprets:
//! the refresher paces it, the registry ages it, and
//! [`crate::status::StatusResponse`] decides who may see what.
//!
//! Only components a deployment actually *has* get a probe. Everything else
//! reports `disabled`, which is why a stateless replica still answers the
//! diagnostic without ever touching a network.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

#[cfg(doc)]
use super::registry::StatusRefresher;
use super::registry::{ComponentProbe, MIN_REFRESH_INTERVAL, StatusSettings};
use super::{Component, ComponentObservation, StatusReason};
use crate::backends::BackendFailure;
use crate::backends::control_plane::postgres::ControlPlaneSettings;
use crate::backends::control_plane::{ControlPlaneStore, StatusProbeAdmission};
use crate::backends::health::BackendHealth;

/// Observes the control plane a stateful replica administers against.
///
/// It shares the store the administrative surface was built on rather than
/// opening its own connection: a second pool would make the diagnostic report on
/// a path no administrative request uses, which is the failure mode where status
/// says `ok` through an outage of the thing being asked about.
pub struct ControlPlaneProbe {
    store: Arc<dyn ControlPlaneStore>,
}

impl ControlPlaneProbe {
    pub fn new(store: Arc<dyn ControlPlaneStore>) -> Self {
        Self { store }
    }

    /// The fastest pacing this probe can be given without reporting a working
    /// control plane as unreachable.
    ///
    /// A probe cut off before the backend's own bounds have elapsed does not
    /// observe a timeout, it *causes* one: [`StatusRefresher`] publishes
    /// `unavailable`/`timeout` for a call the store would have completed, and
    /// `AxondControlPlaneUnreachable` pages while administration against that
    /// same control plane is succeeding. So the timeout is derived from the
    /// store's configuration rather than chosen:
    ///
    /// * the store serialises every operation on one client, so a probe can
    ///   first wait out an administrative operation already running
    ///   (`operation_timeout` — a migration or a publish is entitled to all of
    ///   it);
    /// * a connection the outage dropped is then re-established
    ///   (`connect_timeout`);
    /// * and the health call itself is bounded by `operation_timeout` again.
    ///
    /// The registry's boot-time pacing reserves one administrative operation
    /// ahead of the probe, which keeps its static settings conservative before
    /// the first round. At runtime the Postgres store counts every operation
    /// already holding or waiting for its serialized client, and this probe
    /// asks it for a timeout using that live depth. Operations admitted after
    /// the health call are behind it in the fair queue and do not extend its
    /// budget. A deeper queue therefore delays the probe without turning a
    /// healthy store into a synthetic `unavailable`/`timeout` observation.
    ///
    /// The refresh interval sits above that so a round cannot overlap the next,
    /// and the staleness budget above *that* so a single slow round does not
    /// coarsen every component to `stale`. A deployment that wants a faster
    /// diagnostic lowers `[control_plane] operation_timeout_ms`, which is the
    /// honest lever: status can only be as prompt as the backend it reports on.
    ///
    /// The boot cadence is bounded by [`MAX_REFRESH_INTERVAL`], and a runtime
    /// queue-derived wait is bounded by [`MAX_PROBE_TIMEOUT`]. A queue deeper
    /// than the monitoring pipeline can retain a refresh sample is cut off and
    /// published as a timeout rather than making a live refresher look stalled.
    pub fn pacing(settings: &ControlPlaneSettings) -> StatusSettings {
        // Reserve one operation ahead of the first probe. The live Postgres
        // implementation replaces this fallback with the current queue-aware
        // timeout on every round.
        let bounds = settings.status_probe_timeout(1);
        let spacing = settings.connect_timeout.clamp(SPACING, MAX_SPACING);
        let pacing = derived(
            Component::ControlPlane,
            bounds,
            spacing,
            MIN_REFRESH_INTERVAL,
        );
        if pacing.probe_timeout < bounds {
            // Said once, at construction, and named as configuration rather than
            // as an outage: an operator who later reads `timeout` on the status
            // page has a boot log line saying the diagnostic will not wait as
            // long as `[control_plane]` allows the store to take.
            tracing::warn!(
                component = "control_plane",
                store_bound_ms = bounds.as_millis() as u64,
                probe_timeout_ms = pacing.probe_timeout.as_millis() as u64,
                refresh_interval_ms = pacing.refresh_interval.as_millis() as u64,
                "control plane timeouts exceed the observable cadence; the probe will report \
                 timeout for calls the store is still entitled to complete"
            );
        }
        pacing
    }

    async fn observe_with_status_probe(
        &self,
        admission: Option<StatusProbeAdmission>,
    ) -> ComponentObservation {
        match self.store.health_with_status_probe(admission).await {
            Ok(()) => ComponentObservation::ok(Component::ControlPlane),
            Err(error) => {
                let reason = StatusReason::from_failure(error.category());
                let detail = format!("{}: {error}", self.store.name());
                if reason == StatusReason::Unreachable {
                    ComponentObservation::unavailable(Component::ControlPlane, reason, detail)
                } else {
                    ComponentObservation::degraded(Component::ControlPlane, reason, detail)
                }
            }
        }
    }
}

/// The gap between a round's ceiling and the next round. Taken from the store's
/// own connect bound, clamped: a second is enough to keep rounds from
/// overlapping, and half a minute is already more idle time than a diagnostic
/// needs between observations.
const SPACING: Duration = Duration::from_secs(1);
const MAX_SPACING: Duration = Duration::from_secs(30);

/// The slowest the control plane may be observed.
///
/// A property of this pacing, not a fleet-wide policy: it is applied where the
/// cadence is derived from operator configuration, which today is the control
/// plane alone. A component whose pacing is derived from something else states
/// its own bounds, and the coupling below is what any of them has to satisfy.
///
/// The stall rule reads the *absence* of `axond_status_refreshes`, and absence
/// is the exporter's decision: it holds the last sample for `metric_expiration`
/// (5m in the shipped pipeline) and the rule looks back 10m. A cadence at or
/// above those windows leaves a hole in the series every cycle, which is the
/// same signal a refresher that died leaves — so a deployment with a very
/// generous `operation_timeout_ms` would page continuously while healthy. Kept
/// below the exporter's window, and pinned against it by
/// `the_derived_cadence_cannot_outrun_the_pipeline_that_watches_it`.
///
/// Two minutes rather than something closer to the exporter's window because
/// the budget below has to cover a whole publication gap (an interval plus a
/// round) *and* stay under the rule's threshold: a slower cadence than this
/// cannot satisfy both, and would call a control plane stale that is being
/// observed exactly as configured.
pub const MAX_REFRESH_INTERVAL: Duration = Duration::from_secs(2 * 60);

/// The longest one queue-aware probe may wait before it must publish a result.
///
/// This stays below the shipped five-minute metric expiration, so even a deep
/// administrative queue cannot make `axond_status_refreshes` disappear and
/// trigger `AxondStatusRefresherStalled` merely because the health call is still
/// waiting for its turn.
pub const MAX_PROBE_TIMEOUT: Duration = Duration::from_secs(4 * 60);

/// The oldest a control-plane observation may be before the replica itself
/// calls it `stale`.
///
/// Scoped the same way: derived pacings clamp themselves here so that no
/// component's own definition of stale outlives the rule that pages on it. It
/// is not applied to a registry built with explicit settings, which is the
/// tests' shape and states its budget outright.
///
/// Held at or below `AxondStatusObservationsStale`'s threshold so the two agree
/// on the word: an operator paged for a stale observation must find the
/// component reported `degraded`/`stale` when they read
/// `GET /admin/v1/status`, not an `ok` the registry still believes in. Pinned
/// against the rule by
/// `the_derived_cadence_cannot_outrun_the_pipeline_that_watches_it`.
pub const MAX_STALENESS_BUDGET: Duration = Duration::from_secs(5 * 60);

/// The pacing a component with these bounds may be observed on.
///
/// One function rather than one per probe, because the couplings it enforces are
/// the registry's, not any component's: a probe timeout strictly below the
/// interval that schedules it (so rounds cannot overlap), a staleness budget
/// above the longest gap two publications can be apart (so a slow round is not
/// reported as stale), and both under the ceilings the alerting pipeline can
/// see. `bound` is what the backend is entitled to take; `floor` is the fastest
/// cadence the component is worth observing on.
fn derived(
    component: Component,
    bound: Duration,
    spacing: Duration,
    floor: Duration,
) -> StatusSettings {
    let refresh_interval = bound
        .saturating_add(spacing)
        .max(floor)
        .min(MAX_REFRESH_INTERVAL);
    // Strictly below the interval, and unchanged from the backend's own bound
    // wherever the ceiling does not bite.
    let probe_timeout = bound.min(refresh_interval.saturating_sub(SPACING));
    StatusSettings {
        probe_timeout,
        refresh_interval,
        // Three rounds: one slow round and one missed round are not a stale
        // observation, and the rule that pages for a refresher which stopped
        // entirely is `AxondStatusRefresherStalled`. Never below the longest gap
        // two publications can be apart either — the loop waits an interval
        // *after* a round finishes, so that gap is the interval plus a round, not
        // the interval — and never past [`MAX_STALENESS_BUDGET`], so the replica
        // has already coarsened the component to `stale` by the time the age rule
        // pages for it.
        staleness_budget: refresh_interval
            .saturating_mul(3)
            .max(
                refresh_interval
                    .saturating_add(probe_timeout)
                    .saturating_add(spacing),
            )
            .min(MAX_STALENESS_BUDGET),
        enabled: vec![component],
    }
}

/// The fastest a request-path store is worth observing.
///
/// A `PING` answers in a millisecond, so the bound alone would put the
/// diagnostic on a one-second loop against a store the request path is already
/// using — and a store that is down denies requests, which is a far louder
/// signal than a status gauge. So the cadence is the metric export cadence
/// ([`EXPORT_INTERVAL`]): fresh enough that the component's state is never more
/// than one export behind what an alert rule reads, without turning the
/// diagnostic into traffic.
///
/// [`EXPORT_INTERVAL`]: super::registry::EXPORT_INTERVAL
pub const BACKEND_REFRESH_FLOOR: Duration = super::registry::EXPORT_INTERVAL;

/// Observes one request-path store through the reachability handle that store
/// exposes.
///
/// The component and the handle are separate arguments because the mapping is
/// the deployment's, not the store's: the same Redis implementation backs
/// [`Component::BudgetStore`], [`Component::RateLimitStore`], and
/// [`Component::RevocationStore`], and a deployment that points all three at one
/// server still wants three answers — the caps it enforces, the leases it
/// grants, and the tokens it refuses are three different operator problems even
/// when they share a socket.
pub struct BackendProbe {
    component: Component,
    health: Arc<dyn BackendHealth>,
}

impl BackendProbe {
    pub fn new(component: Component, health: Arc<dyn BackendHealth>) -> Self {
        Self { component, health }
    }

    /// The pacing this store's own bounds allow, for the component it backs.
    pub fn pacing(component: Component, health: &Arc<dyn BackendHealth>) -> StatusSettings {
        derived(
            component,
            health.bound().min(MAX_PROBE_TIMEOUT),
            SPACING,
            BACKEND_REFRESH_FLOOR,
        )
    }
}

#[async_trait]
impl ComponentProbe for BackendProbe {
    fn component(&self) -> Component {
        self.component
    }

    fn begin<'a>(
        &'a self,
        _fallback: Duration,
    ) -> (
        Duration,
        std::pin::Pin<Box<dyn std::future::Future<Output = ComponentObservation> + Send + 'a>>,
    ) {
        // This store's own bound rather than the registry's fallback: the
        // registry's is the merge of every enabled component's, so a Redis store
        // would inherit a Postgres store's patience and report an outage minutes
        // late — or, with the merge the other way, be cut off mid-call.
        let timeout = self.health.bound().min(MAX_PROBE_TIMEOUT);
        (timeout, Box::pin(self.observe()))
    }

    async fn observe(&self) -> ComponentObservation {
        match self.health.check().await {
            Ok(()) => ComponentObservation::ok(self.component),
            Err(failure) => {
                let reason = StatusReason::from_failure(failure.category());
                let detail = format!("{}: {}", self.health.backend(), failure.detail());
                if reason == StatusReason::Unreachable {
                    ComponentObservation::unavailable(self.component, reason, detail)
                } else {
                    // A store that answered and refused is impaired, not gone:
                    // `degraded` keeps the critical unreachability alert for an
                    // outage and routes a rotated credential to whoever owns it.
                    ComponentObservation::degraded(self.component, reason, detail)
                }
            }
        }
    }
}

#[async_trait]
impl ComponentProbe for ControlPlaneProbe {
    fn component(&self) -> Component {
        Component::ControlPlane
    }

    fn begin<'a>(
        &'a self,
        fallback: Duration,
    ) -> (
        Duration,
        std::pin::Pin<Box<dyn std::future::Future<Output = ComponentObservation> + Send + 'a>>,
    ) {
        let admission = self.store.status_probe_admission();
        let timeout = admission
            .as_ref()
            .map(StatusProbeAdmission::timeout)
            .unwrap_or(fallback)
            .min(MAX_PROBE_TIMEOUT);
        (timeout, Box::pin(self.observe_with_status_probe(admission)))
    }

    async fn observe(&self) -> ComponentObservation {
        self.observe_with_status_probe(None).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::control_plane::ControlPlaneError;
    use crate::backends::health::HealthFailure;
    use crate::backends::{Capabilities, FailureCategory};
    use crate::desired_state::oracle::InMemoryControlPlane;
    use crate::desired_state::{
        AccessDenial, AuditEvent, DenialPage, LoadedRevision, RevisionCandidate, RevisionId,
        RevisionManifest,
    };
    use crate::status::ComponentState;
    use crate::status::registry::{CachedStatusRegistry, StatusRefresher};

    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tracing_subscriber::layer::SubscriberExt as _;

    type Health = Box<dyn Fn() -> Result<(), ControlPlaneError> + Send + Sync>;

    /// Everything a subscriber wrote, as a string.
    #[derive(Clone, Default)]
    struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

    impl CapturedLogs {
        fn rendered(&self) -> String {
            String::from_utf8(self.0.lock().expect("not poisoned").clone()).expect("utf-8 logs")
        }
    }

    impl std::io::Write for CapturedLogs {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().expect("not poisoned").extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for CapturedLogs {
        type Writer = Self;

        fn make_writer(&'writer self) -> Self::Writer {
            self.clone()
        }
    }

    /// The in-memory oracle with a `health` answer of the test's choosing, so
    /// each failure category can be classified without a database. Built as a
    /// closure because [`ControlPlaneError`] is not `Clone`.
    struct Answering {
        inner: Arc<InMemoryControlPlane>,
        health: Health,
        health_delay: Option<Duration>,
        probe_timeout: Option<Duration>,
    }

    #[async_trait]
    impl ControlPlaneStore for Answering {
        fn name(&self) -> &'static str {
            self.inner.name()
        }

        fn capabilities(&self) -> Capabilities {
            self.inner.capabilities()
        }

        fn status_probe_admission(&self) -> Option<StatusProbeAdmission> {
            self.probe_timeout.map(StatusProbeAdmission::standalone)
        }

        async fn health(&self) -> Result<(), ControlPlaneError> {
            if let Some(delay) = self.health_delay {
                tokio::time::sleep(delay).await;
            }
            (self.health)()
        }

        async fn desired_revision(&self) -> Result<Option<RevisionId>, ControlPlaneError> {
            self.inner.desired_revision().await
        }

        async fn load_manifest(
            &self,
            id: RevisionId,
        ) -> Result<RevisionManifest, ControlPlaneError> {
            self.inner.load_manifest(id).await
        }

        async fn load_revision(&self, id: RevisionId) -> Result<LoadedRevision, ControlPlaneError> {
            self.inner.load_revision(id).await
        }

        async fn publish_revision(
            &self,
            candidate: RevisionCandidate,
        ) -> Result<RevisionManifest, ControlPlaneError> {
            self.inner.publish_revision(candidate).await
        }

        async fn audit_trail(&self, id: RevisionId) -> Result<Vec<AuditEvent>, ControlPlaneError> {
            self.inner.audit_trail(id).await
        }

        async fn record_denial(&self, denial: &AccessDenial) -> Result<(), ControlPlaneError> {
            self.inner.record_denial(denial).await
        }

        async fn denials(
            &self,
            page: &DenialPage,
            limit: usize,
        ) -> Result<Vec<AccessDenial>, ControlPlaneError> {
            self.inner.denials(page, limit).await
        }
    }

    fn probing(health: Health) -> ControlPlaneProbe {
        ControlPlaneProbe::new(Arc::new(Answering {
            inner: Arc::new(InMemoryControlPlane::new()),
            health,
            health_delay: None,
            probe_timeout: None,
        }))
    }

    fn healthy() -> Health {
        Box::new(|| Ok(()))
    }

    fn failing(error: fn() -> ControlPlaneError) -> Health {
        Box::new(move || Err(error()))
    }

    /// The bug this guards is a page that is *caused* by the diagnostic: the
    /// store serialises work on one client, so a probe can queue behind an
    /// administrative operation entitled to the whole `operation_timeout`,
    /// reconnect, and only then run — all of which the store considers healthy.
    /// A round cut off first would publish `unavailable`/`timeout` and fire the
    /// critical control-plane rule while administration is succeeding.
    #[test]
    fn the_probe_outlives_every_bound_the_store_is_allowed_to_take() {
        let settings = ControlPlaneSettings {
            connect_timeout: Duration::from_secs(5),
            operation_timeout: Duration::from_secs(30),
            ..ControlPlaneSettings::default()
        };
        let pacing = ControlPlaneProbe::pacing(&settings);
        let queued_behind_an_operation = settings.operation_timeout;
        let reconnect = settings.connect_timeout;
        let own_call = settings.operation_timeout;
        assert!(
            pacing.probe_timeout >= queued_behind_an_operation + reconnect + own_call,
            "{:?} cuts a call the store would have completed",
            pacing.probe_timeout
        );
    }

    #[test]
    fn the_probe_timeout_expands_for_every_operation_already_in_the_queue() {
        let settings = ControlPlaneSettings {
            connect_timeout: Duration::from_secs(5),
            operation_timeout: Duration::from_secs(30),
            ..ControlPlaneSettings::default()
        };
        let queued = 3;
        let expected = settings.status_probe_timeout(queued);
        let probe = ControlPlaneProbe::new(Arc::new(Answering {
            inner: Arc::new(InMemoryControlPlane::new()),
            health: healthy(),
            health_delay: None,
            probe_timeout: Some(expected),
        }));

        let (timeout, observation) = probe.begin(Duration::from_secs(1));
        drop(observation);
        assert_eq!(
            timeout, expected,
            "the health probe must budget for all queued operations, not one fixed slot"
        );
        assert_eq!(expected, Duration::from_secs(125));
    }

    #[test]
    fn a_deep_queue_cannot_extend_a_probe_past_metric_expiration() {
        let settings = ControlPlaneSettings {
            connect_timeout: Duration::from_secs(5),
            operation_timeout: Duration::from_secs(30),
            ..ControlPlaneSettings::default()
        };
        let deep_queue = 100;
        let uncapped = settings.status_probe_timeout(deep_queue);
        let probe = ControlPlaneProbe::new(Arc::new(Answering {
            inner: Arc::new(InMemoryControlPlane::new()),
            health: healthy(),
            health_delay: None,
            probe_timeout: Some(uncapped),
        }));

        let (timeout, observation) = probe.begin(Duration::from_secs(1));
        drop(observation);
        assert!(uncapped > MAX_PROBE_TIMEOUT);
        assert_eq!(timeout, MAX_PROBE_TIMEOUT);
        assert!(MAX_PROBE_TIMEOUT < Duration::from_secs(5 * 60));
    }

    /// The queue-aware timeout is part of the refresher contract, not just a
    /// number returned by the Postgres settings helper. A fair queue may make a
    /// healthy health call take the full budget for every operation already in
    /// front of it; the refresher must let that call finish instead of
    /// publishing a synthetic timeout that feeds the control-plane alert.
    #[tokio::test(start_paused = true)]
    async fn a_queued_healthy_probe_is_not_recorded_as_a_timeout() {
        let settings = ControlPlaneSettings {
            connect_timeout: Duration::from_secs(5),
            operation_timeout: Duration::from_secs(30),
            ..ControlPlaneSettings::default()
        };
        let queued = 3;
        let probe_timeout = settings.status_probe_timeout(queued);
        let health_delay = probe_timeout - Duration::from_secs(1);
        let probe = ControlPlaneProbe::new(Arc::new(Answering {
            inner: Arc::new(InMemoryControlPlane::new()),
            health: healthy(),
            health_delay: Some(health_delay),
            probe_timeout: Some(probe_timeout),
        }));
        let registry = Arc::new(CachedStatusRegistry::new(
            StatusSettings {
                refresh_interval: probe_timeout + Duration::from_secs(1),
                probe_timeout: Duration::from_secs(1),
                staleness_budget: Duration::from_secs(300),
                enabled: vec![Component::ControlPlane],
            },
            Arc::new(crate::convergence::SystemClock),
        ));
        let refresher = StatusRefresher::new(Arc::clone(&registry), vec![Arc::new(probe)]);

        let round = tokio::spawn(async move { refresher.refresh_once().await });
        tokio::task::yield_now().await;
        tokio::time::advance(health_delay).await;
        round.await.expect("the refresher does not panic");

        let observed = registry
            .view()
            .components
            .into_iter()
            .find(|observed| observed.component == Component::ControlPlane)
            .expect("control plane is reported");
        assert_eq!(observed.state, ComponentState::Ok);
        assert_eq!(observed.reason, None);
    }

    /// Every derived pacing has to satisfy the registry's own invariants, or the
    /// replica boots with a refresher whose rounds overlap or whose observations
    /// are stale the moment they are published. Checked across the range an
    /// operator can configure, including the sub-second bounds that would
    /// otherwise fall under the one-second floor.
    #[test]
    fn the_derived_pacing_is_valid_for_every_configurable_bound() {
        for (connect_ms, operation_ms) in [
            (1_u64, 1_u64),
            (100, 250),
            (5_000, 30_000),
            (60_000, 600_000),
        ] {
            let settings = ControlPlaneSettings {
                connect_timeout: Duration::from_millis(connect_ms),
                operation_timeout: Duration::from_millis(operation_ms),
                ..ControlPlaneSettings::default()
            };
            let pacing = ControlPlaneProbe::pacing(&settings);
            assert_eq!(
                pacing.validate(),
                Ok(()),
                "connect {connect_ms}ms, operation {operation_ms}ms produced {pacing:?}"
            );
            assert_eq!(pacing.enabled, vec![Component::ControlPlane]);
            // A cadence past the exporter's window is silence, and silence is
            // what the stall rule pages for.
            assert!(
                pacing.refresh_interval <= MAX_REFRESH_INTERVAL,
                "connect {connect_ms}ms, operation {operation_ms}ms outruns the pipeline: \
                 {pacing:?}"
            );
            // A round is scheduled an interval after the last one *finished*, so
            // two publications can be that far apart plus a whole round; a budget
            // under that calls a control plane stale that is being observed
            // exactly as configured.
            assert!(
                pacing.staleness_budget > pacing.refresh_interval + pacing.probe_timeout,
                "connect {connect_ms}ms, operation {operation_ms}ms would report stale between two \
                 healthy rounds: {pacing:?}"
            );
            // And the registry's own definition of stale stays inside the one
            // the shipped rule pages on.
            assert!(
                pacing.staleness_budget <= MAX_STALENESS_BUDGET,
                "connect {connect_ms}ms, operation {operation_ms}ms would page for an observation \
                 the replica still calls fresh: {pacing:?}"
            );
        }
    }

    /// The one case where the derivation stops honouring the store's bounds, so
    /// it is stated rather than implied: past the cap the probe is cut off, the
    /// round is published as a timeout, and construction says so in the log.
    #[test]
    fn a_store_slower_than_the_pipeline_is_capped_and_the_capping_is_announced() {
        let settings = ControlPlaneSettings {
            connect_timeout: Duration::from_secs(60),
            operation_timeout: Duration::from_secs(600),
            ..ControlPlaneSettings::default()
        };
        let bounds = settings.operation_timeout * 2 + settings.connect_timeout;

        let logs = CapturedLogs::default();
        let dispatch = tracing::Dispatch::new(
            tracing_subscriber::registry().with(
                tracing_subscriber::fmt::layer()
                    .with_ansi(false)
                    .with_writer(logs.clone()),
            ),
        );

        let pacing = {
            let _default = tracing::dispatcher::set_default(&dispatch);
            ControlPlaneProbe::pacing(&settings)
        };
        assert_eq!(pacing.refresh_interval, MAX_REFRESH_INTERVAL);
        assert_eq!(pacing.probe_timeout, MAX_REFRESH_INTERVAL - SPACING);
        assert!(pacing.probe_timeout < bounds);
        assert_eq!(pacing.staleness_budget, MAX_STALENESS_BUDGET);
        // Even at the cap, a whole publication gap fits inside the budget.
        assert!(pacing.staleness_budget > pacing.refresh_interval + pacing.probe_timeout);
        assert_eq!(pacing.validate(), Ok(()));

        let rendered = logs.rendered();
        assert!(
            rendered.contains("exceed the observable cadence") && rendered.contains("WARN"),
            "capping the probe below the store's bounds is announced: {rendered}"
        );

        // And the configuration that fits says nothing, so the line means what
        // it says when it appears.
        let quiet = CapturedLogs::default();
        let dispatch = tracing::Dispatch::new(
            tracing_subscriber::registry().with(
                tracing_subscriber::fmt::layer()
                    .with_ansi(false)
                    .with_writer(quiet.clone()),
            ),
        );
        {
            let _default = tracing::dispatcher::set_default(&dispatch);
            ControlPlaneProbe::pacing(&ControlPlaneSettings::default());
        }
        assert_eq!(quiet.rendered(), "");
    }

    #[tokio::test]
    async fn a_reachable_control_plane_is_ok_and_says_nothing_else() {
        let observation = probing(healthy()).observe().await;
        assert_eq!(observation.state, ComponentState::Ok);
        assert_eq!(observation.reason, None);
        // An `ok` with a detail would be a log line per component per round.
        assert_eq!(observation.detail, None);
    }

    /// The distinction an operator acts on: a control plane that cannot be
    /// reached is an outage of the administrative path, while one that answers
    /// and refuses is a configuration or storage problem on a reachable
    /// dependency. Reporting both as `unavailable` would send the second one to
    /// the wrong runbook section.
    #[tokio::test]
    async fn unreachable_and_refusing_are_different_observations() {
        let unreachable = probing(failing(|| ControlPlaneError::Unavailable {
            backend: "postgres",
            message: "connection refused".to_owned(),
        }))
        .observe()
        .await;
        assert_eq!(unreachable.state, ComponentState::Unavailable);
        assert_eq!(unreachable.reason, Some(StatusReason::Unreachable));

        let refusing = probing(failing(|| ControlPlaneError::Denied {
            backend: "postgres",
            message: "permission denied for relation revisions".to_owned(),
        }))
        .observe()
        .await;
        assert_eq!(refusing.state, ComponentState::Degraded);
        assert_eq!(refusing.reason, Some(StatusReason::PermissionDenied));
    }

    /// The backend's message is for the log, and the response has nowhere to put
    /// it: every field of the projection is an enum or a number. This pins the
    /// half that is easy to lose — that the detail is *collected* — since the
    /// redaction half is enforced by the response types.
    #[tokio::test]
    async fn the_backend_message_stays_on_the_detail() {
        let observation = probing(failing(|| ControlPlaneError::Unavailable {
            backend: "postgres",
            message: "host=db.internal port=5432: connection refused".to_owned(),
        }))
        .observe()
        .await;
        let detail = observation.detail.expect("a failure carries a detail");
        assert!(detail.contains("connection refused"), "{detail}");
    }

    // ------------------------------------------------------- request-path stores

    /// A store's reachability handle with an answer of the test's choosing, and a
    /// record of how often it was asked. Neither the trait nor this fake takes an
    /// input, which is the property the seam exists to have: there is nowhere to
    /// put a tenant, a key, or a `jti`.
    struct Answer {
        result: Box<dyn Fn() -> Result<(), HealthFailure> + Send + Sync>,
        bound: Duration,
        delay: Option<Duration>,
        checks: Arc<AtomicUsize>,
    }

    impl Answer {
        fn new(result: impl Fn() -> Result<(), HealthFailure> + Send + Sync + 'static) -> Self {
            Self {
                result: Box::new(result),
                bound: Duration::from_secs(5),
                delay: None,
                checks: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn reachable() -> Self {
            Self::new(|| Ok(()))
        }
    }

    #[async_trait]
    impl BackendHealth for Answer {
        fn backend(&self) -> &'static str {
            "redis"
        }

        fn bound(&self) -> Duration {
            self.bound
        }

        async fn check(&self) -> Result<(), HealthFailure> {
            self.checks.fetch_add(1, Ordering::Relaxed);
            if let Some(delay) = self.delay {
                tokio::time::sleep(delay).await;
            }
            (self.result)()
        }
    }

    fn observing(component: Component, health: Answer) -> BackendProbe {
        BackendProbe::new(component, Arc::new(health))
    }

    #[tokio::test]
    async fn a_reachable_store_is_ok_and_says_nothing_else() {
        let observation = observing(Component::RateLimitStore, Answer::reachable())
            .observe()
            .await;
        assert_eq!(observation.component, Component::RateLimitStore);
        assert_eq!(observation.state, ComponentState::Ok);
        assert_eq!(observation.reason, None);
        assert_eq!(observation.detail, None);
    }

    /// The same distinction the control plane draws, for the same reason: a store
    /// that refused a rotated credential is a credential to fix, and routing it
    /// to the unreachability alert sends the page to the wrong owner.
    #[tokio::test]
    async fn a_store_that_answers_and_refuses_is_degraded_not_unavailable() {
        let unreachable = observing(
            Component::BudgetStore,
            Answer::new(|| Err(HealthFailure::unavailable("connection refused"))),
        )
        .observe()
        .await;
        assert_eq!(unreachable.state, ComponentState::Unavailable);
        assert_eq!(unreachable.reason, Some(StatusReason::Unreachable));

        let refusing = observing(
            Component::BudgetStore,
            Answer::new(|| {
                Err(HealthFailure::new(
                    FailureCategory::Denied,
                    "WRONGPASS invalid username-password pair",
                ))
            }),
        )
        .observe()
        .await;
        assert_eq!(refusing.state, ComponentState::Degraded);
        assert_eq!(refusing.reason, Some(StatusReason::PermissionDenied));
    }

    /// The detail names the implementation and the failure for the operator log,
    /// and the response has nowhere to put either: every projected field is an
    /// enum or a number.
    #[tokio::test]
    async fn the_stores_message_stays_on_the_detail() {
        let observation = observing(
            Component::RevocationStore,
            Answer::new(|| Err(HealthFailure::unavailable("io error: connection reset"))),
        )
        .observe()
        .await;
        let detail = observation.detail.expect("a failure carries a detail");
        assert!(detail.starts_with("redis: "), "{detail}");
        assert!(detail.contains("connection reset"), "{detail}");
    }

    /// A fast store must not inherit a slow one's patience. The registry's
    /// interval is the slowest enabled component's, so a `PING` handed the
    /// fallback would be given a minute before the refresher called it late —
    /// long enough for a Redis outage to go unreported through several export
    /// windows.
    #[test]
    fn a_store_is_probed_under_its_own_bound_not_the_registrys() {
        let mut health = Answer::reachable();
        health.bound = Duration::from_secs(3);
        let probe = observing(Component::RateLimitStore, health);
        let (timeout, _) = probe.begin(Duration::from_secs(90));
        assert_eq!(timeout, Duration::from_secs(3));
    }

    /// However patient a store's own configuration is, one round has to stay
    /// inside what the monitoring pipeline can see, exactly as the control
    /// plane's does.
    #[test]
    fn a_stores_pacing_is_valid_and_bounded_however_it_is_configured() {
        for bound in [
            Duration::from_millis(1),
            Duration::from_secs(5),
            Duration::from_secs(60 * 60),
        ] {
            let mut answer = Answer::reachable();
            answer.bound = bound;
            let health: Arc<dyn BackendHealth> = Arc::new(answer);
            let pacing = BackendProbe::pacing(Component::BudgetStore, &health);
            assert_eq!(pacing.validate(), Ok(()), "{bound:?}");
            assert!(
                pacing.refresh_interval >= BACKEND_REFRESH_FLOOR,
                "{bound:?}"
            );
            assert!(pacing.refresh_interval <= MAX_REFRESH_INTERVAL, "{bound:?}");
            assert!(pacing.probe_timeout <= MAX_PROBE_TIMEOUT, "{bound:?}");
            assert!(pacing.staleness_budget <= MAX_STALENESS_BUDGET, "{bound:?}");
            assert_eq!(pacing.enabled, vec![Component::BudgetStore]);
        }
    }

    /// A probe the refresher abandoned still publishes, and the store is asked
    /// once per round rather than once per reader: the status route reads the
    /// cache, so a poller cannot turn itself into load on the budget store.
    #[tokio::test(start_paused = true)]
    async fn a_store_is_asked_once_per_round_and_a_stuck_check_is_abandoned() {
        let mut answer = Answer::reachable();
        answer.bound = Duration::from_millis(50);
        answer.delay = Some(Duration::from_secs(30));
        let checks = Arc::clone(&answer.checks);
        let health: Arc<dyn BackendHealth> = Arc::new(answer);
        let pacing = BackendProbe::pacing(Component::RateLimitStore, &health);
        let registry = Arc::new(CachedStatusRegistry::new(
            pacing,
            Arc::new(crate::convergence::SystemClock),
        ));
        let refresher = StatusRefresher::new(
            Arc::clone(&registry),
            vec![Arc::new(BackendProbe::new(
                Component::RateLimitStore,
                health,
            ))],
        );

        refresher.refresh_once().await;
        assert_eq!(checks.load(Ordering::Relaxed), 1);
        let component = registry
            .view()
            .components
            .into_iter()
            .find(|component| component.component == Component::RateLimitStore)
            .expect("the enabled component is reported");
        assert_eq!(component.state, ComponentState::Unavailable);
        assert_eq!(component.reason, Some(StatusReason::Timeout));

        // Reading the cache again asks the store nothing.
        let _ = registry.view();
        assert_eq!(checks.load(Ordering::Relaxed), 1);
    }
}
