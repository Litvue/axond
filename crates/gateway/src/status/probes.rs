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
use super::registry::{ComponentProbe, StatusSettings};
use super::{Component, ComponentObservation, StatusReason};
use crate::backends::BackendFailure;
use crate::backends::control_plane::ControlPlaneStore;
use crate::backends::control_plane::postgres::ControlPlaneSettings;

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
    /// The refresh interval sits above that so a round cannot overlap the next,
    /// and the staleness budget above *that* so a single slow round does not
    /// coarsen every component to `stale`. A deployment that wants a faster
    /// diagnostic lowers `[control_plane] operation_timeout_ms`, which is the
    /// honest lever: status can only be as prompt as the backend it reports on.
    ///
    /// It is bounded above by [`MAX_REFRESH_INTERVAL`] regardless: a cadence
    /// slower than the exporter holds a series for is indistinguishable from a
    /// refresher that stopped, so an operation bound generous enough to outrun
    /// the monitoring pipeline buys a permanent `AxondStatusRefresherStalled`
    /// page rather than a patient probe. Past that point the probe is cut off
    /// early and the round is published as a timeout — an honest "this control
    /// plane is slower than a diagnostic can wait for", where silence would say
    /// nothing at all.
    pub fn pacing(settings: &ControlPlaneSettings) -> StatusSettings {
        let bounds = settings
            .operation_timeout
            .saturating_mul(2)
            .saturating_add(settings.connect_timeout)
            // A configuration with sub-second bounds still has to leave
            // `refresh_interval` at the one-second floor `validate` requires.
            .max(Duration::from_secs(2));
        let spacing = settings.connect_timeout.clamp(SPACING, MAX_SPACING);
        let refresh_interval = bounds.saturating_add(spacing).min(MAX_REFRESH_INTERVAL);
        // Still strictly below the interval, so rounds cannot overlap, and
        // unchanged from the store's own bounds wherever the cap does not bite.
        let probe_timeout = bounds.min(refresh_interval.saturating_sub(SPACING));
        StatusSettings {
            probe_timeout,
            refresh_interval,
            // Three rounds: one slow round and one missed round are not a stale
            // observation, and the rule that pages for a refresher which stopped
            // entirely is `AxondStatusRefresherStalled`.
            staleness_budget: refresh_interval.saturating_mul(3),
            enabled: vec![Component::ControlPlane],
        }
    }
}

/// The gap between a round's ceiling and the next round. Taken from the store's
/// own connect bound, clamped: a second is enough to keep rounds from
/// overlapping, and half a minute is already more idle time than a diagnostic
/// needs between observations.
const SPACING: Duration = Duration::from_secs(1);
const MAX_SPACING: Duration = Duration::from_secs(30);

/// The slowest a live component may be observed.
///
/// The stall rule reads the *absence* of `axond_status_refreshes`, and absence
/// is the exporter's decision: it holds the last sample for `metric_expiration`
/// (5m in the shipped pipeline) and the rule looks back 10m. A cadence at or
/// above those windows leaves a hole in the series every cycle, which is the
/// same signal a refresher that died leaves — so a deployment with a very
/// generous `operation_timeout_ms` would page continuously while healthy. Kept
/// below the exporter's window, and pinned against it by
/// `the_derived_cadence_cannot_outrun_the_pipeline_that_watches_it`.
pub const MAX_REFRESH_INTERVAL: Duration = Duration::from_secs(4 * 60);

#[async_trait]
impl ComponentProbe for ControlPlaneProbe {
    fn component(&self) -> Component {
        Component::ControlPlane
    }

    async fn observe(&self) -> ComponentObservation {
        match self.store.health().await {
            Ok(()) => ComponentObservation::ok(Component::ControlPlane),
            // Classified through the backend's own category rather than by
            // matching its variants: a store that grows a failure mode gets a
            // safe code here instead of silently falling through to `ok`.
            Err(error) => {
                let reason = StatusReason::from_failure(error.category());
                // A control plane that answers "denied" or "corrupt" is reachable
                // and wrong, which an operator triages differently from one that
                // is not there at all.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backends::Capabilities;
    use crate::backends::control_plane::ControlPlaneError;
    use crate::desired_state::oracle::InMemoryControlPlane;
    use crate::desired_state::{
        AccessDenial, AuditEvent, DenialPage, LoadedRevision, RevisionCandidate, RevisionId,
        RevisionManifest,
    };
    use crate::status::ComponentState;

    type Health = Box<dyn Fn() -> Result<(), ControlPlaneError> + Send + Sync>;

    /// The in-memory oracle with a `health` answer of the test's choosing, so
    /// each failure category can be classified without a database. Built as a
    /// closure because [`ControlPlaneError`] is not `Clone`.
    struct Answering {
        inner: Arc<InMemoryControlPlane>,
        health: Health,
    }

    #[async_trait]
    impl ControlPlaneStore for Answering {
        fn name(&self) -> &'static str {
            self.inner.name()
        }

        fn capabilities(&self) -> Capabilities {
            self.inner.capabilities()
        }

        async fn health(&self) -> Result<(), ControlPlaneError> {
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
        }
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
}
