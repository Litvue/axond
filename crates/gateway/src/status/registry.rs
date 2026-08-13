//! The cached status registry and the background refresher that fills it.
//!
//! The split is the contract: a request-facing read
//! ([`CachedStatusRegistry::view`]) is synchronous and touches nothing but an
//! in-memory map, while every probe runs from [`StatusRefresher`] on its own
//! task, on a fixed interval, under a per-probe timeout. A handler therefore
//! cannot make a backend call even by accident — [`ComponentProbe`] is only
//! reachable from the refresher, and `view` is not `async`.
//!
//! Two consequences worth stating, because they are the reason for the shape:
//!
//! * **A hung backend cannot hang a status request.** A probe that never returns
//!   is abandoned at [`StatusSettings::probe_timeout`] and recorded as
//!   [`StatusReason::Timeout`]; meanwhile `view` keeps returning the last
//!   observation with a growing age, and marks it
//!   [`StatusReason::Stale`] once it passes [`StatusSettings::staleness_budget`].
//! * **Status cannot influence inference.** Nothing here acquires an admission
//!   permit, a budget reservation, a rate-limit token, or a revocation lookup,
//!   and the registry shares no lock with the request path: the only state is
//!   this map, which the request path never reads.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tracing::{debug, warn};

use super::{Component, ComponentObservation, ComponentState, Observed, StatusReason, StatusView};
use crate::convergence::{Clock, SystemClock};
use crate::telemetry::metrics;

/// Outcomes of one refresh attempt, as the `axond.status.refreshes` counter
/// records them.
pub const REFRESH_OBSERVED: &str = "observed";
pub const REFRESH_FAILED: &str = "failed";
pub const REFRESH_DISABLED: &str = "disabled";

/// How the registry is paced and how long an observation stays usable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusSettings {
    /// How often the refresher observes every enabled component. Bounded below
    /// so a misconfiguration cannot turn the refresher into a load generator
    /// against the backends it observes.
    pub refresh_interval: Duration,
    /// How long one probe may take before it is abandoned and recorded as a
    /// timeout.
    pub probe_timeout: Duration,
    /// How old an observation may be before it is reported as stale rather than
    /// as itself.
    pub staleness_budget: Duration,
    /// The components this deployment has at all. Anything absent reports
    /// [`ComponentState::Disabled`] and is never probed — which is every durable
    /// component in the default stateless posture.
    pub enabled: Vec<Component>,
}

/// The floor on [`StatusSettings::refresh_interval`].
pub const MIN_REFRESH_INTERVAL: Duration = Duration::from_secs(1);

/// How often the view is exported as metrics, when a round is not exporting it
/// anyway. Fast enough that an age crossing a rule's threshold is seen well
/// within the rule's hold window, and it is a fixed-size export: one gauge per
/// component, never per request.
pub const EXPORT_INTERVAL: Duration = Duration::from_secs(15);

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum InvalidStatusSettings {
    #[error("status refresh interval must be at least {}s", MIN_REFRESH_INTERVAL.as_secs())]
    RefreshTooFast,
    #[error("status probe timeout must be shorter than the refresh interval")]
    ProbeTimeoutTooLong,
    #[error("status staleness budget must be longer than the refresh interval")]
    StalenessBudgetTooShort,
}

impl Default for StatusSettings {
    /// The stateless posture: nothing durable is configured, so nothing is
    /// probed and every component reports `disabled`.
    fn default() -> Self {
        Self {
            refresh_interval: Duration::from_secs(10),
            probe_timeout: Duration::from_secs(2),
            staleness_budget: Duration::from_secs(60),
            enabled: Vec::new(),
        }
    }
}

impl StatusSettings {
    /// Reject the pacings that would make the registry a hazard: a refresh loop
    /// faster than a second, a probe allowed to outlive the interval that
    /// schedules it, and a staleness budget so short that a fresh observation is
    /// stale on arrival.
    pub fn validate(&self) -> Result<(), InvalidStatusSettings> {
        if self.refresh_interval < MIN_REFRESH_INTERVAL {
            return Err(InvalidStatusSettings::RefreshTooFast);
        }
        if self.probe_timeout >= self.refresh_interval {
            return Err(InvalidStatusSettings::ProbeTimeoutTooLong);
        }
        if self.staleness_budget <= self.refresh_interval {
            return Err(InvalidStatusSettings::StalenessBudgetTooShort);
        }
        Ok(())
    }
}

/// One component's last observation, with when it was taken.
#[derive(Debug, Clone)]
struct Cached {
    state: ComponentState,
    reason: Option<StatusReason>,
    observed_at: Instant,
}

/// The cached observations a status handler reads.
///
/// Cloned behind an [`Arc`] and shared between the refresher that publishes and
/// the handler that reads. The lock is held only long enough to copy a handful of
/// enum values.
pub struct CachedStatusRegistry {
    settings: StatusSettings,
    clock: Arc<dyn Clock>,
    observations: RwLock<BTreeMap<Component, Cached>>,
}

impl CachedStatusRegistry {
    pub fn new(settings: StatusSettings, clock: Arc<dyn Clock>) -> Self {
        Self {
            settings,
            clock,
            observations: RwLock::new(BTreeMap::new()),
        }
    }

    /// A registry for the stateless posture: every component disabled, the
    /// system clock, and no probes.
    pub fn stateless() -> Self {
        Self::new(StatusSettings::default(), Arc::new(SystemClock))
    }

    pub fn settings(&self) -> &StatusSettings {
        &self.settings
    }

    /// Record one observation. Called only from [`StatusRefresher`].
    ///
    /// The observation's `detail` is logged here and dropped: this is the single
    /// point where a backend's own error text is turned into a bounded reason
    /// code, and the only place it is written down.
    pub fn publish(&self, observation: ComponentObservation) {
        let component = observation.component;
        match (&observation.state, &observation.detail) {
            (ComponentState::Ok, _) => debug!(component = component.as_str(), "component observed"),
            (state, Some(detail)) => warn!(
                component = component.as_str(),
                state = state.as_str(),
                reason = observation.reason.map(StatusReason::code),
                detail = detail.as_str(),
                "component degraded"
            ),
            (state, None) => warn!(
                component = component.as_str(),
                state = state.as_str(),
                reason = observation.reason.map(StatusReason::code),
                "component degraded"
            ),
        }
        let now = self.clock.now();
        self.observations
            .write()
            .expect("status observations lock poisoned")
            .insert(
                component,
                Cached {
                    state: observation.state,
                    reason: observation.reason,
                    observed_at: now,
                },
            );
        metrics::record_status_refresh(
            component.as_str(),
            match observation.state {
                ComponentState::Ok => REFRESH_OBSERVED,
                ComponentState::Disabled => REFRESH_DISABLED,
                _ => REFRESH_FAILED,
            },
        );
    }

    /// The cached read a status handler serves from.
    ///
    /// Synchronous, allocates one small vector, and performs no I/O — that is
    /// the contract, not an implementation detail. An unobserved enabled
    /// component reports `unavailable`/`unknown` rather than `ok`, because "we
    /// have never looked" is not evidence of health.
    pub fn view(&self) -> StatusView {
        let now = self.clock.now();
        let observations = self
            .observations
            .read()
            .expect("status observations lock poisoned");
        let components = Component::ALL
            .iter()
            .map(|component| {
                let enabled = self.settings.enabled.contains(component);
                match (enabled, observations.get(component)) {
                    (false, _) => Observed {
                        component: *component,
                        state: ComponentState::Disabled,
                        reason: Some(StatusReason::NotConfigured),
                        age: Duration::ZERO,
                        stale: false,
                    },
                    (true, None) => Observed {
                        component: *component,
                        state: ComponentState::Unavailable,
                        reason: Some(StatusReason::Unknown),
                        age: Duration::ZERO,
                        stale: false,
                    },
                    (true, Some(cached)) => {
                        let age = now.saturating_duration_since(cached.observed_at);
                        let stale = age > self.settings.staleness_budget;
                        let (state, reason) = match (stale, cached.state) {
                            (true, ComponentState::Ok | ComponentState::Degraded) => {
                                (ComponentState::Degraded, Some(StatusReason::Stale))
                            }
                            (true, state) => (state, Some(StatusReason::Stale)),
                            (false, state) => (state, cached.reason),
                        };
                        Observed {
                            component: *component,
                            state,
                            reason,
                            age,
                            stale,
                        }
                    }
                }
            })
            .collect();
        StatusView { components }
    }

    /// Export the current view as metrics, and return what was exported.
    ///
    /// Called on its own cadence rather than only after a round: a round
    /// republishes every observation, so an export tied to one always reports an
    /// age of about zero and `axond_status_observation_age` could never climb —
    /// the gauge would describe the publishing loop instead of the observations.
    /// Exporting between rounds is what makes a probe that is taking too long, or
    /// a round that never came, visible as ageing.
    pub(super) fn export(&self) -> StatusView {
        let view = self.view();
        for observed in &view.components {
            metrics::record_status_component(
                observed.component.as_str(),
                observed.state,
                observed.age,
            );
        }
        view
    }
}

/// One component's observation, implemented by the slice that owns the backend.
///
/// Deliberately reachable only from [`StatusRefresher`]: a handler that could
/// call `observe` would be a synchronous fan-out across every backend on a
/// route an orchestrator or a dashboard polls.
#[async_trait]
pub trait ComponentProbe: Send + Sync {
    fn component(&self) -> Component;

    /// Observe the backend. Called from the background refresher only, and
    /// bounded by [`StatusSettings::probe_timeout`]; an implementation reports
    /// failure as a bounded [`StatusReason`] plus an operator-facing detail
    /// rather than propagating the backend's error type.
    async fn observe(&self) -> ComponentObservation;
}

/// The background loop that keeps the registry fresh.
pub struct StatusRefresher {
    registry: Arc<CachedStatusRegistry>,
    probes: Vec<Arc<dyn ComponentProbe>>,
}

impl StatusRefresher {
    /// Build a refresher over the probes for this deployment's enabled
    /// components. A probe for a component that is not enabled is dropped: the
    /// enabled set is the deployment's configuration, and a probe list is only
    /// the code that knows how to look.
    pub fn new(registry: Arc<CachedStatusRegistry>, probes: Vec<Arc<dyn ComponentProbe>>) -> Self {
        let enabled = registry.settings().enabled.clone();
        Self {
            registry,
            probes: probes
                .into_iter()
                .filter(|probe| enabled.contains(&probe.component()))
                .collect(),
        }
    }

    /// Observe every probe once, concurrently, each under the probe timeout.
    pub async fn refresh_once(&self) {
        let timeout = self.registry.settings().probe_timeout;
        let observations = futures::future::join_all(self.probes.iter().map(|probe| async move {
            match tokio::time::timeout(timeout, probe.observe()).await {
                Ok(observation) => observation,
                // An abandoned probe is an observation like any other, so a
                // hung backend ages into `unavailable` rather than leaving the
                // last good state in place indefinitely.
                Err(_) => ComponentObservation::unavailable(
                    probe.component(),
                    StatusReason::Timeout,
                    format!("probe exceeded {}ms", timeout.as_millis()),
                ),
            }
        }))
        .await;
        for observation in observations {
            self.registry.publish(observation);
        }
        self.registry.export();
    }

    /// Refresh on the configured interval until `shutdown` resolves, exporting
    /// on [`EXPORT_INTERVAL`] independently so ageing is visible between rounds.
    pub async fn run(self, shutdown: impl std::future::Future<Output = ()> + Send) {
        let refresh_interval = self.registry.settings().refresh_interval;
        let mut ticker = tokio::time::interval(refresh_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let refreshing = async {
            loop {
                ticker.tick().await;
                self.refresh_once().await;
            }
        };

        let registry = Arc::clone(&self.registry);
        let ageing = async move {
            let mut ticker = tokio::time::interval(EXPORT_INTERVAL.min(refresh_interval));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                registry.export();
            }
        };

        tokio::select! {
            () = shutdown => {}
            () = refreshing => {}
            () = ageing => {}
        }
    }
}
