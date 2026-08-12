//! The status contract's tests, in three groups:
//!
//! * **Redaction.** What each scope may see, asserted against golden fixtures and
//!   against a recursive sweep that refuses any string a caller could not have
//!   predicted — the negative test that would catch a DSN, a token, or a raw
//!   backend error appearing in a response.
//! * **Cache semantics.** That a read never probes, that a hung probe ages an
//!   observation instead of hanging a handler, and that staleness degrades a
//!   component rather than failing it.
//! * **Bounded vocabularies.** That every reason code is closed, that the
//!   component names match the metric catalogue's label vocabulary, and that the
//!   registry's pacings are validated.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};

use super::registry::{
    self, CachedStatusRegistry, ComponentProbe, InvalidStatusSettings, StatusRefresher,
    StatusSettings,
};
use super::*;
use crate::convergence::status::testing::ManualClock;
use crate::convergence::{Rejection, RevisionReport, SnapshotSource};
use crate::desired_state::fixtures::revision_id;
use crate::telemetry::catalog;

/// A DSN, a bearer token, and a raw backend error: everything a probe plausibly
/// learns and no caller may ever be told.
const HOSTILE_DETAIL: &str = "connect postgres://axond:s3cr3t@db.internal:5432/axond failed: FATAL password authentication \
     failed for user \"axond\" (bearer sk-live-4f9c)";

fn observed(
    component: Component,
    state: ComponentState,
    reason: Option<StatusReason>,
    age_ms: u64,
) -> Observed {
    Observed {
        component,
        state,
        reason,
        age: Duration::from_millis(age_ms),
        stale: reason == Some(StatusReason::Stale),
    }
}

/// A replica mid-incident: a stale control plane, two failing durable stores, a
/// timing-out revocation store, and credential pools under capacity pressure.
fn incident_view() -> StatusView {
    StatusView {
        components: vec![
            observed(
                Component::ControlPlane,
                ComponentState::Degraded,
                Some(StatusReason::Stale),
                45_000,
            ),
            observed(Component::Catalogue, ComponentState::Ok, None, 1_500),
            observed(
                Component::SecretStore,
                ComponentState::Unavailable,
                Some(StatusReason::SecretUnresolved),
                3_200,
            ),
            observed(
                Component::BudgetStore,
                ComponentState::Unavailable,
                Some(StatusReason::Unreachable),
                2_100,
            ),
            observed(Component::RateLimitStore, ComponentState::Ok, None, 900),
            observed(
                Component::RevocationStore,
                ComponentState::Degraded,
                Some(StatusReason::Timeout),
                4_400,
            ),
            observed(Component::UsageSink, ComponentState::Ok, None, 1_100),
            observed(
                Component::ProviderCredentials,
                ComponentState::Degraded,
                Some(StatusReason::CapacityExhausted),
                1_750,
            ),
        ],
    }
}

fn incident_revision() -> RevisionReport {
    RevisionReport {
        desired: Some(revision_id(2)),
        loaded: Some(revision_id(1)),
        active: Some(revision_id(1)),
        source: Some(SnapshotSource::ControlPlane),
        generation: 7,
        lag: Duration::from_secs(9),
        last_convergence: Some(Duration::from_millis(420)),
        consecutive_failures: 2,
        last_rejection: Some(Rejection {
            revision: Some(revision_id(2)),
            reason: "validation",
            detail: HOSTILE_DETAIL.to_owned(),
        }),
    }
}

#[test]
fn deployment_scope_matches_its_fixture() {
    let response = incident_view().project(
        StatusScope::Deployment,
        crate::shutdown::Phase::Serving,
        Some(&incident_revision()),
    );
    let expected: Value = serde_json::from_str(include_str!("fixtures/deployment.json"))
        .expect("the fixture is valid JSON");
    assert_eq!(
        serde_json::to_value(&response).expect("serializes"),
        expected
    );
}

#[test]
fn namespace_scope_matches_its_fixture() {
    let response = incident_view().project(
        StatusScope::Namespace,
        crate::shutdown::Phase::Serving,
        Some(&incident_revision()),
    );
    let expected: Value = serde_json::from_str(include_str!("fixtures/namespace.json"))
        .expect("the fixture is valid JSON");
    assert_eq!(
        serde_json::to_value(&response).expect("serializes"),
        expected
    );
}

#[test]
fn namespace_scope_drops_operator_components_reasons_and_revision() {
    let view = incident_view();
    let tenant = view.project(
        StatusScope::Namespace,
        crate::shutdown::Phase::Serving,
        Some(&incident_revision()),
    );
    // No revision summary: lag, generation, and convergence describe how the
    // operator runs the deployment.
    assert!(tenant.revision.is_none());
    let names: Vec<&str> = tenant
        .components
        .iter()
        .map(|component| component.component)
        .collect();
    for hidden in [
        Component::ControlPlane,
        Component::SecretStore,
        Component::UsageSink,
    ] {
        assert!(
            !names.contains(&hidden.as_str()),
            "{} is operator-only",
            hidden.as_str()
        );
    }
    // The control plane was the only stale component, and it is not visible to a
    // tenant — so neither is the staleness.
    assert!(view.stale());
    assert!(!tenant.stale);
    // An operator-only reason coarsens; a tenant-safe one survives.
    let reason = |component: Component| {
        tenant
            .components
            .iter()
            .find(|entry| entry.component == component.as_str())
            .and_then(|entry| entry.reason)
    };
    assert_eq!(reason(Component::BudgetStore), Some("unavailable"));
    assert_eq!(reason(Component::RevocationStore), Some("unavailable"));
    assert_eq!(
        reason(Component::ProviderCredentials),
        Some("capacity_exhausted")
    );
    // Ages are coarsened to whole seconds rather than reporting the refresher's
    // exact cadence.
    for component in &tenant.components {
        assert_eq!(component.observed_age_ms % 1_000, 0);
    }
}

#[test]
fn deployment_scope_sees_every_component_and_exact_ages() {
    let response = incident_view().project(
        StatusScope::Deployment,
        crate::shutdown::Phase::Draining,
        Some(&incident_revision()),
    );
    assert_eq!(response.components.len(), Component::ALL.len());
    assert_eq!(response.phase, "draining");
    assert!(response.stale);
    let summary = response.revision.expect("the operator sees convergence");
    assert!(!summary.converged);
    assert_eq!(summary.lag_ms, 9_000);
    assert_eq!(summary.generation, 7);
    assert_eq!(summary.reason, Some("validation_rejected"));
    assert_eq!(summary.source, Some("control-plane"));
}

/// The negative test that makes the redaction structural rather than aspirational:
/// every string in a serialized response, at any depth, must be a value the
/// contract's own vocabularies could have produced. A leaked DSN, token, raw
/// error, namespace, subject, or revision id fails here.
#[test]
fn no_response_field_can_carry_free_text() {
    let mut permitted: Vec<&str> = vec!["status", "replica"];
    permitted.extend(COMPONENTS);
    permitted.extend(ComponentState::ALL.iter().map(|state| state.as_str()));
    permitted.extend(StatusReason::ALL.iter().map(|reason| reason.code()));
    permitted.extend([
        StatusScope::Namespace.as_str(),
        StatusScope::Deployment.as_str(),
    ]);
    permitted.extend(
        [
            crate::shutdown::Phase::Serving,
            crate::shutdown::Phase::Draining,
            crate::shutdown::Phase::Closing,
        ]
        .iter()
        .map(|phase| phase.as_str()),
    );
    permitted.extend([
        SnapshotSource::ControlPlane.as_str(),
        SnapshotSource::LastKnownGood.as_str(),
    ]);

    fn strings(value: &Value, found: &mut Vec<String>) {
        match value {
            Value::String(text) => found.push(text.clone()),
            Value::Array(values) => values.iter().for_each(|value| strings(value, found)),
            Value::Object(entries) => entries.values().for_each(|value| strings(value, found)),
            _ => {}
        }
    }

    for scope in [StatusScope::Namespace, StatusScope::Deployment] {
        let response = incident_view().project(
            scope,
            crate::shutdown::Phase::Serving,
            Some(&incident_revision()),
        );
        let serialized = serde_json::to_value(&response).expect("serializes");
        let mut found = Vec::new();
        strings(&serialized, &mut found);
        for text in &found {
            assert!(
                permitted.contains(&text.as_str()),
                "`{text}` is not from a bounded vocabulary"
            );
        }
        let rendered = serialized.to_string();
        for leak in [
            "postgres://",
            "s3cr3t",
            "sk-live",
            "password authentication",
            &revision_id(2).to_string(),
        ] {
            assert!(!rendered.contains(leak), "`{leak}` reached a response");
        }
    }
}

/// The operator-facing detail exists, is logged, and cannot be projected: the
/// observation carries it, the response has nowhere to put it.
#[test]
fn observation_detail_never_reaches_a_view() {
    let clock = ManualClock::new();
    let registry = registry_with(
        &clock,
        vec![Component::BudgetStore],
        Duration::from_secs(60),
    );
    registry.publish(ComponentObservation::unavailable(
        Component::BudgetStore,
        StatusReason::Unreachable,
        HOSTILE_DETAIL.to_owned(),
    ));
    let response = registry.view().project(
        StatusScope::Deployment,
        crate::shutdown::Phase::Serving,
        None,
    );
    let rendered = serde_json::to_string(&response).expect("serializes");
    assert!(!rendered.contains("postgres://"));
    assert!(rendered.contains("\"reason\":\"unreachable\""));
}

fn registry_with(
    clock: &ManualClock,
    enabled: Vec<Component>,
    staleness_budget: Duration,
) -> Arc<CachedStatusRegistry> {
    let settings = StatusSettings {
        refresh_interval: Duration::from_secs(10),
        probe_timeout: Duration::from_secs(2),
        staleness_budget,
        enabled,
    };
    settings.validate().expect("the test pacing is valid");
    Arc::new(CachedStatusRegistry::new(settings, Arc::new(clock.clone())))
}

#[test]
fn the_stateless_posture_reports_every_component_disabled() {
    let registry = CachedStatusRegistry::stateless();
    let view = registry.view();
    assert_eq!(view.components.len(), Component::ALL.len());
    for observed in &view.components {
        assert_eq!(observed.state, ComponentState::Disabled);
        assert_eq!(observed.reason, Some(StatusReason::NotConfigured));
        assert_eq!(observed.age, Duration::ZERO);
    }
    assert!(!view.stale());
}

#[test]
fn an_enabled_component_nobody_has_observed_is_not_reported_healthy() {
    let clock = ManualClock::new();
    let registry = registry_with(
        &clock,
        vec![Component::ControlPlane],
        Duration::from_secs(60),
    );
    let observed = registry
        .view()
        .components
        .into_iter()
        .find(|observed| observed.component == Component::ControlPlane)
        .expect("every component is reported");
    assert_eq!(observed.state, ComponentState::Unavailable);
    assert_eq!(observed.reason, Some(StatusReason::Unknown));
}

#[test]
fn an_observation_past_the_staleness_budget_degrades_rather_than_fails() {
    let clock = ManualClock::new();
    let registry = registry_with(&clock, vec![Component::Catalogue], Duration::from_secs(60));
    registry.publish(ComponentObservation::ok(Component::Catalogue));

    let fresh = registry.view();
    assert_eq!(fresh.components[1].state, ComponentState::Ok);
    assert!(!fresh.stale());

    clock.advance(Duration::from_secs(61));
    let stale = registry.view();
    let catalogue = stale.components[1];
    assert_eq!(catalogue.component, Component::Catalogue);
    // Degraded, not unavailable: a replica serving a valid snapshot through an
    // observation outage is stale, not down.
    assert_eq!(catalogue.state, ComponentState::Degraded);
    assert_eq!(catalogue.reason, Some(StatusReason::Stale));
    assert_eq!(catalogue.age, Duration::from_secs(61));
    assert!(stale.stale());
}

/// A probe that fails the test if a *read* ever reaches it, and blocks forever
/// when the test asks it to.
struct HostileProbe {
    component: Component,
    observations: AtomicUsize,
    hang: bool,
}

impl HostileProbe {
    fn new(component: Component, hang: bool) -> Arc<Self> {
        Arc::new(Self {
            component,
            observations: AtomicUsize::new(0),
            hang,
        })
    }
}

#[async_trait]
impl ComponentProbe for HostileProbe {
    fn component(&self) -> Component {
        self.component
    }

    async fn observe(&self) -> ComponentObservation {
        self.observations.fetch_add(1, Ordering::SeqCst);
        if self.hang {
            std::future::pending::<()>().await;
        }
        ComponentObservation::ok(self.component)
    }
}

#[tokio::test]
async fn a_status_read_never_probes_a_backend() {
    let clock = ManualClock::new();
    let registry = registry_with(
        &clock,
        vec![Component::BudgetStore],
        Duration::from_secs(60),
    );
    let probe = HostileProbe::new(Component::BudgetStore, false);
    let refresher = StatusRefresher::new(Arc::clone(&registry), vec![probe.clone()]);

    // Reads, repeatedly, before anything has been observed: the handler's path
    // cannot reach `observe`, because `view` is not async and the probe is only
    // reachable from the refresher.
    for _ in 0..3 {
        let _ = registry.view();
    }
    assert_eq!(probe.observations.load(Ordering::SeqCst), 0);

    refresher.refresh_once().await;
    assert_eq!(probe.observations.load(Ordering::SeqCst), 1);
    assert_eq!(
        registry
            .view()
            .components
            .into_iter()
            .find(|observed| observed.component == Component::BudgetStore)
            .expect("reported")
            .state,
        ComponentState::Ok
    );
}

#[tokio::test(start_paused = true)]
async fn a_hung_probe_ages_an_observation_instead_of_blocking_a_read() {
    let clock = ManualClock::new();
    let registry = registry_with(
        &clock,
        vec![Component::ControlPlane],
        Duration::from_secs(60),
    );
    let probe = HostileProbe::new(Component::ControlPlane, true);
    let refresher = StatusRefresher::new(Arc::clone(&registry), vec![probe]);
    let refreshing = tokio::spawn(async move { refresher.refresh_once().await });

    // The refresh is still stuck in the probe, and a read still answers.
    tokio::task::yield_now().await;
    assert_eq!(registry.view().components.len(), Component::ALL.len());

    // The probe is abandoned at its bound and recorded as a timeout rather than
    // being waited on.
    tokio::time::advance(Duration::from_secs(3)).await;
    refreshing.await.expect("the refresher does not panic");
    let observed = registry
        .view()
        .components
        .into_iter()
        .find(|observed| observed.component == Component::ControlPlane)
        .expect("reported");
    assert_eq!(observed.state, ComponentState::Unavailable);
    assert_eq!(observed.reason, Some(StatusReason::Timeout));
}

#[tokio::test]
async fn a_probe_for_a_disabled_component_is_not_run() {
    let clock = ManualClock::new();
    let registry = registry_with(&clock, vec![Component::Catalogue], Duration::from_secs(60));
    let disabled = HostileProbe::new(Component::SecretStore, false);
    let refresher = StatusRefresher::new(Arc::clone(&registry), vec![disabled.clone()]);
    refresher.refresh_once().await;
    assert_eq!(disabled.observations.load(Ordering::SeqCst), 0);
}

#[test]
fn the_registry_refuses_pacings_that_would_make_it_a_hazard() {
    let base = StatusSettings::default();
    base.validate().expect("the default posture is valid");
    assert_eq!(
        StatusSettings {
            refresh_interval: Duration::from_millis(100),
            ..base.clone()
        }
        .validate(),
        Err(InvalidStatusSettings::RefreshTooFast)
    );
    assert_eq!(
        StatusSettings {
            probe_timeout: Duration::from_secs(30),
            ..base.clone()
        }
        .validate(),
        Err(InvalidStatusSettings::ProbeTimeoutTooLong)
    );
    assert_eq!(
        StatusSettings {
            staleness_budget: Duration::from_secs(5),
            ..base
        }
        .validate(),
        Err(InvalidStatusSettings::StalenessBudgetTooShort)
    );
}

#[test]
fn every_reason_code_is_bounded_and_distinct() {
    let mut codes: Vec<&str> = StatusReason::ALL
        .iter()
        .map(|reason| reason.code())
        .collect();
    let count = codes.len();
    codes.sort_unstable();
    codes.dedup();
    assert_eq!(codes.len(), count, "reason codes must be distinct");
    for code in codes {
        assert!(
            code.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
            "`{code}` is not a stable lower-case code"
        );
    }
}

#[test]
fn operator_only_reasons_coarsen_and_tenant_safe_ones_do_not() {
    for reason in StatusReason::ALL {
        let namespaced = reason.for_scope(StatusScope::Namespace);
        assert_eq!(reason.for_scope(StatusScope::Deployment), *reason);
        if reason.is_tenant_safe() {
            assert_eq!(namespaced, *reason);
        } else {
            assert_eq!(namespaced, StatusReason::Unavailable);
        }
        assert!(namespaced.is_tenant_safe());
    }
}

#[test]
fn backend_and_revision_failures_map_to_bounded_codes() {
    use crate::backends::FailureCategory;
    for category in [
        FailureCategory::Unavailable,
        FailureCategory::Conflict,
        FailureCategory::NotFound,
        FailureCategory::Invalid,
        FailureCategory::Denied,
        FailureCategory::Corrupt,
    ] {
        let reason = StatusReason::from_failure(category);
        assert!(StatusReason::ALL.contains(&reason));
    }
    let revision_reasons = [
        ("unavailable", StatusReason::Unreachable),
        ("corrupt", StatusReason::PayloadCorrupt),
        ("projection", StatusReason::ProjectionRejected),
        ("validation", StatusReason::ValidationRejected),
        ("secret", StatusReason::SecretUnresolved),
        ("snapshot", StatusReason::SnapshotRejected),
        ("invalid", StatusReason::ValidationRejected),
        ("not_found", StatusReason::NotConfigured),
        ("denied", StatusReason::PermissionDenied),
        // A lost write says nothing about a component's health, so it stays
        // deliberately opaque rather than borrowing a code that means something
        // else.
        ("conflict", StatusReason::Unknown),
        // A label this vocabulary does not know degrades to a safe code rather
        // than becoming a new response value.
        ("some_new_reason", StatusReason::Unknown),
    ];
    for (label, expected) in revision_reasons {
        assert_eq!(StatusReason::from_revision_reason(label), expected);
    }

    // Every reason the reconciler can emit is one this mapping decided about,
    // so the two vocabularies cannot drift silently.
    for reason in crate::convergence::reconciler::REVISION_REASONS {
        assert!(
            revision_reasons.iter().any(|(label, _)| label == reason)
                && StatusReason::ALL.contains(&StatusReason::from_revision_reason(reason)),
            "`{reason}` is a reason the reconciler emits"
        );
    }
}

/// An alert thresholds `axond.status.component_state`, so the ladder has to put
/// the default stateless posture *below* the healthy state: `disabled` is an
/// absent observation, and ranking it worst would fire every severity alert on
/// the most common deployment forever.
#[test]
fn the_state_gauge_ranks_disabled_below_ok_and_severity_above_it() {
    assert!(ComponentState::Disabled.gauge_value() < ComponentState::Ok.gauge_value());
    assert!(ComponentState::Ok.gauge_value() < ComponentState::Degraded.gauge_value());
    assert!(ComponentState::Degraded.gauge_value() < ComponentState::Unavailable.gauge_value());

    // The stateless default: nothing is configured, so no `>= degraded` alert
    // may be able to see it.
    let stateless = CachedStatusRegistry::stateless();
    let degraded = ComponentState::Degraded.gauge_value();
    for observed in stateless.view().components {
        assert_eq!(observed.state, ComponentState::Disabled);
        assert!(observed.state.gauge_value() < degraded);
    }
}

#[test]
fn component_names_match_the_metric_label_vocabulary() {
    assert_eq!(COMPONENTS.len(), Component::ALL.len());
    for (component, name) in Component::ALL.iter().zip(COMPONENTS) {
        assert_eq!(component.as_str(), *name);
    }
    for component in Component::ALL {
        catalog::validate_label_value(
            "axond.status.component_state",
            "axond.status.component",
            component.as_str(),
        )
        .expect("every component is a catalogued label value");
    }
    for outcome in [
        registry::REFRESH_OBSERVED,
        registry::REFRESH_FAILED,
        registry::REFRESH_DISABLED,
    ] {
        catalog::validate_label_value("axond.status.refreshes", "axond.status.outcome", outcome)
            .expect("every refresh outcome is a catalogued label value");
    }
}

#[test]
fn no_status_field_is_a_forbidden_metric_dimension() {
    // The response's own field names, as the fixture pins them: none may be an
    // identity the metric catalogue refuses, which is the same list of
    // unbounded, tenant-attributable dimensions.
    let response: Value = json!(incident_view().project(
        StatusScope::Deployment,
        crate::shutdown::Phase::Serving,
        Some(&incident_revision()),
    ));
    let mut keys: Vec<String> = Vec::new();
    fn collect(value: &Value, keys: &mut Vec<String>) {
        match value {
            Value::Object(entries) => {
                for (key, value) in entries {
                    keys.push(key.clone());
                    collect(value, keys);
                }
            }
            Value::Array(values) => values.iter().for_each(|value| collect(value, keys)),
            _ => {}
        }
    }
    collect(&response, &mut keys);
    for key in keys {
        assert!(
            !catalog::FORBIDDEN_LABEL_KEYS
                .iter()
                .any(|(forbidden, _)| *forbidden == key),
            "`{key}` is an identity neither a metric nor a status response may carry"
        );
    }
}
