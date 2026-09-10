//! The Store-backed latency baseline (#462): the capacity harness turned on
//! the Store, for both backends, with phase-level evidence.
//!
//! Every scenario offers the same load to a real `axond` process and — where
//! that measures anything — to the fake upstream directly, and writes one
//! artifact per backend under `target/store-baseline/<tier>/` carrying the
//! percentiles, the resources, the decoded `axond.store.*` histograms, and the
//! host that produced them.
//!
//! What fails here is conservation and coverage, never latency: every offered
//! request is accepted, shed, or failed; every accepted request is charged; and
//! the Store instrumentation the optimisation issues will be measured with was
//! exported for the operations it is about. Shared-runner latency is recorded
//! as informational (ADR 0033); the controlled-runner procedure that makes it
//! comparable is in `docs/operations/store-latency-baseline.md`.
//!
//! The smoke tier runs under `cargo test`. The full tier is the baseline
//! proper, behind `AXOND_STORE_BASELINE=1`, and its Postgres arm additionally
//! needs `AXOND_TEST_POSTGRES_DSN`; without the DSN the arm skips and says so.

mod support;

use support::capacity::store_baseline::{
    self, Backend, BaselineResult, BaselineTier, Scenario, ScenarioResult,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_sqlite_store_baseline_smoke_tier_reconciles_and_exports_store_phases() {
    let result = store_baseline::run(&Backend::Sqlite, BaselineTier::Smoke).await;
    verify_and_publish(&result);
}

/// The full tier, SQLite arm. Opt-in: it takes minutes and wants a runner to
/// itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn the_sqlite_store_baseline_full_tier_reconciles_and_exports_store_phases() {
    if !store_baseline::full_tier_requested() {
        eprintln!(
            "skipping the full store baseline; set {}=1 to run it",
            store_baseline::FULL_TIER_ENV
        );
        return;
    }
    let result = store_baseline::run(&Backend::Sqlite, BaselineTier::Full).await;
    verify_and_publish(&result);
}

/// The full tier, Postgres arm: the same scenarios against a schema of its own
/// in the database `AXOND_TEST_POSTGRES_DSN` names. Skips, and says so, when
/// either opt-in is absent.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn the_postgres_store_baseline_full_tier_reconciles_and_exports_store_phases() {
    if !store_baseline::full_tier_requested() {
        eprintln!(
            "skipping the full store baseline; set {}=1 to run it",
            store_baseline::FULL_TIER_ENV
        );
        return;
    }
    let Some(dsn) = store_baseline::postgres_dsn() else {
        eprintln!(
            "skipping the Postgres store baseline; set AXOND_TEST_POSTGRES_DSN to a database the \
             harness may create schemas in"
        );
        return;
    };
    let result = store_baseline::run(&Backend::Postgres { dsn }, BaselineTier::Full).await;
    verify_and_publish(&result);
}

/// The smoke tier's Postgres arm, so the Postgres code path of the harness
/// itself is exercised wherever a test database exists, without waiting for
/// the full tier.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_postgres_store_baseline_smoke_tier_reconciles_and_exports_store_phases() {
    let Some(dsn) = store_baseline::postgres_dsn() else {
        eprintln!("skipping the Postgres store baseline smoke tier; AXOND_TEST_POSTGRES_DSN unset");
        return;
    };
    let result = store_baseline::run(&Backend::Postgres { dsn }, BaselineTier::Smoke).await;
    verify_and_publish(&result);
}

/// Every scenario the harness knows is one the result carries, in order, so a
/// reader comparing two artifacts compares like with like.
#[test]
fn the_scenario_set_is_closed_and_ordered() {
    let ids: Vec<&str> = Scenario::ALL.iter().map(|s| s.as_str()).collect();
    assert_eq!(
        ids,
        [
            "steady-buffered",
            "steady-streamed",
            "burst",
            "summaries",
            "slow-store",
            "large-payload",
        ]
    );
    for scenario in Scenario::ALL {
        let full = scenario.scale(BaselineTier::Full);
        let smoke = scenario.scale(BaselineTier::Smoke);
        assert!(full.requests > smoke.requests, "{}", scenario.as_str());
        assert!(smoke.warmup_requests > 0, "{}", scenario.as_str());
    }
}

fn verify_and_publish(result: &BaselineResult) {
    let (json, markdown) = result.write();
    eprintln!(
        "store-baseline: wrote {} and {}",
        json.display(),
        markdown.display()
    );
    assert!(
        store_baseline::inside_artifact_dir(&json),
        "artifact left the artifact directory: {}",
        json.display()
    );
    let written: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&json).expect("the artifact reads back"))
            .expect("the artifact is JSON");
    assert!(
        store_baseline::is_baseline_artifact(&written),
        "the artifact does not identify itself"
    );

    assert_eq!(result.scenarios.len(), Scenario::ALL.len());
    for scenario in &result.scenarios {
        verify_scenario(result.backend, scenario);
    }
}

fn verify_scenario(backend: &str, scenario: &ScenarioResult) {
    let id = scenario.id;
    assert!(
        !scenario.repetitions.is_empty(),
        "{id}: no measured repetition"
    );
    for (index, repetition) in scenario.repetitions.iter().enumerate() {
        assert_eq!(
            repetition.offered,
            repetition.accepted + repetition.rejected + repetition.errors,
            "{id} repetition {index}: offered != accepted + shed + errors ({repetition:?})"
        );
        assert!(
            repetition.reconciled,
            "{id} repetition {index}: the artifact's own reconciliation flag disagrees"
        );
        assert_eq!(
            repetition.offered, scenario.scale.requests as u64,
            "{id} repetition {index}: offered a different count than the scale asked for"
        );
        assert!(
            repetition.accepted > 0,
            "{id} repetition {index}: nothing was accepted; by_status={:?} errors={:?}",
            repetition.by_status,
            repetition.errors_by_error_type
        );
        assert_eq!(
            repetition.usage_records.missing, 0,
            "{id} repetition {index}: {} accepted requests were never charged",
            repetition.usage_records.missing
        );
        if scenario.scale.summary_readers > 0 {
            let summaries = repetition
                .summaries
                .as_ref()
                .unwrap_or_else(|| panic!("{id} repetition {index}: no summary report"));
            assert!(
                summaries.ok > 0,
                "{id} repetition {index}: no management summary succeeded: {:?}",
                summaries.by_status
            );
        }
    }
    if let Some(control) = &scenario.control {
        assert_eq!(
            control.accepted, control.offered,
            "{id}: the fake upstream refused a control request"
        );
        assert!(
            scenario.overhead_ms.is_some(),
            "{id}: control without overhead"
        );
    }

    let evidence = &scenario.store_evidence;
    assert_eq!(evidence.backend_label, backend);
    for operation in ["namespace_resolve", "budget_charge", "usage_append"] {
        let op = evidence.operations.get(operation).unwrap_or_else(|| {
            panic!(
                "{id}: no axond.store.acquire_wait point for {backend}/{operation}; saw {:?}",
                evidence.operations.keys().collect::<Vec<_>>()
            )
        });
        assert!(op.calls > 0, "{id}: {operation} exported with zero calls");
        assert!(
            op.query_duration.is_some(),
            "{id}: {operation} has acquire_wait but no query_duration point"
        );
        assert!(
            op.outcomes.values().sum::<u64>() > 0,
            "{id}: {operation} has no axond.store.operations count"
        );
    }
    if scenario.scale.summary_readers > 0 {
        assert!(
            evidence.operations.contains_key("usage_summary"),
            "{id}: management readers ran but no usage_summary Store point was exported"
        );
    }
    assert!(
        evidence.index_queue.is_some(),
        "{id}: no axond.usage.index.queue.* points were exported"
    );
}
