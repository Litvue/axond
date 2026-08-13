//! Multi-replica rollout and rollback qualification (#220).
//!
//! Every scenario in `qualification/rollout/manifest.toml` is run against a
//! fleet of real `axond` processes behind a real load balancer, and each run
//! writes a machine-readable artifact under `target/rollout/` carrying the
//! traffic, the drains, the loss ledger, the migration evidence, the rollback
//! decisions, and a timeline of what happened when.
//!
//! What fails here is narrow and machine-independent. Throughput is recorded and
//! never asserted — a shared runner moves it, and a flaky rollout gate is one
//! that gets disabled. The hard failures are the properties of the deployment
//! sequence: no caller sent to a replica the balancer has seen drain, no
//! unanswered request while a replica is being replaced, a buffered request
//! admitted before the signal finished rather than dropped, a stream the
//! upstream never ends cut inside the advertised deadline and accounted for as
//! partial, every usage record flushed before the process exits, and a
//! termination inside the bound an orchestrator's grace period is set from.
//!
//! The reduced tier runs under `cargo test`. The heavy tier is the same code and
//! the same assertions at a scale that wants a runner to itself, behind
//! `AXOND_ROLLOUT=1`.

mod support;

use support::rollout::{self, RolloutResult, Tier};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_reduced_rollout_scenarios_qualify_and_publish_their_evidence() {
    qualify(Tier::Reduced).await;
}

/// The heavy tier: the same scenarios under load a single-replica test never
/// reaches. Opt-in, because it is slow.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn the_heavy_rollout_scenarios_qualify_and_publish_their_evidence() {
    if std::env::var("AXOND_ROLLOUT").as_deref() != Ok("1") {
        eprintln!("skipping the heavy rollout scenarios; set AXOND_ROLLOUT=1 to run them");
        return;
    }
    qualify(Tier::Heavy).await;
}

/// The manifest is the contract, so it is checked as one: a scenario that lost
/// its thresholds, or that was written with a single replica, would still
/// produce an artifact — and the artifact would look like evidence.
#[test]
fn the_committed_manifest_describes_a_real_rolling_deployment() {
    let (manifest, text) = rollout::manifest::load();
    assert!(
        !text.trim().is_empty(),
        "the manifest's own bytes are what the artifact's hash names"
    );
    let mut ids: Vec<&str> = manifest
        .scenarios
        .iter()
        .map(|scenario| scenario.id.as_str())
        .collect();
    ids.sort_unstable();
    let unique = ids.len();
    ids.dedup();
    assert_eq!(unique, ids.len(), "scenario ids must be unique: {ids:?}");

    for scenario in &manifest.scenarios {
        assert!(
            scenario.replicas >= rollout::manifest::MIN_REPLICAS,
            "{}: a rollout needs somewhere to route while a replica drains",
            scenario.id
        );
        for tier in [Tier::Reduced, Tier::Heavy] {
            let scale = scenario.scale(tier);
            assert!(
                scale.workers > 0 && scale.requests_per_phase >= scale.workers,
                "{} [{}]: every worker must get at least one request",
                scenario.id,
                tier.as_str()
            );
            assert!(
                (1..=scale.requests_per_phase).contains(&scale.stream_every),
                "{} [{}]: a phase with no streams cannot exercise a drain",
                scenario.id,
                tier.as_str()
            );
        }
        let shutdown = scenario.shutdown;
        assert!(
            shutdown.drain_grace_ms > 0 && shutdown.deadline_ms > shutdown.drain_grace_ms,
            "{}: work must be cut after the grace window, not during it",
            scenario.id
        );
        assert!(
            shutdown.flush_timeout_ms > 0,
            "{}: accounting needs a budget to flush in",
            scenario.id
        );
        let thresholds = scenario.thresholds;
        assert_eq!(
            (
                thresholds.max_requests_to_drained_replica,
                thresholds.max_request_loss,
                thresholds.max_usage_record_loss
            ),
            (0, 0, 0),
            "{}: routing to a drained replica, losing a request, and losing a usage record are \
             not budgets to spend",
            scenario.id
        );
        assert!(
            thresholds.min_mixed_version_requests >= 1,
            "{}: a rollout that never served both revisions is a restart",
            scenario.id
        );
    }
}

async fn qualify(tier: Tier) {
    let (manifest, text) = rollout::manifest::load();
    for scenario in &manifest.scenarios {
        let result = rollout::run(scenario, tier, &text).await;
        let path = result.write();
        eprintln!("{}\n  artifact: {}", result.summary(), path.display());
        report(&result);
    }
}

/// Fail with everything that failed, and where the evidence is: a rollout that
/// broke two invariants is not diagnosed by the first one.
fn report(result: &RolloutResult) {
    let failures = result.failures();
    assert!(
        failures.is_empty(),
        "{} [{}] failed {} threshold(s):\n{}\ntimeline:\n{}",
        result.scenario.id,
        result.scenario.tier,
        failures.len(),
        failures
            .iter()
            .map(|verdict| format!(
                "  {} {} {} (measured {})",
                verdict.threshold, verdict.comparison, verdict.bound, verdict.value
            ))
            .collect::<Vec<_>>()
            .join("\n"),
        result
            .timeline
            .iter()
            .map(|event| format!(
                "  {:>7} ms  {:<16} {:<20} {}",
                event.at_ms, event.phase, event.kind, event.detail
            ))
            .collect::<Vec<_>>()
            .join("\n"),
    );
}
