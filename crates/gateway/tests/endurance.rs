//! Endurance qualification: a long mixed workload, and what the process looks
//! like at the end of it (issue #221).
//!
//! Every profile in `qualification/endurance/manifest.toml` is offered to a real
//! `axond` process talking to the deterministic fake upstream — three tenants
//! over two providers and both wire families, buffered and streamed, ending in
//! success, caller cancellation, a mid-stream upstream death, and an upstream
//! that refuses. Each run writes an artifact under `target/endurance/` carrying
//! the measurements, the whole resource time series, and the exact inputs that
//! produced them.
//!
//! What fails here is narrow, and deliberately not throughput. A shared runner
//! cannot bound latency without flaking, and a flaky endurance gate is one that
//! gets disabled. The hard failures are the properties that do not move with
//! the machine: nothing shed or failed that the plan did not ask for, every
//! request settling exactly one usage record — not none, not two — no upstream
//! socket left open, descriptors returned once the callers are gone, and
//! resident memory that does not trend upwards over the run.
//!
//! The `smoke` tier runs under `cargo test`: seconds long, and the same code
//! and the same assertions as the tier that qualifies a release. The `soak`
//! tier is twelve hours behind `AXOND_ENDURANCE=1` and the `endurance`
//! workflow, which runs this binary with `--test-threads=1` so no other load is
//! offered to the same host.
//!
//! Nothing here qualifies stateful serving: the profiles run a Tier 0 process.

mod support;

use support::endurance::{self, EnduranceResult, Profile, Tier};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_endurance_smoke_tier_qualifies_and_publishes_its_evidence() {
    qualify(Tier::Smoke).await;
}

/// The soak tier: the same profiles and the same gates over twelve hours.
/// Opt-in, because it needs a runner to itself for half a day.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn the_endurance_soak_tier_qualifies_and_publishes_its_evidence() {
    if std::env::var("AXOND_ENDURANCE").as_deref() != Ok("1") {
        eprintln!("skipping the endurance soak tier; set AXOND_ENDURANCE=1 to run it");
        return;
    }
    qualify(Tier::Soak).await;
}

/// The committed manifest has to describe a run that can qualify anything: a
/// soak inside the window the release process asks for, a smoke short enough to
/// stay in the ordinary test path, enough segments at each tier for a trend,
/// and every ending represented. A manifest that quietly lost one of those
/// would still produce an artifact, and the artifact would look like evidence.
#[test]
fn the_committed_manifest_describes_a_qualifying_run() {
    let (manifest, _) = endurance::manifest::load();
    let mut ids: Vec<&str> = manifest.profiles.iter().map(|p| p.id.as_str()).collect();
    ids.sort_unstable();
    let unique = ids.len();
    ids.dedup();
    assert_eq!(unique, ids.len(), "profile ids must be unique: {ids:?}");

    for profile in &manifest.profiles {
        for ending in endurance::Ending::ALL {
            assert!(
                profile.mix.weight(ending) > 0,
                "{}: the mix offers no {} requests",
                profile.id,
                ending.as_str()
            );
        }
        for tier in [Tier::Smoke, Tier::Soak] {
            let scale = profile.scale(tier);
            assert!(
                scale.concurrency > 0,
                "{} [{}]: a scale with no workers offers nothing",
                profile.id,
                tier.as_str()
            );
            assert!(
                scale.segment_ms > 0
                    && scale.duration_ms / scale.segment_ms >= scale.thresholds.min_segments,
                "{} [{}]: {} ms of {} ms segments cannot produce the {} segments it is gated on",
                profile.id,
                tier.as_str(),
                scale.duration_ms,
                scale.segment_ms,
                scale.thresholds.min_segments
            );
            assert!(
                scale.sample_interval_ms > 0 && scale.sample_interval_ms < scale.segment_ms,
                "{} [{}]: a segment must hold more than one sample to have a median",
                profile.id,
                tier.as_str()
            );
        }
        // The window issue #221 asks for. A "soak" that runs for an hour is a
        // capacity run with a long tail, and would be evidence of nothing.
        let hours = profile.soak.duration_ms as f64 / 3_600_000.0;
        assert!(
            (12.0..=24.0).contains(&hours),
            "{}: the soak tier runs {hours:.1} h, outside the 12-24 h window",
            profile.id
        );
        assert!(
            profile.smoke.duration_ms <= 120_000,
            "{}: the smoke tier must stay inside the ordinary test path",
            profile.id
        );
        // A short tier cannot measure a per-hour slope, so it must not claim to.
        assert!(
            profile
                .smoke
                .thresholds
                .max_rss_drift_kib_per_hour
                .is_none(),
            "{}: the smoke tier is too short to gate on drift per hour",
            profile.id
        );
        assert!(
            profile.soak.thresholds.max_rss_drift_kib_per_hour.is_some(),
            "{}: the soak tier gates on nothing that a long run is for",
            profile.id
        );
    }
}

/// The ending rotation is a pure function of the manifest: same seed, same
/// order, on any host. It also has to be *mixed* — a rotation that happened to
/// order the endings in blocks would run every fault back to back and soak
/// nothing about interleaving.
#[test]
fn the_ending_rotation_is_deterministic_and_interleaved() {
    let (manifest, _) = endurance::manifest::load();
    for profile in &manifest.profiles {
        let rotation = endurance::plan::rotation(&profile.mix, profile.seed);
        assert_eq!(
            rotation,
            endurance::plan::rotation(&profile.mix, profile.seed),
            "{}: the rotation is not reproducible from its seed",
            profile.id
        );
        assert_eq!(
            rotation.len(),
            profile.mix.cycle_len(),
            "{}: the rotation is not one whole mix cycle",
            profile.id
        );
        for ending in endurance::Ending::ALL {
            assert_eq!(
                rotation.iter().filter(|&&e| e == ending).count(),
                profile.mix.weight(ending),
                "{}: the rotation does not offer the committed {} share",
                profile.id,
                ending.as_str()
            );
        }
        assert!(
            rotation
                .windows(2)
                .filter(|pair| pair[0] != pair[1])
                .count()
                > 1,
            "{}: the rotation offers its endings in blocks: {rotation:?}",
            profile.id
        );
    }
}

async fn qualify(tier: Tier) {
    let (manifest, text) = endurance::manifest::load();
    let mut failures = Vec::new();
    for profile in &manifest.profiles {
        let result = endurance::run(profile, tier, &text).await;
        let path = result.write();
        eprintln!(
            "endurance: {}\n            -> {}",
            result.summary(),
            path.display()
        );
        assert_planned_workload(profile, &result);
        for verdict in result.failures() {
            failures.push(format!(
                "{} [{}]: {} {} {} (measured {})",
                profile.id,
                tier.as_str(),
                verdict.threshold,
                verdict.comparison,
                verdict.bound,
                verdict.value
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "endurance thresholds were not met:\n{}",
        failures.join("\n")
    );
}

/// What the run must have *offered*, independently of the thresholds. A run
/// that never reached a tenant, an ending, or a wire family passes every gate
/// below it while having qualified something narrower than the profile claims.
fn assert_planned_workload(profile: &Profile, result: &EnduranceResult) {
    assert!(
        result.throughput.offered >= profile.mix.cycle_len() as u64,
        "{}: fewer requests than one mix cycle: {}",
        profile.id,
        result.throughput.offered
    );
    for ending in endurance::Ending::ALL {
        assert!(
            result.workload.by_ending.contains_key(ending.as_str()),
            "{}: no {} requests were offered",
            profile.id,
            ending.as_str()
        );
    }
    assert_eq!(
        result.workload.by_tenant.len(),
        endurance::plan::TENANTS,
        "{}: the run did not offer every tenant: {:?}",
        profile.id,
        result.workload.by_tenant
    );
    assert!(
        result.workload.by_provider.len() >= 2,
        "{}: the run did not reach both providers: {:?}",
        profile.id,
        result.workload.by_provider
    );
    // At least one fault has to have reached the upstream and come back as a
    // relayed failure. A run whose faults were all shed by an already-open
    // circuit never exercised the failure path they exist to soak.
    assert!(
        result.throughput.planned_faults > result.throughput.circuit_shed,
        "{}: every planned fault was shed by an open circuit",
        profile.id
    );
    assert!(
        result.workload.streamed > 0 && result.workload.buffered > 0,
        "{}: the run was not mixed: {} streamed, {} buffered",
        profile.id,
        result.workload.streamed,
        result.workload.buffered
    );
    // Both credential paths have to have settled spend: a BYOK tenant whose
    // records say `platform` is an attribution bug, and it is invisible to a
    // run that only ever used the operator's own pool.
    for source in ["platform", "byok"] {
        assert!(
            result
                .reconciliation
                .by_credential_source
                .contains_key(source),
            "{}: no usage record was attributed to {source}: {:?}",
            profile.id,
            result.reconciliation.by_credential_source
        );
    }
}
