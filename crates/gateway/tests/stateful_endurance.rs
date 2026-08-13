//! Stateful endurance qualification: a long mixed workload offered to a fleet
//! whose catalogue, credentials, policy, provider, usage database and processes
//! all change while it serves (issue #221, epic #156).
//!
//! The stateless soak next door qualifies one Tier 0 process under twelve hours
//! of mixed traffic. This one qualifies a *deployment*: two replicas behind a
//! rotation, a durable PostgreSQL usage sink, four tenants — platform, BYOK,
//! platform-fallback, and a probe tenant that exists so tenant policy can be
//! revised against a caller whose refusal is unambiguous — and a committed
//! script that, part-way through the run, publishes a catalogue revision, then
//! rotates the credential pool, then withdraws a tenant's permission to borrow
//! it, then makes the provider slow, then takes the provider away, then takes
//! the usage database away, and finally replaces every replica one at a time.
//!
//! What the run has to show is written down before it starts, in
//! `qualification/stateful-endurance/manifest.toml`: every published revision
//! becomes the one serving inside a bound; no request is refused for want of a
//! replica; every dispatched request settles exactly one usage record; every
//! record settled outside the declared database outage reaches the database;
//! no tenant is ever served from another tenant's pool; and resident memory
//! does not trend upwards. Each run writes a secret-free artifact under
//! `target/stateful-endurance/` carrying the measurements, the timeline, the
//! per-replica time series, and the exact inputs that produced them.
//!
//! The `smoke` tier runs under `cargo test` wherever PostgreSQL is configured —
//! a minute long, and the same code and the same gates as the tier that
//! qualifies a release. The `soak` tier is twelve hours behind
//! `AXOND_STATEFUL_ENDURANCE=1`. No twelve-hour envelope is claimed here by
//! anything but a retained soak artifact.

mod support;

use std::time::Duration;

use support::stateful_endurance::manifest::{Event, Tier};
use support::stateful_endurance::{self as stateful_endurance, StatefulEnduranceResult};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_stateful_endurance_smoke_tier_qualifies_and_publishes_its_evidence() {
    qualify(Tier::Smoke).await;
}

/// The soak tier: the same script and the same gates over twelve hours. Opt-in,
/// because it needs a runner, a database, and half a day to itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn the_stateful_endurance_soak_tier_qualifies_and_publishes_its_evidence() {
    if std::env::var("AXOND_STATEFUL_ENDURANCE").as_deref() != Ok("1") {
        eprintln!(
            "skipping the stateful endurance soak tier; set AXOND_STATEFUL_ENDURANCE=1 to run it"
        );
        return;
    }
    qualify(Tier::Soak).await;
}

async fn qualify(tier: Tier) {
    let (manifest, text) = stateful_endurance::load();
    for profile in &manifest.profiles {
        let Some(result) = stateful_endurance::run(profile, tier, &text).await else {
            eprintln!(
                "skipping {} [{}]: no AXOND_TEST_POSTGRES_DSN, and a stateful qualification \
                 without a datastore is not a shorter one",
                profile.id,
                tier.as_str()
            );
            return;
        };
        assert_qualifies(&result);
    }
}

fn assert_qualifies(result: &StatefulEnduranceResult) {
    let failures = result.failures();
    assert!(
        failures.is_empty(),
        "{}\nfailed thresholds: {failures:#?}",
        result.summary()
    );
    // The gates above are only evidence if the run actually did the things it
    // is judging. A script that silently never fired would pass every one of
    // them.
    assert!(
        result.workload.offered > 0,
        "the run offered nothing: {}",
        result.summary()
    );
    assert!(
        result.usage.distinct > 0,
        "the run settled no usage records: {}",
        result.summary()
    );
    assert!(
        result.usage.durable.distinct > 0,
        "nothing reached the durable sink: {}",
        result.summary()
    );
    assert_eq!(
        result.revisions.len(),
        3,
        "the run has to publish a catalogue, a credential and a policy revision: {:#?}",
        result.revisions
    );
    assert!(
        result.faults.iter().any(
            |window| window.event == Event::UpstreamOutageBegins.as_str()
                && window.gate.refused > 0
        ),
        "the declared provider outage refused no connection, so nothing met it: {:#?}",
        result.faults
    );
    // Every declared fault has to end in the fleet serving again. A window that
    // never recovered is the failure this run exists to catch, and one whose
    // recovery was never observed is not evidence of anything.
    for window in &result.faults {
        assert!(
            window.recovered_ms.is_some(),
            "the fleet never served again after `{}` was lifted: {:#?}",
            window.event,
            result.faults
        );
    }
    // Durable loss is only ever excused by the fleet's own account of it.
    assert!(
        result.usage.durable_loss_in_window <= result.usage.sink_drops.records_in_usage_window,
        "more rows are missing than the processes reported dropping: {:#?}",
        result.usage
    );
    assert!(
        result.tenancy.probe_refused_after_policy > 0,
        "the probe tenant was never refused after its policy revision, so isolation was not \
         observed: {:#?}",
        result.tenancy
    );
}

/// The committed manifest has to describe a run that can qualify anything, and
/// a manifest that quietly lost one of those properties would still produce an
/// artifact that looked like evidence.
#[test]
fn the_committed_manifest_describes_a_qualifying_run() {
    let (manifest, _) = stateful_endurance::load();
    let mut ids: Vec<&str> = manifest.profiles.iter().map(|p| p.id.as_str()).collect();
    ids.sort_unstable();
    let unique = ids.len();
    ids.dedup();
    assert_eq!(unique, ids.len(), "profile ids must be unique: {ids:?}");

    for profile in &manifest.profiles {
        for ending in support::endurance::Ending::ALL {
            assert!(
                profile.mix.weight(ending) > 0,
                "{}: the mix offers no {} requests",
                profile.id,
                ending.as_str()
            );
        }
        assert!(
            profile.slo.replicas >= 2,
            "{}: a rolling restart needs more than one replica",
            profile.id
        );
        for tier in [Tier::Smoke, Tier::Soak] {
            let scale = profile.scale(tier);
            let slo = profile.slo(tier);
            assert!(
                scale.concurrency > 0 && scale.segment_ms > 0 && scale.sample_interval_ms > 0,
                "{} [{}]: a scale that offers nothing measures nothing",
                profile.id,
                tier.as_str()
            );
            assert!(
                scale.duration_ms >= scale.segment_ms * slo.min_segments,
                "{} [{}]: the tier is too short for the {} segments it is judged on",
                profile.id,
                tier.as_str(),
                slo.min_segments
            );
            // Every threshold that admits a failure would let a defect through
            // silently, so the ones that must be zero are asserted to be.
            assert_eq!(slo.max_tenant_boundary_violations, 0);
            assert_eq!(slo.max_missing_usage_records, 0);
            assert_eq!(slo.max_duplicate_usage_records, 0);
            assert_eq!(slo.max_durable_usage_loss_outside_windows, 0);
            assert_eq!(slo.max_restart_unavailable, 0);
            assert_eq!(slo.max_unplanned_errors, 0);
            // Recovery is bounded rather than optional: an allowance with no
            // gate behind it would excuse a deployment that never came back.
            assert!(
                slo.max_recovery_ms > 0,
                "{}: recovery is not bounded",
                profile.id
            );
        }
        assert!(
            profile.smoke.duration_ms < profile.soak.duration_ms,
            "{}: the smoke tier is meant to be the short one",
            profile.id
        );
        assert!(
            profile.soak.duration_ms >= 12 * 60 * 60 * 1000,
            "{}: the soak tier is what the twelve-hour envelope is measured over",
            profile.id
        );
    }
}

/// The script has to happen in the order the manifest declares, with every
/// fault opened before it is closed and the restart last — at both tiers, from
/// the same fractions. A smoke tier that ran a different script would not be a
/// shorter run of the same qualification.
#[test]
fn the_script_is_the_same_at_both_tiers() {
    let (manifest, _) = stateful_endurance::load();
    for profile in &manifest.profiles {
        let orders: Vec<Vec<Event>> = [Tier::Smoke, Tier::Soak]
            .into_iter()
            .map(|tier| {
                let duration = Duration::from_millis(profile.scale(tier).duration_ms);
                profile
                    .schedule
                    .resolve(duration)
                    .into_iter()
                    .map(|scheduled| scheduled.event)
                    .collect()
            })
            .collect();
        assert_eq!(
            orders[0], orders[1],
            "{}: the tiers run different scripts",
            profile.id
        );

        let order = &orders[0];
        assert_eq!(
            order.last(),
            Some(&Event::RollingRestart),
            "{}: the fleet is restarted last, so the revisions it takes are the ones under test",
            profile.id
        );
        for (begins, ends) in [
            (Event::UpstreamLatencyBegins, Event::UpstreamLatencyEnds),
            (Event::UpstreamOutageBegins, Event::UpstreamOutageEnds),
            (
                Event::UsageBackendOutageBegins,
                Event::UsageBackendOutageEnds,
            ),
        ] {
            let opened = order.iter().position(|event| *event == begins);
            let closed = order.iter().position(|event| *event == ends);
            assert!(
                opened.is_some() && closed > opened,
                "{}: {} is not closed after it is opened",
                profile.id,
                begins.as_str()
            );
        }
        // Two backends must not be out at once: a run in which the provider and
        // the database fail together cannot attribute what it loses to either.
        let duration = Duration::from_millis(profile.soak.duration_ms);
        let windows = profile.schedule.fault_windows(duration);
        for pair in windows.windows(2) {
            assert!(
                pair[0].1 <= pair[1].0,
                "{}: the declared fault windows overlap",
                profile.id
            );
        }
        // Attribution runs past the end of each fault by the recovery
        // allowance, because a breaker tripped by a declared outage is still
        // open for its cooldown afterwards. The extended windows must not
        // overlap either, or a refusal could be charged to two outages.
        let attributed = profile.schedule.attribution_windows(duration);
        let allowance = Duration::from_millis(profile.schedule.recovery_allowance_ms);
        assert!(allowance > Duration::ZERO, "{}: no allowance", profile.id);
        for (declared, attributed) in windows.iter().zip(&attributed) {
            assert_eq!(declared.0, attributed.0);
            assert_eq!(declared.1 + allowance, attributed.1);
        }
        for pair in attributed.windows(2) {
            assert!(
                pair[0].1 <= pair[1].0,
                "{}: the attribution windows overlap",
                profile.id
            );
        }
    }
}

/// The duration override belongs to the soak tier alone. Both tiers live in one
/// binary, and a dispatched duration honoured for the smoke tier would offer it
/// twice — once as the dispatched run and once as the ordinary test path.
#[test]
fn the_duration_override_applies_to_the_soak_tier_alone() {
    let (manifest, _) = stateful_endurance::load();
    let profile = &manifest.profiles[0];
    let dispatched = "90000";

    let smoke = stateful_endurance::run::requested(
        Tier::Smoke,
        profile.scale(Tier::Smoke),
        Some(dispatched),
    );
    assert_eq!(
        smoke,
        (Duration::from_millis(profile.smoke.duration_ms), "manifest"),
        "the smoke tier ignores the dispatched duration"
    );

    let soak =
        stateful_endurance::run::requested(Tier::Soak, profile.scale(Tier::Soak), Some(dispatched));
    assert_eq!(
        soak,
        (Duration::from_millis(90_000), "environment"),
        "the soak tier honours it"
    );

    let undispatched =
        stateful_endurance::run::requested(Tier::Soak, profile.scale(Tier::Soak), None);
    assert_eq!(
        undispatched,
        (Duration::from_millis(profile.soak.duration_ms), "manifest"),
        "and falls back to the manifest without one"
    );
}

/// A dispatched short soak is segmented to fit, so it still produces the
/// segments its own gates are counted over rather than one segment and no
/// trend.
#[test]
fn a_dispatched_run_is_segmented_to_fit() {
    let (manifest, _) = stateful_endurance::load();
    let profile = &manifest.profiles[0];
    let slo = profile.slo(Tier::Soak);
    let short = Duration::from_secs(600);
    let segment =
        stateful_endurance::run::segment_ms(profile.scale(Tier::Soak), short, slo.min_segments);
    assert!(
        segment > 0 && short.as_millis() as u64 / segment > slo.min_segments,
        "a {short:?} run at {segment}ms segments cannot close {} of them",
        slo.min_segments
    );
    // The committed length is kept when the run is long enough for it.
    let full = Duration::from_millis(profile.soak.duration_ms);
    assert_eq!(
        stateful_endurance::run::segment_ms(profile.scale(Tier::Soak), full, slo.min_segments),
        profile.soak.segment_ms,
        "a full-length soak keeps the manifest's segment length"
    );
}
