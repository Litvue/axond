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

use std::collections::BTreeMap;
use std::time::Duration;

use serde_json::{Value, json};
use support::gateway::alias;
use support::stateful_endurance::fleet;
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
    // Durable loss is only ever excused by the fleet's own account of it, in
    // both halves. Outside the declared outage nothing is excused at all; the
    // half inside it is bounded by what the processes reported dropping, and
    // `durable_usage_loss_in_window` is the verdict that holds it there — so
    // a run that lost rows while no process said it dropped a batch fails
    // however the halves are split.
    assert_eq!(
        result.usage.durable_loss_outside_windows, 0,
        "rows went missing outside every declared outage: {:#?}",
        result.usage
    );
    assert!(
        result.usage.durable_loss_in_window
            <= stateful_endurance::run::excused_in_window(&result.usage.sink_drops),
        "more rows are missing inside the outage than the fleet reported dropping: {:#?}",
        result.usage
    );
    // Both sides of the policy revision, or the gate passed without ever being
    // tested: a probe tenant refused from the start would satisfy the second
    // assertion while proving nothing about the revision.
    assert!(
        result.tenancy.probe_served_before_policy > 0,
        "the probe tenant was never served before its policy revision, so its later refusal is \
         not evidence of the revision: {:#?}",
        result.tenancy
    );
    assert!(
        result.tenancy.probe_refused_after_policy > 0,
        "the probe tenant was never refused after its policy revision, so isolation was not \
         observed: {:#?}",
        result.tenancy
    );
    // A restart the load finished before proves nothing: `unavailable = 0` and
    // the readiness gap are both satisfied by a deployment nobody was asking
    // for anything.
    assert!(
        result.restart.offered_after_last_replacement > 0,
        "no request was offered after the last replacement, so the restart was not measured \
         under load: {:#?}",
        result.restart
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
        // The separations are checked at *both* durations. The allowance below
        // is an absolute duration while every offset is a fraction, so a gap
        // that is comfortable over twelve hours can be nothing at ninety
        // seconds — and a tier where the restart happens inside the database
        // outage's attribution window is running a different qualification.
        let allowance = Duration::from_millis(profile.schedule.recovery_allowance_ms);
        assert!(allowance > Duration::ZERO, "{}: no allowance", profile.id);
        for tier in [Tier::Smoke, Tier::Soak] {
            let duration = Duration::from_millis(profile.scale(tier).duration_ms);
            let label = tier.as_str();
            // Two backends must not be out at once: a run in which the provider
            // and the database fail together cannot attribute what it loses to
            // either.
            let windows = profile.schedule.fault_windows(duration);
            for pair in windows.windows(2) {
                assert!(
                    pair[0].1 <= pair[1].0,
                    "{} [{label}]: the declared fault windows overlap",
                    profile.id
                );
            }
            // Attribution runs past the end of each fault by the recovery
            // allowance, because a breaker tripped by a declared outage is
            // still open for its cooldown afterwards. The extended windows must
            // not overlap either, or a refusal could be charged to two outages.
            let attributed = profile.schedule.attribution_windows(duration);
            for (declared, attributed) in windows.iter().zip(&attributed) {
                assert_eq!(declared.0, attributed.0);
                assert_eq!(declared.1 + allowance, attributed.1);
            }
            for pair in attributed.windows(2) {
                assert!(
                    pair[0].1 <= pair[1].0,
                    "{} [{label}]: the attribution windows overlap",
                    profile.id
                );
            }
            // The database outage's window is widened *backwards* by the
            // attribution slack, which no other window is. A record dropped in
            // the overlap would be excused by the database outage while the
            // provider's outage was still being attributed the errors around
            // it, so the widened window has to clear every other fault's
            // attribution span too.
            let (usage_from, usage_to) = profile.schedule.usage_outage_window(duration);
            let usage_declared = windows.last().copied().expect("a usage outage window");
            for (from, to) in attributed
                .iter()
                .filter(|(from, _)| *from != usage_declared.0)
            {
                assert!(
                    *to <= usage_from || *from >= usage_to,
                    "{} [{label}]: the usage-outage attribution window overlaps another fault's",
                    profile.id
                );
            }
            // Nor may a moment the driver *acts* fall inside one: a revision
            // published or a fleet restarted while a fault is still being
            // attributed has its own errors excused by that fault.
            for scheduled in profile.schedule.resolve(duration) {
                if matches!(
                    scheduled.event,
                    Event::UpstreamOutageBegins
                        | Event::UpstreamOutageEnds
                        | Event::UsageBackendOutageBegins
                        | Event::UsageBackendOutageEnds
                ) {
                    continue;
                }
                for (from, to) in &attributed {
                    assert!(
                        scheduled.at < *from || scheduled.at >= *to,
                        "{} [{label}]: `{}` happens inside a fault's attribution window",
                        profile.id,
                        scheduled.event.as_str()
                    );
                }
                assert!(
                    scheduled.at < usage_from || scheduled.at >= usage_to,
                    "{} [{label}]: `{}` happens inside the usage-outage attribution window",
                    profile.id,
                    scheduled.event.as_str()
                );
            }
            // And the restart has to leave the run time to keep offering load
            // after the last replacement joined. A restart in the final seconds
            // is one no request was measured across, which is the difference
            // between a fleet that stayed available under load and a fleet
            // nobody asked for anything.
            let restart = duration.mul_f64(profile.schedule.rolling_restart_at);
            assert!(
                duration.saturating_sub(restart) >= 2 * allowance,
                "{} [{label}]: the restart leaves no room to offer load afterwards",
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

/// A failure is charged to a declared fault when the request was in flight for
/// any part of it, not only when it happened to be offered inside it. A stream
/// running when the provider is taken away fails because of the outage, and an
/// attribution keyed on the offer instant would make the gate depend on how
/// long requests happen to take.
#[test]
fn a_request_in_flight_when_a_fault_opens_is_the_faults() {
    let windows = [(Duration::from_secs(30), Duration::from_secs(40))];
    let touched = |at_ms: u64, latency_ms: f64| {
        stateful_endurance::run::touched(&windows, Duration::from_millis(at_ms), latency_ms)
    };
    assert!(touched(29_000, 2_000.0), "it was still running at 31s");
    assert!(touched(35_000, 10.0), "it was offered inside the window");
    assert!(touched(39_990, 5_000.0), "it began just inside the window");
    assert!(!touched(20_000, 1_000.0), "it was over before 30s");
    assert!(
        !touched(41_000, 1_000.0),
        "it began after the window closed"
    );
}

/// The reconciliation compares what the workload was owed against what the
/// workload settled. The driver's own probes settle records too — a boundary
/// probe every second and a convergence poll every fifty milliseconds — and
/// counting theirs on the workload's side would let a probe record stand in
/// for a workload record the deployment lost.
#[test]
fn the_drivers_own_probe_records_are_not_the_workloads() {
    let record = |namespace: &str, model: &str| {
        stateful_endurance::run::issued_by_the_driver(&json!({
            "namespace": namespace,
            "model": model,
        }))
    };
    assert!(record(fleet::PROBE, alias::CHAT), "the boundary probe's");
    assert!(
        record(fleet::PLATFORM, fleet::CATALOGUE_ALIAS),
        "the convergence poll's, which only the driver asks for"
    );
    assert!(!record(fleet::PLATFORM, alias::CHAT), "the workload's");
    assert!(
        !record(fleet::BYOK, alias::MESSAGES),
        "and the BYOK tenant's"
    );
}

/// Every form the process reports lost accounting in, normalised to what that
/// report lost. A recogniser that only knew the batch message would turn a
/// shutdown or a full buffer into an unexplained missing row, and one that
/// added the running total as though it were an increment would excuse rows
/// that never went missing.
#[test]
fn every_dropped_accounting_report_is_counted_once() {
    let mut attributed = BTreeMap::new();
    let mut records = |fields: Value| {
        support::gateway::normalise_drop_report(&fields, &mut attributed)
            .map(|report| report["records"].as_u64().expect("a normalised count"))
    };

    // A rejected batch: five records, and the sink's running total is now five.
    assert_eq!(
        records(json!({
            "sink": "postgres",
            "reason": "sink_error",
            "records": 5,
            "message": "usage batch dropped: sink rejected it",
        })),
        Some(5)
    );
    // The buffer-full report carries that running total, so it adds the three
    // it has over what the batch report already accounted for.
    assert_eq!(
        records(json!({
            "sink": "postgres",
            "reason": "buffer_full",
            "dropped": 8,
            "message": "usage record dropped rather than delaying the request path",
        })),
        Some(3)
    );
    // A total that has not moved is not another loss.
    assert_eq!(
        records(json!({
            "sink": "postgres",
            "reason": "buffer_full",
            "dropped": 8,
            "message": "usage record dropped rather than delaying the request path",
        })),
        None
    );
    // An abandoned buffer is what that report lost.
    assert_eq!(
        records(json!({
            "sink": "postgres",
            "reason": "shutdown",
            "abandoned": 4,
            "message": "usage sink flush exceeded its bound; buffered records were abandoned",
        })),
        Some(4)
    );
    // A rejected shutdown flush restates the batch failure the sink already
    // reported, so it is not a second loss.
    assert_eq!(
        records(json!({
            "sink": "postgres",
            "reason": "sink_error",
            "records": 2,
            "message": "usage sink rejected its buffered records on shutdown",
        })),
        None
    );
    // Sinks are accounted apart: one sink's total says nothing about another's.
    assert_eq!(
        records(json!({
            "sink": "stdout",
            "reason": "buffer_full",
            "dropped": 2,
            "message": "usage record dropped rather than delaying the request path",
        })),
        Some(2)
    );
    // An ordinary log line is not a drop report.
    assert_eq!(records(json!({"message": "listening"})), None);
}

/// The heavy tier runs alone. Both tiers and the manifest's own tests live in
/// one binary, and this suite runs in the shared `Stateful tests` lane rather
/// than under `--test-threads=1`: the exclusion has to be something the driver
/// holds, not something an invocation configures, or a resource envelope
/// measured here is measuring whatever else libtest started beside it.
#[test]
fn a_tier_offers_load_alone() {
    let source = include_str!("support/stateful_endurance/run.rs");
    assert!(
        source.contains("let _offering = load_lock().lock().await;"),
        "the driver takes the load lock before it offers anything"
    );

    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime");
    runtime.block_on(async {
        let held = stateful_endurance::run::load_lock().lock().await;
        assert!(
            stateful_endurance::run::load_lock().try_lock().is_err(),
            "a second tier waits rather than offering load beside the first"
        );
        drop(held);
        assert!(
            stateful_endurance::run::load_lock().try_lock().is_ok(),
            "and takes it once the first has finished"
        );
    });
}

/// The database outage excuses the rows it lost, and only those. The split is
/// made on the window both sides can be counted over — records the processes
/// settled outside it against rows the database holds outside it — so a row
/// lost at a safe moment is charged to the deployment however many records the
/// sink reported dropping while the backend was gone.
#[test]
fn a_durable_loss_is_excused_by_when_it_happened() {
    let split = |emitted_outside, duplicates, durable_distinct, durable_outside| {
        stateful_endurance::run::reconcile_durable_loss(
            100,
            emitted_outside,
            duplicates,
            durable_distinct,
            durable_outside,
        )
    };

    // Nothing lost: every settled record is in the database.
    let clean = split(90, 0, 100, 90);
    assert_eq!((clean.total, clean.outside, clean.in_window), (0, 0, 0));

    // Ten lost, and the database holds every record settled outside the
    // window: the outage accounts for all of them.
    let excused = split(90, 0, 90, 90);
    assert_eq!(
        (excused.total, excused.outside, excused.in_window),
        (10, 0, 10)
    );

    // Ten lost, and three of them were settled outside the window. Those three
    // are the deployment's, whatever the sinks reported losing during the
    // outage — the old magnitude comparison excused them.
    let mixed = split(90, 0, 90, 87);
    assert_eq!((mixed.total, mixed.outside, mixed.in_window), (10, 3, 7));

    // Duplicates are charged to the outside bucket, so a second copy of a
    // record cannot be read as a row the database is missing.
    let duplicated = split(90, 3, 90, 87);
    assert_eq!(
        (duplicated.total, duplicated.outside, duplicated.in_window),
        (10, 0, 10)
    );

    // The outside half is a part of the whole-run loss, never more than it: a
    // drain tick's disagreement about which side of the edge a record fell on
    // must not invent a loss that the set difference does not show.
    let bounded = split(90, 0, 98, 80);
    assert_eq!(
        (bounded.total, bounded.outside, bounded.in_window),
        (2, 2, 0)
    );
}

/// The other half: what the outage excuses is as large as the deployment said
/// it lost, and no larger. A single report is an explanation for the records it
/// named, not a licence for every row the run is missing.
#[test]
fn an_outage_excuses_only_the_rows_the_fleet_reported_losing() {
    use stateful_endurance::result::SinkDrops;
    use stateful_endurance::run::{DROP_LOG_SAMPLE, excused_in_window};

    let reported = |records, sampled| SinkDrops {
        records_in_usage_window: records,
        sampled_records_in_usage_window: sampled,
        ..SinkDrops::default()
    };

    // Nothing reported, nothing excused: a run that lost rows in silence fails
    // however many it lost.
    assert_eq!(excused_in_window(&reported(0, 0)), 0);

    // A rejected batch is reported with its exact count, so that count is the
    // bound — one report of a single record no longer excuses thousands.
    assert_eq!(excused_in_window(&reported(1, 0)), 1);
    assert_eq!(excused_in_window(&reported(534, 0)), 534);

    // The buffer-full report is sampled, so a run reporting through it may have
    // lost up to one interval more than its last report named. That allowance
    // exists only where such a report was actually made.
    assert_eq!(
        excused_in_window(&reported(1_000, 1_000)),
        1_000 + DROP_LOG_SAMPLE - 1
    );
}

/// A replica that has not finished reloading is a slow reload, not a tenant
/// reaching into another tenant's credentials. The difference decides which
/// gate fails and whether the run is abandoned, so the probe reads a flag the
/// fleet was *observed* to honour rather than one the driver published.
#[test]
fn a_slow_policy_reload_is_not_a_tenant_boundary_breach() {
    let source = include_str!("support/stateful_endurance/run.rs");
    assert!(
        source.contains("self.state.observe_probe(served, self.policy_withdrawn, now);"),
        "the boundary probe judges against the observed withdrawal"
    );
    assert!(
        source.contains("self.policy_withdrawn = converged.is_some();"),
        "which is set by convergence, not by publication"
    );
    // And the flag starts false, so nothing before the revision is a breach.
    assert!(
        source.contains("policy_withdrawn: false,"),
        "the run begins with the probe tenant still permitted"
    );
}

/// A credential is passed to a replica as it was given. libpq's keyword/value
/// form ends a field at a space, so an unquoted value with one in it makes the
/// rest of the password a keyword; a quote or a backslash inside the value
/// closes or escapes the wrong thing.
#[test]
fn a_replicas_dsn_carries_awkward_credentials_intact() {
    let (rebuilt, reach) = stateful_endurance::durable::through_gate(
        "postgres://a%20user:p%40ss%20w%27o%5Crd@127.0.0.1:5432/some%20db?application_name=axond",
        "127.0.0.1:6543",
    );
    assert_eq!(reach, stateful_endurance::durable::Reach::Gated);

    // What matters is what libpq's own parser makes of it: the gate's address,
    // and every field back exactly as it was given.
    let parsed: tokio_postgres::Config = rebuilt
        .parse()
        .unwrap_or_else(|error| panic!("{rebuilt} does not parse: {error}"));
    assert_eq!(parsed.get_user(), Some("a user"));
    assert_eq!(parsed.get_password(), Some(r"p@ss w'o\rd".as_bytes()));
    assert_eq!(parsed.get_dbname(), Some("some db"));
    assert_eq!(parsed.get_application_name(), Some("axond"));
    assert_eq!(parsed.get_ports(), [6543]);

    // A TLS DSN is handed over untouched: a byte-forwarding gate cannot stand
    // in front of a handshake to a different name.
    let tls = "postgres://user:pw@127.0.0.1:5432/postgres?sslmode=require";
    let (passed, reach) = stateful_endurance::durable::through_gate(tls, "127.0.0.1:6543");
    assert_eq!(passed, tls);
    assert_eq!(reach, stateful_endurance::durable::Reach::Direct);

    // So is a database that is not on this machine. Its default mode is
    // `prefer`, which still attempts a handshake, and rewriting the address
    // would both point that handshake at the gate's name and hand a remote
    // server's credentials to a plaintext forwarder. The outage is then not
    // evaluated, which the artifact says, rather than the run quietly
    // downgrading the connection it was given.
    let remote = "postgres://user:pw@db.internal:5432/postgres";
    let (passed, reach) = stateful_endurance::durable::through_gate(remote, "127.0.0.1:6543");
    assert_eq!(passed, remote);
    assert_eq!(reach, stateful_endurance::durable::Reach::Direct);

    // `localhost` is this machine by another name, and the gate binds it too.
    let (_, reach) = stateful_endurance::durable::through_gate(
        "postgres://user:pw@localhost:5432/postgres",
        "127.0.0.1:6543",
    );
    assert_eq!(reach, stateful_endurance::durable::Reach::Gated);

    // A database that insists on TLS is not a run this harness can reconcile:
    // it counts rows over its own plaintext connection. It is declined before
    // anything boots — see `durable::dsn` — rather than panicking part-way
    // through one.
    assert!(stateful_endurance::durable::requires_tls(
        "postgres://user:pw@127.0.0.1:5432/postgres?sslmode=require"
    ));
    assert!(!stateful_endurance::durable::requires_tls(
        "postgres://user:pw@127.0.0.1:5432/postgres"
    ));
}

/// A gate is a fault, and a fault that misses its moment is evidence the run
/// did not gather: a connection set up as the backend is taken away must be
/// cut, and a gate must stop listening when the run that made it ends.
#[tokio::test]
async fn a_gate_cuts_what_it_joined_and_stops_when_it_is_dropped() {
    use stateful_endurance::gate::{Gate, Mode};

    // The cut is subscribed to before the outage is read, so an outage declared
    // while a connection is being joined to the backend is not missed. A race
    // this narrow cannot be observed reliably, so the ordering is asserted
    // where it is decided.
    let source = include_str!("support/stateful_endurance/gate.rs");
    let subscribes = source
        .find("let mut cuts = state.generation.subscribe();")
        .expect("the connection subscribes to the cut");
    let reads = source
        .find("if state.outage.load(Ordering::SeqCst) {")
        .expect("the connection reads the outage");
    assert!(subscribes < reads, "the cut is subscribed to first");

    // And an accept error does not end the gate. A transient one cannot be
    // provoked reliably, so the loop's shape is asserted where it is written:
    // a `while let Ok(..)` here would close the listening socket on the first
    // half-open client and leave the backend unreachable for the rest of the
    // run, which reads as the deployment refusing everything.
    assert!(
        source.contains("match listener.accept().await {"),
        "the accept loop handles its errors rather than ending on them"
    );
    assert!(
        !source.contains("while let Ok((inbound, _)) = listener.accept().await"),
        "one transient accept error would stop the gate forwarding"
    );

    let backend = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("the fixture backend binds");
    let backend_addr = backend.local_addr().expect("the backend has an address");
    tokio::spawn(async move {
        while let Ok((mut inbound, _)) = backend.accept().await {
            tokio::spawn(async move {
                let mut buffer = [0u8; 64];
                // Echo until the peer goes away, so a live connection stays
                // live and a cut one is visible as a closed read.
                while let Ok(read) = tokio::io::AsyncReadExt::read(&mut inbound, &mut buffer).await
                {
                    if read == 0
                        || tokio::io::AsyncWriteExt::write_all(&mut inbound, &buffer[..read])
                            .await
                            .is_err()
                    {
                        return;
                    }
                }
            });
        }
    });

    let addr = {
        let gate = Gate::start(&backend_addr.to_string()).await;
        let mut through = tokio::net::TcpStream::connect(gate.addr)
            .await
            .expect("the gate accepts");
        tokio::io::AsyncWriteExt::write_all(&mut through, b"ping")
            .await
            .expect("the gate forwards");
        let mut echoed = [0u8; 4];
        tokio::io::AsyncReadExt::read_exact(&mut through, &mut echoed)
            .await
            .expect("the backend answers through the gate");

        // The outage cuts what the gate had already joined.
        gate.set(Mode::Outage);
        let mut buffer = [0u8; 4];
        let read = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            tokio::io::AsyncReadExt::read(&mut through, &mut buffer),
        )
        .await
        .expect("the cut connection ends rather than outliving the outage");
        assert!(matches!(read, Ok(0) | Err(_)), "the connection was cut");
        assert_eq!(gate.counts().cut, 1);
        gate.addr
    };

    // And the listener does not outlive the gate: the next profile is measured
    // without this run's sockets still bound.
    tokio::task::yield_now().await;
    let after = tokio::net::TcpStream::connect(addr).await;
    assert!(
        after.is_err() || {
            let mut orphaned = after.expect("checked");
            tokio::io::AsyncWriteExt::write_all(&mut orphaned, b"ping")
                .await
                .is_err()
        },
        "a dropped gate keeps accepting connections"
    );
}
