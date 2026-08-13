//! Deterministic tests for the availability contract: precedence, expiry,
//! last-known-good retention, cross-tenant isolation, and redaction.
//!
//! Every test fixes `now` explicitly. Availability is a function of an instant, so
//! a test that read the wall clock would be asserting a property of the machine it
//! ran on.

use std::time::{Duration, SystemTime};

use super::*;
use crate::desired_state::{ProjectId, TenantId, Uuid7};
use crate::status::StatusScope;

const SECOND: Duration = Duration::from_secs(1);

fn at(seconds: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(seconds)
}

fn uuid(seed: u64) -> Uuid7 {
    Uuid7::from_parts(seed, 0, seed).expect("seeds are in range")
}

fn tenant(seed: u64) -> TenantId {
    TenantId::new(uuid(seed))
}

fn project(seed: u64) -> ProjectId {
    ProjectId::new(uuid(seed))
}

fn target(model: &str) -> TargetRef {
    TargetRef::parse("openai", model).expect("a well-formed target")
}

fn key(scope: ScopeRef, model: &str) -> AvailabilityKey {
    AvailabilityKey::new(scope, target(model))
}

/// A complete, positive listing observation for one scope and target.
fn present(scope: ScopeRef, model: &str, observed: u64, ttl: Option<u64>) -> DiscoveryObservation {
    let observation = DiscoveryObservation::new(
        scope,
        target(model),
        DiscoveryResult::Present,
        DiscoveryCompleteness::Complete,
        DiscoverySource::ProviderListing,
        at(observed),
    );
    match ttl {
        Some(ttl) => observation.expiring_at(at(observed + ttl)),
        None => observation,
    }
}

/// A complete listing that does not carry the target: the one definitive
/// negative.
fn absent(scope: ScopeRef, model: &str, observed: u64) -> DiscoveryObservation {
    DiscoveryObservation::new(
        scope,
        target(model),
        DiscoveryResult::Absent,
        DiscoveryCompleteness::Complete,
        DiscoverySource::ProviderListing,
        at(observed),
    )
}

/// A look that failed partway: the discovery-outage shape.
fn outage(scope: ScopeRef, model: &str, observed: u64) -> DiscoveryObservation {
    DiscoveryObservation::new(
        scope,
        target(model),
        DiscoveryResult::Indeterminate,
        DiscoveryCompleteness::Partial,
        DiscoverySource::ProviderListing,
        at(observed),
    )
    .detailed("HTTP 503 from https://api.example.test/v1/models?key=sk-live-should-never-be-read")
}

/// A record whose five single-valued dimensions all permit.
fn permitting() -> AvailabilityRecord {
    AvailabilityRecord {
        entitlement: Entitlement::Granted,
        runtime: RuntimeHealth::Healthy,
        ..AvailabilityRecord::enabled()
    }
}

#[test]
fn a_target_with_fresh_complete_positive_evidence_is_available() {
    let scope = ScopeRef::tenant(tenant(1));
    let index = AvailabilityIndex::builder()
        .record(key(scope, "gpt-4o"), permitting())
        .observe(present(scope, "gpt-4o", 100, Some(60)))
        .build();

    let verdict = index.evaluate(&key(scope, "gpt-4o"), at(120));
    assert_eq!(verdict.state, AvailabilityState::Available);
    assert_eq!(verdict.reason, AvailabilityReason::Observed);
    assert_eq!(verdict.decided_by, DecidedBy::Discovery);
    assert_eq!(verdict.observed_at, Some(at(100)));
    assert_eq!(verdict.expires_at, Some(at(160)));
    assert!(!verdict.last_known_good);
    assert!(verdict.permits_attempt());
}

/// The ladder, one rung at a time: each dimension in turn is the only one that
/// objects, and the verdict names it. The order is the contract — a change here
/// is a change to which authority a caller is told to go and talk to.
#[test]
fn precedence_is_deterministic_and_names_the_dimension_that_decided() {
    let scope = ScopeRef::tenant(tenant(1));
    let cases: &[(
        AvailabilityRecord,
        AvailabilityState,
        AvailabilityReason,
        DecidedBy,
    )] = &[
        (
            AvailabilityRecord {
                presence: CataloguePresence::Absent,
                ..permitting()
            },
            AvailabilityState::Unavailable,
            AvailabilityReason::NotInCatalogue,
            DecidedBy::Catalogue,
        ),
        (
            AvailabilityRecord {
                presence: CataloguePresence::Withdrawn,
                ..permitting()
            },
            AvailabilityState::Unavailable,
            AvailabilityReason::WithdrawnFromCatalogue,
            DecidedBy::Catalogue,
        ),
        (
            AvailabilityRecord {
                policy: PolicyDecision::Denied,
                ..permitting()
            },
            AvailabilityState::Denied,
            AvailabilityReason::PolicyDenied,
            DecidedBy::Policy,
        ),
        (
            AvailabilityRecord {
                policy: PolicyDecision::Indeterminate,
                ..permitting()
            },
            AvailabilityState::Unknown,
            AvailabilityReason::PolicyIndeterminate,
            DecidedBy::Policy,
        ),
        (
            AvailabilityRecord {
                enablement: Enablement::NotEnabled,
                ..permitting()
            },
            AvailabilityState::Denied,
            AvailabilityReason::NotEnabled,
            DecidedBy::Enablement,
        ),
        (
            AvailabilityRecord {
                entitlement: Entitlement::Missing,
                ..permitting()
            },
            AvailabilityState::Denied,
            AvailabilityReason::EntitlementMissing,
            DecidedBy::Entitlement,
        ),
        (
            AvailabilityRecord {
                entitlement: Entitlement::Revoked,
                ..permitting()
            },
            AvailabilityState::Denied,
            AvailabilityReason::EntitlementRevoked,
            DecidedBy::Entitlement,
        ),
        (
            AvailabilityRecord {
                entitlement: Entitlement::Unknown,
                ..permitting()
            },
            AvailabilityState::Unknown,
            AvailabilityReason::EntitlementUnknown,
            DecidedBy::Entitlement,
        ),
        (
            AvailabilityRecord {
                runtime: RuntimeHealth::Unavailable,
                ..permitting()
            },
            AvailabilityState::Unavailable,
            AvailabilityReason::RuntimeUnavailable,
            DecidedBy::Runtime,
        ),
        (
            AvailabilityRecord {
                runtime: RuntimeHealth::Impaired,
                ..permitting()
            },
            AvailabilityState::Unknown,
            AvailabilityReason::RuntimeImpaired,
            DecidedBy::Runtime,
        ),
    ];

    for (record, state, reason, decided_by) in cases {
        let index = AvailabilityIndex::builder()
            .record(key(scope, "gpt-4o"), record.clone())
            // Positive evidence throughout, so the verdict comes from the
            // dimension under test and never from the absence of a look.
            .observe(present(scope, "gpt-4o", 100, None))
            .build();
        let verdict = index.evaluate(&key(scope, "gpt-4o"), at(120));
        assert_eq!(
            (verdict.state, verdict.reason, verdict.decided_by),
            (*state, *reason, *decided_by),
            "record {record:?}"
        );
    }
}

/// Enablement outranks every rung that can answer `unknown`, an undecided policy
/// included: a target a scope never switched on is denied, not reported as an
/// attemptable uncertainty, however badly the deployment's own policy evaluation is
/// going.
#[test]
fn an_undecided_policy_cannot_make_an_unenabled_target_attemptable() {
    let scope = ScopeRef::tenant(tenant(1));
    let verdict = AvailabilityIndex::builder()
        .record(
            key(scope, "gpt-4o"),
            AvailabilityRecord {
                policy: PolicyDecision::Indeterminate,
                enablement: Enablement::NotEnabled,
                ..permitting()
            },
        )
        .observe(present(scope, "gpt-4o", 100, None))
        .build()
        .evaluate(&key(scope, "gpt-4o"), at(120));

    assert_eq!(
        (verdict.state, verdict.reason, verdict.decided_by),
        (
            AvailabilityState::Denied,
            AvailabilityReason::NotEnabled,
            DecidedBy::Enablement
        )
    );
    assert!(!verdict.permits_attempt());
}

/// A deployment refusal still outranks the switch, so the two policy rungs are not
/// interchangeable: a denied policy decides even for an unenabled target.
#[test]
fn a_policy_refusal_outranks_the_scope_switch() {
    let scope = ScopeRef::tenant(tenant(1));
    let verdict = AvailabilityIndex::builder()
        .record(
            key(scope, "gpt-4o"),
            AvailabilityRecord {
                policy: PolicyDecision::Denied,
                enablement: Enablement::NotEnabled,
                ..permitting()
            },
        )
        .build()
        .evaluate(&key(scope, "gpt-4o"), at(120));

    assert_eq!(verdict.reason, AvailabilityReason::PolicyDenied);
    assert_eq!(verdict.decided_by, DecidedBy::Policy);
}

/// An open circuit and intermittent failure are different answers. This replica
/// skipping the target is a refusal; a target that merely fails on and off is one
/// the breaker would still attempt (ADR 0008), so it stays routable and only loses
/// certainty.
#[test]
fn an_impaired_target_loses_certainty_while_an_open_circuit_refuses() {
    let scope = ScopeRef::tenant(tenant(1));
    let verdict_for = |runtime| {
        AvailabilityIndex::builder()
            .record(
                key(scope, "gpt-4o"),
                AvailabilityRecord {
                    runtime,
                    ..permitting()
                },
            )
            .observe(present(scope, "gpt-4o", 100, None))
            .build()
            .evaluate(&key(scope, "gpt-4o"), at(120))
    };

    let impaired = verdict_for(RuntimeHealth::Impaired);
    assert_eq!(impaired.state, AvailabilityState::Unknown);
    assert!(
        impaired.permits_attempt(),
        "a flaky target the breaker has not tripped is still worth attempting"
    );

    let open = verdict_for(RuntimeHealth::Unavailable);
    assert_eq!(open.state, AvailabilityState::Unavailable);
    assert!(!open.permits_attempt());
    assert!(
        impaired.state.certainty() < verdict_for(RuntimeHealth::Healthy).state.certainty(),
        "impairment may lower certainty and may never raise it"
    );
}

/// Lowering a verdict is not relabelling it. Impairment lowers certainty, so it has
/// nothing to say about a verdict already at `unknown`: an operator looking at a
/// provider that cannot be enumerated must not be told instead that this replica is
/// flaky, and the two send them to different places.
#[test]
fn an_impaired_replica_does_not_relabel_an_already_uncertain_verdict() {
    let scope = ScopeRef::tenant(tenant(1));
    let impaired = || AvailabilityRecord {
        runtime: RuntimeHealth::Impaired,
        ..permitting()
    };

    let unlistable = AvailabilityIndex::builder()
        .record(key(scope, "gpt-4o"), impaired())
        .observe(outage(scope, "gpt-4o", 100))
        .build()
        .evaluate(&key(scope, "gpt-4o"), at(120));
    assert_eq!(
        (unlistable.state, unlistable.reason, unlistable.decided_by),
        (
            AvailabilityState::Unknown,
            AvailabilityReason::DiscoveryIncomplete,
            DecidedBy::Discovery,
        ),
        "the look that could not finish is still why nobody knows"
    );

    let unlooked = AvailabilityIndex::builder()
        .record(key(scope, "gpt-4o"), impaired())
        .build()
        .evaluate(&key(scope, "gpt-4o"), at(120));
    assert_eq!(
        (unlooked.state, unlooked.reason, unlooked.decided_by),
        (
            AvailabilityState::Unknown,
            AvailabilityReason::NoEvidence,
            DecidedBy::Discovery,
        ),
        "\"nobody has looked\" survives a flaky replica"
    );
}

/// A lowered verdict keeps the evidence it was resting on: `expires_at` is what a
/// namespace-scoped reader is promised, and a stale target lowered by impairment must
/// not stop reporting when its evidence ran out.
#[test]
fn a_verdict_impairment_lowers_keeps_its_evidence() {
    let scope = ScopeRef::tenant(tenant(1));
    let verdict = AvailabilityIndex::builder()
        .record(
            key(scope, "gpt-4o"),
            AvailabilityRecord {
                runtime: RuntimeHealth::Impaired,
                ..permitting()
            },
        )
        .observe(present(scope, "gpt-4o", 100, Some(50)))
        .build()
        .evaluate(&key(scope, "gpt-4o"), at(200));

    assert_eq!(
        (verdict.state, verdict.reason, verdict.decided_by),
        (
            AvailabilityState::Unknown,
            AvailabilityReason::RuntimeImpaired,
            DecidedBy::Runtime,
        ),
        "expired positive evidence on a flaky replica is uncertain, not stale"
    );
    assert_eq!(verdict.observed_at, Some(at(100)));
    assert_eq!(
        verdict.expires_at,
        Some(at(150)),
        "a tenant still learns when the evidence ran out"
    );
    assert_eq!(verdict.source, Some(DiscoverySource::ProviderListing));
    assert_eq!(
        verdict.for_scope(StatusScope::Namespace).expires_at,
        Some(at(150)),
        "and keeps it through namespace redaction"
    );
}

/// Impairment lowers a verdict and never lifts one: a replica failing on and off is
/// no reason to stop reporting that a provider's complete listing dropped the
/// target, and certainly not to make it attemptable again.
#[test]
fn an_impaired_replica_does_not_soften_a_definitive_absence() {
    let scope = ScopeRef::tenant(tenant(1));
    let verdict = AvailabilityIndex::builder()
        .record(
            key(scope, "gpt-4o"),
            AvailabilityRecord {
                runtime: RuntimeHealth::Impaired,
                ..permitting()
            },
        )
        .observe(absent(scope, "gpt-4o", 100))
        .build()
        .evaluate(&key(scope, "gpt-4o"), at(120));

    assert_eq!(
        (verdict.state, verdict.reason, verdict.decided_by),
        (
            AvailabilityState::Denied,
            AvailabilityReason::DiscoveryAbsent,
            DecidedBy::Discovery,
        ),
        "a complete listing that dropped the target still decides"
    );
    assert!(!verdict.permits_attempt());
}

/// Declared evidence obeys the same retention rule as observed evidence: a complete
/// listing that dropped the target discredits the retained positive, so the next
/// failed refresh has nothing stale to fall back onto.
#[test]
fn a_declared_definitive_absence_discredits_the_retained_positive() {
    let scope = ScopeRef::tenant(tenant(1));
    let builder = AvailabilityIndex::builder()
        .record(key(scope, "gpt-4o"), permitting())
        .observe(present(scope, "gpt-4o", 100, None))
        .record(
            key(scope, "gpt-4o"),
            AvailabilityRecord {
                discovery: Some(absent(scope, "gpt-4o", 500)),
                ..permitting()
            },
        );
    let declared = builder.clone().build();
    let record = declared
        .record(&key(scope, "gpt-4o"))
        .expect("the key is held");
    assert!(
        record.last_known_good.is_none(),
        "a complete listing that dropped the target leaves nothing to fall back onto"
    );
    assert_eq!(record.definitive_at, Some(at(500)));

    let after_outage = builder
        .observe(outage(scope, "gpt-4o", 600))
        .build()
        .evaluate(&key(scope, "gpt-4o"), at(700));
    assert_ne!(
        after_outage.state,
        AvailabilityState::Available,
        "a failed refresh cannot resurrect a target a complete listing dropped"
    );
    assert!(!after_outage.last_known_good);
}

/// Declaring is not a way around the ordering rule observing enforces: a look that
/// predates a conclusive answer is refused the current slot whichever call carries
/// it, so a redeclaration cannot resurrect a target a complete listing dropped.
#[test]
fn a_declared_stale_positive_cannot_resurrect_a_target_a_newer_listing_dropped() {
    let scope = ScopeRef::tenant(tenant(1));
    let builder = AvailabilityIndex::builder()
        .record(key(scope, "gpt-4o"), permitting())
        .observe(absent(scope, "gpt-4o", 500))
        .record(
            key(scope, "gpt-4o"),
            AvailabilityRecord {
                discovery: Some(present(scope, "gpt-4o", 100, None)),
                ..permitting()
            },
        );
    assert_eq!(
        builder.superseded(),
        1,
        "a declared look that overturns nothing is counted, not applied silently"
    );

    let index = builder.build();
    let record = index
        .record(&key(scope, "gpt-4o"))
        .expect("the key is held");
    assert_eq!(record.definitive_at, Some(at(500)));
    assert!(record.last_known_good.is_none());

    let verdict = index.evaluate(&key(scope, "gpt-4o"), at(600));
    assert_eq!(
        (verdict.state, verdict.reason, verdict.decided_by),
        (
            AvailabilityState::Denied,
            AvailabilityReason::DiscoveryAbsent,
            DecidedBy::Discovery,
        ),
        "the newest complete listing still decides"
    );
}

/// A record read out of one index survives being declared into another: a
/// declaration carries a conclusion of its own, and judging its evidence against
/// that conclusion rather than against the index's would discard the very evidence
/// it was handing over.
#[test]
fn a_record_round_trips_through_a_fresh_builder_with_its_evidence_intact() {
    let scope = ScopeRef::tenant(tenant(1));
    let derived = AvailabilityIndex::builder()
        .record(key(scope, "gpt-4o"), permitting())
        .observe(present(scope, "gpt-4o", 100, None))
        .observe(outage(scope, "gpt-4o", 200))
        .build();
    let carried = derived
        .record(&key(scope, "gpt-4o"))
        .expect("the key is held")
        .clone();

    let builder = AvailabilityIndex::builder().record(key(scope, "gpt-4o"), carried.clone());
    assert_eq!(
        builder.superseded(),
        0,
        "its own history is not out of order"
    );
    let round_tripped = builder.build();
    assert_eq!(
        round_tripped.record(&key(scope, "gpt-4o")),
        Some(&carried),
        "a redeclaration of a derived record changes nothing about it"
    );
    assert_eq!(
        round_tripped.evaluate(&key(scope, "gpt-4o"), at(300)),
        derived.evaluate(&key(scope, "gpt-4o"), at(300)),
        "including the outage fallback the retained evidence provides"
    );
    assert!(
        round_tripped
            .evaluate(&key(scope, "gpt-4o"), at(300))
            .last_known_good
    );
}

/// Evidence handed over as a whole record is retained like an observed look, or a
/// hand-over would quietly cost the fallback the next outage needs.
#[test]
fn a_declared_listing_is_retained_for_the_outage_that_follows_it() {
    let scope = ScopeRef::tenant(tenant(1));
    let index = AvailabilityIndex::builder()
        .record(
            key(scope, "gpt-4o"),
            AvailabilityRecord {
                discovery: Some(present(scope, "gpt-4o", 500, None)),
                ..permitting()
            },
        )
        .observe(outage(scope, "gpt-4o", 600))
        .build();

    let verdict = index.evaluate(&key(scope, "gpt-4o"), at(700));
    assert_eq!(verdict.state, AvailabilityState::Available);
    assert!(
        verdict.last_known_good,
        "the declared listing is what the outage falls back onto"
    );
}

/// Replaying stored evidence is not disorder: a projection that re-applies the looks
/// it already applied must not make the out-of-order counter climb, or the signal is
/// useless for spotting a genuinely disordered projection.
#[test]
fn replaying_a_look_the_current_slot_already_holds_reports_no_disorder() {
    let scope = ScopeRef::tenant(tenant(1));
    let index = AvailabilityIndex::builder()
        .record(key(scope, "gpt-4o"), permitting())
        .observe(present(scope, "gpt-4o", 100, None))
        .build();

    let builder = AvailabilityIndexBuilder::from_index(&index)
        .observe(present(scope, "gpt-4o", 100, None))
        .observe(present(scope, "gpt-4o", 100, None));

    assert_eq!(builder.superseded(), 0, "nothing arrived out of order");
    let replayed = builder.build();
    assert_eq!(replayed, index, "and nothing changed");
    assert_eq!(
        replayed.evaluate(&key(scope, "gpt-4o"), at(200)).reason,
        AvailabilityReason::Observed
    );
}

/// The probe detail is log-line material an operator opts into, and a record dump is
/// not that opt-in: `Debug` must not reproduce whatever a provider handed back.
#[test]
fn a_record_dump_does_not_print_the_probe_detail() {
    let scope = ScopeRef::tenant(tenant(1));
    let index = AvailabilityIndex::builder()
        .record(key(scope, "gpt-4o"), permitting())
        .observe(outage(scope, "gpt-4o", 100))
        .build();
    let dumped = format!("{index:?}");

    assert!(
        !dumped.contains("sk-live"),
        "a dump must not carry what a probe read back: {dumped}"
    );
    assert!(!dumped.contains("api.example.test"));
    assert!(
        dumped.contains("<redacted>"),
        "and it says the detail is there to be read on purpose"
    );
    assert_eq!(
        index
            .record(&key(scope, "gpt-4o"))
            .and_then(|record| record.discovery.as_ref())
            .and_then(|look| look.detail.as_deref()),
        outage(scope, "gpt-4o", 100).detail.as_deref(),
        "the field itself is intact for the log line that wants it"
    );
}

/// One refused look is one out-of-order arrival, even when a record carries it in
/// both slots: the counter is what tells an operator a projection is disordered, so
/// it must not double-count.
#[test]
fn a_look_refused_from_both_slots_of_one_record_counts_once() {
    let scope = ScopeRef::tenant(tenant(1));
    let confirmed = AvailabilityIndex::builder()
        .record(key(scope, "gpt-4o"), permitting())
        .observe(present(scope, "gpt-4o", 100, None))
        .build();
    let stale_hand_over = confirmed
        .record(&key(scope, "gpt-4o"))
        .expect("the key is held")
        .clone();
    assert_eq!(
        stale_hand_over.discovery, stale_hand_over.last_known_good,
        "one look occupies both slots"
    );

    let newer = AvailabilityIndex::builder()
        .record(key(scope, "gpt-4o"), permitting())
        .observe(absent(scope, "gpt-4o", 500))
        .build();
    let builder =
        AvailabilityIndexBuilder::from_index(&newer).record(key(scope, "gpt-4o"), stale_hand_over);

    assert_eq!(builder.superseded(), 1, "one look, one refusal");
    assert_eq!(
        builder
            .build()
            .evaluate(&key(scope, "gpt-4o"), at(600))
            .state,
        AvailabilityState::Denied,
        "and the target a complete listing dropped stays dropped"
    );
}

/// A record whose conclusive look has already been displaced from both slots — a
/// complete listing that dropped the target, then a failed refresh — still carries
/// that conclusion as a watermark, and declaring it must discredit an older positive
/// the receiving index retained.
#[test]
fn a_declared_conclusion_discredits_a_retained_positive_that_predates_it() {
    let scope = ScopeRef::tenant(tenant(1));
    let dropped_then_unreachable = AvailabilityIndex::builder()
        .record(key(scope, "gpt-4o"), permitting())
        .observe(absent(scope, "gpt-4o", 500))
        .observe(outage(scope, "gpt-4o", 600))
        .build()
        .record(&key(scope, "gpt-4o"))
        .expect("the key is held")
        .clone();
    assert_eq!(dropped_then_unreachable.last_known_good, None);
    assert_eq!(dropped_then_unreachable.definitive_at, Some(at(500)));

    let index = AvailabilityIndex::builder()
        .record(key(scope, "gpt-4o"), permitting())
        .observe(present(scope, "gpt-4o", 300, None))
        .record(key(scope, "gpt-4o"), dropped_then_unreachable)
        .build();

    let verdict = index.evaluate(&key(scope, "gpt-4o"), at(700));
    assert_ne!(
        verdict.state,
        AvailabilityState::Available,
        "a listing that dropped the target is not undone by handing the record over"
    );
    assert_eq!(verdict.state, AvailabilityState::Unknown);
    assert!(
        !verdict.last_known_good,
        "the stale positive is discredited"
    );
}

/// The same conclusion discredits an older positive still sitting in the *current*
/// slot, not only a retained one: read as current definitive evidence it would report
/// `available` for a target the concluding listing had dropped.
#[test]
fn a_declared_conclusion_discredits_an_older_positive_in_either_slot() {
    let scope = ScopeRef::tenant(tenant(1));
    // A declaration that carries the conclusion and no look of its own — the shape a
    // projection produces when it has the watermark but has discarded the evidence.
    let concluded = AvailabilityRecord {
        definitive_at: Some(at(500)),
        ..permitting()
    };

    let index = AvailabilityIndex::builder()
        .record(key(scope, "gpt-4o"), permitting())
        .observe(present(scope, "gpt-4o", 300, None))
        .record(key(scope, "gpt-4o"), concluded)
        .build();
    let record = index
        .record(&key(scope, "gpt-4o"))
        .expect("the key is held");
    assert_eq!(record.discovery, None, "the stale positive is discredited");
    assert_eq!(record.last_known_good, None);

    let verdict = index.evaluate(&key(scope, "gpt-4o"), at(700));
    assert_eq!(
        (verdict.state, verdict.reason),
        (AvailabilityState::Unknown, AvailabilityReason::NoEvidence),
        "a positive older than the conclusion cannot report the target reachable"
    );
}

/// One scope's evidence may never decide another's. `observe` takes the key from the
/// look and so cannot mis-file one; a declaration is handed both halves, so it checks
/// rather than trusting the caller — and says so, instead of dropping it silently.
#[test]
fn a_declaration_refuses_evidence_that_names_another_scope() {
    let mine = ScopeRef::tenant(tenant(1));
    let theirs = ScopeRef::tenant(tenant(2));
    let builder = AvailabilityIndex::builder().record(
        key(mine, "gpt-4o"),
        AvailabilityRecord {
            discovery: Some(present(theirs, "gpt-4o", 100, None)),
            last_known_good: Some(present(theirs, "gpt-4o", 100, None)),
            definitive_at: Some(at(100)),
            ..permitting()
        },
    );

    assert_eq!(builder.misfiled(), 2, "both looks name another tenant");
    assert_eq!(builder.superseded(), 0, "neither is a late arrival");
    let index = builder.build();
    let verdict = index.evaluate(&key(mine, "gpt-4o"), at(120));
    assert_eq!(
        (verdict.state, verdict.reason),
        (AvailabilityState::Unknown, AvailabilityReason::NoEvidence),
        "nobody has looked at this tenant's target"
    );
    let record = index.record(&key(mine, "gpt-4o")).expect("the key is held");
    assert_eq!(record.discovery, None);
    assert_eq!(record.last_known_good, None);

    let target_mismatch = AvailabilityIndex::builder().record(
        key(mine, "gpt-4o"),
        AvailabilityRecord {
            discovery: Some(present(mine, "gpt-4o-mini", 100, None)),
            ..permitting()
        },
    );
    assert_eq!(
        target_mismatch.misfiled(),
        1,
        "a look at another model is mis-filed too"
    );
}

/// Refusing a mis-filed look and then folding in the conclusion it reached would leak
/// the same decision by its timing alone: one scope's listing instant would discard a
/// positive another scope can genuinely reach.
#[test]
fn a_misfiled_declaration_cannot_discredit_another_scope_by_its_timing() {
    let mine = ScopeRef::tenant(tenant(1));
    let theirs = ScopeRef::tenant(tenant(2));
    let builder = AvailabilityIndex::builder()
        .record(key(mine, "gpt-4o"), permitting())
        .observe(present(mine, "gpt-4o", 100, None))
        .record(
            key(mine, "gpt-4o"),
            AvailabilityRecord {
                discovery: Some(absent(theirs, "gpt-4o", 500)),
                definitive_at: Some(at(500)),
                ..permitting()
            },
        );

    assert_eq!(builder.misfiled(), 1);
    let index = builder.build();
    let record = index.record(&key(mine, "gpt-4o")).expect("the key is held");
    assert_eq!(
        record.definitive_at,
        Some(at(100)),
        "another tenant's conclusion is not this tenant's watermark"
    );
    assert_eq!(
        index.evaluate(&key(mine, "gpt-4o"), at(600)).state,
        AvailabilityState::Available,
        "and the model this tenant can reach stays reachable"
    );
}

/// Replaying stored evidence in order is not disorder, including the positive a failed
/// refresh displaced into the retained slot: `superseded` is the operator's signal that
/// a projection is feeding looks out of order, so it must not fire on a faithful
/// replay.
#[test]
fn replaying_a_retained_look_reports_no_disorder() {
    let scope = ScopeRef::tenant(tenant(1));
    let index = AvailabilityIndex::builder()
        .record(key(scope, "gpt-4o"), permitting())
        .observe(present(scope, "gpt-4o", 100, None))
        .observe(outage(scope, "gpt-4o", 600))
        .build();
    let record = index
        .record(&key(scope, "gpt-4o"))
        .expect("the key is held");
    assert_eq!(
        record.last_known_good,
        Some(present(scope, "gpt-4o", 100, None))
    );

    let builder = AvailabilityIndexBuilder::from_index(&index)
        .observe(present(scope, "gpt-4o", 100, None))
        .observe(outage(scope, "gpt-4o", 600));

    assert_eq!(builder.superseded(), 0, "nothing arrived out of order");
    assert_eq!(builder.build(), index, "and nothing changed");
}

/// A replay is the same look when it bears the same evidence, whatever became of the
/// operator-facing detail: a store that truncates or drops the string is replaying the
/// same evidence, and disorder is not the thing to report about it.
#[test]
fn a_replay_that_lost_its_detail_is_still_the_same_look() {
    let scope = ScopeRef::tenant(tenant(1));
    let index = AvailabilityIndex::builder()
        .record(key(scope, "gpt-4o"), permitting())
        .observe(present(scope, "gpt-4o", 100, None).detailed("listing 200 OK, 41 models"))
        .observe(outage(scope, "gpt-4o", 600).detailed("504 from https://api.example/v1/models"))
        .build();

    let builder = AvailabilityIndexBuilder::from_index(&index)
        .observe(present(scope, "gpt-4o", 100, None))
        .observe(outage(scope, "gpt-4o", 600));

    assert_eq!(
        builder.superseded(),
        0,
        "the detail is a log line, not evidence"
    );
    let replayed = builder.build();
    let record = replayed
        .record(&key(scope, "gpt-4o"))
        .expect("the key is held");
    assert_eq!(
        record
            .last_known_good
            .as_ref()
            .and_then(|look| look.detail.as_deref()),
        Some("listing 200 OK, 41 models"),
        "and the held look is left alone rather than replaced by the thinner one"
    );
    assert_eq!(
        replayed.evaluate(&key(scope, "gpt-4o"), at(700)).state,
        index.evaluate(&key(scope, "gpt-4o"), at(700)).state
    );
}

/// A hand-built record can carry a retained look newer than its current one, which
/// no observed sequence produces. Evidence follows the newer of the two, or the
/// record would report a refusal while holding newer positive evidence.
#[test]
fn a_retained_look_newer_than_the_current_one_still_decides() {
    let scope = ScopeRef::tenant(tenant(1));
    let index = AvailabilityIndex::builder()
        .record(
            key(scope, "gpt-4o"),
            AvailabilityRecord {
                discovery: Some(absent(scope, "gpt-4o", 100)),
                last_known_good: Some(present(scope, "gpt-4o", 500, None)),
                definitive_at: Some(at(500)),
                ..permitting()
            },
        )
        .build();

    let verdict = index.evaluate(&key(scope, "gpt-4o"), at(600));
    assert_eq!(verdict.state, AvailabilityState::Available);
    assert!(
        verdict.last_known_good,
        "and it says which evidence it is resting on"
    );
}

/// The shape every freshly confirmed target has — the current look *is* the retained
/// positive — must survive a hand-over as current evidence, not be demoted to a
/// fallback.
#[test]
fn a_freshly_confirmed_record_is_declared_as_current_evidence_not_a_fallback() {
    let scope = ScopeRef::tenant(tenant(1));
    let confirmed = AvailabilityIndex::builder()
        .record(key(scope, "gpt-4o"), permitting())
        .observe(present(scope, "gpt-4o", 100, None))
        .build();
    let carried = confirmed
        .record(&key(scope, "gpt-4o"))
        .expect("the key is held")
        .clone();
    assert_eq!(
        carried.discovery, carried.last_known_good,
        "the current look is the retained positive"
    );

    let builder = AvailabilityIndex::builder().record(key(scope, "gpt-4o"), carried.clone());
    assert_eq!(builder.superseded(), 0);

    let index = builder.build();
    assert_eq!(index.record(&key(scope, "gpt-4o")), Some(&carried));
    let verdict = index.evaluate(&key(scope, "gpt-4o"), at(200));
    assert_eq!(
        (verdict.state, verdict.reason),
        (AvailabilityState::Available, AvailabilityReason::Observed)
    );
    assert!(
        !verdict.last_known_good,
        "it is resting on a current look, not a fallback"
    );
}

/// A refresh that reads an index into a builder and redeclares what it read has not
/// received anything out of order, so the counter must stay quiet.
#[test]
fn redeclaring_what_a_refresh_just_read_reports_no_out_of_order_arrivals() {
    let scope = ScopeRef::tenant(tenant(1));
    let index = AvailabilityIndex::builder()
        .record(key(scope, "gpt-4o"), permitting())
        .observe(present(scope, "gpt-4o", 100, None))
        .observe(outage(scope, "gpt-4o", 200))
        .build();
    let carried = index
        .record(&key(scope, "gpt-4o"))
        .expect("the key is held")
        .clone();

    let builder =
        AvailabilityIndexBuilder::from_index(&index).record(key(scope, "gpt-4o"), carried.clone());
    assert_eq!(builder.superseded(), 0);
    assert_eq!(
        builder.build().record(&key(scope, "gpt-4o")),
        Some(&carried)
    );
}

/// The current slot holds the newest look whichever call carries it, so a
/// declaration cannot rewind it to an older one either.
#[test]
fn an_older_declared_look_does_not_rewind_the_current_slot() {
    let scope = ScopeRef::tenant(tenant(1));
    let builder = AvailabilityIndex::builder()
        .observe(outage(scope, "gpt-4o", 600))
        .record(
            key(scope, "gpt-4o"),
            AvailabilityRecord {
                discovery: Some(outage(scope, "gpt-4o", 100)),
                ..permitting()
            },
        );
    assert_eq!(builder.superseded(), 1);

    let index = builder.build();
    let held = index
        .record(&key(scope, "gpt-4o"))
        .expect("the key is held")
        .discovery
        .as_ref()
        .expect("a look is held");
    assert_eq!(held.observed_at, at(600));
}

/// A *later* failed refresh is newer information than the absence it follows, so the
/// denial drops to `unknown` rather than standing on evidence a later probe could not
/// reproduce. A drop in certainty, never a rise: the target is not reported
/// `available` again, and the uncertainty is routable only under enablement.
#[test]
fn a_later_failed_refresh_lowers_a_denial_to_uncertainty_and_never_to_availability() {
    let scope = ScopeRef::tenant(tenant(1));
    let denied = AvailabilityIndex::builder()
        .record(key(scope, "gpt-4o"), permitting())
        .observe(absent(scope, "gpt-4o", 500))
        .build();
    assert_eq!(
        denied.evaluate(&key(scope, "gpt-4o"), at(600)).state,
        AvailabilityState::Denied
    );

    let after_outage = AvailabilityIndexBuilder::from_index(&denied)
        .observe(outage(scope, "gpt-4o", 700))
        .build();

    let verdict = after_outage.evaluate(&key(scope, "gpt-4o"), at(800));
    assert_eq!(verdict.state, AvailabilityState::Unknown);
    assert!(
        !verdict.last_known_good,
        "the absence cleared the retained positive, so nothing was resurrected"
    );
    assert!(
        verdict.state.certainty() < AvailabilityState::Available.certainty(),
        "a failed refresh lowers certainty and never restores availability"
    );

    let unenabled = AvailabilityIndexBuilder::from_index(&after_outage)
        .record(
            key(scope, "gpt-4o"),
            AvailabilityRecord {
                enablement: Enablement::NotEnabled,
                ..permitting()
            },
        )
        .build();
    assert!(
        !unenabled
            .evaluate(&key(scope, "gpt-4o"), at(800))
            .permits_attempt(),
        "the uncertainty it drops to is routable only where a scope chose it"
    );
}

/// The mirror of the two-definitive-looks tie-break: certainty falls to a later
/// look, never to one merely sharing the instant a conclusion was reached, or an
/// inconclusive probe racing a complete listing would soften a denial into a
/// routable unknown.
#[test]
fn an_inconclusive_look_sharing_an_absence_instant_does_not_soften_it() {
    let scope = ScopeRef::tenant(tenant(1));
    let builder = AvailabilityIndex::builder()
        .record(key(scope, "gpt-4o"), permitting())
        .observe(absent(scope, "gpt-4o", 500))
        .observe(outage(scope, "gpt-4o", 500));
    assert_eq!(builder.superseded(), 1);

    let verdict = builder.build().evaluate(&key(scope, "gpt-4o"), at(600));
    assert_eq!(
        (verdict.state, verdict.reason),
        (
            AvailabilityState::Denied,
            AvailabilityReason::DiscoveryAbsent
        )
    );
    assert!(!verdict.permits_attempt());
}

/// A policy denial outranks a positive listing, and a catalogue absence outranks
/// the denial: the ladder is ordered, not a set of independent vetoes.
#[test]
fn a_higher_rung_decides_even_when_a_lower_one_would_object_differently() {
    let scope = ScopeRef::tenant(tenant(1));
    let index = AvailabilityIndex::builder()
        .record(
            key(scope, "gpt-4o"),
            AvailabilityRecord {
                presence: CataloguePresence::Absent,
                policy: PolicyDecision::Denied,
                enablement: Enablement::NotEnabled,
                entitlement: Entitlement::Missing,
                runtime: RuntimeHealth::Unavailable,
                ..permitting()
            },
        )
        .observe(absent(scope, "gpt-4o", 100))
        .build();

    let verdict = index.evaluate(&key(scope, "gpt-4o"), at(120));
    assert_eq!(verdict.decided_by, DecidedBy::Catalogue);
    assert_eq!(verdict.reason, AvailabilityReason::NotInCatalogue);
}

#[test]
fn a_key_the_index_does_not_hold_is_unknown_rather_than_available_or_denied() {
    let scope = ScopeRef::tenant(tenant(1));
    let empty = AvailabilityIndex::empty();
    assert!(empty.is_empty());

    let verdict = empty.evaluate(&key(scope, "gpt-4o"), at(1));
    assert_eq!(verdict, Availability::no_record());
    assert_eq!(verdict.state, AvailabilityState::Unknown);
    assert_eq!(verdict.reason, AvailabilityReason::NoEvidence);
    assert_eq!(verdict.decided_by, DecidedBy::NoRecord);
    assert!(
        !verdict.permits_attempt(),
        "an index that holds nothing permits nothing: no rung examined this pair, so \
         the uncertainty is not a scope's accepted risk"
    );
}

/// The two kinds of `unknown` are not the same answer. A scope that is catalogued,
/// enabled, and entitled but not yet looked at has passed every authority and is
/// waiting on discovery; a key nothing describes has passed nothing. Only the first
/// is routable.
#[test]
fn a_permitted_target_awaiting_discovery_is_distinct_from_a_key_nothing_describes() {
    let scope = ScopeRef::tenant(tenant(1));
    let awaiting = AvailabilityIndex::builder()
        .record(key(scope, "gpt-4o"), permitting())
        .build()
        .evaluate(&key(scope, "gpt-4o"), at(200));

    assert_eq!(awaiting.state, AvailabilityState::Unknown);
    assert_eq!(awaiting.reason, AvailabilityReason::NoEvidence);
    assert_eq!(
        awaiting.decided_by,
        DecidedBy::Discovery,
        "an operator is sent to the discovery mechanism, not told nothing is known"
    );
    assert!(awaiting.permits_attempt());
    assert_ne!(awaiting, Availability::no_record());
    assert!(!Availability::no_record().permits_attempt());
}

/// Only a complete negative denies. A partial listing that does not mention the
/// model, a provider with no listing endpoint, and an untrustworthy answer are
/// all `unknown` — and each says which, so an operator can tell a broken probe
/// from an unsupported provider.
#[test]
fn incomplete_discovery_is_unknown_and_never_a_denial() {
    let scope = ScopeRef::tenant(tenant(1));
    let cases = [
        (
            DiscoveryCompleteness::Partial,
            AvailabilityReason::DiscoveryIncomplete,
        ),
        (
            DiscoveryCompleteness::Unsupported,
            AvailabilityReason::DiscoveryUnsupported,
        ),
        (
            DiscoveryCompleteness::Unreliable,
            AvailabilityReason::DiscoveryUnreliable,
        ),
    ];

    for (completeness, reason) in cases {
        for result in [DiscoveryResult::Present, DiscoveryResult::Absent] {
            let observation = DiscoveryObservation::new(
                scope,
                target("gpt-4o"),
                result,
                completeness,
                DiscoverySource::ProviderListing,
                at(100),
            );
            let index = AvailabilityIndex::builder()
                .record(key(scope, "gpt-4o"), permitting())
                .observe(observation)
                .build();
            let verdict = index.evaluate(&key(scope, "gpt-4o"), at(120));
            assert_eq!(
                (verdict.state, verdict.reason),
                (AvailabilityState::Unknown, reason),
                "{completeness:?} {result:?} must not be definitive"
            );
        }
    }
}

/// Expiry moves evidence towards *less* confidence in both directions: an expired
/// positive is stale, and an expired denial stops denying.
#[test]
fn expiry_downgrades_positive_evidence_to_stale_and_a_denial_to_unknown() {
    let scope = ScopeRef::tenant(tenant(1));
    let positive = AvailabilityIndex::builder()
        .record(key(scope, "gpt-4o"), permitting())
        .observe(present(scope, "gpt-4o", 100, Some(60)))
        .build();

    // One second before expiry, and exactly at it: the boundary is inclusive, so
    // evidence does not count for the instant it expires.
    let before = positive.evaluate(&key(scope, "gpt-4o"), at(160) - SECOND);
    assert_eq!(before.state, AvailabilityState::Available);
    let at_expiry = positive.evaluate(&key(scope, "gpt-4o"), at(160));
    assert_eq!(at_expiry.state, AvailabilityState::Stale);
    assert_eq!(at_expiry.reason, AvailabilityReason::EvidenceExpired);
    assert_eq!(
        at_expiry.expires_at,
        Some(at(160)),
        "a stale verdict still says which evidence expired and when"
    );
    assert!(at_expiry.permits_attempt());

    let denial = AvailabilityIndex::builder()
        .record(key(scope, "gpt-4o"), permitting())
        .observe(absent(scope, "gpt-4o", 100).expiring_at(at(160)))
        .build();
    assert_eq!(
        denial.evaluate(&key(scope, "gpt-4o"), at(159)).state,
        AvailabilityState::Denied
    );
    let expired_denial = denial.evaluate(&key(scope, "gpt-4o"), at(161));
    assert_eq!(
        (expired_denial.state, expired_denial.reason),
        (
            AvailabilityState::Unknown,
            AvailabilityReason::EvidenceExpired
        ),
        "a denial that rested on an expired look is no longer a denial"
    );
}

#[test]
fn evidence_without_an_expiry_never_expires() {
    let scope = ScopeRef::tenant(tenant(1));
    let index = AvailabilityIndex::builder()
        .record(key(scope, "gpt-4o"), permitting())
        .observe(present(scope, "gpt-4o", 100, None))
        .build();

    let verdict = index.evaluate(&key(scope, "gpt-4o"), at(100_000_000));
    assert_eq!(verdict.state, AvailabilityState::Available);
    assert_eq!(verdict.expires_at, None);
}

/// The outage case, end to end: a look that fails keeps the last positive
/// evidence, and the verdict says it is resting on it.
#[test]
fn a_discovery_outage_preserves_the_last_known_good_state() {
    let scope = ScopeRef::tenant(tenant(1));
    let index = AvailabilityIndex::builder()
        .record(key(scope, "gpt-4o"), permitting())
        .observe(present(scope, "gpt-4o", 100, Some(600)))
        .observe(outage(scope, "gpt-4o", 200))
        .observe(outage(scope, "gpt-4o", 300))
        .build();

    let verdict = index.evaluate(&key(scope, "gpt-4o"), at(400));
    assert_eq!(verdict.state, AvailabilityState::Available);
    assert_eq!(verdict.reason, AvailabilityReason::LastKnownGood);
    assert!(verdict.last_known_good);
    assert_eq!(verdict.observed_at, Some(at(100)));

    // And when the retained evidence itself expires, the deployment is told it is
    // stale rather than quietly kept available.
    let expired = index.evaluate(&key(scope, "gpt-4o"), at(700));
    assert_eq!(expired.state, AvailabilityState::Stale);
    assert!(expired.last_known_good);
}

#[test]
fn a_definitive_negative_discredits_the_retained_positive() {
    let scope = ScopeRef::tenant(tenant(1));
    let index = AvailabilityIndex::builder()
        .record(key(scope, "gpt-4o"), permitting())
        .observe(present(scope, "gpt-4o", 100, Some(600)))
        .observe(absent(scope, "gpt-4o", 200))
        .build();

    let verdict = index.evaluate(&key(scope, "gpt-4o"), at(300));
    assert_eq!(verdict.state, AvailabilityState::Denied);
    assert_eq!(verdict.reason, AvailabilityReason::DiscoveryAbsent);
    assert!(!verdict.last_known_good);
    assert_eq!(
        index
            .record(&key(scope, "gpt-4o"))
            .expect("the record exists")
            .last_known_good,
        None,
        "a complete listing that dropped the target retains no positive evidence"
    );
}

/// Certainty only rises on definitive evidence. Every non-definitive observation
/// is applied to an index in a known state and must not make it more confident.
#[test]
fn unknown_and_stale_evidence_is_never_silently_upgraded() {
    let scope = ScopeRef::tenant(tenant(1));
    let denied = AvailabilityIndex::builder()
        .record(key(scope, "gpt-4o"), permitting())
        .observe(absent(scope, "gpt-4o", 100))
        .build();
    let before = denied.evaluate(&key(scope, "gpt-4o"), at(150));
    assert_eq!(before.state, AvailabilityState::Denied);

    for completeness in [
        DiscoveryCompleteness::Partial,
        DiscoveryCompleteness::Unsupported,
        DiscoveryCompleteness::Unreliable,
    ] {
        let after = AvailabilityIndexBuilder::from_index(&denied)
            .observe(DiscoveryObservation::new(
                scope,
                target("gpt-4o"),
                DiscoveryResult::Present,
                completeness,
                DiscoverySource::ProviderProbe,
                at(200),
            ))
            .build()
            .evaluate(&key(scope, "gpt-4o"), at(250));
        assert_eq!(
            after.state,
            AvailabilityState::Unknown,
            "a {completeness:?} look claiming presence must not become available"
        );
        assert!(after.state.certainty() < before.state.certainty());
    }

    // A stale verdict is not repaired by a look that establishes nothing either.
    let stale = AvailabilityIndex::builder()
        .record(key(scope, "gpt-4o"), permitting())
        .observe(present(scope, "gpt-4o", 100, Some(10)))
        .build();
    assert_eq!(
        stale.evaluate(&key(scope, "gpt-4o"), at(200)).state,
        AvailabilityState::Stale
    );
    let after_outage = AvailabilityIndexBuilder::from_index(&stale)
        .observe(outage(scope, "gpt-4o", 150))
        .build()
        .evaluate(&key(scope, "gpt-4o"), at(200));
    assert_eq!(after_outage.state, AvailabilityState::Stale);
    assert!(after_outage.last_known_good);
}

/// Which of two overlapping probes finishes first must not decide whether a model
/// stays reachable. A definitive positive that lands after a newer inconclusive look
/// is still the best evidence held, so it is retained either way.
#[test]
fn overlapping_probes_produce_the_same_index_whichever_lands_first() {
    let scope = ScopeRef::tenant(tenant(1));
    let conclusive = present(scope, "gpt-4o", 100, None);
    let inconclusive = outage(scope, "gpt-4o", 300);

    let in_order = AvailabilityIndex::builder()
        .record(key(scope, "gpt-4o"), permitting())
        .observe(conclusive.clone())
        .observe(inconclusive.clone());
    let reversed = AvailabilityIndex::builder()
        .record(key(scope, "gpt-4o"), permitting())
        .observe(inconclusive)
        .observe(conclusive);
    assert_eq!(
        reversed.superseded(),
        1,
        "the late look did not become current"
    );

    let expected = in_order.build().evaluate(&key(scope, "gpt-4o"), at(400));
    assert_eq!(
        reversed.build().evaluate(&key(scope, "gpt-4o"), at(400)),
        expected
    );
    assert_eq!(expected.state, AvailabilityState::Available);
    assert!(expected.last_known_good);
}

/// A target a complete listing dropped stays dropped. An older positive that lands
/// afterwards must not become the fallback, or the next inconclusive look would
/// resurrect a model the provider says it no longer offers.
#[test]
fn a_late_positive_cannot_resurrect_a_target_a_newer_complete_listing_dropped() {
    let scope = ScopeRef::tenant(tenant(1));
    let index = AvailabilityIndex::builder()
        .record(key(scope, "gpt-4o"), permitting())
        .observe(absent(scope, "gpt-4o", 300))
        .observe(present(scope, "gpt-4o", 100, None))
        .observe(outage(scope, "gpt-4o", 400))
        .build();

    let record = index.record(&key(scope, "gpt-4o")).expect("a held record");
    assert!(
        record.last_known_good.is_none(),
        "an older positive is not evidence against a newer complete listing"
    );

    let verdict = index.evaluate(&key(scope, "gpt-4o"), at(420));
    assert_ne!(verdict.state, AvailabilityState::Available);
    assert!(!verdict.last_known_good);
}

/// The same three looks in the order that displaces the negative from the current
/// slot before the late positive lands. A conclusive answer stays overturned only by
/// something newer than itself, whether or not it is still the look being held.
#[test]
fn a_dropped_target_stays_dropped_even_once_a_failed_refresh_displaces_the_listing() {
    let scope = ScopeRef::tenant(tenant(1));
    let index = AvailabilityIndex::builder()
        .record(key(scope, "gpt-4o"), permitting())
        .observe(absent(scope, "gpt-4o", 300))
        .observe(outage(scope, "gpt-4o", 400))
        .observe(present(scope, "gpt-4o", 100, None))
        .build();

    let record = index.record(&key(scope, "gpt-4o")).expect("a held record");
    assert!(record.last_known_good.is_none());
    assert_eq!(record.definitive_at, Some(at(300)));

    let verdict = index.evaluate(&key(scope, "gpt-4o"), at(420));
    assert_ne!(verdict.state, AvailabilityState::Available);
    assert!(!verdict.last_known_good);
}

/// A positive and a complete negative bearing the same instant are not evidence a
/// target is reachable, so the negative holds — and it holds whichever lands first,
/// or the index would depend on arrival order at the one instant it cannot order.
#[test]
fn two_looks_at_the_same_instant_resolve_the_same_way_whichever_lands_first() {
    let scope = ScopeRef::tenant(tenant(1));
    let negative_last = AvailabilityIndex::builder()
        .record(key(scope, "gpt-4o"), permitting())
        .observe(present(scope, "gpt-4o", 100, None))
        .observe(absent(scope, "gpt-4o", 100))
        .build();
    let negative_first = AvailabilityIndex::builder()
        .record(key(scope, "gpt-4o"), permitting())
        .observe(absent(scope, "gpt-4o", 100))
        .observe(present(scope, "gpt-4o", 100, None))
        .build();

    for index in [&negative_last, &negative_first] {
        let record = index.record(&key(scope, "gpt-4o")).expect("a held record");
        assert!(
            record.last_known_good.is_none(),
            "a contested instant is not last-known-good evidence"
        );
        assert_ne!(
            index.evaluate(&key(scope, "gpt-4o"), at(120)).state,
            AvailabilityState::Available
        );
    }
}

/// The declared-authorities constructor asserts what a deployment declares and
/// nothing about the provider account, so it stops at the entitlement rung rather
/// than reaching discovery.
#[test]
fn a_declared_record_is_unknown_until_entitlement_is_established() {
    let scope = ScopeRef::tenant(tenant(1));
    let index = AvailabilityIndex::builder()
        .record(key(scope, "gpt-4o"), AvailabilityRecord::enabled())
        .observe(present(scope, "gpt-4o", 100, None))
        .build();

    let verdict = index.evaluate(&key(scope, "gpt-4o"), at(120));
    assert_eq!(verdict.state, AvailabilityState::Unknown);
    assert_eq!(verdict.reason, AvailabilityReason::EntitlementUnknown);
    assert_eq!(verdict.decided_by, DecidedBy::Entitlement);
}

/// Retained evidence counts as "held" for the out-of-order guard too: a record
/// whose last-known-good was declared without a current observation must not let an
/// older look re-adopt evidence it predates.
#[test]
fn an_older_look_cannot_displace_retained_evidence_declared_without_a_current_one() {
    let scope = ScopeRef::tenant(tenant(1));
    let declared = AvailabilityRecord {
        last_known_good: Some(present(scope, "gpt-4o", 300, None)),
        // The watermark a derived record carries alongside its retained look, so the
        // fixture is the shape a projection actually hands over.
        definitive_at: Some(at(300)),
        ..permitting()
    };
    let builder = AvailabilityIndex::builder()
        .record(key(scope, "gpt-4o"), declared)
        .observe(absent(scope, "gpt-4o", 100));
    assert_eq!(builder.superseded(), 1);

    let verdict = builder.build().evaluate(&key(scope, "gpt-4o"), at(400));
    assert_eq!(verdict.state, AvailabilityState::Available);
    assert!(verdict.last_known_good);
    assert_eq!(verdict.observed_at, Some(at(300)));
}

#[test]
fn an_observation_older_than_the_one_held_is_ignored() {
    let scope = ScopeRef::tenant(tenant(1));
    let builder = AvailabilityIndex::builder()
        .record(key(scope, "gpt-4o"), permitting())
        .observe(absent(scope, "gpt-4o", 300))
        .observe(present(scope, "gpt-4o", 100, None));
    assert_eq!(builder.superseded(), 1);
    let index = builder.build();

    let verdict = index.evaluate(&key(scope, "gpt-4o"), at(400));
    assert_eq!(
        verdict.state,
        AvailabilityState::Denied,
        "a late arrival from a slow probe does not rewind the index"
    );

    // Which makes the order observations arrive in irrelevant: both orders of the
    // same two looks evaluate identically.
    let forwards = AvailabilityIndex::builder()
        .record(key(scope, "gpt-4o"), permitting())
        .observe(present(scope, "gpt-4o", 100, None))
        .observe(absent(scope, "gpt-4o", 300))
        .build();
    assert_eq!(
        forwards.evaluate(&key(scope, "gpt-4o"), at(400)),
        verdict.clone()
    );
}

/// One tenant's evidence decides one tenant's verdict. A listing taken with tenant
/// A's credentials describes A's account, and a model absent from it must not deny
/// tenant B — which is why observations are scoped and records are keyed by scope.
#[test]
fn evidence_never_crosses_a_tenant_or_a_project_boundary() {
    let acme = ScopeRef::tenant(tenant(1));
    let globex = ScopeRef::tenant(tenant(2));
    let acme_core = ScopeRef::project(tenant(1), project(3));
    let index = AvailabilityIndex::builder()
        .record(key(acme, "gpt-4o"), permitting())
        .record(key(globex, "gpt-4o"), permitting())
        .record(key(acme_core, "gpt-4o"), permitting())
        .observe(absent(acme, "gpt-4o", 100))
        .observe(present(globex, "gpt-4o", 100, None))
        .build();

    assert_eq!(
        index.evaluate(&key(acme, "gpt-4o"), at(200)).state,
        AvailabilityState::Denied
    );
    assert_eq!(
        index.evaluate(&key(globex, "gpt-4o"), at(200)).state,
        AvailabilityState::Available
    );
    let inherited = index.evaluate(&key(acme_core, "gpt-4o"), at(200));
    assert_eq!(
        (inherited.state, inherited.reason),
        (AvailabilityState::Unknown, AvailabilityReason::NoEvidence),
        "a project scope inherits no evidence from its tenant in this contract"
    );
    assert!(!inherited.state.is_definitive());

    // A scoped read reaches one scope's targets and no other's.
    let scoped = index.evaluate_scope(&globex, at(200));
    assert_eq!(scoped.len(), 1);
    assert_eq!(scoped[0].0, target("gpt-4o"));
    assert_eq!(scoped[0].1.state, AvailabilityState::Available);
    assert!(
        index
            .evaluate_scope(&ScopeRef::tenant(tenant(9)), at(200))
            .is_empty()
    );
}

#[test]
fn evaluating_the_whole_index_is_deterministic() {
    let acme = ScopeRef::tenant(tenant(1));
    let globex = ScopeRef::tenant(tenant(2));
    let build = |reversed: bool| {
        let keys = [
            (globex, "gpt-4o-mini"),
            (acme, "gpt-4o"),
            (globex, "gpt-4o"),
            (acme, "gpt-4o-mini"),
        ];
        let mut builder = AvailabilityIndex::builder();
        let ordered: Vec<_> = if reversed {
            keys.iter().rev().copied().collect()
        } else {
            keys.to_vec()
        };
        for (scope, model) in ordered {
            builder = builder
                .record(key(scope, model), permitting())
                .observe(present(scope, model, 100, None));
        }
        builder.build()
    };

    let forwards = build(false);
    assert_eq!(
        forwards,
        build(true),
        "insertion order is not part of an index"
    );
    let evaluated: Vec<String> = forwards
        .evaluate_all(at(200))
        .into_iter()
        .map(|(key, verdict)| format!("{key} {}", verdict.state.as_str()))
        .collect();
    assert_eq!(
        evaluated,
        [
            format!("{acme} openai/gpt-4o available"),
            format!("{acme} openai/gpt-4o-mini available"),
            format!("{globex} openai/gpt-4o available"),
            format!("{globex} openai/gpt-4o-mini available"),
        ],
        "records evaluate scope-first, then target"
    );
}

/// The redaction floor: operator-facing detail is collected, logged, and has no
/// path into a verdict — in any scope.
#[test]
fn observation_detail_never_reaches_a_verdict() {
    let scope = ScopeRef::tenant(tenant(1));
    let observation = outage(scope, "gpt-4o", 200);
    let detail = observation
        .detail
        .clone()
        .expect("the fixture carries detail");
    assert!(detail.contains("sk-live"));

    let index = AvailabilityIndex::builder()
        .record(
            key(scope, "gpt-4o"),
            AvailabilityRecord {
                credential: Some(
                    CredentialRef::parse("openai-primary").expect("a well-formed reference"),
                ),
                ..permitting()
            },
        )
        .observe(observation)
        .build();

    for scope_seen in [StatusScope::Deployment, StatusScope::Namespace] {
        let verdict = index
            .evaluate(&key(scope, "gpt-4o"), at(300))
            .for_scope(scope_seen);
        let rendered = format!("{verdict:?}");
        assert!(!rendered.contains("sk-live"), "{rendered}");
        assert!(!rendered.contains("api.example.test"), "{rendered}");
        assert!(
            !rendered.contains("openai-primary"),
            "a credential reference is provenance, not part of a verdict: {rendered}"
        );
    }
}

/// What a namespace-scoped reader loses: the deployment's discovery mechanism and
/// the reasons that describe it. What it keeps: its own state and when the
/// evidence behind it expires.
#[test]
fn a_namespace_scoped_verdict_coarsens_operator_only_reasons() {
    let scope = ScopeRef::tenant(tenant(1));
    let index = AvailabilityIndex::builder()
        .record(key(scope, "gpt-4o"), permitting())
        .observe(present(scope, "gpt-4o", 100, Some(600)))
        .observe(outage(scope, "gpt-4o", 200))
        .build();
    let operator = index.evaluate(&key(scope, "gpt-4o"), at(300));
    let tenant_view = operator.for_scope(StatusScope::Namespace);

    assert_eq!(operator.source, Some(DiscoverySource::ProviderListing));
    assert_eq!(
        tenant_view.source, None,
        "how the deployment discovers models is not a tenant's business"
    );
    assert_eq!(tenant_view.state, operator.state);
    assert_eq!(tenant_view.reason, AvailabilityReason::LastKnownGood);
    assert_eq!(tenant_view.expires_at, operator.expires_at);

    // An operator-only reason coarsens, and its dimension is not named.
    let unreliable = AvailabilityIndex::builder()
        .record(key(scope, "gpt-4o"), permitting())
        .observe(outage(scope, "gpt-4o", 200))
        .build()
        .evaluate(&key(scope, "gpt-4o"), at(300));
    assert_eq!(unreliable.reason, AvailabilityReason::DiscoveryIncomplete);
    assert_eq!(unreliable.decided_by, DecidedBy::Discovery);
    let coarsened = unreliable.for_scope(StatusScope::Namespace);
    assert_eq!(coarsened.state, AvailabilityState::Unknown);
    assert_eq!(coarsened.reason, AvailabilityReason::Unspecified);
    assert_eq!(coarsened.decided_by, DecidedBy::Undisclosed);

    // A runtime verdict is the other operator-only dimension.
    let runtime = AvailabilityIndex::builder()
        .record(
            key(scope, "gpt-4o"),
            AvailabilityRecord {
                runtime: RuntimeHealth::Unavailable,
                ..permitting()
            },
        )
        .observe(present(scope, "gpt-4o", 100, None))
        .build()
        .evaluate(&key(scope, "gpt-4o"), at(300))
        .for_scope(StatusScope::Namespace);
    assert_eq!(runtime.state, AvailabilityState::Unavailable);
    assert_eq!(runtime.reason, AvailabilityReason::Unspecified);

    // A policy engine that could not decide is the deployment's own failure, so
    // withholding the code withholds the dimension too — otherwise naming it would
    // disclose exactly what the coarsening was for. A policy *refusal* is the
    // tenant's business, and keeps its name.
    let undecided = AvailabilityIndex::builder()
        .record(
            key(scope, "gpt-4o"),
            AvailabilityRecord {
                policy: PolicyDecision::Indeterminate,
                ..permitting()
            },
        )
        .build()
        .evaluate(&key(scope, "gpt-4o"), at(300));
    assert_eq!(undecided.decided_by, DecidedBy::Policy);
    let coarsened_policy = undecided.for_scope(StatusScope::Namespace);
    assert_eq!(coarsened_policy.state, AvailabilityState::Unknown);
    assert_eq!(coarsened_policy.reason, AvailabilityReason::Unspecified);
    assert_eq!(coarsened_policy.decided_by, DecidedBy::Undisclosed);

    let refused = AvailabilityIndex::builder()
        .record(
            key(scope, "gpt-4o"),
            AvailabilityRecord {
                policy: PolicyDecision::Denied,
                ..permitting()
            },
        )
        .build()
        .evaluate(&key(scope, "gpt-4o"), at(300))
        .for_scope(StatusScope::Namespace);
    assert_eq!(refused.reason, AvailabilityReason::PolicyDenied);
    assert_eq!(refused.decided_by, DecidedBy::Policy);

    // And a reason about the tenant's own access survives intact.
    let denied = AvailabilityIndex::builder()
        .record(
            key(scope, "gpt-4o"),
            AvailabilityRecord {
                entitlement: Entitlement::Revoked,
                ..permitting()
            },
        )
        .build()
        .evaluate(&key(scope, "gpt-4o"), at(300))
        .for_scope(StatusScope::Namespace);
    assert_eq!(denied.reason, AvailabilityReason::EntitlementRevoked);
    assert_eq!(denied.decided_by, DecidedBy::Entitlement);
}

/// Every bounded vocabulary is closed and its codes are distinct: these values are
/// metric label values and response fields, so a duplicate or an unlisted variant
/// is a contract break rather than a cosmetic slip.
#[test]
fn every_vocabulary_is_closed_and_its_codes_are_distinct() {
    fn distinct(codes: Vec<&'static str>) {
        let mut sorted = codes.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), codes.len(), "duplicate code in {codes:?}");
        assert!(
            codes.iter().all(|code| !code.is_empty()
                && code
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte == b'_')),
            "{codes:?}"
        );
    }

    distinct(
        AvailabilityState::ALL
            .iter()
            .map(|state| state.as_str())
            .collect(),
    );
    distinct(
        AvailabilityReason::ALL
            .iter()
            .map(|reason| reason.code())
            .collect(),
    );
    distinct(DecidedBy::ALL.iter().map(|by| by.as_str()).collect());
    distinct(
        DiscoverySource::ALL
            .iter()
            .map(|source| source.as_str())
            .collect(),
    );
    distinct(
        DiscoveryCompleteness::ALL
            .iter()
            .map(|completeness| completeness.as_str())
            .collect(),
    );
    distinct(
        DiscoveryResult::ALL
            .iter()
            .map(|result| result.as_str())
            .collect(),
    );
    distinct(
        CataloguePresence::ALL
            .iter()
            .map(|presence| presence.as_str())
            .collect(),
    );
    distinct(Enablement::ALL.iter().map(|value| value.as_str()).collect());
    distinct(
        Entitlement::ALL
            .iter()
            .map(|value| value.as_str())
            .collect(),
    );
    distinct(
        PolicyDecision::ALL
            .iter()
            .map(|value| value.as_str())
            .collect(),
    );
    distinct(
        RuntimeHealth::ALL
            .iter()
            .map(|value| value.as_str())
            .collect(),
    );
}

#[test]
fn a_reference_is_bounded_printable_and_typed() {
    assert_eq!(
        ProviderRef::parse("openai").expect("valid").as_str(),
        "openai"
    );
    // Upstream model ids carry characters a slug may not, which is why references
    // are their own type rather than a `Slug`.
    for model in [
        "gpt-4.1",
        "llama3.1:8b",
        "meta-llama/Llama-3-8b",
        "gpt-4o@2024",
    ] {
        assert_eq!(ModelRef::parse(model).expect("valid").as_str(), model);
    }

    assert_eq!(
        ModelRef::parse(""),
        Err(InvalidToken::Empty { kind: "model" })
    );
    let long = "m".repeat(Token::MAX_LEN + 1);
    assert_eq!(
        ModelRef::parse(&long),
        Err(InvalidToken::TooLong {
            kind: "model",
            length: Token::MAX_LEN + 1,
            max: Token::MAX_LEN,
        })
    );
    // A newline would let a name forge a second log record; a space, a tab, and a
    // control character are refused for the same reason.
    for hostile in [
        "gpt-4o\ninjected",
        "gpt 4o",
        "gpt\t4o",
        "gpt-4o\u{7f}",
        "gpt\u{feff}4o",
    ] {
        assert!(
            matches!(
                ModelRef::parse(hostile),
                Err(InvalidToken::Unprintable { .. })
            ),
            "{hostile:?} must be refused"
        );
    }
}

#[test]
fn a_scope_renders_its_tenant_and_project() {
    let tenant_wide = ScopeRef::tenant(tenant(1));
    assert!(tenant_wide.is_tenant_wide());
    assert_eq!(tenant_wide.to_string(), tenant(1).to_string());

    let scoped = ScopeRef::project(tenant(1), project(3));
    assert!(!scoped.is_tenant_wide());
    assert_eq!(
        scoped.to_string(),
        format!("{}/{}", tenant(1), project(3)),
        "a project scope is named beyond its tenant"
    );
    assert_ne!(tenant_wide, scoped);
}

/// A record whose looks a later conclusion discredited holds a watermark and no
/// look, so it emits no row. It must still be named as cleared: otherwise the
/// rows the conclusion removed sit in the database until the next restart reads
/// them back as evidence.
#[test]
fn a_key_whose_looks_were_discredited_is_written_as_cleared() {
    let scope = ScopeRef::tenant(tenant(1));
    let discredited = AvailabilityRecord {
        definitive_at: Some(at(500)),
        ..permitting()
    };
    let index = AvailabilityIndex::builder()
        .record(key(scope, "gpt-4o"), permitting())
        .observe(present(scope, "gpt-4o", 300, None))
        .record(key(scope, "gpt-4o"), discredited)
        .build();

    let write = EvidenceWrite::of_index(&index);

    assert!(write.rows().is_empty(), "both looks were discredited");
    assert_eq!(
        write.cleared(),
        [EvidenceClear::new(key(scope, "gpt-4o"), at(500))],
        "so the stored looks must go with them"
    );
}

#[test]
fn clearing_same_key_keeps_the_newest_cutoff() {
    let target_key = key(ScopeRef::tenant(tenant(1)), "gpt-4o");
    let write = EvidenceWrite::default().clearing([
        EvidenceClear::new(target_key.clone(), at(100)),
        EvidenceClear::new(target_key.clone(), at(200)),
    ]);

    assert_eq!(
        write.cleared(),
        [EvidenceClear::new(target_key, at(200))],
        "deduplicating cleanup must retain the maximum cutoff"
    );
}
