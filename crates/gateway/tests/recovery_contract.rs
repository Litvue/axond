//! The recovery qualification contract (axond #219), asserted before the driver
//! that will satisfy it exists.
//!
//! A stateful deployment's recovery story — serving through a control-plane
//! outage, cold-booting from the signed last-known-good cache, converging once
//! Postgres returns, rotating a credential without a redeployment, and restoring
//! the journal from a backup or to a point in time — cannot be exercised yet:
//! the resource bodies a revision is made of belong to slices that have not
//! landed, so a stateful replica has nothing to serve and there is no recovery
//! behaviour to observe.
//!
//! What can be pinned down now is the contract. `qualification/recovery/manifest.toml`
//! declares every scenario, the evidence it retains, the gate that makes it a
//! failure, and the slice each blocked scenario waits on; the tests here are
//! what keep that file from decaying into a wish list while the dependencies
//! land. They fail when a scenario loses its gate, when the evidence #219
//! requires stops being covered, when a dependency edge is dropped or invented,
//! when the prose contract and the manifest disagree, and — the one that matters
//! most while the harness is unimplemented — when a scenario claims to be
//! executable and no driver runs it.

mod support;

use std::collections::{BTreeMap, BTreeSet};

use support::recovery::{self, BLOCKING_ISSUES, Capability, Evidence, Status};

#[test]
fn every_scenario_the_issue_names_is_committed_exactly_once() {
    let manifest = recovery::load();

    let mut by_capability: BTreeMap<Capability, Vec<&str>> = BTreeMap::new();
    for scenario in &manifest.scenarios {
        by_capability
            .entry(scenario.capability)
            .or_default()
            .push(scenario.id.as_str());
    }

    for capability in Capability::ALL {
        let ids = by_capability.get(&capability).map(Vec::as_slice);
        assert_eq!(
            ids.map(<[&str]>::len).unwrap_or_default(),
            1,
            "the {} scenario must be committed exactly once, found {:?}",
            capability.as_str(),
            ids.unwrap_or_default()
        );
    }

    let ids: BTreeSet<&str> = manifest
        .scenarios
        .iter()
        .map(|scenario| scenario.id.as_str())
        .collect();
    assert_eq!(
        ids.len(),
        manifest.scenarios.len(),
        "scenario ids must be unique: {ids:?}"
    );
}

/// A scenario is only evidence if it is gated. These are the bounds that do not
/// move with the machine, so every scenario carries all of them, and the one
/// that is never negotiable — an outage may refuse a change and may never admit
/// an unauthenticated caller — is the same value everywhere.
#[test]
fn every_scenario_carries_a_gate_that_can_fail() {
    for scenario in &recovery::load().scenarios {
        let gate = scenario.gate;
        let id = &scenario.id;
        assert!(
            (0.0..=1.0).contains(&gate.max_serving_error_fraction),
            "{id}: a serving-error ceiling is a fraction, not {}",
            gate.max_serving_error_fraction
        );
        assert!(
            gate.max_convergence_lag_seconds > 0,
            "{id}: a convergence bound of zero cannot be met by any real replica"
        );
        assert_eq!(
            gate.max_unauthenticated_admin_successes, 0,
            "{id}: recovery never relaxes administrative authentication"
        );
        assert!(
            !scenario.description.trim().is_empty(),
            "{id}: a scenario without a description is not reproducible by a reader"
        );
    }
}

/// Every evidence class #219 requires is retained by some scenario, and no
/// scenario declares one twice. A harness that ran every outage and kept no
/// data-loss boundary would still produce artifacts, and the artifacts would
/// read as evidence.
#[test]
fn the_committed_scenarios_retain_every_required_evidence_class() {
    let manifest = recovery::load();
    let retained: BTreeSet<Evidence> = manifest
        .scenarios
        .iter()
        .flat_map(|scenario| scenario.evidence.iter().copied())
        .collect();

    for class in Evidence::ALL {
        assert!(
            retained.contains(&class),
            "no committed scenario retains {}",
            class.as_str()
        );
    }

    for scenario in &manifest.scenarios {
        let unique: BTreeSet<Evidence> = scenario.evidence.iter().copied().collect();
        assert_eq!(
            unique.len(),
            scenario.evidence.len(),
            "{}: evidence classes are declared more than once",
            scenario.id
        );
        assert!(
            !scenario.evidence.is_empty(),
            "{}: a scenario that retains nothing proves nothing",
            scenario.id
        );
    }
}

/// The dependency map, in both directions: a blocked scenario names the slices
/// it waits on and what it needs from each, and between them the scenarios
/// account for every slice #219 is waiting on. An issue no scenario names is
/// either a dependency that is not really one, or a scenario that was dropped.
#[test]
fn the_dependency_map_is_complete_in_both_directions() {
    let manifest = recovery::load();
    let known: BTreeSet<u32> = BLOCKING_ISSUES.into_iter().collect();
    let mut named: BTreeSet<u32> = BTreeSet::new();

    for scenario in &manifest.scenarios {
        let id = &scenario.id;
        match scenario.status {
            Status::Blocked => assert!(
                !scenario.blocked_on.is_empty(),
                "{id}: a blocked scenario must name what blocks it"
            ),
            Status::Executable => assert!(
                scenario.blocked_on.is_empty(),
                "{id}: an executable scenario cannot still be waiting on a slice"
            ),
        }

        let mut issues = BTreeSet::new();
        for dependency in &scenario.blocked_on {
            assert!(
                known.contains(&dependency.issue),
                "{id}: #{} is not one of the slices #219 waits on",
                dependency.issue
            );
            assert!(
                issues.insert(dependency.issue),
                "{id}: #{} is named twice",
                dependency.issue
            );
            assert!(
                dependency.needs.len() > 20,
                "{id}: #{} needs a reason a reader can act on, not {:?}",
                dependency.issue,
                dependency.needs
            );
            named.insert(dependency.issue);
        }
    }

    let unclaimed: Vec<u32> = known.difference(&named).copied().collect();
    assert!(
        unclaimed.is_empty(),
        "no scenario waits on {unclaimed:?}; either the slice is not a blocker or a scenario was dropped"
    );
}

/// The honesty gate. `status = "executable"` is a claim about code, and the code
/// is [`Capability::is_implemented`]: flipping a manifest entry without writing
/// the driver fails here, and writing the driver without flipping the entry
/// fails here too.
#[test]
fn a_scenario_is_executable_only_when_a_driver_runs_it() {
    for scenario in &recovery::load().scenarios {
        let implemented = scenario.capability.is_implemented();
        let claimed = scenario.status == Status::Executable;
        assert_eq!(
            claimed,
            implemented,
            "{}: the manifest says {}, the driver says {}",
            scenario.id,
            if claimed { "executable" } else { "blocked" },
            if implemented {
                "it is implemented"
            } else {
                "it is not implemented"
            }
        );
    }
}

/// The prose contract and the manifest describe one harness. An operator reads
/// the first; the driver will read the second; a scenario or a blocker that
/// exists in only one of them is how the two come to disagree.
#[test]
fn the_prose_contract_and_the_manifest_agree() {
    let manifest = recovery::load();
    let contract = recovery::contract_text();

    for scenario in &manifest.scenarios {
        assert!(
            contract.contains(&format!("`{}`", scenario.id)),
            "{} does not describe the {} scenario",
            recovery::CONTRACT_RELATIVE,
            scenario.id
        );
    }
    for issue in BLOCKING_ISSUES {
        assert!(
            contract.contains(&format!("#{issue}")),
            "{} does not account for blocker #{issue}",
            recovery::CONTRACT_RELATIVE
        );
    }
    for class in Evidence::ALL {
        assert!(
            contract.contains(&format!("`{}`", class.as_str())),
            "{} does not say what {} holds",
            recovery::CONTRACT_RELATIVE,
            class.as_str()
        );
    }
}
