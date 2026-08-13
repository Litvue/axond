//! The recovery qualification contract (axond #219), asserted against the
//! manifest the driver reads.
//!
//! A stateful deployment's recovery story — serving through a control-plane
//! outage, cold-booting from the signed last-known-good cache, converging once
//! Postgres returns, rotating a credential without a redeployment, and restoring
//! the journal from a backup or to a point in time — is only half runnable
//! today. The control-plane half runs against a real Postgres and writes its
//! evidence to `target/recovery/`; the serving half needs a projection a replica
//! can serve, and is a blocked stage rather than a claim.
//!
//! `qualification/recovery/manifest.toml` declares every scenario, the stages it
//! is assembled from, the evidence each stage retains, the gate that makes the
//! scenario a failure, and the slice each blocked stage waits on. The tests here
//! are what keep that file from decaying into a wish list. They fail when a
//! scenario loses its gate, when the evidence #219 requires stops being covered,
//! when a dependency edge is dropped or invented, when a blocked stage claims
//! its scenario is executable, and when the prose contract and the manifest
//! disagree. Whether the *driver* runs the stages the manifest calls executable
//! is asserted next to the driver, in `qualification::recovery`.

mod support;

use std::collections::{BTreeMap, BTreeSet};

use support::recovery::{self, BLOCKING_ISSUES, Capability, Evidence, Readiness, Runner, Status};

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

/// `max_serving_error_fraction` bounds the requests a scenario offers, so the
/// ceiling and the `serving_behavior` evidence have to travel together in both
/// directions: a refusing scenario offers no traffic and must not claim serving
/// evidence, and a serving scenario must retain it, or its ceiling is satisfied
/// by a run that never sent a request.
#[test]
fn the_serving_gate_and_the_serving_evidence_agree() {
    for scenario in &recovery::load().scenarios {
        let retains_serving = scenario.evidence().contains(&Evidence::ServingBehavior);
        match scenario.gate.readiness {
            Readiness::Refuses => assert!(
                !retains_serving,
                "{}: a scenario that refuses readiness serves nothing, so its zero \
                 serving-error ceiling is vacuous rather than a serving guarantee",
                scenario.id
            ),
            Readiness::Serves => assert!(
                retains_serving,
                "{}: a serving scenario must retain serving_behavior, or its \
                 serving-error ceiling passes without a request being offered",
                scenario.id
            ),
        }
    }
}

/// Every evidence class #219 requires is retained by some stage, and no stage
/// declares one twice. A harness that ran every outage and kept no data-loss
/// boundary would still produce artifacts, and the artifacts would read as
/// evidence.
#[test]
fn the_committed_scenarios_retain_every_required_evidence_class() {
    let manifest = recovery::load();
    let retained: BTreeSet<Evidence> = manifest
        .scenarios
        .iter()
        .flat_map(|scenario| scenario.evidence())
        .collect();

    for class in Evidence::ALL {
        assert!(
            retained.contains(&class),
            "no committed scenario retains {}",
            class.as_str()
        );
    }

    for scenario in &manifest.scenarios {
        for stage in &scenario.stages {
            let unique: BTreeSet<Evidence> = stage.evidence.iter().copied().collect();
            assert_eq!(
                unique.len(),
                stage.evidence.len(),
                "{}/{}: evidence classes are declared more than once",
                scenario.id,
                stage.id
            );
            assert!(
                !stage.evidence.is_empty(),
                "{}/{}: a stage that retains nothing proves nothing",
                scenario.id,
                stage.id
            );
            assert!(
                !stage.covers.trim().is_empty(),
                "{}/{}: a stage without a description is not reproducible by a reader",
                scenario.id,
                stage.id
            );
        }
    }
}

/// A stage is one half of a scenario, and the halves must partition it: two
/// stages retaining the same evidence class means one of them is not the stage
/// that produces it, and a reader cannot tell which artifact to look in.
#[test]
fn no_two_stages_of_a_scenario_claim_the_same_evidence() {
    for scenario in &recovery::load().scenarios {
        let mut ids = BTreeSet::new();
        let mut seen: BTreeMap<Evidence, &str> = BTreeMap::new();
        for stage in &scenario.stages {
            assert!(
                ids.insert(stage.id.as_str()),
                "{}: stage `{}` is declared twice",
                scenario.id,
                stage.id
            );
            for class in &stage.evidence {
                if let Some(other) = seen.insert(*class, stage.id.as_str()) {
                    panic!(
                        "{}: stages `{other}` and `{}` both retain {}",
                        scenario.id,
                        stage.id,
                        class.as_str()
                    );
                }
            }
        }
        assert!(
            !scenario.stages.is_empty(),
            "{}: a scenario with no stages runs nothing",
            scenario.id
        );
    }
}

/// The dependency map, in both directions: a blocked stage names the slices it
/// waits on and what it needs from each, and between them the stages account
/// for every slice #219 is waiting on. An issue no stage names is either a
/// dependency that is not really one, or a stage that was dropped.
#[test]
fn the_dependency_map_is_complete_in_both_directions() {
    let manifest = recovery::load();
    let known: BTreeSet<u32> = BLOCKING_ISSUES.into_iter().collect();
    let mut named: BTreeSet<u32> = BTreeSet::new();

    for scenario in &manifest.scenarios {
        for stage in &scenario.stages {
            let id = format!("{}/{}", scenario.id, stage.id);
            match stage.status {
                Status::Blocked => assert!(
                    !stage.blocked_on.is_empty(),
                    "{id}: a blocked stage must name what blocks it"
                ),
                Status::Executable => assert!(
                    stage.blocked_on.is_empty(),
                    "{id}: an executable stage cannot still be waiting on a slice"
                ),
            }

            let mut issues = BTreeSet::new();
            for dependency in &stage.blocked_on {
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
    }

    let unclaimed: Vec<u32> = known.difference(&named).copied().collect();
    assert!(
        unclaimed.is_empty(),
        "no stage waits on {unclaimed:?}; either the slice is not a blocker or a stage was dropped"
    );
}

/// Durable inventory is the only recovery stage that can claim restoration of
/// the state introduced by the catalogue, pricing, and SecretStore slices.
/// Keep those edges exact: attaching one to serving or reconvergence would
/// either make the wrong stage appear unblocked or overstate what the restore
/// drill proves.
#[test]
fn durable_inventory_owns_the_secret_catalogue_and_pricing_dependencies() {
    let manifest = recovery::load();
    let owners = |issue| {
        manifest
            .scenarios
            .iter()
            .flat_map(|scenario| {
                scenario.stages.iter().flat_map(move |stage| {
                    stage
                        .blocked_on
                        .iter()
                        .filter(move |dependency| dependency.issue == issue)
                        .map(move |_| format!("{}/{}", scenario.id, stage.id))
                })
            })
            .collect::<BTreeSet<_>>()
    };
    let expected = |stages: &[&str]| {
        stages
            .iter()
            .map(|stage| (*stage).to_owned())
            .collect::<BTreeSet<_>>()
    };

    assert_eq!(
        owners(145),
        expected(&[
            "backup-restore/durable-inventory",
            "secret-rotation/rotation",
        ])
    );
    assert_eq!(owners(146), expected(&["backup-restore/durable-inventory"]));
    assert_eq!(owners(147), expected(&["backup-restore/durable-inventory"]));
    assert_eq!(
        owners(158),
        expected(&[
            "cold-boot-no-cache/readiness",
            "cold-boot-invalid-cache/readiness",
        ])
    );
}

/// A slice may leave the dependency map, but only by saying what became of it.
/// Deleting the last claim on a slice is indistinguishable from deleting the
/// stage that needed it, so a retirement is recorded, cannot also be a live
/// blocker, cannot still be named by a stage, and has to be accounted for in
/// the operator contract with the issue that still tracks the rest of it.
#[test]
fn a_retired_blocker_says_what_became_of_the_slice() {
    let manifest = recovery::load();
    let live: BTreeSet<u32> = BLOCKING_ISSUES.into_iter().collect();
    let contract = recovery::contract_text();

    let committed: BTreeSet<u32> = manifest.retired_blockers.iter().map(|r| r.issue).collect();
    let recorded: BTreeSet<u32> = recovery::RETIRED_BLOCKERS.iter().map(|(i, _)| *i).collect();
    assert_eq!(
        committed,
        recorded,
        "{} and RETIRED_BLOCKERS disagree about which slices were retired",
        recovery::MANIFEST_RELATIVE
    );
    for retired in &manifest.retired_blockers {
        assert!(
            retired.became.len() > 80,
            "#{}: the manifest has to say what became of a retired slice, not {:?}",
            retired.issue,
            retired.became
        );
    }

    for (issue, became) in recovery::RETIRED_BLOCKERS {
        assert!(
            !live.contains(&issue),
            "#{issue} is both retired and a live blocker"
        );
        assert!(
            became.len() > 80,
            "#{issue}: a retirement needs a reason a reader can act on, not {became:?}"
        );
        for scenario in &manifest.scenarios {
            for stage in &scenario.stages {
                assert!(
                    stage.blocked_on.iter().all(|d| d.issue != issue),
                    "{}/{}: waits on retired slice #{issue}",
                    scenario.id,
                    stage.id
                );
            }
        }
        assert!(
            contract.contains(&format!("#{issue}")),
            "{} does not say what became of retired slice #{issue}",
            recovery::CONTRACT_RELATIVE
        );
    }
}

/// The honesty gate at the scenario level: a scenario is executable exactly
/// when every stage of it is, so a scenario cannot be reported as qualified
/// while the half that offers traffic is still waiting on a slice.
#[test]
fn a_scenario_is_executable_only_when_every_stage_of_it_is() {
    for scenario in &recovery::load().scenarios {
        let blocked: Vec<&str> = scenario
            .stages
            .iter()
            .filter(|stage| stage.status == Status::Blocked)
            .map(|stage| stage.id.as_str())
            .collect();
        match scenario.status() {
            Status::Executable => assert!(
                blocked.is_empty(),
                "{}: reported executable while {blocked:?} are blocked",
                scenario.id
            ),
            Status::Blocked => assert!(
                !blocked.is_empty(),
                "{}: reported blocked with every stage executable",
                scenario.id
            ),
        }
    }
}

/// A stage that runs names the lane that runs it, and a stage that does not
/// names none. The runner is what `ops/check-recovery-evidence.py` reads to
/// decide which artifacts a lane owes: an executable stage without one would
/// be a stage no lane is checked for, which is exactly the shape of evidence
/// quietly going missing.
#[test]
fn every_executable_stage_names_the_lane_that_runs_it() {
    let manifest = recovery::load();
    let mut per_runner: BTreeMap<Runner, Vec<String>> = BTreeMap::new();

    for scenario in &manifest.scenarios {
        for stage in &scenario.stages {
            let id = format!("{}/{}", scenario.id, stage.id);
            match (stage.status, stage.runner) {
                (Status::Executable, Some(runner)) => {
                    per_runner.entry(runner).or_default().push(id);
                }
                (Status::Executable, None) => {
                    panic!("{id}: an executable stage must name the lane that runs it")
                }
                (Status::Blocked, Some(runner)) => panic!(
                    "{id}: a blocked stage cannot claim the `{}` lane runs it",
                    runner.as_str()
                ),
                (Status::Blocked, None) => {}
            }
        }
    }

    for runner in Runner::ALL {
        assert!(
            per_runner.contains_key(&runner),
            "the `{}` lane runs no stage; either it is dead or its stages were dropped",
            runner.as_str()
        );
        assert!(
            recovery::contract_text().contains(&format!("`{}`", runner.as_str())),
            "{} does not say what the `{}` lane runs",
            recovery::CONTRACT_RELATIVE,
            runner.as_str()
        );
    }
}

/// The prose contract and the manifest describe one harness. An operator reads
/// the first; the driver reads the second; a scenario, a stage, or a blocker
/// that exists in only one of them is how the two come to disagree.
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
        for stage in &scenario.stages {
            assert!(
                contract.contains(&format!("`{}/{}`", scenario.id, stage.id)),
                "{} does not say what the {}/{} stage covers",
                recovery::CONTRACT_RELATIVE,
                scenario.id,
                stage.id
            );
        }
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

/// The two lanes write into one evidence directory, and a reader compares the
/// artifacts in it. That only holds while they agree on the schema: the shell
/// lane's recorder repeats the version the driver's `EVIDENCE_SCHEMA_VERSION`
/// declares, and a bump on one side that is not made on the other silently
/// produces a directory holding two schemas under one version number.
#[test]
fn the_two_lanes_write_the_same_artifact_schema() {
    let root = recovery::workspace_root();
    let driver = std::fs::read_to_string(root.join("crates/gateway/src/qualification/evidence.rs"))
        .expect("the driver's evidence module is readable");
    let declared = driver
        .lines()
        .find_map(|line| {
            line.trim()
                .strip_prefix("pub(crate) const EVIDENCE_SCHEMA_VERSION: u32 = ")
                .and_then(|rest| rest.trim_end_matches(';').parse::<u32>().ok())
        })
        .expect("the driver declares an evidence schema version");

    for lane in ["ops/recovery-evidence.py", "ops/check-recovery-evidence.py"] {
        let source = std::fs::read_to_string(root.join(lane)).expect("the lane script is readable");
        let repeated = source
            .lines()
            .find_map(|line| {
                line.strip_prefix("SCHEMA_VERSION = ")
                    .and_then(|rest| rest.trim().parse::<u32>().ok())
            })
            .unwrap_or_else(|| panic!("{lane} declares no SCHEMA_VERSION"));
        assert_eq!(
            repeated, declared,
            "{lane} writes schema {repeated} and the driver writes {declared}"
        );
    }
}

/// Every stage the manifest calls executable is owed an artifact, and the
/// checker is what turns a lane that produced none into a failure rather than
/// an empty upload. It reads the manifest, so it must accept every lane the
/// manifest names — a lane it rejects is a lane nothing checks.
#[test]
fn the_evidence_checker_accepts_every_lane_the_manifest_names() {
    let checker =
        std::fs::read_to_string(recovery::workspace_root().join("ops/check-recovery-evidence.py"))
            .expect("the evidence checker is readable");
    for runner in Runner::ALL {
        assert!(
            checker.contains(&format!("\"{}\"", runner.as_str())),
            "ops/check-recovery-evidence.py cannot be run for the `{}` lane",
            runner.as_str()
        );
    }
}
