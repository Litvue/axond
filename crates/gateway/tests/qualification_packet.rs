//! The production qualification packet (axond #156), checked against the tree
//! it describes.
//!
//! #156 is the integrated gate: capacity, faults, recovery, rollout, and a long
//! soak, reported together. Its children merge separately, and a merged child
//! is easy to mistake for an answered question — the capacity harness landing
//! says how the measuring is done, not what a replica was measured doing. This
//! suite is what makes that distinction mechanical.
//!
//! It fails when the packet names a file that is not there, when a slice claims
//! a rung of the ladder its own fields do not reach, when retained evidence is
//! not reproducible from the manifest it claims to have run, when a scenario
//! #156 lists belongs to no slice, when the recovery dependency map drifts from
//! the recovery manifest, and when the packet and its prose page disagree.

mod support;

use std::collections::BTreeSet;

use support::packet::{self, EPIC_ISSUE, Runner, Scenario, SliceId, Status};
use support::recovery;

/// Every slice #156 decomposes into is committed exactly once, owned by a child
/// issue, and carries the question it exists to answer.
#[test]
fn every_slice_the_epic_names_is_committed_exactly_once() {
    let packet = packet::load();

    let ids: Vec<SliceId> = packet.slices.iter().map(|slice| slice.id).collect();
    for id in SliceId::ALL {
        assert_eq!(
            ids.iter().filter(|committed| **committed == id).count(),
            1,
            "the {} slice must be committed exactly once, found {ids:?}",
            id.as_str()
        );
    }
    assert_eq!(
        ids.len(),
        SliceId::ALL.len(),
        "the packet committed a slice #156 does not name: {ids:?}"
    );

    for slice in &packet.slices {
        assert!(
            slice.issue != EPIC_ISSUE && slice.issue > 0,
            "{}: a slice is owned by a child issue, not by the epic",
            slice.id.as_str()
        );
        assert!(
            !slice.question.trim().is_empty(),
            "{}: a slice without a question is not a slice of anything",
            slice.id.as_str()
        );
    }
}

/// Every scenario the issue lists is some slice's responsibility. A scenario no
/// slice covers is one the epic would close over in silence.
#[test]
fn every_scenario_the_epic_lists_belongs_to_a_slice() {
    let packet = packet::load();

    let covered: BTreeSet<Scenario> = packet
        .slices
        .iter()
        .flat_map(|slice| slice.covers.iter().copied())
        .collect();
    for scenario in Scenario::ALL {
        assert!(
            covered.contains(&scenario),
            "no slice covers {}, so #156 could close without it",
            scenario.as_str()
        );
    }

    for slice in &packet.slices {
        let unique: BTreeSet<Scenario> = slice.covers.iter().copied().collect();
        assert_eq!(
            unique.len(),
            slice.covers.len(),
            "{}: a scenario is listed twice",
            slice.id.as_str()
        );
        assert!(
            !slice.covers.is_empty(),
            "{}: a slice that covers no scenario answers nothing",
            slice.id.as_str()
        );
    }
}

/// Every path the packet names exists. The packet is the index a reader follows
/// to the evidence, and an index into files that are not there is worse than no
/// index — it reads like coverage.
#[test]
fn every_path_the_packet_names_exists() {
    let root = packet::workspace_root();
    for slice in &packet::load().slices {
        for relative in slice.paths() {
            assert!(
                root.join(relative).exists(),
                "{}: {relative} does not exist",
                slice.id.as_str()
            );
        }
    }
}

/// The status ladder is derived, not asserted. Each rung has a requirement the
/// slice's own fields either meet or do not, so a slice cannot promote itself:
/// `declared` needs a manifest, `harnessed` needs a driver that runs, and
/// `evidenced` needs a run retained in the repository.
#[test]
fn a_slice_cannot_claim_a_rung_it_has_not_reached() {
    for slice in &packet::load().slices {
        let id = slice.id.as_str();
        match slice.status {
            Status::Unbuilt => {
                assert!(
                    slice.manifest.is_none() && slice.driver.is_none(),
                    "{id}: it has inputs, so it is past unbuilt"
                );
                assert!(
                    slice.retained.is_empty(),
                    "{id}: an unbuilt slice cannot have retained a run"
                );
            }
            Status::Declared => {
                assert!(
                    slice.manifest.is_some() && slice.contract.is_some(),
                    "{id}: a declared slice commits a manifest and a contract page"
                );
                assert!(
                    slice.contract_test.is_some(),
                    "{id}: a declared contract nothing checks is a wish list"
                );
                assert!(
                    slice.driver.is_none(),
                    "{id}: it has a driver, so its scenarios are run rather than declared"
                );
                assert!(
                    slice.retained.is_empty(),
                    "{id}: a declared slice has no driver, so it has measured nothing"
                );
            }
            Status::Harnessed => {
                assert!(
                    slice.manifest.is_some() && slice.driver.is_some(),
                    "{id}: a harnessed slice has a manifest and a driver"
                );
                assert!(
                    slice.reduced_lane.is_some(),
                    "{id}: a driver that no lane runs is not a harness"
                );
                // A short run may be retained here — it is how a harness shows
                // it produces records at all. What it may not do is pass for
                // the measurement the heavy tier exists to take.
                if let Some(heavy) = slice.heavy_tier.as_deref() {
                    assert!(
                        !slice
                            .retained
                            .iter()
                            .any(|relative| packet::load_record(relative).tier == heavy),
                        "{id}: it retains a {heavy}-tier run, so it is evidenced rather \
                         than harnessed"
                    );
                }
            }
            Status::Evidenced => {
                assert!(
                    slice.manifest.is_some() && slice.driver.is_some(),
                    "{id}: an evidenced slice has a manifest and a driver"
                );
                assert!(
                    !slice.retained.is_empty(),
                    "{id}: evidence is a retained run, not a status"
                );
                let heavy = slice
                    .heavy_tier
                    .as_deref()
                    .unwrap_or_else(|| panic!("{id}: a slice with runs names its heavy tier"));
                assert!(
                    slice
                        .retained
                        .iter()
                        .any(|relative| packet::load_record(relative).tier == heavy),
                    "{id}: it is evidenced by its short tier alone, which is a \
                     correctness run rather than a measurement of what a replica does"
                );
            }
        }

        assert_eq!(
            slice.outstanding.is_some(),
            slice.status != Status::Evidenced,
            "{id}: a slice owes #156 something until it is evidenced, and nothing after"
        );
    }
}

/// Every retained record, whichever slice retains it, has to be reproducible
/// from the repository: the manifest it ran is the manifest its slice commits
/// and still hashes to what the run saw, every workload in that manifest is in
/// the record, the run was judged rather than merely measured, and it came off
/// a clean tree.
#[test]
fn retained_evidence_is_reproducible_from_the_committed_inputs() {
    let packet = packet::load();
    let mut checked = 0;

    for slice in &packet.slices {
        if slice.retained.is_empty() {
            continue;
        }
        let slice_manifest = slice.manifest.as_deref().unwrap_or_else(|| {
            panic!(
                "{}: a record of a run against no committed manifest",
                slice.id.as_str()
            )
        });
        let (manifest, manifest_sha256) = packet::load_slice_manifest(slice_manifest);
        let profiles: BTreeSet<String> = manifest
            .profiles
            .iter()
            .map(|profile| profile.id.clone())
            .collect();

        for relative in &slice.retained {
            checked += 1;
            let record = packet::load_record(relative);
            assert_eq!(
                record.slice_id,
                slice.id,
                "{relative}: retained by the {} slice but recorded against another",
                slice.id.as_str()
            );
            assert_eq!(
                record.inputs.manifest.as_str(),
                slice_manifest,
                "{relative}: the run read a manifest the slice does not commit"
            );
            assert_eq!(
                record.inputs.manifest_sha256, manifest_sha256,
                "{relative}: the manifest has changed since the run, so the record \
                 describes a workload the repository no longer defines \u{2014} re-run \
                 the tier and rewrite it"
            );
            assert!(
                !record.source.git_dirty,
                "{relative}: a run off a modified tree cannot be reproduced from a commit"
            );
            assert!(
                !record.source.git_commit.is_empty() && !record.binary.sha256.is_empty(),
                "{relative}: a record without a commit and a binary digest is anonymous"
            );

            let recorded: BTreeSet<String> = record
                .profiles
                .iter()
                .map(|profile| profile.id.clone())
                .collect();
            assert_eq!(
                recorded, profiles,
                "{relative}: the record and the manifest disagree about the profiles"
            );

            for profile in &record.profiles {
                let id = &profile.id;
                assert!(
                    profile.verdicts > 0,
                    "{relative}: {id} was measured against no threshold"
                );
                assert!(
                    profile.passed,
                    "{relative}: {id} is retained as evidence of a run that failed its gates"
                );
                assert!(
                    profile.elapsed_ms > 0 && !profile.config_sha256.is_empty(),
                    "{relative}: {id} records neither how long it ran nor what it booted"
                );
                assert_eq!(
                    profile.offered, profile.requests,
                    "{relative}: {id} offered fewer requests than the manifest asks for"
                );
                assert_eq!(
                    profile.accepted + profile.rejected + profile.errors,
                    profile.offered,
                    "{relative}: {id} loses requests between offered and accounted for"
                );
                assert_eq!(
                    profile.missing_usage_records, 0,
                    "{relative}: {id} lost usage rows"
                );
                assert_eq!(
                    profile.leaked_upstream_streams, 0,
                    "{relative}: {id} left an upstream stream open"
                );
            }
        }
    }

    assert!(
        checked > 0,
        "the packet retains no evidence at all, so nothing here checked anything"
    );
}

/// A record off a contributor's machine is evidence about that machine. The
/// packet may hold one — it is how the first envelope gets written — but the
/// prose has to say so, so nobody reads a debug build on a laptop as a fleet
/// baseline.
#[test]
fn a_locally_recorded_run_is_disclosed_as_one() {
    let packet = packet::load();
    let contract = packet::contract_text();

    for slice in &packet.slices {
        for relative in &slice.retained {
            let record = packet::load_record(relative);
            if record.runner == Runner::Local {
                assert!(
                    contract.contains(relative),
                    "{relative}: a locally recorded run must be named in \
                     {}, where its provenance is disclosed",
                    packet::CONTRACT_RELATIVE
                );
                assert!(
                    !record.runner_note.trim().is_empty(),
                    "{relative}: a local run without a note about the machine is \
                     an envelope nobody can reproduce"
                );
                // The commit too, and not only the path: a re-run rewrites the
                // record but leaves the prose pointing at history the reader
                // cannot check out.
                let short = &record.source.git_commit[..7];
                assert!(
                    contract.contains(short),
                    "{relative}: it was taken at {short}, which {} does not \
                     mention \u{2014} the disclosure has drifted from the record",
                    packet::CONTRACT_RELATIVE
                );
            }
        }
    }
}

/// The recovery slice's dependency list is the recovery manifest's, not a
/// second copy that drifts. Landing a slice has to move both, or the packet
/// starts reporting a blocker that is gone.
#[test]
fn the_recovery_slice_waits_on_exactly_what_its_manifest_waits_on() {
    let slice = packet::load().slice(SliceId::Recovery).clone();

    let declared: BTreeSet<u32> = slice.blocked_on.iter().copied().collect();
    let manifest: BTreeSet<u32> = recovery::load()
        .scenarios
        .iter()
        .flat_map(|scenario| {
            scenario
                .blocked_on
                .iter()
                .map(|dependency| dependency.issue)
        })
        .collect();

    assert_eq!(
        declared,
        manifest,
        "the packet and {} disagree about what recovery waits on",
        recovery::MANIFEST_RELATIVE
    );
}

/// Closure is derived from the slices. The flag exists so the packet states its
/// own conclusion in one place, and this is what stops that conclusion from
/// being an opinion: #156 is answered when every slice is evidenced, and not
/// one merge earlier.
#[test]
fn the_epic_is_closed_by_its_slices_rather_than_by_a_flag() {
    let packet = packet::load();

    let outstanding: Vec<&str> = packet
        .slices
        .iter()
        .filter(|slice| slice.status != Status::Evidenced)
        .map(|slice| slice.id.as_str())
        .collect();

    assert_eq!(
        packet.closure.satisfied,
        outstanding.is_empty(),
        "closure.satisfied disagrees with the slices; still outstanding: {outstanding:?}"
    );
    assert_eq!(packet.closure.issue, EPIC_ISSUE);
    assert!(
        !packet.closure.requirement.is_empty(),
        "closure without a requirement is closure by assertion"
    );
}

/// The prose page and the packet describe the same thing. The page is what an
/// operator reads, so a slice missing from it is a slice they will not know is
/// outstanding.
#[test]
fn the_packet_and_its_prose_agree() {
    let packet = packet::load();
    let contract = packet::contract_text();

    for slice in &packet.slices {
        let id = slice.id.as_str();
        assert!(
            contract.contains(id),
            "{} never mentions the {id} slice",
            packet::CONTRACT_RELATIVE
        );
        assert!(
            contract.contains(&format!("#{}", slice.issue)),
            "{} never cites #{}, the issue that owns {id}",
            packet::CONTRACT_RELATIVE,
            slice.issue
        );
        assert!(
            contract.contains(slice.status.as_str()),
            "{} never states that {id} is {}",
            packet::CONTRACT_RELATIVE,
            slice.status.as_str()
        );
    }

    assert!(
        contract.contains(packet::MANIFEST_RELATIVE),
        "{} does not point at the packet it states",
        packet::CONTRACT_RELATIVE
    );
}
