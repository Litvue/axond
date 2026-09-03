//! The production qualification packet (axond #156), checked against the tree
//! it describes.
//!
//! ADR 0063 retired the tier / mode matrix. Recovery, rollout, and stateful
//! endurance are gone. This suite fails when the packet names a file that is
//! not there, when a slice claims a rung its own fields do not reach, when
//! retained evidence is not reproducible from the manifest it claims to have
//! run, when a scenario the packet still lists belongs to no slice, and when
//! the packet and its prose page disagree.

mod support;

use std::collections::{BTreeMap, BTreeSet};

use figment::Figment;
use figment::providers::{Format, Toml};
use support::capacity::manifest::RESULT_SCHEMA_VERSION as CAPACITY_RESULT_SCHEMA_VERSION;
use support::endurance::manifest::RESULT_SCHEMA_VERSION as ENDURANCE_RESULT_SCHEMA_VERSION;
use support::fault::manifest::RESULT_SCHEMA_VERSION as FAULT_RESULT_SCHEMA_VERSION;
use support::packet::{
    self, EPIC_ISSUE, PENDING_SOURCE_COMMIT, QUALIFICATION_CANDIDATE_VERSION, Runner, Scenario,
    SliceId, Status,
};

fn validate_observation_artifact_schema(
    slice_id: SliceId,
    observation: &packet::RecordObservation,
) -> Result<(), String> {
    let shared_ledger_claims = [
        (
            observation.request_identities_sha256.as_deref(),
            observation.request_identities_files,
            observation.request_identities_bytes,
        ),
        (
            observation.correlations_sha256.as_deref(),
            observation.correlations_files,
            observation.correlations_bytes,
        ),
    ];
    let complete_claim = |(digest, files, bytes): (Option<&str>, Option<u32>, Option<u64>)| {
        digest.is_some_and(|value| {
            value.len() == 64
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        }) && files.is_some_and(|count| count > 0)
            && bytes.is_some_and(|count| count > 0)
    };
    let has_any_shared_claim = shared_ledger_claims
        .iter()
        .any(|(digest, files, bytes)| digest.is_some() || files.is_some() || bytes.is_some());
    let sample_claim = (
        observation.samples_sha256.as_deref(),
        observation.samples_files,
        observation.samples_bytes,
    );
    let has_any_sample_claim =
        sample_claim.0.is_some() || sample_claim.1.is_some() || sample_claim.2.is_some();
    if slice_id == SliceId::Endurance {
        if !shared_ledger_claims.iter().copied().all(&complete_claim) {
            return Err(
                "endurance observations require complete request-identity and correlation digest, file, and byte claims"
                    .to_owned(),
            );
        }
        if !complete_claim(sample_claim) || observation.samples_files != Some(1) {
            return Err("endurance requires exactly one sample JSONL".to_owned());
        }
    } else if has_any_shared_claim || has_any_sample_claim {
        return Err(format!(
            "{} observations must not declare endurance ledger or sample claims",
            slice_id.as_str()
        ));
    }
    match (slice_id, observation.artifact_schema_version) {
        (SliceId::Endurance, Some(version)) if version == ENDURANCE_RESULT_SCHEMA_VERSION => Ok(()),
        (SliceId::Endurance, actual) => Err(format!(
            "endurance observations require artifact schema version \
             {ENDURANCE_RESULT_SCHEMA_VERSION}, found {actual:?}"
        )),
        (SliceId::Fault, Some(version)) if version == FAULT_RESULT_SCHEMA_VERSION => Ok(()),
        (SliceId::Fault, actual) => Err(format!(
            "fault observations require artifact schema version \
             {FAULT_RESULT_SCHEMA_VERSION}, found {actual:?}"
        )),
        (_, None) => Ok(()),
        (_, Some(version)) => Err(format!(
            "{} observations must not declare artifact schema version {version}",
            slice_id.as_str()
        )),
    }
}

fn generated_observation_fixture(
    artifact_schema_version: Option<u32>,
) -> packet::RecordObservation {
    let artifact_schema_version = artifact_schema_version
        .map(|version| format!("artifact_schema_version = {version}"))
        .unwrap_or_default();
    let fixture = format!(
        r#"
id = "mixed-endurance"
{artifact_schema_version}
artifact_sha256 = "39a7e072cf523642753e09f23db6f29c67f0b78e455efa951c1722d110dfd5d5"
elapsed_ms = 15100
verdicts = 14
passed = true
duration_ms = 15000
manifest_duration_ms = 15000
requested_duration_ms = 15000
duration_source = "manifest"
"#
    );

    Figment::from(Toml::string(&fixture))
        .extract()
        .expect("the generated compact observation fixture should load")
}

fn generated_endurance_observation_fixture(
    artifact_schema_version: Option<u32>,
) -> packet::RecordObservation {
    let mut observation = generated_observation_fixture(artifact_schema_version);
    observation.request_identities_sha256 = Some("a".repeat(64));
    observation.request_identities_files = Some(32);
    observation.request_identities_bytes = Some(1024);
    observation.correlations_sha256 = Some("b".repeat(64));
    observation.correlations_files = Some(32);
    observation.correlations_bytes = Some(2048);
    observation.samples_sha256 = Some("c".repeat(64));
    observation.samples_files = Some(1);
    observation.samples_bytes = Some(4096);
    observation
}

#[test]
fn generated_endurance_observation_schema_field_loads() {
    let observation =
        generated_endurance_observation_fixture(Some(ENDURANCE_RESULT_SCHEMA_VERSION));

    assert_eq!(
        observation.artifact_schema_version,
        Some(ENDURANCE_RESULT_SCHEMA_VERSION)
    );
    assert!(validate_observation_artifact_schema(SliceId::Endurance, &observation).is_ok());
}

#[test]
fn endurance_observations_require_the_current_artifact_schema() {
    let missing = generated_endurance_observation_fixture(None);
    assert!(validate_observation_artifact_schema(SliceId::Endurance, &missing).is_err());

    let stale_version = ENDURANCE_RESULT_SCHEMA_VERSION
        .checked_sub(1)
        .expect("endurance artifact schemas start above zero");
    let stale = generated_endurance_observation_fixture(Some(stale_version));
    assert!(validate_observation_artifact_schema(SliceId::Endurance, &stale).is_err());
}

#[test]
fn endurance_requires_exact_shared_ledgers_and_one_sample_jsonl() {
    let current = generated_endurance_observation_fixture(Some(ENDURANCE_RESULT_SCHEMA_VERSION));
    assert!(validate_observation_artifact_schema(SliceId::Endurance, &current).is_ok());

    let mut missing_requests = current.clone();
    missing_requests.request_identities_sha256 = None;
    assert!(validate_observation_artifact_schema(SliceId::Endurance, &missing_requests).is_err());

    let mut empty_correlations = current.clone();
    empty_correlations.correlations_bytes = Some(0);
    assert!(validate_observation_artifact_schema(SliceId::Endurance, &empty_correlations).is_err());

    let mut missing_samples = current.clone();
    missing_samples.samples_sha256 = None;
    assert!(validate_observation_artifact_schema(SliceId::Endurance, &missing_samples).is_err());

    let mut empty_sample_set = current.clone();
    empty_sample_set.samples_files = Some(0);
    assert!(validate_observation_artifact_schema(SliceId::Endurance, &empty_sample_set).is_err());

    let mut multiple_samples = current;
    multiple_samples.samples_files = Some(2);
    assert!(validate_observation_artifact_schema(SliceId::Endurance, &multiple_samples).is_err());
}

#[test]
fn fault_observations_require_raw_schema_one() {
    let current = generated_observation_fixture(Some(FAULT_RESULT_SCHEMA_VERSION));
    assert!(validate_observation_artifact_schema(SliceId::Fault, &current).is_ok());

    let missing = generated_observation_fixture(None);
    assert!(validate_observation_artifact_schema(SliceId::Fault, &missing).is_err());

    let stale = generated_observation_fixture(Some(FAULT_RESULT_SCHEMA_VERSION + 1));
    assert!(validate_observation_artifact_schema(SliceId::Fault, &stale).is_err());
}

/// Every remaining request-path slice is committed exactly once, owned by a
/// child issue, and carries the question it exists to answer.
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
        "the packet committed a slice the remaining request-path set does not name: {ids:?}"
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

/// Every scenario the packet still lists is some slice's responsibility.
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

/// Every path the packet names exists.
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

/// The status ladder is derived, not asserted.
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
                let heavy = slice.heavy_tier.as_deref().unwrap_or_else(|| {
                    panic!("{id}: a slice with a driver names the tier that would evidence it")
                });
                assert!(
                    !slice
                        .retained
                        .iter()
                        .any(|relative| packet::load_record(relative).tier == heavy),
                    "{id}: it retains a {heavy}-tier run, so it is evidenced rather \
                     than harnessed"
                );
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
                let heavy = slice.heavy_tier.as_deref().unwrap_or_else(|| {
                    panic!("{id}: a slice with a driver names the tier that evidences it")
                });
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

#[test]
fn historical_debug_records_are_indexed_but_cannot_promote_a_slice() {
    let qualification = packet::load();
    let contract = packet::contract_text();

    let slice = qualification.slice(SliceId::Fault);
    assert_eq!(slice.historical.len(), 1);
    let historical = &slice.historical[0];
    assert!(contract.contains(historical));

    let record = packet::load_record(historical);
    assert_eq!(record.source.crate_version, "0.3.39");
    assert_eq!(record.binary.version, "0.3.39");
    assert_eq!(record.binary.cargo_profile, "debug");

    for slice in &qualification.slices {
        assert!(
            slice
                .historical
                .iter()
                .all(|relative| !slice.retained.contains(relative)),
            "{} lists one record as both active and historical",
            slice.id.as_str()
        );
    }
}

/// Every retained record has to be reproducible from the repository.
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
            .workloads()
            .map(|workload| workload.id.clone())
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

            if slice.id == SliceId::Capacity {
                assert!(
                    record.observations.is_empty(),
                    "{relative}: capacity records use profile rows, not generic observations"
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
                    assert_eq!(
                        profile.artifact_schema_version,
                        Some(CAPACITY_RESULT_SCHEMA_VERSION),
                        "{relative}: {id} does not bind the current raw capacity schema"
                    );
                    assert!(
                        profile.artifact_sha256.as_deref().is_some_and(|digest| {
                            digest.len() == 64
                                && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
                        }),
                        "{relative}: {id} does not bind its raw capacity artifact"
                    );
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
                    if let Some(tenants) = profile.tenants {
                        assert!(
                            tenants > 1,
                            "{relative}: {id} claims tenant isolation with {tenants} tenant"
                        );
                        assert_eq!(
                            (
                                profile.foreign_credential_uses,
                                profile.misattributed_usage_records
                            ),
                            (Some(0), Some(0)),
                            "{relative}: {id} retains a run where a credential or a \
                         charge crossed a namespace"
                        );
                    }
                    if let Some(bound) = profile.upstream_bound_ms {
                        assert_eq!(
                            profile.over_bound,
                            Some(0),
                            "{relative}: {id} retains a run that outlived the bound \
                         the replica declares"
                        );
                        assert!(
                            profile
                                .max_latency_ms
                                .is_some_and(|slowest| slowest >= bound as f64),
                            "{relative}: {id} declares a {bound} ms bound nothing in \
                         the run ever reached, so the bound was not exercised"
                        );
                    }
                    if let Some(ceiling) = profile.admission_max_in_flight {
                        assert!(
                            profile.rejected > 0,
                            "{relative}: {id} booted a ceiling of {ceiling} and shed \
                         nothing, so the ceiling was not exercised"
                        );
                    }
                    if profile.admission_max_in_flight.is_some()
                        || profile.upstream_bound_ms.is_some()
                    {
                        assert_eq!(
                            profile.served_after_load,
                            Some(true),
                            "{relative}: {id} pushed the replica to a limit and never \
                         checked it could still serve afterwards"
                        );
                    }
                }
            } else {
                assert!(
                    record.profiles.is_empty(),
                    "{relative}: non-capacity records use observation rows, not capacity profiles"
                );
                let recorded: BTreeSet<String> = record
                    .observations
                    .iter()
                    .map(|observation| observation.id.clone())
                    .collect();
                assert_eq!(
                    recorded, profiles,
                    "{relative}: the record and the manifest disagree about the workloads"
                );
                for observation in &record.observations {
                    validate_observation_artifact_schema(slice.id, observation)
                        .unwrap_or_else(|error| panic!("{relative}: {}: {error}", observation.id));
                    assert!(
                        !observation.artifact_sha256.is_empty(),
                        "{relative}: {} has no raw artifact digest",
                        observation.id
                    );
                    assert!(
                        observation.elapsed_ms > 0 && observation.verdicts > 0,
                        "{relative}: {} records no run duration or verdicts",
                        observation.id
                    );
                    assert!(
                        observation.passed,
                        "{relative}: {} is retained as evidence of a failed workload",
                        observation.id
                    );
                    if slice.id == SliceId::Endurance {
                        let workload = manifest
                            .profiles
                            .iter()
                            .find(|workload| workload.id == observation.id)
                            .unwrap_or_else(|| {
                                panic!(
                                    "{relative}: {} is not in the endurance manifest",
                                    observation.id
                                )
                            });
                        let required_duration = match record.tier.as_str() {
                            "smoke" => workload.smoke.as_ref().map(|tier| tier.duration_ms),
                            "soak" => workload.soak.as_ref().map(|tier| tier.duration_ms),
                            other => panic!(
                                "{relative}: {} retains unknown endurance tier {other}",
                                observation.id
                            ),
                        }
                        .unwrap_or_else(|| {
                            panic!(
                                "{relative}: {} has no committed {} duration",
                                observation.id, record.tier
                            )
                        });
                        assert_eq!(
                            observation.manifest_duration_ms,
                            Some(required_duration),
                            "{relative}: {} does not retain the committed {} duration",
                            observation.id,
                            record.tier
                        );
                        assert!(
                            observation
                                .duration_ms
                                .is_some_and(|duration| duration >= required_duration),
                            "{relative}: {} offered less than the committed {} ms {}",
                            observation.id,
                            required_duration,
                            record.tier
                        );
                        assert!(
                            observation.requested_duration_ms.is_some()
                                && observation
                                    .duration_source
                                    .as_deref()
                                    .is_some_and(|source| !source.is_empty()),
                            "{relative}: {} does not retain how the {} duration was selected",
                            observation.id,
                            record.tier
                        );
                    }
                }
            }
        }
    }

    assert_eq!(
        checked,
        packet
            .slices
            .iter()
            .map(|slice| slice.retained.len())
            .sum::<usize>(),
        "every active retained record must pass reproducibility validation"
    );
}

/// A record off a contributor's machine is evidence about that machine.
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
                let identity = &record.binary.sha256[..12];
                assert!(
                    contract.contains(identity),
                    "{relative}: it was produced by binary {identity}, which {} \
                     does not name \u{2014} the disclosure has drifted from the record",
                    packet::CONTRACT_RELATIVE
                );
            }
        }
    }
}

fn qualification_closure_errors<F>(
    qualification: &packet::Packet,
    mut load_record: F,
) -> Vec<String>
where
    F: FnMut(&str) -> packet::Record,
{
    let mut errors = Vec::new();
    let cohort = &qualification.cohort;

    if cohort.id.trim().is_empty() {
        errors.push("the qualification cohort has no id".to_owned());
    }
    if cohort.candidate_version != QUALIFICATION_CANDIDATE_VERSION {
        errors.push(format!(
            "the qualification cohort candidate is {}, expected {QUALIFICATION_CANDIDATE_VERSION}",
            cohort.candidate_version
        ));
    }
    if cohort.source_commit == PENDING_SOURCE_COMMIT {
        errors.push("the qualification cohort source commit is still pending".to_owned());
    } else if !packet::is_exact_git_commit(&cohort.source_commit) {
        errors.push("the qualification cohort source is not an exact Git commit".to_owned());
    }

    for slice in &qualification.slices {
        let id = slice.id.as_str();
        if slice.status != Status::Evidenced {
            errors.push(format!("{id} is not evidenced"));
        }
        let Some(heavy_tier) = slice.heavy_tier.as_deref() else {
            errors.push(format!("{id} has no heavy tier"));
            continue;
        };
        let heavy_records: Vec<(&str, packet::Record)> = slice
            .retained
            .iter()
            .map(|relative| (relative.as_str(), load_record(relative)))
            .filter(|(_, record)| record.tier == heavy_tier)
            .collect();
        if heavy_records.len() != 1 {
            errors.push(format!(
                "{id} retains {} {heavy_tier}-tier records; closure requires exactly one",
                heavy_records.len()
            ));
            continue;
        }

        let (relative, record) = &heavy_records[0];
        if record.slice_id != slice.id {
            errors.push(format!("{relative} belongs to another slice"));
        }
        if record.binary.cargo_profile != "release" {
            errors.push(format!("{relative} is not a release-profile record"));
        }
        if record.source.git_dirty {
            errors.push(format!("{relative} was produced from a dirty tree"));
        }
        if record.source.git_commit != cohort.source_commit {
            errors.push(format!("{relative} is outside the frozen source cohort"));
        }
        if record.source.crate_version != QUALIFICATION_CANDIDATE_VERSION
            || record.binary.version != QUALIFICATION_CANDIDATE_VERSION
        {
            errors.push(format!(
                "{relative} is not candidate {QUALIFICATION_CANDIDATE_VERSION} evidence"
            ));
        }

        if slice.id == SliceId::Endurance {
            if record.observations.is_empty() {
                errors.push(format!("{relative} has no endurance observations"));
            }
            for observation in &record.observations {
                if let Err(error) = validate_observation_artifact_schema(slice.id, observation) {
                    errors.push(format!("{relative}: {}: {error}", observation.id));
                }
            }
        } else if slice.id == SliceId::Fault {
            if record.observations.is_empty() {
                errors.push(format!("{relative} has no fault observations"));
            }
            for observation in &record.observations {
                if let Err(error) =
                    validate_observation_artifact_schema(SliceId::Fault, observation)
                {
                    errors.push(format!("{relative}: {}: {error}", observation.id));
                }
            }
        }
    }

    errors
}

fn synthetic_closed_packet() -> (packet::Packet, BTreeMap<String, packet::Record>) {
    let mut qualification = packet::load();
    qualification.closure.satisfied = true;
    qualification.cohort.source_commit = "d".repeat(40);

    let mut base = packet::load_record("qualification/faults/evidence/full-ci.toml");
    base.source.git_commit = qualification.cohort.source_commit.clone();
    base.source.git_dirty = false;
    base.source.crate_version = QUALIFICATION_CANDIDATE_VERSION.to_owned();
    base.binary.version = QUALIFICATION_CANDIDATE_VERSION.to_owned();
    base.binary.cargo_profile = "release".to_owned();

    let mut records = BTreeMap::new();
    for slice in &mut qualification.slices {
        let relative = format!("synthetic/{}.toml", slice.id.as_str());
        slice.status = Status::Evidenced;
        slice.outstanding = None;
        slice.retained = vec![relative.clone()];

        let mut record = base.clone();
        record.slice_id = slice.id;
        record.tier = slice
            .heavy_tier
            .clone()
            .expect("every qualification slice has a heavy tier");
        record.profiles.clear();
        record.observations.clear();
        record.stages.clear();
        match slice.id {
            SliceId::Endurance => {
                record
                    .observations
                    .push(generated_endurance_observation_fixture(Some(
                        ENDURANCE_RESULT_SCHEMA_VERSION,
                    )))
            }
            SliceId::Fault => record.observations.push(generated_observation_fixture(Some(
                FAULT_RESULT_SCHEMA_VERSION,
            ))),
            SliceId::Capacity => {}
        }
        records.insert(relative, record);
    }

    (qualification, records)
}

fn synthetic_closure_errors(
    qualification: &packet::Packet,
    records: &BTreeMap<String, packet::Record>,
) -> Vec<String> {
    qualification_closure_errors(qualification, |relative| {
        records
            .get(relative)
            .unwrap_or_else(|| panic!("missing synthetic record {relative}"))
            .clone()
    })
}

#[test]
fn one_clean_release_record_per_slice_from_the_frozen_cohort_can_close() {
    let (qualification, records) = synthetic_closed_packet();
    assert!(synthetic_closure_errors(&qualification, &records).is_empty());
}

#[test]
fn pending_or_mixed_candidate_provenance_cannot_close() {
    let (mut qualification, records) = synthetic_closed_packet();
    qualification.cohort.source_commit = PENDING_SOURCE_COMMIT.to_owned();
    assert!(!synthetic_closure_errors(&qualification, &records).is_empty());

    let (qualification, mut dirty_records) = synthetic_closed_packet();
    dirty_records
        .get_mut("synthetic/fault.toml")
        .expect("fault fixture")
        .source
        .git_dirty = true;
    assert!(!synthetic_closure_errors(&qualification, &dirty_records).is_empty());
}

#[test]
fn closure_requires_exactly_one_heavy_record_for_each_slice() {
    let (mut qualification, records) = synthetic_closed_packet();
    let capacity = qualification
        .slices
        .iter_mut()
        .find(|slice| slice.id == SliceId::Capacity)
        .expect("capacity slice");
    capacity.retained.clear();
    assert!(!synthetic_closure_errors(&qualification, &records).is_empty());
}

#[test]
fn endurance_ledger_and_sample_claims_are_closure_requirements() {
    let (qualification, records) = synthetic_closed_packet();
    let mutations: [fn(&mut packet::RecordObservation); 3] = [
        |observation: &mut packet::RecordObservation| {
            observation.request_identities_files = None;
        },
        |observation: &mut packet::RecordObservation| {
            observation.correlations_sha256 = None;
        },
        |observation: &mut packet::RecordObservation| {
            observation.samples_bytes = Some(0);
        },
    ];

    for mutate in mutations {
        let mut mutated = records.clone();
        let observation = mutated
            .get_mut("synthetic/endurance.toml")
            .expect("endurance fixture")
            .observations
            .first_mut()
            .expect("endurance observation");
        mutate(observation);
        assert!(!synthetic_closure_errors(&qualification, &mutated).is_empty());
    }
}

/// Closure is derived from the slices. The flag exists so the packet states its
/// own conclusion in one place, and this is what stops that conclusion from
/// being an opinion.
#[test]
fn the_epic_is_closed_by_its_slices_rather_than_by_a_flag() {
    let packet = packet::load();
    let errors = qualification_closure_errors(&packet, packet::load_record);

    assert_eq!(
        packet.closure.satisfied,
        errors.is_empty(),
        "closure.satisfied disagrees with the v0.4.0 cohort gate: {errors:?}"
    );
    assert_eq!(packet.closure.issue, EPIC_ISSUE);
    assert!(
        !packet.closure.requirement.is_empty(),
        "closure without a requirement is closure by assertion"
    );
}

/// Optional audit: set AXOND_REQUIRE_QUALIFICATION_CLOSURE=1 on a 0.4.0
/// workspace to assert packet closure. The release workflow does not set this;
/// production is not held on a frozen cohort.
#[test]
fn v0_4_0_release_candidate_requires_closed_production_qualification() {
    if std::env::var("AXOND_REQUIRE_QUALIFICATION_CLOSURE").as_deref() != Ok("1")
        || env!("CARGO_PKG_VERSION") != QUALIFICATION_CANDIDATE_VERSION
    {
        return;
    }
    let packet = packet::load();
    let errors = qualification_closure_errors(&packet, packet::load_record);
    assert!(
        packet.closure.satisfied && errors.is_empty(),
        "v0.4.0 publication requires a closed production qualification cohort: {errors:?}"
    );
}

/// The prose page and the packet describe the same thing.
#[test]
fn the_packet_and_its_prose_agree() {
    let packet = packet::load();
    let contract = packet::contract_text();

    for slice in &packet.slices {
        let id = slice.id.as_str();
        let row = contract
            .lines()
            .find(|line| line.starts_with(&format!("| `{id}` |")))
            .unwrap_or_else(|| {
                panic!(
                    "{} has no row for the {id} slice",
                    packet::CONTRACT_RELATIVE
                )
            });
        assert!(
            row.contains(&format!("#{}", slice.issue)),
            "{}: the {id} row does not cite #{}, the issue that owns it",
            packet::CONTRACT_RELATIVE,
            slice.issue
        );
        assert!(
            row.contains(&format!("`{}`", slice.status.as_str())),
            "{}: the {id} row does not state that it is {} \u{2014} the page and the \
             packet disagree about how far it got",
            packet::CONTRACT_RELATIVE,
            slice.status.as_str()
        );
    }

    assert!(
        contract.contains(packet::MANIFEST_RELATIVE),
        "{} does not point at the packet it states",
        packet::CONTRACT_RELATIVE
    );
    assert!(
        contract.contains(&packet.cohort.id)
            && contract.contains(&packet.cohort.candidate_version)
            && contract.contains(&format!(
                "`source_commit` is truthfully `{}`",
                packet.cohort.source_commit
            )),
        "{} does not state the packet's candidate cohort and source state",
        packet::CONTRACT_RELATIVE
    );
}

/// Cross-slice dependency removals stay reviewable in both the data file and
/// the operator-facing page rather than disappearing as an untested deletion.
#[test]
fn retired_cross_slice_dependencies_are_recorded() {
    let packet = packet::load();
    let packet_text = std::fs::read_to_string(packet::manifest_path())
        .expect("qualification packet should be readable as text");
    let contract = packet::contract_text();

    assert!(
        packet_text.contains("retired the tier / mode matrix"),
        "the packet must explain why recovery, rollout, and stateful-endurance are gone"
    );
    for slice in &packet.slices {
        assert!(
            !slice.blocked_on.contains(&158),
            "{} must not reacquire retired dependency #158",
            slice.id.as_str()
        );
    }
    assert!(
        contract.contains("## Retired harnesses") && contract.contains("ADR 0063"),
        "the qualification page must record that the tier-matrix harnesses are gone"
    );
}
