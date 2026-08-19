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

use std::collections::{BTreeMap, BTreeSet};

use figment::Figment;
use figment::providers::{Format, Toml};
use support::capacity::manifest::RESULT_SCHEMA_VERSION as CAPACITY_RESULT_SCHEMA_VERSION;
use support::endurance::manifest::RESULT_SCHEMA_VERSION as ENDURANCE_RESULT_SCHEMA_VERSION;
use support::fault::manifest::RESULT_SCHEMA_VERSION as FAULT_RESULT_SCHEMA_VERSION;
use support::packet::{
    self, EPIC_ISSUE, PENDING_SOURCE_COMMIT, QUALIFICATION_CANDIDATE_VERSION,
    ROLLOUT_PREVIOUS_VERSION, Runner, Scenario, SliceId, Status,
};
use support::recovery;
use support::rollout::manifest::RESULT_SCHEMA_VERSION as ROLLOUT_RESULT_SCHEMA_VERSION;
use support::stateful_endurance::manifest::RESULT_SCHEMA_VERSION as STATEFUL_ENDURANCE_RESULT_SCHEMA_VERSION;

const RECOVERY_RESULT_SCHEMA_VERSION: u32 = 2;

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
    let stateful_ledger_claims = [
        (
            observation.correlation_windows_sha256.as_deref(),
            observation.correlation_windows_files,
            observation.correlation_windows_bytes,
        ),
        (
            observation.durable_identities_sha256.as_deref(),
            observation.durable_identities_files,
            observation.durable_identities_bytes,
        ),
        (
            observation.durable_outside_identities_sha256.as_deref(),
            observation.durable_outside_identities_files,
            observation.durable_outside_identities_bytes,
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
    let has_any_stateful_claim = stateful_ledger_claims
        .iter()
        .any(|(digest, files, bytes)| digest.is_some() || files.is_some() || bytes.is_some());
    let sample_claim = (
        observation.samples_sha256.as_deref(),
        observation.samples_files,
        observation.samples_bytes,
    );
    let has_any_sample_claim =
        sample_claim.0.is_some() || sample_claim.1.is_some() || sample_claim.2.is_some();
    if matches!(slice_id, SliceId::Endurance | SliceId::StatefulEndurance) {
        if !shared_ledger_claims.iter().copied().all(&complete_claim) {
            return Err(
                "endurance observations require complete request-identity and correlation digest, file, and byte claims"
                    .to_owned(),
            );
        }
        if !complete_claim(sample_claim)
            || (slice_id == SliceId::Endurance && observation.samples_files != Some(1))
        {
            return Err(
                "stateless endurance requires one sample JSONL; stateful endurance requires a non-empty per-incarnation sample set"
                    .to_owned(),
            );
        }
        if slice_id == SliceId::StatefulEndurance
            && (!stateful_ledger_claims.iter().copied().all(complete_claim)
                || observation.request_identities_files != Some(64)
                || observation.correlations_files != Some(128)
                || observation.correlation_windows_files != Some(64)
                || observation.durable_identities_files != Some(128)
                || observation.durable_outside_identities_files != Some(128))
        {
            return Err(
                "stateful endurance observations require all five fixed-width exact-ledger shard sets"
                    .to_owned(),
            );
        }
    } else if has_any_shared_claim || has_any_stateful_claim || has_any_sample_claim {
        return Err(format!(
            "{} observations must not declare endurance ledger or sample claims",
            slice_id.as_str()
        ));
    }
    let rollout_digests = [
        observation.rollout_previous_binary_sha256.as_deref(),
        observation.rollout_candidate_binary_sha256.as_deref(),
        observation.rollout_retained_archive_sha256.as_deref(),
    ];
    let has_any_rollout_claim = observation.rollout_previous_version.is_some()
        || observation.rollout_candidate_version.is_some()
        || rollout_digests.iter().any(|value| value.is_some())
        || observation.rollout_shared_stateful_revision.is_some()
        || observation.rollout_shared_alias.is_some()
        || observation.rollout_previous_serves_shared_alias.is_some()
        || observation.rollout_candidate_serves_shared_alias.is_some()
        || observation.rollout_usage_reconciliation.is_some()
        || observation.rollout_exact_trace_replicas.is_some()
        || observation.rollout_retained_trace_context.is_some()
        || observation.rollout_otlp_trace_exports.is_some()
        || observation.rollout_otlp_trace_export_replicas.is_some()
        || observation.rollout_otlp_trace_identities.is_some()
        || observation.rollout_otlp_trace_identities_sha256.is_some();
    if slice_id == SliceId::Rollout {
        let versions_complete = observation.rollout_previous_version.as_deref()
            == Some(ROLLOUT_PREVIOUS_VERSION)
            && observation.rollout_candidate_version.as_deref()
                == Some(QUALIFICATION_CANDIDATE_VERSION);
        let digests_complete = rollout_digests.iter().all(|value| {
            value.is_some_and(|value| {
                value.len() == 64
                    && value
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            })
        });
        let identities_distinct = observation.rollout_previous_version
            != observation.rollout_candidate_version
            && observation.rollout_previous_binary_sha256
                != observation.rollout_candidate_binary_sha256;
        let shared_stateful_serving = observation
            .rollout_shared_stateful_revision
            .as_deref()
            .is_some_and(|revision| !revision.trim().is_empty())
            && observation.rollout_shared_alias.as_deref() == Some("chat")
            && observation.rollout_previous_serves_shared_alias == Some(true)
            && observation.rollout_candidate_serves_shared_alias == Some(true);
        let usage_reconciliation_disclosed = observation.rollout_usage_reconciliation.as_deref()
            == Some("exact_trace")
            && observation
                .rollout_exact_trace_replicas
                .is_some_and(|count| count > 0)
            && observation.rollout_retained_trace_context.as_deref() == Some("loopback_otlp_http")
            && observation
                .rollout_otlp_trace_exports
                .zip(observation.rollout_exact_trace_replicas)
                .is_some_and(|(exports, replicas)| exports >= u64::from(replicas))
            && observation.rollout_otlp_trace_export_replicas
                == observation.rollout_exact_trace_replicas
            && observation
                .rollout_otlp_trace_identities
                .is_some_and(|count| count > 0)
            && observation
                .rollout_otlp_trace_identities_sha256
                .as_deref()
                .is_some_and(|digest| {
                    digest.len() == 64
                        && digest
                            .bytes()
                            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                });
        if !versions_complete
            || !digests_complete
            || !identities_distinct
            || !shared_stateful_serving
            || !usage_reconciliation_disclosed
        {
            return Err(
                "rollout observations require v0.3.40/v0.4.0 executable identities, both fleets serving the shared durable `chat` alias, and exact-trace reconciliation with an OTLP witness"
                    .to_owned(),
            );
        }
    } else if has_any_rollout_claim {
        return Err(format!(
            "{} observations must not declare rollout executable claims",
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
        (SliceId::Rollout, Some(version)) if version == ROLLOUT_RESULT_SCHEMA_VERSION => Ok(()),
        (SliceId::Rollout, actual) => Err(format!(
            "rollout observations require artifact schema version \
             {ROLLOUT_RESULT_SCHEMA_VERSION}, found {actual:?}"
        )),
        (SliceId::StatefulEndurance, Some(version))
            if version == STATEFUL_ENDURANCE_RESULT_SCHEMA_VERSION =>
        {
            Ok(())
        }
        (SliceId::StatefulEndurance, actual) => Err(format!(
            "stateful endurance observations require artifact schema version \
             {STATEFUL_ENDURANCE_RESULT_SCHEMA_VERSION}, found {actual:?}"
        )),
        (_, None) => Ok(()),
        (_, Some(version)) => Err(format!(
            "{} observations must not declare artifact schema version {version}",
            slice_id.as_str()
        )),
    }
}

fn validate_recovery_stage(
    stage: &packet::RecordStage,
    record_binary_sha256: &str,
) -> Result<(), String> {
    if stage.artifact_schema_version != Some(RECOVERY_RESULT_SCHEMA_VERSION) {
        return Err(format!(
            "recovery stages require artifact schema version {RECOVERY_RESULT_SCHEMA_VERSION}, found {:?}",
            stage.artifact_schema_version
        ));
    }
    let stage_binary = stage
        .binary_sha256
        .as_deref()
        .filter(|digest| {
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
        .ok_or_else(|| "recovery stages require an exact executable digest".to_owned())?;
    if stage_binary != record_binary_sha256 {
        return Err("the recovery stage executable differs from the record binary".to_owned());
    }
    match stage.driver.as_deref() {
        Some("stateful-integration") => {
            let executed = stage
                .executed_binary_sha256
                .as_deref()
                .filter(|digest| {
                    digest.len() == 64
                        && digest
                            .bytes()
                            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                })
                .ok_or_else(|| {
                    "process-backed recovery stages require the executed binary digest".to_owned()
                })?;
            if executed != record_binary_sha256 || stage.execution_bound != Some(true) {
                return Err(
                    "the process-backed recovery stage is not bound to the record binary"
                        .to_owned(),
                );
            }
        }
        Some("restore-drill") => {
            if stage.executed_binary_sha256.is_some() || stage.execution_bound.is_some() {
                return Err(
                    "restore-drill stages must not claim process-driver provenance".to_owned(),
                );
            }
        }
        other => {
            return Err(format!(
                "recovery stages require a current manifest driver, found {other:?}"
            ));
        }
    }
    Ok(())
}

fn recovery_driver_name(driver: recovery::Driver) -> &'static str {
    match driver {
        recovery::Driver::Qualification => "qualification",
        recovery::Driver::StatefulIntegration => "stateful-integration",
        recovery::Driver::RestoreDrill => "restore-drill",
    }
}

fn expected_recovery_stages() -> BTreeMap<String, (String, String)> {
    recovery::load()
        .scenarios
        .into_iter()
        .flat_map(|scenario| {
            scenario
                .stages
                .into_iter()
                .filter(|stage| stage.status == recovery::Status::Executable)
                .map(move |stage| {
                    let runner = stage
                        .runner
                        .expect("an executable recovery stage must name its runner");
                    (
                        format!("{}/{}", scenario.id, stage.id),
                        (
                            runner.as_str().to_owned(),
                            recovery_driver_name(stage.driver).to_owned(),
                        ),
                    )
                })
        })
        .collect()
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
elapsed_ms = 43200351
verdicts = 14
passed = true
duration_ms = 43200000
manifest_duration_ms = 43200000
requested_duration_ms = 43200000
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

fn generated_stateful_observation_fixture(
    artifact_schema_version: Option<u32>,
) -> packet::RecordObservation {
    let artifact_schema_version = artifact_schema_version
        .map(|version| format!("artifact_schema_version = {version}"))
        .unwrap_or_default();
    let fixture = format!(
        r#"
id = "mixed-stateful-endurance"
{artifact_schema_version}
artifact_sha256 = "39a7e072cf523642753e09f23db6f29c67f0b78e455efa951c1722d110dfd5d5"
elapsed_ms = 43200351
verdicts = 14
passed = true
duration_ms = 43200000
manifest_duration_ms = 43200000
requested_duration_ms = 43200000
duration_source = "manifest"
request_identities_sha256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
request_identities_files = 64
request_identities_bytes = 1024
correlations_sha256 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
correlations_files = 128
correlations_bytes = 2048
correlation_windows_sha256 = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
correlation_windows_files = 64
correlation_windows_bytes = 2112
samples_sha256 = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
samples_files = 6
samples_bytes = 4096
durable_identities_sha256 = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
durable_identities_files = 128
durable_identities_bytes = 2048
durable_outside_identities_sha256 = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
durable_outside_identities_files = 128
durable_outside_identities_bytes = 2048
"#
    );

    Figment::from(Toml::string(&fixture))
        .extract()
        .expect("the generated compact stateful observation fixture should load")
}

fn generated_rollout_observation_fixture(
    artifact_schema_version: Option<u32>,
) -> packet::RecordObservation {
    let mut observation = generated_observation_fixture(artifact_schema_version);
    observation.rollout_previous_version = Some("0.3.40".to_owned());
    observation.rollout_previous_binary_sha256 = Some("a".repeat(64));
    observation.rollout_candidate_version = Some("0.4.0".to_owned());
    observation.rollout_candidate_binary_sha256 = Some("b".repeat(64));
    observation.rollout_retained_archive_sha256 = Some("c".repeat(64));
    observation.rollout_shared_stateful_revision = Some("revision-v040".to_owned());
    observation.rollout_shared_alias = Some("chat".to_owned());
    observation.rollout_previous_serves_shared_alias = Some(true);
    observation.rollout_candidate_serves_shared_alias = Some(true);
    observation.rollout_usage_reconciliation = Some("exact_trace".to_owned());
    observation.rollout_exact_trace_replicas = Some(3);
    observation.rollout_retained_trace_context = Some("loopback_otlp_http".to_owned());
    observation.rollout_otlp_trace_exports = Some(3);
    observation.rollout_otlp_trace_export_replicas = Some(3);
    observation.rollout_otlp_trace_identities = Some(10);
    observation.rollout_otlp_trace_identities_sha256 = Some("a".repeat(64));
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
fn both_endurance_slices_require_exact_shared_ledgers_and_valid_sample_sets() {
    for slice_id in [SliceId::Endurance, SliceId::StatefulEndurance] {
        let current = if slice_id == SliceId::Endurance {
            generated_endurance_observation_fixture(Some(ENDURANCE_RESULT_SCHEMA_VERSION))
        } else {
            generated_stateful_observation_fixture(Some(STATEFUL_ENDURANCE_RESULT_SCHEMA_VERSION))
        };
        assert!(validate_observation_artifact_schema(slice_id, &current).is_ok());

        let mut missing_requests = current.clone();
        missing_requests.request_identities_sha256 = None;
        assert!(validate_observation_artifact_schema(slice_id, &missing_requests).is_err());

        let mut empty_correlations = current.clone();
        empty_correlations.correlations_bytes = Some(0);
        assert!(validate_observation_artifact_schema(slice_id, &empty_correlations).is_err());

        let mut missing_samples = current.clone();
        missing_samples.samples_sha256 = None;
        assert!(validate_observation_artifact_schema(slice_id, &missing_samples).is_err());

        let mut empty_sample_set = current;
        empty_sample_set.samples_files = Some(0);
        assert!(validate_observation_artifact_schema(slice_id, &empty_sample_set).is_err());
    }

    let mut stateless_multiple_samples =
        generated_endurance_observation_fixture(Some(ENDURANCE_RESULT_SCHEMA_VERSION));
    stateless_multiple_samples.samples_files = Some(2);
    assert!(
        validate_observation_artifact_schema(SliceId::Endurance, &stateless_multiple_samples)
            .is_err()
    );

    let mut stateful_multiple_samples =
        generated_stateful_observation_fixture(Some(STATEFUL_ENDURANCE_RESULT_SCHEMA_VERSION));
    stateful_multiple_samples.samples_files = Some(8);
    assert!(
        validate_observation_artifact_schema(
            SliceId::StatefulEndurance,
            &stateful_multiple_samples,
        )
        .is_ok()
    );
}

#[test]
fn rollout_observations_require_the_current_artifact_schema() {
    let current = generated_rollout_observation_fixture(Some(ROLLOUT_RESULT_SCHEMA_VERSION));
    assert!(validate_observation_artifact_schema(SliceId::Rollout, &current).is_ok());

    let missing = generated_observation_fixture(None);
    assert!(validate_observation_artifact_schema(SliceId::Rollout, &missing).is_err());

    let stale = generated_rollout_observation_fixture(Some(ROLLOUT_RESULT_SCHEMA_VERSION - 1));
    assert!(validate_observation_artifact_schema(SliceId::Rollout, &stale).is_err());

    let mut missing_identity = current;
    missing_identity.rollout_previous_binary_sha256 = None;
    assert!(validate_observation_artifact_schema(SliceId::Rollout, &missing_identity).is_err());
}

#[test]
fn rollout_raw_and_compact_contracts_are_both_schema_four() {
    assert_eq!(ROLLOUT_RESULT_SCHEMA_VERSION, 4);
    assert_eq!(packet::ROLLOUT_RECORD_SCHEMA_VERSION, 4);
}

#[test]
fn rollout_observations_require_complete_shared_stateful_serving_proof() {
    let current = generated_rollout_observation_fixture(Some(ROLLOUT_RESULT_SCHEMA_VERSION));

    let mut missing_revision = current.clone();
    missing_revision.rollout_shared_stateful_revision = None;
    assert!(validate_observation_artifact_schema(SliceId::Rollout, &missing_revision).is_err());

    let mut wrong_alias = current.clone();
    wrong_alias.rollout_shared_alias = Some("chat-next-only".to_owned());
    assert!(validate_observation_artifact_schema(SliceId::Rollout, &wrong_alias).is_err());

    let mut previous_did_not_serve = current.clone();
    previous_did_not_serve.rollout_previous_serves_shared_alias = Some(false);
    assert!(
        validate_observation_artifact_schema(SliceId::Rollout, &previous_did_not_serve).is_err()
    );

    let mut candidate_did_not_serve = current;
    candidate_did_not_serve.rollout_candidate_serves_shared_alias = None;
    assert!(
        validate_observation_artifact_schema(SliceId::Rollout, &candidate_did_not_serve).is_err()
    );
}

#[test]
fn stateful_endurance_observations_require_the_current_artifact_schema() {
    let current =
        generated_stateful_observation_fixture(Some(STATEFUL_ENDURANCE_RESULT_SCHEMA_VERSION));
    assert!(validate_observation_artifact_schema(SliceId::StatefulEndurance, &current).is_ok());

    let missing = generated_stateful_observation_fixture(None);
    assert!(validate_observation_artifact_schema(SliceId::StatefulEndurance, &missing).is_err());

    let missing_ledgers =
        generated_observation_fixture(Some(STATEFUL_ENDURANCE_RESULT_SCHEMA_VERSION));
    assert!(
        validate_observation_artifact_schema(SliceId::StatefulEndurance, &missing_ledgers).is_err()
    );

    let mut empty_ledger = current;
    empty_ledger.request_identities_bytes = Some(0);
    assert!(
        validate_observation_artifact_schema(SliceId::StatefulEndurance, &empty_ledger).is_err()
    );
}

#[test]
fn slices_without_versioned_raw_contracts_must_omit_the_artifact_schema() {
    let absent = generated_observation_fixture(None);
    assert!(validate_observation_artifact_schema(SliceId::Recovery, &absent).is_ok());

    let present = generated_observation_fixture(Some(ENDURANCE_RESULT_SCHEMA_VERSION));
    assert!(validate_observation_artifact_schema(SliceId::Recovery, &present).is_err());

    let leaked_endurance_claims = generated_endurance_observation_fixture(None);
    assert!(
        validate_observation_artifact_schema(SliceId::Recovery, &leaked_endurance_claims).is_err()
    );
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

#[test]
fn evidenced_recovery_stages_require_raw_schema_and_exact_binary_identity() {
    let binary_sha256 = "a".repeat(64);
    let mut stage = packet::load_record("qualification/recovery/evidence/serving-ci.toml")
        .stages
        .into_iter()
        .next()
        .expect("historical recovery fixture has a stage");
    stage.artifact_schema_version = Some(RECOVERY_RESULT_SCHEMA_VERSION);
    stage.binary_sha256 = Some(binary_sha256.clone());
    stage.driver = Some("restore-drill".to_owned());
    assert!(validate_recovery_stage(&stage, &binary_sha256).is_ok());

    let mut missing_schema = stage.clone();
    missing_schema.artifact_schema_version = None;
    assert!(validate_recovery_stage(&missing_schema, &binary_sha256).is_err());

    let mut stale_schema = stage.clone();
    stale_schema.artifact_schema_version = Some(RECOVERY_RESULT_SCHEMA_VERSION - 1);
    assert!(validate_recovery_stage(&stale_schema, &binary_sha256).is_err());

    let mut missing_binary = stage.clone();
    missing_binary.binary_sha256 = None;
    assert!(validate_recovery_stage(&missing_binary, &binary_sha256).is_err());

    let mut process = stage.clone();
    process.driver = Some("stateful-integration".to_owned());
    process.executed_binary_sha256 = Some(binary_sha256.clone());
    process.execution_bound = Some(true);
    assert!(validate_recovery_stage(&process, &binary_sha256).is_ok());

    let mut post_stamped = process.clone();
    post_stamped.executed_binary_sha256 = Some("b".repeat(64));
    assert!(validate_recovery_stage(&post_stamped, &binary_sha256).is_err());

    let mut unbound = process;
    unbound.execution_bound = Some(false);
    assert!(validate_recovery_stage(&unbound, &binary_sha256).is_err());

    stage.binary_sha256 = Some("b".repeat(64));
    assert!(validate_recovery_stage(&stage, &binary_sha256).is_err());
}

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

/// Fault and recovery records must come from the frozen branch head. Ordinary
/// pull-request CI intentionally checks GitHub's synthetic merge ref, so it is
/// a merge gate rather than promotable cohort provenance.
#[test]
fn fault_and_recovery_have_an_exact_head_dispatch_lane() {
    let packet = packet::load();
    for slice_id in [SliceId::Fault, SliceId::Recovery] {
        assert_eq!(
            packet.slice(slice_id).heavy_lane.as_deref(),
            Some(".github/workflows/ci.yml"),
            "{} must use the service-backed CI workflow",
            slice_id.as_str()
        );
    }

    let workflow =
        std::fs::read_to_string(packet::workspace_root().join(".github/workflows/ci.yml"))
            .expect("CI workflow should be readable");
    assert!(
        workflow.lines().any(|line| line == "  workflow_dispatch:"),
        "CI must be manually dispatchable from the frozen release branch"
    );

    let contract = packet::contract_text();
    assert!(
        contract.contains("manual `CI` workflow dispatch")
            && contract.contains("synthetic merge ref"),
        "the qualification runbook must distinguish exact-head evidence from PR merge-ref CI"
    );
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
                // the measurement the heavy tier exists to take, which is why
                // the tier that would promote the slice has to be named even
                // while no run of it exists.
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

    for id in [SliceId::Fault, SliceId::Recovery] {
        let slice = qualification.slice(id);
        assert_eq!(slice.status, Status::Harnessed);
        assert!(slice.retained.is_empty());
        assert_eq!(slice.historical.len(), 1);
        let historical = &slice.historical[0];
        assert!(contract.contains(historical));

        let record = packet::load_record(historical);
        assert_eq!(record.source.crate_version, "0.3.39");
        assert_eq!(record.binary.version, "0.3.39");
        assert_eq!(record.binary.cargo_profile, "debug");
    }

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
                    // A profile that carries one of the specific claims carries the
                    // count behind it, at the value that makes it a claim. Without
                    // this a re-run that quietly started crossing tenants, or
                    // outliving its bound, could still be retained as evidence:
                    // the throughput above would look the same.
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
            } else if slice.id == SliceId::Recovery {
                assert!(
                    record.profiles.is_empty() && record.observations.is_empty(),
                    "{relative}: recovery records use stage rows only"
                );
                let expected: BTreeMap<String, (String, String)> = manifest
                    .scenarios
                    .iter()
                    .flat_map(|scenario| {
                        scenario
                            .stages
                            .iter()
                            .filter(|stage| stage.status == "executable")
                            .map(|stage| {
                                (
                                    format!("{}/{}", scenario.id, stage.id),
                                    (
                                        stage.runner.clone().unwrap_or_default(),
                                        stage.driver.clone().unwrap_or_default(),
                                    ),
                                )
                            })
                    })
                    .collect();
                let recorded: BTreeSet<String> =
                    record.stages.iter().map(|stage| stage.id.clone()).collect();
                assert_eq!(
                    recorded,
                    expected.keys().cloned().collect(),
                    "{relative}: the record and recovery manifest disagree about executable stages"
                );
                assert_eq!(
                    record.stages.len(),
                    expected.len(),
                    "{relative}: a recovery stage appears more than once"
                );
                for stage in &record.stages {
                    validate_recovery_stage(stage, &record.binary.sha256)
                        .unwrap_or_else(|error| panic!("{relative}: {}: {error}", stage.id));
                    assert_eq!(
                        stage.runner, expected[&stage.id].0,
                        "{relative}: {} is attributed to the wrong recovery lane",
                        stage.id
                    );
                    assert_eq!(
                        stage.driver.as_deref(),
                        Some(expected[&stage.id].1.as_str()),
                        "{relative}: {} is attributed to the wrong recovery driver",
                        stage.id
                    );
                    assert!(
                        !stage.artifact_sha256.is_empty()
                            && stage.elapsed_ms > 0
                            && stage.verdicts > 0,
                        "{relative}: {} retains no artifact identity or judged duration",
                        stage.id
                    );
                    assert!(
                        stage.passed,
                        "{relative}: {} is retained as evidence of failed recovery",
                        stage.id
                    );
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
                    if matches!(slice.id, SliceId::Endurance | SliceId::StatefulEndurance) {
                        let required_duration = manifest
                            .profiles
                            .iter()
                            .find(|workload| workload.id == observation.id)
                            .and_then(|workload| workload.soak.as_ref())
                            .map(|soak| soak.duration_ms)
                            .unwrap_or_else(|| {
                                panic!(
                                    "{relative}: {} has no committed soak duration",
                                    observation.id
                                )
                            });
                        assert_eq!(
                            observation.manifest_duration_ms,
                            Some(required_duration),
                            "{relative}: {} does not retain the committed soak duration",
                            observation.id
                        );
                        assert!(
                            observation
                                .duration_ms
                                .is_some_and(|duration| duration >= required_duration),
                            "{relative}: {} offered less than the committed {} ms soak",
                            observation.id,
                            required_duration
                        );
                        assert!(
                            observation.requested_duration_ms.is_some()
                                && observation
                                    .duration_source
                                    .as_deref()
                                    .is_some_and(|source| !source.is_empty()),
                            "{relative}: {} does not retain how the soak duration was selected",
                            observation.id
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
                // And the binary digest, not the commit: this repository
                // squashes, so a branch SHA names history that never lands,
                // while the digest of the artifact that produced the numbers is
                // content-addressed and survives any rewriting. Disclosing it
                // is also what makes a re-run visible in the prose, because a
                // rebuilt binary hashes differently.
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
        .flat_map(|scenario| &scenario.stages)
        .flat_map(|stage| stage.blocked_on.iter().map(|dependency| dependency.issue))
        .collect();

    assert_eq!(
        declared,
        manifest,
        "the packet and {} disagree about what recovery waits on",
        recovery::MANIFEST_RELATIVE
    );
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

        if matches!(slice.id, SliceId::Endurance | SliceId::StatefulEndurance) {
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
        } else if slice.id == SliceId::Recovery {
            let expected = expected_recovery_stages();
            let mut recorded = BTreeSet::new();
            for stage in &record.stages {
                if !recorded.insert(stage.id.as_str()) {
                    errors.push(format!(
                        "{relative}: {} appears more than once in the recovery record",
                        stage.id
                    ));
                }
                if let Err(error) = validate_recovery_stage(stage, &record.binary.sha256) {
                    errors.push(format!("{relative}: {}: {error}", stage.id));
                }
                match expected.get(&stage.id) {
                    Some((runner, driver)) => {
                        if &stage.runner != runner {
                            errors.push(format!(
                                "{relative}: {} is attributed to runner {}, expected {runner}",
                                stage.id, stage.runner
                            ));
                        }
                        if stage.driver.as_deref() != Some(driver.as_str()) {
                            errors.push(format!(
                                "{relative}: {} is attributed to driver {:?}, expected {driver}",
                                stage.id, stage.driver
                            ));
                        }
                    }
                    None => errors.push(format!(
                        "{relative}: {} is not an executable recovery-manifest stage",
                        stage.id
                    )),
                }
                if stage.artifact_sha256.is_empty() || stage.elapsed_ms == 0 || stage.verdicts == 0
                {
                    errors.push(format!(
                        "{relative}: {} retains no artifact identity or judged duration",
                        stage.id
                    ));
                }
                if !stage.passed {
                    errors.push(format!(
                        "{relative}: {} is retained as evidence of failed recovery",
                        stage.id
                    ));
                }
            }
            for missing in expected.keys().filter(|id| !recorded.contains(id.as_str())) {
                errors.push(format!(
                    "{relative}: executable recovery stage {missing} is missing"
                ));
            }
        } else if slice.id == SliceId::Rollout {
            if record.observations.is_empty() {
                errors.push(format!("{relative} has no rollout observation"));
                continue;
            }
            let shared_revisions: BTreeSet<&str> = record
                .observations
                .iter()
                .filter_map(|observation| observation.rollout_shared_stateful_revision.as_deref())
                .collect();
            if shared_revisions.len() != 1 {
                errors.push(format!(
                    "{relative} does not bind one shared durable revision across rollout observations"
                ));
            }
            for observation in &record.observations {
                if let Err(error) =
                    validate_observation_artifact_schema(SliceId::Rollout, observation)
                {
                    errors.push(format!("{relative}: {}: {error}", observation.id));
                }
                if observation.rollout_candidate_binary_sha256.as_deref()
                    != Some(record.binary.sha256.as_str())
                {
                    errors.push(format!(
                        "{relative}: {} does not bind the candidate executable named by the record",
                        observation.id
                    ));
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
    let recovery_stage = packet::load_record("qualification/recovery/evidence/serving-ci.toml")
        .stages
        .into_iter()
        .next()
        .expect("historical recovery fixture has a stage");

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
            SliceId::StatefulEndurance => {
                record
                    .observations
                    .push(generated_stateful_observation_fixture(Some(
                        STATEFUL_ENDURANCE_RESULT_SCHEMA_VERSION,
                    )))
            }
            SliceId::Fault => record.observations.push(generated_observation_fixture(Some(
                FAULT_RESULT_SCHEMA_VERSION,
            ))),
            SliceId::Recovery => {
                for (id, (runner, driver)) in expected_recovery_stages() {
                    let mut stage = recovery_stage.clone();
                    stage.id = id;
                    stage.runner = runner;
                    stage.driver = Some(driver.clone());
                    stage.artifact_schema_version = Some(RECOVERY_RESULT_SCHEMA_VERSION);
                    stage.binary_sha256 = Some(record.binary.sha256.clone());
                    if driver == "stateful-integration" {
                        stage.executed_binary_sha256 = Some(record.binary.sha256.clone());
                        stage.execution_bound = Some(true);
                    } else {
                        stage.executed_binary_sha256 = None;
                        stage.execution_bound = None;
                    }
                    record.stages.push(stage);
                }
            }
            SliceId::Rollout => {
                let mut observation =
                    generated_rollout_observation_fixture(Some(ROLLOUT_RESULT_SCHEMA_VERSION));
                observation.rollout_candidate_binary_sha256 = Some(record.binary.sha256.clone());
                record.observations.push(observation);
            }
            _ => {}
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
fn recovery_closure_requires_the_exact_executable_manifest_stage_set() {
    let (qualification, records) = synthetic_closed_packet();
    let recovery = records
        .get("synthetic/recovery.toml")
        .expect("recovery fixture");
    assert_eq!(recovery.stages.len(), expected_recovery_stages().len());

    let mut missing = records.clone();
    missing
        .get_mut("synthetic/recovery.toml")
        .expect("recovery fixture")
        .stages
        .pop();
    assert!(!synthetic_closure_errors(&qualification, &missing).is_empty());

    let mut duplicate = records.clone();
    let recovery = duplicate
        .get_mut("synthetic/recovery.toml")
        .expect("recovery fixture");
    let repeated = recovery.stages.first().expect("recovery stage").clone();
    recovery.stages.push(repeated);
    assert!(!synthetic_closure_errors(&qualification, &duplicate).is_empty());

    let mut misattributed = records.clone();
    let stage = misattributed
        .get_mut("synthetic/recovery.toml")
        .expect("recovery fixture")
        .stages
        .first_mut()
        .expect("recovery stage");
    stage.runner = "wrong-runner".to_owned();
    stage.driver = Some("wrong-driver".to_owned());
    assert!(!synthetic_closure_errors(&qualification, &misattributed).is_empty());

    let mut unqualified = records.clone();
    let stage = unqualified
        .get_mut("synthetic/recovery.toml")
        .expect("recovery fixture")
        .stages
        .first_mut()
        .expect("recovery stage");
    stage.artifact_sha256.clear();
    stage.verdicts = 0;
    stage.passed = false;
    assert!(!synthetic_closure_errors(&qualification, &unqualified).is_empty());
}

#[test]
fn pending_or_mixed_candidate_provenance_cannot_close() {
    let (mut qualification, mut records) = synthetic_closed_packet();
    qualification.cohort.source_commit = PENDING_SOURCE_COMMIT.to_owned();
    assert!(!synthetic_closure_errors(&qualification, &records).is_empty());

    let (qualification, mut dirty_records) = synthetic_closed_packet();
    dirty_records
        .get_mut("synthetic/fault.toml")
        .expect("fault fixture")
        .source
        .git_dirty = true;
    assert!(!synthetic_closure_errors(&qualification, &dirty_records).is_empty());

    let recovery = records
        .get_mut("synthetic/recovery.toml")
        .expect("recovery fixture");
    recovery.binary.cargo_profile = "debug".to_owned();
    recovery.source.git_commit = "e".repeat(40);
    recovery.binary.version = "0.3.39".to_owned();
    assert!(!synthetic_closure_errors(&qualification, &records).is_empty());
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

    for relative in [
        "synthetic/endurance.toml",
        "synthetic/stateful-endurance.toml",
    ] {
        for mutate in mutations {
            let mut mutated = records.clone();
            let observation = mutated
                .get_mut(relative)
                .unwrap_or_else(|| panic!("missing {relative}"))
                .observations
                .first_mut()
                .expect("endurance observation");
            mutate(observation);
            assert!(!synthetic_closure_errors(&qualification, &mutated).is_empty());
        }
    }
}

#[test]
fn rollout_candidate_identity_and_shared_serving_are_closure_requirements() {
    let (qualification, records) = synthetic_closed_packet();

    let mutations: [fn(&mut packet::RecordObservation); 6] = [
        |observation: &mut packet::RecordObservation| {
            observation.rollout_previous_version = Some("0.3.39".to_owned());
        },
        |observation: &mut packet::RecordObservation| {
            observation.rollout_candidate_version = Some("0.3.41".to_owned());
        },
        |observation: &mut packet::RecordObservation| {
            observation.rollout_shared_stateful_revision = None;
        },
        |observation: &mut packet::RecordObservation| {
            observation.rollout_shared_alias = Some("chat-next-only".to_owned());
        },
        |observation: &mut packet::RecordObservation| {
            observation.rollout_previous_serves_shared_alias = Some(false);
        },
        |observation: &mut packet::RecordObservation| {
            observation.rollout_candidate_serves_shared_alias = Some(false);
        },
    ];
    for mutate in mutations {
        let mut mutated = records.clone();
        let observation = mutated
            .get_mut("synthetic/rollout.toml")
            .expect("rollout fixture")
            .observations
            .first_mut()
            .expect("rollout observation");
        mutate(observation);
        assert!(!synthetic_closure_errors(&qualification, &mutated).is_empty());
    }
}

/// Closure is derived from the slices. The flag exists so the packet states its
/// own conclusion in one place, and this is what stops that conclusion from
/// being an opinion: #156 is answered only when all six heavy records belong to
/// the frozen v0.4.0 release cohort.
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

/// Release-please runs this exact test before it can create the v0.4.0 tag.
/// Earlier versions may keep the release PR current while the candidate is
/// assembled, but the candidate version itself cannot publish without all six
/// heavy records from the frozen source cohort.
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

/// The prose page and the packet describe the same thing. The page is what an
/// operator reads, so a slice missing from it is a slice they will not know is
/// outstanding.
#[test]
fn the_packet_and_its_prose_agree() {
    let packet = packet::load();
    let contract = packet::contract_text();

    for slice in &packet.slices {
        let id = slice.id.as_str();
        // The slice's own row, not the page as a whole: every rung word appears
        // in the ladder table below the rows, so a page-wide search for the
        // status word is satisfied by the ladder no matter what the row says.
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
        packet_text.contains("#158 dependency formerly attached to fault and"),
        "the packet must explain why #158 no longer blocks its slices"
    );
    for slice_id in [SliceId::Fault, SliceId::Rollout] {
        let slice = packet.slice(slice_id);
        assert!(
            !slice.blocked_on.contains(&158),
            "{} must not reacquire retired dependency #158",
            slice_id.as_str()
        );
    }
    assert!(
        contract.contains("## Dependency retirements") && contract.contains("formerly named #158"),
        "the qualification page must record the retired #158 dependency"
    );
}
