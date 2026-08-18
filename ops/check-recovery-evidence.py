#!/usr/bin/env python3
"""Fail a lane whose executable recovery stages left no evidence.

The recovery manifest says which stages run and which lane runs them; the lanes
write one artifact per stage to `target/recovery/`. Between the two sits the
failure mode a retained evidence directory cannot describe on its own: a stage
that did not run leaves nothing, and nothing looks exactly like a lane whose
upload found no files. `if-no-files-found: warn` then turns a recovery claim
that was never exercised into a green tick.

So the manifest is read as the list of artifacts a lane owes:

    ops/check-recovery-evidence.py --runner stateful-tests \
        --executable target/release/axond
    ops/check-recovery-evidence.py --runner restore-drill \
        --executable target/release/axond

Each owed artifact must exist, name the stage and lane the manifest gave it,
carry the capability and evidence classes the contract grants it, be written in
the schema this build understands, and hold no failed gate or check. Anything
else exits non-zero with the reason.

Secrets are checked too, with `--forbid-env`: an artifact is published as a CI
artifact, so the rule that it retains references and counts rather than material
is enforced rather than trusted. The secret is named by the environment variable
holding it rather than passed as an argument, which would put it in the process
listing the caller took care to keep it out of.

Freshness is checked with `--since-unix-ms`: an artifact a previous run left in
`target/recovery/` reads exactly like one this run wrote, so a lane is asked to
say when it started and an older artifact is refused.

`--self-test` runs the checker against synthetic artifacts, one per way an
artifact can lie, because a checker that accepts everything is indistinguishable
from a lane that passed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

from recovery_contract import (
    RECOVERY_RESULT_SCHEMA_VERSION,
    RELEASE_CARGO_PROFILE,
    REQUIRED_GATE_NAMES,
    STAGE_REQUIRED_CHECKS,
    STAGE_REQUIRED_OBSERVATIONS,
    deferred_gate_detail,
    derive_verdict_outcome,
    gate_owner,
    gate_not_applicable,
    reconstruct_required_check,
    reconstruct_required_gate,
    validate_gate_ownership_model,
    validate_recovery_artifact,
)

# `tomllib` is standard from 3.11; on the repository's 3.10 ops floor the
# hash-pinned deploy lockfile supplies `tomli`, the backport it was extracted
# from. Say so, because a bare ModuleNotFoundError names neither the floor nor
# the lockfile that fixes it.
try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - exercised only on Python 3.10
    try:
        import tomli as tomllib  # type: ignore[no-redef]
    except ModuleNotFoundError:  # pragma: no cover - a floor without the backport
        raise SystemExit(
            f"this needs a TOML reader: {sys.executable} is "
            f"{sys.version_info.major}.{sys.version_info.minor}, which has no "
            "`tomllib` (3.11+), and the `tomli` backport is not installed. "
            "`just ops-venv` provisions it from ops/deploy-requirements.txt."
        ) from None

ROOT = Path(__file__).resolve().parent.parent
MANIFEST = ROOT / "qualification/recovery/manifest.toml"

# The schema both lanes write. Bumped with `EVIDENCE_SCHEMA_VERSION` in
# `crates/gateway/src/qualification/evidence.rs`.
SCHEMA_VERSION = RECOVERY_RESULT_SCHEMA_VERSION

RESTORE_DRILL = ROOT / "ops/restore-drill.sh"


def sha256_file(path: Path) -> str:
    """Hash the executable the lane actually retained and exercised."""
    if not path.is_file():
        raise SystemExit(f"the recovery executable {path} is not a regular file")
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def audit_scan_regression() -> list[str]:
    """Prove the restore drill cannot invert a matching scan under pipefail."""
    source = RESTORE_DRILL.read_text(encoding="utf-8")
    drained = 'grep -F "$GW_DRILL_BREAKGLASS" >/dev/null'
    complaints: list[str] = []
    if 'grep -qF "$GW_DRILL_BREAKGLASS"' in source:
        complaints.append("the audit scan still uses grep -qF and can false-clean")
    if source.count(drained) != 2:
        complaints.append(
            f"the audit scan has {source.count(drained)} drained callsites, expected 2"
        )

    # A match at the beginning of a payload larger than the pipe buffer is the
    # failure mode: grep -q exits early, printf receives SIGPIPE, and pipefail
    # makes the `then` branch look like a clean scan. The production form drains
    # the input before returning, so the same payload must report `leaked`.
    probe = r"""
set -o pipefail
needle='drill-secret'
payload="$(printf '%s\n' "$needle"; head -c 1048576 /dev/zero | tr '\0' x)"
if printf '%s' "$payload" | grep -F "$needle" >/dev/null; then
  printf 'leaked\n'
else
  printf 'clean\n'
fi
"""
    result = subprocess.run(
        ["bash", "-c", probe],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0 or result.stdout.strip() != "leaked":
        complaints.append(
            "a matching payload larger than the pipe buffer did not report leaked: "
            f"status={result.returncode}, stdout={result.stdout.strip()!r}, "
            f"stderr={result.stderr.strip()!r}"
        )
    return complaints


def owed(runner: str) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    manifest = tomllib.loads(MANIFEST.read_text(encoding="utf-8"))
    stages = [
        (scenario, stage)
        for scenario in manifest["scenario"]
        for stage in scenario["stage"]
        if stage["status"] == "executable" and stage.get("runner") == runner
    ]
    if not stages:
        raise SystemExit(f"the manifest gives no executable stage to the {runner} lane")
    return stages


def check(
    scenario: dict[str, Any],
    stage: dict[str, Any],
    directory: Path,
    runner: str,
    forbid: list[str],
    since_unix_ms: int = 0,
    expected_executable_sha256: str | None = None,
    expected_executable_path: Path | None = None,
) -> list[str]:
    key = f"{scenario['id']}/{stage['id']}"
    path = directory / f"{scenario['id']}.{stage['id']}.json"
    if not path.exists():
        return [
            f"{key}: the manifest calls this stage executable in the {runner} lane, "
            f"but {path.relative_to(ROOT) if path.is_relative_to(ROOT) else path} "
            "does not exist, so the stage did not run or did not retain its evidence"
        ]

    raw = path.read_text(encoding="utf-8")
    problems = [
        f"{key}: the artifact retains secret material"
        for secret in forbid
        if secret and secret in raw
    ]
    try:
        artifact = json.loads(raw)
    except json.JSONDecodeError as error:
        # A stage killed part way through leaves a half-written file, which is
        # exactly the case this checker exists to name rather than crash on.
        problems.append(f"{key}: the artifact is not readable JSON ({error})")
        return problems
    if not isinstance(artifact, dict):
        problems.append(f"{key}: the artifact root is not a JSON object")
        return problems

    expected = {
        "schema_version": SCHEMA_VERSION,
        "scenario": scenario["id"],
        "stage": stage["id"],
        "runner": runner,
        "capability": scenario["capability"],
        "evidence": stage["evidence"],
    }
    for field, want in expected.items():
        got = artifact.get(field)
        if got != want:
            problems.append(f"{key}: {field} is {got!r}, and the manifest says {want!r}")

    problems.extend(
        f"{key}: {problem}"
        for problem in validate_recovery_artifact(artifact, scenario, stage)
    )
    run = artifact.get("run") if isinstance(artifact.get("run"), dict) else {}
    executable_sha256 = run.get("axond_executable_sha256")
    if (
        expected_executable_sha256 is not None
        and executable_sha256 != expected_executable_sha256
    ):
        problems.append(
            f"{key}: run.axond_executable_sha256 does not identify the supplied "
            "release axond executable"
        )
    if stage.get("driver") == "stateful-integration" and expected_executable_path:
        recorded_path = run.get("axond_executable_path")
        if (
            not isinstance(recorded_path, str)
            or Path(recorded_path).resolve() != expected_executable_path.resolve()
        ):
            problems.append(
                f"{key}: run.axond_executable_path does not identify the supplied "
                "release axond executable"
            )
    started = run.get("started_at_unix_ms", 0)
    if since_unix_ms and isinstance(started, int) and started < since_unix_ms:
        problems.append(
            f"{key}: the artifact began at {started}, before this run started at "
            f"{since_unix_ms}, so it was left by an earlier run and this stage did not run"
        )
    if not problems:
        evaluated = sum(
            1 for entry in artifact.get("gates", []) if entry.get("outcome") != "not_evaluated"
        )
        print(
            f"  ok  {key}: {len(artifact['timeline'])} events, "
            f"{len(artifact.get('observations', {}))} observations, "
            f"{evaluated} gates evaluated, {len(artifact.get('checks', []))} checks"
        )
    return problems


def check_gate_coverage(
    stages: list[tuple[dict[str, Any], dict[str, Any]]],
    directory: Path,
    runner: str,
) -> list[str]:
    """Prove each scenario gate is evaluated by exactly its designated owner."""
    scenarios = {
        scenario["id"]: scenario
        for scenario, _ in stages
        if isinstance(scenario.get("id"), str)
    }
    problems = [
        f"gate ownership model: {problem}"
        for problem in validate_gate_ownership_model(list(scenarios.values()))
    ]
    evaluated: dict[tuple[str, str], list[str]] = {}
    for scenario, stage in stages:
        scenario_id = scenario["id"]
        stage_id = stage["id"]
        path = directory / f"{scenario_id}.{stage_id}.json"
        if not path.is_file():
            continue
        try:
            artifact = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        gates = artifact.get("gates") if isinstance(artifact, dict) else None
        if not isinstance(gates, list):
            continue
        for entry in gates:
            if (
                isinstance(entry, dict)
                and entry.get("gate") in REQUIRED_GATE_NAMES
                and entry.get("outcome") != "not_evaluated"
            ):
                evaluated.setdefault((scenario_id, entry["gate"]), []).append(stage_id)

    for scenario_id, scenario in scenarios.items():
        for gate in REQUIRED_GATE_NAMES:
            owner = gate_owner(scenario, gate)
            actual = evaluated.get((scenario_id, gate), [])
            if owner is None and gate_not_applicable(scenario, gate) is None:
                problems.append(
                    f"{scenario_id}/{gate}: no designated owner exists; evaluated by "
                    f"{actual or 'no stage'}"
                )
            elif owner is None and actual:
                problems.append(
                    f"{scenario_id}/{gate}: gate is explicitly non-applicable but was "
                    f"evaluated by {actual} in the {runner} lane"
                )
            elif actual != [owner]:
                problems.append(
                    f"{scenario_id}/{gate}: expected exactly one evaluation by {owner!r}, "
                    f"got {actual or 'none'} in the {runner} lane"
                )
    return problems


def self_test() -> int:
    """Check the checker: each way an artifact can lie has to be caught."""
    runner = "restore-drill"
    scenario, stage = owed(runner)[0]
    executable_sha256 = "a" * 64

    def artifact(
        contract_scenario: dict[str, Any] = scenario,
        contract_stage: dict[str, Any] = stage,
        contract_runner: str = runner,
    ) -> dict[str, Any]:
        gates = []
        for gate in REQUIRED_GATE_NAMES:
            bound = str(contract_scenario["gate"][gate])
            owns_gate = gate_owner(contract_scenario, gate) == contract_stage["id"]
            gates.append(
                {
                    "gate": gate,
                    "bound": bound,
                    "observed": bound if owns_gate else "not measured",
                    "outcome": "met" if owns_gate else "not_evaluated",
                    "detail": (
                        "the synthetic complete fixture evaluates the gate owned by this stage"
                        if owns_gate
                        else deferred_gate_detail(
                            gate,
                            contract_stage["evidence"],
                            "the synthetic complete fixture assigns the measurement "
                            "to its designated owner",
                        )
                    ),
                }
            )
        contract_key = f"{contract_scenario['id']}/{contract_stage['id']}"
        observations = {
            field: "observed"
            for field in STAGE_REQUIRED_OBSERVATIONS[contract_key]
        }
        synthetic_bindings: dict[str, dict[str, Any]] = {
            "control-plane-outage/journal-outage": {
                "revision": "rev-outage",
                "active_revision": "rev-outage",
                "convergence_rejection_reason": "unavailable",
                "convergence_lag_ms": 1,
                "proxy_severed_connections": 1,
                "admin_write_status": 503,
                "admin_write_error": "control_plane_unavailable",
            },
            "control-plane-outage/serving": {
                "revision": "rev-outage",
                "proxy_severed_connections": 1,
                "inference_status": 200,
                "ready_status": 200,
            },
            "control-plane-outage/administration": {
                "authenticated_state_status": 503,
                "mutation_status": 503,
                "anonymous_state_status": 401,
            },
            "recovery-convergence/journal-recovery": {
                "outage_revision": "rev-outage",
                "unseen_revision": "rev-recovered",
                "loaded_unseen_revision": "rev-recovered",
                "recovered_head_revision": "rev-recovered",
                "active_revision": "rev-recovered",
                "direct_replica_active_revision": "rev-recovered",
                "snapshot_source": "journal",
                "converged": True,
                "residual_lag_ms": 0,
                "recovery_seconds": 1,
                "recovered_history_revisions": 3,
                "recovered_history_contains_required_revisions": True,
                "post_recovery_write_accepted": True,
            },
            "recovery-convergence/serving": {
                "revision": "rev-recovered",
                "source": "journal",
                "converged": True,
                "chat_status": 200,
                "ready_status": 200,
            },
            "recovery-convergence/administration": {
                "audit_status": 200,
                "actor": "breakglass",
                "anonymous_admin_status": 401,
            },
            "secret-rotation/rotation": {
                "revision": "rev-rotated",
                "active_revision": "rev-rotated",
                "source": "journal",
                "converged": True,
                "rotation_seconds": 1,
                "publication_accepted": True,
                "rotated_revision_published": True,
                "rotation_history_contains_required_revisions": True,
                "same_replica_before_and_after_rotation": True,
            },
            "secret-rotation/serving": {
                "chat_status": 200,
                "ready_status": 200,
                "audit_status": 200,
                "audit_actor": "breakglass",
                "credential": "rotated-reference",
                "anonymous_admin_status": 401,
                "rotated_material_authenticated_upstream": True,
            },
            "cold-boot-valid-cache/cold-boot": {
                "boot_note": "cache restored",
                "cold_start_outcome": "served",
                "cached_revision": "rev-cached",
                "restored_revision": "rev-cached",
                "active_revision": "rev-cached",
                "snapshot_source": "last-known-good",
                "ready_status": 200,
            },
            "cold-boot-valid-cache/serving": {
                "ready_status": 200,
                "chat_status": 200,
                "anonymous_models_status": 401,
                "admin_catalogue_status": 503,
                "admin_mutation_status": 503,
                "anonymous_admin_status": 401,
            },
            "cold-boot-no-cache/cold-boot": {
                "boot_note": "no cache",
                "cold_start_outcome": "refused",
                "refusal": "no serving snapshot",
                "snapshot_generation_after_cold_boot": 0,
                "ready_status": 503,
                "active_revision": None,
                "anonymous_models_status": 401,
            },
            "cold-boot-no-cache/readiness": {
                "ready_status": 503,
                "admin_state_status": 503,
                "admin_mutation_status": 503,
                "anonymous_admin_status": 401,
                "anonymous_models_status": 401,
            },
            "cold-boot-invalid-cache/cold-boot": {
                "boot_note": "invalid cache",
                "cold_start_outcome": "refused",
                "unauthentic_cache_variants_refused": 3,
                "ready_status": 503,
                "edited_record_refused": True,
                "truncated_file_refused": True,
                "foreign_signing_key_refused": True,
            },
            "cold-boot-invalid-cache/readiness": {
                "ready_status": 503,
                "admin_state_status": 503,
                "admin_mutation_status": 503,
                "anonymous_admin_status": 401,
                "anonymous_models_status": 401,
            },
            "backup-restore/restore": {
                "live_head_revision": "rev-backup",
                "restored_head_revision": "rev-backup",
                "live_revisions": 3,
                "restored_revision_count": 3,
                "live_resources": 4,
                "restored_resource_count": 4,
                "live_head_checksum": "checksum-backup",
                "restored_head_checksum": "checksum-backup",
                "revision_after_restore": "rev-after-restore",
            },
            "backup-restore/reconvergence": {
                "survivor_inference_status": 200,
                "survivor_convergence_lag_seconds": 1,
                "survivor_readiness_status": 200,
            },
            "backup-restore/administration": {
                "unauthenticated_admin_successes": 0,
            },
            "backup-restore/durable-inventory": {
                "expected_secret_owner": "tenant-fixture",
                "restored_secret_versions": 1,
                "restored_secret_owner": "tenant-fixture",
                "restored_secret_ciphertext_rows": 1,
                "restored_catalog_snapshot_rows": 1,
                "expected_catalog_active_content": "catalog-fixture",
                "restored_catalog_active_content": "catalog-fixture",
                "live_usage_rows": 1,
                "restored_usage_rows": 1,
                "live_usage_outbox_rows": 1,
                "restored_usage_outbox_rows": 1,
                "logical_backup_usage_request_id": "req_aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "logical_backup_usage_status": "200",
                "logical_backup_new_usage_rows": 1,
                "logical_backup_source_usage_identity_rows": 1,
                "logical_backup_source_outbox_identity_rows": 1,
                "logical_backup_restored_usage_identity_rows": 1,
                "logical_backup_restored_outbox_identity_rows": 1,
                "restored_price_book_rows": 1,
                "live_price_book_checksum": "checksum-fixture",
                "restored_price_book_checksum": "checksum-fixture",
                "restored_price_book_schema": "axond.price-book.v2",
                "restored_price_book_catalog_version": 1,
                "restored_price_book_approval_state": "approved",
                "restored_price_book_approval_citation": "restore drill",
                "restored_price_book_rule_count": 2,
                "expected_price_book_history": "history-fixture",
                "restored_price_book_history": "history-fixture",
            },
            "point-in-time-recovery/recovery": {
                "recovery_in_progress": "f",
                "recovered_schema_status": "current",
                "pre_target_head_revision": "rev-pre-target",
                "post_target_head_revision": "rev-post-target",
                "revisions_before_target": 3,
                "pitr_secret_versions": 1,
                "expected_secret_owner": "tenant-fixture",
                "pitr_secret_owner": "tenant-fixture",
                "pitr_secret_lifecycle": "active",
                "expected_pitr_catalogue_content_id": "catalog-fixture",
                "pitr_catalogue_content_id": "catalog-fixture",
                "expected_pitr_catalogue_raw_digest": "digest-fixture",
                "pitr_catalogue_raw_digest": "digest-fixture",
                "expected_pitr_catalogue_raw_bytes": 42,
                "pitr_catalogue_raw_bytes": 42,
                "pitr_catalogue_payload_bytes": 42,
                "recovered_head_revision": "rev-pre-target",
                "revisions_after_recovery": 3,
                "post_target_revision_presence": "absent",
                "revision_after_recovery": "rev-recovered-write",
                "revisions_after_recovery_publication": 4,
                "recovered_axond_usage_table_count": 1,
                "recovered_axond_budget_table_count": 1,
                "recovered_axond_revocation_table_count": 1,
            },
            "point-in-time-recovery/usage-boundary": {
                "pre_target_usage_request_id": "req_aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "post_target_usage_request_id": "req_bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                "pre_target_chat_status": "200",
                "post_target_chat_status": "200",
                "pre_target_usage_count": 1,
                "post_target_usage_count": 2,
                "pre_target_new_usage_rows": 1,
                "post_target_new_usage_rows": 1,
                "recovered_pre_target_usage": 1,
                "recovered_pre_target_outbox": 1,
                "recovered_post_target_usage": 0,
                "recovered_post_target_outbox": 0,
            },
            "point-in-time-recovery/reconvergence": {
                "survivor_inference_status": 200,
                "survivor_convergence_lag_seconds": 1,
                "survivor_readiness_status": 200,
            },
            "point-in-time-recovery/administration": {
                "unauthenticated_admin_successes": 0,
            },
        }
        observations.update(synthetic_bindings.get(contract_key, {}))
        for gate_entry in gates:
            gate_name = gate_entry["gate"]
            if gate_owner(contract_scenario, gate_name) != contract_stage["id"]:
                continue
            reconstructed = reconstruct_required_gate(
                contract_scenario, contract_stage, gate_name, observations
            )
            gate_entry["observed"] = reconstructed
            gate_entry["outcome"] = derive_verdict_outcome(
                "gate", gate_name, gate_entry["bound"], reconstructed
            )
        checks = []
        for check in sorted(STAGE_REQUIRED_CHECKS.get(contract_key, ())):
            expected, observed = reconstruct_required_check(
                contract_scenario, contract_stage, check, observations
            )
            checks.append(
                {
                    "gate": check,
                    "bound": expected,
                    "observed": observed,
                    "outcome": "met" if expected == observed else "failed",
                    "detail": "the synthetic fixture retains the reconstructed equality check",
                }
            )
        run = {
            "started_at_unix_ms": 2_000,
            "elapsed_ms": 10,
            "axond_version": "0.4.0",
            "control_plane": "postgres",
            "schema": "recovery_self_test",
            "schema_identity": "schema 3 current",
            "axond_executable_sha256": executable_sha256,
            "cargo_profile": RELEASE_CARGO_PROFILE,
        }
        if contract_stage.get("driver") in {"stateful-integration", "restore-drill"}:
            run.update(
                {
                    "axond_executed_sha256": executable_sha256,
                    "axond_executable_path": "/workspace/target/release/axond",
                    "axond_execution_bound": True,
                }
            )
        return {
            "schema_version": SCHEMA_VERSION,
            "scenario": contract_scenario["id"],
            "stage": contract_stage["id"],
            "runner": contract_runner,
            "capability": contract_scenario["capability"],
            "evidence": contract_stage["evidence"],
            "run": run,
            "timeline": [{"at_ms": 1, "event": "restored", "detail": "a restore happened"}],
            "observations": observations,
            "gates": gates,
            "checks": checks,
        }

    failed = {
        "gate": "the_restored_head_is_the_backed_up_head",
        "bound": "rev_a",
        "observed": "rev_b",
        "outcome": "failed",
        "detail": "the restore landed on another head",
    }
    failed_gate = artifact()
    failed_gate["gates"][0].update(observed="1", outcome="failed")
    missing_gate = artifact()
    missing_gate["gates"].pop()
    invalid_outcome = artifact()
    invalid_outcome["gates"][0]["outcome"] = "unknown"
    false_numeric_met = artifact()
    false_numeric_met["gates"][0].update(observed="1", outcome="met")
    false_check_met = artifact()
    false_check_met["checks"] = [
        {
            "gate": "a_forged_equality",
            "bound": "expected",
            "observed": "different",
            "outcome": "met",
            "detail": "the forged fixture claims unequal values match",
        }
    ]
    malformed_verdict = artifact()
    malformed_verdict["checks"] = [{}]
    unjustified_deferral = artifact()
    unjustified_deferral["gates"][0]["detail"] = "another stage handles this"
    owner_deferral = artifact()
    next(
        entry
        for entry in owner_deferral["gates"]
        if entry["gate"] == "max_data_loss_revisions"
    ).update(
        observed="not measured",
        outcome="not_evaluated",
        detail=deferred_gate_detail(
            "max_data_loss_revisions",
            stage["evidence"],
            "the synthetic owner improperly deferred its assigned gate",
        ),
    )
    nonowner_evaluation = artifact()
    next(
        entry
        for entry in nonowner_evaluation["gates"]
        if entry["gate"] == "max_serving_error_fraction"
    ).update(
        observed="0.0",
        outcome="met",
        detail="the synthetic non-owner tried to evaluate another stage's gate",
    )
    missing_observation = artifact()
    missing_observation["observations"].pop(next(iter(missing_observation["observations"])))

    # A body given as a string is written verbatim, for the shapes that are not
    # valid JSON in the first place.
    cases: list[tuple[str, dict[str, Any] | str | None, list[str], bool]] = [
        ("a complete artifact", artifact(), [], True),
        ("a missing artifact", None, [], False),
        ("a future schema", {**artifact(), "schema_version": SCHEMA_VERSION + 1}, [], False),
        ("another lane's artifact", {**artifact(), "runner": "stateful-tests"}, [], False),
        (
            "missing executable identity",
            {
                **artifact(),
                "run": {
                    "started_at_unix_ms": 2_000,
                    "cargo_profile": RELEASE_CARGO_PROFILE,
                },
            },
            [],
            False,
        ),
        (
            "a malformed executable identity",
            {
                **artifact(),
                "run": {
                    **artifact()["run"],
                    "axond_executable_sha256": "SHA256:not-a-digest",
                },
            },
            [],
            False,
        ),
        (
            "another executable's identity",
            {
                **artifact(),
                "run": {
                    **artifact()["run"],
                    "axond_executable_sha256": "b" * 64,
                },
            },
            [],
            False,
        ),
        (
            "missing cargo profile",
            {
                **artifact(),
                "run": {
                    "started_at_unix_ms": 2_000,
                    "axond_executable_sha256": executable_sha256,
                },
            },
            [],
            False,
        ),
        (
            "a debug cargo profile",
            {
                **artifact(),
                "run": {**artifact()["run"], "cargo_profile": "debug"},
            },
            [],
            False,
        ),
        (
            "evidence the contract did not grant",
            {**artifact(), "evidence": [*stage["evidence"], "serving_behavior"]},
            [],
            False,
        ),
        ("a failed check", {**artifact(), "checks": [failed]}, [], False),
        ("a failed gate", failed_gate, [], False),
        ("a missing required gate", missing_gate, [], False),
        ("an unsupported verdict outcome", invalid_outcome, [], False),
        ("a false numeric met outcome", false_numeric_met, [], False),
        ("a false equality-check met outcome", false_check_met, [], False),
        ("a verdict with no schema fields", malformed_verdict, [], False),
        ("a deferral without evidence-class justification", unjustified_deferral, [], False),
        ("a designated owner that defers its gate", owner_deferral, [], False),
        ("a non-owner that evaluates a gate", nonowner_evaluation, [], False),
        ("a missing stage observation", missing_observation, [], False),
        (
            "a non-Postgres recovery",
            {
                **artifact(),
                "run": {**artifact()["run"], "control_plane": "memory"},
            },
            [],
            False,
        ),
        (
            "an empty schema identity",
            {
                **artifact(),
                "run": {**artifact()["run"], "schema_identity": ""},
            },
            [],
            False,
        ),
        ("an empty timeline", {**artifact(), "timeline": []}, [], False),
        ("a truncated artifact", json.dumps(artifact())[:120], [], False),
        ("a non-object artifact", [], [], False),
        (
            "a leaked credential",
            {**artifact(), "observations": {"note": "the-drill-credential"}},
            ["the-drill-credential"],
            False,
        ),
    ]

    # The pairing case needs a stage the contract does *not* grant the loss
    # boundary to, judging the gate that measures it.
    unbacked = {
        **stage,
        "evidence": [c for c in stage["evidence"] if c != "revision_loss_boundary"],
    }

    # A leak check that checks for nothing is the one lie the artifacts cannot
    # carry: it is told by the caller, so it is caught here.
    os.environ.pop("AXOND_ABSENT_SECRET", None)
    os.environ["AXOND_PRESENT_SECRET"] = "the-drill-credential"
    complaints: list[str] = []
    try:
        resolve_forbidden(["AXOND_PRESENT_SECRET", "AXOND_ABSENT_SECRET"])
    except SystemExit:
        pass
    else:
        complaints.append("an unset --forbid-env name: the checker accepted it")
    if resolve_forbidden(["AXOND_PRESENT_SECRET"]) != ["the-drill-credential"]:
        complaints.append("a set --forbid-env name: the checker did not resolve it")
    complaints.extend(audit_scan_regression())

    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / f"{scenario['id']}.{stage['id']}.json"
        path.write_text(
            json.dumps(
                {
                    **artifact(),
                    "evidence": unbacked["evidence"],
                    "gates": [
                        {
                            "gate": "max_data_loss_revisions",
                            "bound": "0",
                            "observed": "0",
                            "outcome": "met",
                            "detail": "nothing was lost",
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        if not check(
            scenario,
            unbacked,
            Path(directory),
            runner,
            [],
            expected_executable_sha256=executable_sha256,
        ):
            complaints.append(
                "a gate judged without the evidence class that measures it: "
                "the checker accepted it"
            )
        path.unlink(missing_ok=True)
        for name, body, forbid, want_ok in cases:
            path.unlink(missing_ok=True)
            if body is not None:
                raw = body if isinstance(body, str) else json.dumps(body)
                path.write_text(raw, encoding="utf-8")
            problems = check(
                scenario,
                stage,
                Path(directory),
                runner,
                forbid,
                expected_executable_sha256=executable_sha256,
            )
            if bool(problems) == want_ok:
                verb = "rejected" if problems else "accepted"
                complaints.append(f"{name}: the checker {verb} it: {'; '.join(problems)}")

        # Freshness is the one lie a single artifact cannot tell on its own: it
        # depends on when the run asking about it began.
        path.write_text(json.dumps(artifact()), encoding="utf-8")
        for name, since, want_ok in [
            ("an artifact from this run", 1_000, True),
            ("an artifact an earlier run left behind", 3_000, False),
        ]:
            problems = check(
                scenario,
                stage,
                Path(directory),
                runner,
                [],
                since,
                executable_sha256,
            )
            if bool(problems) == want_ok:
                verb = "rejected" if problems else "accepted"
                complaints.append(f"{name}: the checker {verb} it: {'; '.join(problems)}")

        process_scenario, process_stage = next(
            pair
            for pair in owed("stateful-tests")
            if pair[0]["id"] == "recovery-convergence"
            and pair[1]["id"] == "serving"
        )
        process_path = (
            Path(directory)
            / f"{process_scenario['id']}.{process_stage['id']}.json"
        )
        process_complete = artifact(
            process_scenario, process_stage, "stateful-tests"
        )

        def changed_process_run(**changes: Any) -> dict[str, Any]:
            candidate = json.loads(json.dumps(process_complete))
            candidate["run"].update(changes)
            return candidate

        missing_executed_digest = json.loads(json.dumps(process_complete))
        missing_executed_digest["run"].pop("axond_executed_sha256")
        missing_executable_path = json.loads(json.dumps(process_complete))
        missing_executable_path["run"].pop("axond_executable_path")
        forged_process_check_pair = json.loads(json.dumps(process_complete))
        forged_process_check_pair["checks"][0].update(
            bound="forged-but-equal",
            observed="forged-but-equal",
            outcome="met",
        )
        forged_process_gate = json.loads(json.dumps(process_complete))
        forged_process_gate["observations"]["ready_status"] = 503
        unexpected_process_check = json.loads(json.dumps(process_complete))
        extra_check = json.loads(json.dumps(unexpected_process_check["checks"][0]))
        extra_check["gate"] = "uncommitted_process_claim"
        unexpected_process_check["checks"].append(extra_check)
        process_cases = [
            ("complete execution-bound process evidence", process_complete, True),
            ("process evidence without its executed digest", missing_executed_digest, False),
            ("process evidence without its executable path", missing_executable_path, False),
            (
                "process evidence whose executed digest was post-stamped",
                changed_process_run(axond_executable_sha256="b" * 64),
                False,
            ),
            (
                "process evidence without an execution-bound marker",
                changed_process_run(axond_execution_bound=False),
                False,
            ),
            (
                "process evidence with a forged equal check pair",
                forged_process_check_pair,
                False,
            ),
            (
                "process evidence with a gate detached from its observation",
                forged_process_gate,
                False,
            ),
            (
                "process evidence with an unexpected passing check",
                unexpected_process_check,
                False,
            ),
            (
                "process evidence naming another release executable path",
                changed_process_run(
                    axond_executable_path="/other/target/release/axond"
                ),
                False,
            ),
            (
                "process evidence from a debug executable path",
                changed_process_run(
                    axond_executable_path="/workspace/target/debug/axond",
                    cargo_profile="debug",
                ),
                False,
            ),
        ]
        for name, body, want_ok in process_cases:
            process_path.write_text(json.dumps(body), encoding="utf-8")
            problems = check(
                process_scenario,
                process_stage,
                Path(directory),
                "stateful-tests",
                [],
                expected_executable_sha256=executable_sha256,
                expected_executable_path=Path("/workspace/target/release/axond"),
            )
            if bool(problems) == want_ok:
                verb = "rejected" if problems else "accepted"
                complaints.append(f"{name}: the checker {verb} it: {'; '.join(problems)}")

        for parity_scenario, parity_stage in owed("stateful-tests"):
            parity_path = Path(directory) / (
                f"{parity_scenario['id']}.{parity_stage['id']}.json"
            )
            parity_path.write_text(
                json.dumps(
                    artifact(parity_scenario, parity_stage, "stateful-tests")
                ),
                encoding="utf-8",
            )
            parity_problems = check(
                parity_scenario,
                parity_stage,
                Path(directory),
                "stateful-tests",
                [],
                expected_executable_sha256=executable_sha256,
                expected_executable_path=Path("/workspace/target/release/axond"),
            )
            if parity_problems:
                complaints.append(
                    f"complete {parity_scenario['id']}/{parity_stage['id']} "
                    "process contract: the checker rejected it: "
                    + "; ".join(parity_problems)
                )
            parity_path.unlink(missing_ok=True)

        no_cache_scenario, no_cache_stage = next(
            pair
            for pair in owed("stateful-tests")
            if pair[0]["id"] == "cold-boot-no-cache"
            and pair[1]["id"] == "cold-boot"
        )
        nonnull_no_cache = artifact(
            no_cache_scenario, no_cache_stage, "stateful-tests"
        )
        nonnull_no_cache["observations"]["active_revision"] = "rev-forged"
        no_cache_path = Path(directory) / "cold-boot-no-cache.cold-boot.json"
        no_cache_path.write_text(json.dumps(nonnull_no_cache), encoding="utf-8")
        no_cache_problems = check(
            no_cache_scenario,
            no_cache_stage,
            Path(directory),
            "stateful-tests",
            [],
            expected_executable_sha256=executable_sha256,
            expected_executable_path=Path("/workspace/target/release/axond"),
        )
        if not no_cache_problems:
            complaints.append(
                "a non-null no-cache active_revision: the checker accepted it"
            )

        durable_scenario, durable_stage = next(
            pair for pair in owed("restore-drill") if pair[1]["id"] == "durable-inventory"
        )
        durable_complete = artifact(durable_scenario, durable_stage, "restore-drill")
        durable_missing_check = json.loads(json.dumps(durable_complete))
        durable_missing_check["checks"].pop()
        durable_missing_observation = json.loads(json.dumps(durable_complete))
        durable_missing_observation["observations"].pop("restored_price_book_history")
        durable_missing_usage = json.loads(json.dumps(durable_complete))
        durable_missing_usage["observations"].pop("restored_usage_rows")
        durable_forged_pair = json.loads(json.dumps(durable_complete))
        durable_forged_pair["checks"][0].update(
            bound="forged-but-equal",
            observed="forged-but-equal",
            outcome="met",
        )
        durable_missing_binding = json.loads(json.dumps(durable_complete))
        durable_missing_binding["observations"].pop("expected_secret_owner")
        durable_path = (
            Path(directory) / f"{durable_scenario['id']}.{durable_stage['id']}.json"
        )
        durable_cases = [
            ("complete durable-inventory evidence", durable_complete, True),
            ("durable inventory without a required check", durable_missing_check, False),
            (
                "durable inventory without pricing-history observations",
                durable_missing_observation,
                False,
            ),
            (
                "durable inventory without usage-boundary observations",
                durable_missing_usage,
                False,
            ),
            (
                "durable inventory with a forged equal bound and observation",
                durable_forged_pair,
                False,
            ),
            (
                "durable inventory without a check-binding observation",
                durable_missing_binding,
                False,
            ),
        ]
        for name, body, want_ok in durable_cases:
            durable_path.write_text(json.dumps(body), encoding="utf-8")
            problems = check(
                durable_scenario,
                durable_stage,
                Path(directory),
                "restore-drill",
                [],
                expected_executable_sha256=executable_sha256,
            )
            if bool(problems) == want_ok:
                verb = "rejected" if problems else "accepted"
                complaints.append(f"{name}: the checker {verb} it: {'; '.join(problems)}")

        coverage_directory = Path(directory) / "coverage"
        coverage_directory.mkdir()
        coverage_stages = [
            pair for pair in owed(runner) if pair[0]["id"] == scenario["id"]
        ]
        for coverage_scenario, coverage_stage in coverage_stages:
            coverage_path = coverage_directory / (
                f"{coverage_scenario['id']}.{coverage_stage['id']}.json"
            )
            coverage_path.write_text(
                json.dumps(artifact(coverage_scenario, coverage_stage, runner)),
                encoding="utf-8",
            )
        coverage_problems = check_gate_coverage(
            coverage_stages, coverage_directory, runner
        )
        if coverage_problems:
            complaints.append(
                "complete exact gate ownership: the checker rejected it: "
                + "; ".join(coverage_problems)
            )

        owner_path = coverage_directory / "backup-restore.restore.json"
        owner_artifact = json.loads(owner_path.read_text(encoding="utf-8"))
        owner_gate = next(
            entry
            for entry in owner_artifact["gates"]
            if entry["gate"] == "max_data_loss_revisions"
        )
        owner_gate.update(
            observed="not measured",
            outcome="not_evaluated",
            detail=deferred_gate_detail(
                "max_data_loss_revisions",
                stage["evidence"],
                "the synthetic forged owner improperly deferred its assigned gate",
            ),
        )
        owner_path.write_text(json.dumps(owner_artifact), encoding="utf-8")
        if not check_gate_coverage(coverage_stages, coverage_directory, runner):
            complaints.append(
                "an owner that deferred its gate: combined coverage accepted it"
            )

        owner_path.write_text(json.dumps(artifact()), encoding="utf-8")
        nonowner_scenario, nonowner_stage = next(
            pair for pair in coverage_stages if pair[1]["id"] == "durable-inventory"
        )
        nonowner_path = coverage_directory / (
            f"{nonowner_scenario['id']}.{nonowner_stage['id']}.json"
        )
        nonowner_artifact = json.loads(nonowner_path.read_text(encoding="utf-8"))
        nonowner_gate = next(
            entry
            for entry in nonowner_artifact["gates"]
            if entry["gate"] == "max_data_loss_revisions"
        )
        nonowner_gate.update(
            observed="0",
            outcome="met",
            detail="the synthetic non-owner tried to duplicate the owner verdict",
        )
        nonowner_path.write_text(json.dumps(nonowner_artifact), encoding="utf-8")
        if not check_gate_coverage(coverage_stages, coverage_directory, runner):
            complaints.append(
                "a non-owner that duplicated a gate: combined coverage accepted it"
            )

    if complaints:
        print("the recovery evidence checker does not catch what it claims:", file=sys.stderr)
        for complaint in complaints:
            print(f"  {complaint}", file=sys.stderr)
        return 1
    print(
        "the evidence checker catches "
        f"{len(cases) + len(process_cases) + len(durable_cases) + 20} "
        "complete and forged artifact shapes"
    )
    return 0


def resolve_forbidden(names: list[str]) -> list[str]:
    """The secret values behind `--forbid-env` names, or a refusal.

    A misspelled or unexported name would otherwise resolve to the empty string
    and be skipped, so the run would report clean evidence while checking for
    nothing. The gate is the reason the artifacts can be published, so it fails
    loudly instead.
    """
    missing = [name for name in names if not os.environ.get(name)]
    if missing:
        raise SystemExit(
            "--forbid-env names "
            + ", ".join(missing)
            + ", which is unset or empty, so the leak check would pass by checking "
            "for nothing; export it or drop the flag"
        )
    return [os.environ[name] for name in names]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="check the checker against synthetic artifacts and exit",
    )
    parser.add_argument(
        "--runner",
        choices=("stateful-tests", "restore-drill"),
        help="the lane whose stages must have left evidence",
    )
    parser.add_argument("--dir", type=Path, default=ROOT / "target/recovery")
    parser.add_argument(
        "--executable",
        type=Path,
        help=(
            "the exact release axond executable every owed artifact must identify "
            "by SHA-256"
        ),
    )
    parser.add_argument(
        "--forbid-env",
        action="append",
        default=[],
        metavar="NAME",
        help=(
            "an environment variable whose value no artifact may contain, such as "
            "the drill's credential; a name that is unset or empty is an error, "
            "because a leak check that checks for nothing passes everything"
        ),
    )
    parser.add_argument(
        "--since-unix-ms",
        type=int,
        default=0,
        help="refuse an artifact that began before this instant, in Unix milliseconds",
    )
    args = parser.parse_args()
    forbid = resolve_forbidden(args.forbid_env)
    if args.self_test:
        return self_test()
    if not args.runner:
        parser.error("--runner is required unless --self-test is given")
    if args.executable is None:
        parser.error("--executable is required unless --self-test is given")
    expected_executable_sha256 = sha256_file(args.executable)
    expected_executable_path = args.executable.resolve()

    stages = owed(args.runner)
    problems: list[str] = []
    for scenario, stage in stages:
        problems.extend(
            check(
                scenario,
                stage,
                args.dir,
                args.runner,
                forbid,
                args.since_unix_ms,
                expected_executable_sha256,
                expected_executable_path,
            )
        )
    problems.extend(check_gate_coverage(stages, args.dir, args.runner))

    if problems:
        print(f"\nrecovery evidence is not complete for the {args.runner} lane:", file=sys.stderr)
        for problem in problems:
            print(f"  {problem}", file=sys.stderr)
        return 1
    print(f"the {args.runner} lane retained evidence for every stage it owes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
