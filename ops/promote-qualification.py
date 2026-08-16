#!/usr/bin/env python3
"""Validate and copy a generated qualification record into the packet.

The CI jobs publish records as workflow artifacts. This command is the narrow
promotion boundary from that disposable artifact into the committed packet:
it checks the current manifest digest, the slice's heavy tier, complete
workload coverage, passing verdicts, and clean-tree provenance before copying.
It deliberately does not edit ``qualification/packet.toml``; changing a
slice's status is a reviewed claim and the packet test remains the final gate.

Usage:
    ops/promote-qualification.py target/qualification-records/rollout-heavy.toml \
        --artifacts target/rollout/heavy \
        --out qualification/rollout/evidence/heavy-ci.toml
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import shutil
import sys
from pathlib import Path
from typing import Any

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10 ops floor
    import tomli as tomllib  # type: ignore[no-redef]


ROOT = Path(__file__).resolve().parent.parent
PACKET_PATH = ROOT / "qualification/packet.toml"
ENDURANCE_RESULT_SCHEMA_VERSION = 3
ENDURANCE_SURPLUS_VERDICT = "max_unexpected_usage_records"


def manifest_workloads(slice_id: str, manifest: dict[str, Any]) -> set[str]:
    if slice_id == "fault":
        return {row["id"] for row in manifest["row"]}
    if slice_id == "rollout":
        return {scenario["id"] for scenario in manifest["scenario"]}
    if slice_id == "recovery":
        return {
            f"{scenario['id']}/{stage['id']}"
            for scenario in manifest["scenario"]
            for stage in scenario["stage"]
            if stage["status"] == "executable"
        }
    return {profile["id"] for profile in manifest["profile"]}


def manifest_endurance_duration(manifest: dict[str, Any]) -> int:
    profiles = manifest.get("profile", [])
    if len(profiles) != 1 or "soak" not in profiles[0]:
        fail("the endurance manifest has no single committed soak duration")
    return profiles[0]["soak"]["duration_ms"]


def fail(message: str) -> None:
    raise SystemExit(f"record refused: {message}")


def artifact_digests(record: dict[str, Any]) -> set[str]:
    """Return the raw artifact identities the compact record claims."""
    slice_id = record.get("slice")
    if slice_id == "capacity":
        rows = record.get("profile", [])
    elif slice_id == "recovery":
        rows = record.get("stage", [])
    else:
        rows = record.get("observation", [])
    return {
        row["artifact_sha256"]
        for row in rows
        if isinstance(row, dict) and row.get("artifact_sha256")
    }


def verify_raw_artifacts(record: dict[str, Any], directory: Path) -> None:
    """Bind every compact-record digest to the raw JSON supplied for promotion."""
    if not directory.is_dir():
        fail(f"raw artifact directory does not exist: {directory}")
    expected = artifact_digests(record)
    if not expected:
        fail("the record has no raw artifact digests to verify")
    actual: set[str] = set()
    endurance_bounds: dict[str, int] = {}
    endurance_rows: dict[str, dict[str, Any]] = {}
    if record.get("slice") == "endurance":
        manifest_relative = record.get("inputs", {}).get("manifest")
        manifest = tomllib.loads((ROOT / manifest_relative).read_text(encoding="utf-8"))
        endurance_bounds = {
            row["id"]: row[record["tier"]]["thresholds"][ENDURANCE_SURPLUS_VERDICT]
            for row in manifest["profile"]
        }
        endurance_rows = {
            row["artifact_sha256"]: row for row in record.get("observation", [])
        }
    for path in directory.rglob("*.json"):
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        actual.add(digest)
        if record.get("slice") == "endurance" and digest in endurance_rows:
            result = json.loads(path.read_text(encoding="utf-8"))
            row = endurance_rows[digest]
            workload = row.get("id")
            if workload not in endurance_bounds:
                fail(f"{path}: raw endurance artifact names unknown workload {workload!r}")
            validate_raw_endurance(
                result, endurance_bounds[workload], str(path), record, row
            )
    missing = expected - actual
    unexpected = actual - expected
    if missing or unexpected:
        details = []
        if missing:
            details.append(f"missing {len(missing)} claimed artifact(s)")
        if unexpected:
            details.append(f"found {len(unexpected)} unclaimed artifact(s)")
        fail("raw artifact set does not match the record: " + ", ".join(details))


def verify_promotion_artifacts(record: dict[str, Any], directory: Path | None) -> None:
    """Require the raw directory whenever the compact record claims identities."""
    if artifact_digests(record) and directory is None:
        fail("the record claims raw artifact digests but --artifacts was not supplied")
    if directory is not None:
        verify_raw_artifacts(record, directory)


def validate_raw_endurance(
    result: dict[str, Any],
    expected_bound: int,
    label: str,
    record: dict[str, Any],
    row: dict[str, Any],
) -> None:
    """Require the raw result contract that evaluates surplus accounting."""
    if result.get("schema_version") != ENDURANCE_RESULT_SCHEMA_VERSION:
        fail(
            f"{label}: unsupported endurance artifact schema "
            f"{result.get('schema_version')!r}"
        )
    unexpected = result.get("reconciliation", {}).get("unexpected_records")
    if not isinstance(unexpected, int) or unexpected < 0:
        fail(f"{label}: endurance reconciliation has no surplus identity count")
    verdicts = result.get("verdicts")
    if not isinstance(verdicts, list) or not verdicts or any(
        verdict.get("passed") is not True for verdict in verdicts
    ):
        fail(f"{label}: the raw endurance artifact has a failed or missing verdict")
    surplus = [
        verdict
        for verdict in verdicts
        if verdict.get("threshold") == ENDURANCE_SURPLUS_VERDICT
    ]
    if len(surplus) != 1:
        fail(f"{label}: the raw endurance artifact did not evaluate the surplus usage gate")
    verdict = surplus[0]
    if (
        verdict.get("comparison") != "<="
        or verdict.get("value") != unexpected
        or verdict.get("bound") != expected_bound
        or verdict.get("passed") is not (unexpected <= expected_bound)
        or unexpected > expected_bound
    ):
        fail(f"{label}: the raw surplus verdict does not match reconciliation")
    if len(verdicts) != row.get("verdicts"):
        fail(f"{label}: raw verdict count does not match the compact observation")

    profile = result.get("profile", {})
    run = result.get("run", {})
    environment = result.get("environment", {})
    expected_fields = {
        "profile.id": (profile.get("id"), row.get("id")),
        "profile.tier": (profile.get("tier"), record.get("tier")),
        "profile.duration_ms": (profile.get("duration_ms"), row.get("duration_ms")),
        "profile.manifest_duration_ms": (
            profile.get("manifest_duration_ms"),
            row.get("manifest_duration_ms"),
        ),
        "profile.thresholds.max_unexpected_usage_records": (
            profile.get("thresholds", {}).get(ENDURANCE_SURPLUS_VERDICT),
            expected_bound,
        ),
        "run.elapsed_ms": (run.get("elapsed_ms"), row.get("elapsed_ms")),
        "run.requested_duration_ms": (
            run.get("requested_duration_ms"),
            row.get("requested_duration_ms"),
        ),
        "run.duration_source": (
            run.get("duration_source"),
            row.get("duration_source"),
        ),
        "environment.source": (environment.get("source"), record.get("source")),
        "environment.binary.sha256": (
            environment.get("binary", {}).get("sha256"),
            record.get("binary", {}).get("sha256"),
        ),
        "environment.binary.version": (
            environment.get("binary", {}).get("version"),
            record.get("binary", {}).get("version"),
        ),
        "environment.toolchain.cargo_profile": (
            environment.get("toolchain", {}).get("cargo_profile"),
            record.get("binary", {}).get("cargo_profile"),
        ),
        "environment.toolchain.rustc": (
            environment.get("toolchain", {}).get("rustc"),
            record.get("binary", {}).get("rustc"),
        ),
        "environment.manifest.path": (
            environment.get("manifest", {}).get("path"),
            record.get("inputs", {}).get("manifest"),
        ),
        "environment.manifest.sha256": (
            environment.get("manifest", {}).get("sha256"),
            record.get("inputs", {}).get("manifest_sha256"),
        ),
    }
    for field, (actual, expected) in expected_fields.items():
        if actual != expected:
            fail(f"{label}: raw {field} does not match the compact record")
    if run.get("requested_duration_ms") != profile.get("duration_ms"):
        fail(f"{label}: raw requested and offered durations disagree")
    if run.get("elapsed_ms", 0) < profile.get("duration_ms", 0):
        fail(f"{label}: raw elapsed time is shorter than the offered duration")


def validate(record: dict[str, Any]) -> None:
    if record.get("schema_version") != 1:
        fail(f"unsupported record schema {record.get('schema_version')!r}")
    slice_id = record.get("slice")
    if not isinstance(slice_id, str):
        fail("the record has no slice")

    packet = tomllib.loads(PACKET_PATH.read_text(encoding="utf-8"))
    slice_rows = [row for row in packet["slice"] if row["id"] == slice_id]
    if len(slice_rows) != 1:
        fail(f"{slice_id!r} is not a committed packet slice")
    slice_row = slice_rows[0]
    heavy_tier = slice_row["heavy_tier"]
    if record.get("tier") != heavy_tier:
        fail(
            f"{slice_id} record is tier {record.get('tier')!r}, "
            f"but the packet promotes only {heavy_tier!r}"
        )

    inputs = record.get("inputs", {})
    manifest_relative = slice_row["manifest"]
    if inputs.get("manifest") != manifest_relative:
        fail(
            f"the record names {inputs.get('manifest')!r}, "
            f"not the slice manifest {manifest_relative!r}"
        )
    manifest_path = ROOT / manifest_relative
    manifest_bytes = manifest_path.read_bytes()
    manifest_text = manifest_bytes.decode("utf-8")
    expected_digest = hashlib.sha256(manifest_bytes).hexdigest()
    if inputs.get("manifest_sha256") != expected_digest:
        fail("the record's manifest digest is stale")

    source = record.get("source", {})
    if source.get("git_dirty") is not False:
        fail("the source tree was dirty")
    if not source.get("git_commit"):
        fail("the record has no source commit")
    if not record.get("binary", {}).get("sha256"):
        fail("the record has no binary digest")

    manifest = tomllib.loads(manifest_text)
    expected = manifest_workloads(slice_id, manifest)
    expected_recovery_runners = {
        f"{scenario['id']}/{stage['id']}": stage.get("runner", "")
        for scenario in manifest.get("scenario", [])
        for stage in scenario.get("stage", [])
        if stage.get("status") == "executable"
    }
    profiles = record.get("profile", [])
    observations = record.get("observation", [])
    stages = record.get("stage", [])
    rows = profiles if slice_id == "capacity" else stages if slice_id == "recovery" else observations
    if slice_id == "capacity" and observations:
        fail("capacity records must use profile rows")
    if slice_id == "recovery" and (profiles or observations):
        fail("recovery records must use stage rows")
    if slice_id not in ("capacity", "recovery") and profiles:
        fail("non-capacity records must use observation rows")
    ids = [row.get("id") for row in rows]
    if set(ids) != expected or len(ids) != len(expected):
        fail(
            f"workload coverage is {sorted(set(ids))}, expected {sorted(expected)}"
        )
    for row in rows:
        if not row.get("passed") or row.get("verdicts", 0) <= 0:
            fail(f"{row.get('id')}: a retained workload did not pass its verdicts")
        if row.get("elapsed_ms", 0) <= 0:
            fail(f"{row.get('id')}: the recorded duration is not positive")
        if slice_id != "capacity" and not row.get("artifact_sha256"):
            fail(f"{row.get('id')}: the raw artifact digest is missing")
        if slice_id == "recovery" and not row.get("runner"):
            fail(f"{row.get('id')}: the recovery lane is missing")
        if slice_id == "recovery" and row.get("runner") != expected_recovery_runners.get(
            row.get("id")
        ):
            fail(f"{row.get('id')}: the recovery lane does not match the manifest")
        if slice_id == "endurance":
            if row.get("artifact_schema_version") != ENDURANCE_RESULT_SCHEMA_VERSION:
                fail(
                    f"{row.get('id')}: the compact record does not bind result "
                    f"schema {ENDURANCE_RESULT_SCHEMA_VERSION}"
                )
            required_duration = manifest_endurance_duration(manifest)
            if row.get("manifest_duration_ms") != required_duration:
                fail(
                    f"{row.get('id')}: the record does not name the committed "
                    "soak duration"
                )
            offered_duration = row.get("duration_ms")
            if not isinstance(offered_duration, int) or offered_duration < required_duration:
                fail(
                    f"{row.get('id')}: the run offered {offered_duration!r} ms, "
                    f"but promotion requires at least {required_duration} ms"
                )
            if row.get("requested_duration_ms") != offered_duration:
                fail(f"{row.get('id')}: requested and offered soak durations disagree")
            if row.get("elapsed_ms", 0) < offered_duration:
                fail(
                    f"{row.get('id')}: only {row.get('elapsed_ms', 0)} ms elapsed "
                    f"during a {offered_duration} ms soak"
                )
            if not row.get("duration_source"):
                fail(f"{row.get('id')}: the soak duration provenance is incomplete")


def self_test() -> int:
    path = ROOT / "qualification/capacity/evidence/heavy-local.toml"
    record = tomllib.loads(path.read_text(encoding="utf-8"))
    validate(record)

    cases = {
        "dirty": ("source", "git_dirty", True),
        "wrong tier": (None, "tier", "reduced"),
        "stale manifest": ("inputs", "manifest_sha256", "stale"),
    }
    for label, (section, key, value) in cases.items():
        candidate = copy.deepcopy(record)
        target = candidate if section is None else candidate[section]
        target[key] = value
        try:
            validate(candidate)
        except SystemExit:
            pass
        else:
            raise AssertionError(f"a {label} record was accepted")

    partial = copy.deepcopy(record)
    partial["profile"] = partial["profile"][:-1]
    try:
        validate(partial)
    except SystemExit:
        pass
    else:
        raise AssertionError("a partial workload set was accepted")

    endurance_manifest = tomllib.loads(
        (ROOT / "qualification/endurance/manifest.toml").read_text(encoding="utf-8")
    )
    endurance_duration = manifest_endurance_duration(endurance_manifest)
    short_endurance = {
        "schema_version": 1,
        "slice": "endurance",
        "tier": "soak",
        "source": {
            "git_commit": "commit",
            "git_dirty": False,
            "crate_version": "0.0.0",
        },
        "binary": {
            "sha256": "binary",
            "version": "0.0.0",
            "cargo_profile": "release",
            "rustc": "rustc test",
        },
        "inputs": {
            "manifest": "qualification/endurance/manifest.toml",
            "manifest_sha256": hashlib.sha256(
                (ROOT / "qualification/endurance/manifest.toml").read_bytes()
            ).hexdigest(),
        },
        "observation": [
            {
                "id": "mixed-endurance",
                "artifact_sha256": "artifact",
                "elapsed_ms": 1,
                "verdicts": 1,
                "passed": True,
                "duration_ms": endurance_duration - 1,
                "manifest_duration_ms": endurance_duration,
                "requested_duration_ms": endurance_duration - 1,
                "duration_source": "environment",
                "artifact_schema_version": ENDURANCE_RESULT_SCHEMA_VERSION,
            }
        ],
    }
    try:
        validate(short_endurance)
    except SystemExit:
        pass
    else:
        raise AssertionError("a shortened endurance soak was accepted")

    full_endurance = copy.deepcopy(short_endurance)
    full_row = full_endurance["observation"][0]
    full_row["duration_ms"] = endurance_duration
    full_row["requested_duration_ms"] = endurance_duration
    full_row["elapsed_ms"] = endurance_duration
    validate(full_endurance)
    try:
        verify_promotion_artifacts(full_endurance, None)
    except SystemExit:
        pass
    else:
        raise AssertionError("an endurance record was accepted without its raw artifacts")
    stale_endurance = copy.deepcopy(full_endurance)
    stale_endurance["observation"][0]["artifact_schema_version"] = (
        ENDURANCE_RESULT_SCHEMA_VERSION - 1
    )
    try:
        validate(stale_endurance)
    except SystemExit:
        pass
    else:
        raise AssertionError("a compact record naming a stale endurance schema was accepted")
    short_elapsed_endurance = copy.deepcopy(full_endurance)
    short_elapsed_endurance["observation"][0]["elapsed_ms"] = endurance_duration - 1
    try:
        validate(short_elapsed_endurance)
    except SystemExit:
        pass
    else:
        raise AssertionError("a compact endurance record with short elapsed time was accepted")

    recovery_manifest = tomllib.loads(
        (ROOT / "qualification/recovery/manifest.toml").read_text(encoding="utf-8")
    )
    recovery_rows = [
        {
            "id": f"{scenario['id']}/{stage['id']}",
            "runner": stage["runner"],
            "artifact_sha256": "artifact",
            "elapsed_ms": 1,
            "verdicts": 1,
            "passed": True,
        }
        for scenario in recovery_manifest["scenario"]
        for stage in scenario["stage"]
        if stage["status"] == "executable"
    ]
    recovery_record = {
        "schema_version": 1,
        "slice": "recovery",
        "tier": "serving",
        "source": {"git_commit": "commit", "git_dirty": False},
        "binary": {"sha256": "binary"},
        "inputs": {
            "manifest": "qualification/recovery/manifest.toml",
            "manifest_sha256": hashlib.sha256(
                (ROOT / "qualification/recovery/manifest.toml").read_bytes()
            ).hexdigest(),
        },
        "stage": recovery_rows,
    }
    validate(recovery_record)
    partial_recovery = copy.deepcopy(recovery_record)
    partial_recovery["stage"] = partial_recovery["stage"][:-1]
    try:
        validate(partial_recovery)
    except SystemExit:
        pass
    else:
        raise AssertionError("a partial recovery record was accepted")

    import tempfile

    with tempfile.TemporaryDirectory() as directory:
        raw = Path(directory) / "stage.json"
        raw.write_text('{"stage":"raw"}', encoding="utf-8")
        digest = hashlib.sha256(raw.read_bytes()).hexdigest()
        compact = {"slice": "recovery", "stage": [{"artifact_sha256": digest}]}
        verify_promotion_artifacts(compact, Path(directory))
        raw.write_text('{"stage":"changed"}', encoding="utf-8")
        try:
            verify_promotion_artifacts(compact, Path(directory))
        except SystemExit:
            pass
        else:
            raise AssertionError("a changed raw artifact was accepted")

    with tempfile.TemporaryDirectory() as directory:
        raw = Path(directory) / "mixed-endurance.json"
        raw_result = {
            "schema_version": ENDURANCE_RESULT_SCHEMA_VERSION,
            "profile": {
                "id": "mixed-endurance",
                "tier": "soak",
                "duration_ms": endurance_duration,
                "manifest_duration_ms": endurance_duration,
                "thresholds": {ENDURANCE_SURPLUS_VERDICT: 0},
            },
            "run": {
                "elapsed_ms": endurance_duration,
                "requested_duration_ms": endurance_duration,
                "duration_source": "environment",
            },
            "environment": {
                "source": copy.deepcopy(full_endurance["source"]),
                "binary": {
                    "sha256": full_endurance["binary"]["sha256"],
                    "version": full_endurance["binary"]["version"],
                },
                "toolchain": {
                    "cargo_profile": full_endurance["binary"]["cargo_profile"],
                    "rustc": full_endurance["binary"]["rustc"],
                },
                "manifest": {
                    "path": full_endurance["inputs"]["manifest"],
                    "sha256": full_endurance["inputs"]["manifest_sha256"],
                },
            },
            "reconciliation": {"unexpected_records": 0},
            "verdicts": [
                {
                    "threshold": ENDURANCE_SURPLUS_VERDICT,
                    "comparison": "<=",
                    "value": 0,
                    "bound": 0,
                    "passed": True,
                }
            ],
        }
        raw.write_text(json.dumps(raw_result), encoding="utf-8")
        compact = copy.deepcopy(full_endurance)
        compact["observation"][0]["artifact_sha256"] = hashlib.sha256(
            raw.read_bytes()
        ).hexdigest()
        verify_promotion_artifacts(compact, Path(directory))

        raw_result["schema_version"] = ENDURANCE_RESULT_SCHEMA_VERSION - 1
        raw.write_text(json.dumps(raw_result), encoding="utf-8")
        compact["observation"][0]["artifact_sha256"] = hashlib.sha256(
            raw.read_bytes()
        ).hexdigest()
        try:
            verify_promotion_artifacts(compact, Path(directory))
        except SystemExit:
            pass
        else:
            raise AssertionError("a stale raw endurance result schema was accepted")

        raw_result["schema_version"] = ENDURANCE_RESULT_SCHEMA_VERSION
        raw_result["profile"]["duration_ms"] = endurance_duration - 1
        raw.write_text(json.dumps(raw_result), encoding="utf-8")
        compact["observation"][0]["artifact_sha256"] = hashlib.sha256(
            raw.read_bytes()
        ).hexdigest()
        try:
            verify_promotion_artifacts(compact, Path(directory))
        except SystemExit:
            pass
        else:
            raise AssertionError("a shortened raw endurance artifact was accepted")

        raw_result["profile"]["duration_ms"] = endurance_duration
        raw_result["profile"]["tier"] = "smoke"
        raw.write_text(json.dumps(raw_result), encoding="utf-8")
        compact["observation"][0]["artifact_sha256"] = hashlib.sha256(
            raw.read_bytes()
        ).hexdigest()
        try:
            verify_promotion_artifacts(compact, Path(directory))
        except SystemExit:
            pass
        else:
            raise AssertionError("a mismatched raw endurance tier was accepted")

        raw_result["profile"]["tier"] = "soak"
        raw_result["run"]["elapsed_ms"] = endurance_duration - 1
        compact["observation"][0]["elapsed_ms"] = endurance_duration - 1
        raw.write_text(json.dumps(raw_result), encoding="utf-8")
        compact["observation"][0]["artifact_sha256"] = hashlib.sha256(
            raw.read_bytes()
        ).hexdigest()
        try:
            verify_promotion_artifacts(compact, Path(directory))
        except SystemExit:
            pass
        else:
            raise AssertionError("a raw endurance artifact with short elapsed time was accepted")

        raw_result["run"]["elapsed_ms"] = endurance_duration
        compact["observation"][0]["elapsed_ms"] = endurance_duration
        raw_result["reconciliation"]["unexpected_records"] = 1
        raw_result["verdicts"][0].update(value=1, passed=True)
        raw.write_text(json.dumps(raw_result), encoding="utf-8")
        compact["observation"][0]["artifact_sha256"] = hashlib.sha256(
            raw.read_bytes()
        ).hexdigest()
        try:
            verify_promotion_artifacts(compact, Path(directory))
        except SystemExit:
            pass
        else:
            raise AssertionError("a false passing raw surplus verdict was accepted")
    print("qualification promotion self-test passed")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("record", nargs="?", type=Path)
    parser.add_argument(
        "--artifacts",
        type=Path,
        help="directory containing the raw JSON artifacts named by the record",
    )
    parser.add_argument("--out", type=Path)
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        return self_test()
    if args.record is None or args.out is None:
        parser.error("record and --out are required unless --self-test is used")

    record = tomllib.loads(args.record.read_text(encoding="utf-8"))
    validate(record)
    verify_promotion_artifacts(record, args.artifacts)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(args.record, args.out)
    print(f"promoted {args.record} -> {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
