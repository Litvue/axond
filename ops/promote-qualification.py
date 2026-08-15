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
    for path in directory.rglob("*.json"):
        actual.add(hashlib.sha256(path.read_bytes()).hexdigest())
    missing = expected - actual
    unexpected = actual - expected
    if missing or unexpected:
        details = []
        if missing:
            details.append(f"missing {len(missing)} claimed artifact(s)")
        if unexpected:
            details.append(f"found {len(unexpected)} unclaimed artifact(s)")
        fail("raw artifact set does not match the record: " + ", ".join(details))


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
            if row.get("requested_duration_ms", 0) <= 0 or not row.get(
                "duration_source"
            ):
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
        "source": {"git_commit": "commit", "git_dirty": False},
        "binary": {"sha256": "binary"},
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
            }
        ],
    }
    try:
        validate(short_endurance)
    except SystemExit:
        pass
    else:
        raise AssertionError("a shortened endurance soak was accepted")

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
        verify_raw_artifacts(compact, Path(directory))
        raw.write_text('{"stage":"changed"}', encoding="utf-8")
        try:
            verify_raw_artifacts(compact, Path(directory))
        except SystemExit:
            pass
        else:
            raise AssertionError("a changed raw artifact was accepted")
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
    if args.artifacts is not None:
        verify_raw_artifacts(record, args.artifacts)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(args.record, args.out)
    print(f"promoted {args.record} -> {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
