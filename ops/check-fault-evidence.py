#!/usr/bin/env python3
"""Check that the stateless fault lane left complete, fresh evidence.

The provider and transport rows in ``qualification/faults/manifest.toml`` are
the first bounded qualification slice that needs a post-run evidence gate. The
Rust test writes one JSON artifact per row, but an upload directory on its own
cannot distinguish a row that ran from one that was skipped, left by an older
run, or failed after writing its artifact.

This checker deliberately covers only provider and transport rows. Redis and
Postgres rows belong to the stateful lane and are outside this stateless slice.
It is intended to run after the dedicated fault test:

    python3 ops/check-fault-evidence.py --since-unix-ms "$lane_started_ms"

``--self-test`` exercises the missing, stale, malformed, mismatched, and
failed-artifact cases without needing a gateway or provider credentials.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

# ``tomllib`` is standard from 3.11; the repository's 3.10 ops floor supplies
# the backport through the hash-pinned ops environment.
try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - exercised on Python 3.10
    try:
        import tomli as tomllib  # type: ignore[no-redef]
    except ModuleNotFoundError:  # pragma: no cover - a misprovisioned floor
        raise SystemExit(
            f"this needs a TOML reader: {sys.executable} is "
            f"{sys.version_info.major}.{sys.version_info.minor}, which has no "
            "`tomllib` (3.11+), and the `tomli` backport is not installed"
        ) from None


ROOT = Path(__file__).resolve().parent.parent
MANIFEST = ROOT / "qualification/faults/manifest.toml"
MANIFEST_RELATIVE = "qualification/faults/manifest.toml"
MANIFEST_SCHEMA_VERSION = 1
RESULT_SCHEMA_VERSION = 1
HARNESS = "axond fault matrix harness"
STATELESS_FAMILIES = {"provider", "transport"}


def load_manifest() -> tuple[dict[str, Any], str, str]:
    text = MANIFEST.read_text(encoding="utf-8")
    manifest = tomllib.loads(text)
    if manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        raise SystemExit(
            f"{MANIFEST.relative_to(ROOT)}: unsupported manifest schema "
            f"{manifest.get('schema_version')!r}"
        )
    rows = manifest.get("row", [])
    if not rows:
        raise SystemExit(f"{MANIFEST.relative_to(ROOT)}: no fault rows")
    ids = [row.get("id") for row in rows]
    if any(not isinstance(row_id, str) or not row_id for row_id in ids):
        raise SystemExit(f"{MANIFEST.relative_to(ROOT)}: every row needs an id")
    if len(ids) != len(set(ids)):
        raise SystemExit(f"{MANIFEST.relative_to(ROOT)}: row ids are not unique")
    expected = [row for row in rows if row.get("family") in STATELESS_FAMILIES]
    if not expected:
        raise SystemExit(
            f"{MANIFEST.relative_to(ROOT)}: no provider or transport rows"
        )
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return manifest, text, digest


def get_path(value: dict[str, Any], path: str) -> Any:
    current: Any = value
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def check_artifact(
    row: dict[str, Any],
    artifact_path: Path,
    manifest_digest: str,
    since_unix_ms: int,
    require_clean: bool,
) -> list[str]:
    key = f"{row['family']}/{row['id']}"
    display_path = (
        artifact_path.relative_to(ROOT)
        if artifact_path.is_relative_to(ROOT)
        else artifact_path
    )
    if not artifact_path.exists():
        return [
            f"{key}: {display_path} is missing; the stateless fault row did not "
            "run or did not retain its evidence"
        ]

    problems: list[str] = []
    try:
        artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        return [f"{key}: {display_path} is not readable JSON ({error})"]
    if not isinstance(artifact, dict):
        return [
            f"{key}: {display_path} has JSON type {type(artifact).__name__}, "
            "but an evidence object is required"
        ]

    expected = {
        "schema_version": RESULT_SCHEMA_VERSION,
        "row.id": row["id"],
        "row.family": row["family"],
        "row.fault": row["fault"],
        "run.harness": HARNESS,
        "environment.manifest.path": MANIFEST_RELATIVE,
        "environment.manifest.sha256": manifest_digest,
    }
    for path, want in expected.items():
        got = artifact.get(path) if "." not in path else get_path(artifact, path)
        if got != want:
            problems.append(f"{key}: {path} is {got!r}, expected {want!r}")

    started = get_path(artifact, "run.started_at_unix_ms")
    if not isinstance(started, int) or isinstance(started, bool):
        problems.append(f"{key}: run.started_at_unix_ms is not an integer")
    elif since_unix_ms and started < since_unix_ms:
        problems.append(
            f"{key}: run began at {started}, before this lane started at "
            f"{since_unix_ms}; the artifact is stale"
        )

    for path in (
        "run.elapsed_ms",
        "environment.binary.sha256",
        "environment.source.git_commit",
        "deadline.elapsed_ms",
        "deadline.wall_clock_ms",
        "cleanup.upstream_streams_open_at_end",
    ):
        value = get_path(artifact, path)
        if value is None or (isinstance(value, str) and not value):
            problems.append(f"{key}: evidence is missing {path}")

    if require_clean and get_path(artifact, "environment.source.git_dirty") is not False:
        problems.append(
            f"{key}: environment.source.git_dirty is not false, so the run is "
            "not reproducible from its recorded commit"
        )

    verdicts = artifact.get("verdicts")
    if not isinstance(verdicts, list) or not verdicts:
        problems.append(f"{key}: evidence contains no judged verdicts")
    elif any(
        not isinstance(verdict, dict) or verdict.get("passed") is not True
        for verdict in verdicts
    ):
        problems.append(f"{key}: at least one retained verdict failed")

    missing_metrics = get_path(artifact, "telemetry.metrics_missing")
    if missing_metrics != []:
        problems.append(f"{key}: telemetry.metrics_missing is {missing_metrics!r}")

    leakage = get_path(artifact, "leakage.findings")
    if leakage != []:
        problems.append(f"{key}: leakage.findings is {leakage!r}")

    open_streams = get_path(artifact, "cleanup.upstream_streams_open_at_end")
    if isinstance(open_streams, (int, float)) and open_streams > 0:
        problems.append(f"{key}: {open_streams} upstream streams remained open")
    if get_path(artifact, "cleanup.process_exited_cleanly") is not True:
        problems.append(f"{key}: the gateway did not exit cleanly")

    elapsed = get_path(artifact, "deadline.elapsed_ms")
    wall_clock = get_path(artifact, "deadline.wall_clock_ms")
    if (
        isinstance(elapsed, (int, float))
        and isinstance(wall_clock, (int, float))
        and elapsed > wall_clock
    ):
        problems.append(
            f"{key}: elapsed {elapsed} ms exceeded the row's {wall_clock} ms bound"
        )

    return problems


def check(
    directory: Path,
    rows: list[dict[str, Any]],
    manifest_digest: str,
    since_unix_ms: int,
    require_clean: bool,
) -> list[str]:
    problems: list[str] = []
    for row in rows:
        artifact_path = directory / row["family"] / f"{row['id']}.json"
        problems.extend(
            check_artifact(
                row, artifact_path, manifest_digest, since_unix_ms, require_clean
            )
        )
    return problems


def synthetic_artifact(row: dict[str, Any], manifest_digest: str) -> dict[str, Any]:
    return {
        "schema_version": RESULT_SCHEMA_VERSION,
        "row": {"id": row["id"], "family": row["family"], "fault": row["fault"]},
        "run": {
            "started_at_unix_ms": int(time.time() * 1000),
            "elapsed_ms": 10,
            "harness": HARNESS,
        },
        "environment": {
            "manifest": {"path": MANIFEST_RELATIVE, "sha256": manifest_digest},
            "binary": {"sha256": "binary"},
            "source": {"git_commit": "commit", "git_dirty": False},
        },
        "deadline": {"elapsed_ms": 10, "wall_clock_ms": 100},
        "cleanup": {
            "upstream_streams_open_at_end": 0,
            "process_exited_cleanly": True,
        },
        "telemetry": {"metrics_missing": []},
        "leakage": {"findings": []},
        "verdicts": [{"check": "complete", "passed": True}],
    }


def self_test() -> int:
    manifest, _, manifest_digest = load_manifest()
    rows = [
        row for row in manifest["row"] if row.get("family") in STATELESS_FAMILIES
    ]
    row = rows[0]
    now = int(time.time() * 1000)
    complaints: list[str] = []

    with tempfile.TemporaryDirectory() as raw_directory:
        directory = Path(raw_directory)
        path = directory / row["family"] / f"{row['id']}.json"
        path.parent.mkdir(parents=True)

        def write(body: Any) -> None:
            path.write_text(
                body if isinstance(body, str) else json.dumps(body), encoding="utf-8"
            )

        valid = synthetic_artifact(row, manifest_digest)
        write(valid)
        if problems := check(directory, [row], manifest_digest, now - 1, False):
            complaints.append(f"valid evidence rejected: {problems}")

        cases: list[tuple[str, Any, bool]] = [
            ("missing artifact", None, True),
            ("malformed artifact", "{not valid json", True),
            ("wrong JSON shape", [], True),
            (
                "wrong row",
                {**valid, "row": {**valid["row"], "fault": "wrong"}},
                True,
            ),
            (
                "wrong manifest digest",
                {
                    **valid,
                    "environment": {
                        **valid["environment"],
                        "manifest": {
                            "path": MANIFEST_RELATIVE,
                            "sha256": "wrong",
                        },
                    },
                },
                True,
            ),
            ("failed verdict", {**valid, "verdicts": [{"passed": False}]}, True),
            ("stale artifact", {**valid, "run": {**valid["run"], "started_at_unix_ms": now - 2}}, True),
        ]
        for name, body, should_fail in cases:
            path.unlink(missing_ok=True)
            if body is not None:
                write(body)
            failed = bool(check(directory, [row], manifest_digest, now - 1, False))
            if failed != should_fail:
                complaints.append(f"{name}: checker {'rejected' if failed else 'accepted'} it")

    if complaints:
        print(
            "the stateless fault evidence checker does not catch what it claims:",
            file=sys.stderr,
        )
        for complaint in complaints:
            print(f"  {complaint}", file=sys.stderr)
        return 1
    print(f"the stateless fault evidence checker catches {len(cases) + 1} invalid cases")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="test the checker with synthetic artifacts",
    )
    parser.add_argument("--dir", type=Path, default=ROOT / "target/faults")
    parser.add_argument(
        "--since-unix-ms",
        type=int,
        default=0,
        help="reject an artifact whose run began before this Unix-millisecond timestamp",
    )
    parser.add_argument(
        "--require-clean",
        action="store_true",
        help="also require the run to report a clean source tree",
    )
    args = parser.parse_args()
    if args.self_test:
        return self_test()

    manifest, _, manifest_digest = load_manifest()
    rows = [row for row in manifest["row"] if row.get("family") in STATELESS_FAMILIES]
    problems = check(args.dir, rows, manifest_digest, args.since_unix_ms, args.require_clean)
    if problems:
        print("stateless fault evidence is incomplete:", file=sys.stderr)
        for problem in problems:
            print(f"  {problem}", file=sys.stderr)
        return 1
    print(f"stateless fault evidence is complete: {len(rows)} rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
