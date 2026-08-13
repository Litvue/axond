#!/usr/bin/env python3
"""Record a recovery stage's evidence from a shell drill, in the harness schema.

`ops/restore-drill.sh` qualifies the restore and point-in-time-recovery stages
of `qualification/recovery/manifest.toml`, and the in-process driver in
`crates/gateway/src/qualification/recovery.rs` qualifies the rest. They run in
different lanes for good reasons — one needs a cluster it can promote, the other
needs a link it can cut — but a reader comparing two recoveries should not have
to read two schemas, so both write the same artifact to `target/recovery/`.

This is the shell lane's recorder. A stage opens a log, appends events,
observations, gate verdicts, and required checks as it goes, then finishes:

    ops/recovery-evidence.py start  --log $L --stage backup-restore/restore \\
        --schema live --schema-identity "schema v2 is current"
    ops/recovery-evidence.py mark   --log $L --event severed --detail "..."
    ops/recovery-evidence.py require --log $L --check head_survives \\
        --expected rev_1 --observed rev_1 --detail "..."
    ops/recovery-evidence.py finish --log $L

`finish` writes `target/recovery/<scenario>.<stage>.json` and exits non-zero if
any gate or check failed. The order matters: the artifact is written *before*
the failure is raised, so a stage that fails is a stage a reader can inspect
rather than a missing file. The scenario's capability, evidence classes, and
gate bounds are read from the manifest rather than repeated here, so a lane
cannot quietly claim evidence the contract does not grant it.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

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
EVIDENCE_DIR = ROOT / "target/recovery"

# Kept in step with `EVIDENCE_SCHEMA_VERSION` in
# `crates/gateway/src/qualification/evidence.rs`; the contract test
# `the_two_lanes_write_the_same_artifact_schema` fails when they drift.
SCHEMA_VERSION = 2
RUNNER = "restore-drill"


def manifest_stage(key: str) -> tuple[dict[str, Any], dict[str, Any]]:
    """The scenario and stage a `scenario/stage` key names."""
    scenario_id, _, stage_id = key.partition("/")
    manifest = tomllib.loads(MANIFEST.read_text(encoding="utf-8"))
    for scenario in manifest["scenario"]:
        if scenario["id"] != scenario_id:
            continue
        for stage in scenario["stage"]:
            if stage["id"] == stage_id:
                return scenario, stage
        raise SystemExit(f"{scenario_id} declares no {stage_id} stage")
    raise SystemExit(f"the manifest declares no {scenario_id} scenario")


def append(log: Path, record: dict[str, Any]) -> None:
    with log.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record) + "\n")


def records(log: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in log.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def axond_version() -> str:
    """The build that produced the evidence, as `CARGO_PKG_VERSION` would give it.

    The gateway inherits its version from the workspace, so the crate manifest
    holds `version.workspace = true` rather than a number.
    """
    crate = tomllib.loads((ROOT / "crates/gateway/Cargo.toml").read_text(encoding="utf-8"))
    version = crate["package"]["version"]
    if isinstance(version, dict):
        workspace = tomllib.loads((ROOT / "Cargo.toml").read_text(encoding="utf-8"))
        version = workspace["workspace"]["package"]["version"]
    return str(version)


def observation(value: str, kind: str) -> Any:
    if kind == "count":
        return int(value)
    if kind == "seconds":
        return float(value)
    return value


def verdict(gate: str, bound: str, observed: str, outcome: str, detail: str) -> dict[str, Any]:
    return {
        "gate": gate,
        "bound": bound,
        "observed": observed,
        "outcome": outcome,
        "detail": detail,
    }


def start(args: argparse.Namespace) -> int:
    scenario, stage = manifest_stage(args.stage)
    if stage.get("runner") != RUNNER:
        raise SystemExit(
            f"{args.stage}: the manifest gives this stage to "
            f"{stage.get('runner', 'no lane')}, not to {RUNNER}"
        )
    args.log.parent.mkdir(parents=True, exist_ok=True)
    args.log.write_text("", encoding="utf-8")
    append(
        args.log,
        {
            "kind": "start",
            "at_ms": int(time.time() * 1000),
            "scenario": scenario["id"],
            "stage": stage["id"],
            "capability": scenario["capability"],
            "evidence": stage["evidence"],
            "gate": scenario["gate"],
            "schema": args.schema,
            "schema_identity": " ".join(args.schema_identity.split()),
        },
    )
    return 0


def mark(args: argparse.Namespace) -> int:
    append(
        args.log,
        {
            "kind": "event",
            "at_ms": int(time.time() * 1000),
            "event": args.event,
            "detail": args.detail,
        },
    )
    return 0


def observe(args: argparse.Namespace) -> int:
    append(
        args.log,
        {
            "kind": "observation",
            "key": args.key,
            "value": observation(args.value, args.type),
        },
    )
    return 0


def gate(args: argparse.Namespace) -> int:
    """A manifest gate field this stage evaluated.

    The bound comes from the manifest rather than the caller, so editing the
    contract edits the verdict instead of leaving a literal in the drill.
    """
    start_record = records(args.log)[0]
    bound = start_record["gate"].get(args.gate)
    if bound is None:
        raise SystemExit(f"{args.gate} is not a gate field the manifest declares")
    met = args.met == "true"
    append(
        args.log,
        {
            "kind": "gate",
            **verdict(
                args.gate,
                str(bound),
                args.observed,
                "met" if met else "failed",
                args.detail,
            ),
        },
    )
    return 0


def defer(args: argparse.Namespace) -> int:
    start_record = records(args.log)[0]
    bound = start_record["gate"].get(args.gate)
    if bound is None:
        raise SystemExit(f"{args.gate} is not a gate field the manifest declares")
    append(
        args.log,
        {
            "kind": "gate",
            **verdict(args.gate, str(bound), "not measured", "not_evaluated", args.why),
        },
    )
    return 0


def require(args: argparse.Namespace) -> int:
    """A condition the stage requires, recorded rather than asserted."""
    met = args.expected == args.observed
    append(
        args.log,
        {
            "kind": "check",
            **verdict(
                args.check,
                args.expected,
                args.observed,
                "met" if met else "failed",
                args.detail,
            ),
        },
    )
    return 0


def finish(args: argparse.Namespace) -> int:
    entries = records(args.log)
    head = entries[0]
    started = head["at_ms"]
    timeline = [
        {"at_ms": entry["at_ms"] - started, "event": entry["event"], "detail": entry["detail"]}
        for entry in entries
        if entry["kind"] == "event"
    ]
    observations = {
        entry["key"]: entry["value"] for entry in entries if entry["kind"] == "observation"
    }
    gates = [strip(entry) for entry in entries if entry["kind"] == "gate"]
    checks = [strip(entry) for entry in entries if entry["kind"] == "check"]

    artifact = {
        "schema_version": SCHEMA_VERSION,
        "scenario": head["scenario"],
        "stage": head["stage"],
        "runner": RUNNER,
        "capability": head["capability"],
        "evidence": head["evidence"],
        "run": {
            "started_at_unix_ms": started,
            "elapsed_ms": int(time.time() * 1000) - started,
            "axond_version": axond_version(),
            "control_plane": "postgres",
            "schema": head["schema"],
            "schema_identity": head["schema_identity"],
        },
        "timeline": timeline,
        "observations": observations,
        "gates": gates,
        "checks": checks,
    }

    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    path = EVIDENCE_DIR / f"{head['scenario']}.{head['stage']}.json"
    path.write_text(json.dumps(artifact, indent=2) + "\n", encoding="utf-8")

    failures = [entry for entry in gates + checks if entry["outcome"] == "failed"]
    evaluated = sum(1 for entry in gates if entry["outcome"] != "not_evaluated")
    print(
        f"{head['scenario']}/{head['stage']}: {len(timeline)} events, "
        f"{len(observations)} observations, {evaluated}/{len(gates)} gates evaluated, "
        f"{len(checks)} checks, {len(failures)} failed -> {path}"
    )
    for failure in failures:
        print(
            f"  FAILED {failure['gate']}: expected {failure['bound']}, "
            f"observed {failure['observed']} ({failure['detail']})",
            file=sys.stderr,
        )
    return 1 if failures else 0


def strip(entry: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in entry.items() if key != "kind"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)

    def with_log(sub: argparse.ArgumentParser) -> argparse.ArgumentParser:
        sub.add_argument("--log", type=Path, required=True)
        return sub

    opened = with_log(subcommands.add_parser("start"))
    opened.add_argument("--stage", required=True, help="`scenario/stage`")
    opened.add_argument("--schema", required=True, help="the database the stage ran against")
    opened.add_argument("--schema-identity", required=True, help="what migrate status reported")
    opened.set_defaults(handler=start)

    marked = with_log(subcommands.add_parser("mark"))
    marked.add_argument("--event", required=True)
    marked.add_argument("--detail", required=True)
    marked.set_defaults(handler=mark)

    observed = with_log(subcommands.add_parser("observe"))
    observed.add_argument("--key", required=True)
    observed.add_argument("--value", required=True)
    observed.add_argument("--type", choices=("text", "count", "seconds"), default="text")
    observed.set_defaults(handler=observe)

    gated = with_log(subcommands.add_parser("gate"))
    gated.add_argument("--gate", required=True)
    gated.add_argument("--observed", required=True)
    gated.add_argument("--met", choices=("true", "false"), required=True)
    gated.add_argument("--detail", required=True)
    gated.set_defaults(handler=gate)

    deferred = with_log(subcommands.add_parser("defer"))
    deferred.add_argument("--gate", required=True)
    deferred.add_argument("--why", required=True)
    deferred.set_defaults(handler=defer)

    required = with_log(subcommands.add_parser("require"))
    required.add_argument("--check", required=True)
    required.add_argument("--expected", required=True)
    required.add_argument("--observed", required=True)
    required.add_argument("--detail", required=True)
    required.set_defaults(handler=require)

    finished = with_log(subcommands.add_parser("finish"))
    finished.set_defaults(handler=finish)

    args = parser.parse_args()
    return int(args.handler(args))


if __name__ == "__main__":
    sys.exit(main())
