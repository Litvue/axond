#!/usr/bin/env python3
"""Fail a lane whose executable recovery stages left no evidence.

The recovery manifest says which stages run and which lane runs them; the lanes
write one artifact per stage to `target/recovery/`. Between the two sits the
failure mode a retained evidence directory cannot describe on its own: a stage
that did not run leaves nothing, and nothing looks exactly like a lane whose
upload found no files. `if-no-files-found: warn` then turns a recovery claim
that was never exercised into a green tick.

So the manifest is read as the list of artifacts a lane owes:

    ops/check-recovery-evidence.py --runner stateful-tests
    ops/check-recovery-evidence.py --runner restore-drill

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
import json
import os
import sys
import tempfile
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

# The schema both lanes write. Bumped with `EVIDENCE_SCHEMA_VERSION` in
# `crates/gateway/src/qualification/evidence.rs`.
SCHEMA_VERSION = 2


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

    verdicts = artifact.get("gates", []) + artifact.get("checks", [])
    problems.extend(
        f"{key}: {entry['gate']} failed \u2014 expected {entry['bound']}, "
        f"observed {entry['observed']} ({entry['detail']})"
        for entry in verdicts
        if entry.get("outcome") == "failed"
    )
    started = artifact.get("run", {}).get("started_at_unix_ms", 0)
    if since_unix_ms and started < since_unix_ms:
        problems.append(
            f"{key}: the artifact began at {started}, before this run started at "
            f"{since_unix_ms}, so it was left by an earlier run and this stage did not run"
        )
    if not artifact.get("timeline"):
        problems.append(f"{key}: the artifact retains no timeline, so it describes no recovery")
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


def self_test() -> int:
    """Check the checker: each way an artifact can lie has to be caught."""
    runner = "restore-drill"
    scenario, stage = owed(runner)[0]

    def artifact() -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "scenario": scenario["id"],
            "stage": stage["id"],
            "runner": runner,
            "capability": scenario["capability"],
            "evidence": stage["evidence"],
            "run": {"started_at_unix_ms": 2_000},
            "timeline": [{"at_ms": 1, "event": "restored", "detail": "a restore happened"}],
            "observations": {"live_revisions": 5},
            "gates": [],
            "checks": [],
        }

    failed = {
        "gate": "the_restored_head_is_the_backed_up_head",
        "bound": "rev_a",
        "observed": "rev_b",
        "outcome": "failed",
        "detail": "the restore landed on another head",
    }
    # A body given as a string is written verbatim, for the shapes that are not
    # valid JSON in the first place.
    cases: list[tuple[str, dict[str, Any] | str | None, list[str], bool]] = [
        ("a complete artifact", artifact(), [], True),
        ("a missing artifact", None, [], False),
        ("a future schema", {**artifact(), "schema_version": SCHEMA_VERSION + 1}, [], False),
        ("another lane's artifact", {**artifact(), "runner": "stateful-tests"}, [], False),
        (
            "evidence the contract did not grant",
            {**artifact(), "evidence": [*stage["evidence"], "serving_behavior"]},
            [],
            False,
        ),
        ("a failed check", {**artifact(), "checks": [failed]}, [], False),
        ("a failed gate", {**artifact(), "gates": [failed]}, [], False),
        ("an empty timeline", {**artifact(), "timeline": []}, [], False),
        ("a truncated artifact", json.dumps(artifact())[:120], [], False),
        (
            "a leaked credential",
            {**artifact(), "observations": {"note": "the-drill-credential"}},
            ["the-drill-credential"],
            False,
        ),
    ]

    complaints: list[str] = []
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / f"{scenario['id']}.{stage['id']}.json"
        for name, body, forbid, want_ok in cases:
            path.unlink(missing_ok=True)
            if body is not None:
                raw = body if isinstance(body, str) else json.dumps(body)
                path.write_text(raw, encoding="utf-8")
            problems = check(scenario, stage, Path(directory), runner, forbid)
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
            problems = check(scenario, stage, Path(directory), runner, [], since)
            if bool(problems) == want_ok:
                verb = "rejected" if problems else "accepted"
                complaints.append(f"{name}: the checker {verb} it: {'; '.join(problems)}")

    if complaints:
        print("the recovery evidence checker does not catch what it claims:", file=sys.stderr)
        for complaint in complaints:
            print(f"  {complaint}", file=sys.stderr)
        return 1
    print(f"the evidence checker catches {len(cases)} ways an artifact can lie")
    return 0


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
        "--forbid-env",
        action="append",
        default=[],
        metavar="NAME",
        help=(
            "an environment variable whose value no artifact may contain, such as "
            "the drill's credential; unset or empty names are ignored"
        ),
    )
    parser.add_argument(
        "--since-unix-ms",
        type=int,
        default=0,
        help="refuse an artifact that began before this instant, in Unix milliseconds",
    )
    args = parser.parse_args()
    forbid = [os.environ.get(name, "") for name in args.forbid_env]
    if args.self_test:
        return self_test()
    if not args.runner:
        parser.error("--runner is required unless --self-test is given")

    problems: list[str] = []
    for scenario, stage in owed(args.runner):
        problems.extend(
            check(scenario, stage, args.dir, args.runner, forbid, args.since_unix_ms)
        )

    if problems:
        print(f"\nrecovery evidence is not complete for the {args.runner} lane:", file=sys.stderr)
        for problem in problems:
            print(f"  {problem}", file=sys.stderr)
        return 1
    print(f"the {args.runner} lane retained evidence for every stage it owes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
