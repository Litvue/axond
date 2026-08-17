#!/usr/bin/env python3
"""Turn qualification result artifacts into a retained evidence record.

The artifacts a run writes under `target/capacity/` are complete but verbose,
host-specific, and thrown away with the build directory. A qualification packet
needs the opposite: a small committed record a reader can compare two of, which
keeps the provenance that makes the comparison legitimate and drops the parts
that only describe where the run happened to be built.

Usage:
    ops/qualification-evidence.py target/capacity/heavy \\
        --runner local \\
        --note "8 vCPU cloud VM, debug build" \\
        --out qualification/capacity/evidence/heavy-local.toml

    ops/qualification-evidence.py target/faults \\
        --slice fault --tier full --runner github-actions \\
        --note "GitHub Actions ubuntu-latest" \\
        --out target/qualification-records/fault-full.toml

    ops/qualification-evidence.py target/recovery \\
        --slice recovery --tier serving --runner github-actions \\
        --binary target/recovery-binaries/stateful/axond \\
        --note "GitHub Actions stateful-tests plus restore-drill" \\
        --out target/qualification-records/recovery-serving.toml

    ops/qualification-evidence.py target/stateful-endurance/soak \\
        --slice stateful-endurance --tier soak --runner github-actions \\
        --note "GitHub Actions stateful endurance soak" \\
        --out target/qualification-records/stateful-endurance-soak.toml

Every field written here comes from the artifacts; nothing is supplied by hand
except the runner classification and its note, which say where the run happened
and are the reader's warning about what may be compared with what.

Two runs are refused rather than written, because a record carrying either is
worse than no record: artifacts that disagree about their provenance — `target/`
survives across commits, so a leftover result sorts in beside a fresh one — and
a run whose provenance the harness could not determine, which would otherwise be
rendered as a null and read back as broken TOML.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Callable

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10 ops floor
    import tomli as tomllib  # type: ignore[no-redef]


ROOT = Path(__file__).resolve().parent.parent


def toml_string(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def number(value: float, digits: int = 1) -> str:
    return f"{value:.{digits}f}"


def load_results(directory: Path, recursive: bool = False) -> list[dict]:
    paths = directory.rglob("*.json") if recursive else directory.glob("*.json")
    results = []
    for path in sorted(paths):
        result = json.loads(path.read_text(encoding="utf-8"))
        # Writer metadata, never part of the result contract: it binds a
        # compact observation to the raw artifact retained by the workflow.
        result["_artifact_path"] = str(path)
        results.append(result)
    if not results:
        raise SystemExit(f"{directory}: no result artifacts to retain")
    return results


# What a record has to carry to be evidence. Every one of these is optional in
# the result schema — a host without `/proc`, a checkout without git, a sampler
# that lost its subject — and a record missing any of them says nothing another
# record could be compared against.
REQUIRED: dict[str, Callable[[dict], Any]] = {
    "the commit it ran": lambda result: result["environment"]["source"]["git_commit"],
    "whether the tree was clean": lambda result: result["environment"]["source"]["git_dirty"],
    "the compiler": lambda result: result["environment"]["toolchain"]["rustc"],
    "the kernel": lambda result: result["environment"]["hardware"]["kernel"],
    "the CPU model": lambda result: result["environment"]["hardware"]["cpu_model"],
    "the memory size": lambda result: result["environment"]["hardware"]["total_memory_kib"],
    "resident memory": lambda result: result["resources"]["rss_kib"],
    "the socket count": lambda result: result["resources"]["sockets"],
    "CPU time": lambda result: result["resources"]["cpu_seconds"],
}

# The provenance every artifact in one record has to agree about. A difference
# here means two runs, not one, and the numbers must not be pooled under the
# first artifact's environment.
PROVENANCE: dict[str, Callable[[dict], Any]] = {
    "the tier": lambda result: result["profile"]["tier"],
    "the source": lambda result: result["environment"]["source"],
    "the binary": lambda result: result["environment"]["binary"]["sha256"],
    "the toolchain": lambda result: result["environment"]["toolchain"],
    "the manifest": lambda result: result["environment"]["manifest"],
    # Not the config: a profile boots the process it needs — `mixed` adds a
    # second credential per provider — so the booted config is per profile and
    # is recorded there.
    "the fixtures": lambda result: result["environment"]["fixtures"],
    "the hardware": lambda result: result["environment"]["hardware"],
}


def check_complete(results: list[dict]) -> None:
    for result in results:
        profile = result["profile"]["id"]
        for name, reader in REQUIRED.items():
            if reader(result) is None:
                raise SystemExit(
                    f"{profile}: the run did not record {name}, so a record of it "
                    "would not be reproducible"
                )


def check_one_run(results: list[dict]) -> None:
    for name, reader in PROVENANCE.items():
        values = {json.dumps(reader(result), sort_keys=True) for result in results}
        if len(values) != 1:
            raise SystemExit(
                f"the artifacts disagree about {name}, so they are not one run "
                f"({len(values)} values). Remove the stale ones and re-run."
            )


def render(results: list[dict], runner: str, note: str) -> str:
    check_complete(results)
    check_one_run(results)

    environment = results[0]["environment"]
    hardware = environment["hardware"]
    lines = [
        "# A retained capacity run: what one replica did, and on what.",
        "#",
        "# Written by ops/qualification-evidence.py from the artifacts of a single",
        "# run; see docs/operations/qualification.md for how a record is read and",
        "# docs/operations/capacity.md for what the numbers mean. Two records may",
        "# only be compared when their provenance matches — the digests and the",
        "# hardware below are what makes that checkable rather than assumed.",
        "#",
        "# The digests are the record's identity: binary.sha256, inputs.",
        "# manifest_sha256, and each profile's config_sha256 are content-addressed",
        "# and outlive any rewriting of history. source.git_commit is the branch",
        "# commit the run happened on, which a squash merge does not land — read",
        "# it as a note about the run, not as something to check out.",
        "",
        "schema_version = 1",
        'slice = "capacity"',
        f"tier = {toml_string(results[0]['profile']['tier'])}",
        f"runner = {toml_string(runner)}",
        f"runner_note = {toml_string(note)}",
        "",
        "[source]",
        f"git_commit = {toml_string(environment['source']['git_commit'])}",
        f"git_dirty = {str(environment['source']['git_dirty']).lower()}",
        f"crate_version = {toml_string(environment['source']['crate_version'])}",
        "",
        "[binary]",
        f"sha256 = {toml_string(environment['binary']['sha256'])}",
        f"version = {toml_string(environment['binary']['version'])}",
        f"cargo_profile = {toml_string(environment['toolchain']['cargo_profile'])}",
        f"rustc = {toml_string(environment['toolchain']['rustc'])}",
        "",
        "[inputs]",
        f"manifest = {toml_string(environment['manifest']['path'])}",
        f"manifest_sha256 = {toml_string(environment['manifest']['sha256'])}",
        f"fixtures = {len(environment['fixtures'])}",
        "",
        "[hardware]",
        f"os = {toml_string(hardware['os'])}",
        f"arch = {toml_string(hardware['arch'])}",
        f"kernel = {toml_string(hardware['kernel'])}",
        f"cpu_model = {toml_string(hardware['cpu_model'])}",
        f"cpus = {hardware['cpus']}",
        f"total_memory_kib = {hardware['total_memory_kib']}",
        f"containerized = {str(hardware['containerized']).lower()}",
    ]

    for result in sorted(results, key=lambda result: result["profile"]["id"]):
        profile = result["profile"]
        throughput = result["throughput"]
        latency = result["latency_ms"]
        resources = result["resources"]
        usage = result["usage_records"]
        ttft = result.get("ttft_ms")
        rss = resources["rss_kib"]
        lines += [
            "",
            "[[profile]]",
            f"id = {toml_string(profile['id'])}",
            f"concurrency = {profile['concurrency']}",
            f"config_sha256 = {toml_string(result['environment']['config']['sha256'])}",
            f"requests = {profile['requests']}",
            f"offered = {throughput['offered']}",
            f"accepted = {throughput['accepted']}",
            f"rejected = {throughput['rejected']}",
            f"errors = {throughput['errors']}",
            f"elapsed_ms = {throughput['elapsed_ms']}",
            f"accepted_rps = {number(throughput['accepted_rps'])}",
            f"latency_p50_ms = {number(latency['p50'], 2)}",
            f"latency_p95_ms = {number(latency['p95'], 2)}",
            f"latency_p99_ms = {number(latency['p99'], 2)}",
        ]
        if ttft:
            lines.append(f"ttft_p95_ms = {number(ttft['p95'], 2)}")
        # What the profile was built to show, when it was built to show one.
        # Absent means the profile did not measure it, which is why these are
        # written only where the run recorded them rather than as zeroes: a
        # zero here would read as "nothing crossed" on a run that never looked.
        ceiling = result["occupancy"].get("admission_max_in_flight")
        if ceiling is not None:
            lines.append(f"admission_max_in_flight = {ceiling}")
        tenancy = result.get("tenancy")
        if tenancy is not None:
            lines += [
                f"tenants = {len(tenancy['by_namespace'])}",
                f"foreign_credential_uses = {tenancy['foreign_credential_uses']}",
                "misattributed_usage_records = "
                f"{tenancy['misattributed_usage_records']}",
            ]
        deadlines = result.get("deadlines")
        if deadlines is not None:
            lines += [
                f"upstream_bound_ms = {deadlines['bound_ms']}",
                f"over_bound = {deadlines['over_bound']}",
                f"max_latency_ms = {number(deadlines['max_latency_ms'], 2)}",
            ]
        recovery = result.get("recovery")
        if recovery is not None:
            lines.append(f"served_after_load = {str(recovery['served']).lower()}")
        lines += [
            f"peak_rss_kib = {rss['peak']}",
            f"rss_growth_kib = {max(rss['peak'], rss['settled']) - rss['baseline']}",
            f"peak_sockets = {resources['sockets']['peak']}",
            f"cpu_seconds = {number(resources['cpu_seconds'], 2)}",
            f"missing_usage_records = {usage['missing']}",
            f"leaked_upstream_streams = {result['upstream']['streams_open_at_end']}",
            f"verdicts = {len(result['verdicts'])}",
            f"passed = {str(all(verdict['passed'] for verdict in result['verdicts'])).lower()}",
        ]

    return "\n".join(lines) + "\n"


GENERIC_MANIFESTS = {
    "endurance": "qualification/endurance/manifest.toml",
    "stateful-endurance": "qualification/stateful-endurance/manifest.toml",
    "fault": "qualification/faults/manifest.toml",
    "rollout": "qualification/rollout/manifest.toml",
}

RECOVERY_MANIFEST = "qualification/recovery/manifest.toml"
ENDURANCE_RESULT_SCHEMA_VERSION = 3
ENDURANCE_SURPLUS_VERDICT = "max_unexpected_usage_records"


def recovery_expected_stages(manifest_path: Path) -> dict[str, dict[str, Any]]:
    manifest = tomllib.loads(manifest_path.read_text(encoding="utf-8"))
    expected: dict[str, dict[str, Any]] = {}
    for scenario in manifest["scenario"]:
        for stage in scenario["stage"]:
            if stage["status"] != "executable":
                continue
            key = f"{scenario['id']}/{stage['id']}"
            expected[key] = {
                "capability": scenario["capability"],
                "evidence": stage["evidence"],
                "runner": stage.get("runner", ""),
            }
    return expected


def recovery_id(result: dict) -> str:
    return f"{result['scenario']}/{result['stage']}"


def check_recovery_complete(results: list[dict], manifest_path: Path) -> None:
    expected = recovery_expected_stages(manifest_path)
    observed = [recovery_id(result) for result in results]
    if set(observed) != set(expected) or len(observed) != len(expected):
        raise SystemExit(
            f"recovery: artifacts cover {sorted(set(observed))}, expected "
            f"{sorted(expected)}; do not retain a partial stage set"
        )

    for result in results:
        key = recovery_id(result)
        contract = expected[key]
        if result.get("schema_version") != 2:
            raise SystemExit(f"{key}: unsupported recovery artifact schema")
        if result.get("runner") != contract["runner"]:
            raise SystemExit(
                f"{key}: artifact runner {result.get('runner')!r} does not match "
                f"the manifest's {contract['runner']!r}"
            )
        if result.get("capability") != contract["capability"]:
            raise SystemExit(f"{key}: artifact capability does not match the manifest")
        if result.get("evidence") != contract["evidence"]:
            raise SystemExit(f"{key}: artifact evidence classes do not match the manifest")
        if not result.get("timeline"):
            raise SystemExit(f"{key}: artifact retains no recovery timeline")
        elapsed = result.get("run", {}).get("elapsed_ms")
        if not isinstance(elapsed, int) or elapsed <= 0:
            raise SystemExit(f"{key}: artifact has no positive elapsed duration")
        verdicts = result.get("gates", []) + result.get("checks", [])
        if not verdicts:
            raise SystemExit(f"{key}: artifact contains no gates or checks")
        if any(verdict.get("outcome") == "failed" for verdict in verdicts):
            raise SystemExit(f"{key}: at least one recovery gate or check failed")


def shell_output(*args: str) -> str:
    completed = subprocess.run(
        list(args), cwd=ROOT, capture_output=True, text=True, check=False
    )
    if completed.returncode != 0:
        raise SystemExit(
            f"could not collect qualification provenance from {' '.join(args)}: "
            f"{completed.stderr.strip()}"
        )
    return completed.stdout.strip()


def read_memory_kib() -> int | None:
    meminfo = Path("/proc/meminfo")
    if meminfo.exists():
        for line in meminfo.read_text(encoding="utf-8").splitlines():
            if line.startswith("MemTotal:"):
                return int(line.split()[1])
    try:
        return int(shell_output("sysctl", "-n", "hw.memsize")) // 1024
    except (SystemExit, ValueError):
        return None


def cpu_model_from(cpuinfo: str) -> str | None:
    fields: dict[str, str] = {}
    for line in cpuinfo.splitlines():
        if ":" not in line:
            continue
        key, value = (part.strip() for part in line.split(":", 1))
        if value and key not in fields:
            fields[key] = value
    for preferred in ("model name", "Hardware"):
        if preferred in fields:
            return fields[preferred]
    implementer = fields.get("CPU implementer")
    part = fields.get("CPU part")
    if implementer and part:
        return f"CPU implementer {implementer}, part {part}"
    return None


def read_cpu_model() -> str | None:
    cpuinfo = Path("/proc/cpuinfo")
    if cpuinfo.exists():
        model = cpu_model_from(cpuinfo.read_text(encoding="utf-8", errors="replace"))
        if model:
            return model
    try:
        return shell_output("sysctl", "-n", "machdep.cpu.brand_string")
    except SystemExit:
        return platform.processor() or None


def collect_recovery_provenance(binary_path: Path) -> dict[str, Any]:
    if not binary_path.is_file():
        raise SystemExit(f"{binary_path}: the recovery binary does not exist")
    manifest_path = ROOT / RECOVERY_MANIFEST
    manifest_bytes = manifest_path.read_bytes()
    binary_bytes = binary_path.read_bytes()
    commit = shell_output("git", "rev-parse", "HEAD")
    dirty = bool(shell_output("git", "status", "--porcelain", "--untracked-files=all"))
    crate = tomllib.loads((ROOT / "Cargo.toml").read_text(encoding="utf-8"))
    version = str(crate["workspace"]["package"]["version"])
    rustc = shell_output("rustc", "-V")
    cpu_count = os.cpu_count()
    memory = read_memory_kib()
    cpu_model = read_cpu_model()
    kernel = platform.release()
    cgroup = Path("/proc/1/cgroup")
    containerized = Path("/.dockerenv").exists()
    if cgroup.exists():
        containerized = containerized or "kubepods" in cgroup.read_text(
            encoding="utf-8", errors="ignore"
        )
    if not cpu_count or memory is None or not cpu_model or not kernel:
        raise SystemExit(
            "the host did not provide complete recovery hardware provenance "
            "(CPU count/model, memory, or kernel)"
        )
    return {
        "source": {
            "git_commit": commit,
            "git_dirty": dirty,
            "crate_version": version,
        },
        "binary": {
            "sha256": hashlib.sha256(binary_bytes).hexdigest(),
            "version": version,
            "cargo_profile": "debug" if "/release/" not in str(binary_path) else "release",
            "rustc": rustc,
        },
        "inputs": {
            "manifest": RECOVERY_MANIFEST,
            "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
            "fixtures": 0,
        },
        "hardware": {
            "os": platform.system().lower(),
            "arch": platform.machine().lower(),
            "kernel": kernel,
            "cpu_model": cpu_model,
            "cpus": cpu_count,
            "total_memory_kib": memory,
            "containerized": containerized,
        },
    }


def render_recovery(
    results: list[dict], runner: str, note: str, binary_path: Path, provenance: dict[str, Any] | None = None
) -> str:
    manifest_path = ROOT / RECOVERY_MANIFEST
    check_recovery_complete(results, manifest_path)
    provenance = provenance or collect_recovery_provenance(binary_path)
    for result in results:
        if result["run"].get("axond_version") != provenance["source"]["crate_version"]:
            raise SystemExit(
                f"{recovery_id(result)}: stage binary version "
                f"{result['run'].get('axond_version')!r} does not match the "
                f"recorded {provenance['source']['crate_version']!r}"
            )
    lines = [
        "# A retained recovery qualification run, reduced to executable stage observations.",
        "# Each stage binds its raw JSON artifact by digest; the raw artifacts remain",
        "# the detailed diagnosis for the outage, cache, restore, and convergence windows.",
        "",
        "schema_version = 1",
        'slice = "recovery"',
        'tier = "serving"',
        f"runner = {toml_string(runner)}",
        f"runner_note = {toml_string(note)}",
        "",
        "[source]",
        f"git_commit = {toml_string(provenance['source']['git_commit'])}",
        f"git_dirty = {str(provenance['source']['git_dirty']).lower()}",
        f"crate_version = {toml_string(provenance['source']['crate_version'])}",
        "",
        "[binary]",
        f"sha256 = {toml_string(provenance['binary']['sha256'])}",
        f"version = {toml_string(provenance['binary']['version'])}",
        f"cargo_profile = {toml_string(provenance['binary']['cargo_profile'])}",
        f"rustc = {toml_string(provenance['binary']['rustc'])}",
        "",
        "[inputs]",
        f"manifest = {toml_string(provenance['inputs']['manifest'])}",
        f"manifest_sha256 = {toml_string(provenance['inputs']['manifest_sha256'])}",
        f"fixtures = {provenance['inputs']['fixtures']}",
        "",
        "[hardware]",
        f"os = {toml_string(provenance['hardware']['os'])}",
        f"arch = {toml_string(provenance['hardware']['arch'])}",
        f"kernel = {toml_string(provenance['hardware']['kernel'])}",
        f"cpu_model = {toml_string(provenance['hardware']['cpu_model'])}",
        f"cpus = {provenance['hardware']['cpus']}",
        f"total_memory_kib = {provenance['hardware']['total_memory_kib']}",
        f"containerized = {str(provenance['hardware']['containerized']).lower()}",
    ]
    for result in sorted(results, key=recovery_id):
        verdicts = result.get("gates", []) + result.get("checks", [])
        lines += [
            "",
            "[[stage]]",
            f"id = {toml_string(recovery_id(result))}",
            f"runner = {toml_string(result['runner'])}",
            "artifact_sha256 = "
            f"{toml_string(hashlib.sha256(Path(result['_artifact_path']).read_bytes()).hexdigest())}",
            f"elapsed_ms = {result['run']['elapsed_ms']}",
            f"verdicts = {len(verdicts)}",
            f"passed = {str(not any(v.get('outcome') == 'failed' for v in verdicts)).lower()}",
        ]
    return "\n".join(lines) + "\n"


def generic_id(result: dict, slice_id: str) -> str:
    if slice_id == "fault":
        return result["row"]["id"]
    if slice_id == "rollout":
        return result["scenario"]["id"]
    return result["profile"]["id"]


def generic_tier(result: dict, slice_id: str) -> str | None:
    if slice_id == "fault":
        return None
    if slice_id == "rollout":
        return result["scenario"]["tier"]
    return result["profile"]["tier"]


def binary_identity(result: dict) -> dict[str, str]:
    """Normalize the common binary block and rollout's per-revision blocks."""
    environment = result["environment"]
    if "binary" in environment:
        return environment["binary"]
    binaries = [revision["binary"] for revision in result.get("revisions", [])]
    if not binaries:
        raise SystemExit("the artifact has no binary provenance")
    identities = sorted(
        {
            json.dumps(
                {
                    "sha256": binary["sha256"],
                    "version": binary["version"],
                },
                sort_keys=True,
            )
            for binary in binaries
        }
    )
    if len(identities) == 1:
        return binaries[0]
    # A true mixed-binary rollout is identified by the digest of its complete
    # binary set; the raw artifact still retains each revision's exact digest.
    return {
        "sha256": hashlib.sha256("\n".join(identities).encode()).hexdigest(),
        "version": "mixed",
    }


def expected_generic_ids(slice_id: str, manifest_path: Path) -> set[str]:
    manifest = tomllib.loads(manifest_path.read_text(encoding="utf-8"))
    if slice_id == "fault":
        return {row["id"] for row in manifest["row"]}
    if slice_id == "rollout":
        return {scenario["id"] for scenario in manifest["scenario"]}
    return {profile["id"] for profile in manifest["profile"]}


def check_generic_complete(
    results: list[dict], slice_id: str, tier: str, manifest_path: Path
) -> None:
    manifest = tomllib.loads(manifest_path.read_text(encoding="utf-8"))
    expected = expected_generic_ids(slice_id, manifest_path)
    observed = [generic_id(result, slice_id) for result in results]
    if set(observed) != expected or len(observed) != len(expected):
        raise SystemExit(
            f"{slice_id}: artifacts cover {sorted(set(observed))}, expected "
            f"{sorted(expected)}; do not retain a partial workload"
        )

    manifest_relative = GENERIC_MANIFESTS[slice_id]
    for result in results:
        workload = generic_id(result, slice_id)
        verdicts = result.get("verdicts")
        if not isinstance(verdicts, list) or not verdicts:
            raise SystemExit(f"{workload}: evidence contains no judged verdicts")
        if slice_id == "endurance":
            if result.get("schema_version") != ENDURANCE_RESULT_SCHEMA_VERSION:
                raise SystemExit(
                    f"{workload}: unsupported endurance artifact schema "
                    f"{result.get('schema_version')!r}"
                )
            unexpected = result.get("reconciliation", {}).get("unexpected_records")
            if not isinstance(unexpected, int) or unexpected < 0:
                raise SystemExit(
                    f"{workload}: endurance reconciliation has no surplus identity count"
                )
            surplus = [
                verdict
                for verdict in verdicts
                if verdict.get("threshold") == ENDURANCE_SURPLUS_VERDICT
            ]
            expected_bound = next(
                row[tier]["thresholds"][ENDURANCE_SURPLUS_VERDICT]
                for row in manifest["profile"]
                if row["id"] == workload
            )
            if len(surplus) != 1:
                raise SystemExit(
                    f"{workload}: endurance evidence did not evaluate the surplus usage gate"
                )
            verdict = surplus[0]
            if (
                verdict.get("comparison") != "<="
                or verdict.get("value") != unexpected
                or verdict.get("bound") != expected_bound
                or verdict.get("passed") is not (unexpected <= expected_bound)
                or unexpected > expected_bound
            ):
                raise SystemExit(
                    f"{workload}: the surplus usage verdict does not match reconciliation"
                )
        environment = result["environment"]
        if environment["manifest"]["path"] != manifest_relative:
            raise SystemExit(
                f"{workload}: artifact names {environment['manifest']['path']}, "
                f"not {manifest_relative}"
            )
        result_tier = generic_tier(result, slice_id)
        if result_tier is not None and result_tier != tier:
            raise SystemExit(
                f"{workload}: artifact is tier {result_tier!r}, expected {tier!r}"
            )
        for name, value in (
            ("git commit", environment["source"]["git_commit"]),
            ("binary digest", binary_identity(result)["sha256"]),
            ("compiler", environment["toolchain"]["rustc"]),
            ("kernel", environment["hardware"]["kernel"]),
            ("CPU model", environment["hardware"]["cpu_model"]),
            ("memory size", environment["hardware"]["total_memory_kib"]),
        ):
            if value is None or value == "":
                raise SystemExit(
                    f"{workload}: the run did not record {name}, so its "
                    "provenance cannot be compared"
                )
        if any(not verdict.get("passed", False) for verdict in verdicts):
            raise SystemExit(f"{workload}: at least one retained verdict failed")
        elapsed = result.get("run", {}).get("elapsed_ms")
        if not isinstance(elapsed, int) or elapsed <= 0:
            raise SystemExit(f"{workload}: the run has no positive elapsed duration")
        if slice_id in ("endurance", "stateful-endurance"):
            profile = result["profile"]
            run = result["run"]
            committed_duration = next(
                row[tier]["duration_ms"]
                for row in manifest["profile"]
                if row["id"] == workload
            )
            if profile.get("manifest_duration_ms") != committed_duration:
                raise SystemExit(
                    f"{workload}: artifact manifest duration is stale or incomplete"
                )
            if not isinstance(profile.get("duration_ms"), int) or profile["duration_ms"] <= 0:
                raise SystemExit(f"{workload}: the offered duration is missing")
            if slice_id == "endurance" and (
                not isinstance(run.get("requested_duration_ms"), int)
                or run["requested_duration_ms"] <= 0
            ):
                raise SystemExit(f"{workload}: the requested duration is missing")
            if slice_id == "endurance" and run["requested_duration_ms"] != profile["duration_ms"]:
                raise SystemExit(
                    f"{workload}: requested and offered endurance durations disagree"
                )
            if slice_id == "endurance" and elapsed < profile["duration_ms"]:
                raise SystemExit(
                    f"{workload}: only {elapsed} ms elapsed during a "
                    f"{profile['duration_ms']} ms endurance run"
                )
            if not isinstance(run.get("duration_source"), str) or not run["duration_source"]:
                raise SystemExit(f"{workload}: the duration source is missing")


def check_one_run_generic(results: list[dict]) -> None:
    readers: dict[str, Callable[[dict], Any]] = {
        "the source": lambda result: result["environment"]["source"],
        "the binary": binary_identity,
        "the toolchain": lambda result: result["environment"]["toolchain"],
        "the manifest": lambda result: result["environment"]["manifest"],
        # Rollout artifacts have no fixture set; absence is the same explicit
        # zero-fixture input as an empty list on the other generic slices.
        "the fixtures": lambda result: result["environment"].get("fixtures", []),
        "the hardware": lambda result: result["environment"]["hardware"],
    }
    for name, reader in readers.items():
        values = {json.dumps(reader(result), sort_keys=True) for result in results}
        if len(values) != 1:
            raise SystemExit(
                f"the artifacts disagree about {name}, so they are not one run "
                f"({len(values)} values). Remove the stale ones and re-run."
            )


def render_generic(
    results: list[dict], slice_id: str, tier: str, runner: str, note: str
) -> str:
    if slice_id not in GENERIC_MANIFESTS:
        raise SystemExit(f"generic records do not support slice {slice_id!r}")
    manifest_path = ROOT / GENERIC_MANIFESTS[slice_id]
    check_generic_complete(results, slice_id, tier, manifest_path)
    check_one_run_generic(results)

    environment = results[0]["environment"]
    hardware = environment["hardware"]
    binary = binary_identity(results[0])
    lines = [
        f"# A retained {slice_id} qualification run, reduced to workload observations.",
        "# The raw JSON artifacts remain alongside the workflow result; each",
        "# observation below binds one workload to its artifact digest and verdicts.",
        "",
        "schema_version = 1",
        f"slice = {toml_string(slice_id)}",
        f"tier = {toml_string(tier)}",
        f"runner = {toml_string(runner)}",
        f"runner_note = {toml_string(note)}",
        "",
        "[source]",
        f"git_commit = {toml_string(environment['source']['git_commit'])}",
        f"git_dirty = {str(environment['source']['git_dirty']).lower()}",
        f"crate_version = {toml_string(environment['source']['crate_version'])}",
        "",
        "[binary]",
        f"sha256 = {toml_string(binary['sha256'])}",
        f"version = {toml_string(binary['version'])}",
        f"cargo_profile = {toml_string(environment['toolchain']['cargo_profile'])}",
        f"rustc = {toml_string(environment['toolchain']['rustc'])}",
        "",
        "[inputs]",
        f"manifest = {toml_string(environment['manifest']['path'])}",
        f"manifest_sha256 = {toml_string(environment['manifest']['sha256'])}",
        f"fixtures = {len(environment.get('fixtures', []))}",
        "",
        "[hardware]",
        f"os = {toml_string(hardware['os'])}",
        f"arch = {toml_string(hardware['arch'])}",
        f"kernel = {toml_string(hardware['kernel'])}",
        f"cpu_model = {toml_string(hardware['cpu_model'])}",
        f"cpus = {hardware['cpus']}",
        f"total_memory_kib = {hardware['total_memory_kib']}",
        f"containerized = {str(hardware['containerized']).lower()}",
    ]
    for result in sorted(results, key=lambda item: generic_id(item, slice_id)):
        lines += [
            "",
            "[[observation]]",
            f"id = {toml_string(generic_id(result, slice_id))}",
            "artifact_sha256 = "
            f"{toml_string(hashlib.sha256(Path(result['_artifact_path']).read_bytes()).hexdigest())}",
            f"elapsed_ms = {result['run']['elapsed_ms']}",
            f"verdicts = {len(result['verdicts'])}",
            f"passed = {str(all(v['passed'] for v in result['verdicts'])).lower()}",
        ]
        if slice_id in ("endurance", "stateful-endurance"):
            profile = result["profile"]
            run = result["run"]
            lines += [
                f"duration_ms = {profile['duration_ms']}",
                f"manifest_duration_ms = {profile['manifest_duration_ms']}",
                f"requested_duration_ms = {run.get('requested_duration_ms', profile['duration_ms'])}",
                f"duration_source = {toml_string(run['duration_source'])}",
            ]
            if slice_id == "endurance":
                lines.append(f"artifact_schema_version = {result['schema_version']}")
    return "\n".join(lines) + "\n"


def self_test() -> int:
    """Exercise the generic record's completeness and provenance refusals."""
    assert cpu_model_from("model name : Example CPU\n") == "Example CPU"
    assert cpu_model_from("Hardware : Example Board\n") == "Example Board"
    assert (
        cpu_model_from("CPU implementer : 0x61\nCPU part : 0x000\n")
        == "CPU implementer 0x61, part 0x000"
    )
    assert cpu_model_from("CPU implementer : 0x61\n") is None

    manifest_relative = GENERIC_MANIFESTS["rollout"]
    manifest_bytes = (ROOT / manifest_relative).read_bytes()
    environment = {
        "source": {"git_commit": "commit", "git_dirty": False, "crate_version": "0.0.0"},
        "toolchain": {"cargo_profile": "debug", "rustc": "rustc test"},
        "manifest": {
            "path": manifest_relative,
            "sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        },
        "hardware": {
            "os": "test",
            "arch": "test",
            "kernel": "kernel",
            "cpu_model": "cpu",
            "cpus": 1,
            "total_memory_kib": 1,
            "containerized": False,
        },
    }
    result = {
        "scenario": {"id": "rolling-replace", "tier": "heavy"},
        "run": {"elapsed_ms": 1},
        "environment": environment,
        "revisions": [{"binary": {"sha256": "binary", "version": "0.0.0"}}],
        "verdicts": [{"passed": True}],
    }
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "rolling-replace.json"
        path.write_text(json.dumps(result), encoding="utf-8")
        result["_artifact_path"] = str(path)
        rendered = render_generic(
            [result], "rollout", "heavy", "local", "qualification self-test"
        )
        parsed = tomllib.loads(rendered)
        assert parsed["observation"][0]["id"] == "rolling-replace"
        assert parsed["observation"][0]["passed"] is True
        assert parsed["inputs"]["fixtures"] == 0

        try:
            render_generic([], "rollout", "heavy", "local", "missing")
        except SystemExit:
            pass
        else:
            raise AssertionError("a missing workload was accepted")

        failed = dict(result)
        failed["verdicts"] = [{"passed": False}]
        try:
            render_generic([failed], "rollout", "heavy", "local", "failed")
        except SystemExit:
            pass
        else:
            raise AssertionError("a failed verdict was accepted")

        endurance_manifest = tomllib.loads(
            (ROOT / GENERIC_MANIFESTS["endurance"]).read_text(encoding="utf-8")
        )
        endurance_duration = endurance_manifest["profile"][0]["soak"]["duration_ms"]
        endurance_environment = dict(environment)
        endurance_environment["manifest"] = {
            "path": GENERIC_MANIFESTS["endurance"],
            "sha256": hashlib.sha256(
                (ROOT / GENERIC_MANIFESTS["endurance"]).read_bytes()
            ).hexdigest(),
        }
        endurance_environment["binary"] = {"sha256": "binary", "version": "0.0.0"}
        endurance_result = {
            "schema_version": ENDURANCE_RESULT_SCHEMA_VERSION,
            "profile": {
                "id": "mixed-endurance",
                "tier": "soak",
                "duration_ms": endurance_duration - 1,
                "manifest_duration_ms": endurance_duration,
            },
            "run": {
                "elapsed_ms": endurance_duration - 1,
                "requested_duration_ms": endurance_duration - 1,
                "duration_source": "environment",
            },
            "environment": endurance_environment,
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
        endurance_result["_artifact_path"] = str(path)
        rendered = render_generic(
            [endurance_result], "endurance", "soak", "local", "short diagnostic soak"
        )
        parsed = tomllib.loads(rendered)
        assert parsed["observation"][0]["duration_ms"] == endurance_duration - 1
        assert (
            parsed["observation"][0]["artifact_schema_version"]
            == ENDURANCE_RESULT_SCHEMA_VERSION
        )

        stale_endurance = dict(endurance_result)
        stale_endurance["schema_version"] = ENDURANCE_RESULT_SCHEMA_VERSION - 1
        try:
            render_generic(
                [stale_endurance], "endurance", "soak", "local", "stale schema"
            )
        except SystemExit:
            pass
        else:
            raise AssertionError("a stale endurance result schema was accepted")

        ungated_endurance = dict(endurance_result)
        ungated_endurance["verdicts"] = [{"passed": True}]
        try:
            render_generic(
                [ungated_endurance], "endurance", "soak", "local", "missing gate"
            )
        except SystemExit:
            pass
        else:
            raise AssertionError("an endurance result without the surplus gate was accepted")

        false_pass_endurance = dict(endurance_result)
        false_pass_endurance["reconciliation"] = {"unexpected_records": 1}
        false_pass_endurance["verdicts"] = [
            {
                "threshold": ENDURANCE_SURPLUS_VERDICT,
                "comparison": "<=",
                "value": 1,
                "bound": 0,
                "passed": True,
            }
        ]
        try:
            render_generic(
                [false_pass_endurance], "endurance", "soak", "local", "false pass"
            )
        except SystemExit:
            pass
        else:
            raise AssertionError("a false passing surplus verdict was accepted")

        short_elapsed_endurance = dict(endurance_result)
        short_elapsed_endurance["run"] = dict(endurance_result["run"])
        short_elapsed_endurance["run"]["elapsed_ms"] = endurance_duration - 2
        try:
            render_generic(
                [short_elapsed_endurance],
                "endurance",
                "soak",
                "local",
                "short elapsed",
            )
        except SystemExit:
            pass
        else:
            raise AssertionError("an endurance result with short elapsed time was accepted")

        stateful_manifest = tomllib.loads(
            (ROOT / GENERIC_MANIFESTS["stateful-endurance"]).read_text(encoding="utf-8")
        )
        stateful_duration = stateful_manifest["profile"][0]["soak"]["duration_ms"]
        stateful_environment = dict(environment)
        stateful_environment["manifest"] = {
            "path": GENERIC_MANIFESTS["stateful-endurance"],
            "sha256": hashlib.sha256(
                (ROOT / GENERIC_MANIFESTS["stateful-endurance"]).read_bytes()
            ).hexdigest(),
        }
        stateful_environment["binary"] = {"sha256": "binary", "version": "0.0.0"}
        stateful_result = {
            "profile": {
                "id": "mixed-stateful-endurance",
                "tier": "soak",
                "duration_ms": stateful_duration,
                "manifest_duration_ms": stateful_duration,
            },
            "run": {
                "elapsed_ms": 1,
                "duration_source": "manifest",
            },
            "environment": stateful_environment,
            "verdicts": [{"passed": True}],
        }
        stateful_result["_artifact_path"] = str(path)
        rendered = render_generic(
            [stateful_result],
            "stateful-endurance",
            "soak",
            "github-actions",
            "stateful endurance self-test",
        )
        parsed = tomllib.loads(rendered)
        assert parsed["slice"] == "stateful-endurance"
        assert parsed["observation"][0]["manifest_duration_ms"] == stateful_duration
        assert parsed["observation"][0]["requested_duration_ms"] == stateful_duration

        recovery_expected = recovery_expected_stages(ROOT / RECOVERY_MANIFEST)
        recovery_provenance = {
            "source": {"git_commit": "commit", "git_dirty": False, "crate_version": "0.0.0"},
            "binary": {
                "sha256": "binary",
                "version": "0.0.0",
                "cargo_profile": "debug",
                "rustc": "rustc test",
            },
            "inputs": {
                "manifest": RECOVERY_MANIFEST,
                "manifest_sha256": hashlib.sha256(
                    (ROOT / RECOVERY_MANIFEST).read_bytes()
                ).hexdigest(),
                "fixtures": 0,
            },
            "hardware": {
                "os": "test",
                "arch": "test",
                "kernel": "kernel",
                "cpu_model": "cpu",
                "cpus": 1,
                "total_memory_kib": 1,
                "containerized": False,
            },
        }
        recovery_results = []
        with tempfile.TemporaryDirectory() as recovery_directory:
            for key, contract in recovery_expected.items():
                scenario, stage = key.split("/", 1)
                result = {
                    "schema_version": 2,
                    "scenario": scenario,
                    "stage": stage,
                    "runner": contract["runner"],
                    "capability": contract["capability"],
                    "evidence": contract["evidence"],
                    "run": {
                        "started_at_unix_ms": 1,
                        "elapsed_ms": 1,
                        "axond_version": "0.0.0",
                        "control_plane": "postgres",
                        "schema": "test",
                        "schema_identity": "test",
                    },
                    "timeline": [{"at_ms": 0, "event": "complete", "detail": "test"}],
                    "observations": {},
                    "gates": [],
                    "checks": [{"gate": "test", "outcome": "met"}],
                }
                path = Path(recovery_directory) / f"{scenario}.{stage}.json"
                path.write_text(json.dumps(result), encoding="utf-8")
                result["_artifact_path"] = str(path)
                recovery_results.append(result)
            rendered = render_recovery(
                recovery_results,
                "github-actions",
                "qualification self-test",
                Path("synthetic-binary"),
                recovery_provenance,
            )
            parsed = tomllib.loads(rendered)
            assert len(parsed["stage"]) == len(recovery_expected)
            try:
                check_recovery_complete(recovery_results[:-1], ROOT / RECOVERY_MANIFEST)
            except SystemExit:
                pass
            else:
                raise AssertionError("a partial recovery stage set was accepted")
            failed = dict(recovery_results[0])
            failed["gates"] = [{"gate": "test", "outcome": "failed"}]
            try:
                check_recovery_complete([failed, *recovery_results[1:]], ROOT / RECOVERY_MANIFEST)
            except SystemExit:
                pass
            else:
                raise AssertionError("a failed recovery stage was accepted")

    print("qualification evidence self-test passed")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "results", type=Path, nargs="?", help="a directory of result artifacts"
    )
    parser.add_argument(
        "--self-test", action="store_true", help="exercise writer refusal paths"
    )
    parser.add_argument(
        "--slice",
        choices=[
            "capacity",
            "endurance",
            "stateful-endurance",
            "fault",
            "rollout",
            "recovery",
        ],
        default="capacity",
        help="the qualification slice (capacity is the legacy default)",
    )
    parser.add_argument(
        "--tier",
        help="the tier for a generic record; fault matrices use `full`",
    )
    parser.add_argument("--out", type=Path, help="the record to write")
    parser.add_argument(
        "--binary",
        type=Path,
        help="the binary whose provenance the recovery record should retain",
    )
    parser.add_argument(
        "--runner",
        choices=["local", "github-actions"],
        help="where the run happened, which is what bounds who may compare it",
    )
    parser.add_argument("--note", help="one line about the machine and build")
    arguments = parser.parse_args()

    if arguments.self_test:
        return self_test()
    if (
        arguments.results is None
        or arguments.out is None
        or arguments.note is None
        or arguments.runner is None
        ):
        parser.error(
            "results, --out, --runner, and --note are required unless "
            "--self-test is used"
        )

    if arguments.slice == "capacity":
        record = render(
            load_results(arguments.results), arguments.runner, arguments.note
        )
    elif arguments.slice == "recovery":
        if arguments.binary is None:
            parser.error("--binary is required for recovery records")
        record = render_recovery(
            load_results(arguments.results, recursive=True),
            arguments.runner,
            arguments.note,
            arguments.binary,
        )
    else:
        results = load_results(arguments.results, recursive=True)
        inferred_tiers = {
            generic_tier(result, arguments.slice)
            for result in results
            if generic_tier(result, arguments.slice) is not None
        }
        tier = arguments.tier or (inferred_tiers.pop() if len(inferred_tiers) == 1 else None)
        if not tier:
            raise SystemExit(
                "--tier is required when the generic artifacts do not carry one "
                "consistent tier"
            )
        record = render_generic(
            results, arguments.slice, tier, arguments.runner, arguments.note
        )
    arguments.out.parent.mkdir(parents=True, exist_ok=True)
    arguments.out.write_text(record, encoding="utf-8")
    print(f"wrote {arguments.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
