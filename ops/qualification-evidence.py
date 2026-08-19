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
import copy
import hashlib
import json
import os
import platform
import subprocess
import sys
import tempfile
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Callable

from recovery_contract import (
    CHECK_RECONSTRUCTIONS,
    GATE_RECONSTRUCTIONS,
    LOWER_SHA256,
    RECOVERY_RESULT_SCHEMA_VERSION,
    REQUIRED_GATE_NAMES,
    STAGE_REQUIRED_NULL_OBSERVATIONS,
    deferred_gate_detail,
    derive_verdict_outcome,
    gate_owner,
    reconstruct_required_check,
    reconstruct_required_gate,
    required_checks,
    required_observations,
    validate_gate_ownership_model,
    validate_recovery_artifact,
)

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10 ops floor
    import tomli as tomllib  # type: ignore[no-redef]


ROOT = Path(__file__).resolve().parent.parent
CAPACITY_RESULT_SCHEMA_VERSION = 2
STATEFUL_LEDGER_SHARDS = 64
STATEFUL_MAX_SHARD_ROWS = 1_500_000
STATEFUL_LEDGER_FIELDS: tuple[tuple[str, int], ...] = (
    ("request_identities", 1),
    ("correlations", 2),
    ("durable_identities", 2),
    ("durable_outside_identities", 2),
    ("correlation_windows", 1),
)
STATEFUL_LEDGER_STEMS: dict[str, tuple[str, ...]] = {
    "request_identities": ("request",),
    "correlations": ("expected", "observed"),
    "durable_identities": ("expected-request", "observed-request"),
    "durable_outside_identities": ("expected-request", "observed-request"),
    "correlation_windows": ("window",),
}
STATEFUL_LEDGER_WIDTHS = {
    "request_identities": 16,
    "correlations": 17,
    "durable_identities": 16,
    "durable_outside_identities": 16,
    "correlation_windows": 33,
}
STATEFUL_LEDGER_COUNTS: dict[str, tuple[str, ...]] = {
    "request_identities": ("recorded",),
    "correlations": ("expected", "observed"),
    "durable_identities": ("expected_rows", "observed_rows"),
    "durable_outside_identities": ("expected_rows", "observed_rows"),
    "correlation_windows": ("recorded",),
}


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


def resolve_stateful_ledger(raw_path: Path, declared: object, label: str) -> Path:
    """Resolve one retained ledger without trusting an absolute or parent path."""
    if not isinstance(declared, str) or not declared:
        raise SystemExit(f"{label}: exact ledger path is missing")
    relative = Path(declared)
    if relative.is_absolute() or ".." in relative.parts:
        raise SystemExit(f"{label}: exact ledger path is not a safe relative path")
    candidates = (
        ROOT / relative,
        raw_path.parent / relative,
        raw_path.parent / relative.name,
    )
    existing: dict[Path, Path] = {}
    for candidate in candidates:
        if candidate.is_symlink():
            raise SystemExit(f"{label}: exact ledger directory must not be a symlink")
        if candidate.is_dir():
            resolved = candidate.resolve()
            existing[resolved] = candidate
    if len(existing) != 1:
        raise SystemExit(
            f"{label}: exact ledger path resolves to {len(existing)} retained directories"
        )
    return next(iter(existing.values()))


def stateful_ledger_claim(
    directory: Path,
    label: str,
    field: str,
    evidence: dict,
    *,
    schema_label: str,
    digest_domain: bytes,
) -> dict[str, object]:
    """Hash every fixed-width shard with names and lengths in canonical order."""
    entries = sorted(directory.iterdir(), key=lambda path: path.name)
    expected_names = {
        f"{stem}-shard-{shard:02}.bin"
        for stem in STATEFUL_LEDGER_STEMS[field]
        for shard in range(STATEFUL_LEDGER_SHARDS)
    }
    if (
        {path.name for path in entries} != expected_names
        or any(path.is_symlink() or not path.is_file() for path in entries)
    ):
        raise SystemExit(
            f"{label}: exact ledger filenames do not match {schema_label}"
        )
    digest = hashlib.sha256(digest_domain)
    total_bytes = 0
    row_width = STATEFUL_LEDGER_WIDTHS[field]
    for path in entries:
        name = path.name.encode("utf-8")
        size = path.stat().st_size
        if size % row_width:
            raise SystemExit(
                f"{label}: {path.name} is not a multiple of its {row_width}-byte row width"
            )
        if size // row_width > STATEFUL_MAX_SHARD_ROWS:
            raise SystemExit(
                f"{label}: {path.name} exceeds the bounded reconciliation ceiling "
                f"of {STATEFUL_MAX_SHARD_ROWS} rows"
            )
        digest.update(len(name).to_bytes(4, "big"))
        digest.update(name)
        digest.update(size.to_bytes(8, "big"))
        with path.open("rb") as shard:
            while chunk := shard.read(1024 * 1024):
                digest.update(chunk)
        total_bytes += size
    expected_rows = 0
    for count_field in STATEFUL_LEDGER_COUNTS[field]:
        count = evidence.get(count_field)
        if not isinstance(count, int) or isinstance(count, bool) or count < 0:
            raise SystemExit(f"{label}: {count_field} row count is malformed")
        expected_rows += count
    if total_bytes != expected_rows * row_width:
        raise SystemExit(
            f"{label}: retained bytes encode {total_bytes // row_width} rows, "
            f"but the result claims {expected_rows}"
        )
    return {"sha256": digest.hexdigest(), "files": len(entries), "bytes": total_bytes}


def stateful_ledger_claims(result: dict, workload: str) -> dict[str, dict[str, object]]:
    raw_path = Path(result["_artifact_path"])
    usage = result.get("usage", {})
    claims: dict[str, dict[str, object]] = {}
    for field, files_per_shard in STATEFUL_LEDGER_FIELDS:
        evidence = usage.get(field, {})
        shards = evidence.get("shards")
        if shards != STATEFUL_LEDGER_SHARDS:
            raise SystemExit(
                f"{workload}: {field} has {shards!r} shards, expected "
                f"schema-3 count {STATEFUL_LEDGER_SHARDS}"
            )
        directory = resolve_stateful_ledger(
            raw_path, evidence.get("path"), f"{workload}: {field}"
        )
        claim = stateful_ledger_claim(
            directory,
            f"{workload}: {field}",
            field,
            evidence,
            schema_label="stateful-endurance schema 3",
            digest_domain=b"axond-stateful-ledger-v2\0",
        )
        expected_files = shards * files_per_shard
        if claim["files"] != expected_files or claim["bytes"] <= 0:
            raise SystemExit(
                f"{workload}: {field} retained {claim['files']} shards, "
                f"expected {expected_files}, with non-empty evidence"
            )
        claims[field] = claim
    return claims


def endurance_ledger_claims(result: dict, workload: str) -> dict[str, dict[str, object]]:
    """Bind the stateless run's exact request and correlation spill ledgers."""
    raw_path = Path(result["_artifact_path"])
    reconciliation = result.get("reconciliation", {})
    claims: dict[str, dict[str, object]] = {}
    for field, files_per_shard in STATEFUL_LEDGER_FIELDS[:2]:
        evidence = reconciliation.get(field, {})
        if evidence.get("exact") is not True:
            raise SystemExit(f"{workload}: {field} is not exact evidence")
        shards = evidence.get("shards")
        if shards != STATEFUL_LEDGER_SHARDS:
            raise SystemExit(
                f"{workload}: {field} has {shards!r} shards, expected "
                f"schema-4 count {STATEFUL_LEDGER_SHARDS}"
            )
        directory = resolve_stateful_ledger(
            raw_path, evidence.get("path"), f"{workload}: {field}"
        )
        claim = stateful_ledger_claim(
            directory,
            f"{workload}: {field}",
            field,
            evidence,
            schema_label="endurance schema 4",
            digest_domain=b"axond-stateful-ledger-v1\0",
        )
        expected_files = shards * files_per_shard
        if claim["files"] != expected_files or claim["bytes"] <= 0:
            raise SystemExit(
                f"{workload}: {field} retained {claim['files']} shards, "
                f"expected {expected_files}, with non-empty evidence"
            )
        claims[field] = claim
    return claims


def sample_claim(result: dict, workload: str) -> dict[str, object]:
    """Bind every append-only JSONL resource series named by the result."""
    raw_path = Path(result["_artifact_path"])
    run = result.get("run", {})
    declared_value = run.get("samples_paths", run.get("samples_path"))
    declared = [declared_value] if isinstance(declared_value, str) else declared_value
    if (
        not isinstance(declared, list)
        or not declared
        or any(not isinstance(value, str) or not value for value in declared)
    ):
        raise SystemExit(f"{workload}: resource sample path is missing")
    resolved: dict[Path, Path] = {}
    for value in declared:
        relative = Path(value)
        if relative.is_absolute() or ".." in relative.parts:
            raise SystemExit(f"{workload}: resource sample path is not safe and relative")
        candidates = (
            ROOT / relative,
            raw_path.parent / relative,
            raw_path.parent / relative.name,
        )
        existing: dict[Path, Path] = {}
        for candidate in candidates:
            if candidate.is_symlink():
                raise SystemExit(f"{workload}: resource sample file must not be a symlink")
            if candidate.is_file():
                existing[candidate.resolve()] = candidate
        if len(existing) != 1:
            raise SystemExit(
                f"{workload}: resource sample path resolves to "
                f"{len(existing)} retained files"
            )
        path = next(iter(existing.values()))
        resolved[path.resolve()] = path
    if len(resolved) != len(declared):
        raise SystemExit(f"{workload}: resource sample paths are not unique")
    digest = hashlib.sha256(b"axond-resource-samples-v1\0")
    total_bytes = 0
    for path in sorted(resolved.values(), key=lambda candidate: candidate.name):
        payload = path.read_bytes()
        if not payload:
            raise SystemExit(f"{workload}: resource sample file is empty")
        name = path.name.encode("utf-8")
        digest.update(len(name).to_bytes(4, "big"))
        digest.update(name)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
        total_bytes += len(payload)
    return {"sha256": digest.hexdigest(), "files": len(resolved), "bytes": total_bytes}


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
    for result in results:
        if result.get("schema_version") != CAPACITY_RESULT_SCHEMA_VERSION:
            raise SystemExit(
                f"{result.get('profile', {}).get('id', 'capacity')}: unsupported "
                f"capacity artifact schema {result.get('schema_version')!r}"
            )

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
        "schema_version = 2",
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
            "artifact_sha256 = "
            f"{toml_string(hashlib.sha256(Path(result['_artifact_path']).read_bytes()).hexdigest())}",
            f"artifact_schema_version = {result['schema_version']}",
            f"concurrency = {profile['concurrency']}",
            f"config_sha256 = {toml_string(result['environment']['config']['sha256'])}",
            f"requests = {profile['requests']}",
            f"offered = {throughput['offered']}",
            f"accepted = {throughput['accepted']}",
            f"rejected = {throughput['rejected']}",
            f"errors = {throughput['errors']}",
            f"elapsed_ms = {result['run']['elapsed_ms']}",
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
        queue = result.get("queue")
        if queue is not None:
            if (
                type(queue.get("observations")) is not int
                or queue["observations"] <= 0
                or type(queue.get("min_depth")) is not int
                or type(queue.get("max_depth")) is not int
                or queue.get("attributes") != 0
                or queue.get("exact") is not True
            ):
                raise SystemExit(
                    f"{profile['id']}: queue evidence is incomplete, labelled, or inexact"
                )
            lines += [
                f"queue_observations = {queue['observations']}",
                f"queue_min_depth = {queue['min_depth']}",
                f"queue_max_depth = {queue['max_depth']}",
                f"queue_attributes = {queue['attributes']}",
                f"queue_exact = {str(queue['exact']).lower()}",
            ]
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
ENDURANCE_RESULT_SCHEMA_VERSION = 4
FAULT_RESULT_SCHEMA_VERSION = 1
ROLLOUT_RESULT_SCHEMA_VERSION = 4
ROLLOUT_RECORD_SCHEMA_VERSION = 4
STATEFUL_ENDURANCE_RESULT_SCHEMA_VERSION = 3
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
                "driver": stage.get("driver", ""),
                "scenario": scenario,
                "stage": stage,
            }
    return expected


def recovery_id(result: dict) -> str:
    return f"{result['scenario']}/{result['stage']}"


def check_recovery_complete(results: list[dict], manifest_path: Path) -> None:
    expected = recovery_expected_stages(manifest_path)
    scenarios = {
        contract["scenario"]["id"]: contract["scenario"] for contract in expected.values()
    }
    ownership_problems = validate_gate_ownership_model(list(scenarios.values()))
    if ownership_problems:
        raise SystemExit(
            "recovery: malformed gate ownership model: " + "; ".join(ownership_problems)
        )
    observed = [recovery_id(result) for result in results]
    if set(observed) != set(expected) or len(observed) != len(expected):
        raise SystemExit(
            f"recovery: artifacts cover {sorted(set(observed))}, expected "
            f"{sorted(expected)}; do not retain a partial stage set"
        )

    for result in results:
        key = recovery_id(result)
        contract = expected[key]
        if result.get("schema_version") != RECOVERY_RESULT_SCHEMA_VERSION:
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
        problems = validate_recovery_artifact(
            result,
            contract["scenario"],
            contract["stage"],
        )
        if problems:
            raise SystemExit(f"{key}: malformed recovery evidence: {'; '.join(problems)}")
        executable = result.get("run", {}).get("axond_executable_sha256")
        if not isinstance(executable, str) or not LOWER_SHA256.fullmatch(executable):
            raise SystemExit(f"{key}: artifact has no exact release executable digest")


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


def arm_cpu_identities(cpuinfo: str) -> list[tuple[str, str]] | None:
    identities: set[tuple[str, str]] = set()
    implementers: list[str] = []
    parts: list[str] = []

    def finish_record() -> bool:
        if not implementers and not parts:
            return True
        if (
            len(implementers) != 1
            or len(parts) != 1
            or not implementers[0]
            or not parts[0]
        ):
            return False
        identities.add((implementers[0], parts[0]))
        implementers.clear()
        parts.clear()
        return True

    for line in cpuinfo.splitlines():
        if not line.strip():
            if not finish_record():
                return None
            continue
        if ":" not in line:
            continue
        key, value = (item.strip() for item in line.split(":", 1))
        if key == "processor" and not finish_record():
            return None
        if key == "CPU implementer":
            implementers.append(value)
        elif key == "CPU part":
            parts.append(value)

    if not finish_record() or not identities:
        return None
    return sorted(identities)


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
    identities = arm_cpu_identities(cpuinfo)
    if identities is None:
        return None
    return "; ".join(
        f"CPU implementer {implementer}, part {part}"
        for implementer, part in identities
    )


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


def agreed_recovery_cargo_profile(results: list[dict]) -> str:
    profiles = {
        result.get("run", {}).get("cargo_profile")
        for result in results
    }
    if len(profiles) != 1:
        raise SystemExit(
            "recovery: raw stage Cargo profiles disagree; retain one exact build profile"
        )
    profile = next(iter(profiles))
    if profile not in {"debug", "release"}:
        raise SystemExit(
            f"recovery: raw stages name unsupported Cargo profile {profile!r}"
        )
    return profile


def collect_recovery_provenance(
    binary_path: Path, cargo_profile: str
) -> dict[str, Any]:
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
            "cargo_profile": cargo_profile,
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
    results: list[dict],
    runner: str,
    note: str,
    binary_path: Path,
    provenance: dict[str, Any] | None = None,
) -> str:
    manifest_path = ROOT / RECOVERY_MANIFEST
    check_recovery_complete(results, manifest_path)
    expected = recovery_expected_stages(manifest_path)
    cargo_profile = agreed_recovery_cargo_profile(results)
    provenance = provenance or collect_recovery_provenance(binary_path, cargo_profile)
    if provenance.get("binary", {}).get("cargo_profile") != cargo_profile:
        raise SystemExit(
            "recovery: retained binary Cargo profile does not match the agreed raw stages"
        )
    for result in results:
        if result["run"].get("axond_version") != provenance["source"]["crate_version"]:
            raise SystemExit(
                f"{recovery_id(result)}: stage binary version "
                f"{result['run'].get('axond_version')!r} does not match the "
                f"recorded {provenance['source']['crate_version']!r}"
            )
        if result["run"].get("axond_executable_sha256") != provenance["binary"]["sha256"]:
            raise SystemExit(
                f"{recovery_id(result)}: stage executable digest does not match "
                "the retained recovery binary"
            )
        if result["run"].get("cargo_profile") != provenance["binary"]["cargo_profile"]:
            raise SystemExit(
                f"{recovery_id(result)}: stage Cargo profile does not match the record"
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
        run = result["run"]
        lines += [
            "",
            "[[stage]]",
            f"id = {toml_string(recovery_id(result))}",
            f"runner = {toml_string(result['runner'])}",
            f"driver = {toml_string(expected[recovery_id(result)]['driver'])}",
            f"artifact_schema_version = {result['schema_version']}",
            f"binary_sha256 = {toml_string(run['axond_executable_sha256'])}",
            "artifact_sha256 = "
            f"{toml_string(hashlib.sha256(Path(result['_artifact_path']).read_bytes()).hexdigest())}",
            f"elapsed_ms = {run['elapsed_ms']}",
            f"verdicts = {len(verdicts)}",
            f"passed = {str(not any(v.get('outcome') == 'failed' for v in verdicts)).lower()}",
        ]
        if run.get("axond_execution_bound") is not None:
            lines += [
                f"executed_binary_sha256 = {toml_string(run['axond_executed_sha256'])}",
                f"execution_bound = {str(run['axond_execution_bound']).lower()}",
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
        if (
            slice_id == "fault"
            and result.get("schema_version") != FAULT_RESULT_SCHEMA_VERSION
        ):
            raise SystemExit(
                f"{workload}: unsupported fault artifact schema "
                f"{result.get('schema_version')!r}"
            )
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
        if slice_id == "rollout":
            check_rollout_qualifiable(result, workload)
        if slice_id == "stateful-endurance":
            check_stateful_endurance_exact(result, workload)
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
            if profile["duration_ms"] < committed_duration:
                raise SystemExit(
                    f"{workload}: offered {profile['duration_ms']} ms, but retained "
                    f"{tier} evidence requires {committed_duration} ms"
                )
            requested_duration = run.get("requested_duration_ms", profile["duration_ms"])
            if (
                not isinstance(requested_duration, int)
                or isinstance(requested_duration, bool)
                or requested_duration <= 0
            ):
                raise SystemExit(f"{workload}: the requested duration is missing")
            if requested_duration != profile["duration_ms"]:
                raise SystemExit(
                    f"{workload}: requested and offered durations disagree"
                )
            if elapsed < profile["duration_ms"]:
                raise SystemExit(
                    f"{workload}: only {elapsed} ms elapsed during a "
                    f"{profile['duration_ms']} ms {slice_id} run"
                )
            if not isinstance(run.get("duration_source"), str) or not run["duration_source"]:
                raise SystemExit(f"{workload}: the duration source is missing")


def check_rollout_qualifiable(result: dict, workload: str) -> None:
    """Refuse same-binary, diagnostic, or synthetic migration rollout evidence."""
    if result.get("schema_version") != ROLLOUT_RESULT_SCHEMA_VERSION:
        raise SystemExit(
            f"{workload}: unsupported rollout artifact schema "
            f"{result.get('schema_version')!r}"
        )
    run = result.get("run", {})
    if run.get("mode") != "qualification" or run.get("promotable") is not True:
        raise SystemExit(f"{workload}: rollout artifact is diagnostic, not promotable")
    if result.get("environment", {}).get("toolchain", {}).get("cargo_profile") != "release":
        raise SystemExit(f"{workload}: promotable rollout did not use the release profile")
    revisions = result.get("revisions", [])
    by_label = {
        revision.get("label"): revision
        for revision in revisions
        if isinstance(revision, dict)
    }
    expected = {"previous", "candidate-previous-config", "next"}
    if set(by_label) != expected or len(revisions) != len(expected):
        raise SystemExit(f"{workload}: rollout revision set is incomplete")
    previous = by_label["previous"]
    compatibility = by_label["candidate-previous-config"]
    candidate = by_label["next"]
    retained = run.get("retained_release", {})
    previous_binary = previous.get("binary", {})
    if (
        previous_binary.get("version") != "0.3.40"
        or candidate.get("binary", {}).get("version") != "0.4.0"
        or retained.get("expected_version") != previous_binary.get("version")
        or retained.get("expected_binary_sha256") != previous_binary.get("sha256")
        or not isinstance(retained.get("archive_sha256"), str)
        or len(retained["archive_sha256"]) != 64
    ):
        raise SystemExit(f"{workload}: retained release pin does not match the previous revision")
    loss = result.get("loss", {})
    usage_reconciliation = loss.get("usage_reconciliation", {})
    expected_usage_identities = loss.get("expected_usage_identities")
    expected_non_usage_trace_identities = usage_reconciliation.get(
        "expected_non_usage_trace_identities"
    )
    draining_refusal_attempts = loss.get("draining_refusal_attempts")
    otlp_trace_identities = usage_reconciliation.get("otlp_trace_identities")
    fleet = result.get("fleet")
    fleet_replicas = (
        sorted(member.get("id") for member in fleet)
        if isinstance(fleet, list)
        and fleet
        and all(
            isinstance(member, dict)
            and isinstance(member.get("id"), str)
            and member.get("id")
            for member in fleet
        )
        else None
    )
    if (
        usage_reconciliation.get("mode") != "exact_trace"
        or usage_reconciliation.get("retained_trace_context") != "loopback_otlp_http"
        or not isinstance(usage_reconciliation.get("exact_trace_replicas"), list)
        or fleet_replicas is None
        or len(set(fleet_replicas)) != len(fleet_replicas)
        or usage_reconciliation["exact_trace_replicas"] != fleet_replicas
        or usage_reconciliation.get("otlp_trace_export_replicas") != fleet_replicas
        or not isinstance(expected_usage_identities, list)
        or not isinstance(expected_non_usage_trace_identities, list)
        or not isinstance(draining_refusal_attempts, list)
        or not isinstance(otlp_trace_identities, list)
        or not otlp_trace_identities
        or any(
            not isinstance(identity, dict)
            or identity.get("replica") not in fleet_replicas
            or not isinstance(identity.get("trace_id"), str)
            or len(identity["trace_id"]) != 32
            or not identity["trace_id"].startswith("61786f6e642d726f")
            for identity in expected_usage_identities
        )
        or any(
            not isinstance(identity, dict)
            or identity.get("replica") not in fleet_replicas
            or not isinstance(identity.get("trace_id"), str)
            or len(identity["trace_id"]) != 32
            or not identity["trace_id"].startswith("61786f6e642d726f")
            for identity in otlp_trace_identities
        )
        or any(
            not isinstance(identity, dict)
            or identity.get("replica") not in fleet_replicas
            or not isinstance(identity.get("trace_id"), str)
            or len(identity["trace_id"]) != 32
            or not identity["trace_id"].startswith("61786f6e642d726f")
            or identity.get("reason") != "draining_refusal"
            for identity in expected_non_usage_trace_identities
        )
        or sorted(
            {
                identity["replica"]
                for identity in otlp_trace_identities
            }
        )
        != fleet_replicas
        or not isinstance(usage_reconciliation.get("otlp_trace_exports"), int)
        or isinstance(usage_reconciliation.get("otlp_trace_exports"), bool)
        or usage_reconciliation.get("otlp_trace_exports", 0) < len(fleet_replicas)
    ):
        raise SystemExit(
            f"{workload}: rollout does not prove exact trace reconciliation through its loopback OTLP receiver"
        )
    expected_usage_keys = {
        (identity["replica"], identity["trace_id"])
        for identity in expected_usage_identities
    }
    caller_request_count = loss.get("caller_requests")
    canonical_refusal_attempts = []
    seen_refusal_keys = set()
    seen_caller_refusals = set()
    for attempt in draining_refusal_attempts:
        if not isinstance(attempt, dict):
            raise SystemExit(f"{workload}: rollout drain-refusal attempt is malformed")
        caller_id = attempt.get("caller_id")
        trace_id = attempt.get("trace_id")
        refused_replica = attempt.get("refused_replica")
        accepted_replica = attempt.get("accepted_replica")
        accepted_status = attempt.get("accepted_status")
        refusal_key = (refused_replica, trace_id)
        caller_refusal = (caller_id, refused_replica)
        if (
            not isinstance(caller_request_count, int)
            or isinstance(caller_request_count, bool)
            or not isinstance(caller_id, int)
            or isinstance(caller_id, bool)
            or not 0 <= caller_id < caller_request_count
            or refused_replica not in fleet_replicas
            or accepted_replica not in fleet_replicas
            or refused_replica == accepted_replica
            or not isinstance(accepted_status, int)
            or isinstance(accepted_status, bool)
            or not 200 <= accepted_status < 300
            or not isinstance(trace_id, str)
            or len(trace_id) != 32
            or not trace_id.startswith("61786f6e642d726f")
            or refusal_key in expected_usage_keys
            or refusal_key in seen_refusal_keys
            or caller_refusal in seen_caller_refusals
            or (accepted_replica, trace_id) not in expected_usage_keys
        ):
            raise SystemExit(
                f"{workload}: rollout drain-refusal attempt is not canonical"
            )
        seen_refusal_keys.add(refusal_key)
        seen_caller_refusals.add(caller_refusal)
        canonical_refusal_attempts.append(
            {
                "caller_id": caller_id,
                "trace_id": trace_id,
                "refused_replica": refused_replica,
                "accepted_replica": accepted_replica,
                "accepted_status": accepted_status,
            }
        )
    canonical_refusal_attempts.sort(
        key=lambda attempt: (
            attempt["caller_id"],
            attempt["trace_id"],
            attempt["refused_replica"],
            attempt["accepted_replica"],
            attempt["accepted_status"],
        )
    )
    derived_non_usage_trace_identities = sorted(
        [
            {
                "replica": attempt["refused_replica"],
                "trace_id": attempt["trace_id"],
                "reason": "draining_refusal",
            }
            for attempt in canonical_refusal_attempts
        ],
        key=lambda identity: (
            identity["replica"],
            identity["trace_id"],
            identity["reason"],
        ),
    )
    if (
        draining_refusal_attempts != canonical_refusal_attempts
        or expected_non_usage_trace_identities
        != derived_non_usage_trace_identities
    ):
        raise SystemExit(
            f"{workload}: rollout non-usage traces do not match exact refusal attempts"
        )
    expected_otlp_trace_identities = sorted(
        [
            {"replica": identity.get("replica"), "trace_id": identity.get("trace_id")}
            for identity in expected_usage_identities
        ]
        + [
            {"replica": identity["replica"], "trace_id": identity["trace_id"]}
            for identity in expected_non_usage_trace_identities
        ],
        key=lambda identity: (identity["replica"], identity["trace_id"]),
    )
    if (
        len(
            {
                (identity["replica"], identity["trace_id"])
                for identity in expected_otlp_trace_identities
            }
        )
        != len(expected_otlp_trace_identities)
        or otlp_trace_identities != expected_otlp_trace_identities
    ):
        raise SystemExit(
            f"{workload}: rollout OTLP witness does not match its complete caller trace ledger"
        )
    if expected_non_usage_trace_identities:
        per_replica = loss.get("per_replica")
        if not isinstance(per_replica, list):
            raise SystemExit(
                f"{workload}: rollout drain-refusal trace ledger has no per-replica witness"
            )
        expected_refusals = {
            row.get("replica"): row.get("caller_requests_refused_while_draining")
            for row in per_replica
            if isinstance(row, dict)
            and isinstance(row.get("caller_requests_refused_while_draining"), int)
            and row.get("caller_requests_refused_while_draining") > 0
        }
        observed_refusals = Counter(
            identity["replica"] for identity in expected_non_usage_trace_identities
        )
        usage_trace_owners: dict[str, set[str]] = defaultdict(set)
        for identity in expected_usage_identities:
            usage_trace_owners[identity["trace_id"]].add(identity["replica"])
        if (
            dict(observed_refusals) != expected_refusals
            or loss.get("refusals_retried") != sum(observed_refusals.values())
            or any(
                not (
                    usage_trace_owners.get(identity["trace_id"], set())
                    - {identity["replica"]}
                )
                for identity in expected_non_usage_trace_identities
            )
        ):
            raise SystemExit(
                f"{workload}: rollout non-usage traces are not exact retried drain refusals"
            )
    digests = {revision.get("binary", {}).get("sha256") for revision in revisions}
    if None in digests or len(digests) != 2:
        raise SystemExit(f"{workload}: rollout did not use exactly two binary digests")
    desired_state_revisions = {
        revision.get("desired_state_revision") for revision in revisions
    }
    config_digests = {
        revision.get("config", {}).get("sha256") for revision in revisions
    }
    if (
        previous["binary"]["sha256"] == compatibility["binary"]["sha256"]
        or compatibility["binary"]["sha256"] != candidate["binary"]["sha256"]
        or previous.get("distinct_binary") is not False
        or compatibility.get("distinct_binary") is not True
        or candidate.get("distinct_binary") is not True
        or None in config_digests
        or len(config_digests) != 1
        or None in desired_state_revisions
        or "" in desired_state_revisions
        or len(desired_state_revisions) != 1
    ):
        raise SystemExit(
            f"{workload}: stateful binary/config/revision phases are inconsistent"
        )
    traffic = result.get("traffic", [])
    compatibility_traffic = [
        phase for phase in traffic if phase.get("phase") == "candidate-on-previous-config"
    ]
    if (
        len(compatibility_traffic) != 1
        or compatibility_traffic[0].get("answered", 0) <= 0
        or compatibility_traffic[0]
        .get("by_revision", {})
        .get("candidate-previous-config", 0)
        <= 0
    ):
        raise SystemExit(f"{workload}: candidate did not serve with previous config")
    mixed = result.get("mixed_version", {})
    if (
        mixed.get("shared_stateful_revision") not in desired_state_revisions
        or mixed.get("shared_alias") != "chat"
        or mixed.get("shared_alias") == mixed.get("exclusive_alias")
        or mixed.get("previous_serves_shared_alias") is not True
        or mixed.get("next_serves_shared_alias") is not True
    ):
        raise SystemExit(
            f"{workload}: both binaries did not serve the shared durable alias and revision"
        )
    matrix = result.get("migration", {}).get("matrix", {})
    if matrix.get("evaluated") is not True:
        raise SystemExit(f"{workload}: migration matrix was not evaluated")
    for command in (
        "previous_apply",
        "previous_status_before",
        "candidate_apply",
        "candidate_status_after",
    ):
        if matrix.get(command, {}).get("succeeded") is not True:
            raise SystemExit(f"{workload}: migration matrix command {command} did not pass")
    previous_versions = matrix.get("previous_versions")
    candidate_versions = matrix.get("candidate_versions")
    if (
        not isinstance(previous_versions, list)
        or not previous_versions
        or not isinstance(candidate_versions, list)
        or candidate_versions[: len(previous_versions)] != previous_versions
    ):
        raise SystemExit(f"{workload}: candidate migration ledger does not extend retained rows")
    added = matrix.get("candidate_added_versions")
    recomputed_added = [
        migration.get("version")
        for migration in candidate_versions[len(previous_versions) :]
        if isinstance(migration, dict)
    ]
    if added != recomputed_added:
        raise SystemExit(f"{workload}: migration added-version ledger is not exact")
    classification = matrix.get("classification")
    forward_only = bool(added)
    if classification != ("forward-only" if forward_only else "unchanged"):
        raise SystemExit(f"{workload}: migration classification disagrees with ledger")
    candidate_before = matrix.get("candidate_status_before", {})
    if candidate_before.get("succeeded") is not (not forward_only):
        raise SystemExit(f"{workload}: candidate pre-apply status disagrees with ledger")
    if forward_only and "migration(s) pending" not in candidate_before.get("output", ""):
        raise SystemExit(f"{workload}: candidate did not name its pending migrations")
    previous_after = matrix.get("previous_status_after_candidate", {})
    fence = result.get("rollback", {}).get("migrated_layout_fence", {})
    rollback = result.get("rollback", {}).get("compatible_patch_rollback", {})
    if forward_only:
        valid = (
            previous_after.get("succeeded") is False
            and fence.get("expected_refused") is True
            and fence.get("refused") is True
            and fence.get("refusal_names_newer_build") is True
            and rollback.get("performed") is False
        )
    else:
        valid = (
            previous_after.get("succeeded") is True
            and fence.get("expected_refused") is False
            and fence.get("refused") is False
            and rollback.get("performed") is True
            and rollback.get("served_traffic") is True
        )
    if not valid:
        raise SystemExit(f"{workload}: rollback evidence contradicts migration classification")


def check_stateful_endurance_exact(result: dict, workload: str) -> None:
    if result.get("schema_version") != STATEFUL_ENDURANCE_RESULT_SCHEMA_VERSION:
        raise SystemExit(
            f"{workload}: unsupported stateful endurance artifact schema "
            f"{result.get('schema_version')!r}"
        )
    usage = result.get("usage", {})
    for field in (
        "request_identities",
        "correlations",
        "durable_identities",
        "durable_outside_identities",
        "correlation_windows",
    ):
        evidence = usage.get(field, {})
        if evidence.get("exact") is not True or not evidence.get("path"):
            raise SystemExit(f"{workload}: {field} is not exact retained evidence")
    for field in (
        "missing",
        "unexpected_records",
        "unexpected_statuses",
        "concurrent_ending_membership_mismatches",
        "unidentified",
        "uncorrelated",
        "refusal_records",
        "durable_loss_outside_windows",
        "durable_unexpected_rows",
    ):
        if usage.get(field) != 0:
            raise SystemExit(f"{workload}: stateful reconciliation has nonzero {field}")


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
        f"schema_version = {ROLLOUT_RECORD_SCHEMA_VERSION if slice_id == 'rollout' else 1}",
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
            if slice_id in ("endurance", "stateful-endurance"):
                lines.append(f"artifact_schema_version = {result['schema_version']}")
            if slice_id == "stateful-endurance":
                for field, claim in stateful_ledger_claims(
                    result, generic_id(result, slice_id)
                ).items():
                    lines += [
                        f"{field}_sha256 = {toml_string(str(claim['sha256']))}",
                        f"{field}_files = {claim['files']}",
                        f"{field}_bytes = {claim['bytes']}",
                    ]
            if slice_id == "endurance":
                for field, claim in endurance_ledger_claims(
                    result, generic_id(result, slice_id)
                ).items():
                    lines += [
                        f"{field}_sha256 = {toml_string(str(claim['sha256']))}",
                        f"{field}_files = {claim['files']}",
                        f"{field}_bytes = {claim['bytes']}",
                    ]
            claim = sample_claim(result, generic_id(result, slice_id))
            lines += [
                f"samples_sha256 = {toml_string(str(claim['sha256']))}",
                f"samples_files = {claim['files']}",
                f"samples_bytes = {claim['bytes']}",
            ]
        if slice_id == "fault":
            lines.append(f"artifact_schema_version = {result['schema_version']}")
        if slice_id == "rollout":
            revisions = {revision["label"]: revision for revision in result["revisions"]}
            retained = result["run"]["retained_release"]
            mixed = result["mixed_version"]
            reconciliation = result["loss"]["usage_reconciliation"]
            trace_identities_sha256 = hashlib.sha256(
                json.dumps(
                    reconciliation["otlp_trace_identities"],
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()
            lines += [
                f"artifact_schema_version = {result['schema_version']}",
                f"rollout_previous_version = {toml_string(revisions['previous']['binary']['version'])}",
                f"rollout_previous_binary_sha256 = {toml_string(revisions['previous']['binary']['sha256'])}",
                f"rollout_candidate_version = {toml_string(revisions['next']['binary']['version'])}",
                f"rollout_candidate_binary_sha256 = {toml_string(revisions['next']['binary']['sha256'])}",
                f"rollout_retained_archive_sha256 = {toml_string(retained['archive_sha256'])}",
                f"rollout_shared_stateful_revision = {toml_string(mixed['shared_stateful_revision'])}",
                f"rollout_shared_alias = {toml_string(mixed['shared_alias'])}",
                "rollout_previous_serves_shared_alias = "
                f"{str(mixed['previous_serves_shared_alias']).lower()}",
                "rollout_candidate_serves_shared_alias = "
                f"{str(mixed['next_serves_shared_alias']).lower()}",
                f"rollout_usage_reconciliation = {toml_string(reconciliation['mode'])}",
                f"rollout_exact_trace_replicas = {len(reconciliation['exact_trace_replicas'])}",
                f"rollout_retained_trace_context = {toml_string(reconciliation['retained_trace_context'])}",
                f"rollout_otlp_trace_exports = {reconciliation['otlp_trace_exports']}",
                "rollout_otlp_trace_export_replicas = "
                f"{len(reconciliation['otlp_trace_export_replicas'])}",
                "rollout_otlp_trace_identities = "
                f"{len(reconciliation['otlp_trace_identities'])}",
                "rollout_otlp_trace_identities_sha256 = "
                f"{toml_string(trace_identities_sha256)}",
            ]
    return "\n".join(lines) + "\n"


def self_test() -> int:
    """Exercise the generic record's completeness and provenance refusals."""
    assert (
        cpu_model_from(
            "Hardware : Example Board\n"
            "model name : Example CPU\n"
            "CPU implementer : 0x61\n"
            "CPU part : 0x000\n"
        )
        == "Example CPU"
    )
    assert cpu_model_from("Hardware : Example Board\n") == "Example Board"
    assert (
        cpu_model_from("CPU implementer : 0x61\nCPU part : 0x000\n")
        == "CPU implementer 0x61, part 0x000"
    )
    assert cpu_model_from("CPU implementer : 0x61\n") is None
    assert (
        cpu_model_from(
            "processor : 7\n"
            "CPU implementer : 0x61\n"
            "CPU part : 0x000\n"
            "\n"
            "processor : 3\n"
            "CPU implementer : 0x41\n"
            "CPU part : 0xD03\n"
        )
        == "CPU implementer 0x41, part 0xD03; "
        "CPU implementer 0x61, part 0x000"
    )
    assert (
        cpu_model_from(
            "processor : 0\n"
            "CPU implementer : 0x61\n"
            "CPU part : 0x000\n"
            "processor : 1\n"
            "CPU implementer : 0x61\n"
            "CPU part : 0x000\n"
        )
        == "CPU implementer 0x61, part 0x000"
    )
    assert (
        cpu_model_from(
            "processor : 0\n"
            "CPU implementer : 0x61\n"
            "CPU part : 0x000\n"
            "\n"
            "processor : 1\n"
            "CPU implementer : 0x41\n"
        )
        is None
    )
    assert (
        cpu_model_from(
            "processor : 0\n"
            "CPU implementer : 0x61\n"
            "CPU implementer : 0x41\n"
            "CPU part : 0x000\n"
        )
        is None
    )

    capacity_result = {
        "schema_version": CAPACITY_RESULT_SCHEMA_VERSION,
        "profile": {
            "id": "elapsed-contract",
            "tier": "heavy",
            "concurrency": 1,
            "requests": 1,
        },
        "run": {"elapsed_ms": 1001},
        "throughput": {
            "offered": 1,
            "accepted": 1,
            "rejected": 0,
            "errors": 0,
            "elapsed_ms": 1000,
            "accepted_rps": 1.0,
        },
        "latency_ms": {"p50": 1.0, "p95": 1.0, "p99": 1.0},
        "resources": {
            "rss_kib": {"baseline": 1, "peak": 2, "settled": 2},
            "sockets": {"peak": 1},
            "cpu_seconds": 0.01,
        },
        "usage_records": {"missing": 0},
        "occupancy": {"admission_max_in_flight": None},
        "upstream": {"streams_open_at_end": 0},
        "verdicts": [{"passed": True}],
        "environment": {
            "source": {
                "git_commit": "commit",
                "git_dirty": False,
                "crate_version": "0.0.0",
            },
            "binary": {"sha256": "binary", "version": "0.0.0"},
            "toolchain": {"cargo_profile": "release", "rustc": "rustc test"},
            "manifest": {"path": "manifest.toml", "sha256": "manifest"},
            "config": {"sha256": "config"},
            "fixtures": [],
            "hardware": {
                "os": "test",
                "arch": "test",
                "kernel": "kernel",
                "cpu_model": "cpu",
                "cpus": 1,
                "total_memory_kib": 1,
                "containerized": False,
            },
        },
    }
    with tempfile.TemporaryDirectory() as capacity_directory:
        capacity_path = Path(capacity_directory) / "elapsed-contract.json"
        capacity_path.write_text(json.dumps(capacity_result), encoding="utf-8")
        capacity_result["_artifact_path"] = str(capacity_path)
        capacity_record = tomllib.loads(
            render([capacity_result], "github-actions", "capacity self-test")
        )
        assert capacity_record["profile"][0]["elapsed_ms"] == 1001

    manifest_relative = GENERIC_MANIFESTS["rollout"]
    manifest_bytes = (ROOT / manifest_relative).read_bytes()
    environment = {
        "source": {"git_commit": "commit", "git_dirty": False, "crate_version": "0.0.0"},
        "toolchain": {"cargo_profile": "release", "rustc": "rustc test"},
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
        "schema_version": ROLLOUT_RESULT_SCHEMA_VERSION,
        "scenario": {"id": "rolling-replace", "tier": "heavy"},
        "run": {
            "elapsed_ms": 1,
            "mode": "qualification",
            "promotable": True,
            "retained_release": {
                "expected_version": "0.3.40",
                "expected_binary_sha256": "previous",
                "archive_sha256": "a" * 64,
            },
        },
        "environment": environment,
        "revisions": [
            {
                "label": "previous",
                "binary": {"sha256": "previous", "version": "0.3.40"},
                "config": {"sha256": "shared-bootstrap"},
                "distinct_binary": False,
                "desired_state_revision": "rev_shared",
            },
            {
                "label": "candidate-previous-config",
                "binary": {"sha256": "candidate", "version": "0.4.0"},
                "config": {"sha256": "shared-bootstrap"},
                "distinct_binary": True,
                "desired_state_revision": "rev_shared",
            },
            {
                "label": "next",
                "binary": {"sha256": "candidate", "version": "0.4.0"},
                "config": {"sha256": "shared-bootstrap"},
                "distinct_binary": True,
                "desired_state_revision": "rev_shared",
            },
        ],
        "traffic": [
            {
                "phase": "candidate-on-previous-config",
                "answered": 1,
                "by_revision": {"candidate-previous-config": 1},
            }
        ],
        "fleet": [
            {
                "id": "candidate-0",
                "revision": "next",
            }
        ],
        "mixed_version": {
            "exclusive_alias": "chat-next-only",
            "shared_stateful_revision": "rev_shared",
            "shared_alias": "chat",
            "previous_serves_shared_alias": True,
            "next_serves_shared_alias": True,
        },
        "loss": {
            "caller_requests": 1,
            "draining_refusal_attempts": [],
            "expected_usage_identities": [
                {
                    "replica": "candidate-0",
                    "trace_id": "61786f6e642d726f0000000000000001",
                    "status": "ok",
                }
            ],
            "usage_reconciliation": {
                "mode": "exact_trace",
                "exact_trace_replicas": ["candidate-0"],
                "retained_trace_context": "loopback_otlp_http",
                "otlp_trace_exports": 1,
                "otlp_trace_export_replicas": ["candidate-0"],
                "expected_non_usage_trace_identities": [],
                "otlp_trace_identities": [
                    {
                        "replica": "candidate-0",
                        "trace_id": "61786f6e642d726f0000000000000001",
                    }
                ],
            }
        },
        "migration": {
            "matrix": {
                "evaluated": True,
                "previous_apply": {"succeeded": True},
                "previous_status_before": {"succeeded": True},
                "candidate_status_before": {"succeeded": True},
                "candidate_apply": {"succeeded": True},
                "candidate_status_after": {"succeeded": True},
                "previous_status_after_candidate": {"succeeded": True},
                "previous_versions": [
                    {"version": 1, "name": "base", "checksum": "checksum"}
                ],
                "candidate_versions": [
                    {"version": 1, "name": "base", "checksum": "checksum"}
                ],
                "candidate_added_versions": [],
                "classification": "unchanged",
            }
        },
        "rollback": {
            "migrated_layout_fence": {
                "expected_refused": False,
                "refused": False,
            },
            "compatible_patch_rollback": {
                "performed": True,
                "served_traffic": True,
            },
        },
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
        assert (
            parsed["observation"][0]["artifact_schema_version"]
            == ROLLOUT_RESULT_SCHEMA_VERSION
        )
        assert parsed["schema_version"] == ROLLOUT_RECORD_SCHEMA_VERSION
        assert (
            parsed["observation"][0]["rollout_shared_stateful_revision"]
            == "rev_shared"
        )
        assert parsed["observation"][0]["rollout_shared_alias"] == "chat"
        assert parsed["observation"][0]["rollout_previous_serves_shared_alias"] is True
        assert parsed["observation"][0]["rollout_candidate_serves_shared_alias"] is True
        assert parsed["inputs"]["fixtures"] == 0

        retried = copy.deepcopy(result)
        retried["fleet"].append({"id": "previous-0", "revision": "previous"})
        accepted_trace = "61786f6e642d726f0000000000000002"
        retried["loss"]["caller_requests"] = 2
        retried["loss"]["expected_usage_identities"].append(
            {"replica": "previous-0", "trace_id": accepted_trace, "status": "ok"}
        )
        retried["loss"]["draining_refusal_attempts"] = [
            {
                "caller_id": 1,
                "trace_id": accepted_trace,
                "refused_replica": "candidate-0",
                "accepted_replica": "previous-0",
                "accepted_status": 200,
            }
        ]
        retried["loss"]["per_replica"] = [
            {
                "replica": "candidate-0",
                "caller_requests_refused_while_draining": 1,
            },
            {
                "replica": "previous-0",
                "caller_requests_refused_while_draining": 0,
            },
        ]
        retried["loss"]["refusals_retried"] = 1
        retried_reconciliation = retried["loss"]["usage_reconciliation"]
        retried_reconciliation["exact_trace_replicas"] = [
            "candidate-0",
            "previous-0",
        ]
        retried_reconciliation["otlp_trace_exports"] = 2
        retried_reconciliation["otlp_trace_export_replicas"] = [
            "candidate-0",
            "previous-0",
        ]
        retried_reconciliation["expected_non_usage_trace_identities"] = [
            {
                "replica": "candidate-0",
                "trace_id": accepted_trace,
                "reason": "draining_refusal",
            }
        ]
        retried_reconciliation["otlp_trace_identities"] = [
            {
                "replica": "candidate-0",
                "trace_id": "61786f6e642d726f0000000000000001",
            },
            {"replica": "candidate-0", "trace_id": accepted_trace},
            {"replica": "previous-0", "trace_id": accepted_trace},
        ]
        check_rollout_qualifiable(retried, "retried refusal self-test")

        for name, mutate in (
            (
                "missing durable revision",
                lambda candidate: candidate["revisions"][0].update(
                    desired_state_revision=None
                ),
            ),
            (
                "mismatched durable revision",
                lambda candidate: candidate["revisions"][2].update(
                    desired_state_revision="rev_other"
                ),
            ),
            (
                "wrong shared alias",
                lambda candidate: candidate["mixed_version"].update(
                    shared_alias="other"
                ),
            ),
            (
                "previous binary did not serve",
                lambda candidate: candidate["mixed_version"].update(
                    previous_serves_shared_alias=False
                ),
            ),
            (
                "candidate binary did not serve",
                lambda candidate: candidate["mixed_version"].update(
                    next_serves_shared_alias=False
                ),
            ),
        ):
            invalid = copy.deepcopy(result)
            mutate(invalid)
            try:
                render_generic([invalid], "rollout", "heavy", "local", name)
            except SystemExit:
                pass
            else:
                raise AssertionError(f"rollout accepted {name}")

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
                "duration_ms": endurance_duration,
                "manifest_duration_ms": endurance_duration,
            },
            "run": {
                "elapsed_ms": endurance_duration,
                "requested_duration_ms": endurance_duration,
                "duration_source": "environment",
                "samples_path": "endurance.samples.jsonl",
            },
            "environment": endurance_environment,
            "reconciliation": {
                "unexpected_records": 0,
                "request_identities": {
                    "exact": True,
                    "path": "endurance-request-ledger",
                    "shards": STATEFUL_LEDGER_SHARDS,
                    "recorded": 1,
                    "peak_shard_rows": 1,
                },
                "correlations": {
                    "exact": True,
                    "path": "endurance-correlation-ledger",
                    "shards": STATEFUL_LEDGER_SHARDS,
                    "expected": 1,
                    "observed": 1,
                    "peak_shard_rows": 2,
                },
            },
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
        request_identity = bytearray(16)
        request_identity[6] = 0x70
        request_identity[8] = 0x80
        request_identity[15] = 1
        request_identity_bytes = bytes(request_identity)
        endurance_request_ledger = path.parent / "endurance-request-ledger"
        endurance_correlation_ledger = path.parent / "endurance-correlation-ledger"
        endurance_request_ledger.mkdir()
        endurance_correlation_ledger.mkdir()
        for shard in range(STATEFUL_LEDGER_SHARDS):
            (endurance_request_ledger / f"request-shard-{shard:02}.bin").write_bytes(
                request_identity_bytes if shard == 1 else b""
            )
            (endurance_correlation_ledger / f"expected-shard-{shard:02}.bin").write_bytes(
                bytes(17) if shard == 0 else b""
            )
            (endurance_correlation_ledger / f"observed-shard-{shard:02}.bin").write_bytes(
                bytes(17) if shard == 0 else b""
            )
        (path.parent / "endurance.samples.jsonl").write_text(
            '{"at_ms":0,"rss_kib":1,"cpu_ticks":0,"fds":1,"sockets":0}\n',
            encoding="utf-8",
        )
        rendered = render_generic(
            [endurance_result], "endurance", "soak", "local", "endurance self-test"
        )
        parsed = tomllib.loads(rendered)
        assert parsed["observation"][0]["duration_ms"] == endurance_duration
        assert (
            parsed["observation"][0]["artifact_schema_version"]
            == ENDURANCE_RESULT_SCHEMA_VERSION
        )

        short_endurance = copy.deepcopy(endurance_result)
        short_endurance["profile"]["duration_ms"] = endurance_duration - 1
        short_endurance["run"]["elapsed_ms"] = endurance_duration - 1
        short_endurance["run"]["requested_duration_ms"] = endurance_duration - 1
        try:
            render_generic(
                [short_endurance], "endurance", "soak", "local", "short soak"
            )
        except SystemExit:
            pass
        else:
            raise AssertionError("a shortened endurance result was accepted")

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
            "schema_version": STATEFUL_ENDURANCE_RESULT_SCHEMA_VERSION,
            "profile": {
                "id": "mixed-stateful-endurance",
                "tier": "soak",
                "seed": 1,
                "duration_ms": stateful_duration,
                "manifest_duration_ms": stateful_duration,
            },
            "run": {
                "elapsed_ms": stateful_duration,
                "duration_source": "manifest",
                "samples_paths": [
                    "stateful-replica-0.samples.jsonl",
                    "stateful-replica-1.samples.jsonl",
                ],
            },
            "environment": stateful_environment,
            "usage": {
                "missing": 0,
                "unexpected_records": 0,
                "unexpected_statuses": 0,
                "concurrent_endings": 0,
                "concurrent_ending_membership_mismatches": 0,
                "unidentified": 0,
                "uncorrelated": 0,
                "refusal_records": 0,
                "durable_loss_outside_windows": 0,
                "durable_unexpected_rows": 0,
                "request_identities": {
                    "exact": True,
                    "path": "request-ledger",
                    "shards": STATEFUL_LEDGER_SHARDS,
                },
                "correlations": {
                    "exact": True,
                    "path": "correlation-ledger",
                    "shards": STATEFUL_LEDGER_SHARDS,
                },
                "durable_identities": {
                    "exact": True,
                    "path": "durable-ledger",
                    "shards": STATEFUL_LEDGER_SHARDS,
                },
                "durable_outside_identities": {
                    "exact": True,
                    "path": "durable-outside-ledger",
                    "shards": STATEFUL_LEDGER_SHARDS,
                },
                "correlation_windows": {
                    "exact": True,
                    "path": "correlation-window-ledger",
                    "shards": STATEFUL_LEDGER_SHARDS,
                },
            },
            "verdicts": [{"passed": True}],
        }
        stateful_result["_artifact_path"] = str(path)
        for sample_name in stateful_result["run"]["samples_paths"]:
            (path.parent / sample_name).write_text(
                '{"at_ms":0,"rss_kib":1,"cpu_ticks":0,"fds":1,"sockets":0}\n',
                encoding="utf-8",
            )
        for field, files_per_shard in STATEFUL_LEDGER_FIELDS:
            ledger = path.parent / stateful_result["usage"][field]["path"]
            ledger.mkdir()
            for stem in STATEFUL_LEDGER_STEMS[field]:
                for shard in range(STATEFUL_LEDGER_SHARDS):
                    width = STATEFUL_LEDGER_WIDTHS[field]
                    payload = bytes(width) if shard == 0 else b""
                    (ledger / f"{stem}-shard-{shard:02}.bin").write_bytes(payload)
            for count_field in STATEFUL_LEDGER_COUNTS[field]:
                stateful_result["usage"][field][count_field] = 1
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
        assert (
            parsed["observation"][0]["artifact_schema_version"]
            == STATEFUL_ENDURANCE_RESULT_SCHEMA_VERSION
        )
        for field, files_per_shard in STATEFUL_LEDGER_FIELDS:
            assert (
                parsed["observation"][0][f"{field}_files"]
                == STATEFUL_LEDGER_SHARDS * files_per_shard
            )
            assert len(parsed["observation"][0][f"{field}_sha256"]) == 64
        assert parsed["observation"][0]["samples_files"] == 2

        inexact_correlation_windows = copy.deepcopy(stateful_result)
        inexact_correlation_windows["usage"]["correlation_windows"]["exact"] = False
        try:
            render_generic(
                [inexact_correlation_windows],
                "stateful-endurance",
                "soak",
                "github-actions",
                "inexact stateful correlation windows",
            )
        except SystemExit:
            pass
        else:
            raise AssertionError("inexact stateful correlation-window evidence was accepted")

        mismatched_correlation_windows = copy.deepcopy(stateful_result)
        mismatched_correlation_windows["usage"][
            "concurrent_ending_membership_mismatches"
        ] = 1
        try:
            render_generic(
                [mismatched_correlation_windows],
                "stateful-endurance",
                "soak",
                "github-actions",
                "mismatched stateful correlation windows",
            )
        except SystemExit:
            pass
        else:
            raise AssertionError(
                "stateful correlation-window membership mismatches were accepted"
            )

        short_stateful = dict(stateful_result)
        short_stateful["profile"] = dict(stateful_result["profile"])
        short_stateful["run"] = dict(stateful_result["run"])
        short_stateful["profile"]["duration_ms"] = stateful_duration - 1
        short_stateful["run"]["elapsed_ms"] = stateful_duration - 1
        try:
            render_generic(
                [short_stateful],
                "stateful-endurance",
                "soak",
                "github-actions",
                "short stateful endurance",
            )
        except SystemExit:
            pass
        else:
            raise AssertionError("a shortened stateful endurance result was accepted")

        missing_ledger = path.parent / "request-ledger" / "request-shard-00.bin"
        missing_ledger.unlink()
        try:
            render_generic(
                [stateful_result],
                "stateful-endurance",
                "soak",
                "github-actions",
                "missing stateful ledger",
            )
        except SystemExit:
            pass
        else:
            raise AssertionError("stateful evidence missing an exact shard was accepted")

        recovery_expected = recovery_expected_stages(ROOT / RECOVERY_MANIFEST)
        recovery_provenance = {
            "source": {"git_commit": "commit", "git_dirty": False, "crate_version": "0.0.0"},
            "binary": {
                "sha256": "b" * 64,
                "version": "0.0.0",
                "cargo_profile": "release",
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

        def satisfy_operand(
            operand: tuple[str, ...], desired: Any, observations: dict[str, Any]
        ) -> None:
            operation, *arguments = operand
            if operation == "literal":
                return
            if operation == "observation":
                observations[arguments[0]] = desired
                return
            if operation in {"all_positive", "positive"}:
                for key in arguments:
                    observations[key] = 1 if desired == "true" else 0
                return
            if operation in {"canonical_request_id", "canonical_request_id_pair"}:
                identities = (
                    "req_aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                    "req_bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                )
                for index, key in enumerate(arguments):
                    observations[key] = identities[index]
                return
            if operation == "distinct":
                observations[arguments[0]] = (
                    "req_aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
                )
                observations[arguments[1]] = (
                    "req_bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
                )
                return
            if operation == "accepted_revision":
                observations[arguments[0]] = (
                    "refused" if desired == "refused" else "rev-self-test"
                )
                return
            if operation == "boolean_label":
                observations[arguments[0]] = desired == arguments[1]
                return
            if operation == "positive_label":
                observations[arguments[0]] = 1 if desired == arguments[1] else 0
                return
            if operation == "null_label":
                observations[arguments[0]] = (
                    None if desired == arguments[1] else "present"
                )
                return
            if operation == "zero_if_equal_pairs":
                for index in range(0, len(arguments), 2):
                    observations[arguments[index]] = "same"
                    observations[arguments[index + 1]] = "same"
                return
            if operation == "zero_if_equal_pairs_and_literal":
                for index in range(0, len(arguments) - 2, 2):
                    observations[arguments[index]] = "same"
                    observations[arguments[index + 1]] = "same"
                observations[arguments[-2]] = arguments[-1]
                return
            if operation.startswith("http_"):
                if operation == "http_unauthenticated_successes":
                    observations[arguments[0]] = 200 if desired == "1" else 401
                else:
                    success = desired in {"0", "0.0", "serves", "accepted"}
                    observations[arguments[0]] = 200 if success else 503
                return
            raise AssertionError(f"unsupported synthetic recovery operand {operand!r}")

        def default_operand_value(operand: tuple[str, ...]) -> str:
            operation, *arguments = operand
            if operation == "literal":
                return arguments[0]
            if operation in {
                "all_positive",
                "positive",
                "canonical_request_id",
                "canonical_request_id_pair",
                "distinct",
            }:
                return "true"
            if operation == "accepted_revision":
                return "accepted"
            if operation in {"boolean_label", "positive_label", "null_label"}:
                return arguments[1]
            return "self-test"

        with tempfile.TemporaryDirectory() as recovery_directory:
            for key, contract in recovery_expected.items():
                scenario, stage = key.split("/", 1)
                contract_scenario = contract["scenario"]
                contract_stage = contract["stage"]
                observations = {
                    name: "self-test"
                    for name in required_observations(
                        contract_scenario, contract_stage
                    )
                }
                for name in STAGE_REQUIRED_NULL_OBSERVATIONS.get(
                    key, frozenset()
                ):
                    observations[name] = None
                check_bindings = CHECK_RECONSTRUCTIONS.get(key, {})
                for check in sorted(required_checks(contract_scenario, contract_stage)):
                    expected_operand, observed_operand = check_bindings[check]
                    desired = (
                        observations[expected_operand[1]]
                        if expected_operand[0] == "observation"
                        else default_operand_value(expected_operand)
                    )
                    satisfy_operand(expected_operand, desired, observations)
                    satisfy_operand(observed_operand, desired, observations)
                gate_bindings = GATE_RECONSTRUCTIONS.get(key, {})
                for gate, operand in gate_bindings.items():
                    desired = (
                        "0"
                        if gate.startswith("max_")
                        else str(contract_scenario["gate"][gate])
                    )
                    satisfy_operand(operand, desired, observations)

                gates = []
                for gate in REQUIRED_GATE_NAMES:
                    bound = str(contract_scenario["gate"][gate])
                    if gate_owner(contract_scenario, gate) == stage:
                        observed = reconstruct_required_gate(
                            contract_scenario, contract_stage, gate, observations
                        )
                        outcome = derive_verdict_outcome(
                            "gate", gate, bound, observed
                        )
                        detail = "the synthetic fixture evaluates its owned gate"
                    else:
                        observed = "not measured"
                        outcome = "not_evaluated"
                        detail = deferred_gate_detail(
                            gate,
                            contract_stage["evidence"],
                            "the synthetic fixture assigns this gate to its owner",
                        )
                    gates.append(
                        {
                            "gate": gate,
                            "bound": bound,
                            "observed": observed,
                            "outcome": outcome,
                            "detail": detail,
                        }
                    )

                checks = []
                for check in sorted(required_checks(contract_scenario, contract_stage)):
                    bound, observed = reconstruct_required_check(
                        contract_scenario, contract_stage, check, observations
                    )
                    checks.append(
                        {
                            "gate": check,
                            "bound": bound,
                            "observed": observed,
                            "outcome": "met" if bound == observed else "failed",
                            "detail": "the synthetic fixture retains the reconstructed check",
                        }
                    )
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
                        "axond_executable_sha256": "b" * 64,
                        "cargo_profile": "release",
                    },
                    "timeline": [{"at_ms": 0, "event": "complete", "detail": "test"}],
                    "observations": observations,
                    "gates": gates,
                    "checks": checks,
                }
                if contract["driver"] in {"stateful-integration", "restore-drill"}:
                    result["run"].update(
                        {
                            "axond_executed_sha256": "b" * 64,
                            "axond_executable_path": "/workspace/target/release/axond",
                            "axond_execution_bound": True,
                        }
                    )
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
            assert agreed_recovery_cargo_profile(recovery_results) == "release"
            assert len(parsed["stage"]) == len(recovery_expected)
            assert all(
                stage["artifact_schema_version"] == RECOVERY_RESULT_SCHEMA_VERSION
                and stage["binary_sha256"] == "b" * 64
                and stage["driver"] in {"stateful-integration", "restore-drill"}
                for stage in parsed["stage"]
            )
            mixed_profiles = copy.deepcopy(recovery_results)
            mixed_profiles[0]["run"]["cargo_profile"] = "debug"
            try:
                render_recovery(
                    mixed_profiles,
                    "github-actions",
                    "mixed-profile qualification self-test",
                    Path("downloaded-recovery-binary"),
                    recovery_provenance,
                )
            except SystemExit:
                pass
            else:
                raise AssertionError("disagreeing recovery Cargo profiles were accepted")
            wrong_profile_provenance = copy.deepcopy(recovery_provenance)
            wrong_profile_provenance["binary"]["cargo_profile"] = "debug"
            try:
                render_recovery(
                    recovery_results,
                    "github-actions",
                    "downloaded-path qualification self-test",
                    Path("downloaded-recovery-binary"),
                    wrong_profile_provenance,
                )
            except SystemExit:
                pass
            else:
                raise AssertionError(
                    "recovery provenance disagreeing with raw profiles was accepted"
                )
            try:
                check_recovery_complete(recovery_results[:-1], ROOT / RECOVERY_MANIFEST)
            except SystemExit:
                pass
            else:
                raise AssertionError("a partial recovery stage set was accepted")
            failed = copy.deepcopy(recovery_results[0])
            failed["gates"][0].update(observed="1", outcome="failed")
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
