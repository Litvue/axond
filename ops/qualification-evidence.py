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
    "fault": "qualification/faults/manifest.toml",
}

ENDURANCE_RESULT_SCHEMA_VERSION = 4
FAULT_RESULT_SCHEMA_VERSION = 1
ENDURANCE_SURPLUS_VERDICT = "max_unexpected_usage_records"








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








def generic_id(result: dict, slice_id: str) -> str:
    if slice_id == "fault":
        return result["row"]["id"]
    return result["profile"]["id"]


def generic_tier(result: dict, slice_id: str) -> str | None:
    if slice_id == "fault":
        return None
    return result["profile"]["tier"]


def binary_identity(result: dict) -> dict[str, str]:
    """The binary block every remaining slice writes on the artifact."""
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
        # Request-path evidence: provider and transport rows. Redis is retired
        # (ADR 0063); Postgres HA is optional and not this set.
        return {
            row["id"]
            for row in manifest["row"]
            if row.get("family") != "backend"
        }
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
        if slice_id == "endurance":
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






def check_one_run_generic(results: list[dict]) -> None:
    readers: dict[str, Callable[[dict], Any]] = {
        "the source": lambda result: result["environment"]["source"],
        "the binary": binary_identity,
        "the toolchain": lambda result: result["environment"]["toolchain"],
        "the manifest": lambda result: result["environment"]["manifest"],
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
        if slice_id == "endurance":
            profile = result["profile"]
            run = result["run"]
            lines += [
                f"duration_ms = {profile['duration_ms']}",
                f"manifest_duration_ms = {profile['manifest_duration_ms']}",
                f"requested_duration_ms = {run.get('requested_duration_ms', profile['duration_ms'])}",
                f"duration_source = {toml_string(run['duration_source'])}",
            ]
            lines.append(f"artifact_schema_version = {result['schema_version']}")
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

    manifest_relative = GENERIC_MANIFESTS["endurance"]
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
    fault_manifest = tomllib.loads(
        (ROOT / GENERIC_MANIFESTS["fault"]).read_text(encoding="utf-8")
    )
    expected_fault = expected_generic_ids("fault", ROOT / GENERIC_MANIFESTS["fault"])
    manifest_fault = {row["id"] for row in fault_manifest["row"]}
    retired = manifest_fault - expected_fault
    assert all(row.get("family") != "backend" for row in fault_manifest["row"] if row["id"] in expected_fault)
    assert any(row_id.startswith("redis-") for row_id in retired), retired
    assert "provider-rate-limited" in expected_fault

    with tempfile.TemporaryDirectory() as endurance_directory:
        endurance_dir = Path(endurance_directory)
        endurance_path = endurance_dir / "mixed-endurance.json"
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
        endurance_path.write_text(json.dumps(endurance_result), encoding="utf-8")
        endurance_result["_artifact_path"] = str(endurance_path)
        request_identity = bytearray(16)
        request_identity[6] = 0x70
        request_identity[8] = 0x80
        request_identity[15] = 1
        request_identity_bytes = bytes(request_identity)
        endurance_request_ledger = endurance_dir / "endurance-request-ledger"
        endurance_correlation_ledger = endurance_dir / "endurance-correlation-ledger"
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
        (endurance_dir / "endurance.samples.jsonl").write_text(
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

    with tempfile.TemporaryDirectory() as fault_directory:
        fault_dir = Path(fault_directory)
        fault_environment = dict(environment)
        fault_environment["manifest"] = {
            "path": GENERIC_MANIFESTS["fault"],
            "sha256": hashlib.sha256(
                (ROOT / GENERIC_MANIFESTS["fault"]).read_bytes()
            ).hexdigest(),
        }
        fault_environment["binary"] = {"sha256": "binary", "version": "0.0.0"}
        fault_results = []
        for row in fault_manifest["row"]:
            if row.get("family") == "backend":
                continue
            result = {
                "schema_version": FAULT_RESULT_SCHEMA_VERSION,
                "row": {
                    "id": row["id"],
                    "family": row["family"],
                    "fault": row["fault"],
                },
                "run": {"elapsed_ms": 1},
                "environment": fault_environment,
                "verdicts": [{"passed": True}],
            }
            path = fault_dir / f"{row['id']}.json"
            path.write_text(json.dumps(result), encoding="utf-8")
            result["_artifact_path"] = str(path)
            fault_results.append(result)
        rendered = render_generic(
            fault_results, "fault", "full", "local", "fault self-test"
        )
        parsed = tomllib.loads(rendered)
        assert {obs["id"] for obs in parsed["observation"]} == expected_fault
        try:
            render_generic(
                fault_results[:1], "fault", "full", "local", "partial fault"
            )
        except SystemExit:
            pass
        else:
            raise AssertionError("a partial fault matrix was accepted")

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
            "fault",
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
