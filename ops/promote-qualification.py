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
import math
import shutil
import sys
import tarfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from recovery_contract import (
    LOWER_SHA256,
    RECOVERY_RESULT_SCHEMA_VERSION,
    REQUIRED_GATE_NAMES,
    deferred_gate_detail,
    required_observations,
    validate_gate_ownership_model,
    validate_recovery_artifact,
)

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10 ops floor
    import tomli as tomllib  # type: ignore[no-redef]


ROOT = Path(__file__).resolve().parent.parent
PACKET_PATH = ROOT / "qualification/packet.toml"
ENDURANCE_RESULT_SCHEMA_VERSION = 4
CAPACITY_RESULT_SCHEMA_VERSION = 2
FAULT_RESULT_SCHEMA_VERSION = 1
ROLLOUT_RESULT_SCHEMA_VERSION = 5
ROLLOUT_RECORD_SCHEMA_VERSION = 4
ROLLOUT_PREVIOUS_VERSION = "0.3.40"
ROLLOUT_CANDIDATE_VERSION = "0.4.0"
STATEFUL_ENDURANCE_RESULT_SCHEMA_VERSION = 3
STATEFUL_LEDGER_SHARDS = 64
STATEFUL_MAX_SHARD_ROWS = 1_500_000
STATEFUL_DROP_LOG_SAMPLE = 1_000
ENDURANCE_SURPLUS_VERDICT = "max_unexpected_usage_records"
RECOVERY_DRIVERS = frozenset({"stateful-integration", "restore-drill"})
HARDWARE_FIELDS: tuple[tuple[str, type], ...] = (
    ("os", str),
    ("arch", str),
    ("kernel", str),
    ("cpu_model", str),
    ("cpus", int),
    ("total_memory_kib", int),
    ("containerized", bool),
)
RAW_HARDWARE_SLICES = frozenset(
    {"capacity", "fault", "rollout", "endurance", "stateful-endurance"}
)
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
CORRELATION_DOMAIN = 0x6178_6F6E_642D_656E
PROBE_CORRELATION_DOMAIN = 0x7072_6F62_652D_6964
STATUS_NAMES = (
    "ok",
    "upstream_error",
    "client_cancelled",
    "partial",
    "rejected",
)


@dataclass(frozen=True)
class ArtifactClaim:
    workload: str
    digest: str
    row: dict[str, Any]


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


def require_nonnegative_integer(value: object, field: str) -> int:
    """Return one schema integer or fail with its fully qualified field."""
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        fail(f"{field} is missing or malformed")
    return value


def resolve_stateful_ledger(raw_path: Path, declared: object, label: str) -> Path:
    if not isinstance(declared, str) or not declared:
        fail(f"{label}: exact ledger path is missing")
    relative = Path(declared)
    if relative.is_absolute() or ".." in relative.parts:
        fail(f"{label}: exact ledger path is not a safe relative path")
    candidates = (
        ROOT / relative,
        raw_path.parent / relative,
        raw_path.parent / relative.name,
    )
    existing: dict[Path, Path] = {}
    for candidate in candidates:
        if candidate.is_symlink():
            fail(f"{label}: exact ledger directory must not be a symlink")
        if candidate.is_dir():
            existing[candidate.resolve()] = candidate
    if len(existing) != 1:
        fail(f"{label}: exact ledger path resolves to {len(existing)} retained directories")
    return next(iter(existing.values()))


def resolve_resource_samples(raw_path: Path, declared: object, label: str) -> list[Path]:
    """Resolve one or more retained JSONL series without accepting path escapes."""
    values = [declared] if isinstance(declared, str) else declared
    if (
        not isinstance(values, list)
        or not values
        or any(not isinstance(value, str) or not value for value in values)
    ):
        fail(f"{label}: resource sample path is missing")
    resolved: dict[Path, Path] = {}
    ordered: list[Path] = []
    for value in values:
        relative = Path(value)
        if relative.is_absolute() or ".." in relative.parts:
            fail(f"{label}: resource sample path is not safe and relative")
        candidates = (
            ROOT / relative,
            raw_path.parent / relative,
            raw_path.parent / relative.name,
        )
        existing: dict[Path, Path] = {}
        for candidate in candidates:
            if candidate.is_symlink():
                fail(f"{label}: resource sample file must not be a symlink")
            if candidate.is_file():
                existing[candidate.resolve()] = candidate
        if len(existing) != 1:
            fail(
                f"{label}: resource sample path resolves to "
                f"{len(existing)} retained files"
            )
        path = next(iter(existing.values()))
        resolved[path.resolve()] = path
        ordered.append(path)
    if len(resolved) != len(values):
        fail(f"{label}: resource sample paths are not unique")
    return ordered


def resource_sample_claim(paths: list[Path], label: str) -> dict[str, object]:
    digest = hashlib.sha256(b"axond-resource-samples-v1\0")
    total_bytes = 0
    for path in sorted(paths, key=lambda candidate: candidate.name):
        payload = path.read_bytes()
        if not payload:
            fail(f"{label}: resource sample file is empty")
        name = path.name.encode("utf-8")
        digest.update(len(name).to_bytes(4, "big"))
        digest.update(name)
        digest.update(len(payload).to_bytes(8, "big"))
        digest.update(payload)
        total_bytes += len(payload)
    return {"sha256": digest.hexdigest(), "files": len(paths), "bytes": total_bytes}


def verify_resource_sample_claim(
    paths: list[Path], row: dict[str, Any], label: str
) -> None:
    expected = {
        "sha256": sha256_digest(row.get("samples_sha256"), f"{label}: sample digest"),
        "files": row.get("samples_files"),
        "bytes": row.get("samples_bytes"),
    }
    if (
        not isinstance(expected["files"], int)
        or isinstance(expected["files"], bool)
        or expected["files"] != len(paths)
        or not isinstance(expected["bytes"], int)
        or isinstance(expected["bytes"], bool)
        or expected["bytes"] <= 0
        or resource_sample_claim(paths, label) != expected
    ):
        fail(f"{label}: retained resource samples do not match the compact claim")


def stateful_ledger_claim(
    directory: Path,
    label: str,
    field: str,
    evidence: dict,
    *,
    schema_label: str,
    digest_domain: bytes,
) -> dict[str, object]:
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
        fail(f"{label}: exact ledger filenames do not match {schema_label}")
    digest = hashlib.sha256(digest_domain)
    total_bytes = 0
    row_width = STATEFUL_LEDGER_WIDTHS[field]
    for path in entries:
        name = path.name.encode("utf-8")
        size = path.stat().st_size
        if size % row_width:
            fail(f"{label}: {path.name} is not a multiple of its {row_width}-byte row width")
        if size // row_width > STATEFUL_MAX_SHARD_ROWS:
            fail(
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
            fail(f"{label}: {count_field} row count is malformed")
        expected_rows += count
    if total_bytes != expected_rows * row_width:
        fail(
            f"{label}: retained bytes encode {total_bytes // row_width} rows, "
            f"but the result claims {expected_rows}"
        )
    return {"sha256": digest.hexdigest(), "files": len(entries), "bytes": total_bytes}


def stateful_shard_rows(
    directory: Path,
    stem: str,
    shard: int,
    width: int,
    label: str,
    *,
    request_ids: bool,
) -> list[bytes]:
    """Read and structurally validate one exact-ledger shard."""
    path = directory / f"{stem}-shard-{shard:02}.bin"
    data = path.read_bytes()
    rows = [data[offset : offset + width] for offset in range(0, len(data), width)]
    for row_at, row in enumerate(rows):
        identity = row[:16]
        if identity[-1] % STATEFUL_LEDGER_SHARDS != shard:
            fail(f"{label}: {path.name} row {row_at} is in the wrong shard")
        if request_ids:
            if identity[6] >> 4 != 7 or identity[8] >> 6 != 0b10:
                fail(f"{label}: {path.name} row {row_at} is not a canonical UUIDv7")
        elif identity == bytes(16):
            fail(f"{label}: {path.name} row {row_at} has the forbidden zero trace ID")
    return rows


def stateful_request_tally(directory: Path, label: str) -> dict[str, int]:
    recorded = distinct = duplicates = peak = 0
    for shard in range(STATEFUL_LEDGER_SHARDS):
        rows = stateful_shard_rows(
            directory, "request", shard, 16, label, request_ids=True
        )
        counts = Counter(rows)
        recorded += len(rows)
        distinct += len(counts)
        duplicates += sum(count - 1 for count in counts.values())
        peak = max(peak, len(rows))
    return {
        "recorded": recorded,
        "distinct": distinct,
        "duplicates": duplicates,
        "peak_shard_rows": peak,
    }


def stateful_identity_pair_tally(directory: Path, label: str) -> dict[str, int]:
    tally = {
        "expected_rows": 0,
        "observed_rows": 0,
        "expected_distinct": 0,
        "observed_distinct": 0,
        "expected_duplicates": 0,
        "observed_duplicates": 0,
        "missing": 0,
        "unexpected": 0,
        "peak_shard_rows": 0,
    }
    for shard in range(STATEFUL_LEDGER_SHARDS):
        expected = stateful_shard_rows(
            directory,
            "expected-request",
            shard,
            16,
            label,
            request_ids=True,
        )
        observed = stateful_shard_rows(
            directory,
            "observed-request",
            shard,
            16,
            label,
            request_ids=True,
        )
        expected_counts = Counter(expected)
        observed_counts = Counter(observed)
        expected_set = set(expected_counts)
        observed_set = set(observed_counts)
        tally["expected_rows"] += len(expected)
        tally["observed_rows"] += len(observed)
        tally["expected_distinct"] += len(expected_set)
        tally["observed_distinct"] += len(observed_set)
        tally["expected_duplicates"] += len(expected) - len(expected_set)
        tally["observed_duplicates"] += len(observed) - len(observed_set)
        tally["missing"] += len(expected_set - observed_set)
        tally["unexpected"] += len(observed_set - expected_set)
        tally["peak_shard_rows"] = max(
            tally["peak_shard_rows"], len(expected) + len(observed)
        )
    return tally


def compatible_correlation_pairs(expected: Counter[int], observed: Counter[int]) -> int:
    """Maximum compatible ending/status matching for one trace identity."""
    source, settlement_start, status_start, sink = 0, 1, 6, 11
    capacity = [[0] * 12 for _ in range(12)]
    for code, count in expected.items():
        capacity[source][settlement_start + code] = count
    for code, count in observed.items():
        capacity[status_start + code][sink] = count
    compatibility = {
        0: (0,),
        1: (2, 3),
        2: (1, 3),
        3: (1,),
        # Caller cancellation overlapping the declared upstream fault window:
        # either close may win at the gateway, and the retained code makes
        # that bounded race explicit without widening ordinary cancellation.
        4: (1, 2, 3),
    }
    edge_capacity = min(sum(expected.values()), sum(observed.values()))
    for settlement, statuses in compatibility.items():
        for status in statuses:
            capacity[settlement_start + settlement][status_start + status] = edge_capacity
    total = 0
    while True:
        parent = [-1] * len(capacity)
        parent[source] = source
        queue = [source]
        for node in queue:
            for target, available in enumerate(capacity[node]):
                if parent[target] == -1 and available > 0:
                    parent[target] = node
                    queue.append(target)
        if parent[sink] == -1:
            return total
        increment = sys.maxsize
        node = sink
        while node != source:
            previous = parent[node]
            increment = min(increment, capacity[previous][node])
            node = previous
        node = sink
        while node != source:
            previous = parent[node]
            capacity[previous][node] -= increment
            capacity[node][previous] += increment
            node = previous
        total += increment


def stateful_correlation_tally(
    directory: Path,
    label: str,
    seed: int,
    *,
    allow_concurrent_endings: bool,
) -> dict[str, Any]:
    tally: dict[str, Any] = {
        "expected": 0,
        "observed": 0,
        "missing": 0,
        "unexpected": 0,
        "status_mismatches": 0,
        "peak_shard_rows": 0,
        "workload_expected": 0,
        "probe_expected": 0,
        "concurrent_endings": 0,
        "by_status": Counter(),
    }
    workload_high = (seed ^ CORRELATION_DOMAIN).to_bytes(8, "big")
    probe_high = (seed ^ PROBE_CORRELATION_DOMAIN ^ CORRELATION_DOMAIN).to_bytes(
        8, "big"
    )
    for shard in range(STATEFUL_LEDGER_SHARDS):
        expected_rows = stateful_shard_rows(
            directory, "expected", shard, 17, label, request_ids=False
        )
        observed_rows = stateful_shard_rows(
            directory, "observed", shard, 17, label, request_ids=False
        )
        expected_by_identity: dict[bytes, Counter[int]] = defaultdict(Counter)
        observed_by_identity: dict[bytes, Counter[int]] = defaultdict(Counter)
        for row_at, row in enumerate(expected_rows):
            code = row[16]
            maximum_expected_code = 4 if allow_concurrent_endings else 3
            if code > maximum_expected_code:
                fail(
                    f"{label}: expected shard {shard} row {row_at} has invalid "
                    f"settlement {code}"
                )
            high, low = row[:8], int.from_bytes(row[8:16], "big")
            if low == 0 or high not in (workload_high, probe_high):
                fail(f"{label}: expected shard {shard} row {row_at} was not issued by the driver")
            if high == workload_high:
                tally["workload_expected"] += 1
            else:
                tally["probe_expected"] += 1
            if code == 4:
                tally["concurrent_endings"] += 1
            expected_by_identity[row[:16]][code] += 1
        for row_at, row in enumerate(observed_rows):
            code = row[16]
            if code >= len(STATUS_NAMES):
                fail(f"{label}: observed shard {shard} row {row_at} has invalid status {code}")
            observed_by_identity[row[:16]][code] += 1
            tally["by_status"][STATUS_NAMES[code]] += 1
        tally["expected"] += len(expected_rows)
        tally["observed"] += len(observed_rows)
        tally["peak_shard_rows"] = max(
            tally["peak_shard_rows"], len(expected_rows) + len(observed_rows)
        )
        for identity in expected_by_identity.keys() | observed_by_identity.keys():
            expected = expected_by_identity[identity]
            observed = observed_by_identity[identity]
            expected_count = sum(expected.values())
            observed_count = sum(observed.values())
            paired = min(expected_count, observed_count)
            tally["missing"] += max(0, expected_count - observed_count)
            tally["unexpected"] += max(0, observed_count - expected_count)
            tally["status_mismatches"] += paired - compatible_correlation_pairs(
                expected, observed
            )
    return tally


def stateful_correlation_window_ms(
    duration_ms: int, faults: Any, schedule: dict[str, Any], label: str
) -> tuple[int, int]:
    """Combine the committed opening edge with the observed gate restoration."""
    outage_at = schedule.get("upstream_outage_at")
    outage_for = schedule.get("upstream_outage_for")
    leading_slack_ms = schedule.get("upstream_outage_correlation_slack_ms")
    dispatch_slack_ms = schedule.get("event_dispatch_slack_ms")
    if (
        not isinstance(duration_ms, int)
        or isinstance(duration_ms, bool)
        or duration_ms <= 0
        or not isinstance(outage_at, (int, float))
        or isinstance(outage_at, bool)
        or not math.isfinite(outage_at)
        or not isinstance(outage_for, (int, float))
        or isinstance(outage_for, bool)
        or not math.isfinite(outage_for)
        or not isinstance(leading_slack_ms, int)
        or isinstance(leading_slack_ms, bool)
        or leading_slack_ms < 0
        or not isinstance(dispatch_slack_ms, int)
        or isinstance(dispatch_slack_ms, bool)
        or dispatch_slack_ms < 0
    ):
        fail(f"{label}: correlation-window schedule is malformed")
    if not isinstance(faults, list):
        fail(f"{label}: observed fault windows are malformed")
    outages = [
        fault
        for fault in faults
        if isinstance(fault, dict) and fault.get("event") == "upstream-outage-begins"
    ]
    if len(outages) != 1:
        fail(f"{label}: expected exactly one observed upstream outage")
    observed = outages[0]
    raw_opened_ms = observed.get("opened_ms")
    closed_ms = observed.get("closed_ms")
    if any(
        not isinstance(value, int) or isinstance(value, bool) or value < 0
        for value in (raw_opened_ms, closed_ms)
    ):
        fail(f"{label}: observed upstream outage timestamps are malformed")
    nominal_opened_ms = int(
        duration_ms * min(max(float(outage_at), 0.0), 1.0)
    )
    nominal_closed_ms = int(
        duration_ms * min(max(float(outage_at + outage_for), 0.0), 1.0)
    )
    if raw_opened_ms < nominal_opened_ms or closed_ms < nominal_closed_ms:
        fail(f"{label}: observed upstream outage precedes its committed schedule")
    if (
        raw_opened_ms > nominal_opened_ms + dispatch_slack_ms
        or closed_ms > nominal_closed_ms + dispatch_slack_ms
        or closed_ms > duration_ms
    ):
        fail(f"{label}: observed upstream outage exceeds its dispatch bound")
    if raw_opened_ms >= closed_ms:
        fail(f"{label}: observed upstream gate interval is empty")
    opened_ms = max(0, nominal_opened_ms - leading_slack_ms)
    if opened_ms >= closed_ms:
        fail(f"{label}: observed upstream correlation window is empty")
    return opened_ms, closed_ms


def stateful_correlation_window_tally(
    directory: Path,
    correlation_directory: Path,
    label: str,
    seed: int,
    duration_ms: int,
    faults: Any,
    schedule: dict[str, Any],
) -> dict[str, int]:
    """Re-derive exact code-4 membership from retained request intervals."""
    opened_ms, closed_ms = stateful_correlation_window_ms(
        duration_ms, faults, schedule, label
    )

    workload_high = (seed ^ CORRELATION_DOMAIN).to_bytes(8, "big")
    tally = {
        "recorded": 0,
        "concurrent_endings": 0,
        "membership_mismatches": 0,
        "peak_shard_rows": 0,
    }
    for shard in range(STATEFUL_LEDGER_SHARDS):
        window_rows = stateful_shard_rows(
            directory, "window", shard, 33, label, request_ids=False
        )
        expected_rows = stateful_shard_rows(
            correlation_directory,
            "expected",
            shard,
            17,
            f"{label}: correlations",
            request_ids=False,
        )
        workload_expected = Counter(
            row for row in expected_rows if row[:8] == workload_high
        )
        derived: Counter[bytes] = Counter()
        for row_at, row in enumerate(window_rows):
            identity = row[:16]
            if identity[:8] != workload_high or int.from_bytes(identity[8:], "big") == 0:
                fail(
                    f"{label}: window shard {shard} row {row_at} was not "
                    "issued by the workload driver"
                )
            base_ending = row[16]
            if base_ending > 3:
                fail(
                    f"{label}: window shard {shard} row {row_at} has invalid "
                    f"base settlement {base_ending}"
                )
            started_ms = int.from_bytes(row[17:25], "big")
            ended_ms = int.from_bytes(row[25:33], "big")
            if ended_ms < started_ms:
                fail(
                    f"{label}: window shard {shard} row {row_at} ends before it starts"
                )
            overlaps = ended_ms >= opened_ms and started_ms < closed_ms
            settlement = 4 if base_ending == 1 and overlaps else base_ending
            if settlement == 4:
                tally["concurrent_endings"] += 1
            derived[identity + bytes((settlement,))] += 1
        tally["recorded"] += len(window_rows)
        tally["peak_shard_rows"] = max(
            tally["peak_shard_rows"],
            len(window_rows) + sum(workload_expected.values()),
        )
        tally["membership_mismatches"] += sum((derived - workload_expected).values())
        tally["membership_mismatches"] += sum((workload_expected - derived).values())
    return tally


def stateful_request_ids_are_subset(request_dir: Path, durable_dir: Path, label: str) -> bool:
    for shard in range(STATEFUL_LEDGER_SHARDS):
        workload = set(
            stateful_shard_rows(
                request_dir, "request", shard, 16, label, request_ids=True
            )
        )
        all_emitted = set(
            stateful_shard_rows(
                durable_dir,
                "expected-request",
                shard,
                16,
                label,
                request_ids=True,
            )
        )
        if not workload <= all_emitted:
            return False
    return True


def stateful_outside_loss_relation(
    durable_dir: Path, outside_dir: Path, label: str
) -> bool:
    """Prove the outside-loss ledger uses the complete durable population.

    Its expected side is the emitted-outside subset; its observed side must be
    byte-for-byte the whole durable observed set. That makes its missing rows
    exactly `emitted outside - durable anywhere`, independent of which side of
    a timestamp boundary PostgreSQL assigned a row that was in fact stored.
    """
    for shard in range(STATEFUL_LEDGER_SHARDS):
        all_expected = set(
            stateful_shard_rows(
                durable_dir,
                "expected-request",
                shard,
                16,
                label,
                request_ids=True,
            )
        )
        outside_expected = set(
            stateful_shard_rows(
                outside_dir,
                "expected-request",
                shard,
                16,
                label,
                request_ids=True,
            )
        )
        all_observed = stateful_shard_rows(
            durable_dir,
            "observed-request",
            shard,
            16,
            label,
            request_ids=True,
        )
        outside_observed = stateful_shard_rows(
            outside_dir,
            "observed-request",
            shard,
            16,
            label,
            request_ids=True,
        )
        if not outside_expected <= all_expected or outside_observed != all_observed:
            return False
    return True


def hardware_fields(
    hardware: Any, label: str, *, allow_extra: bool = False
) -> dict[str, Any]:
    """Project and strictly validate the seven compact hardware fields."""
    if not isinstance(hardware, dict):
        fail(f"{label} is not a table")
    expected_fields = {field for field, _ in HARDWARE_FIELDS}
    unexpected = hardware.keys() - expected_fields
    if unexpected and not allow_extra:
        fail(f"{label} has unknown field(s): {sorted(unexpected)}")
    projected: dict[str, Any] = {}
    for field, expected_type in HARDWARE_FIELDS:
        if field not in hardware:
            fail(f"{label} is missing {field!r}")
        value = hardware[field]
        if type(value) is not expected_type:
            fail(
                f"{label}.{field} has type {type(value).__name__}, "
                f"expected {expected_type.__name__}"
            )
        if expected_type is str and not value.strip():
            fail(f"{label}.{field} is empty")
        if expected_type is int and value <= 0:
            fail(f"{label}.{field} must be positive")
        projected[field] = value
    return projected


def compact_hardware(record: dict[str, Any]) -> dict[str, Any]:
    """Return the validated compact hardware identity."""
    return hardware_fields(record.get("hardware"), "compact hardware")


def validate_raw_hardware(
    result: dict[str, Any], expected: dict[str, Any], label: str
) -> None:
    """Bind every current compact hardware field to the raw environment."""
    environment = result.get("environment")
    if not isinstance(environment, dict):
        fail(f"{label}: raw environment is missing or malformed")
    actual = hardware_fields(
        environment.get("hardware"), f"{label}: raw hardware", allow_extra=True
    )
    for field, _ in HARDWARE_FIELDS:
        if actual[field] != expected[field]:
            fail(f"{label}: raw hardware.{field} does not match the compact record")


def artifact_rows(record: dict[str, Any]) -> list[Any]:
    """Return compact workload rows without discarding duplicate claims."""
    slice_id = record.get("slice")
    if slice_id == "capacity":
        # Schema 1 is the historical compact envelope, before raw artifact
        # binding. New promotion records use schema 2 and claim every profile.
        if record.get("schema_version") == 1:
            return []
        rows = record.get("profile", [])
    elif slice_id == "recovery":
        rows = record.get("stage", [])
    else:
        rows = record.get("observation", [])
    if not isinstance(rows, list):
        fail("the compact artifact rows are not a list")
    return rows


def workload_id(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        fail(f"{label} is missing or malformed")
    return value


def sha256_digest(value: Any, label: str) -> str:
    if not isinstance(value, str) or not LOWER_SHA256.fullmatch(value):
        fail(f"{label} is not a lowercase 64-character hexadecimal SHA-256 digest")
    return value


def artifact_claims(record: dict[str, Any]) -> list[ArtifactClaim]:
    """Validate and preserve one compact workload-to-digest claim per row."""
    claims: list[ArtifactClaim] = []
    workloads: set[str] = set()
    digests: set[str] = set()
    for index, row in enumerate(artifact_rows(record)):
        if not isinstance(row, dict):
            fail(f"compact artifact row {index} is not a table")
        workload = workload_id(row.get("id"), f"compact artifact row {index} id")
        digest = sha256_digest(
            row.get("artifact_sha256"),
            f"compact artifact row {workload!r} digest",
        )
        if workload in workloads:
            fail(f"compact workload {workload!r} is claimed more than once")
        if digest in digests:
            fail(f"compact digest {digest} is reused by multiple workloads")
        workloads.add(workload)
        digests.add(digest)
        claims.append(ArtifactClaim(workload, digest, row))
    return claims


def validate_raw_recovery(
    result: dict[str, Any],
    label: str,
    record: dict[str, Any],
    row: dict[str, Any],
) -> None:
    """Bind one raw schema-2 stage to its manifest and release executable."""
    if (
        result.get("schema_version") != RECOVERY_RESULT_SCHEMA_VERSION
        or row.get("artifact_schema_version") != RECOVERY_RESULT_SCHEMA_VERSION
    ):
        fail(
            f"{label}: recovery artifact schema is not version "
            f"{RECOVERY_RESULT_SCHEMA_VERSION}"
        )
    scenario_id = result.get("scenario")
    stage_id = result.get("stage")
    manifest_relative = record.get("inputs", {}).get("manifest")
    if not all(isinstance(value, str) and value for value in (scenario_id, stage_id)):
        fail(f"{label}: recovery scenario or stage identity is malformed")
    try:
        manifest = tomllib.loads((ROOT / manifest_relative).read_text(encoding="utf-8"))
    except (OSError, TypeError, UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        fail(f"{label}: recovery manifest is unreadable: {error}")
    scenarios = [
        scenario
        for scenario in manifest.get("scenario", [])
        if isinstance(scenario, dict) and scenario.get("id") == scenario_id
    ]
    if len(scenarios) != 1:
        fail(f"{label}: recovery artifact names no unique manifest scenario")
    scenario = scenarios[0]
    stages = [
        stage
        for stage in scenario.get("stage", [])
        if isinstance(stage, dict) and stage.get("id") == stage_id
    ]
    if len(stages) != 1 or stages[0].get("status") != "executable":
        fail(f"{label}: recovery artifact names no executable manifest stage")
    stage = stages[0]
    expected_runner = stage.get("runner")
    if (
        result.get("runner") != expected_runner
        or row.get("runner") != expected_runner
        or row.get("driver") != stage.get("driver")
        or result.get("capability") != scenario.get("capability")
        or result.get("evidence") != stage.get("evidence")
    ):
        fail(f"{label}: recovery stage contract does not match the manifest")

    semantic_problems = validate_recovery_artifact(result, scenario, stage)
    if semantic_problems:
        fail(f"{label}: malformed recovery evidence: {'; '.join(semantic_problems)}")

    run = result.get("run")
    record_binary = sha256_digest(
        record.get("binary", {}).get("sha256"), f"{label}: recovery record binary"
    )
    if not isinstance(run, dict):
        fail(f"{label}: recovery run provenance is malformed")
    raw_binary = sha256_digest(
        run.get("axond_executable_sha256"), f"{label}: raw recovery binary"
    )
    stage_binary = sha256_digest(
        row.get("binary_sha256"), f"{label}: compact recovery stage binary"
    )
    if raw_binary != record_binary or stage_binary != record_binary:
        fail(f"{label}: recovery stage does not identify the retained executable")
    if stage.get("driver") in RECOVERY_DRIVERS:
        executed_binary = sha256_digest(
            run.get("axond_executed_sha256"),
            f"{label}: process-executed recovery binary",
        )
        compact_executed_binary = sha256_digest(
            row.get("executed_binary_sha256"),
            f"{label}: compact process-executed recovery binary",
        )
        if executed_binary != record_binary or compact_executed_binary != record_binary:
            fail(f"{label}: process stage was not executed by the retained binary")
        if row.get("execution_bound") is not True:
            fail(f"{label}: compact process stage is not execution-bound")
    elif row.get("executed_binary_sha256") is not None or row.get(
        "execution_bound"
    ) is not None:
        fail(f"{label}: non-process recovery stage claims process execution provenance")
    if run.get("cargo_profile") != "release" or record.get("binary", {}).get(
        "cargo_profile"
    ) != "release":
        fail(f"{label}: recovery stage is not release-profile evidence")
    if run.get("axond_version") != record.get("source", {}).get("crate_version"):
        fail(f"{label}: recovery stage version does not match the candidate record")
    elapsed = run.get("elapsed_ms")
    started = run.get("started_at_unix_ms")
    if (
        not isinstance(elapsed, int)
        or isinstance(elapsed, bool)
        or elapsed <= 0
        or elapsed != row.get("elapsed_ms")
        or not isinstance(started, int)
        or isinstance(started, bool)
        or started <= 0
    ):
        fail(f"{label}: recovery run timing provenance is malformed")

    timeline = result.get("timeline")
    if not isinstance(timeline, list) or not timeline:
        fail(f"{label}: recovery artifact retains no timeline")
    previous_at = -1
    for event in timeline:
        at = event.get("at_ms") if isinstance(event, dict) else None
        if (
            not isinstance(at, int)
            or isinstance(at, bool)
            or at < previous_at
            or not isinstance(event.get("event"), str)
            or not event["event"]
        ):
            fail(f"{label}: recovery timeline is malformed or non-monotonic")
        previous_at = at

    gates = result["gates"]
    checks = result["checks"]
    if (
        not isinstance(gates, list)
        or not isinstance(checks, list)
        or len(gates) + len(checks) != row.get("verdicts")
        or row.get("passed") is not True
    ):
        fail(f"{label}: recovery verdict set is missing, failed, or inconsistent")


def validate_compact_recovery(
    record: dict[str, Any], rows: Any, manifest: dict[str, Any]
) -> None:
    """Validate recovery identity before retained raw artifacts are available."""
    if not isinstance(rows, list):
        fail("the compact recovery stages are not a list")
    record_binary = sha256_digest(
        record.get("binary", {}).get("sha256"), "recovery record binary"
    )
    if record.get("binary", {}).get("cargo_profile") != "release":
        fail("the compact recovery record is not release-profile evidence")
    ownership_problems = validate_gate_ownership_model(manifest.get("scenario", []))
    if ownership_problems:
        fail("the recovery gate ownership model is malformed: " + "; ".join(ownership_problems))

    contracts = {
        f"{scenario['id']}/{stage['id']}": stage
        for scenario in manifest.get("scenario", [])
        for stage in scenario.get("stage", [])
        if stage.get("status") == "executable"
    }
    observed_ids: list[str] = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            fail(f"compact recovery stage {index} is not a table")
        stage_id = workload_id(row.get("id"), f"compact recovery stage {index} id")
        observed_ids.append(stage_id)
        contract = contracts.get(stage_id)
        if contract is None:
            fail(f"{stage_id}: compact recovery stage is not executable")
        driver = row.get("driver")
        if driver not in RECOVERY_DRIVERS or driver != contract.get("driver"):
            fail(f"{stage_id}: compact recovery driver is unrecognized or stale")
        if row.get("runner") != contract.get("runner"):
            fail(f"{stage_id}: compact recovery runner does not match the manifest")
        if row.get("artifact_schema_version") != RECOVERY_RESULT_SCHEMA_VERSION:
            fail(
                f"{stage_id}: compact recovery stage does not bind schema "
                f"{RECOVERY_RESULT_SCHEMA_VERSION}"
            )
        sha256_digest(
            row.get("artifact_sha256"),
            f"{stage_id}: compact recovery artifact digest",
        )
        stage_binary = sha256_digest(
            row.get("binary_sha256"),
            f"{stage_id}: compact recovery binary digest",
        )
        if stage_binary != record_binary:
            fail(f"{stage_id}: compact recovery binary does not match the record")

        if driver in RECOVERY_DRIVERS:
            executed_binary = sha256_digest(
                row.get("executed_binary_sha256"),
                f"{stage_id}: compact process-executed recovery binary",
            )
            if (
                executed_binary != record_binary
                or row.get("execution_bound") is not True
            ):
                fail(f"{stage_id}: compact process stage is not execution-bound")
        elif row.get("executed_binary_sha256") is not None or row.get(
            "execution_bound"
        ) is not None:
            fail(
                f"{stage_id}: compact restore stage claims process execution provenance"
            )

        if row.get("passed") is not True:
            fail(f"{stage_id}: compact recovery stage did not pass")
        verdicts = row.get("verdicts")
        if (
            not isinstance(verdicts, int)
            or isinstance(verdicts, bool)
            or verdicts <= 0
        ):
            fail(f"{stage_id}: compact recovery stage has no verdicts")

    duplicates = sorted(
        stage_id for stage_id, count in Counter(observed_ids).items() if count > 1
    )
    if duplicates:
        fail(f"compact recovery stage IDs are duplicated: {duplicates}")


def raw_workload_id(slice_id: str, result: dict[str, Any], label: str) -> str:
    """Extract the workload identity encoded by a supported raw result."""
    if slice_id == "capacity":
        parent = result.get("profile")
        if not isinstance(parent, dict):
            fail(f"{label}: raw capacity profile is missing or malformed")
        return workload_id(parent.get("id"), f"{label}: raw capacity workload id")
    if slice_id == "fault":
        parent = result.get("row")
        if not isinstance(parent, dict):
            fail(f"{label}: raw fault row is missing or malformed")
        return workload_id(parent.get("id"), f"{label}: raw fault workload id")
    if slice_id == "rollout":
        parent = result.get("scenario")
        if not isinstance(parent, dict):
            fail(f"{label}: raw rollout scenario is missing or malformed")
        return workload_id(parent.get("id"), f"{label}: raw rollout workload id")
    if slice_id == "recovery":
        scenario = workload_id(result.get("scenario"), f"{label}: raw recovery scenario")
        stage = workload_id(result.get("stage"), f"{label}: raw recovery stage")
        if "/" in scenario or "/" in stage:
            fail(f"{label}: raw recovery identity components may not contain '/'")
        return f"{scenario}/{stage}"
    if slice_id in ("endurance", "stateful-endurance"):
        parent = result.get("profile")
        if not isinstance(parent, dict):
            fail(f"{label}: raw {slice_id} profile is missing or malformed")
        return workload_id(parent.get("id"), f"{label}: raw {slice_id} workload id")
    fail(f"raw workload identity extraction is unsupported for slice {slice_id!r}")


def validate_raw_claim(
    slice_id: str,
    result: dict[str, Any],
    claim: ArtifactClaim,
    label: str,
    expected_hardware: dict[str, Any],
    record: dict[str, Any],
) -> None:
    actual_workload = raw_workload_id(slice_id, result, label)
    if actual_workload != claim.workload:
        fail(
            f"{label}: raw workload {actual_workload!r} does not match "
            f"compact workload {claim.workload!r}"
        )
    if slice_id in RAW_HARDWARE_SLICES:
        validate_raw_hardware(result, expected_hardware, label)
    if slice_id == "rollout":
        validate_raw_rollout(result, label, record, claim.row)
    if slice_id == "fault":
        validate_raw_fault(result, label, claim.row)
    if slice_id == "recovery":
        validate_raw_recovery(result, label, record, claim.row)
    if slice_id == "stateful-endurance":
        validate_raw_stateful_endurance(result, label, record, claim.row)


def verify_raw_artifacts(record: dict[str, Any], directory: Path) -> None:
    """Bind each compact row to one unique raw JSON file, digest, and workload."""
    if not directory.is_dir():
        fail(f"raw artifact directory does not exist: {directory}")
    claims = artifact_claims(record)
    if not claims:
        fail("the record has no raw artifact digests to verify")
    expected_hardware = compact_hardware(record)
    expected = {claim.digest: claim for claim in claims}
    actual: dict[str, Path] = {}
    claimed_stateful_shards: set[Path] = set()
    claimed_endurance_shards: set[Path] = set()
    claimed_sample_files: set[Path] = set()
    endurance_bounds: dict[str, int] = {}
    if record.get("slice") == "endurance":
        manifest_relative = record.get("inputs", {}).get("manifest")
        manifest = tomllib.loads((ROOT / manifest_relative).read_text(encoding="utf-8"))
        endurance_bounds = {
            row["id"]: row[record["tier"]]["thresholds"][ENDURANCE_SURPLUS_VERDICT]
            for row in manifest["profile"]
        }
    for path in sorted(directory.rglob("*.json")):
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest in actual:
            fail(
                f"raw artifact digest {digest} is duplicated by {actual[digest]} "
                f"and {path}"
            )
        actual[digest] = path
    missing = expected.keys() - actual.keys()
    unexpected = actual.keys() - expected.keys()
    if missing or unexpected:
        details = []
        if missing:
            details.append(f"missing {len(missing)} claimed artifact(s)")
        if unexpected:
            details.append(f"found {len(unexpected)} unclaimed artifact(s)")
        fail("raw artifact set does not match the record: " + ", ".join(details))
    slice_id = record.get("slice")
    if slice_id == "rollout":
        verify_retained_rollout_archive(record, directory)
    for digest, claim in expected.items():
        path = actual[digest]
        try:
            result = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
            fail(f"{path}: raw artifact is not valid JSON: {error}")
        if not isinstance(result, dict):
            fail(f"{path}: raw artifact root is not an object")
        validate_raw_claim(
            slice_id, result, claim, str(path), expected_hardware, record
        )
        if slice_id == "stateful-endurance":
            usage = result.get("usage", {})
            for field, _ in STATEFUL_LEDGER_FIELDS:
                ledger = resolve_stateful_ledger(
                    path, usage.get(field, {}).get("path"), f"{path}: {field}"
                )
                claimed_stateful_shards.update(
                    shard.resolve() for shard in ledger.iterdir() if shard.is_file()
                )
            claimed_sample_files.update(
                sample.resolve()
                for sample in resolve_resource_samples(
                    path,
                    result.get("run", {}).get("samples_paths"),
                    f"{path}: resource samples",
                )
            )
        if slice_id == "endurance":
            if claim.workload not in endurance_bounds:
                fail(
                    f"{path}: raw endurance artifact names unknown workload "
                    f"{claim.workload!r}"
                )
            validate_raw_endurance(
                result,
                endurance_bounds[claim.workload],
                str(path),
                record,
                claim.row,
            )
            reconciliation = result.get("reconciliation", {})
            for field, _ in STATEFUL_LEDGER_FIELDS[:2]:
                ledger = resolve_stateful_ledger(
                    path,
                    reconciliation.get(field, {}).get("path"),
                    f"{path}: {field}",
                )
                claimed_endurance_shards.update(
                    shard.resolve() for shard in ledger.iterdir() if shard.is_file()
                )
            claimed_sample_files.update(
                sample.resolve()
                for sample in resolve_resource_samples(
                    path,
                    result.get("run", {}).get("samples_path"),
                    f"{path}: resource samples",
                )
            )
        elif slice_id == "capacity":
            validate_raw_capacity(result, str(path), record, claim.row)
    if slice_id == "stateful-endurance":
        actual_shards = {path.resolve() for path in directory.rglob("*.bin")}
        if actual_shards != claimed_stateful_shards:
            fail(
                "stateful endurance shard set is incomplete or contains unclaimed files: "
                f"claimed {len(claimed_stateful_shards)}, found {len(actual_shards)}"
            )
    if slice_id == "endurance":
        actual_shards = {path.resolve() for path in directory.rglob("*.bin")}
        if actual_shards != claimed_endurance_shards:
            fail(
                "endurance shard set is incomplete or contains unclaimed files: "
                f"claimed {len(claimed_endurance_shards)}, found {len(actual_shards)}"
            )
    if slice_id in ("endurance", "stateful-endurance"):
        actual_samples = {
            path.resolve() for path in directory.rglob("*.samples.jsonl")
        }
        if actual_samples != claimed_sample_files:
            fail(
                f"{slice_id} sample set is incomplete or contains unclaimed files: "
                f"claimed {len(claimed_sample_files)}, found {len(actual_samples)}"
            )


def verify_retained_rollout_archive(record: dict[str, Any], directory: Path) -> None:
    """Bind both executables to the retained candidate and published predecessor."""
    observations = record.get("observation", [])
    identities = {
        (
            row.get("rollout_previous_version"),
            row.get("rollout_previous_binary_sha256"),
            row.get("rollout_retained_archive_sha256"),
            row.get("rollout_candidate_version"),
            row.get("rollout_candidate_binary_sha256"),
        )
        for row in observations
    }
    if len(identities) != 1:
        fail("rollout observations disagree about the retained release identity")
    version, binary_digest, archive_digest, candidate_version, candidate_digest = identities.pop()
    binary_digest = sha256_digest(binary_digest, "retained release binary digest")
    archive_digest = sha256_digest(archive_digest, "retained release archive digest")
    candidate_digest = sha256_digest(candidate_digest, "candidate binary digest")
    if not isinstance(candidate_version, str) or not candidate_version:
        fail("rollout candidate version is missing")
    retained = directory / "retained"
    archive = retained / f"axond-{version}-x86_64-unknown-linux-gnu.tar.gz"
    checksum = archive.with_name(f"{archive.name}.sha256")
    candidate = retained / "candidate" / "axond"
    expected_files = {archive.resolve(), checksum.resolve(), candidate.resolve()}
    actual_files = (
        {path.resolve() for path in retained.rglob("*") if path.is_file()}
        if retained.is_dir()
        else set()
    )
    if actual_files != expected_files:
        fail("rollout retained-release artifact set is missing or contains unclaimed files")
    if hashlib.sha256(archive.read_bytes()).hexdigest() != archive_digest:
        fail("retained release archive bytes do not match the compact digest")
    checksum_fields = checksum.read_text(encoding="utf-8").strip().split()
    if checksum_fields != [archive_digest, archive.name]:
        fail("retained release checksum file does not bind the retained archive")
    try:
        with tarfile.open(archive, mode="r:gz") as bundle:
            members = [member for member in bundle.getmembers() if member.isfile()]
            if [member.name for member in members] != ["axond"]:
                fail("retained release archive does not contain exactly the axond binary")
            extracted = bundle.extractfile(members[0])
            if extracted is None:
                fail("retained release archive binary cannot be read")
            archived_binary_digest = hashlib.sha256(extracted.read()).hexdigest()
    except (OSError, tarfile.TarError) as error:
        fail(f"retained release archive is malformed: {error}")
    if archived_binary_digest != binary_digest:
        fail("retained release archive does not contain the claimed previous binary")
    if hashlib.sha256(candidate.read_bytes()).hexdigest() != candidate_digest:
        fail("retained candidate executable does not match the claimed candidate binary")


def validate_raw_fault(
    result: dict[str, Any], label: str, row: dict[str, Any]
) -> None:
    """Reconstruct the complete fault verdict contract from raw measurements."""
    if result.get("schema_version") != FAULT_RESULT_SCHEMA_VERSION:
        fail(f"{label}: unsupported fault artifact schema")
    if row.get("artifact_schema_version") != FAULT_RESULT_SCHEMA_VERSION:
        fail(f"{label}: compact fault row does not bind raw schema 1")

    manifest = tomllib.loads(
        (ROOT / "qualification/faults/manifest.toml").read_text(encoding="utf-8")
    )
    raw_row = result.get("row")
    if not isinstance(raw_row, dict):
        fail(f"{label}: fault row echo is missing")
    matches = [
        candidate
        for candidate in manifest.get("row", [])
        if candidate.get("id") == raw_row.get("id")
    ]
    if len(matches) != 1:
        fail(f"{label}: fault row does not name one committed manifest entry")
    committed = matches[0]
    expected_echo = {
        "id": committed.get("id"),
        "family": committed.get("family"),
        "fault": committed.get("fault"),
        "description": committed.get("description"),
        "streamed": committed.get("streamed", False),
    }
    if raw_row != expected_echo:
        fail(f"{label}: fault row does not exactly echo the committed manifest")

    fault = committed["fault"]
    family = committed["family"]
    expect = committed["expect"]
    service = (
        "redis"
        if fault.startswith("redis_")
        else "postgres"
        if fault.startswith("postgres_")
        else None
    )
    on_unavailable = (
        "allow"
        if fault.endswith("_outage_fail_open")
        else "deny"
        if service is not None
        else None
    )
    recovery_fault = fault in {"redis_recovery", "postgres_recovery"}
    outage_fault = recovery_fault or "_outage_" in fault
    abandons_upstream = fault in {
        "response_header_timeout",
        "buffered_body_timeout",
        "stream_idle_before_bytes",
        "stream_idle_after_bytes",
        "stream_truncation",
    }
    stream_fault = fault in {
        "stream_idle_before_bytes",
        "stream_idle_after_bytes",
        "stream_truncation",
    }
    if raw_row["streamed"] is not stream_fault:
        fail(f"{label}: fault streaming shape is inconsistent")

    injection = result.get("injection")
    if not isinstance(injection, dict):
        fail(f"{label}: fault injection evidence is missing")
    if (
        injection.get("fault") != fault
        or injection.get("family") != family
        or injection.get("service") != service
        or injection.get("on_unavailable") != on_unavailable
        or injection.get("injected_latency_ms") != committed.get("injected_latency_ms")
        or not isinstance(injection.get("how"), str)
        or not injection["how"]
    ):
        fail(f"{label}: fault injection does not match the committed row")

    classification = result.get("classification")
    deadline = result.get("deadline")
    retries = result.get("retries")
    cleanup = result.get("cleanup")
    usage = result.get("usage")
    telemetry = result.get("telemetry")
    leakage = result.get("leakage")
    timing = injection.get("timing")
    if not all(
        isinstance(value, dict)
        for value in (classification, deadline, retries, cleanup, usage, telemetry, leakage, timing)
    ):
        fail(f"{label}: fault measurements are incomplete")

    expected_status = expect.get("status")
    expected_error = expect.get("error_type")
    expected_usage_records = expect.get("usage_records", 1)
    expected_usage_status = expect.get("usage_status")
    expected_measured_status = (
        None if expected_usage_status == "none" else expected_usage_status
    )
    expected_relayed = expect.get("relayed_output", False)
    relayed_output_bytes = require_nonnegative_integer(
        classification.get("relayed_output_bytes"),
        f"{label}: classification.relayed_output_bytes",
    )
    if (
        classification.get("status") != expected_status
        or classification.get("error_type") != expected_error
        or not isinstance(classification.get("transport_failure"), bool)
        or (relayed_output_bytes > 0) is not expected_relayed
        or classification.get("during_outage_status")
        != expect.get("during_outage_status")
        or (recovery_fault and classification.get("after_recovery_status") != expected_status)
        or retries.get("attempts") != expect.get("attempts")
        or retries.get("upstream_requests") != expect.get("upstream_requests")
        or retries.get("max_attempts") != 2
    ):
        fail(f"{label}: fault classification or retry evidence contradicts the manifest")

    elapsed = deadline.get("elapsed_ms")
    if (
        not isinstance(elapsed, int)
        or isinstance(elapsed, bool)
        or elapsed < 0
        or elapsed > committed["deadline_ms"]
        or deadline.get("wall_clock_ms") != committed["deadline_ms"]
        or not isinstance(deadline.get("bound"), str)
        or not deadline["bound"]
        or timing.get("elapsed_ms") != elapsed
        or not isinstance(timing.get("started_at_unix_ms"), int)
        or isinstance(timing.get("started_at_unix_ms"), bool)
        or timing["started_at_unix_ms"] <= 0
    ):
        fail(f"{label}: fault deadline evidence is not bounded by the manifest")

    by_status = usage.get("by_status")
    if not isinstance(by_status, dict) or any(
        not isinstance(count, int) or isinstance(count, bool) or count < 0
        for count in by_status.values()
    ):
        fail(f"{label}: fault usage status summary is malformed")
    if (
        usage.get("records") != expected_usage_records
        or sum(by_status.values()) != expected_usage_records
        or usage.get("measured_status") != expected_measured_status
        or usage.get("attributed_by") != "request_id_mint_time"
        or usage.get("unattributable_records") != 0
        or (expected_usage_records > 0 and usage.get("carries_request_id") is not True)
        or (
            expected_measured_status is not None
            and by_status.get(expected_measured_status, 0) < 1
        )
    ):
        fail(f"{label}: fault usage outcome is not exactly attributable")

    exports = telemetry.get("exports")
    expected_metrics = expect.get("metrics", [])
    observed_metrics = telemetry.get("metrics_observed")
    if (
        telemetry.get("collector") is not True
        or not isinstance(exports, dict)
        or any(
            not isinstance(count, int) or isinstance(count, bool) or count < 0
            for count in exports.values()
        )
        or sum(exports.values()) <= 0
        or not isinstance(observed_metrics, list)
        or telemetry.get("metrics_missing")
        != [metric for metric in expected_metrics if metric not in observed_metrics]
        or any(metric not in observed_metrics for metric in expected_metrics)
        or (
            expect.get("upstream_requests", 0) > 0
            and "axond.upstream.attempt" not in telemetry.get("spans_observed", [])
        )
    ):
        fail(f"{label}: fault telemetry does not prove the committed instruments")

    surfaces = leakage.get("surfaces")
    if (
        not isinstance(surfaces, list)
        or len(surfaces) != 4
        or {surface.get("name") for surface in surfaces if isinstance(surface, dict)}
        != {"caller_response", "usage_records", "process_output", "telemetry_exports"}
        or not isinstance(leakage.get("needles"), dict)
        or not leakage["needles"]
        or leakage.get("findings") != []
    ):
        fail(f"{label}: fault leakage scan is incomplete or found secret material")

    if (
        cleanup.get("process_exited_cleanly") is not True
        or not isinstance(cleanup.get("upstream_streams_open_at_end"), int)
        or cleanup["upstream_streams_open_at_end"] > 0
        or (abandons_upstream and cleanup.get("upstream_streams_opened", 0) <= 0)
        or not isinstance(cleanup.get("settled_within_ms"), int)
        or cleanup["settled_within_ms"] > 5_000
    ):
        fail(f"{label}: fault cleanup evidence is not bounded")

    outage = injection.get("outage")
    if isinstance(outage, dict) != outage_fault:
        fail(f"{label}: fault outage evidence presence is inconsistent")
    if isinstance(outage, dict):
        began = outage.get("began_at_unix_ms")
        restored = outage.get("restored_at_unix_ms")
        duration = outage.get("duration_ms")
        started = timing["started_at_unix_ms"]
        if not all(isinstance(value, int) and not isinstance(value, bool) for value in (began, duration)):
            fail(f"{label}: fault outage window is malformed")
        covered = (
            restored is not None
            and isinstance(restored, int)
            and restored >= began
            and restored <= started
            and duration >= restored - began
            if recovery_fault
            else restored is None
            and began <= started
            and duration >= started - began + elapsed
        )
        if outage.get("connections_severed", 0) <= 0 or not covered:
            fail(f"{label}: fault outage window does not cover the measured request")

    if classification.get("operator_reason_retained") not in (None, True):
        fail(f"{label}: fault operator reason was not retained")

    expected_checks = {
        "status",
        "error_type",
        "attempts",
        "upstream_requests",
        "usage_records",
        "usage_status",
        "relayed_output",
        "deadline",
        "clean_shutdown",
        "telemetry_exported",
        "telemetry_metrics",
        "telemetry_attempt_span",
        "no_leakage",
        "upstream_cleanup",
        "upstream_abandoned_response_tracked",
        "upstream_released_promptly",
        "usage_attributed_by_identity",
    }
    if expect.get("during_outage_status") is not None:
        expected_checks.add("during_outage_status")
    if committed.get("injected_latency_ms") is not None:
        expected_checks.add("injected_latency_is_observable")
        if elapsed < committed["injected_latency_ms"]:
            fail(f"{label}: injected latency was not observable")
    if isinstance(outage, dict):
        expected_checks.update(("outage_severed_connections", "outage_window_recorded"))
    if classification.get("operator_reason_retained") is not None:
        expected_checks.add("operator_reason_retained")
    if expected_usage_records > 0:
        expected_checks.add("usage_carries_request_id")
    verdicts = result.get("verdicts")
    if (
        not isinstance(verdicts, list)
        or any(not isinstance(verdict, dict) for verdict in verdicts)
        or len(verdicts) != len(expected_checks)
        or {verdict.get("check") for verdict in verdicts} != expected_checks
        or any(verdict.get("passed") is not True for verdict in verdicts)
    ):
        fail(f"{label}: fault verdict set is incomplete, duplicated, or failed")


def validate_raw_rollout(
    result: dict[str, Any], label: str, record: dict[str, Any], row: dict[str, Any]
) -> None:
    """Bind compact schema 4 to true two-binary stateful raw-schema-5 evidence."""
    if result.get("schema_version") != ROLLOUT_RESULT_SCHEMA_VERSION:
        fail(f"{label}: unsupported rollout artifact schema")
    if row.get("artifact_schema_version") != ROLLOUT_RESULT_SCHEMA_VERSION:
        fail(
            f"{label}: compact rollout row does not bind result schema "
            f"{ROLLOUT_RESULT_SCHEMA_VERSION}"
        )
    if (
        record.get("binary", {}).get("cargo_profile") != "release"
        or result.get("environment", {})
        .get("toolchain", {})
        .get("cargo_profile")
        != "release"
    ):
        fail(f"{label}: promotable rollout evidence must use the release profile")
    run = result.get("run", {})
    scenario = result.get("scenario", {})
    verdicts = result.get("verdicts")
    if run.get("mode") != "qualification" or run.get("promotable") is not True:
        fail(f"{label}: raw rollout is diagnostic, not promotable")
    if scenario.get("tier") != record.get("tier"):
        fail(f"{label}: raw rollout tier does not match compact record")
    if run.get("elapsed_ms") != row.get("elapsed_ms"):
        fail(f"{label}: raw rollout duration does not match compact row")
    if (
        not isinstance(verdicts, list)
        or not verdicts
        or len(verdicts) != row.get("verdicts")
        or any(
            not isinstance(verdict, dict) or verdict.get("passed") is not True
            for verdict in verdicts
        )
    ):
        fail(f"{label}: raw rollout verdicts are missing, failed, or unbound")

    manifest_relative = record.get("inputs", {}).get("manifest")
    if not isinstance(manifest_relative, str) or not manifest_relative:
        fail(f"{label}: rollout manifest path is missing")
    try:
        rollout_manifest = tomllib.loads(
            (ROOT / manifest_relative).read_text(encoding="utf-8")
        )
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        fail(f"{label}: rollout manifest is unreadable: {error}")
    if rollout_manifest.get("schema_version") != 1:
        fail(f"{label}: rollout manifest schema is unsupported")
    manifest_scenarios = [
        candidate
        for candidate in rollout_manifest.get("scenario", [])
        if isinstance(candidate, dict) and candidate.get("id") == scenario.get("id")
    ]
    if len(manifest_scenarios) != 1:
        fail(f"{label}: raw rollout does not name one committed scenario")
    manifest_scenario = manifest_scenarios[0]
    tier = scenario.get("tier")
    scale = manifest_scenario.get(tier)
    shutdown = manifest_scenario.get("shutdown")
    thresholds = manifest_scenario.get("thresholds")
    if not all(isinstance(value, dict) for value in (scale, shutdown, thresholds)):
        fail(f"{label}: committed rollout scenario is malformed")
    expected_shutdown = {
        **shutdown,
        "budget_ms": sum(
            shutdown[field]
            for field in ("drain_grace_ms", "deadline_ms", "flush_timeout_ms")
        ),
        "stream_budget_ms": shutdown["drain_grace_ms"] + shutdown["deadline_ms"],
    }
    expected_scenario = {
        "id": manifest_scenario.get("id"),
        "description": manifest_scenario.get("description"),
        "tier": tier,
        "replicas": manifest_scenario.get("replicas"),
        "workers": scale.get("workers"),
        "requests_per_phase": scale.get("requests_per_phase"),
        "stream_every": scale.get("stream_every"),
        "shutdown": expected_shutdown,
        "thresholds": thresholds,
    }
    if scenario != expected_scenario:
        fail(f"{label}: raw rollout scenario does not exactly echo the manifest")
    if (
        run.get("harness") != "axond rollout harness"
        or run.get("harness_version")
        != record.get("source", {}).get("crate_version")
        or not isinstance(run.get("started_at_unix_ms"), int)
        or isinstance(run.get("started_at_unix_ms"), bool)
        or run["started_at_unix_ms"] <= 0
    ):
        fail(f"{label}: raw rollout harness identity is missing or malformed")

    revisions = result.get("revisions", [])
    by_label = {
        revision.get("label"): revision
        for revision in revisions
        if isinstance(revision, dict)
    }
    expected = {"previous", "candidate-previous-config", "next"}
    if set(by_label) != expected or len(revisions) != len(expected):
        fail(f"{label}: raw rollout revision set is incomplete")
    previous = by_label["previous"]
    compatibility = by_label["candidate-previous-config"]
    candidate = by_label["next"]
    retained = run.get("retained_release", {})
    expected_previous_digest = sha256_digest(
        retained.get("expected_binary_sha256"),
        f"{label}: retained release binary digest",
    )
    sha256_digest(
        retained.get("archive_sha256"), f"{label}: retained release archive digest"
    )
    if (
        previous.get("binary", {}).get("version") != ROLLOUT_PREVIOUS_VERSION
        or candidate.get("binary", {}).get("version") != ROLLOUT_CANDIDATE_VERSION
        or retained.get("expected_version") != previous.get("binary", {}).get("version")
        or expected_previous_digest
        != str(previous.get("binary", {}).get("sha256", "")).lower()
    ):
        fail(f"{label}: retained release pin does not match the previous revision")
    compact_rollout_identity = {
        "rollout_previous_version": previous.get("binary", {}).get("version"),
        "rollout_previous_binary_sha256": previous.get("binary", {}).get("sha256"),
        "rollout_candidate_version": candidate.get("binary", {}).get("version"),
        "rollout_candidate_binary_sha256": candidate.get("binary", {}).get("sha256"),
        "rollout_retained_archive_sha256": retained.get("archive_sha256"),
    }
    for field, expected_value in compact_rollout_identity.items():
        if row.get(field) != expected_value:
            fail(f"{label}: compact {field} does not match raw rollout provenance")

    def semver_triplet(value: Any) -> tuple[int, int, int] | None:
        if not isinstance(value, str):
            return None
        pieces = value.split(".")
        if len(pieces) != 3 or any(not piece.isdigit() for piece in pieces):
            return None
        return tuple(int(piece) for piece in pieces)  # type: ignore[return-value]

    previous_version = semver_triplet(previous.get("binary", {}).get("version"))
    candidate_version = semver_triplet(candidate.get("binary", {}).get("version"))
    if (
        previous_version is None
        or candidate_version is None
        or previous_version >= candidate_version
    ):
        fail(f"{label}: retained release is not older than the candidate")
    identities = sorted(
        {
            json.dumps(
                {
                    "sha256": revision.get("binary", {}).get("sha256"),
                    "version": revision.get("binary", {}).get("version"),
                },
                sort_keys=True,
            )
            for revision in revisions
        }
    )
    if len(identities) != 2:
        fail(f"{label}: raw rollout did not use exactly two binary identities")
    binary_set_digest = hashlib.sha256("\n".join(identities).encode()).hexdigest()
    if (
        record.get("binary", {}).get("sha256") != binary_set_digest
        or record.get("binary", {}).get("version") != "mixed"
    ):
        fail(f"{label}: compact binary set identity does not match raw revisions")
    desired_state_revisions = {
        revision.get("desired_state_revision") for revision in revisions
    }
    config_digests = {
        revision.get("config", {}).get("sha256") for revision in revisions
    }
    if (
        previous.get("binary", {}).get("sha256")
        == compatibility.get("binary", {}).get("sha256")
        or compatibility.get("binary", {}).get("sha256")
        != candidate.get("binary", {}).get("sha256")
        or previous.get("distinct_binary") is not False
        or compatibility.get("distinct_binary") is not True
        or candidate.get("distinct_binary") is not True
        or None in config_digests
        or len(config_digests) != 1
        or None in desired_state_revisions
        or "" in desired_state_revisions
        or len(desired_state_revisions) != 1
    ):
        fail(f"{label}: raw rollout binary/config/durable revision phases are inconsistent")
    source_version = record.get("source", {}).get("crate_version")
    if candidate.get("binary", {}).get("version") != source_version:
        fail(f"{label}: candidate revision version does not match the source crate")
    sha256_digest(
        candidate.get("binary", {}).get("sha256"),
        f"{label}: candidate binary digest",
    )
    verified_control_plane: tuple[str, str, str] | None = None
    for revision_label, revision in by_label.items():
        config = revision.get("config", {})
        normalized = config.get("normalized_toml")
        if (
            not isinstance(normalized, str)
            or not normalized
            or hashlib.sha256(normalized.encode()).hexdigest()
            != config.get("sha256")
        ):
            fail(f"{label}: revision {revision_label!r} config digest is unbound")
        try:
            bootstrap = tomllib.loads(normalized)
        except tomllib.TOMLDecodeError as error:
            fail(f"{label}: revision {revision_label!r} config is invalid TOML: {error}")
        control_plane = bootstrap.get("control_plane")
        secret_store = bootstrap.get("secret_store")
        catalog = bootstrap.get("catalog")
        if (
            bootstrap.get("mode") != "stateful"
            or not isinstance(control_plane, dict)
            or not isinstance(secret_store, dict)
            or not isinstance(catalog, dict)
        ):
            fail(
                f"{label}: revision {revision_label!r} is not a complete stateful "
                "PostgreSQL bootstrap"
            )
        dsn_env = control_plane.get("dsn_env")
        schema = control_plane.get("schema")
        if (
            not isinstance(dsn_env, str)
            or not dsn_env.strip()
            or dsn_env != dsn_env.strip()
            or not isinstance(schema, str)
            or not schema.strip()
            or schema != schema.strip()
            or secret_store.get("backend") != "postgres"
            or secret_store.get("schema") != schema
            or catalog.get("store") != "postgres"
            or catalog.get("schema") != schema
        ):
            fail(
                f"{label}: revision {revision_label!r} does not bind its control "
                "plane, secret store, and catalogue to one PostgreSQL schema"
            )
        if verified_control_plane is None:
            verified_control_plane = (dsn_env, schema, config["sha256"])
    if verified_control_plane is None:
        fail(f"{label}: rollout has no verified PostgreSQL control plane")
    if any(revision.get("exclusive_aliases") != [] for revision in revisions):
        fail(f"{label}: stateful rollout fabricated revision-exclusive aliases")
    compatibility_traffic = [
        phase
        for phase in result.get("traffic", [])
        if phase.get("phase") == "candidate-on-previous-config"
    ]
    if (
        len(compatibility_traffic) != 1
        or compatibility_traffic[0].get("answered", 0) <= 0
        or compatibility_traffic[0]
        .get("by_revision", {})
        .get("candidate-previous-config", 0)
        <= 0
    ):
        fail(f"{label}: candidate did not serve using previous config")
    mixed = result.get("mixed_version", {})
    shared_revision = next(iter(desired_state_revisions))
    if (
        not isinstance(mixed.get("previous_requests"), int)
        or isinstance(mixed.get("previous_requests"), bool)
        or not isinstance(mixed.get("next_requests"), int)
        or isinstance(mixed.get("next_requests"), bool)
        or mixed.get("previous_requests", -1) < 0
        or mixed.get("next_requests", -1) < 0
        or mixed.get("shared_stateful_revision") != shared_revision
        or mixed.get("shared_alias") != "chat"
        or mixed.get("shared_alias") == mixed.get("exclusive_alias")
        or mixed.get("previous_serves_shared_alias") is not True
        or mixed.get("next_serves_shared_alias") is not True
    ):
        fail(f"{label}: mixed-version shared stateful serving contract is not proven")
    compact_stateful_identity = {
        "rollout_shared_stateful_revision": shared_revision,
        "rollout_shared_alias": mixed.get("shared_alias"),
        "rollout_previous_serves_shared_alias": True,
        "rollout_candidate_serves_shared_alias": True,
    }
    for field, expected_value in compact_stateful_identity.items():
        if row.get(field) != expected_value:
            fail(f"{label}: compact {field} does not match raw stateful rollout")

    loss = result.get("loss")
    traffic = result.get("traffic")
    fleet = result.get("fleet")
    if not isinstance(loss, dict) or not isinstance(traffic, list) or not isinstance(fleet, list):
        fail(f"{label}: raw rollout traffic or usage evidence is malformed")
    expected_rows = loss.get("expected_usage_identities")
    observed_rows = loss.get("observed_usage_identities")
    if not isinstance(expected_rows, list) or not isinstance(observed_rows, list):
        fail(f"{label}: raw rollout exact usage ledgers are missing")

    fleet_revision_by_replica: dict[str, str] = {}
    for index, member in enumerate(fleet):
        if not isinstance(member, dict):
            fail(f"{label}: rollout fleet row {index} is malformed")
        replica = member.get("id")
        revision = member.get("revision")
        if (
            not isinstance(replica, str)
            or not replica
            or replica in fleet_revision_by_replica
            or revision not in expected
        ):
            fail(f"{label}: rollout fleet identity {index} is malformed")
        fleet_revision_by_replica[replica] = revision

    reconciliation = loss.get("usage_reconciliation")
    if not isinstance(reconciliation, dict):
        fail(f"{label}: rollout usage reconciliation disclosure is missing")
    expected_exact_replicas = sorted(fleet_revision_by_replica)
    trace_exports = reconciliation.get("otlp_trace_exports")
    claimed_trace_identities = reconciliation.get("otlp_trace_identities")
    claimed_non_usage_trace_identities = reconciliation.get(
        "expected_non_usage_trace_identities"
    )
    claimed_refusal_attempts = loss.get("draining_refusal_attempts")
    claimed_failed_attempts = loss.get("failed_ingress_attempts")
    claimed_unexpected_trace_identities = reconciliation.get(
        "unexpected_otlp_trace_identities"
    )
    claimed_trace_collection_errors = reconciliation.get(
        "otlp_trace_collection_errors"
    )
    if (
        reconciliation.get("mode") != "exact_trace"
        or reconciliation.get("exact_trace_replicas") != expected_exact_replicas
        or reconciliation.get("retained_trace_context") != "loopback_otlp_http"
        or reconciliation.get("otlp_trace_export_replicas")
        != expected_exact_replicas
        or not isinstance(claimed_non_usage_trace_identities, list)
        or not isinstance(claimed_refusal_attempts, list)
        or not isinstance(claimed_failed_attempts, list)
        or not isinstance(claimed_trace_identities, list)
        or not isinstance(claimed_unexpected_trace_identities, list)
        or claimed_trace_collection_errors != []
        or not isinstance(trace_exports, int)
        or isinstance(trace_exports, bool)
        or trace_exports < len(expected_exact_replicas)
    ):
        fail(f"{label}: exact trace reconciliation or its OTLP witness is malformed")

    def canonical_rollout_trace(value: object) -> bool:
        return (
            isinstance(value, str)
            and len(value) == 32
            and value.startswith("61786f6e642d726f")
            and value[16:] != "0000000000000000"
            and all(character in "0123456789abcdef" for character in value)
        )

    def canonical_request_id(value: object) -> bool:
        if not isinstance(value, str) or not value.startswith("req_"):
            return False
        uuid = value[4:]
        return (
            len(uuid) == 36
            and all(uuid[index] == "-" for index in (8, 13, 18, 23))
            and uuid[14] == "7"
            and uuid[19] in "89ab"
            and all(
                index in (8, 13, 18, 23) or character in "0123456789abcdef"
                for index, character in enumerate(uuid)
            )
        )

    statuses = {"ok", "upstream_error", "client_cancelled", "partial", "rejected"}
    expected_counter: Counter[tuple[str, str]] = Counter()
    expected_status: dict[tuple[str, str], str] = {}
    expected_by_replica: Counter[str] = Counter()
    for index, identity in enumerate(expected_rows):
        if not isinstance(identity, dict):
            fail(f"{label}: expected usage identity {index} is malformed")
        replica = identity.get("replica")
        trace_id = identity.get("trace_id")
        status = identity.get("status")
        if (
            not isinstance(replica, str)
            or not replica
            or replica not in fleet_revision_by_replica
            or not canonical_rollout_trace(trace_id)
            or status not in statuses
        ):
            fail(f"{label}: expected usage identity {index} is not canonical")
        key = (replica, trace_id)
        expected_counter[key] += 1
        expected_status[key] = status
        expected_by_replica[replica] += 1
    if any(count != 1 for count in expected_counter.values()):
        fail(f"{label}: expected rollout trace identities are duplicated")
    if len({trace_id for _, trace_id in expected_counter}) != len(expected_rows):
        fail(f"{label}: one rollout caller trace is expected by more than one replica")
    if set(expected_by_replica) != set(fleet_revision_by_replica):
        fail(f"{label}: every rollout fleet replica must own exact caller traces")
    expected_refusal_attempts: list[dict[str, Any]] = []
    expected_non_usage_keys: set[tuple[str, str]] = set()
    refusal_callers: set[tuple[int, str]] = set()
    caller_trace_ids: dict[int, str] = {}
    trace_caller_ids: dict[str, int] = {}
    caller_request_count = loss.get("caller_requests")
    if (
        not isinstance(caller_request_count, int)
        or isinstance(caller_request_count, bool)
        or caller_request_count < 0
    ):
        fail(f"{label}: rollout caller request count is malformed")
    for index, attempt in enumerate(claimed_refusal_attempts):
        if not isinstance(attempt, dict):
            fail(f"{label}: draining-refusal attempt {index} is malformed")
        caller_id = attempt.get("caller_id")
        trace_id = attempt.get("trace_id")
        refused_replica = attempt.get("refused_replica")
        accepted_replica = attempt.get("accepted_replica")
        accepted_status = attempt.get("accepted_status")
        key = (refused_replica, trace_id)
        caller_key = (caller_id, refused_replica)
        if (
            not isinstance(caller_id, int)
            or isinstance(caller_id, bool)
            or caller_id < 0
            or caller_id >= caller_request_count
            or refused_replica not in fleet_revision_by_replica
            or accepted_replica not in fleet_revision_by_replica
            or refused_replica == accepted_replica
            or not isinstance(accepted_status, int)
            or isinstance(accepted_status, bool)
            or not 200 <= accepted_status < 300
            or not canonical_rollout_trace(trace_id)
            or key in expected_counter
            or key in expected_non_usage_keys
            or caller_key in refusal_callers
            or caller_trace_ids.get(caller_id, trace_id) != trace_id
            or trace_caller_ids.get(trace_id, caller_id) != caller_id
            or (accepted_replica, trace_id) not in expected_counter
        ):
            fail(f"{label}: draining-refusal attempt {index} is not canonical")
        expected_non_usage_keys.add(key)
        refusal_callers.add(caller_key)
        caller_trace_ids[caller_id] = trace_id
        trace_caller_ids[trace_id] = caller_id
        expected_refusal_attempts.append(
            {
                "caller_id": caller_id,
                "trace_id": trace_id,
                "refused_replica": refused_replica,
                "accepted_replica": accepted_replica,
                "accepted_status": accepted_status,
            }
        )
    expected_refusal_attempts.sort(
        key=lambda attempt: (
            attempt["caller_id"],
            attempt["trace_id"],
            attempt["refused_replica"],
            attempt["accepted_replica"],
            attempt["accepted_status"],
        )
    )
    if claimed_refusal_attempts != expected_refusal_attempts:
        fail(f"{label}: draining-refusal attempt ledger is not canonical")
    expected_non_usage_trace_identities = [
        {
            "replica": attempt["refused_replica"],
            "trace_id": attempt["trace_id"],
            "reason": "draining_refusal",
        }
        for attempt in expected_refusal_attempts
    ]
    expected_non_usage_trace_identities.sort(
        key=lambda identity: (
            identity["replica"],
            identity["trace_id"],
            identity["reason"],
        )
    )
    if claimed_non_usage_trace_identities != expected_non_usage_trace_identities:
        fail(f"{label}: non-usage trace ledger does not match exact refusal attempts")

    expected_trace_identities = [
        {"replica": replica, "trace_id": trace_id}
        for replica, trace_id in sorted(
            set(expected_counter) | expected_non_usage_keys
        )
    ]
    expected_failed_attempts: list[dict[str, Any]] = []
    failed_reasons_by_identity: dict[tuple[str, str], str] = {}
    failed_attempt_keys: set[tuple[int, str, str, str]] = set()
    usage_trace_owners: dict[str, set[str]] = defaultdict(set)
    for identity in expected_rows:
        usage_trace_owners[identity["trace_id"]].add(identity["replica"])
    transport_failures_by_replica: Counter[str] = Counter()
    for index, attempt in enumerate(claimed_failed_attempts):
        if not isinstance(attempt, dict):
            fail(f"{label}: failed ingress attempt {index} is malformed")
        caller_id = attempt.get("caller_id")
        trace_id = attempt.get("trace_id")
        replica = attempt.get("replica")
        reason = attempt.get("reason")
        attempt_key = (caller_id, trace_id, replica, reason)
        identity_key = (replica, trace_id)
        if reason == "untyped_503":
            fail(f"{label}: an untyped 503 attempt is not promotable")
        if (
            not isinstance(caller_id, int)
            or isinstance(caller_id, bool)
            or caller_id < 0
            or caller_id >= caller_request_count
            or replica not in fleet_revision_by_replica
            or not canonical_rollout_trace(trace_id)
            or reason != "transport_failure"
            or attempt_key in failed_attempt_keys
            or identity_key in failed_reasons_by_identity
            or caller_trace_ids.get(caller_id, trace_id) != trace_id
            or trace_caller_ids.get(trace_id, caller_id) != caller_id
            or not (usage_trace_owners.get(trace_id, set()) - {replica})
        ):
            fail(f"{label}: failed ingress attempt {index} is not canonical")
        failed_attempt_keys.add(attempt_key)
        failed_reasons_by_identity[identity_key] = reason
        caller_trace_ids[caller_id] = trace_id
        trace_caller_ids[trace_id] = caller_id
        transport_failures_by_replica[replica] += 1
        expected_failed_attempts.append(
            {
                "caller_id": caller_id,
                "trace_id": trace_id,
                "replica": replica,
                "reason": reason,
            }
        )
    expected_failed_attempts.sort(
        key=lambda attempt: (
            attempt["caller_id"],
            attempt["trace_id"],
            attempt["replica"],
            attempt["reason"],
        )
    )
    fleet_refusals = {}
    for index, member in enumerate(fleet):
        replica = member.get("id") if isinstance(member, dict) else None
        refusals = member.get("refusals") if isinstance(member, dict) else None
        if (
            replica not in fleet_revision_by_replica
            or replica in fleet_refusals
            or not isinstance(refusals, int)
            or isinstance(refusals, bool)
            or refusals < 0
        ):
            fail(f"{label}: rollout fleet row {index} has malformed refusal evidence")
        fleet_refusals[replica] = refusals
    draining_refusals = {}
    claimed_per_replica = loss.get("per_replica")
    if not isinstance(claimed_per_replica, list):
        fail(f"{label}: rollout failed attempts lack per-replica evidence")
    for index, per_replica_row in enumerate(claimed_per_replica):
        replica = (
            per_replica_row.get("replica")
            if isinstance(per_replica_row, dict)
            else None
        )
        refusals = (
            per_replica_row.get("caller_requests_refused_while_draining")
            if isinstance(per_replica_row, dict)
            else None
        )
        if (
            replica not in fleet_refusals
            or replica in draining_refusals
            or not isinstance(refusals, int)
            or isinstance(refusals, bool)
            or not 0 <= refusals <= fleet_refusals[replica]
        ):
            fail(f"{label}: rollout per-replica row {index} has malformed refusal evidence")
        draining_refusals[replica] = refusals
    expected_transport_failures = Counter(
        {
            replica: fleet_refusals[replica]
            - draining_refusals.get(replica, 0)
            for replica in fleet_refusals
            if fleet_refusals[replica] - draining_refusals.get(replica, 0) > 0
        }
    )
    if transport_failures_by_replica != expected_transport_failures:
        fail(f"{label}: rollout transport attempts do not match fleet refusals")
    expected_trace_keys = {
        (identity["replica"], identity["trace_id"])
        for identity in expected_trace_identities
    }
    observed_trace_keys = set()
    for index, identity in enumerate(claimed_trace_identities):
        if (
            not isinstance(identity, dict)
            or identity.get("replica") not in fleet_revision_by_replica
            or not canonical_rollout_trace(identity.get("trace_id"))
        ):
            fail(f"{label}: OTLP caller-trace identity {index} is malformed")
        observed_trace_keys.add((identity["replica"], identity["trace_id"]))
    expected_unexpected_trace_identities = [
        {
            "replica": replica,
            "trace_id": trace_id,
            "reason": failed_reasons_by_identity.get(
                (replica, trace_id), "unattributed"
            ),
        }
        for replica, trace_id in sorted(observed_trace_keys - expected_trace_keys)
    ]
    if (
        claimed_failed_attempts != expected_failed_attempts
        or claimed_unexpected_trace_identities
        != expected_unexpected_trace_identities
    ):
        fail(f"{label}: failed-attempt trace attribution is not canonical")
    if claimed_trace_identities != expected_trace_identities:
        fail(f"{label}: OTLP caller-trace witness does not match the exact caller ledger")
    trace_identities_sha256 = hashlib.sha256(
        json.dumps(
            expected_trace_identities,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()

    observed_by_identity: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    observed_by_replica: Counter[str] = Counter()
    unidentified_by_replica: Counter[str] = Counter()
    unexpected_by_replica: Counter[str] = Counter()
    request_ids: Counter[str] = Counter()
    usage_by_status: Counter[str] = Counter()
    for index, identity in enumerate(observed_rows):
        if not isinstance(identity, dict):
            fail(f"{label}: observed usage identity {index} is malformed")
        replica = identity.get("replica")
        trace_id = identity.get("trace_id")
        status = identity.get("status")
        request_id = identity.get("request_id")
        if (
            not isinstance(replica, str)
            or not replica
            or replica not in fleet_revision_by_replica
        ):
            fail(f"{label}: observed usage identity {index} has no replica")
        observed_by_replica[replica] += 1
        usage_by_status[status if isinstance(status, str) else "unknown"] += 1
        trace_status_valid = canonical_rollout_trace(trace_id) and status in statuses
        request_valid = canonical_request_id(request_id)
        if request_valid:
            request_ids[request_id] += 1
        if not trace_status_valid or not request_valid:
            unidentified_by_replica[replica] += 1
        if trace_status_valid:
            observed_by_identity[(replica, trace_id)].append(identity)
        else:
            unexpected_by_replica[replica] += 1

    missing_by_replica: Counter[str] = Counter()
    duplicate_by_replica: Counter[str] = Counter()
    mismatch_by_replica: Counter[str] = Counter()
    for key, status in expected_status.items():
        replica = key[0]
        matches = observed_by_identity.get(key, [])
        if not matches:
            missing_by_replica[replica] += 1
            continue
        if not any(identity.get("status") == status for identity in matches):
            mismatch_by_replica[replica] += 1
        extras = len(matches) - 1
        duplicate_by_replica[replica] += extras
        unexpected_by_replica[replica] += extras
    for key, matches in observed_by_identity.items():
        if key in expected_status:
            continue
        replica = key[0]
        unexpected_by_replica[replica] += len(matches)
        duplicate_by_replica[replica] += max(0, len(matches) - 1)

    fleet_refusals: dict[str, int] = {}
    for index, member in enumerate(fleet):
        if not isinstance(member, dict):
            fail(f"{label}: rollout fleet row {index} is malformed")
        replica = member.get("id")
        refusals = member.get("refusals")
        if (
            not isinstance(replica, str)
            or not replica
            or replica in fleet_refusals
            or not isinstance(refusals, int)
            or isinstance(refusals, bool)
            or refusals < 0
        ):
            fail(f"{label}: rollout fleet row {index} has malformed refusal evidence")
        fleet_refusals[replica] = refusals
    replicas = sorted(set(expected_by_replica) | set(observed_by_replica) | set(fleet_refusals))
    claimed_per_replica = loss.get("per_replica")
    if not isinstance(claimed_per_replica, list):
        fail(f"{label}: rollout per-replica usage ledger is missing")
    draining_refusals: dict[str, int] = {}
    for index, claimed in enumerate(claimed_per_replica):
        if not isinstance(claimed, dict):
            fail(f"{label}: rollout per-replica usage row {index} is malformed")
        replica = claimed.get("replica")
        refusals = claimed.get("caller_requests_refused_while_draining")
        if (
            not isinstance(replica, str)
            or replica in draining_refusals
            or not isinstance(refusals, int)
            or isinstance(refusals, bool)
            or refusals < 0
            or refusals > fleet_refusals.get(replica, -1)
        ):
            fail(f"{label}: rollout draining-refusal row {index} is malformed")
        draining_refusals[replica] = refusals
    non_usage_by_replica = Counter(
        identity["replica"] for identity in expected_non_usage_trace_identities
    )
    usage_trace_owners: dict[str, set[str]] = defaultdict(set)
    for replica, trace_id in expected_counter:
        usage_trace_owners[trace_id].add(replica)
    if (
        non_usage_by_replica
        != Counter(
            {
                replica: count
                for replica, count in draining_refusals.items()
                if count > 0
            }
        )
        or any(
            not (
                usage_trace_owners.get(identity["trace_id"], set())
                - {identity["replica"]}
            )
            for identity in expected_non_usage_trace_identities
        )
    ):
        fail(f"{label}: non-usage trace ledger is not exact retried drain-refusal evidence")
    recomputed_per_replica = [
        {
            "replica": replica,
            "reconciliation": "exact_trace",
            "caller_requests_answered": expected_by_replica[replica],
            "usage_records": observed_by_replica[replica],
            "caller_requests_refused_while_draining": draining_refusals.get(replica, 0),
            "retry_duplicates": 0,
            "missing": missing_by_replica[replica],
            "unexplained_surplus": unexpected_by_replica[replica],
            "identity_duplicates": duplicate_by_replica[replica],
            "status_mismatches": mismatch_by_replica[replica],
            "unidentified": unidentified_by_replica[replica],
        }
        for replica in replicas
    ]
    if claimed_per_replica != recomputed_per_replica:
        fail(f"{label}: rollout per-replica usage ledger is not independently reproducible")

    missing = sum(missing_by_replica.values())
    unexpected = sum(unexpected_by_replica.values())
    identity_duplicates = sum(duplicate_by_replica.values())
    status_mismatches = sum(mismatch_by_replica.values())
    unidentified = sum(unidentified_by_replica.values())
    request_id_duplicates = sum(count - 1 for count in request_ids.values())
    expected_reconciliation = {
        "mode": "exact_trace",
        "exact_trace_replicas": expected_exact_replicas,
        "retained_trace_context": "loopback_otlp_http",
        "otlp_trace_exports": trace_exports,
        "otlp_trace_export_replicas": expected_exact_replicas,
        "expected_non_usage_trace_identities": expected_non_usage_trace_identities,
        "otlp_trace_collection_errors": [],
        "otlp_trace_identities": expected_trace_identities,
        "unexpected_otlp_trace_identities": expected_unexpected_trace_identities,
    }
    if reconciliation != expected_reconciliation:
        fail(f"{label}: rollout usage reconciliation disclosure is not reproducible")
    compact_reconciliation = {
        "rollout_usage_reconciliation": "exact_trace",
        "rollout_exact_trace_replicas": len(expected_exact_replicas),
        "rollout_retained_trace_context": "loopback_otlp_http",
        "rollout_otlp_trace_exports": trace_exports,
        "rollout_otlp_trace_export_replicas": len(expected_exact_replicas),
        "rollout_otlp_trace_identities": len(expected_trace_identities),
        "rollout_otlp_trace_identities_sha256": trace_identities_sha256,
    }
    for field, expected_value in compact_reconciliation.items():
        if row.get(field) != expected_value:
            fail(f"{label}: compact {field} does not match raw usage reconciliation")
    traffic_fields = ("offered", "answered", "errors", "unanswered", "torn_streams")
    traffic_totals = {field: 0 for field in traffic_fields}
    for index, phase in enumerate(traffic):
        if not isinstance(phase, dict):
            fail(f"{label}: rollout traffic phase {index} is malformed")
        for field in traffic_fields:
            value = phase.get(field)
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value < 0
            ):
                fail(f"{label}: rollout traffic phase {index} has malformed {field}")
            traffic_totals[field] += value
    expected_loss_fields = {
        **traffic_totals,
        "caller_requests": traffic_totals["offered"],
        "usage_records_expected": len(expected_rows),
        "usage_records_observed": len(observed_rows),
        "usage_records_distinct": len(request_ids),
        "usage_reconciliation": expected_reconciliation,
        "draining_refusal_attempts": expected_refusal_attempts,
        "failed_ingress_attempts": expected_failed_attempts,
        "usage_identity_duplicates": identity_duplicates,
        "usage_record_id_duplicates": request_id_duplicates,
        "usage_status_mismatches": status_mismatches,
        "usage_records_unidentified": unidentified,
        "usage_records_retry_duplicates": 0,
        "usage_records_missing": missing,
        "usage_records_surplus": unexpected,
        "refusals_retried": sum(draining_refusals.values()),
        "usage_by_status": dict(sorted(usage_by_status.items())),
    }
    for field, expected_value in expected_loss_fields.items():
        if loss.get(field) != expected_value:
            fail(f"{label}: rollout loss.{field} is not independently reproducible")

    def nonnegative_integer(value: object, field: str) -> int:
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            fail(f"{label}: rollout {field} is missing or malformed")
        return value

    def nonnegative_number(value: object, field: str) -> int | float:
        if (
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not math.isfinite(float(value))
            or value < 0
        ):
            fail(f"{label}: rollout {field} is missing or malformed")
        return value

    requests_per_phase = nonnegative_integer(
        scenario.get("requests_per_phase"), "scenario.requests_per_phase"
    )
    stream_every = nonnegative_integer(
        scenario.get("stream_every"), "scenario.stream_every"
    )
    expected_streamed = (
        0
        if stream_every == 0
        else (requests_per_phase + stream_every - 1) // stream_every
    )
    traffic_by_phase: dict[str, dict[str, Any]] = {}
    routed_by_replica: Counter[str] = Counter()
    retried = 0
    for index, phase in enumerate(traffic):
        phase_name = phase.get("phase")
        if (
            not isinstance(phase_name, str)
            or not phase_name
            or phase_name in traffic_by_phase
        ):
            fail(f"{label}: rollout traffic phase names are malformed or duplicated")
        traffic_by_phase[phase_name] = phase
        if phase.get("offered") != requests_per_phase:
            fail(f"{label}: rollout phase {phase_name!r} did not offer committed load")
        answered = nonnegative_integer(phase.get("answered"), f"{phase_name}.answered")
        errors = nonnegative_integer(phase.get("errors"), f"{phase_name}.errors")
        unanswered = nonnegative_integer(
            phase.get("unanswered"), f"{phase_name}.unanswered"
        )
        torn = nonnegative_integer(
            phase.get("torn_streams"), f"{phase_name}.torn_streams"
        )
        if answered + errors + unanswered != requests_per_phase or torn > answered:
            fail(f"{label}: rollout phase {phase_name!r} outcome counts do not reconcile")
        if phase.get("streamed") != expected_streamed:
            fail(f"{label}: rollout phase {phase_name!r} did not exercise committed mix")
        if nonnegative_integer(phase.get("elapsed_ms"), f"{phase_name}.elapsed_ms") == 0:
            fail(f"{label}: rollout phase {phase_name!r} has no measured duration")
        nonnegative_number(phase.get("answered_rps"), f"{phase_name}.answered_rps")
        latency = phase.get("latency_ms")
        if not isinstance(latency, dict):
            fail(f"{label}: rollout phase {phase_name!r} has no latency evidence")
        latency_values = [
            nonnegative_number(latency.get(field), f"{phase_name}.latency_ms.{field}")
            for field in ("p50", "p95", "p99")
        ]
        if latency_values != sorted(latency_values):
            fail(f"{label}: rollout phase {phase_name!r} latency percentiles are invalid")

        def count_table(field: str) -> dict[str, int]:
            table = phase.get(field)
            if (
                not isinstance(table, dict)
                or any(
                    not isinstance(key, str)
                    or not key
                    or not isinstance(value, int)
                    or isinstance(value, bool)
                    or value < 0
                    for key, value in table.items()
                )
            ):
                fail(f"{label}: rollout phase {phase_name!r} {field} is malformed")
            return table

        by_status = count_table("by_status")
        by_replica = count_table("by_replica")
        by_revision = count_table("by_revision")
        if sum(by_status.values()) != requests_per_phase:
            fail(f"{label}: rollout phase {phase_name!r} status counts do not reconcile")
        attributed = answered + errors
        if sum(by_replica.values()) != attributed or sum(by_revision.values()) != attributed:
            fail(f"{label}: rollout phase {phase_name!r} routing counts do not reconcile")
        routed_by_replica.update(by_replica)
        retried += nonnegative_integer(phase.get("retried"), f"{phase_name}.retried")
    if (
        retried < loss.get("refusals_retried", 0)
        or retried > sum(fleet_refusals.values())
    ):
        fail(f"{label}: rollout retry tally contradicts refusal evidence")
    if run.get("elapsed_ms", 0) < sum(phase["elapsed_ms"] for phase in traffic):
        fail(f"{label}: rollout phase durations exceed the recorded run duration")

    def phase_envelope(select: Any) -> tuple[float, float | None]:
        phases = [phase for phase in traffic if select(phase["phase"])]
        if not phases:
            return 0.0, None
        # Match Rust's ordered `Iterator::sum::<f64>()` exactly. Python's
        # built-in sum uses a different floating-point algorithm and can round
        # the same finite sequence one ULP differently.
        answered_rps_total = 0.0
        for phase in phases:
            answered_rps_total += phase["answered_rps"]
        answered_rps = answered_rps_total / len(phases)
        if not math.isfinite(answered_rps):
            fail(f"{label}: rollout capacity envelope is non-finite")
        latency_p95 = max(phase["latency_ms"]["p95"] for phase in phases)
        return answered_rps, latency_p95

    steady_rps, steady_p95 = phase_envelope(lambda name: name.startswith("steady"))
    degraded_rps, degraded_p95 = phase_envelope(lambda name: "drain" in name)
    degraded_fraction = degraded_rps / steady_rps if steady_rps > 0 else 0.0
    if not math.isfinite(degraded_fraction):
        fail(f"{label}: rollout capacity envelope is non-finite")
    expected_capacity = {
        "steady_answered_rps": steady_rps,
        "degraded_answered_rps": degraded_rps,
        "degraded_fraction": degraded_fraction,
        "steady_latency_p95_ms": steady_p95,
        "degraded_latency_p95_ms": degraded_p95,
    }
    if result.get("capacity") != expected_capacity:
        fail(f"{label}: rollout capacity envelope is not reproducible from traffic")

    timeline = result.get("timeline")
    if not isinstance(timeline, list) or not timeline:
        fail(f"{label}: rollout timeline is missing")
    previous_event_ms = -1
    for index, event in enumerate(timeline):
        if not isinstance(event, dict):
            fail(f"{label}: rollout timeline event {index} is malformed")
        at_ms = nonnegative_integer(event.get("at_ms"), f"timeline[{index}].at_ms")
        if (
            at_ms < previous_event_ms
            or at_ms > run["elapsed_ms"]
            or any(
                not isinstance(event.get(field), str) or not event[field]
                for field in ("phase", "kind", "detail")
            )
        ):
            fail(f"{label}: rollout timeline event {index} is inconsistent")
        previous_event_ms = at_ms

    fleet_by_id: dict[str, dict[str, Any]] = {}
    for index, member in enumerate(fleet):
        replica = member.get("id")
        if not isinstance(replica, str) or not replica or replica in fleet_by_id:
            fail(f"{label}: rollout fleet identities are malformed or duplicated")
        if member.get("revision") not in expected:
            fail(f"{label}: rollout fleet member {replica!r} has an unknown revision")
        for field in (
            "requests_served",
            "requests_after_withdrawal",
            "refusals",
            "usage_records",
        ):
            nonnegative_integer(member.get(field), f"fleet[{replica}].{field}")
        for field in ("admitted_at_ms", "admission_took_ms", "withdrawn_at_ms"):
            value = member.get(field)
            if value is not None:
                nonnegative_integer(value, f"fleet[{replica}].{field}")
        if member.get("admitted_at_ms") is None or member.get("admission_took_ms") is None:
            fail(f"{label}: rollout fleet member {replica!r} was never admitted")
        if not isinstance(member.get("retired"), bool):
            fail(f"{label}: rollout fleet member {replica!r} retirement state is malformed")
        if member["requests_served"] != (
            routed_by_replica[replica] + member["refusals"]
        ):
            fail(f"{label}: rollout fleet member {replica!r} serving count is unbound")
        if member["usage_records"] != observed_by_replica[replica]:
            fail(f"{label}: rollout fleet member {replica!r} usage count is unbound")
        fleet_by_id[replica] = member
    if set(replicas) != set(fleet_by_id):
        fail(f"{label}: exact usage ledgers do not cover precisely the rollout fleet")

    drains = result.get("drains")
    if not isinstance(drains, list) or not drains:
        fail(f"{label}: rollout drain evidence is missing")
    drains_by_replica: dict[str, dict[str, Any]] = {}
    for index, drain in enumerate(drains):
        if not isinstance(drain, dict):
            fail(f"{label}: rollout drain {index} is malformed")
        replica = drain.get("replica")
        if (
            not isinstance(replica, str)
            or replica not in fleet_by_id
            or replica in drains_by_replica
            or drain.get("revision") != fleet_by_id[replica].get("revision")
        ):
            fail(f"{label}: rollout drain {index} does not bind one fleet member")
        for field in (
            "signalled_at_ms",
            "exit_budget_ms",
            "requests_after_withdrawal",
            "dispatches_after_withdrawal",
            "dispatches_beyond_drain_grace",
            "drain_grace_ms",
            "usage_records_flushed",
        ):
            nonnegative_integer(drain.get(field), f"drains[{replica}].{field}")
        for field in (
            "readiness_removed_after_ms",
            "exited_after_ms",
            "worst_dispatch_lag_ms",
        ):
            value = drain.get(field)
            if value is not None:
                nonnegative_integer(value, f"drains[{replica}].{field}")
        if (
            drain.get("drain_grace_ms") != shutdown.get("drain_grace_ms")
            or drain.get("exit_budget_ms") != expected_shutdown["budget_ms"]
            or drain.get("exit_clean") is not True
            or fleet_by_id[replica].get("retired") is not True
            or fleet_by_id[replica].get("withdrawn_at_ms") is None
            or drain.get("requests_after_withdrawal")
            != fleet_by_id[replica].get("requests_after_withdrawal")
        ):
            fail(f"{label}: rollout drain {replica!r} contradicts fleet lifecycle")
        buffered = drain.get("buffered_in_flight")
        stream = drain.get("stream_in_flight")
        if (
            not isinstance(buffered, dict)
            or not isinstance(stream, dict)
            or not isinstance(buffered.get("status"), int)
            or isinstance(buffered.get("status"), bool)
            or not 200 <= buffered["status"] < 300
            or buffered.get("usage_status") != "ok"
            or stream.get("usage_status") != "client_cancelled"
            or stream.get("within_deadline") is not True
            or nonnegative_integer(
                stream.get("relayed_bytes"), f"drains[{replica}].stream.relayed_bytes"
            )
            == 0
        ):
            fail(f"{label}: rollout drain {replica!r} did not preserve in-flight work")
        nonnegative_integer(
            buffered.get("completed_after_signal_ms"),
            f"drains[{replica}].buffered.completed_after_signal_ms",
        )
        cut_after = nonnegative_integer(
            stream.get("cut_after_signal_ms"),
            f"drains[{replica}].stream.cut_after_signal_ms",
        )
        if cut_after > (
            expected_shutdown["stream_budget_ms"]
            + thresholds["max_stream_cut_observation_slack_ms"]
        ):
            fail(f"{label}: rollout drain {replica!r} exceeded its stream deadline")
        drains_by_replica[replica] = drain
    if {replica for replica, member in fleet_by_id.items() if member["retired"]} != set(
        drains_by_replica
    ):
        fail(f"{label}: rollout retired fleet and drain ledgers do not match")

    usage_verdicts = {
        "max_usage_record_loss": (missing, thresholds.get("max_usage_record_loss")),
        "unexplained_usage_record_surplus": (unexpected, 0),
        "duplicate_usage_trace_identities": (identity_duplicates, 0),
        "usage_status_mismatches": (status_mismatches, 0),
        "unidentified_usage_records": (unidentified, 0),
        "duplicate_usage_record_ids": (request_id_duplicates, 0),
    }
    verdicts_by_name = defaultdict(list)
    for verdict in verdicts:
        if isinstance(verdict, dict):
            verdicts_by_name[verdict.get("threshold")].append(verdict)
    for name, (value, bound) in usage_verdicts.items():
        if not isinstance(bound, (int, float)) or isinstance(bound, bool):
            fail(f"{label}: rollout threshold {name!r} is missing or malformed")
        matches = verdicts_by_name[name]
        independently_passed = value <= bound
        if len(matches) != 1 or (
            matches[0].get("comparison") != "<="
            or matches[0].get("value") != value
            or matches[0].get("bound") != bound
            or matches[0].get("passed") is not independently_passed
            or independently_passed is not True
        ):
            fail(f"{label}: rollout usage verdict {name!r} is not independently valid")
    trace_verdicts = verdicts_by_name["otlp_trace_context_exported"]
    if (
        len(trace_verdicts) != 1
        or trace_verdicts[0].get("comparison") != ">="
        or trace_verdicts[0].get("value")
        != len(reconciliation["otlp_trace_export_replicas"])
        or trace_verdicts[0].get("bound") != len(expected_exact_replicas)
        or trace_verdicts[0].get("passed") is not True
    ):
        fail(f"{label}: rollout OTLP trace-context verdict is not independently valid")
    trace_identity_verdicts = verdicts_by_name[
        "otlp_trace_export_identity_mismatches"
    ]
    if (
        len(trace_identity_verdicts) != 1
        or trace_identity_verdicts[0].get("comparison") != "<="
        or trace_identity_verdicts[0].get("value") != 0
        or trace_identity_verdicts[0].get("bound") != 0
        or trace_identity_verdicts[0].get("passed") is not True
    ):
        fail(f"{label}: rollout OTLP process-identity verdict is not independently valid")

    environment = result.get("environment", {})
    expected_fields = {
        "source.git_commit": (
            environment.get("source", {}).get("git_commit"),
            record.get("source", {}).get("git_commit"),
        ),
        "source.git_dirty": (
            environment.get("source", {}).get("git_dirty"),
            record.get("source", {}).get("git_dirty"),
        ),
        "source.crate_version": (
            environment.get("source", {}).get("crate_version"),
            record.get("source", {}).get("crate_version"),
        ),
        "toolchain.cargo_profile": (
            environment.get("toolchain", {}).get("cargo_profile"),
            record.get("binary", {}).get("cargo_profile"),
        ),
        "toolchain.rustc": (
            environment.get("toolchain", {}).get("rustc"),
            record.get("binary", {}).get("rustc"),
        ),
        "manifest.path": (
            environment.get("manifest", {}).get("path"),
            record.get("inputs", {}).get("manifest"),
        ),
        "manifest.sha256": (
            environment.get("manifest", {}).get("sha256"),
            record.get("inputs", {}).get("manifest_sha256"),
        ),
    }
    for field, (actual, expected_value) in expected_fields.items():
        if actual != expected_value:
            fail(f"{label}: raw {field} does not match compact record")

    migration = result.get("migration")
    if not isinstance(migration, dict):
        fail(f"{label}: raw migration evidence is missing")

    migration_target = migration.get("target")
    if not isinstance(migration_target, dict):
        fail(f"{label}: raw migration target identity is missing")
    target_dsn_env = migration_target.get("dsn_env")
    target_schema = migration_target.get("schema")
    target_config_sha256 = sha256_digest(
        migration_target.get("config_sha256"),
        f"{label}: migration target config digest",
    )
    expected_dsn_env, expected_schema, expected_config_sha256 = verified_control_plane
    if (
        target_dsn_env != expected_dsn_env
        or target_schema != expected_schema
        or target_config_sha256 != expected_config_sha256
    ):
        fail(
            f"{label}: migration target is not bound to the digest-bound "
            "PostgreSQL bootstrap"
        )

    def command_record(command: object, field: str) -> dict[str, Any]:
        if not isinstance(command, dict):
            fail(f"{label}: migration command {field} is missing")
        argv = command.get("argv")
        exit_code = command.get("exit_code")
        succeeded = command.get("succeeded")
        if (
            not isinstance(argv, list)
            or not argv
            or any(not isinstance(argument, str) or not argument for argument in argv)
            or not isinstance(exit_code, int)
            or isinstance(exit_code, bool)
            or not isinstance(succeeded, bool)
            or succeeded is not (exit_code == 0)
            or not isinstance(command.get("output"), str)
        ):
            fail(f"{label}: migration command {field} is malformed")
        return command

    preflight = migration.get("preflight")
    status = migration.get("status")
    preflight = command_record(preflight, "preflight")
    status = command_record(status, "status")
    control_plane_description = migration.get("control_plane")
    if (
        not isinstance(preflight, dict)
        or not isinstance(status, dict)
        or preflight.get("succeeded") is not True
        or status.get("succeeded") is not True
        or migration.get("gate_passed") is not True
        or not isinstance(control_plane_description, str)
        or not control_plane_description.strip()
    ):
        fail(f"{label}: rollout deployment gate did not pass against PostgreSQL")
    matrix = migration.get("matrix", {})
    if matrix.get("evaluated") is not True:
        fail(f"{label}: raw migration matrix was not evaluated")
    if matrix.get("skipped_reason") is not None:
        fail(f"{label}: evaluated migration matrix carries a skip reason")
    for command in (
        "previous_apply",
        "previous_status_before",
        "candidate_apply",
        "candidate_status_after",
    ):
        if command_record(matrix.get(command), command).get("succeeded") is not True:
            fail(f"{label}: raw migration command {command} did not pass")
    command_record(matrix.get("candidate_status_before"), "candidate_status_before")
    command_record(
        matrix.get("previous_status_after_candidate"),
        "previous_status_after_candidate",
    )
    def migration_rows(field: str) -> list[dict[str, Any]]:
        rows = matrix.get(field)
        if not isinstance(rows, list) or not rows:
            fail(f"{label}: raw migration matrix has no {field} ledger")
        versions: list[int] = []
        for index, migration in enumerate(rows):
            if not isinstance(migration, dict):
                fail(f"{label}: {field} row {index} is malformed")
            version = migration.get("version")
            if (
                not isinstance(version, int)
                or isinstance(version, bool)
                or version < 0
                or not isinstance(migration.get("name"), str)
                or not migration["name"]
                or not isinstance(migration.get("checksum"), str)
                or not migration["checksum"]
            ):
                fail(f"{label}: {field} row {index} is incomplete")
            versions.append(version)
        if versions != sorted(set(versions)):
            fail(f"{label}: {field} versions are not unique and ordered")
        return rows

    previous_versions = migration_rows("previous_versions")
    candidate_versions = migration_rows("candidate_versions")
    if candidate_versions[: len(previous_versions)] != previous_versions:
        fail(f"{label}: candidate migration ledger does not extend the retained ledger")
    candidate_suffix = candidate_versions[len(previous_versions) :]
    recomputed_added = [migration["version"] for migration in candidate_suffix]
    added = matrix.get("candidate_added_versions")
    if added != recomputed_added:
        fail(f"{label}: added-version ledger does not match the exact migration suffix")
    forward_only = bool(added)
    if matrix.get("classification") != (
        "forward-only" if forward_only else "unchanged"
    ):
        fail(f"{label}: raw migration classification disagrees with ledger")
    candidate_before = matrix.get("candidate_status_before", {})
    if candidate_before.get("succeeded") is not (not forward_only):
        fail(f"{label}: candidate pre-apply status disagrees with ledger")
    if forward_only and "migration(s) pending" not in candidate_before.get("output", ""):
        fail(f"{label}: candidate did not name its pending migrations")
    previous_after = matrix.get("previous_status_after_candidate", {})
    fence = result.get("rollback", {}).get("migrated_layout_fence", {})
    rollback = result.get("rollback", {}).get("compatible_patch_rollback", {})
    if (
        fence.get("evaluated") is not True
        or fence.get("skipped_reason") is not None
        or not isinstance(fence.get("status"), dict)
        or fence.get("status") != previous_after
    ):
        fail(f"{label}: migration fence is not bound to the retained binary status")
    cold_start_output = fence.get("cold_start_output")
    cold_start_common = (
        fence.get("cold_start_attempted") is True
        and isinstance(cold_start_output, str)
        and bool(cold_start_output)
        and "://" not in cold_start_output
    )
    valid = (
        cold_start_common
        and previous_after.get("succeeded") is False
        and fence.get("expected_refused") is True
        and fence.get("cold_start_reached_readiness") is False
        and isinstance(fence.get("cold_start_exit_code"), int)
        and not isinstance(fence.get("cold_start_exit_code"), bool)
        and fence["cold_start_exit_code"] != 0
        and fence.get("refused") is True
        and fence.get("refusal_names_newer_build") is True
        and rollback.get("performed") is False
        if forward_only
        else cold_start_common
        and previous_after.get("succeeded") is True
        and fence.get("expected_refused") is False
        and fence.get("cold_start_reached_readiness") is True
        and fence.get("cold_start_exit_code") is None
        and fence.get("refused") is False
        and rollback.get("performed") is True
        and rollback.get("served_traffic") is True
    )
    if not valid:
        fail(f"{label}: raw rollback contradicts migration classification")

    replica_count = nonnegative_integer(scenario.get("replicas"), "scenario.replicas")
    expected_phases = [
        "steady-previous",
        "candidate-on-previous-config",
        "compatibility-drain",
    ]
    for index in range(replica_count):
        expected_phases.extend((f"mixed-{index}", f"drain-{index}"))
    expected_phases.append("steady-next")
    if not forward_only:
        expected_phases.extend(("rollback-drain", "rolled-back"))
    if [phase.get("phase") for phase in traffic] != expected_phases:
        fail(f"{label}: rollout did not execute the committed phase sequence")

    expected_fleet_revisions = Counter(
        {
            "previous": replica_count + int(not forward_only),
            "candidate-previous-config": 1,
            "next": replica_count,
        }
    )
    if Counter(member["revision"] for member in fleet) != expected_fleet_revisions:
        fail(f"{label}: rollout fleet does not contain the committed surge sequence")

    expected_drain_count = replica_count + 1 + int(not forward_only)
    if len(drains) != expected_drain_count:
        fail(f"{label}: rollout drain ledger does not cover every replacement")
    drained_revisions = Counter(drain.get("revision") for drain in drains)
    expected_drained_revisions = Counter(
        {
            "candidate-previous-config": 1,
            "previous": replica_count,
            "next": int(not forward_only),
        }
    )
    if drained_revisions != +expected_drained_revisions:
        fail(f"{label}: rollout drained the wrong revision set")

    mixed_phase = traffic_by_phase["mixed-0"]
    mixed_by_revision = mixed_phase.get("by_revision", {})
    if (
        mixed.get("previous_requests") != mixed_by_revision.get("previous", 0)
        or mixed.get("next_requests") != mixed_by_revision.get("next", 0)
    ):
        fail(f"{label}: mixed-version request counts do not match retained traffic")

    expected_usage_count = (
        sum(phase["answered"] for phase in traffic) + 2 * len(drains) + 2
    )
    if len(expected_rows) != expected_usage_count:
        fail(f"{label}: rollout exact caller ledger does not cover all accepted work")
    unavailable = nonnegative_integer(loss.get("unavailable"), "loss.unavailable")
    upstream_open = loss.get("upstream_streams_open_at_end")
    if not isinstance(upstream_open, int) or isinstance(upstream_open, bool):
        fail(f"{label}: rollout upstream stream count is malformed")

    if forward_only:
        if (
            rollback.get("performed") is not False
            or not isinstance(rollback.get("skipped_reason"), str)
            or not rollback["skipped_reason"]
            or rollback.get("replica") is not None
            or rollback.get("answered") != 0
            or rollback.get("errors") != 0
            or rollback.get("served_traffic") is not False
        ):
            fail(f"{label}: forward-only rollout fabricated a patch rollback")
    else:
        rollback_replica = rollback.get("replica")
        rolled_back = traffic_by_phase["rolled-back"]
        if (
            rollback.get("performed") is not True
            or rollback.get("skipped_reason") is not None
            or not isinstance(rollback_replica, str)
            or fleet_by_id.get(rollback_replica, {}).get("revision") != "previous"
            or rollback.get("answered")
            != rolled_back.get("by_replica", {}).get(rollback_replica, 0)
            or rollback.get("errors") != rolled_back.get("errors")
            or rollback.get("served_traffic")
            is not (rollback.get("answered", 0) > 0)
        ):
            fail(f"{label}: compatible patch rollback is not bound to served traffic")

    def max_optional(rows: list[dict[str, Any]], field: str) -> int:
        values = [row[field] for row in rows if row.get(field) is not None]
        return max(values, default=0)

    exact_phase_contract = (
        previous.get("config", {}).get("sha256")
        == compatibility.get("config", {}).get("sha256")
        and previous.get("binary", {}).get("sha256")
        != compatibility.get("binary", {}).get("sha256")
        and compatibility.get("binary", {}).get("sha256")
        == candidate.get("binary", {}).get("sha256")
        and compatibility.get("config", {}).get("sha256")
        == candidate.get("config", {}).get("sha256")
        and len(desired_state_revisions) == 1
    )
    rollback_mismatch = (
        rollback.get("performed") is True or fence.get("refused") is not True
        if fence.get("expected_refused") is True
        else rollback.get("performed") is not True
        or rollback.get("served_traffic") is not True
        or fence.get("refused") is True
    )
    fence_mismatch = (
        not (
            fence.get("cold_start_attempted") is True
            and fence.get("cold_start_reached_readiness") is False
            and isinstance(fence.get("cold_start_exit_code"), int)
            and not isinstance(fence.get("cold_start_exit_code"), bool)
            and fence["cold_start_exit_code"] != 0
            and fence.get("refused") is True
            and fence.get("refusal_names_newer_build") is True
        )
        if fence.get("expected_refused") is True
        else not (
            fence.get("cold_start_attempted") is True
            and fence.get("cold_start_reached_readiness") is True
            and fence.get("cold_start_exit_code") is None
            and fence.get("refused") is False
        )
    )
    expected_verdicts: dict[str, tuple[str, int | float, int | float]] = {
        "max_requests_to_drained_replica": (
            "<=",
            max(
                (
                    max(
                        drain["requests_after_withdrawal"],
                        drain["dispatches_beyond_drain_grace"],
                    )
                    for drain in drains
                ),
                default=0,
            ),
            thresholds["max_requests_to_drained_replica"],
        ),
        "max_request_loss": (
            "<=",
            traffic_totals["unanswered"]
            + traffic_totals["errors"]
            + traffic_totals["torn_streams"],
            thresholds["max_request_loss"],
        ),
        "max_unavailable_responses": (
            "<=",
            unavailable,
            thresholds["max_unavailable_responses"],
        ),
        "max_usage_record_loss": (
            "<=",
            missing,
            thresholds["max_usage_record_loss"],
        ),
        "unexplained_usage_record_surplus": ("<=", unexpected, 0),
        "duplicate_usage_trace_identities": ("<=", identity_duplicates, 0),
        "usage_status_mismatches": ("<=", status_mismatches, 0),
        "unidentified_usage_records": ("<=", unidentified, 0),
        "duplicate_usage_record_ids": ("<=", request_id_duplicates, 0),
        "otlp_trace_context_exported": (
            ">=",
            len(reconciliation["otlp_trace_export_replicas"]),
            len(expected_exact_replicas),
        ),
        "otlp_trace_export_identity_mismatches": ("<=", 0, 0),
        "readiness_removal_observed": (
            "<=",
            sum(drain.get("readiness_removed_after_ms") is None for drain in drains),
            0,
        ),
        "max_readiness_removal_ms": (
            "<=",
            max_optional(drains, "readiness_removed_after_ms"),
            thresholds["max_readiness_removal_ms"],
        ),
        "max_replacement_admission_ms": (
            "<=",
            max_optional(fleet, "admission_took_ms"),
            thresholds["max_replacement_admission_ms"],
        ),
        "bounded_termination": (
            "<=",
            sum(drain.get("exited_after_ms") is None for drain in drains),
            0,
        ),
        "max_drain_exit_slack_ms": (
            "<=",
            max(
                (
                    max(0, drain["exited_after_ms"] - drain["exit_budget_ms"])
                    for drain in drains
                    if drain.get("exited_after_ms") is not None
                ),
                default=0,
            ),
            thresholds["max_drain_exit_slack_ms"],
        ),
        "min_mixed_version_requests": (
            ">=",
            min(mixed["previous_requests"], mixed["next_requests"]),
            thresholds["min_mixed_version_requests"],
        ),
        "mixed_version_shared_stateful_serving": ("<=", 0, 0),
        "buffered_requests_completed_during_drain": (
            "<=",
            sum(
                not 200 <= drain["buffered_in_flight"]["status"] < 300
                for drain in drains
            ),
            0,
        ),
        "streams_cut_within_deadline": (
            "<=",
            sum(
                drain["stream_in_flight"].get("within_deadline") is not True
                or drain["stream_in_flight"].get("relayed_bytes") == 0
                for drain in drains
            ),
            0,
        ),
        "partial_streams_accounted": (
            "<=",
            sum(
                drain["stream_in_flight"].get("usage_status")
                != "client_cancelled"
                for drain in drains
            ),
            0,
        ),
        "upstream_streams_open_at_end": ("<=", upstream_open, 0),
        "migration_gate_passed": ("<=", 0, 0),
        "rollback_matches_migration_classification": (
            "<=",
            int(rollback_mismatch),
            0,
        ),
        "migration_fence_matches_classification": (
            "<=",
            int(fence_mismatch),
            0,
        ),
        "heavy_rollout_is_promotable": ("<=", int(run.get("promotable") is not True), 0),
        "heavy_rollout_uses_two_binary_digests": ("<=", int(len(identities) != 2), 0),
        "candidate_serves_shared_stateful_revision": (
            "<=",
            int(not exact_phase_contract),
            0,
        ),
        "migration_matrix_evaluated": (
            "<=",
            int(matrix.get("evaluated") is not True),
            0,
        ),
    }
    verdicts_by_threshold: dict[str, dict[str, Any]] = {}
    for index, verdict in enumerate(verdicts):
        threshold = verdict.get("threshold")
        if not isinstance(threshold, str) or threshold in verdicts_by_threshold:
            fail(f"{label}: rollout verdict thresholds are malformed or duplicated")
        verdicts_by_threshold[threshold] = verdict
    if set(verdicts_by_threshold) != set(expected_verdicts):
        missing_verdicts = sorted(set(expected_verdicts) - set(verdicts_by_threshold))
        unexpected_verdicts = sorted(set(verdicts_by_threshold) - set(expected_verdicts))
        fail(
            f"{label}: rollout verdict contract is incomplete "
            f"(missing {missing_verdicts}, unexpected {unexpected_verdicts})"
        )
    for threshold, (comparison, value, bound) in expected_verdicts.items():
        verdict = verdicts_by_threshold[threshold]
        passed = value <= bound if comparison == "<=" else value >= bound
        if (
            verdict.get("comparison") != comparison
            or verdict.get("value") != value
            or verdict.get("bound") != bound
            or verdict.get("passed") is not passed
            or passed is not True
        ):
            fail(f"{label}: rollout verdict {threshold!r} is not independently valid")


def verify_promotion_artifacts(record: dict[str, Any], directory: Path | None) -> None:
    """Require the raw directory whenever the compact record claims identities."""
    if artifact_claims(record) and directory is None:
        fail("the record claims raw artifact digests but --artifacts was not supplied")
    if directory is not None:
        verify_raw_artifacts(record, directory)


def validate_raw_stateful_endurance(
    result: dict[str, Any], label: str, record: dict[str, Any], row: dict[str, Any]
) -> None:
    if result.get("schema_version") != STATEFUL_ENDURANCE_RESULT_SCHEMA_VERSION:
        fail(f"{label}: unsupported stateful endurance artifact schema")
    if row.get("artifact_schema_version") != STATEFUL_ENDURANCE_RESULT_SCHEMA_VERSION:
        fail(
            f"{label}: compact stateful endurance row does not bind result schema "
            f"{STATEFUL_ENDURANCE_RESULT_SCHEMA_VERSION}"
        )
    verdicts = result.get("verdicts")
    if (
        not isinstance(verdicts, list)
        or not verdicts
        or len(verdicts) != row.get("verdicts")
        or any(
            not isinstance(verdict, dict) or verdict.get("passed") is not True
            for verdict in verdicts
        )
    ):
        fail(f"{label}: raw stateful endurance verdicts are missing or failed")
    profile = result.get("profile", {})
    run = result.get("run", {})
    if not isinstance(profile, dict) or not isinstance(run, dict):
        fail(f"{label}: raw stateful endurance profile or run metadata is malformed")
    if profile.get("tier") != record.get("tier"):
        fail(f"{label}: raw stateful endurance tier does not match compact record")
    if run.get("elapsed_ms") != row.get("elapsed_ms"):
        fail(f"{label}: raw stateful endurance elapsed time does not match compact row")
    if run.get("duration_source") != row.get("duration_source"):
        fail(f"{label}: raw stateful duration provenance does not match compact row")
    for field in ("duration_ms", "manifest_duration_ms"):
        if profile.get(field) != row.get(field):
            fail(f"{label}: raw stateful endurance {field} does not match compact row")

    manifest_relative = record.get("inputs", {}).get("manifest")
    if not isinstance(manifest_relative, str) or not manifest_relative:
        fail(f"{label}: stateful endurance manifest path is missing")
    manifest_path = ROOT / manifest_relative
    try:
        manifest = tomllib.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        fail(f"{label}: stateful endurance manifest is unreadable: {error}")
    manifest_profiles = [
        candidate
        for candidate in manifest.get("profile", [])
        if isinstance(candidate, dict) and candidate.get("id") == profile.get("id")
    ]
    if len(manifest_profiles) != 1:
        fail(f"{label}: raw stateful profile does not name one manifest profile")
    manifest_profile = manifest_profiles[0]
    tier = profile.get("tier")
    scale = manifest_profile.get(tier)
    schedule = manifest_profile.get("schedule")
    termination = manifest_profile.get("termination")
    manifest_slo = manifest_profile.get("slo")
    if not all(
        isinstance(value, dict)
        for value in (scale, schedule, termination, manifest_slo)
    ):
        fail(f"{label}: stateful manifest profile contract is malformed")
    expected_slo = copy.deepcopy(manifest_slo)
    expected_slo.setdefault("max_rss_drift_kib_per_hour", None)
    overrides = scale.get("slo_overrides", {})
    if not isinstance(overrides, dict):
        fail(f"{label}: stateful manifest SLO overrides are malformed")
    for field, value in overrides.items():
        if value is not None:
            expected_slo[field] = value
    profile_echoes = {
        "description": manifest_profile.get("description"),
        "seed": manifest_profile.get("seed"),
        "manifest_duration_ms": scale.get("duration_ms"),
        "concurrency": scale.get("concurrency"),
        "think_time_ms": scale.get("think_time_ms"),
        "sample_interval_ms": scale.get("sample_interval_ms"),
        "segment_ms": scale.get("segment_ms"),
        "mix": manifest_profile.get("mix"),
        "schedule": schedule,
        "slo": expected_slo,
        "termination": termination,
    }
    for field, expected_value in profile_echoes.items():
        if profile.get(field) != expected_value:
            fail(
                f"{label}: raw stateful profile.{field} does not match the "
                "qualification manifest"
            )
    if run.get("settle_ms") != termination.get("settle_ms"):
        fail(f"{label}: raw settlement interval does not match the manifest")

    usage = result.get("usage", {})
    if not isinstance(usage, dict):
        fail(f"{label}: raw stateful usage evidence is malformed")
    raw_path = Path(label)
    ledger_directories: dict[str, Path] = {}
    for field, files_per_shard in STATEFUL_LEDGER_FIELDS:
        evidence = usage.get(field, {})
        if evidence.get("exact") is not True or not evidence.get("path"):
            fail(f"{label}: raw stateful endurance {field} is not exact evidence")
        shards = evidence.get("shards")
        if shards != STATEFUL_LEDGER_SHARDS:
            fail(
                f"{label}: raw stateful endurance {field} has {shards!r} shards, "
                f"expected schema-3 count {STATEFUL_LEDGER_SHARDS}"
            )
        directory = resolve_stateful_ledger(
            raw_path, evidence.get("path"), f"{label}: {field}"
        )
        ledger_directories[field] = directory
        actual_claim = stateful_ledger_claim(
            directory,
            f"{label}: {field}",
            field,
            evidence,
            schema_label="stateful-endurance schema 3",
            digest_domain=b"axond-stateful-ledger-v2\0",
        )
        expected_claim = {
            "sha256": sha256_digest(
                row.get(f"{field}_sha256"), f"{label}: compact {field} digest"
            ),
            "files": row.get(f"{field}_files"),
            "bytes": row.get(f"{field}_bytes"),
        }
        if (
            not isinstance(expected_claim["files"], int)
            or isinstance(expected_claim["files"], bool)
            or expected_claim["files"] != shards * files_per_shard
            or not isinstance(expected_claim["bytes"], int)
            or isinstance(expected_claim["bytes"], bool)
            or expected_claim["bytes"] <= 0
            or actual_claim != expected_claim
        ):
            fail(f"{label}: retained {field} shards do not match the compact claim")

    seed = profile.get("seed")
    if not isinstance(seed, int) or isinstance(seed, bool) or seed < 0 or seed >= 2**64:
        fail(f"{label}: stateful profile seed is missing or malformed")
    request_tally = stateful_request_tally(
        ledger_directories["request_identities"], f"{label}: request identities"
    )
    correlation_tally = stateful_correlation_tally(
        ledger_directories["correlations"],
        f"{label}: correlations",
        seed,
        allow_concurrent_endings=True,
    )
    correlation_window_tally = stateful_correlation_window_tally(
        ledger_directories["correlation_windows"],
        ledger_directories["correlations"],
        f"{label}: correlation windows",
        seed,
        profile.get("duration_ms"),
        result.get("faults"),
        schedule,
    )
    durable_tally = stateful_identity_pair_tally(
        ledger_directories["durable_identities"], f"{label}: durable identities"
    )
    outside_tally = stateful_identity_pair_tally(
        ledger_directories["durable_outside_identities"],
        f"{label}: outside-window durable identities",
    )

    def require_evidence(field: str, actual: dict[str, Any], keys: tuple[str, ...]) -> None:
        evidence = usage.get(field, {})
        for key in keys:
            if evidence.get(key) != actual[key]:
                fail(
                    f"{label}: raw {field}.{key}={evidence.get(key)!r} does not "
                    f"match retained shards ({actual[key]!r})"
                )

    require_evidence(
        "request_identities", request_tally, ("recorded", "peak_shard_rows")
    )
    require_evidence(
        "correlations",
        correlation_tally,
        ("expected", "observed", "peak_shard_rows"),
    )
    require_evidence(
        "correlation_windows",
        correlation_window_tally,
        ("recorded", "peak_shard_rows"),
    )
    identity_fields = (
        "expected_rows",
        "observed_rows",
        "expected_distinct",
        "observed_distinct",
        "expected_duplicates",
        "observed_duplicates",
        "missing",
        "unexpected",
        "peak_shard_rows",
    )
    require_evidence("durable_identities", durable_tally, identity_fields)
    require_evidence("durable_outside_identities", outside_tally, identity_fields)

    if not stateful_request_ids_are_subset(
        ledger_directories["request_identities"],
        ledger_directories["durable_identities"],
        f"{label}: workload/durable identity relation",
    ):
        fail(f"{label}: workload request identities are not a subset of emitted identities")
    if not stateful_outside_loss_relation(
        ledger_directories["durable_identities"],
        ledger_directories["durable_outside_identities"],
        f"{label}: outside-loss identity relation",
    ):
        fail(
            f"{label}: outside-loss evidence is not an emitted-outside subset "
            "paired with the complete durable population"
        )

    durable = usage.get("durable", {})
    if not isinstance(durable, dict):
        fail(f"{label}: raw durable SQL counts are missing or malformed")
    for field in ("unidentified", "uncorrelated"):
        value = usage.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            fail(f"{label}: raw usage.{field} is missing or malformed")
    outside_missing = outside_tally["missing"]
    if outside_missing > durable_tally["missing"]:
        fail(f"{label}: outside durable loss exceeds whole-run durable loss")
    exact_summary = {
        "owed": correlation_tally["workload_expected"],
        "emitted": durable_tally["expected_rows"] + usage["unidentified"],
        "distinct": request_tally["distinct"],
        "probe_distinct": durable_tally["expected_distinct"]
        - request_tally["distinct"],
        "duplicates": durable_tally["expected_duplicates"],
        "missing": correlation_tally["missing"],
        "unexpected_records": correlation_tally["unexpected"]
        + usage["uncorrelated"],
        "concurrent_endings": correlation_tally["concurrent_endings"],
        "concurrent_ending_membership_mismatches": correlation_window_tally[
            "membership_mismatches"
        ]
        + abs(
            correlation_tally["concurrent_endings"]
            - correlation_window_tally["concurrent_endings"]
        ),
        "refusal_records": correlation_tally["by_status"]["rejected"],
        "durable_loss_total": durable_tally["missing"],
        "durable_loss_outside_windows": outside_missing,
        "durable_loss_in_window": durable_tally["missing"] - outside_missing,
        "settled_outside_usage_window": outside_tally["expected_distinct"],
        "durable_duplicate_rows": durable_tally["observed_duplicates"],
        "durable_unexpected_rows": durable_tally["unexpected"],
    }
    for field, expected_value in exact_summary.items():
        if usage.get(field) != expected_value:
            fail(
                f"{label}: raw usage.{field}={usage.get(field)!r} does not match "
                f"retained shards ({expected_value!r})"
            )
    if usage.get("emitted") != correlation_tally["observed"] + usage["uncorrelated"]:
        fail(f"{label}: emitted row count does not match retained correlations")
    if durable.get("rows") != durable_tally["observed_rows"] or durable.get(
        "distinct"
    ) != durable_tally["observed_distinct"]:
        fail(f"{label}: durable SQL counts do not match retained observed identities")
    durable_outside = usage.get("durable_outside_usage_window")
    if (
        not isinstance(durable_outside, int)
        or isinstance(durable_outside, bool)
        or durable_outside < 0
        or durable_outside > durable_tally["observed_distinct"]
    ):
        fail(f"{label}: durable outside-window SQL count is malformed")
    if usage.get("by_status") != dict(correlation_tally["by_status"]):
        fail(f"{label}: usage status summary does not match retained correlations")
    if correlation_tally["status_mismatches"] != usage.get("unexpected_statuses"):
        fail(f"{label}: usage status mismatch count does not match retained correlations")

    def table(field: str) -> dict[str, Any]:
        value = result.get(field)
        if not isinstance(value, dict):
            fail(f"{label}: raw stateful {field} evidence is missing or malformed")
        return value

    def tables(field: str) -> list[dict[str, Any]]:
        value = result.get(field)
        if not isinstance(value, list) or any(not isinstance(item, dict) for item in value):
            fail(f"{label}: raw stateful {field} evidence is missing or malformed")
        return value

    def nonnegative_integer(value: object, field: str) -> int:
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            fail(f"{label}: raw stateful {field} is missing or malformed")
        return value

    workload = table("workload")
    telemetry = table("telemetry")
    restart = table("restart")
    tenancy = table("tenancy")
    trend = table("trend")
    backends = table("backends")
    segments = tables("segments")
    resources = tables("resources")
    revisions = tables("revisions")
    faults = tables("faults")

    sample_paths = resolve_resource_samples(
        raw_path, run.get("samples_paths"), f"{label}: resource samples"
    )
    verify_resource_sample_claim(sample_paths, row, f"{label}: resource samples")
    if len(sample_paths) != len(resources):
        fail(f"{label}: resource sample series do not match replica incarnations")
    validate_stateful_resource_samples(sample_paths, resources, label)

    expected_revision_events = {
        "catalogue-revision",
        "credential-revision",
        "policy-revision",
    }
    revision_events = [revision.get("event") for revision in revisions]
    if (
        set(revision_events) != expected_revision_events
        or len(revision_events) != len(expected_revision_events)
    ):
        fail(f"{label}: required stateful convergence observations are incomplete")
    convergence_values: list[int] = []
    for revision in revisions:
        convergence_values.append(
            nonnegative_integer(
                revision.get("converged_ms"),
                f"revision {revision.get('event')!r} convergence",
            )
        )

    if backends.get("usage_reach") != "gated":
        fail(f"{label}: stateful qualification did not gate the usage backend")
    expected_fault_events = {
        "upstream-latency-begins",
        "upstream-outage-begins",
        "usage-backend-outage-begins",
    }
    fault_events = [fault.get("event") for fault in faults]
    if set(fault_events) != expected_fault_events or len(fault_events) != len(
        expected_fault_events
    ):
        fail(f"{label}: required stateful recovery observations are incomplete")
    recovery_values: list[int] = []
    for fault in faults:
        recovery_values.append(
            nonnegative_integer(
                fault.get("recovered_ms"),
                f"fault {fault.get('event')!r} recovery",
            )
        )

    replicas = nonnegative_integer(expected_slo.get("replicas"), "manifest replicas")
    replicas_booted = nonnegative_integer(run.get("replicas_booted"), "replicas_booted")
    if replicas_booted != replicas or len(resources) != replicas:
        fail(f"{label}: stateful resource evidence does not cover the fleet")
    sample_paths = run.get("samples_paths")
    if (
        not isinstance(sample_paths, list)
        or len(sample_paths) != replicas
        or len(set(sample_paths)) != replicas
        or any(not isinstance(path, str) or not path for path in sample_paths)
    ):
        fail(f"{label}: stateful resource sample paths do not cover the fleet")
    resource_growth: list[int] = []
    for index, resource in enumerate(resources):
        if resource.get("sampled") is not True:
            fail(f"{label}: resource observation {index} was not sampled")
        if nonnegative_integer(resource.get("samples"), f"resources[{index}].samples") == 0:
            fail(f"{label}: resource observation {index} has no samples")
        growth = resource.get("growth_kib")
        if growth is not None and (
            not isinstance(growth, int) or isinstance(growth, bool)
        ):
            fail(f"{label}: resource observation {index} growth is malformed")
        if growth is not None:
            resource_growth.append(growth)
    rss_growth = max(0, max(resource_growth, default=0))
    if trend.get("segments") != len(segments):
        fail(f"{label}: raw resource trend does not cover the retained segments")

    if nonnegative_integer(tenancy.get("probes"), "tenancy.probes") == 0:
        fail(f"{label}: stateful tenancy boundary was not probed")
    if nonnegative_integer(
        tenancy.get("probe_served_before_policy"),
        "tenancy.probe_served_before_policy",
    ) == 0 or nonnegative_integer(
        tenancy.get("probe_refused_after_policy"),
        "tenancy.probe_refused_after_policy",
    ) == 0:
        fail(f"{label}: stateful tenancy policy transition was not observed")
    for field in ("probe_served_after_policy", "misattributed_records"):
        if nonnegative_integer(tenancy.get(field), f"tenancy.{field}") != 0:
            fail(f"{label}: stateful tenancy evidence has nonzero {field}")
    if nonnegative_integer(
        restart.get("offered_after_last_replacement"),
        "restart.offered_after_last_replacement",
    ) == 0:
        fail(f"{label}: rolling restart was not followed by offered load")

    offered = nonnegative_integer(workload.get("offered"), "workload.offered")
    if offered == 0:
        fail(f"{label}: stateful workload offered no traffic")
    for field in ("streamed", "buffered"):
        nonnegative_integer(workload.get(field), f"workload.{field}")
    if workload["streamed"] == 0 or workload["buffered"] == 0 or (
        workload["streamed"] + workload["buffered"] != offered
    ):
        fail(f"{label}: stateful workload did not exercise both response modes")
    for field in ("by_tenant", "by_ending"):
        counts = workload.get(field)
        if (
            not isinstance(counts, dict)
            or not counts
            or any(
                not isinstance(key, str)
                or not key
                or not isinstance(count, int)
                or isinstance(count, bool)
                or count <= 0
                for key, count in counts.items()
            )
            or sum(counts.values()) != offered
        ):
            fail(f"{label}: stateful workload {field} does not reconcile with offered load")
    if set(workload["by_ending"]) != set(profile.get("mix", {})):
        fail(f"{label}: stateful workload did not exercise the committed ending mix")
    if len(workload["by_tenant"]) < 3:
        fail(f"{label}: stateful workload did not exercise the committed tenant mix")
    if exact_summary["concurrent_ending_membership_mismatches"] != 0:
        fail(
            f"{label}: retained concurrent endings do not exactly match request "
            "lifetimes in the committed upstream correlation window"
        )

    segment_offered = 0
    previous_end = 0
    for index, segment in enumerate(segments):
        if segment.get("index") != index:
            fail(f"{label}: stateful segment indexes are not contiguous")
        started = nonnegative_integer(segment.get("started_ms"), f"segments[{index}].started_ms")
        ended = nonnegative_integer(segment.get("ended_ms"), f"segments[{index}].ended_ms")
        if ended <= started or started < previous_end:
            fail(f"{label}: stateful segment timeline is overlapping or empty")
        previous_end = ended
        segment_offered += nonnegative_integer(
            segment.get("offered"), f"segments[{index}].offered"
        )
        nonnegative_integer(segment.get("unplanned"), f"segments[{index}].unplanned")
        nonnegative_integer(
            segment.get("usage_records"), f"segments[{index}].usage_records"
        )
    if segment_offered != offered:
        fail(f"{label}: stateful segment load does not reconcile with the workload")

    sink_drops = usage.get("sink_drops")
    if not isinstance(sink_drops, dict):
        fail(f"{label}: raw sink-drop evidence is missing or malformed")
    sink_drop_counts = {
        field: nonnegative_integer(sink_drops.get(field), f"sink_drops.{field}")
        for field in (
            "records",
            "records_in_usage_window",
            "sampled_records_in_usage_window",
            "records_outside_windows",
        )
    }
    reasons = sink_drops.get("by_reason")
    if not isinstance(reasons, dict) or any(
        not isinstance(reason, str)
        or not isinstance(count, int)
        or isinstance(count, bool)
        or count < 0
        for reason, count in reasons.items()
    ):
        fail(f"{label}: raw sink-drop reason tally is malformed")
    if sink_drop_counts["records"] != sum(reasons.values()) or sink_drop_counts[
        "records"
    ] != (
        sink_drop_counts["records_in_usage_window"]
        + sink_drop_counts["records_outside_windows"]
    ):
        fail(f"{label}: raw sink-drop totals do not reconcile")
    if sink_drop_counts["sampled_records_in_usage_window"] > sink_drop_counts[
        "records_in_usage_window"
    ]:
        fail(f"{label}: sampled sink-drop count exceeds in-window drops")
    excused_in_window = sink_drop_counts["records_in_usage_window"] + (
        STATEFUL_DROP_LOG_SAMPLE - 1
        if sink_drop_counts["sampled_records_in_usage_window"] > 0
        else 0
    )

    durable_counts_mismatch = int(
        durable_tally["observed_rows"] != durable.get("rows")
        or durable_tally["observed_distinct"] != durable.get("distinct")
        or outside_tally["observed_rows"] != durable_tally["observed_rows"]
        or outside_tally["observed_distinct"] != durable_tally["observed_distinct"]
        or outside_tally["observed_duplicates"]
        != durable_tally["observed_duplicates"]
    )
    slo_bound_fields = (
        "min_segments",
        "max_unplanned_errors",
        "max_missing_usage_records",
        "max_duplicate_usage_records",
        "max_durable_usage_loss_outside_windows",
        "max_durable_usage_lag_ms",
        "max_tenant_boundary_violations",
        "max_restart_unavailable",
        "max_recovery_ms",
        "max_readiness_gap_ms",
        "max_rss_growth_kib",
        "max_convergence_ms",
    )
    for field in slo_bound_fields:
        nonnegative_integer(expected_slo.get(field), f"manifest SLO {field}")

    expected_verdicts: dict[str, tuple[str, int | float, int | float]] = {
        "segments": (">=", len(segments), expected_slo["min_segments"]),
        "unplanned_errors": (
            "<=",
            nonnegative_integer(workload.get("unplanned"), "workload.unplanned"),
            expected_slo["max_unplanned_errors"],
        ),
        "missing_usage_records": (
            "<=",
            exact_summary["missing"],
            expected_slo["max_missing_usage_records"],
        ),
        "duplicate_usage_records": (
            "<=",
            exact_summary["duplicates"],
            expected_slo["max_duplicate_usage_records"],
        ),
        "unexpected_usage_records": ("<=", exact_summary["unexpected_records"], 0),
        "unexpected_usage_statuses": ("<=", usage.get("unexpected_statuses"), 0),
        "concurrent_ending_membership_mismatches": (
            "<=",
            exact_summary["concurrent_ending_membership_mismatches"],
            0,
        ),
        "unidentified_usage_records": ("<=", usage.get("unidentified"), 0),
        "uncorrelated_usage_records": ("<=", usage.get("uncorrelated"), 0),
        "refusal_usage_records": ("<=", exact_summary["refusal_records"], 0),
        "durable_usage_unexpected_records": (
            "<=",
            exact_summary["durable_unexpected_rows"],
            0,
        ),
        "durable_identity_counts_match_sql": ("<=", durable_counts_mismatch, 0),
        "durable_usage_loss_outside_windows": (
            "<=",
            exact_summary["durable_loss_outside_windows"],
            expected_slo["max_durable_usage_loss_outside_windows"],
        ),
        "durable_usage_loss_in_window": (
            "<=",
            exact_summary["durable_loss_in_window"],
            excused_in_window,
        ),
        "durable_usage_lag_ms": (
            "<=",
            nonnegative_integer(usage.get("durable_lag_ms"), "usage.durable_lag_ms"),
            expected_slo["max_durable_usage_lag_ms"],
        ),
        "tenant_boundary_violations": (
            "<=",
            nonnegative_integer(tenancy.get("violations"), "tenancy.violations"),
            expected_slo["max_tenant_boundary_violations"],
        ),
        "restart_unavailable_requests": (
            "<=",
            nonnegative_integer(restart.get("unavailable"), "restart.unavailable"),
            expected_slo["max_restart_unavailable"],
        ),
        "recovery_ms": ("<=", max(recovery_values), expected_slo["max_recovery_ms"]),
        "readiness_gap_ms": (
            "<=",
            nonnegative_integer(
                telemetry.get("worst_readiness_gap_ms"),
                "telemetry.worst_readiness_gap_ms",
            ),
            expected_slo["max_readiness_gap_ms"],
        ),
        "rss_growth_kib": ("<=", rss_growth, expected_slo["max_rss_growth_kib"]),
        "unconverged_revisions": ("<=", 0, 0),
        "convergence_ms": (
            "<=",
            max(convergence_values),
            expected_slo["max_convergence_ms"],
        ),
        "replicas_restarted": (
            ">=",
            nonnegative_integer(
                restart.get("replicas_restarted"), "restart.replicas_restarted"
            ),
            replicas,
        ),
        "retiring_replicas_exited_cleanly": (
            ">=",
            int(restart.get("all_exits_clean") is True),
            1,
        ),
        "retiring_replicas_exited_in_bound": (
            ">=",
            int(restart.get("all_exits_bounded") is True),
            1,
        ),
        "terminated_normally": (
            ">=",
            int(run.get("stop") == "duration_elapsed"),
            1,
        ),
    }
    drift_bound = expected_slo.get("max_rss_drift_kib_per_hour")
    if drift_bound is not None:
        if not isinstance(drift_bound, (int, float)) or isinstance(drift_bound, bool):
            fail(f"{label}: manifest RSS drift SLO is malformed")
        drift_value = trend.get("rss_kib_per_hour")
        if trend.get("evaluated") is not True or not isinstance(
            drift_value, (int, float)
        ) or isinstance(drift_value, bool):
            fail(f"{label}: required RSS drift evidence was not evaluated")
        expected_verdicts["rss_drift_kib_per_hour"] = (
            "<=",
            drift_value,
            drift_bound,
        )
    elif trend.get("evaluated") is not False:
        fail(f"{label}: raw RSS drift evaluation disagrees with the manifest")

    lag_value = expected_verdicts["durable_usage_lag_ms"][1]
    lag_bound = expected_verdicts["durable_usage_lag_ms"][2]
    if usage.get("durable_settled") is not (lag_value <= lag_bound):
        fail(f"{label}: raw durable settlement flag disagrees with its SLO")
    if run.get("stop") == "duration_elapsed" and run.get("stop_detail") is not None:
        fail(f"{label}: normally terminated run carries a stop failure detail")

    verdicts_by_threshold: dict[str, dict[str, Any]] = {}
    for index, verdict in enumerate(verdicts):
        if not isinstance(verdict, dict):
            fail(f"{label}: verdict {index} is malformed")
        threshold = verdict.get("threshold")
        if not isinstance(threshold, str) or threshold in verdicts_by_threshold:
            fail(f"{label}: verdict thresholds are malformed or duplicated")
        verdicts_by_threshold[threshold] = verdict
    if set(verdicts_by_threshold) != set(expected_verdicts):
        missing = sorted(set(expected_verdicts) - set(verdicts_by_threshold))
        unexpected = sorted(set(verdicts_by_threshold) - set(expected_verdicts))
        fail(
            f"{label}: stateful verdict contract is incomplete "
            f"(missing {missing}, unexpected {unexpected})"
        )
    for threshold, (comparison, expected_value, expected_bound) in expected_verdicts.items():
        verdict = verdicts_by_threshold[threshold]
        value = verdict.get("value")
        bound = verdict.get("bound")
        if (
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not isinstance(bound, (int, float))
            or isinstance(bound, bool)
            or verdict.get("comparison") != comparison
            or value != expected_value
            or bound != expected_bound
        ):
            fail(
                f"{label}: verdict {threshold!r} comparison, value, or bound "
                "does not match the independently reconstructed contract"
            )
        passed = value <= bound if comparison == "<=" else value >= bound
        if verdict.get("passed") is not passed:
            fail(f"{label}: verdict {threshold!r} has an inconsistent pass result")
    for field in (
        "missing",
        "unexpected_records",
        "unexpected_statuses",
        "unidentified",
        "uncorrelated",
        "refusal_records",
        "durable_loss_outside_windows",
        "durable_unexpected_rows",
    ):
        if usage.get(field) != 0:
            fail(f"{label}: raw stateful endurance has nonzero {field}")
    environment = result.get("environment", {})
    expected_fields = {
        "binary.sha256": (
            environment.get("binary", {}).get("sha256"),
            record.get("binary", {}).get("sha256"),
        ),
        "binary.version": (
            environment.get("binary", {}).get("version"),
            record.get("binary", {}).get("version"),
        ),
        "source.git_commit": (
            environment.get("source", {}).get("git_commit"),
            record.get("source", {}).get("git_commit"),
        ),
        "source.git_dirty": (
            environment.get("source", {}).get("git_dirty"),
            record.get("source", {}).get("git_dirty"),
        ),
        "manifest.path": (
            environment.get("manifest", {}).get("path"),
            record.get("inputs", {}).get("manifest"),
        ),
        "manifest.sha256": (
            environment.get("manifest", {}).get("sha256"),
            record.get("inputs", {}).get("manifest_sha256"),
        ),
    }
    for field, (actual, expected_value) in expected_fields.items():
        if actual != expected_value:
            fail(f"{label}: raw stateful endurance {field} does not match compact record")


def validate_raw_capacity(
    result: dict[str, Any], label: str, record: dict[str, Any], row: dict[str, Any]
) -> None:
    """Bind a schema-2 compact capacity row to its complete raw result."""
    if result.get("schema_version") != CAPACITY_RESULT_SCHEMA_VERSION:
        fail(
            f"{label}: unsupported capacity artifact schema "
            f"{result.get('schema_version')!r}"
        )
    if row.get("artifact_schema_version") != CAPACITY_RESULT_SCHEMA_VERSION:
        fail(
            f"{label}: compact capacity row does not bind schema "
            f"{CAPACITY_RESULT_SCHEMA_VERSION}"
        )
    if record.get("binary", {}).get("cargo_profile") != "release":
        fail(f"{label}: promotable capacity evidence must use the release profile")
    verdicts = result.get("verdicts")
    if not isinstance(verdicts, list) or not verdicts or any(
        verdict.get("passed") is not True for verdict in verdicts
    ):
        fail(f"{label}: raw capacity artifact has a failed or missing verdict")
    if len(verdicts) != row.get("verdicts"):
        fail(f"{label}: raw verdict count does not match the compact profile")

    profile = result.get("profile", {})
    run = result.get("run", {})
    environment = result.get("environment", {})
    if environment.get("toolchain", {}).get("cargo_profile") != "release":
        fail(f"{label}: raw capacity evidence was not produced by a release binary")
    manifest_path = ROOT / record.get("inputs", {}).get("manifest", "")
    manifest = tomllib.loads(manifest_path.read_text(encoding="utf-8"))
    committed_profiles = [
        candidate
        for candidate in manifest.get("profile", [])
        if candidate.get("id") == profile.get("id")
    ]
    if len(committed_profiles) != 1:
        fail(f"{label}: raw capacity profile is not unique in the manifest")
    committed = committed_profiles[0]
    tier = record.get("tier")
    scale = committed.get(tier, {})
    expected_profile = {
        "id": committed.get("id"),
        "workload": committed.get("workload"),
        "description": committed.get("description"),
        "tier": tier,
        "concurrency": scale.get("concurrency"),
        "requests": scale.get("requests"),
        "max_in_flight": committed.get("max_in_flight"),
        "queue_capacity": committed.get("queue_capacity"),
        "queue_wait_ms": committed.get("queue_wait_ms"),
    }
    for field, expected in expected_profile.items():
        if profile.get(field) != expected:
            fail(
                f"{label}: raw profile.{field} does not match the committed manifest"
            )
    raw_thresholds = {
        field: value
        for field, value in profile.get("thresholds", {}).items()
        if value is not None
    }
    committed_thresholds = committed.get("thresholds", {})
    if raw_thresholds != committed_thresholds:
        fail(f"{label}: raw capacity thresholds do not match the committed manifest")

    retained_fixtures = environment.get("fixtures")
    if not isinstance(retained_fixtures, list):
        fail(f"{label}: raw capacity fixture provenance is missing")
    expected_fixtures = []
    fixtures_root = ROOT / "tests/fixtures"
    for fixture in sorted(path for path in fixtures_root.rglob("*") if path.is_file()):
        expected_fixtures.append(
            {
                "path": fixture.relative_to(ROOT).as_posix(),
                "sha256": hashlib.sha256(fixture.read_bytes()).hexdigest(),
            }
        )
    if retained_fixtures != expected_fixtures:
        fail(f"{label}: raw capacity fixture provenance is stale or incomplete")

    def rounded(value: Any, digits: int) -> float | None:
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            return None
        return float(f"{value:.{digits}f}")

    throughput = result.get("throughput", {})
    latency = result.get("latency_ms", {})
    ttft = result.get("ttft_ms")
    resources = result.get("resources", {})
    rss = resources.get("rss_kib") or {}
    sockets = resources.get("sockets") or {}
    occupancy = result.get("occupancy", {})
    tenancy = result.get("tenancy")
    deadlines = result.get("deadlines")
    recovery = result.get("recovery")
    expected_fields = {
        "profile.id": (profile.get("id"), row.get("id")),
        "profile.tier": (profile.get("tier"), record.get("tier")),
        "profile.concurrency": (profile.get("concurrency"), row.get("concurrency")),
        "profile.requests": (profile.get("requests"), row.get("requests")),
        "run.elapsed_ms": (run.get("elapsed_ms"), row.get("elapsed_ms")),
        "throughput.offered": (
            result.get("throughput", {}).get("offered"),
            row.get("offered"),
        ),
        "throughput.accepted": (
            result.get("throughput", {}).get("accepted"),
            row.get("accepted"),
        ),
        "throughput.rejected": (
            result.get("throughput", {}).get("rejected"),
            row.get("rejected"),
        ),
        "throughput.errors": (
            result.get("throughput", {}).get("errors"),
            row.get("errors"),
        ),
        "throughput.accepted_rps": (
            rounded(throughput.get("accepted_rps"), 1),
            row.get("accepted_rps"),
        ),
        "latency_ms.p50": (rounded(latency.get("p50"), 2), row.get("latency_p50_ms")),
        "latency_ms.p95": (rounded(latency.get("p95"), 2), row.get("latency_p95_ms")),
        "latency_ms.p99": (rounded(latency.get("p99"), 2), row.get("latency_p99_ms")),
        "ttft_ms.p95": (
            rounded(ttft.get("p95"), 2) if isinstance(ttft, dict) else None,
            row.get("ttft_p95_ms"),
        ),
        "occupancy.admission_max_in_flight": (
            occupancy.get("admission_max_in_flight"),
            row.get("admission_max_in_flight"),
        ),
        "tenancy.namespaces": (
            len(tenancy.get("by_namespace", {})) if isinstance(tenancy, dict) else None,
            row.get("tenants"),
        ),
        "tenancy.foreign_credential_uses": (
            tenancy.get("foreign_credential_uses") if isinstance(tenancy, dict) else None,
            row.get("foreign_credential_uses"),
        ),
        "tenancy.misattributed_usage_records": (
            tenancy.get("misattributed_usage_records") if isinstance(tenancy, dict) else None,
            row.get("misattributed_usage_records"),
        ),
        "deadlines.bound_ms": (
            deadlines.get("bound_ms") if isinstance(deadlines, dict) else None,
            row.get("upstream_bound_ms"),
        ),
        "deadlines.over_bound": (
            deadlines.get("over_bound") if isinstance(deadlines, dict) else None,
            row.get("over_bound"),
        ),
        "deadlines.max_latency_ms": (
            rounded(deadlines.get("max_latency_ms"), 2)
            if isinstance(deadlines, dict)
            else None,
            row.get("max_latency_ms"),
        ),
        "recovery.served": (
            recovery.get("served") if isinstance(recovery, dict) else None,
            row.get("served_after_load"),
        ),
        "resources.rss_kib.peak": (rss.get("peak"), row.get("peak_rss_kib")),
        "resources.rss_kib.growth": (
            max(0, max(rss.get("peak", 0), rss.get("settled", 0)) - rss.get("baseline", 0))
            if rss
            else None,
            row.get("rss_growth_kib"),
        ),
        "resources.sockets.peak": (sockets.get("peak"), row.get("peak_sockets")),
        "resources.cpu_seconds": (
            rounded(resources.get("cpu_seconds"), 2),
            row.get("cpu_seconds"),
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
        "environment.fixtures": (
            len(environment.get("fixtures", [])),
            record.get("inputs", {}).get("fixtures"),
        ),
        "environment.config.sha256": (
            environment.get("config", {}).get("sha256"),
            row.get("config_sha256"),
        ),
        "usage_records.missing": (
            result.get("usage_records", {}).get("missing"),
            row.get("missing_usage_records"),
        ),
        "upstream.streams_open_at_end": (
            result.get("upstream", {}).get("streams_open_at_end"),
            row.get("leaked_upstream_streams"),
        ),
    }
    for field, (actual, expected) in expected_fields.items():
        if actual != expected:
            fail(f"{label}: raw {field} does not match the compact record")

    offered = max(1, throughput.get("offered", 0))
    outcomes = result.get("outcomes", {})

    expected_verdicts: dict[str, tuple[str, float, float]] = {}

    def add_verdict(name: str, comparison: str, value: Any, bound: Any) -> None:
        if value is None or bound is None:
            fail(f"{label}: required capacity verdict {name!r} has no measurement")
        expected_verdicts[name] = (comparison, float(value), float(bound))

    add_verdict(
        "max_missing_usage_records",
        "<=",
        result.get("usage_records", {}).get("missing"),
        committed_thresholds.get("max_missing_usage_records"),
    )
    add_verdict(
        "max_leaked_upstream_streams",
        "<=",
        result.get("upstream", {}).get("streams_open_at_end"),
        committed_thresholds.get("max_leaked_upstream_streams"),
    )
    if resources.get("procfs") and resources.get("samples") == 0:
        add_verdict("resource_sampling", "<=", 1, 0)
    elif rss:
        add_verdict(
            "max_rss_growth_kib",
            "<=",
            max(0, max(rss.get("peak", 0), rss.get("settled", 0)) - rss.get("baseline", 0)),
            committed_thresholds.get("max_rss_growth_kib"),
        )
    elif resources.get("procfs"):
        add_verdict("resource_sampling", "<=", 1, 0)

    threshold_measurements = {
        "min_accepted": (">=", throughput.get("accepted")),
        "min_accepted_fraction": (">=", throughput.get("accepted", 0) / offered),
        "max_rejections": ("<=", throughput.get("rejected")),
        "max_errors": ("<=", throughput.get("errors")),
        "max_untyped_errors": (
            "<=",
            outcomes.get("errors_by_error_type", {}).get("untyped", 0)
            + outcomes.get("transport_failures", 0),
        ),
        "max_over_deadline": (
            "<=",
            deadlines.get("over_bound") if isinstance(deadlines, dict) else None,
        ),
        "max_foreign_credential_uses": (
            "<=",
            tenancy.get("foreign_credential_uses") if isinstance(tenancy, dict) else None,
        ),
        "max_misattributed_usage_records": (
            "<=",
            tenancy.get("misattributed_usage_records") if isinstance(tenancy, dict) else None,
        ),
        "max_unserved_after_load": (
            "<=",
            int(not recovery.get("served")) if isinstance(recovery, dict) else None,
        ),
        "max_rejected_fraction": ("<=", throughput.get("rejected", 0) / offered),
        "min_rejected_fraction": (">=", throughput.get("rejected", 0) / offered),
        "max_error_fraction": ("<=", throughput.get("errors", 0) / offered),
        "min_queue_depth": (
            ">=",
            result.get("queue", {}).get("max_depth")
            if isinstance(result.get("queue"), dict)
            else None,
        ),
        "max_queue_depth": (
            "<=",
            result.get("queue", {}).get("max_depth")
            if isinstance(result.get("queue"), dict)
            else None,
        ),
    }
    for name, bound in committed_thresholds.items():
        if name in {
            "max_missing_usage_records",
            "max_leaked_upstream_streams",
            "max_rss_growth_kib",
        }:
            continue
        if name not in threshold_measurements:
            fail(f"{label}: committed capacity threshold {name!r} is unsupported")
        comparison, value = threshold_measurements[name]
        add_verdict(name, comparison, value, bound)
    queue = result.get("queue")
    if profile.get("queue_capacity") is not None:
        add_verdict(
            "queue_telemetry_exact",
            ">=",
            int(isinstance(queue, dict) and queue.get("exact") is True),
            1,
        )
        add_verdict(
            "queue_observations",
            ">=",
            queue.get("observations") if isinstance(queue, dict) else None,
            1,
        )
    actual_verdicts: dict[str, dict[str, Any]] = {}
    for verdict in verdicts:
        name = verdict.get("threshold")
        if not isinstance(name, str) or name in actual_verdicts:
            fail(f"{label}: raw capacity verdict names are malformed or duplicated")
        actual_verdicts[name] = verdict
    if set(actual_verdicts) != set(expected_verdicts):
        fail(f"{label}: raw capacity verdict set does not match committed thresholds")
    for name, (comparison, value, bound) in expected_verdicts.items():
        verdict = actual_verdicts[name]
        independently_passed = value <= bound if comparison == "<=" else value >= bound
        if (
            verdict.get("comparison") != comparison
            or verdict.get("value") != value
            or verdict.get("bound") != bound
            or verdict.get("passed") is not independently_passed
            or independently_passed is not True
        ):
            fail(f"{label}: raw capacity verdict {name!r} is not independently valid")

    compact_queue_fields = {
        "queue_observations",
        "queue_min_depth",
        "queue_max_depth",
        "queue_attributes",
        "queue_exact",
    }
    if profile.get("queue_capacity") is None:
        if queue is not None or compact_queue_fields.intersection(row):
            fail(f"{label}: a queue-disabled profile carries queue evidence")
        return
    if not isinstance(queue, dict):
        fail(f"{label}: queue-enabled profile has no decoded queue evidence")
    queue_fields = {
        "queue_observations": queue.get("observations"),
        "queue_min_depth": queue.get("min_depth"),
        "queue_max_depth": queue.get("max_depth"),
        "queue_attributes": queue.get("attributes"),
        "queue_exact": queue.get("exact"),
    }
    for field, actual in queue_fields.items():
        if row.get(field) != actual:
            fail(f"{label}: raw {field} does not match the compact record")
    capacity = profile.get("queue_capacity")
    if (
        type(queue.get("observations")) is not int
        or queue["observations"] <= 0
        or queue.get("attributes") != 0
        or queue.get("exact") is not True
        or queue.get("max_depth") != capacity
    ):
        fail(f"{label}: queue evidence is absent, labelled, inexact, or below its bound")


def numeric_equal(actual: object, expected: object) -> bool:
    if actual is None or expected is None:
        return actual is expected
    if isinstance(actual, bool) or isinstance(expected, bool):
        return actual is expected
    if isinstance(actual, (int, float)) and isinstance(expected, (int, float)):
        return math.isclose(float(actual), float(expected), rel_tol=1e-9, abs_tol=1e-9)
    return actual == expected


def sample_median(samples: list[dict[str, int]], field: str) -> int | None:
    values = sorted(sample[field] for sample in samples)
    return values[len(values) // 2] if values else None


def sample_slope(points: list[tuple[float, float]]) -> float | None:
    if len(points) < 2:
        return None
    mean_x = sum(point[0] for point in points) / len(points)
    mean_y = sum(point[1] for point in points) / len(points)
    covariance = sum((x - mean_x) * (y - mean_y) for x, y in points)
    variance = sum((x - mean_x) ** 2 for x, _ in points)
    return covariance / variance if variance > sys.float_info.epsilon else None


def read_resource_samples(path: Path, label: str) -> list[dict[str, int]]:
    samples: list[dict[str, int]] = []
    expected_fields = {"at_ms", "rss_kib", "cpu_ticks", "fds", "sockets"}
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        try:
            sample = json.loads(line)
        except json.JSONDecodeError as error:
            fail(f"{label}: sample line {line_number} is invalid JSON: {error}")
        if not isinstance(sample, dict) or set(sample) != expected_fields:
            fail(f"{label}: sample line {line_number} has the wrong fields")
        if any(
            not isinstance(value, int) or isinstance(value, bool) or value < 0
            for value in sample.values()
        ):
            fail(f"{label}: sample line {line_number} has a malformed measurement")
        if sample["rss_kib"] <= 0 or sample["sockets"] > sample["fds"]:
            fail(f"{label}: sample line {line_number} has impossible resources")
        if samples and (
            sample["at_ms"] <= samples[-1]["at_ms"]
            or sample["cpu_ticks"] < samples[-1]["cpu_ticks"]
        ):
            fail(f"{label}: resource samples are not monotonic")
        samples.append(sample)
    if not samples or samples[0]["at_ms"] != 0:
        fail(f"{label}: resource samples have no time-zero baseline")
    return samples


def validate_stateful_resource_samples(
    paths: list[Path], resources: list[dict[str, Any]], label: str
) -> None:
    """Bind each series and reproduce every summary the retained rows determine."""
    for path, summary in zip(paths, resources, strict=True):
        samples = read_resource_samples(path, f"{label}: {path.name}")
        periodic = samples[1:]
        # The sampler writes its time-zero baseline and every periodic reading.
        # `finish()` takes one additional settled reading for the summary but
        # does not append it, so summary.samples equals the retained row count:
        # periodic + settled versus baseline + periodic.
        expected = {
            "sampled": bool(samples),
            "samples": len(samples),
            "baseline_rss_kib": periodic[0]["rss_kib"] if periodic else None,
        }
        for field, expected_value in expected.items():
            if not numeric_equal(summary.get(field), expected_value):
                fail(
                    f"{label}: resource summary {summary.get('replica')!r}.{field} "
                    f"does not match {path.name}"
                )
        final_rss = summary.get("final_rss_kib")
        final_fds = summary.get("final_open_fds")
        final_sockets = summary.get("final_sockets")
        if any(
            not isinstance(value, int) or isinstance(value, bool) or value < 0
            for value in (final_rss, final_fds, final_sockets)
        ):
            fail(f"{label}: resource summary {summary.get('replica')!r} has no settled sample")
        exact_with_settled = {
            "peak_rss_kib": max(
                [final_rss, *(sample["rss_kib"] for sample in periodic)]
            ),
            "growth_kib": final_rss - summary["baseline_rss_kib"],
            "peak_open_fds": max(
                [final_fds, *(sample["fds"] for sample in periodic)]
            ),
            "peak_sockets": max(
                [final_sockets, *(sample["sockets"] for sample in periodic)]
            ),
        }
        for field, expected_value in exact_with_settled.items():
            if summary.get(field) != expected_value:
                fail(f"{label}: resource summary {summary.get('replica')!r}.{field} is inexact")
        cpu_seconds = summary.get("cpu_seconds")
        retained_cpu_floor = (
            (periodic[-1]["cpu_ticks"] - periodic[0]["cpu_ticks"]) / 100.0
            if periodic
            else 0.0
        )
        if (
            not isinstance(cpu_seconds, (int, float))
            or isinstance(cpu_seconds, bool)
            or cpu_seconds < retained_cpu_floor
        ):
            fail(f"{label}: resource summary {summary.get('replica')!r}.cpu_seconds is inexact")


def validate_stateless_resource_samples(
    result: dict[str, Any], path: Path, label: str
) -> None:
    """Rebuild resource spans, segment medians, and drift from retained JSONL."""
    samples = read_resource_samples(path, f"{label}: resource samples")
    baseline, periodic = samples[0], samples[1:]
    resources = result.get("resources")
    segments = result.get("segments")
    trend = result.get("trend")
    profile = result.get("profile", {})
    run = result.get("run", {})
    if (
        not isinstance(resources, dict)
        or not isinstance(segments, list)
        or any(not isinstance(segment, dict) for segment in segments)
        or not isinstance(trend, dict)
    ):
        fail(f"{label}: resource, segment, or trend evidence is malformed")
    if resources.get("sampled") is not True or resources.get("procfs") is not True:
        fail(f"{label}: endurance resource sampling is not exact procfs evidence")
    if resources.get("samples") != len(periodic):
        fail(f"{label}: resource sample count does not match retained JSONL")
    if resources.get("sample_interval_ms") != profile.get("sample_interval_ms"):
        fail(f"{label}: resource sample interval does not match the profile")
    if resources.get("user_hz") != 100.0:
        fail(f"{label}: resource CPU tick rate is not the harness contract")

    for field in ("rss_kib", "fds", "sockets"):
        span = resources.get(field)
        if not isinstance(span, dict):
            fail(f"{label}: resources.{field} span is missing")
        expected_peak = max(
            max(sample[field] for sample in samples), span.get("settled", -1)
        )
        if (
            span.get("baseline") != baseline[field]
            or span.get("peak") != expected_peak
            or not isinstance(span.get("settled"), int)
            or isinstance(span.get("settled"), bool)
            or span["settled"] < 0
        ):
            fail(f"{label}: resources.{field} does not follow from retained samples")

    cpu_seconds = resources.get("cpu_seconds")
    utilization = resources.get("cpu_utilization")
    if (
        not isinstance(cpu_seconds, (int, float))
        or isinstance(cpu_seconds, bool)
        or cpu_seconds < 0
        or not isinstance(utilization, (int, float))
        or isinstance(utilization, bool)
        or utilization < 0
    ):
        fail(f"{label}: aggregate CPU evidence is malformed")
    retained_cpu_floor = (samples[-1]["cpu_ticks"] - baseline["cpu_ticks"]) / 100.0
    if cpu_seconds < retained_cpu_floor or not numeric_equal(
        utilization, cpu_seconds / max(run.get("elapsed_ms", 0) / 1000.0, sys.float_info.epsilon)
    ):
        fail(f"{label}: aggregate CPU evidence does not follow from retained samples")

    cursor = 0
    verified_under_load: list[dict[str, Any]] = []
    for index, segment in enumerate(segments):
        count = segment.get("samples")
        if not isinstance(count, int) or isinstance(count, bool) or count < 0:
            fail(f"{label}: segment {index} sample count is malformed")
        subset = periodic[cursor : cursor + count]
        if len(subset) != count:
            fail(f"{label}: segment {index} claims unavailable samples")
        cursor += count
        for field in ("rss_kib", "fds", "sockets"):
            expected_median = sample_median(subset, field)
            expected_peak = max((sample[field] for sample in subset), default=None)
            if (
                segment.get(f"{field}_median") != expected_median
                or segment.get(f"{field}_peak") != expected_peak
            ):
                fail(f"{label}: segment {index} {field} summary is not exact")
        expected_cpu = (
            (subset[-1]["cpu_ticks"] - subset[0]["cpu_ticks"]) / 100.0
            if subset
            else None
        )
        elapsed_seconds = max(
            segment.get("elapsed_ms", 0) / 1000.0, sys.float_info.epsilon
        )
        expected_utilization = (
            expected_cpu / elapsed_seconds if expected_cpu is not None else None
        )
        if not numeric_equal(segment.get("cpu_seconds"), expected_cpu) or not numeric_equal(
            segment.get("cpu_utilization"), expected_utilization
        ):
            fail(f"{label}: segment {index} CPU summary is not exact")
        if segment.get("under_load") is True:
            verified_under_load.append(segment)
    if cursor != len(periodic):
        fail(f"{label}: retained samples are not all represented by segments")

    if trend.get("segments") != len(verified_under_load):
        fail(f"{label}: trend segment count is not exact")
    covered_hours = (
        (
            verified_under_load[-1].get("started_ms", 0)
            + verified_under_load[-1].get("elapsed_ms", 0)
        )
        / 3_600_000.0
        if verified_under_load
        else 0.0
    )
    expected_fitted = (
        len(verified_under_load) >= profile.get("thresholds", {}).get("min_segments", 0)
        and covered_hours >= 0.5
    )
    if trend.get("fitted") is not expected_fitted:
        fail(f"{label}: trend fitted flag is not exact")
    for field in ("rss_kib", "sockets", "fds"):
        points = [
            (
                (segment.get("started_ms", 0) + segment.get("elapsed_ms", 0) / 2.0)
                / 3_600_000.0,
                float(segment[f"{field}_median"]),
            )
            for segment in verified_under_load
            if segment.get(f"{field}_median") is not None
        ]
        if not numeric_equal(trend.get(f"{field}_per_hour"), sample_slope(points)):
            fail(f"{label}: trend {field} slope is not exact")
    quarter = len(verified_under_load) // 4
    first = verified_under_load[:quarter]
    last = verified_under_load[len(verified_under_load) - quarter :] if quarter else []
    for key, selected in (("first", first), ("last", last)):
        values = sorted(
            segment["rss_kib_median"]
            for segment in selected
            if segment.get("rss_kib_median") is not None
        )
        expected = values[len(values) // 2] if values else None
        if trend.get(f"{key}_quarter_rss_kib") != expected:
            fail(f"{label}: trend {key}-quarter RSS median is not exact")


def validate_endurance_verdicts(result: dict[str, Any], label: str) -> None:
    """Rebuild the complete schema-4 verdict set from measured fields."""
    profile = result["profile"]
    thresholds = profile["thresholds"]
    throughput = result.get("throughput", {})
    reconciliation = result["reconciliation"]
    workload = result.get("workload", {})
    upstream = result.get("upstream", {})
    resources = result.get("resources", {})
    trend = result.get("trend", {})
    if not all(
        isinstance(value, dict)
        for value in (thresholds, throughput, workload, upstream, resources, trend)
    ):
        fail(f"{label}: endurance verdict inputs are malformed")

    expected: dict[str, tuple[str, float, float]] = {}

    def add(name: str, comparison: str, value: object, bound: object) -> None:
        if (
            not isinstance(value, (int, float))
            or isinstance(value, bool)
            or not isinstance(bound, (int, float))
            or isinstance(bound, bool)
        ):
            fail(f"{label}: endurance verdict input {name!r} is malformed")
        expected[name] = (comparison, float(value), float(bound))

    offered = throughput.get("offered")
    planned_faults = throughput.get("planned_faults")
    accepted = throughput.get("accepted")
    if any(
        not isinstance(value, int) or isinstance(value, bool) or value < 0
        for value in (offered, planned_faults, accepted)
    ):
        fail(f"{label}: endurance throughput counters are malformed")
    planned_successes = max(offered - planned_faults, 1)
    add(
        "min_accepted_fraction",
        ">=",
        accepted / planned_successes,
        thresholds.get("min_accepted_fraction"),
    )
    add(
        "max_unplanned_errors",
        "<=",
        throughput.get("unplanned_errors"),
        thresholds.get("max_unplanned_errors"),
    )
    add(
        "max_missing_usage_records",
        "<=",
        reconciliation.get("missing"),
        thresholds.get("max_missing_usage_records"),
    )
    add(
        ENDURANCE_SURPLUS_VERDICT,
        "<=",
        reconciliation.get("unexpected_records"),
        thresholds.get(ENDURANCE_SURPLUS_VERDICT),
    )
    add(
        "max_duplicate_usage_records",
        "<=",
        reconciliation.get("duplicates"),
        thresholds.get("max_duplicate_usage_records"),
    )
    add(
        "max_unexpected_usage_statuses",
        "<=",
        reconciliation.get("unexpected_statuses", 0)
        + reconciliation.get("unidentified", 0),
        thresholds.get("max_unexpected_usage_statuses"),
    )
    add(
        "max_leaked_upstream_streams",
        "<=",
        upstream.get("streams_open_at_end"),
        thresholds.get("max_leaked_upstream_streams"),
    )
    add("min_segments", ">=", trend.get("segments"), thresholds.get("min_segments"))
    ending_keys = set(profile.get("mix", {}))
    coverage = int(
        ending_keys
        and ending_keys <= set(workload.get("by_ending", {}))
        and len(workload.get("by_tenant", {})) >= 3
        and workload.get("streamed", 0) > 0
        and workload.get("buffered", 0) > 0
    )
    add("workload_coverage", ">=", coverage, 1)

    if resources.get("procfs") is True and resources.get("samples") == 0:
        add("resource_sampling", "<=", 1, 0)
    else:
        rss = resources.get("rss_kib")
        sockets = resources.get("sockets")
        if isinstance(rss, dict) and isinstance(sockets, dict):
            rss_growth = max(rss.get("peak", 0), rss.get("settled", 0)) - rss.get(
                "baseline", 0
            )
            socket_excess = max(0, sockets.get("settled", 0) - sockets.get("baseline", 0))
            add(
                "max_rss_growth_kib",
                "<=",
                max(0, rss_growth),
                thresholds.get("max_rss_growth_kib"),
            )
            add(
                "max_settled_socket_excess",
                "<=",
                socket_excess,
                thresholds.get("max_settled_socket_excess"),
            )
        elif resources.get("procfs") is True:
            add("resource_sampling", "<=", 1, 0)

    if trend.get("fitted") is True:
        for name, threshold, value in (
            (
                "max_rss_drift_kib_per_hour",
                thresholds.get("max_rss_drift_kib_per_hour"),
                trend.get("rss_kib_per_hour"),
            ),
            (
                "max_socket_drift_per_hour",
                thresholds.get("max_socket_drift_per_hour"),
                trend.get("sockets_per_hour"),
            ),
            (
                "max_fd_drift_per_hour",
                thresholds.get("max_fd_drift_per_hour"),
                trend.get("fds_per_hour"),
            ),
        ):
            if threshold is not None and value is not None:
                add(name, "<=", value, threshold)

    verdicts = result.get("verdicts")
    actual: dict[str, dict[str, Any]] = {}
    if not isinstance(verdicts, list):
        fail(f"{label}: endurance verdict set is malformed")
    for verdict in verdicts:
        name = verdict.get("threshold") if isinstance(verdict, dict) else None
        if not isinstance(name, str) or name in actual:
            fail(f"{label}: endurance verdict names are malformed or duplicated")
        actual[name] = verdict
    if set(actual) != set(expected):
        fail(f"{label}: endurance verdict set does not match committed thresholds")
    for name, (comparison, value, bound) in expected.items():
        verdict = actual[name]
        passed = value <= bound if comparison == "<=" else value >= bound
        if (
            verdict.get("comparison") != comparison
            or not numeric_equal(verdict.get("value"), value)
            or not numeric_equal(verdict.get("bound"), bound)
            or verdict.get("passed") is not passed
            or passed is not True
        ):
            fail(f"{label}: endurance verdict {name!r} is not independently valid")


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
    reconciliation = result.get("reconciliation", {})
    if not isinstance(reconciliation, dict):
        fail(f"{label}: endurance reconciliation is malformed")
    unexpected = reconciliation.get("unexpected_records")
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

    manifest_relative = record.get("inputs", {}).get("manifest")
    if not isinstance(manifest_relative, str) or not manifest_relative:
        fail(f"{label}: endurance manifest path is missing")
    try:
        manifest = tomllib.loads((ROOT / manifest_relative).read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as error:
        fail(f"{label}: endurance manifest is unreadable: {error}")
    manifest_profiles = [
        candidate
        for candidate in manifest.get("profile", [])
        if isinstance(candidate, dict) and candidate.get("id") == profile.get("id")
    ]
    if len(manifest_profiles) != 1:
        fail(f"{label}: raw endurance profile does not name one manifest profile")
    manifest_profile = manifest_profiles[0]
    scale = manifest_profile.get(record.get("tier"))
    if not isinstance(scale, dict):
        fail(f"{label}: endurance manifest tier is malformed")
    expected_thresholds = copy.deepcopy(scale.get("thresholds"))
    if not isinstance(expected_thresholds, dict):
        fail(f"{label}: endurance manifest thresholds are malformed")
    for field in (
        "max_rss_drift_kib_per_hour",
        "max_socket_drift_per_hour",
        "max_fd_drift_per_hour",
    ):
        expected_thresholds.setdefault(field, None)
    profile_echoes = {
        "description": manifest_profile.get("description"),
        "seed": manifest_profile.get("seed"),
        "manifest_duration_ms": scale.get("duration_ms"),
        "concurrency": scale.get("concurrency"),
        "think_time_ms": scale.get("think_time_ms"),
        "sample_interval_ms": scale.get("sample_interval_ms"),
        "segment_ms": scale.get("segment_ms"),
        "mix": manifest_profile.get("mix"),
        "thresholds": expected_thresholds,
    }
    for field, expected_value in profile_echoes.items():
        if profile.get(field) != expected_value:
            fail(
                f"{label}: raw endurance profile.{field} does not match the "
                "qualification manifest"
            )
    if run.get("requested_duration_ms") != profile.get("duration_ms"):
        fail(f"{label}: raw requested and offered durations disagree")
    if run.get("elapsed_ms", 0) < profile.get("duration_ms", 0):
        fail(f"{label}: raw elapsed time is shorter than the offered duration")

    raw_path = Path(label)
    ledger_directories: dict[str, Path] = {}
    for field, files_per_shard in STATEFUL_LEDGER_FIELDS[:2]:
        evidence = reconciliation.get(field, {})
        if not isinstance(evidence, dict) or evidence.get("exact") is not True:
            fail(f"{label}: raw endurance {field} is not exact evidence")
        shards = evidence.get("shards")
        if shards != STATEFUL_LEDGER_SHARDS:
            fail(
                f"{label}: raw endurance {field} has {shards!r} shards, "
                f"expected schema-4 count {STATEFUL_LEDGER_SHARDS}"
            )
        directory = resolve_stateful_ledger(
            raw_path, evidence.get("path"), f"{label}: {field}"
        )
        ledger_directories[field] = directory
        actual_claim = stateful_ledger_claim(
            directory,
            f"{label}: {field}",
            field,
            evidence,
            schema_label="endurance schema 4",
            digest_domain=b"axond-stateful-ledger-v1\0",
        )
        expected_claim = {
            "sha256": sha256_digest(
                row.get(f"{field}_sha256"), f"{label}: compact {field} digest"
            ),
            "files": row.get(f"{field}_files"),
            "bytes": row.get(f"{field}_bytes"),
        }
        if (
            not isinstance(expected_claim["files"], int)
            or isinstance(expected_claim["files"], bool)
            or expected_claim["files"] != shards * files_per_shard
            or not isinstance(expected_claim["bytes"], int)
            or isinstance(expected_claim["bytes"], bool)
            or expected_claim["bytes"] <= 0
            or actual_claim != expected_claim
        ):
            fail(f"{label}: retained {field} shards do not match the compact claim")

    seed = profile.get("seed")
    if not isinstance(seed, int) or isinstance(seed, bool) or seed < 0 or seed >= 2**64:
        fail(f"{label}: endurance profile seed is missing or malformed")
    request_tally = stateful_request_tally(
        ledger_directories["request_identities"], f"{label}: request identities"
    )
    correlation_tally = stateful_correlation_tally(
        ledger_directories["correlations"],
        f"{label}: correlations",
        seed,
        allow_concurrent_endings=False,
    )
    identity_evidence = reconciliation["request_identities"]
    correlation_evidence = reconciliation["correlations"]
    for field in ("recorded", "peak_shard_rows"):
        if identity_evidence.get(field) != request_tally[field]:
            fail(f"{label}: request identity {field} does not match retained shards")
    for field in ("expected", "observed", "peak_shard_rows"):
        if correlation_evidence.get(field) != correlation_tally[field]:
            fail(f"{label}: correlation {field} does not match retained shards")
    for field in ("unidentified", "uncorrelated", "unexpected_statuses"):
        value = reconciliation.get(field)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            fail(f"{label}: reconciliation.{field} is missing or malformed")
    exact_summary = {
        "expected": correlation_tally["workload_expected"],
        "records_observed": correlation_tally["observed"]
        + reconciliation["uncorrelated"],
        "distinct_request_ids": request_tally["distinct"],
        "duplicates": request_tally["duplicates"],
        "missing": correlation_tally["missing"],
        "unexpected_records": correlation_tally["unexpected"]
        + reconciliation["uncorrelated"],
    }
    for field, expected_value in exact_summary.items():
        if reconciliation.get(field) != expected_value:
            fail(
                f"{label}: reconciliation.{field} does not match retained shards "
                f"({expected_value!r})"
            )
    if correlation_tally["probe_expected"] != 0:
        fail(f"{label}: stateless endurance ledger contains probe correlations")
    if (
        request_tally["recorded"]
        != reconciliation["records_observed"] - reconciliation["unidentified"]
    ):
        fail(f"{label}: identified usage count does not match retained request IDs")
    if reconciliation.get("by_status") != dict(correlation_tally["by_status"]):
        fail(f"{label}: status summary does not match retained correlations")
    mismatch_floor = correlation_tally["status_mismatches"]
    mismatch_ceiling = mismatch_floor + reconciliation["uncorrelated"]
    if not mismatch_floor <= reconciliation["unexpected_statuses"] <= mismatch_ceiling:
        fail(f"{label}: unexpected status count cannot follow from retained correlations")

    sample_paths = resolve_resource_samples(
        raw_path, run.get("samples_path"), f"{label}: resource samples"
    )
    verify_resource_sample_claim(sample_paths, row, f"{label}: resource samples")
    if len(sample_paths) != 1:
        fail(f"{label}: stateless endurance must retain exactly one sample series")
    validate_stateless_resource_samples(result, sample_paths[0], label)
    validate_endurance_verdicts(result, label)


def validate(record: dict[str, Any]) -> None:
    slice_id = record.get("slice")
    if not isinstance(slice_id, str):
        fail("the record has no slice")
    schema_version = record.get("schema_version")
    supported = (
        {1, 2}
        if slice_id == "capacity"
        else {ROLLOUT_RECORD_SCHEMA_VERSION}
        if slice_id == "rollout"
        else {1}
    )
    if schema_version not in supported:
        fail(f"unsupported record schema {schema_version!r}")
    compact_hardware(record)

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
    if slice_id == "recovery":
        validate_compact_recovery(record, stages, manifest)
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
        if (slice_id != "capacity" or schema_version == 2) and not row.get(
            "artifact_sha256"
        ):
            fail(f"{row.get('id')}: the raw artifact digest is missing")
        if slice_id == "capacity" and schema_version == 2:
            if row.get("artifact_schema_version") != CAPACITY_RESULT_SCHEMA_VERSION:
                fail(
                    f"{row.get('id')}: compact capacity row does not bind result "
                    f"schema {CAPACITY_RESULT_SCHEMA_VERSION}"
                )
        if slice_id == "rollout":
            if row.get("artifact_schema_version") != ROLLOUT_RESULT_SCHEMA_VERSION:
                fail(
                    f"{row.get('id')}: compact rollout row does not bind result "
                    f"schema {ROLLOUT_RESULT_SCHEMA_VERSION}"
                )
        if slice_id == "stateful-endurance":
            if (
                row.get("artifact_schema_version")
                != STATEFUL_ENDURANCE_RESULT_SCHEMA_VERSION
            ):
                fail(
                    f"{row.get('id')}: compact stateful endurance row does not bind "
                    f"result schema {STATEFUL_ENDURANCE_RESULT_SCHEMA_VERSION}"
                )
            for field, files_per_shard in STATEFUL_LEDGER_FIELDS:
                sha256_digest(
                    row.get(f"{field}_sha256"),
                    f"{row.get('id')}: compact {field} digest",
                )
                if row.get(f"{field}_files") != STATEFUL_LEDGER_SHARDS * files_per_shard:
                    fail(
                        f"{row.get('id')}: compact {field} file count does not match "
                        "the schema-3 shard topology"
                    )
                byte_count = row.get(f"{field}_bytes")
                if (
                    not isinstance(byte_count, int)
                    or isinstance(byte_count, bool)
                    or byte_count < 0
                ):
                    fail(f"{row.get('id')}: compact {field} byte count is malformed")
        if slice_id == "recovery" and not row.get("runner"):
            fail(f"{row.get('id')}: the recovery lane is missing")
        if slice_id == "recovery" and row.get("runner") != expected_recovery_runners.get(
            row.get("id")
        ):
            fail(f"{row.get('id')}: the recovery lane does not match the manifest")
        if slice_id in ("endurance", "stateful-endurance"):
            expected_schema = (
                ENDURANCE_RESULT_SCHEMA_VERSION
                if slice_id == "endurance"
                else STATEFUL_ENDURANCE_RESULT_SCHEMA_VERSION
            )
            if row.get("artifact_schema_version") != expected_schema:
                fail(
                    f"{row.get('id')}: the compact record does not bind result "
                    f"schema {expected_schema}"
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
                    f"but {slice_id} promotion requires at least {required_duration} ms"
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
    assert compatible_correlation_pairs(Counter({4: 1}), Counter({1: 1})) == 1
    assert compatible_correlation_pairs(Counter({4: 1}), Counter({2: 1})) == 1
    assert compatible_correlation_pairs(Counter({4: 1}), Counter({3: 1})) == 1
    assert compatible_correlation_pairs(Counter({1: 1}), Counter({1: 1})) == 0
    assert compatible_correlation_pairs(Counter({4: 1}), Counter({4: 1})) == 0

    path = ROOT / "qualification/capacity/evidence/heavy-local.toml"
    record = tomllib.loads(path.read_text(encoding="utf-8"))
    # The retained file is intentionally historical and must keep the manifest
    # digest and workload set it actually measured. Use its complete compact
    # shape as a fixture, but adapt an in-memory copy to the current manifest so
    # the promoter self-test does not rewrite history whenever a profile lands.
    capacity_manifest_path = ROOT / "qualification/capacity/manifest.toml"
    capacity_manifest = tomllib.loads(capacity_manifest_path.read_text(encoding="utf-8"))
    record["inputs"]["manifest_sha256"] = hashlib.sha256(
        capacity_manifest_path.read_bytes()
    ).hexdigest()
    current_profiles = manifest_workloads("capacity", capacity_manifest)
    recorded_profiles = {row["id"] for row in record["profile"]}
    template = record["profile"][0]
    for profile_id in sorted(current_profiles - recorded_profiles):
        added = copy.deepcopy(template)
        added["id"] = profile_id
        record["profile"].append(added)
    validate(record)
    test_hardware = compact_hardware(record)

    invalid_hardware = {
        "empty string": ("cpu_model", ""),
        "bool as integer": ("cpus", True),
        "zero CPUs": ("cpus", 0),
        "nonpositive memory": ("total_memory_kib", -1),
        "integer as bool": ("containerized", 0),
    }
    for label, (field, value) in invalid_hardware.items():
        candidate = copy.deepcopy(record)
        candidate["hardware"][field] = value
        try:
            validate(candidate)
        except SystemExit:
            pass
        else:
            raise AssertionError(f"compact hardware with {label} was accepted")

    missing_hardware = copy.deepcopy(record)
    del missing_hardware["hardware"]["kernel"]
    try:
        validate(missing_hardware)
    except SystemExit:
        pass
    else:
        raise AssertionError("compact hardware missing a current field was accepted")

    extra_compact_hardware = copy.deepcopy(record)
    extra_compact_hardware["hardware"]["future_field"] = "not-in-schema"
    try:
        validate(extra_compact_hardware)
    except SystemExit:
        pass
    else:
        raise AssertionError("compact hardware with an unknown field was accepted")

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
        "hardware": copy.deepcopy(test_hardware),
        "inputs": {
            "manifest": "qualification/endurance/manifest.toml",
            "manifest_sha256": hashlib.sha256(
                (ROOT / "qualification/endurance/manifest.toml").read_bytes()
            ).hexdigest(),
        },
        "observation": [
            {
                "id": "mixed-endurance",
                "artifact_sha256": "a" * 64,
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

    stateful_manifest_path = ROOT / "qualification/stateful-endurance/manifest.toml"
    stateful_manifest = tomllib.loads(stateful_manifest_path.read_text(encoding="utf-8"))
    stateful_duration = manifest_endurance_duration(stateful_manifest)
    full_stateful = copy.deepcopy(full_endurance)
    full_stateful["slice"] = "stateful-endurance"
    full_stateful["inputs"] = {
        "manifest": "qualification/stateful-endurance/manifest.toml",
        "manifest_sha256": hashlib.sha256(stateful_manifest_path.read_bytes()).hexdigest(),
    }
    stateful_row = full_stateful["observation"][0]
    stateful_row.update(
        id="mixed-stateful-endurance",
        duration_ms=stateful_duration,
        manifest_duration_ms=stateful_duration,
        requested_duration_ms=stateful_duration,
        elapsed_ms=stateful_duration,
        artifact_schema_version=STATEFUL_ENDURANCE_RESULT_SCHEMA_VERSION,
    )
    for field, files_per_shard in STATEFUL_LEDGER_FIELDS:
        stateful_row[f"{field}_sha256"] = "a" * 64
        stateful_row[f"{field}_files"] = STATEFUL_LEDGER_SHARDS * files_per_shard
        stateful_row[f"{field}_bytes"] = 1
    validate(full_stateful)
    short_stateful = copy.deepcopy(full_stateful)
    short_stateful["observation"][0]["duration_ms"] = stateful_duration - 1
    short_stateful["observation"][0]["requested_duration_ms"] = stateful_duration - 1
    short_stateful["observation"][0]["elapsed_ms"] = stateful_duration - 1
    try:
        validate(short_stateful)
    except SystemExit:
        pass
    else:
        raise AssertionError("a shortened compact stateful soak was accepted")

    def expect_refusal(
        label: str, action: Any, expected_message: str | None = None
    ) -> None:
        try:
            action()
        except SystemExit as error:
            if expected_message is not None and expected_message not in str(error):
                raise AssertionError(
                    f"{label} was refused for the wrong reason: {error}"
                ) from error
        else:
            raise AssertionError(f"{label} was accepted")

    recovery_manifest = tomllib.loads(
        (ROOT / "qualification/recovery/manifest.toml").read_text(encoding="utf-8")
    )
    recovery_binary = "a" * 64
    recovery_rows = []
    for scenario in recovery_manifest["scenario"]:
        for stage in scenario["stage"]:
            if stage["status"] != "executable":
                continue
            stage_id = f"{scenario['id']}/{stage['id']}"
            row = {
                "id": stage_id,
                "runner": stage["runner"],
                "driver": stage["driver"],
                "artifact_schema_version": RECOVERY_RESULT_SCHEMA_VERSION,
                "artifact_sha256": hashlib.sha256(stage_id.encode()).hexdigest(),
                "binary_sha256": recovery_binary,
                "elapsed_ms": 1,
                "verdicts": 1,
                "passed": True,
            }
            if stage["driver"] in RECOVERY_DRIVERS:
                row.update(
                    executed_binary_sha256=recovery_binary,
                    execution_bound=True,
                )
            recovery_rows.append(row)
    recovery_record = {
        "schema_version": 1,
        "slice": "recovery",
        "tier": "serving",
        "source": {"git_commit": "commit", "git_dirty": False},
        "binary": {"sha256": recovery_binary, "cargo_profile": "release"},
        "hardware": copy.deepcopy(test_hardware),
        "inputs": {
            "manifest": "qualification/recovery/manifest.toml",
            "manifest_sha256": hashlib.sha256(
                (ROOT / "qualification/recovery/manifest.toml").read_bytes()
            ).hexdigest(),
        },
        "stage": recovery_rows,
    }
    validate(recovery_record)

    process_index = next(
        index
        for index, row in enumerate(recovery_rows)
        if row["driver"] == "stateful-integration"
    )
    restore_index = next(
        index
        for index, row in enumerate(recovery_rows)
        if row["driver"] == "restore-drill"
    )
    malformed_recovery_fields = (
        ("stale recovery schema", 0, "artifact_schema_version", 1),
        ("uppercase recovery artifact digest", 0, "artifact_sha256", "A" * 64),
        ("uppercase recovery binary digest", 0, "binary_sha256", "A" * 64),
        ("unrecognized recovery driver", 0, "driver", "shell"),
        ("stale recovery runner", 0, "runner", "other-runner"),
        ("substituted recovery binary", 0, "binary_sha256", "b" * 64),
        ("failed recovery stage", 0, "passed", False),
        ("empty recovery verdict set", 0, "verdicts", 0),
        (
            "uppercase process-executed digest",
            process_index,
            "executed_binary_sha256",
            "A" * 64,
        ),
        (
            "unbound process recovery stage",
            process_index,
            "execution_bound",
            False,
        ),
    )
    for label, index, field, value in malformed_recovery_fields:
        candidate = copy.deepcopy(recovery_record)
        candidate["stage"][index][field] = value
        expect_refusal(label, lambda candidate=candidate: validate(candidate))

    malformed_record_binary = copy.deepcopy(recovery_record)
    malformed_record_binary["binary"]["sha256"] = "A" * 64
    expect_refusal(
        "uppercase recovery record binary",
        lambda: validate(malformed_record_binary),
    )
    debug_recovery = copy.deepcopy(recovery_record)
    debug_recovery["binary"]["cargo_profile"] = "debug"
    expect_refusal("debug compact recovery record", lambda: validate(debug_recovery))
    missing_process_identity = copy.deepcopy(recovery_record)
    del missing_process_identity["stage"][process_index]["executed_binary_sha256"]
    expect_refusal(
        "process recovery stage missing executable identity",
        lambda: validate(missing_process_identity),
    )
    missing_restore_identity = copy.deepcopy(recovery_record)
    del missing_restore_identity["stage"][restore_index]["executed_binary_sha256"]
    expect_refusal(
        "restore recovery stage missing executable identity",
        lambda: validate(missing_restore_identity),
    )
    duplicate_recovery = copy.deepcopy(recovery_record)
    duplicate_recovery["stage"].append(copy.deepcopy(duplicate_recovery["stage"][0]))
    expect_refusal(
        "duplicate compact recovery stage ID",
        lambda: validate(duplicate_recovery),
    )
    partial_recovery = copy.deepcopy(recovery_record)
    partial_recovery["stage"] = partial_recovery["stage"][:-1]
    expect_refusal("partial recovery record", lambda: validate(partial_recovery))

    import tempfile

    valid_digest_a = "a" * 64
    valid_digest_b = "b" * 64
    malformed_claims = {
        "missing compact workload id": {
            "slice": "fault",
            "observation": [{"artifact_sha256": valid_digest_a}],
        },
        "empty compact workload id": {
            "slice": "fault",
            "observation": [{"id": "", "artifact_sha256": valid_digest_a}],
        },
        "whitespace compact workload id": {
            "slice": "fault",
            "observation": [{"id": " row", "artifact_sha256": valid_digest_a}],
        },
        "short compact digest": {
            "slice": "fault",
            "observation": [{"id": "row", "artifact_sha256": "a" * 63}],
        },
        "non-hex compact digest": {
            "slice": "fault",
            "observation": [{"id": "row", "artifact_sha256": "g" * 64}],
        },
        "non-string compact digest": {
            "slice": "fault",
            "observation": [{"id": "row", "artifact_sha256": 1}],
        },
        "duplicate compact workload id": {
            "slice": "fault",
            "observation": [
                {"id": "row", "artifact_sha256": valid_digest_a},
                {"id": "row", "artifact_sha256": valid_digest_b},
            ],
        },
        "reused compact digest": {
            "slice": "fault",
            "observation": [
                {"id": "row-a", "artifact_sha256": valid_digest_a},
                {"id": "row-b", "artifact_sha256": valid_digest_a.upper()},
            ],
        },
    }
    for label, candidate in malformed_claims.items():
        expect_refusal(label, lambda candidate=candidate: artifact_claims(candidate))

    assert artifact_claims(
        {
            "slice": "capacity",
            "schema_version": 1,
            "profile": [{"id": "profile", "artifact_sha256": valid_digest_a}],
        }
    ) == []
    capacity_claims = artifact_claims(
        {
            "slice": "capacity",
            "schema_version": 2,
            "profile": [{"id": "profile", "artifact_sha256": valid_digest_a}],
        }
    )
    assert [(claim.workload, claim.digest) for claim in capacity_claims] == [
        ("profile", valid_digest_a)
    ]

    raw_identity_cases = (
        ("capacity", {"profile": {"id": "capacity-profile"}}, "capacity-profile"),
        ("fault", {"row": {"id": "fault-row"}}, "fault-row"),
        ("rollout", {"scenario": {"id": "rollout-scenario"}}, "rollout-scenario"),
        (
            "recovery",
            {"scenario": "restore", "stage": "verification"},
            "restore/verification",
        ),
        ("endurance", {"profile": {"id": "mixed-endurance"}}, "mixed-endurance"),
        (
            "stateful-endurance",
            {"profile": {"id": "mixed-stateful-endurance"}},
            "mixed-stateful-endurance",
        ),
    )
    for slice_id, raw_result, expected_id in raw_identity_cases:
        assert raw_workload_id(slice_id, raw_result, "self-test") == expected_id

    malformed_raw_identities = (
        ("capacity", {"profile": {"id": ""}}),
        ("fault", {"row": {"id": ""}}),
        ("rollout", {"scenario": []}),
        ("recovery", {"scenario": "restore", "stage": 1}),
        ("recovery", {"scenario": "restore/other", "stage": "verification"}),
        ("endurance", {"profile": {}}),
        ("stateful-endurance", {"profile": {"id": " stateful"}}),
    )
    for slice_id, raw_result in malformed_raw_identities:
        expect_refusal(
            f"malformed {slice_id} raw workload id",
            lambda slice_id=slice_id, raw_result=raw_result: raw_workload_id(
                slice_id, raw_result, "self-test"
            ),
        )

    fault_checks = {
        "status",
        "error_type",
        "attempts",
        "upstream_requests",
        "usage_records",
        "usage_status",
        "relayed_output",
        "deadline",
        "clean_shutdown",
        "telemetry_exported",
        "telemetry_metrics",
        "telemetry_attempt_span",
        "no_leakage",
        "upstream_cleanup",
        "upstream_abandoned_response_tracked",
        "upstream_released_promptly",
        "usage_attributed_by_identity",
        "usage_carries_request_id",
    }
    fault_result = {
        "schema_version": FAULT_RESULT_SCHEMA_VERSION,
        "row": {
            "id": "provider-rate-limited",
            "family": "provider",
            "fault": "provider_rate_limited",
            "description": "A 429 from the only target: the caller gets a typed verdict, not a hung request.",
            "streamed": False,
        },
        "injection": {
            "fault": "provider_rate_limited",
            "family": "provider",
            "service": None,
            "on_unavailable": None,
            "how": "deterministic provider fixture",
            "injected_latency_ms": None,
            "outage": None,
            "timing": {
                "started_at_unix_ms": 1,
                "elapsed_ms": 10,
                "first_byte_ms": 1,
            },
        },
        "classification": {
            "status": 502,
            "error_type": "provider_dependency_failed",
            "phase": None,
            "transport_failure": False,
            "relayed_output_bytes": 0,
            "during_outage_status": None,
            "after_recovery_status": None,
            "operator_reason_retained": None,
        },
        "deadline": {
            "bound": "overall_timeout_ms",
            "bound_ms": 20_000,
            "wall_clock_ms": 10_000,
            "elapsed_ms": 10,
        },
        "retries": {"attempts": 1, "upstream_requests": 1, "max_attempts": 2},
        "cleanup": {
            "upstream_streams_opened": 0,
            "upstream_streams_open_at_end": 0,
            "settled_within_ms": 1,
            "process_exited_cleanly": True,
        },
        "usage": {
            "records": 1,
            "by_status": {"upstream_error": 1},
            "measured_status": "upstream_error",
            "cost_microdollars": 0,
            "carries_request_id": True,
            "attributed_by": "request_id_mint_time",
            "records_before_measured": 0,
            "unattributable_records": 0,
        },
        "telemetry": {
            "collector": True,
            "exports": {"metrics": 1, "traces": 1},
            "bytes": 1,
            "metrics_observed": ["axond.request.count", "axond.upstream.errors"],
            "metrics_missing": [],
            "spans_observed": ["axond.upstream.attempt"],
        },
        "leakage": {
            "surfaces": [
                {"name": name, "bytes_scanned": 1}
                for name in (
                    "caller_response",
                    "usage_records",
                    "process_output",
                    "telemetry_exports",
                )
            ],
            "needles": {"credential": 1, "secret": 1},
            "findings": [],
        },
        "verdicts": [
            {"check": check, "expected": "bound", "observed": "value", "passed": True}
            for check in sorted(fault_checks)
        ],
    }
    fault_row = {"artifact_schema_version": FAULT_RESULT_SCHEMA_VERSION}
    validate_raw_fault(fault_result, "fault self-test", fault_row)
    forged_fault = copy.deepcopy(fault_result)
    forged_fault["classification"]["status"] = 200
    expect_refusal(
        "forged fault classification",
        lambda: validate_raw_fault(forged_fault, "forged fault self-test", fault_row),
    )
    incomplete_fault = copy.deepcopy(fault_result)
    incomplete_fault["verdicts"].pop()
    expect_refusal(
        "incomplete fault verdict set",
        lambda: validate_raw_fault(
            incomplete_fault, "incomplete fault self-test", fault_row
        ),
    )

    previous_digest = "1" * 64
    candidate_digest = "2" * 64
    rollout_manifest_path = ROOT / "qualification/rollout/manifest.toml"
    rollout_manifest = tomllib.loads(rollout_manifest_path.read_text(encoding="utf-8"))
    rollout_scenario = rollout_manifest["scenario"][0]
    rollout_scale = rollout_scenario["heavy"]
    rollout_shutdown = rollout_scenario["shutdown"]
    rollout_thresholds = rollout_scenario["thresholds"]
    previous_config = """\
mode = "stateful"
[control_plane]
dsn_env = "GW_ROLLOUT_CONTROL_PLANE_DSN"
schema = "axond_rollout_self_test"
[secret_store]
backend = "postgres"
schema = "axond_rollout_self_test"
[catalog]
source = "seed"
store = "postgres"
schema = "axond_rollout_self_test"
bootstrap = "seed"
"""
    revision_by_replica = {
        "compat-0": "candidate-previous-config",
        "next-0": "next",
        "next-1": "next",
        "prev-0": "previous",
        "prev-1": "previous",
        "rollback-0": "previous",
    }
    phase_routes = [
        ("steady-previous", {"prev-0": 1200}),
        ("candidate-on-previous-config", {"compat-0": 1200}),
        ("compatibility-drain", {"prev-0": 1200}),
        ("mixed-0", {"next-0": 600, "prev-0": 600}),
        ("drain-0", {"next-0": 1200}),
        ("mixed-1", {"next-1": 600, "prev-1": 600}),
        ("drain-1", {"next-1": 1200}),
        ("steady-next", {"next-0": 1200}),
        ("rollback-drain", {"rollback-0": 1200}),
        ("rolled-back", {"rollback-0": 1200}),
    ]
    assert rollout_scale["requests_per_phase"] == 1200
    streamed = (
        rollout_scale["requests_per_phase"] + rollout_scale["stream_every"] - 1
    ) // rollout_scale["stream_every"]
    rollout_traffic: list[dict[str, Any]] = []
    routed_by_replica: Counter[str] = Counter()
    expected_usage: list[dict[str, Any]] = []
    trace_sequence = 0

    def add_expected(replica: str, status: str) -> None:
        nonlocal trace_sequence
        trace_sequence += 1
        expected_usage.append(
            {
                "replica": replica,
                "trace_id": f"61786f6e642d726f{trace_sequence:016x}",
                "status": status,
            }
        )

    for phase_name, routes in phase_routes:
        by_revision: Counter[str] = Counter()
        for replica, count in routes.items():
            routed_by_replica[replica] += count
            by_revision[revision_by_replica[replica]] += count
            for _ in range(count):
                add_expected(replica, "ok")
        rollout_traffic.append(
            {
                "phase": phase_name,
                "offered": rollout_scale["requests_per_phase"],
                "answered": rollout_scale["requests_per_phase"],
                "errors": 0,
                "unanswered": 0,
                "torn_streams": 0,
                "streamed": streamed,
                "elapsed_ms": 1,
                "answered_rps": 1200.0,
                "latency_ms": {"p50": 1.0, "p95": 1.0, "p99": 1.0},
                "by_status": {"200": rollout_scale["requests_per_phase"]},
                "by_replica": routes,
                "by_revision": dict(sorted(by_revision.items())),
                "retried": 0,
            }
        )

    drained_replicas = ("compat-0", "prev-0", "prev-1", "next-0")
    for replica in drained_replicas:
        add_expected(replica, "ok")
        add_expected(replica, "client_cancelled")
    # The shared-state probe is sent directly to one retained and one candidate
    # replica. Both accepted requests owe usage even though they bypass ingress.
    add_expected("prev-0", "ok")
    add_expected("next-0", "ok")
    observed_usage = [
        {
            **identity,
            "request_id": f"req_00000000-0000-7000-8000-{index:012x}",
        }
        for index, identity in enumerate(expected_usage, start=1)
    ]
    expected_by_replica = Counter(row["replica"] for row in expected_usage)
    usage_by_status = Counter(row["status"] for row in observed_usage)
    exact_replicas = sorted(revision_by_replica)
    otlp_trace_identities = [
        {
            "replica": identity["replica"],
            "trace_id": identity["trace_id"],
        }
        for identity in sorted(
            expected_usage,
            key=lambda identity: (
                identity["replica"],
                identity["trace_id"],
            ),
        )
    ]
    otlp_trace_identities_sha256 = hashlib.sha256(
        json.dumps(
            otlp_trace_identities,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    per_replica = [
        {
            "replica": replica,
            "reconciliation": "exact_trace",
            "caller_requests_answered": expected_by_replica[replica],
            "usage_records": expected_by_replica[replica],
            "caller_requests_refused_while_draining": 0,
            "retry_duplicates": 0,
            "missing": 0,
            "unexplained_surplus": 0,
            "identity_duplicates": 0,
            "status_mismatches": 0,
            "unidentified": 0,
        }
        for replica in sorted(revision_by_replica)
    ]
    rollout_fleet = [
        {
            "id": replica,
            "revision": revision,
            "admitted_at_ms": 1,
            "admission_took_ms": 1,
            "withdrawn_at_ms": 1 if replica in drained_replicas else None,
            "requests_served": routed_by_replica[replica],
            "requests_after_withdrawal": 0,
            "refusals": 0,
            "usage_records": expected_by_replica[replica],
            "retired": replica in drained_replicas,
        }
        for replica, revision in sorted(revision_by_replica.items())
    ]
    exit_budget = sum(
        rollout_shutdown[field]
        for field in ("drain_grace_ms", "deadline_ms", "flush_timeout_ms")
    )
    rollout_drains = [
        {
            "replica": replica,
            "revision": revision_by_replica[replica],
            "signalled_at_ms": index,
            "readiness_removed_after_ms": 1,
            "exited_after_ms": 1,
            "exit_clean": True,
            "exit_budget_ms": exit_budget,
            "requests_after_withdrawal": 0,
            "dispatches_after_withdrawal": 0,
            "dispatches_beyond_drain_grace": 0,
            "worst_dispatch_lag_ms": None,
            "drain_grace_ms": rollout_shutdown["drain_grace_ms"],
            "buffered_in_flight": {
                "status": 200,
                "completed_after_signal_ms": 1,
                "usage_status": "ok",
            },
            "stream_in_flight": {
                "cut_after_signal_ms": 1,
                "relayed_bytes": 1,
                "usage_status": "client_cancelled",
                "within_deadline": True,
            },
            "usage_records_flushed": expected_by_replica[replica],
        }
        for index, replica in enumerate(drained_replicas, start=1)
    ]
    rollout_identities = sorted(
        {
            json.dumps(
                {
                    "sha256": previous_digest,
                    "version": ROLLOUT_PREVIOUS_VERSION,
                },
                sort_keys=True,
            ),
            json.dumps(
                {
                    "sha256": candidate_digest,
                    "version": ROLLOUT_CANDIDATE_VERSION,
                },
                sort_keys=True,
            ),
        }
    )
    rollout_record = {
        "tier": "heavy",
        "source": {
            "git_commit": "commit",
            "git_dirty": False,
            "crate_version": ROLLOUT_CANDIDATE_VERSION,
        },
        "binary": {
            "sha256": hashlib.sha256("\n".join(rollout_identities).encode()).hexdigest(),
            "version": "mixed",
            "cargo_profile": "release",
            "rustc": "rustc test",
        },
        "inputs": {
            "manifest": "qualification/rollout/manifest.toml",
            "manifest_sha256": "manifest",
        },
    }
    rollout_row = {
        "artifact_schema_version": ROLLOUT_RESULT_SCHEMA_VERSION,
        "elapsed_ms": 100,
        "verdicts": 0,
        "rollout_previous_version": ROLLOUT_PREVIOUS_VERSION,
        "rollout_previous_binary_sha256": previous_digest,
        "rollout_candidate_version": ROLLOUT_CANDIDATE_VERSION,
        "rollout_candidate_binary_sha256": candidate_digest,
        "rollout_retained_archive_sha256": "3" * 64,
        "rollout_shared_stateful_revision": "rev_shared",
        "rollout_shared_alias": "chat",
        "rollout_previous_serves_shared_alias": True,
        "rollout_candidate_serves_shared_alias": True,
        "rollout_usage_reconciliation": "exact_trace",
        "rollout_exact_trace_replicas": len(exact_replicas),
        "rollout_retained_trace_context": "loopback_otlp_http",
        "rollout_otlp_trace_exports": len(exact_replicas),
        "rollout_otlp_trace_export_replicas": len(exact_replicas),
        "rollout_otlp_trace_identities": len(expected_usage),
        "rollout_otlp_trace_identities_sha256": otlp_trace_identities_sha256,
    }
    command_passed = {
        "argv": ["axond", "migrate", "status"],
        "exit_code": 0,
        "succeeded": True,
        "output": "ok",
    }
    previous_config_digest = hashlib.sha256(previous_config.encode()).hexdigest()
    migration_row = {"version": 1, "name": "base", "checksum": "checksum"}
    previous_after = copy.deepcopy(command_passed)
    rollout_result = {
        "schema_version": ROLLOUT_RESULT_SCHEMA_VERSION,
        "scenario": {
            "id": rollout_scenario["id"],
            "description": rollout_scenario["description"],
            "tier": "heavy",
            "replicas": rollout_scenario["replicas"],
            "workers": rollout_scale["workers"],
            "requests_per_phase": rollout_scale["requests_per_phase"],
            "stream_every": rollout_scale["stream_every"],
            "shutdown": {
                **rollout_shutdown,
                "budget_ms": exit_budget,
                "stream_budget_ms": rollout_shutdown["drain_grace_ms"]
                + rollout_shutdown["deadline_ms"],
            },
            "thresholds": copy.deepcopy(rollout_thresholds),
        },
        "run": {
            "started_at_unix_ms": 1,
            "elapsed_ms": 100,
            "harness": "axond rollout harness",
            "harness_version": ROLLOUT_CANDIDATE_VERSION,
            "mode": "qualification",
            "promotable": True,
            "retained_release": {
                "expected_version": ROLLOUT_PREVIOUS_VERSION,
                "expected_binary_sha256": previous_digest,
                "archive_sha256": "3" * 64,
            },
        },
        "environment": {
            "source": copy.deepcopy(rollout_record["source"]),
            "toolchain": {"cargo_profile": "release", "rustc": "rustc test"},
            "manifest": {
                "path": "qualification/rollout/manifest.toml",
                "sha256": "manifest",
            },
        },
        "revisions": [
            {
                "label": "previous",
                "binary": {
                    "sha256": previous_digest,
                    "version": ROLLOUT_PREVIOUS_VERSION,
                },
                "config": {
                    "sha256": previous_config_digest,
                    "normalized_toml": previous_config,
                },
                "distinct_binary": False,
                "exclusive_aliases": [],
                "desired_state_revision": "rev_shared",
            },
            {
                "label": "candidate-previous-config",
                "binary": {
                    "sha256": candidate_digest,
                    "version": ROLLOUT_CANDIDATE_VERSION,
                },
                "config": {
                    "sha256": previous_config_digest,
                    "normalized_toml": previous_config,
                },
                "distinct_binary": True,
                "exclusive_aliases": [],
                "desired_state_revision": "rev_shared",
            },
            {
                "label": "next",
                "binary": {
                    "sha256": candidate_digest,
                    "version": ROLLOUT_CANDIDATE_VERSION,
                },
                "config": {
                    "sha256": previous_config_digest,
                    "normalized_toml": previous_config,
                },
                "distinct_binary": True,
                "exclusive_aliases": [],
                "desired_state_revision": "rev_shared",
            },
        ],
        "fleet": rollout_fleet,
        "traffic": rollout_traffic,
        "drains": rollout_drains,
        "mixed_version": {
            "previous_requests": 600,
            "next_requests": 600,
            "exclusive_alias": "chat-next-only",
            "next_serves_exclusive_alias": False,
            "previous_refuses_exclusive_alias": False,
            "previous_status_for_exclusive_alias": None,
            "previous_error_type_for_exclusive_alias": None,
            "shared_stateful_revision": "rev_shared",
            "shared_alias": "chat",
            "previous_serves_shared_alias": True,
            "next_serves_shared_alias": True,
        },
        "loss": {
            "offered": 12000,
            "answered": 12000,
            "errors": 0,
            "unanswered": 0,
            "torn_streams": 0,
            "unavailable": 0,
            "usage_records_expected": len(expected_usage),
            "usage_records_observed": len(observed_usage),
            "usage_records_distinct": len(observed_usage),
            "draining_refusal_attempts": [],
            "failed_ingress_attempts": [],
            "usage_reconciliation": {
                "mode": "exact_trace",
                "exact_trace_replicas": exact_replicas,
                "retained_trace_context": "loopback_otlp_http",
                "otlp_trace_exports": len(exact_replicas),
                "otlp_trace_export_replicas": exact_replicas,
                "expected_non_usage_trace_identities": [],
                "otlp_trace_collection_errors": [],
                "otlp_trace_identities": otlp_trace_identities,
                "unexpected_otlp_trace_identities": [],
            },
            "expected_usage_identities": expected_usage,
            "observed_usage_identities": observed_usage,
            "usage_identity_duplicates": 0,
            "usage_record_id_duplicates": 0,
            "usage_status_mismatches": 0,
            "usage_records_unidentified": 0,
            "caller_requests": 12000,
            "per_replica": per_replica,
            "usage_records_retry_duplicates": 0,
            "usage_records_missing": 0,
            "usage_records_surplus": 0,
            "refusals_retried": 0,
            "usage_by_status": dict(sorted(usage_by_status.items())),
            "upstream_streams_open_at_end": 0,
        },
        "capacity": {
            "steady_answered_rps": 1200.0,
            "degraded_answered_rps": 1200.0,
            "degraded_fraction": 1.0,
            "steady_latency_p95_ms": 1.0,
            "degraded_latency_p95_ms": 1.0,
        },
        "migration": {
            "preflight": copy.deepcopy(command_passed),
            "status": copy.deepcopy(command_passed),
            "gate_passed": True,
            "target": {
                "dsn_env": "GW_ROLLOUT_CONTROL_PLANE_DSN",
                "schema": "axond_rollout_self_test",
                "config_sha256": previous_config_digest,
            },
            "control_plane": (
                "one real PostgreSQL schema (axond_rollout_self_test) supplied "
                "migrations, desired state, and the serving fleet"
            ),
            "matrix": {
                "evaluated": True,
                "skipped_reason": None,
                "previous_apply": copy.deepcopy(command_passed),
                "previous_status_before": copy.deepcopy(command_passed),
                "candidate_status_before": copy.deepcopy(command_passed),
                "candidate_apply": copy.deepcopy(command_passed),
                "candidate_status_after": copy.deepcopy(command_passed),
                "previous_status_after_candidate": previous_after,
                "previous_versions": [migration_row],
                "candidate_versions": [copy.deepcopy(migration_row)],
                "candidate_added_versions": [],
                "classification": "unchanged",
            },
        },
        "rollback": {
            "migrated_layout_fence": {
                "evaluated": True,
                "skipped_reason": None,
                "status": copy.deepcopy(previous_after),
                "cold_start_attempted": True,
                "cold_start_reached_readiness": True,
                "cold_start_exit_code": None,
                "cold_start_output": "retained binary reached authenticated readiness",
                "expected_refused": False,
                "refused": False,
                "refusal_names_newer_build": False,
            },
            "compatible_patch_rollback": {
                "performed": True,
                "skipped_reason": None,
                "replica": "rollback-0",
                "answered": 1200,
                "errors": 0,
                "served_traffic": True,
            },
        },
        "timeline": [{"at_ms": 1, "phase": "gate", "kind": "start", "detail": "ok"}],
        "verdicts": [],
    }
    rollout_verdict_specs = {
        "max_requests_to_drained_replica": ("<=", 0, rollout_thresholds["max_requests_to_drained_replica"]),
        "max_request_loss": ("<=", 0, rollout_thresholds["max_request_loss"]),
        "max_unavailable_responses": ("<=", 0, rollout_thresholds["max_unavailable_responses"]),
        "max_usage_record_loss": ("<=", 0, rollout_thresholds["max_usage_record_loss"]),
        "unexplained_usage_record_surplus": ("<=", 0, 0),
        "duplicate_usage_trace_identities": ("<=", 0, 0),
        "usage_status_mismatches": ("<=", 0, 0),
        "unidentified_usage_records": ("<=", 0, 0),
        "duplicate_usage_record_ids": ("<=", 0, 0),
        "otlp_trace_context_exported": (
            ">=",
            len(exact_replicas),
            len(exact_replicas),
        ),
        "otlp_trace_export_identity_mismatches": ("<=", 0, 0),
        "readiness_removal_observed": ("<=", 0, 0),
        "max_readiness_removal_ms": ("<=", 1, rollout_thresholds["max_readiness_removal_ms"]),
        "max_replacement_admission_ms": ("<=", 1, rollout_thresholds["max_replacement_admission_ms"]),
        "bounded_termination": ("<=", 0, 0),
        "max_drain_exit_slack_ms": ("<=", 0, rollout_thresholds["max_drain_exit_slack_ms"]),
        "min_mixed_version_requests": (">=", 600, rollout_thresholds["min_mixed_version_requests"]),
        "mixed_version_shared_stateful_serving": ("<=", 0, 0),
        "buffered_requests_completed_during_drain": ("<=", 0, 0),
        "streams_cut_within_deadline": ("<=", 0, 0),
        "partial_streams_accounted": ("<=", 0, 0),
        "upstream_streams_open_at_end": ("<=", 0, 0),
        "migration_gate_passed": ("<=", 0, 0),
        "rollback_matches_migration_classification": ("<=", 0, 0),
        "migration_fence_matches_classification": ("<=", 0, 0),
        "heavy_rollout_is_promotable": ("<=", 0, 0),
        "heavy_rollout_uses_two_binary_digests": ("<=", 0, 0),
        "candidate_serves_shared_stateful_revision": ("<=", 0, 0),
        "migration_matrix_evaluated": ("<=", 0, 0),
    }
    rollout_result["verdicts"] = [
        {
            "threshold": threshold,
            "comparison": comparison,
            "value": value,
            "bound": bound,
            "passed": value <= bound if comparison == "<=" else value >= bound,
        }
        for threshold, (comparison, value, bound) in rollout_verdict_specs.items()
    ]
    rollout_row["verdicts"] = len(rollout_result["verdicts"])
    validate_raw_rollout(
        rollout_result, "rollout self-test", rollout_record, rollout_row
    )
    ordered_float_capacity = copy.deepcopy(rollout_result)
    phase_rps = {
        "steady-previous": 207.77132243898762,
        "compatibility-drain": 207.53550111145506,
        "drain-0": 207.44541025409146,
        "drain-1": 208.06369261293688,
        "steady-next": 208.17288068827708,
        "rollback-drain": 208.10973582528283,
    }
    for phase in ordered_float_capacity["traffic"]:
        phase["answered_rps"] = phase_rps.get(phase["phase"], 208.0)
    ordered_float_capacity["capacity"].update(
        steady_answered_rps=207.97210156363235,
        degraded_answered_rps=207.78858495094153,
        degraded_fraction=0.999117590237772,
    )
    validate_raw_rollout(
        ordered_float_capacity,
        "ordered-float rollout capacity self-test",
        rollout_record,
        rollout_row,
    )
    overflowed_fraction = copy.deepcopy(rollout_result)
    for phase in overflowed_fraction["traffic"]:
        if phase["phase"].startswith("steady"):
            phase["answered_rps"] = sys.float_info.min
        elif "drain" in phase["phase"]:
            phase["answered_rps"] = 1e307
    overflowed_fraction["capacity"].update(
        steady_answered_rps=sys.float_info.min,
        degraded_answered_rps=1e307,
        degraded_fraction=math.inf,
    )
    expect_refusal(
        "overflowed rollout degraded fraction",
        lambda: validate_raw_rollout(
            overflowed_fraction,
            "overflowed degraded fraction self-test",
            rollout_record,
            rollout_row,
        ),
        expected_message="capacity envelope is non-finite",
    )
    drifted_capacity = copy.deepcopy(ordered_float_capacity)
    value = drifted_capacity["capacity"]["degraded_answered_rps"]
    drifted_capacity["capacity"]["degraded_answered_rps"] = math.nextafter(
        value, math.inf
    )
    expect_refusal(
        "one-ULP drifted rollout capacity envelope",
        lambda: validate_raw_rollout(
            drifted_capacity,
            "one-ULP drifted capacity self-test",
            rollout_record,
            rollout_row,
        ),
        expected_message="capacity envelope is not reproducible",
    )
    extra_capacity_field = copy.deepcopy(ordered_float_capacity)
    extra_capacity_field["capacity"]["unverified"] = 0.0
    expect_refusal(
        "extra rollout capacity field",
        lambda: validate_raw_rollout(
            extra_capacity_field,
            "extra capacity field self-test",
            rollout_record,
            rollout_row,
        ),
        expected_message="capacity envelope is not reproducible",
    )
    missing_capacity_field = copy.deepcopy(ordered_float_capacity)
    del missing_capacity_field["capacity"]["degraded_fraction"]
    expect_refusal(
        "missing rollout capacity field",
        lambda: validate_raw_rollout(
            missing_capacity_field,
            "missing capacity field self-test",
            rollout_record,
            rollout_row,
        ),
        expected_message="capacity envelope is not reproducible",
    )
    nonfinite_capacity = copy.deepcopy(ordered_float_capacity)
    nonfinite_capacity["capacity"]["degraded_answered_rps"] = math.nan
    expect_refusal(
        "non-finite rollout capacity envelope",
        lambda: validate_raw_rollout(
            nonfinite_capacity,
            "non-finite capacity self-test",
            rollout_record,
            rollout_row,
        ),
        expected_message="capacity envelope is not reproducible",
    )
    descriptive_control_plane = copy.deepcopy(rollout_result)
    descriptive_control_plane["migration"]["control_plane"] = (
        "redacted PostgreSQL topology description"
    )
    validate_raw_rollout(
        descriptive_control_plane,
        "descriptive control-plane prose self-test",
        rollout_record,
        rollout_row,
    )
    missing_control_plane_description = copy.deepcopy(rollout_result)
    missing_control_plane_description["migration"]["control_plane"] = ""
    expect_refusal(
        "missing rollout control-plane description",
        lambda: validate_raw_rollout(
            missing_control_plane_description,
            "missing control-plane description self-test",
            rollout_record,
            rollout_row,
        ),
    )

    def replace_rollout_configs(candidate: dict[str, Any], normalized: str) -> None:
        digest = hashlib.sha256(normalized.encode()).hexdigest()
        for revision in candidate["revisions"]:
            revision["config"] = {
                "sha256": digest,
                "normalized_toml": normalized,
            }

    invalid_bootstraps = (
        (
            "stateless rollout bootstrap",
            previous_config.replace('mode = "stateful"', 'mode = "stateless"'),
        ),
        (
            "missing rollout control-plane reference",
            previous_config.replace(
                'dsn_env = "GW_ROLLOUT_CONTROL_PLANE_DSN"', 'dsn_env = ""'
            ),
        ),
        (
            "mismatched rollout control-plane schema",
            previous_config.replace(
                'schema = "axond_rollout_self_test"',
                'schema = "axond_rollout_other"',
                1,
            ),
        ),
        (
            "non-PostgreSQL rollout secret store",
            previous_config.replace(
                '[secret_store]\nbackend = "postgres"',
                '[secret_store]\nbackend = "memory"',
            ),
        ),
        (
            "non-PostgreSQL rollout catalogue",
            previous_config.replace(
                '[catalog]\nsource = "seed"\nstore = "postgres"',
                '[catalog]\nsource = "seed"\nstore = "memory"',
            ),
        ),
    )
    for name, invalid_config in invalid_bootstraps:
        invalid = copy.deepcopy(rollout_result)
        replace_rollout_configs(invalid, invalid_config)
        expect_refusal(
            name,
            lambda invalid=invalid, name=name: validate_raw_rollout(
                invalid,
                f"{name} self-test",
                rollout_record,
                rollout_row,
            ),
        )

    unbound_migration_target = copy.deepcopy(rollout_result)
    other_control_plane = previous_config.replace(
        "axond_rollout_self_test", "axond_rollout_other"
    )
    replace_rollout_configs(unbound_migration_target, other_control_plane)
    expect_refusal(
        "migration target detached from all rollout configs",
        lambda: validate_raw_rollout(
            unbound_migration_target,
            "unbound migration target self-test",
            rollout_record,
            rollout_row,
        ),
        expected_message="migration target is not bound",
    )

    missing_migration_target = copy.deepcopy(rollout_result)
    del missing_migration_target["migration"]["target"]
    expect_refusal(
        "missing migration target",
        lambda: validate_raw_rollout(
            missing_migration_target,
            "missing migration target self-test",
            rollout_record,
            rollout_row,
        ),
        expected_message="migration target identity is missing",
    )

    for field, value in (
        ("dsn_env", "GW_OTHER_DSN"),
        ("schema", "axond_rollout_other"),
        ("config_sha256", "0" * 64),
    ):
        wrong_target = copy.deepcopy(rollout_result)
        wrong_target["migration"]["target"][field] = value
        expect_refusal(
            f"migration target with wrong {field}",
            lambda wrong_target=wrong_target, field=field: validate_raw_rollout(
                wrong_target,
                f"wrong migration target {field} self-test",
                rollout_record,
                rollout_row,
            ),
            expected_message="migration target is not bound",
        )

    for name, matrix_values in (
        ("unevaluated rollout migration matrix", {"evaluated": False}),
        (
            "skipped rollout migration matrix",
            {"skipped_reason": "PostgreSQL unavailable"},
        ),
    ):
        invalid = copy.deepcopy(rollout_result)
        invalid["migration"]["matrix"].update(matrix_values)
        expect_refusal(
            name,
            lambda invalid=invalid, name=name: validate_raw_rollout(
                invalid,
                f"{name} self-test",
                rollout_record,
                rollout_row,
            ),
        )
    transport_retry = copy.deepcopy(rollout_result)
    failed_replica = exact_replicas[0]
    retried_identity = next(
        identity
        for identity in expected_usage
        if identity["replica"] != failed_replica
    )
    transport_retry["loss"]["failed_ingress_attempts"] = [
        {
            "caller_id": 0,
            "trace_id": retried_identity["trace_id"],
            "replica": failed_replica,
            "reason": "transport_failure",
        }
    ]
    failed_member = next(
        member
        for member in transport_retry["fleet"]
        if member["id"] == failed_replica
    )
    failed_member["refusals"] = 1
    failed_member["requests_served"] += 1
    transport_retry["traffic"][0]["retried"] = 1
    validate_raw_rollout(
        transport_retry,
        "retried transport failure without export self-test",
        rollout_record,
        rollout_row,
    )

    attributed_export = copy.deepcopy(transport_retry)
    attributed_identity = {
        "replica": failed_replica,
        "trace_id": retried_identity["trace_id"],
    }
    attributed_export["loss"]["usage_reconciliation"][
        "otlp_trace_identities"
    ].append(attributed_identity)
    attributed_export["loss"]["usage_reconciliation"][
        "otlp_trace_identities"
    ].sort(key=lambda identity: (identity["replica"], identity["trace_id"]))
    attributed_export["loss"]["usage_reconciliation"][
        "unexpected_otlp_trace_identities"
    ] = [{**attributed_identity, "reason": "transport_failure"}]
    expect_refusal(
        "attributed exported transport trace",
        lambda: validate_raw_rollout(
            attributed_export,
            "attributed exported transport trace self-test",
            rollout_record,
            rollout_row,
        ),
    )
    substituted_failure_reason = copy.deepcopy(transport_retry)
    substituted_failure_reason["loss"]["failed_ingress_attempts"][0][
        "reason"
    ] = "untyped_503"
    expect_refusal(
        "failed-attempt reason substitution",
        lambda: validate_raw_rollout(
            substituted_failure_reason,
            "failed-attempt reason substitution self-test",
            rollout_record,
            rollout_row,
        ),
    )
    inconsistent_failed_caller = copy.deepcopy(transport_retry)
    second_identity = next(
        identity
        for identity in expected_usage
        if identity["replica"] != failed_replica
        and identity["trace_id"] != retried_identity["trace_id"]
    )
    inconsistent_failed_caller["loss"]["failed_ingress_attempts"].append(
        {
            "caller_id": 0,
            "trace_id": second_identity["trace_id"],
            "replica": failed_replica,
            "reason": "transport_failure",
        }
    )
    expect_refusal(
        "inconsistent failed-attempt caller trace",
        lambda: validate_raw_rollout(
            inconsistent_failed_caller,
            "inconsistent failed-attempt caller trace self-test",
            rollout_record,
            rollout_row,
        ),
    )
    mismatched_failed_count = copy.deepcopy(transport_retry)
    next(
        member
        for member in mismatched_failed_count["fleet"]
        if member["id"] == failed_replica
    )["refusals"] = 0
    expect_refusal(
        "failed-attempt refusal count mismatch",
        lambda: validate_raw_rollout(
            mismatched_failed_count,
            "failed-attempt refusal count mismatch self-test",
            rollout_record,
            rollout_row,
        ),
    )
    deleted_failed_attempts = copy.deepcopy(transport_retry)
    deleted_failed_attempts["loss"]["failed_ingress_attempts"] = []
    expect_refusal(
        "deleted failed-attempt ledger",
        lambda: validate_raw_rollout(
            deleted_failed_attempts,
            "deleted failed-attempt ledger self-test",
            rollout_record,
            rollout_row,
        ),
    )
    same_binary_rollout = copy.deepcopy(rollout_result)
    for revision in same_binary_rollout["revisions"]:
        revision["binary"] = {
            "sha256": candidate_digest,
            "version": ROLLOUT_CANDIDATE_VERSION,
        }
    expect_refusal(
        "same-binary rollout evidence",
        lambda: validate_raw_rollout(
            same_binary_rollout,
            "same-binary rollout self-test",
            rollout_record,
            rollout_row,
        ),
    )
    diagnostic_rollout = copy.deepcopy(rollout_result)
    diagnostic_rollout["run"]["promotable"] = False
    expect_refusal(
        "diagnostic rollout evidence",
        lambda: validate_raw_rollout(
            diagnostic_rollout,
            "diagnostic rollout self-test",
            rollout_record,
            rollout_row,
        ),
    )
    wrong_retained_version = copy.deepcopy(rollout_result)
    wrong_retained_version["run"]["retained_release"]["expected_version"] = "0.3.39"
    wrong_retained_version["revisions"][0]["binary"]["version"] = "0.3.39"
    wrong_retained_row = copy.deepcopy(rollout_row)
    wrong_retained_row["rollout_previous_version"] = "0.3.39"
    expect_refusal(
        "wrong retained rollout version",
        lambda: validate_raw_rollout(
            wrong_retained_version,
            "wrong retained version self-test",
            rollout_record,
            wrong_retained_row,
        ),
    )
    for name, mutate in (
        (
            "missing rollout durable revision",
            lambda candidate: candidate["revisions"][0].update(
                desired_state_revision=None
            ),
        ),
        (
            "mismatched rollout durable revision",
            lambda candidate: candidate["revisions"][2].update(
                desired_state_revision="rev_other"
            ),
        ),
        (
            "wrong rollout shared alias",
            lambda candidate: candidate["mixed_version"].update(shared_alias="other"),
        ),
        (
            "retained binary did not serve shared alias",
            lambda candidate: candidate["mixed_version"].update(
                previous_serves_shared_alias=False
            ),
        ),
        (
            "candidate binary did not serve shared alias",
            lambda candidate: candidate["mixed_version"].update(
                next_serves_shared_alias=False
            ),
        ),
    ):
        invalid = copy.deepcopy(rollout_result)
        mutate(invalid)
        expect_refusal(
            name,
            lambda invalid=invalid, name=name: validate_raw_rollout(
                invalid,
                f"{name} self-test",
                rollout_record,
                rollout_row,
            ),
        )
    rewritten_migration = copy.deepcopy(rollout_result)
    rewritten_migration["migration"]["matrix"]["candidate_versions"][0][
        "checksum"
    ] = "rewritten"
    expect_refusal(
        "rewritten retained migration row",
        lambda: validate_raw_rollout(
            rewritten_migration,
            "rewritten migration self-test",
            rollout_record,
            rollout_row,
        ),
    )
    substituted_usage = copy.deepcopy(rollout_result)
    substituted_usage["loss"]["observed_usage_identities"][0]["trace_id"] = (
        "61786f6e642d726fffffffffffffffff"
    )
    expect_refusal(
        "same-count substituted rollout usage",
        lambda: validate_raw_rollout(
            substituted_usage,
            "substituted usage self-test",
            rollout_record,
            rollout_row,
        ),
    )
    untraced_usage = copy.deepcopy(rollout_result)
    untraced_usage["loss"]["observed_usage_identities"][0]["trace_id"] = None
    expect_refusal(
        "untraced rollout usage",
        lambda: validate_raw_rollout(
            untraced_usage,
            "untraced usage self-test",
            rollout_record,
            rollout_row,
        ),
    )
    unexpected_otlp_trace = copy.deepcopy(rollout_result)
    unexpected_otlp_trace["loss"]["usage_reconciliation"][
        "otlp_trace_identities"
    ].append(
        {
            "replica": exact_replicas[0],
            "trace_id": "61786f6e642d726fffffffffffffffff",
        }
    )
    expect_refusal(
        "unexpected rollout-domain OTLP trace",
        lambda: validate_raw_rollout(
            unexpected_otlp_trace,
            "unexpected OTLP trace self-test",
            rollout_record,
            rollout_row,
        ),
    )
    retried_refusal = copy.deepcopy(rollout_result)
    retried_refusal_row = copy.deepcopy(rollout_row)
    refused_replica = exact_replicas[0]
    accepted_identity = next(
        identity
        for identity in expected_usage
        if identity["replica"] != refused_replica
    )
    refusal_identity = {
        "replica": refused_replica,
        "trace_id": accepted_identity["trace_id"],
    }
    retried_refusal["loss"]["draining_refusal_attempts"].append(
        {
            "caller_id": 0,
            "trace_id": accepted_identity["trace_id"],
            "refused_replica": refused_replica,
            "accepted_replica": accepted_identity["replica"],
            "accepted_status": 200,
        }
    )
    retried_refusal["loss"]["usage_reconciliation"][
        "expected_non_usage_trace_identities"
    ].append({**refusal_identity, "reason": "draining_refusal"})
    retried_witness = retried_refusal["loss"]["usage_reconciliation"][
        "otlp_trace_identities"
    ]
    retried_witness.append(refusal_identity)
    retried_witness.sort(key=lambda identity: (identity["replica"], identity["trace_id"]))
    for member in retried_refusal["fleet"]:
        if member["id"] == exact_replicas[0]:
            member["refusals"] = 1
            member["requests_served"] += 1
    for replica in retried_refusal["loss"]["per_replica"]:
        if replica["replica"] == exact_replicas[0]:
            replica["caller_requests_refused_while_draining"] = 1
    retried_refusal["loss"]["refusals_retried"] = 1
    retried_refusal["traffic"][0]["retried"] = 1
    retried_refusal_row["rollout_otlp_trace_identities"] += 1
    retried_refusal_row["rollout_otlp_trace_identities_sha256"] = hashlib.sha256(
        json.dumps(
            retried_witness,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    validate_raw_rollout(
        retried_refusal,
        "exact retried drain-refusal self-test",
        rollout_record,
        retried_refusal_row,
    )

    substituted_refusal = copy.deepcopy(retried_refusal)
    substituted_row = copy.deepcopy(retried_refusal_row)
    substitute = next(
        identity
        for identity in expected_usage
        if identity["replica"] != refused_replica
        and identity["trace_id"] != accepted_identity["trace_id"]
    )
    substituted_identity = {
        "replica": refused_replica,
        "trace_id": substitute["trace_id"],
    }
    substituted_refusal["loss"]["usage_reconciliation"][
        "expected_non_usage_trace_identities"
    ] = [{**substituted_identity, "reason": "draining_refusal"}]
    substituted_witness = substituted_refusal["loss"]["usage_reconciliation"][
        "otlp_trace_identities"
    ]
    substituted_witness.remove(refusal_identity)
    substituted_witness.append(substituted_identity)
    substituted_witness.sort(key=lambda identity: (identity["replica"], identity["trace_id"]))
    substituted_row["rollout_otlp_trace_identities_sha256"] = hashlib.sha256(
        json.dumps(
            substituted_witness,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    expect_refusal(
        "substituted valid drain-refusal trace",
        lambda: validate_raw_rollout(
            substituted_refusal,
            "substituted refusal trace self-test",
            rollout_record,
            substituted_row,
        ),
    )
    for name, field, value in (
        ("weakened reconciliation mode", "mode", "count_only"),
        ("incomplete exact-trace scope", "exact_trace_replicas", exact_replicas[1:]),
        ("wrong retained trace context", "retained_trace_context", "declared_only"),
        ("insufficient OTLP trace exports", "otlp_trace_exports", 1),
        (
            "incomplete OTLP trace-export scope",
            "otlp_trace_export_replicas",
            exact_replicas[1:],
        ),
        (
            "incomplete OTLP caller-trace witness",
            "otlp_trace_identities",
            otlp_trace_identities[1:],
        ),
        (
            "OTLP trace collection error",
            "otlp_trace_collection_errors",
            ["settlement timed out"],
        ),
    ):
        invalid = copy.deepcopy(rollout_result)
        invalid["loss"]["usage_reconciliation"][field] = value
        expect_refusal(
            name,
            lambda invalid=invalid, name=name: validate_raw_rollout(
                invalid,
                f"{name} self-test",
                rollout_record,
                rollout_row,
            ),
        )
    mismatched_compact_reconciliation = copy.deepcopy(rollout_row)
    mismatched_compact_reconciliation["rollout_otlp_trace_exports"] += 1
    expect_refusal(
        "mismatched compact OTLP witness",
        lambda: validate_raw_rollout(
            rollout_result,
            "mismatched compact OTLP witness self-test",
            rollout_record,
            mismatched_compact_reconciliation,
        ),
    )
    mismatched_compact_trace_digest = copy.deepcopy(rollout_row)
    mismatched_compact_trace_digest["rollout_otlp_trace_identities_sha256"] = "f" * 64
    expect_refusal(
        "mismatched compact OTLP trace digest",
        lambda: validate_raw_rollout(
            rollout_result,
            "mismatched compact OTLP trace digest self-test",
            rollout_record,
            mismatched_compact_trace_digest,
        ),
    )
    weakened_rollout_verdict = copy.deepcopy(rollout_result)
    weakened_rollout_verdict["verdicts"][0]["bound"] += 1
    expect_refusal(
        "weakened rollout verdict",
        lambda: validate_raw_rollout(
            weakened_rollout_verdict,
            "weakened rollout verdict self-test",
            rollout_record,
            rollout_row,
        ),
    )

    with tempfile.TemporaryDirectory() as directory:
        artifact_dir = Path(directory)
        raw = artifact_dir / "mixed-stateful-endurance.json"
        manifest_stateful_profile = stateful_manifest["profile"][0]
        manifest_stateful_scale = manifest_stateful_profile[full_stateful["tier"]]
        manifest_stateful_slo = copy.deepcopy(manifest_stateful_profile["slo"])
        manifest_stateful_slo.setdefault("max_rss_drift_kib_per_hour", None)
        assert stateful_correlation_window_ms(
            90_000,
            [
                {
                    "event": "upstream-outage-begins",
                    "opened_ms": 25_200,
                    "closed_ms": 30_700,
                }
            ],
            manifest_stateful_profile["schedule"],
            "stateful observed-window self-test",
        ) == (24_950, 30_700)
        assert stateful_correlation_window_ms(
            1_001,
            [
                {
                    "event": "upstream-outage-begins",
                    "opened_ms": 1,
                    "closed_ms": 3,
                }
            ],
            {
                "upstream_outage_at": 0.001,
                "upstream_outage_for": 0.002,
                "upstream_outage_correlation_slack_ms": 1,
                "event_dispatch_slack_ms": 1,
            },
            "stateful saturating-slack self-test",
        ) == (0, 3)
        expect_refusal(
            "duplicate observed upstream outages",
            lambda: stateful_correlation_window_ms(
                1_001,
                [
                    {
                        "event": "upstream-outage-begins",
                        "opened_ms": 1,
                        "closed_ms": 3,
                    },
                    {
                        "event": "upstream-outage-begins",
                        "opened_ms": 4,
                        "closed_ms": 6,
                    },
                ],
                {
                    "upstream_outage_at": 0.001,
                    "upstream_outage_for": 0.002,
                    "upstream_outage_correlation_slack_ms": 1,
                    "event_dispatch_slack_ms": 1,
                },
                "duplicate observed-window self-test",
            ),
        )
        expect_refusal(
            "overlong observed upstream outage",
            lambda: stateful_correlation_window_ms(
                1_001,
                [
                    {
                        "event": "upstream-outage-begins",
                        "opened_ms": 1,
                        "closed_ms": 5,
                    }
                ],
                {
                    "upstream_outage_at": 0.001,
                    "upstream_outage_for": 0.002,
                    "upstream_outage_correlation_slack_ms": 1,
                    "event_dispatch_slack_ms": 1,
                },
                "overlong observed-window self-test",
            ),
        )
        for field, value in manifest_stateful_scale.get("slo_overrides", {}).items():
            if value is not None:
                manifest_stateful_slo[field] = value
        stateful_verdict_specs = (
            ("segments", ">=", manifest_stateful_slo["min_segments"]),
            ("unplanned_errors", "<=", manifest_stateful_slo["max_unplanned_errors"]),
            (
                "missing_usage_records",
                "<=",
                manifest_stateful_slo["max_missing_usage_records"],
            ),
            (
                "duplicate_usage_records",
                "<=",
                manifest_stateful_slo["max_duplicate_usage_records"],
            ),
            ("unexpected_usage_records", "<=", 0),
            ("unexpected_usage_statuses", "<=", 0),
            ("concurrent_ending_membership_mismatches", "<=", 0),
            ("unidentified_usage_records", "<=", 0),
            ("uncorrelated_usage_records", "<=", 0),
            ("refusal_usage_records", "<=", 0),
            ("durable_usage_unexpected_records", "<=", 0),
            ("durable_identity_counts_match_sql", "<=", 0),
            (
                "durable_usage_loss_outside_windows",
                "<=",
                manifest_stateful_slo["max_durable_usage_loss_outside_windows"],
            ),
            ("durable_usage_loss_in_window", "<=", 0),
            (
                "durable_usage_lag_ms",
                "<=",
                manifest_stateful_slo["max_durable_usage_lag_ms"],
            ),
            (
                "tenant_boundary_violations",
                "<=",
                manifest_stateful_slo["max_tenant_boundary_violations"],
            ),
            (
                "restart_unavailable_requests",
                "<=",
                manifest_stateful_slo["max_restart_unavailable"],
            ),
            ("recovery_ms", "<=", manifest_stateful_slo["max_recovery_ms"]),
            (
                "readiness_gap_ms",
                "<=",
                manifest_stateful_slo["max_readiness_gap_ms"],
            ),
            ("rss_growth_kib", "<=", manifest_stateful_slo["max_rss_growth_kib"]),
            ("unconverged_revisions", "<=", 0),
            ("convergence_ms", "<=", manifest_stateful_slo["max_convergence_ms"]),
            ("replicas_restarted", ">=", manifest_stateful_slo["replicas"]),
            ("retiring_replicas_exited_cleanly", ">=", 1),
            ("retiring_replicas_exited_in_bound", ">=", 1),
            ("terminated_normally", ">=", 1),
            (
                "rss_drift_kib_per_hour",
                "<=",
                manifest_stateful_slo["max_rss_drift_kib_per_hour"],
            ),
        )
        stateful_verdict_values = {
            "segments": manifest_stateful_slo["min_segments"],
            "replicas_restarted": manifest_stateful_slo["replicas"],
            "retiring_replicas_exited_cleanly": 1,
            "retiring_replicas_exited_in_bound": 1,
            "terminated_normally": 1,
        }
        stateful_offered = sum(manifest_stateful_profile["mix"].values())
        stateful_tenant_base = stateful_offered // 3
        raw_stateful = {
            "schema_version": STATEFUL_ENDURANCE_RESULT_SCHEMA_VERSION,
            "profile": {
                "id": "mixed-stateful-endurance",
                "description": manifest_stateful_profile["description"],
                "tier": "soak",
                "seed": manifest_stateful_profile["seed"],
                "duration_ms": stateful_duration,
                "manifest_duration_ms": stateful_duration,
                "concurrency": manifest_stateful_scale["concurrency"],
                "think_time_ms": manifest_stateful_scale["think_time_ms"],
                "sample_interval_ms": manifest_stateful_scale["sample_interval_ms"],
                "segment_ms": manifest_stateful_scale["segment_ms"],
                "mix": copy.deepcopy(manifest_stateful_profile["mix"]),
                "schedule": copy.deepcopy(manifest_stateful_profile["schedule"]),
                "slo": copy.deepcopy(manifest_stateful_slo),
                "termination": copy.deepcopy(manifest_stateful_profile["termination"]),
            },
            "run": {
                "elapsed_ms": stateful_duration,
                "stop": "duration_elapsed",
                "stop_detail": None,
                "duration_source": stateful_row["duration_source"],
                "settle_ms": manifest_stateful_profile["termination"]["settle_ms"],
                "replicas_booted": manifest_stateful_slo["replicas"],
                "samples_paths": [
                    f"replica-{index}.samples.jsonl"
                    for index in range(manifest_stateful_slo["replicas"])
                ],
            },
            "environment": {
                "source": copy.deepcopy(full_stateful["source"]),
                "binary": {
                    "sha256": full_stateful["binary"]["sha256"],
                    "version": full_stateful["binary"]["version"],
                },
                "manifest": {
                    "path": full_stateful["inputs"]["manifest"],
                    "sha256": full_stateful["inputs"]["manifest_sha256"],
                },
                "hardware": copy.deepcopy(test_hardware),
            },
            "backends": {"usage_reach": "gated"},
            "workload": {
                "offered": stateful_offered,
                "streamed": (stateful_offered + 1) // 2,
                "buffered": stateful_offered // 2,
                "by_tenant": {
                    "tenant-a": stateful_offered - 2 * stateful_tenant_base,
                    "tenant-b": stateful_tenant_base,
                    "tenant-c": stateful_tenant_base,
                },
                "by_ending": copy.deepcopy(manifest_stateful_profile["mix"]),
                "unplanned": 0,
            },
            "segments": [
                {
                    "index": index,
                    "started_ms": index * manifest_stateful_scale["segment_ms"],
                    "ended_ms": (index + 1) * manifest_stateful_scale["segment_ms"],
                    "offered": 1 if index < stateful_offered else 0,
                    "unplanned": 0,
                    "usage_records": 1 if index == 0 else 0,
                }
                for index in range(manifest_stateful_slo["min_segments"])
            ],
            "resources": [
                {
                    "replica": f"replica-{index}",
                    "sampled": True,
                    "samples": 2,
                    "baseline_rss_kib": 100 + index,
                    "peak_rss_kib": 100 + index,
                    "final_rss_kib": 100 + index,
                    "growth_kib": 0,
                    "peak_open_fds": 10,
                    "final_open_fds": 10,
                    "peak_sockets": 2,
                    "final_sockets": 2,
                    "cpu_seconds": 0.0,
                }
                for index in range(manifest_stateful_slo["replicas"])
            ],
            "trend": {
                "rss_kib_per_hour": 0.0,
                "evaluated": True,
                "segments": manifest_stateful_slo["min_segments"],
            },
            "revisions": [
                {"event": event, "converged_ms": 0}
                for event in (
                    "catalogue-revision",
                    "credential-revision",
                    "policy-revision",
                )
            ],
            "faults": [
                {
                    "event": event,
                    "opened_ms": opened_ms,
                    "closed_ms": closed_ms,
                    "recovered_ms": 0,
                }
                for event, opened_ms, closed_ms in (
                    (
                        "upstream-latency-begins",
                        int(stateful_duration * 0.20) + 1,
                        int(stateful_duration * 0.26) + 2,
                    ),
                    (
                        "upstream-outage-begins",
                        int(stateful_duration * 0.28) + 1,
                        int(stateful_duration * 0.34) + 2,
                    ),
                    (
                        "usage-backend-outage-begins",
                        int(stateful_duration * 0.52) + 1,
                        int(stateful_duration * 0.58) + 2,
                    ),
                )
            ],
            "restart": {
                "replicas_restarted": manifest_stateful_slo["replicas"],
                "unavailable": 0,
                "all_exits_clean": True,
                "all_exits_bounded": True,
                "offered_after_last_replacement": 1,
            },
            "tenancy": {
                "probes": 2,
                "violations": 0,
                "probe_served_before_policy": 1,
                "probe_refused_after_policy": 1,
                "probe_served_after_policy": 0,
                "misattributed_records": 0,
            },
            "telemetry": {"worst_readiness_gap_ms": 0},
            "usage": {
                "owed": 1,
                "emitted": 1,
                "distinct": 1,
                "probe_distinct": 0,
                "duplicates": 0,
                "missing": 0,
                "unexpected_records": 0,
                "unexpected_statuses": 0,
                "concurrent_endings": 0,
                "concurrent_ending_membership_mismatches": 0,
                "unidentified": 0,
                "uncorrelated": 0,
                "refusal_records": 0,
                "by_status": {"ok": 1},
                "durable": {"rows": 1, "distinct": 1},
                "durable_lag_ms": 0,
                "durable_settled": True,
                "durable_loss_total": 0,
                "durable_loss_outside_windows": 0,
                "durable_loss_in_window": 0,
                "settled_outside_usage_window": 1,
                "durable_outside_usage_window": 1,
                "durable_duplicate_rows": 0,
                "durable_unexpected_rows": 0,
                "sink_drops": {
                    "reports": 0,
                    "records": 0,
                    "by_reason": {},
                    "records_in_usage_window": 0,
                    "sampled_records_in_usage_window": 0,
                    "records_outside_windows": 0,
                    "examples": [],
                },
            },
            "verdicts": [
                {
                    "threshold": threshold,
                    "comparison": comparison,
                    "value": stateful_verdict_values.get(threshold, 0.0),
                    "bound": bound,
                    "passed": True,
                }
                for threshold, comparison, bound in stateful_verdict_specs
            ],
        }
        stateful_samples: list[Path] = []
        for index, declared in enumerate(raw_stateful["run"]["samples_paths"]):
            sample_path = artifact_dir / declared
            sample_path.write_text(
                json.dumps(
                    {
                        "at_ms": 0,
                        "rss_kib": 100 + index,
                        "cpu_ticks": 0,
                        "fds": 10,
                        "sockets": 2,
                    }
                )
                + "\n"
                + json.dumps(
                    {
                        "at_ms": 1,
                        "rss_kib": 100 + index,
                        "cpu_ticks": 0,
                        "fds": 10,
                        "sockets": 2,
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            stateful_samples.append(sample_path)
        compact_stateful = copy.deepcopy(full_stateful)
        compact_row = compact_stateful["observation"][0]
        request_identity = bytearray(16)
        request_identity[6] = 0x70
        request_identity[8] = 0x80
        request_identity[15] = 1
        request_identity_bytes = bytes(request_identity)
        trace_identity = (
            manifest_stateful_profile["seed"] ^ CORRELATION_DOMAIN
        ).to_bytes(8, "big") + (1).to_bytes(8, "big")
        for field, files_per_shard in STATEFUL_LEDGER_FIELDS:
            ledger = artifact_dir / field
            ledger.mkdir()
            for stem in STATEFUL_LEDGER_STEMS[field]:
                for shard in range(STATEFUL_LEDGER_SHARDS):
                    (ledger / f"{stem}-shard-{shard:02}.bin").write_bytes(b"")
            raw_stateful["usage"][field] = {
                "exact": True,
                "path": field,
                "shards": STATEFUL_LEDGER_SHARDS,
                "peak_shard_rows": (
                    2 if field in ("correlations", "correlation_windows") else 1
                ),
            }
            for count_field in STATEFUL_LEDGER_COUNTS[field]:
                raw_stateful["usage"][field][count_field] = 1
            if field == "request_identities":
                (ledger / "request-shard-01.bin").write_bytes(request_identity_bytes)
            elif field == "correlations":
                (ledger / "expected-shard-01.bin").write_bytes(
                    trace_identity + bytes([0])
                )
                (ledger / "observed-shard-01.bin").write_bytes(
                    trace_identity + bytes([0])
                )
            elif field == "correlation_windows":
                (ledger / "window-shard-01.bin").write_bytes(
                    trace_identity
                    + bytes([0])
                    + (0).to_bytes(8, "big")
                    + (1).to_bytes(8, "big")
                )
            else:
                (ledger / "expected-request-shard-01.bin").write_bytes(
                    request_identity_bytes
                )
                (ledger / "observed-request-shard-01.bin").write_bytes(
                    request_identity_bytes
                )
                raw_stateful["usage"][field].update(
                    stateful_identity_pair_tally(
                        ledger, f"stateful self-test: {field}"
                    )
                )
            claim = stateful_ledger_claim(
                ledger,
                f"stateful self-test: {field}",
                field,
                raw_stateful["usage"][field],
                schema_label="stateful-endurance schema 3",
                digest_domain=b"axond-stateful-ledger-v2\0",
            )
            compact_row[f"{field}_sha256"] = claim["sha256"]
            compact_row[f"{field}_files"] = claim["files"]
            compact_row[f"{field}_bytes"] = claim["bytes"]
        sample = resource_sample_claim(stateful_samples, "stateful self-test samples")
        for claim_field, claim_value in sample.items():
            compact_row[f"samples_{claim_field}"] = claim_value
        compact_row["verdicts"] = len(raw_stateful["verdicts"])

        def persist_stateful_fixture() -> None:
            raw.write_text(json.dumps(raw_stateful), encoding="utf-8")
            compact_row["artifact_sha256"] = hashlib.sha256(
                raw.read_bytes()
            ).hexdigest()

        persist_stateful_fixture()
        verify_promotion_artifacts(compact_stateful, artifact_dir)

        original_stateful_samples = stateful_samples[0].read_bytes()
        stateful_samples[0].write_bytes(
            original_stateful_samples + original_stateful_samples.splitlines()[0] + b"\n"
        )
        expect_refusal(
            "mutated stateful resource samples",
            lambda: verify_promotion_artifacts(compact_stateful, artifact_dir),
        )
        stateful_samples[0].write_bytes(original_stateful_samples)

        segments_verdict = next(
            verdict
            for verdict in raw_stateful["verdicts"]
            if verdict["threshold"] == "segments"
        )
        segments_verdict["comparison"] = "<="
        persist_stateful_fixture()
        expect_refusal(
            "stateful verdict comparison rewrite",
            lambda: verify_promotion_artifacts(compact_stateful, artifact_dir),
        )
        segments_verdict["comparison"] = ">="
        persist_stateful_fixture()

        in_window_verdict = next(
            verdict
            for verdict in raw_stateful["verdicts"]
            if verdict["threshold"] == "durable_usage_loss_in_window"
        )
        in_window_verdict["bound"] = 1
        persist_stateful_fixture()
        expect_refusal(
            "stateful sink-drop allowance rewrite",
            lambda: verify_promotion_artifacts(compact_stateful, artifact_dir),
        )
        in_window_verdict["bound"] = 0
        persist_stateful_fixture()

        recovery_verdict_index = next(
            index
            for index, verdict in enumerate(raw_stateful["verdicts"])
            if verdict["threshold"] == "recovery_ms"
        )
        recovery_verdict = raw_stateful["verdicts"].pop(recovery_verdict_index)
        compact_row["verdicts"] = len(raw_stateful["verdicts"])
        persist_stateful_fixture()
        expect_refusal(
            "stateful required recovery verdict removal",
            lambda: verify_promotion_artifacts(compact_stateful, artifact_dir),
        )
        raw_stateful["verdicts"].insert(recovery_verdict_index, recovery_verdict)
        compact_row["verdicts"] = len(raw_stateful["verdicts"])
        persist_stateful_fixture()

        removed_fault = raw_stateful["faults"].pop()
        persist_stateful_fixture()
        expect_refusal(
            "stateful required recovery observation removal",
            lambda: verify_promotion_artifacts(compact_stateful, artifact_dir),
        )
        raw_stateful["faults"].append(removed_fault)
        persist_stateful_fixture()

        schedule = raw_stateful["profile"]["schedule"]
        original_catalogue_revision = schedule["catalogue_revision_at"]
        schedule["catalogue_revision_at"] = original_catalogue_revision + 0.01
        persist_stateful_fixture()
        expect_refusal(
            "stateful schedule echo rewrite",
            lambda: verify_promotion_artifacts(compact_stateful, artifact_dir),
        )
        schedule["catalogue_revision_at"] = original_catalogue_revision
        persist_stateful_fixture()

        slo = raw_stateful["profile"]["slo"]
        slo["max_recovery_ms"] += 1
        persist_stateful_fixture()
        expect_refusal(
            "stateful SLO echo rewrite",
            lambda: verify_promotion_artifacts(compact_stateful, artifact_dir),
        )
        slo["max_recovery_ms"] -= 1
        persist_stateful_fixture()

        unclaimed_shard = artifact_dir / "stale.bin"
        unclaimed_shard.write_bytes(b"stale")
        expect_refusal(
            "unclaimed stateful ledger shard",
            lambda: verify_promotion_artifacts(compact_stateful, artifact_dir),
        )
        unclaimed_shard.unlink()

        changed_shard = artifact_dir / "correlations" / "expected-shard-00.bin"
        original_shard = changed_shard.read_bytes()
        changed_shard.write_bytes(b"changed")
        expect_refusal(
            "stateful ledger content mutation",
            lambda: verify_promotion_artifacts(compact_stateful, artifact_dir),
        )
        changed_shard.write_bytes(original_shard)

        window_shard = artifact_dir / "correlation_windows" / "window-shard-01.bin"
        original_window_shard = window_shard.read_bytes()
        window_shard.write_bytes(
            trace_identity
            + bytes([1])
            + (0).to_bytes(8, "big")
            + (1).to_bytes(8, "big")
        )
        forged_window_claim = stateful_ledger_claim(
            artifact_dir / "correlation_windows",
            "stateful timing-forgery self-test",
            "correlation_windows",
            raw_stateful["usage"]["correlation_windows"],
            schema_label="stateful-endurance schema 3",
            digest_domain=b"axond-stateful-ledger-v2\0",
        )
        for claim_field, claim_value in forged_window_claim.items():
            compact_row[f"correlation_windows_{claim_field}"] = claim_value
        persist_stateful_fixture()
        expect_refusal(
            "stateful correlation-window semantic forgery with matching hashes",
            lambda: verify_promotion_artifacts(compact_stateful, artifact_dir),
        )
        window_shard.write_bytes(original_window_shard)
        restored_window_claim = stateful_ledger_claim(
            artifact_dir / "correlation_windows",
            "stateful restored timing-ledger self-test",
            "correlation_windows",
            raw_stateful["usage"]["correlation_windows"],
            schema_label="stateful-endurance schema 3",
            digest_domain=b"axond-stateful-ledger-v2\0",
        )
        for claim_field, claim_value in restored_window_claim.items():
            compact_row[f"correlation_windows_{claim_field}"] = claim_value
        persist_stateful_fixture()

        observed_shard = artifact_dir / "correlations" / "observed-shard-01.bin"
        forged_trace_identity = bytearray(trace_identity)
        forged_trace_identity[14] ^= 1
        observed_shard.write_bytes(bytes(forged_trace_identity) + bytes([0]))
        raw_stateful["usage"]["correlations"]["observed"] = 1
        raw_stateful["usage"]["correlations"]["peak_shard_rows"] = 1
        forged_claim = stateful_ledger_claim(
            artifact_dir / "correlations",
            "stateful semantic-forgery self-test",
            "correlations",
            raw_stateful["usage"]["correlations"],
            schema_label="stateful-endurance schema 3",
            digest_domain=b"axond-stateful-ledger-v2\0",
        )
        for claim_field, claim_value in forged_claim.items():
            compact_row[f"correlations_{claim_field}"] = claim_value
        raw.write_text(json.dumps(raw_stateful), encoding="utf-8")
        compact_row["artifact_sha256"] = hashlib.sha256(raw.read_bytes()).hexdigest()
        expect_refusal(
            "stateful ledger semantic forgery with matching hashes",
            lambda: verify_promotion_artifacts(compact_stateful, artifact_dir),
        )

        observed_shard.write_bytes(b"")
        raw_stateful["usage"]["correlations"]["observed"] = 0
        raw_stateful["usage"]["correlations"]["peak_shard_rows"] = 0
        restored_claim = stateful_ledger_claim(
            artifact_dir / "correlations",
            "stateful restored-ledger self-test",
            "correlations",
            raw_stateful["usage"]["correlations"],
            schema_label="stateful-endurance schema 3",
            digest_domain=b"axond-stateful-ledger-v2\0",
        )
        for claim_field, claim_value in restored_claim.items():
            compact_row[f"correlations_{claim_field}"] = claim_value
        raw.write_text(json.dumps(raw_stateful), encoding="utf-8")
        compact_row["artifact_sha256"] = hashlib.sha256(raw.read_bytes()).hexdigest()
        changed_shard.unlink()
        expect_refusal(
            "stateful ledger shard removal",
            lambda: verify_promotion_artifacts(compact_stateful, artifact_dir),
        )

    valid_fault_result = copy.deepcopy(fault_result)

    def fault_artifact() -> dict[str, Any]:
        result = copy.deepcopy(valid_fault_result)
        result["environment"] = {"hardware": copy.deepcopy(test_hardware)}
        return result

    def fault_stub(identifier: str) -> dict[str, Any]:
        return {
            "schema_version": FAULT_RESULT_SCHEMA_VERSION,
            "row": {"id": identifier},
            "environment": {"hardware": copy.deepcopy(test_hardware)},
        }

    with tempfile.TemporaryDirectory() as directory:
        raw = Path(directory) / "fault.json"
        exact = fault_artifact()
        exact["environment"]["hardware"]["future_field"] = "preserved"
        raw.write_text(json.dumps(exact), encoding="utf-8")
        compact = {
            "slice": "fault",
            "hardware": copy.deepcopy(test_hardware),
            "observation": [
                {
                    "id": exact["row"]["id"],
                    "artifact_schema_version": FAULT_RESULT_SCHEMA_VERSION,
                    "artifact_sha256": hashlib.sha256(raw.read_bytes()).hexdigest(),
                }
            ],
        }
        verify_promotion_artifacts(compact, Path(directory))

        hardware_mutations = {
            "os": f"{test_hardware['os']}-changed",
            "arch": f"{test_hardware['arch']}-changed",
            "kernel": f"{test_hardware['kernel']}-changed",
            "cpu_model": f"{test_hardware['cpu_model']}-changed",
            "cpus": test_hardware["cpus"] + 1,
            "total_memory_kib": test_hardware["total_memory_kib"] + 1,
            "containerized": not test_hardware["containerized"],
        }
        for field, value in hardware_mutations.items():
            mutated = fault_artifact()
            mutated["environment"]["hardware"][field] = value
            raw.write_text(json.dumps(mutated), encoding="utf-8")
            compact["observation"][0]["artifact_sha256"] = hashlib.sha256(
                raw.read_bytes()
            ).hexdigest()
            expect_refusal(
                f"raw hardware mutation for {field}",
                lambda: verify_promotion_artifacts(compact, Path(directory)),
            )

        malformed_raw_hardware = fault_artifact()
        malformed_raw_hardware["environment"]["hardware"]["cpus"] = True
        raw.write_text(json.dumps(malformed_raw_hardware), encoding="utf-8")
        compact["observation"][0]["artifact_sha256"] = hashlib.sha256(
            raw.read_bytes()
        ).hexdigest()
        expect_refusal(
            "raw bool-as-int hardware",
            lambda: verify_promotion_artifacts(compact, Path(directory)),
        )

        missing_raw_hardware = fault_artifact()
        del missing_raw_hardware["environment"]["hardware"]["kernel"]
        raw.write_text(json.dumps(missing_raw_hardware), encoding="utf-8")
        compact["observation"][0]["artifact_sha256"] = hashlib.sha256(
            raw.read_bytes()
        ).hexdigest()
        expect_refusal(
            "raw hardware missing a current field",
            lambda: verify_promotion_artifacts(compact, Path(directory)),
        )

    with tempfile.TemporaryDirectory() as directory:
        first = Path(directory) / "first.json"
        second = Path(directory) / "second.json"
        duplicate = fault_artifact()
        first.write_text(json.dumps(duplicate), encoding="utf-8")
        second.write_bytes(first.read_bytes())
        compact = {
            "slice": "fault",
            "hardware": copy.deepcopy(test_hardware),
            "observation": [
                {
                    "id": duplicate["row"]["id"],
                    "artifact_schema_version": FAULT_RESULT_SCHEMA_VERSION,
                    "artifact_sha256": hashlib.sha256(first.read_bytes()).hexdigest(),
                }
            ],
        }
        expect_refusal(
            "duplicate raw files",
            lambda: verify_promotion_artifacts(compact, Path(directory)),
        )

    with tempfile.TemporaryDirectory() as directory:
        first = Path(directory) / "first.json"
        second = Path(directory) / "second.json"
        first.write_text(json.dumps(fault_stub("row-b")), encoding="utf-8")
        second.write_text(json.dumps(fault_stub("row-a")), encoding="utf-8")
        compact = {
            "slice": "fault",
            "hardware": copy.deepcopy(test_hardware),
            "observation": [
                {
                    "id": "row-a",
                    "artifact_sha256": hashlib.sha256(first.read_bytes()).hexdigest(),
                },
                {
                    "id": "row-b",
                    "artifact_sha256": hashlib.sha256(second.read_bytes()).hexdigest(),
                },
            ],
        }
        expect_refusal(
            "swapped raw workload ids",
            lambda: verify_promotion_artifacts(compact, Path(directory)),
        )

    with tempfile.TemporaryDirectory() as directory:
        raw = Path(directory) / "stage.json"
        recovery_manifest_path = ROOT / "qualification/recovery/manifest.toml"
        recovery_manifest = tomllib.loads(
            recovery_manifest_path.read_text(encoding="utf-8")
        )
        recovery_scenario = next(
            scenario
            for scenario in recovery_manifest["scenario"]
            if scenario["id"] == "control-plane-outage"
        )
        recovery_stage = next(
            stage
            for stage in recovery_scenario["stage"]
            if stage["id"] == "administration"
        )
        recovery_gates = []
        for gate in REQUIRED_GATE_NAMES:
            if gate == "max_unauthenticated_admin_successes":
                recovery_gates.append(
                    {
                        "gate": gate,
                        "bound": str(recovery_scenario["gate"][gate]),
                        "observed": "0",
                        "outcome": "met",
                        "detail": "the anonymous administration request was refused",
                    }
                )
            else:
                recovery_gates.append(
                    {
                        "gate": gate,
                        "bound": str(recovery_scenario["gate"][gate]),
                        "observed": "not measured",
                        "outcome": "not_evaluated",
                        "detail": deferred_gate_detail(
                            gate,
                            recovery_stage["evidence"],
                            "another executable stage owns this scenario gate",
                        ),
                    }
                )
        recovery_observations = {
            "authenticated_state_status": 503,
            "mutation_status": 503,
            "anonymous_state_status": 401,
        }
        recovery_checks = [
            {
                "gate": name,
                "bound": expected,
                "observed": expected,
                "outcome": "met",
                "detail": "reconstructed from the synthetic HTTP observation",
            }
            for name, expected in (
                ("authenticated_administration_refused", "503"),
                ("mutation_refused", "503"),
                ("anonymous_administration_refused", "401"),
            )
        ]
        raw_recovery = {
            "schema_version": 2,
            "scenario": recovery_scenario["id"],
            "stage": recovery_stage["id"],
            "runner": recovery_stage["runner"],
            "capability": recovery_scenario["capability"],
            "evidence": recovery_stage["evidence"],
            "run": {
                "started_at_unix_ms": 1,
                "elapsed_ms": 1,
                "axond_version": "0.0.0",
                "control_plane": "postgres",
                "schema": "self_test",
                "schema_identity": "self-test schema is current",
                "axond_executable_sha256": "a" * 64,
                "cargo_profile": "release",
            },
            "timeline": [{"at_ms": 0, "event": "complete", "detail": "test"}],
            "observations": recovery_observations,
            "gates": recovery_gates,
            "checks": recovery_checks,
        }
        if recovery_stage.get("driver") in RECOVERY_DRIVERS:
            raw_recovery["run"].update(
                {
                    "axond_executed_sha256": "a" * 64,
                    "axond_executable_path": "/workspace/target/release/axond",
                    "axond_execution_bound": True,
                }
            )
        raw.write_text(json.dumps(raw_recovery), encoding="utf-8")
        digest = hashlib.sha256(raw.read_bytes()).hexdigest()
        compact = {
            "slice": "recovery",
            "source": {"crate_version": "0.0.0"},
            "binary": {"sha256": "a" * 64, "cargo_profile": "release"},
            "inputs": {
                "manifest": "qualification/recovery/manifest.toml",
                "manifest_sha256": hashlib.sha256(
                    recovery_manifest_path.read_bytes()
                ).hexdigest(),
            },
            "hardware": copy.deepcopy(test_hardware),
            "stage": [
                {
                    "id": f"{recovery_scenario['id']}/{recovery_stage['id']}",
                    "runner": recovery_stage["runner"],
                    "driver": recovery_stage["driver"],
                    "artifact_sha256": digest,
                    "artifact_schema_version": 2,
                    "binary_sha256": "a" * 64,
                    "elapsed_ms": 1,
                    "verdicts": len(recovery_gates) + len(recovery_checks),
                    "passed": True,
                }
            ],
        }
        if recovery_stage.get("driver") in RECOVERY_DRIVERS:
            compact["stage"][0].update(
                {
                    "executed_binary_sha256": "a" * 64,
                    "execution_bound": True,
                }
            )
        verify_promotion_artifacts(compact, Path(directory))
        raw_recovery["run"]["axond_executable_sha256"] = "b" * 64
        raw.write_text(json.dumps(raw_recovery), encoding="utf-8")
        compact["stage"][0]["artifact_sha256"] = hashlib.sha256(
            raw.read_bytes()
        ).hexdigest()
        try:
            verify_promotion_artifacts(compact, Path(directory))
        except SystemExit:
            pass
        else:
            raise AssertionError("a substituted recovery executable was accepted")

    with tempfile.TemporaryDirectory() as directory:
        artifact_dir = Path(directory)
        raw = artifact_dir / "mixed-endurance.json"
        request_dir = artifact_dir / "request-identities"
        correlation_dir = artifact_dir / "correlations"
        request_dir.mkdir()
        correlation_dir.mkdir()
        for shard in range(STATEFUL_LEDGER_SHARDS):
            (request_dir / f"request-shard-{shard:02}.bin").write_bytes(b"")
            (correlation_dir / f"expected-shard-{shard:02}.bin").write_bytes(b"")
            (correlation_dir / f"observed-shard-{shard:02}.bin").write_bytes(b"")
        request_id = bytearray(16)
        request_id[6] = 0x70
        request_id[8] = 0x80
        request_id[15] = 1
        (request_dir / "request-shard-01.bin").write_bytes(bytes(request_id))
        seed = endurance_manifest["profile"][0]["seed"]
        trace_id = (seed ^ CORRELATION_DOMAIN).to_bytes(8, "big") + (1).to_bytes(
            8, "big"
        )
        (correlation_dir / "expected-shard-01.bin").write_bytes(trace_id + bytes([0]))
        (correlation_dir / "observed-shard-01.bin").write_bytes(trace_id + bytes([0]))
        samples_path = artifact_dir / "mixed-endurance.samples.jsonl"
        manifest_endurance_profile = endurance_manifest["profile"][0]
        manifest_endurance_scale = manifest_endurance_profile["soak"]
        manifest_endurance_thresholds = copy.deepcopy(
            manifest_endurance_scale["thresholds"]
        )
        for optional_threshold in (
            "max_rss_drift_kib_per_hour",
            "max_socket_drift_per_hour",
            "max_fd_drift_per_hour",
        ):
            manifest_endurance_thresholds.setdefault(optional_threshold, None)
        segment_count = manifest_endurance_thresholds["min_segments"]
        baseline = {"at_ms": 0, "rss_kib": 100, "cpu_ticks": 0, "fds": 10, "sockets": 2}
        periodic_samples = [
            {
                "at_ms": endurance_duration * (index + 1) // segment_count,
                "rss_kib": 100,
                "cpu_ticks": 100 * (index + 1),
                "fds": 10,
                "sockets": 2,
            }
            for index in range(segment_count)
        ]
        samples_path.write_text(
            "\n".join(json.dumps(sample) for sample in [baseline, *periodic_samples])
            + "\n",
            encoding="utf-8",
        )
        raw_result = {
            "schema_version": ENDURANCE_RESULT_SCHEMA_VERSION,
            "profile": {
                "id": "mixed-endurance",
                "description": manifest_endurance_profile["description"],
                "tier": "soak",
                "duration_ms": endurance_duration,
                "manifest_duration_ms": endurance_duration,
                "concurrency": manifest_endurance_scale["concurrency"],
                "think_time_ms": manifest_endurance_scale["think_time_ms"],
                "sample_interval_ms": manifest_endurance_scale["sample_interval_ms"],
                "segment_ms": manifest_endurance_scale["segment_ms"],
                "mix": copy.deepcopy(manifest_endurance_profile["mix"]),
                "seed": seed,
                "thresholds": manifest_endurance_thresholds,
            },
            "run": {
                "elapsed_ms": endurance_duration,
                "requested_duration_ms": endurance_duration,
                "duration_source": "environment",
                "samples_path": samples_path.name,
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
                "hardware": copy.deepcopy(full_endurance["hardware"]),
            },
            "reconciliation": {
                "expected": 1,
                "records_observed": 1,
                "distinct_request_ids": 1,
                "duplicates": 0,
                "missing": 0,
                "unexpected_records": 0,
                "unexpected_statuses": 0,
                "unidentified": 0,
                "uncorrelated": 0,
                "by_status": {"ok": 1},
                "request_identities": {
                    "recorded": 1,
                    "shards": STATEFUL_LEDGER_SHARDS,
                    "peak_shard_rows": 1,
                    "exact": True,
                    "path": request_dir.name,
                },
                "correlations": {
                    "expected": 1,
                    "observed": 1,
                    "shards": STATEFUL_LEDGER_SHARDS,
                    "peak_shard_rows": 2,
                    "exact": True,
                    "path": correlation_dir.name,
                },
            },
            "throughput": {
                "offered": 4,
                "accepted": 4,
                "planned_faults": 0,
                "unplanned_errors": 0,
            },
            "workload": {
                "by_ending": {
                    ending: 1 for ending in manifest_endurance_profile["mix"]
                },
                "by_tenant": {f"tenant-{index}": 1 for index in range(3)},
                "streamed": 1,
                "buffered": 1,
            },
            "upstream": {"streams_open_at_end": 0},
            "resources": {
                "sampled": True,
                "procfs": True,
                "samples": segment_count,
                "sample_interval_ms": manifest_endurance_scale["sample_interval_ms"],
                "rss_kib": {"baseline": 100, "peak": 100, "settled": 100},
                "fds": {"baseline": 10, "peak": 10, "settled": 10},
                "sockets": {"baseline": 2, "peak": 2, "settled": 2},
                "cpu_seconds": segment_count,
                "cpu_utilization": segment_count / (endurance_duration / 1000.0),
                "user_hz": 100.0,
            },
            "segments": [
                {
                    "index": index,
                    "under_load": True,
                    "started_ms": endurance_duration * index // segment_count,
                    "elapsed_ms": endurance_duration // segment_count,
                    "samples": 1,
                    "rss_kib_median": 100,
                    "rss_kib_peak": 100,
                    "fds_median": 10,
                    "fds_peak": 10,
                    "sockets_median": 2,
                    "sockets_peak": 2,
                    "cpu_seconds": 0.0,
                    "cpu_utilization": 0.0,
                }
                for index in range(segment_count)
            ],
            "trend": {
                "segments": segment_count,
                "fitted": True,
                "rss_kib_per_hour": 0.0,
                "fds_per_hour": 0.0,
                "sockets_per_hour": 0.0,
                "first_quarter_rss_kib": 100,
                "last_quarter_rss_kib": 100,
            },
            "verdicts": [],
        }
        for threshold, comparison, value, bound in (
            (
                "min_accepted_fraction",
                ">=",
                1.0,
                manifest_endurance_thresholds["min_accepted_fraction"],
            ),
            (
                "max_unplanned_errors",
                "<=",
                0.0,
                manifest_endurance_thresholds["max_unplanned_errors"],
            ),
            (
                "max_missing_usage_records",
                "<=",
                0.0,
                manifest_endurance_thresholds["max_missing_usage_records"],
            ),
            (
                ENDURANCE_SURPLUS_VERDICT,
                "<=",
                0.0,
                manifest_endurance_thresholds[ENDURANCE_SURPLUS_VERDICT],
            ),
            (
                "max_duplicate_usage_records",
                "<=",
                0.0,
                manifest_endurance_thresholds["max_duplicate_usage_records"],
            ),
            (
                "max_unexpected_usage_statuses",
                "<=",
                0.0,
                manifest_endurance_thresholds["max_unexpected_usage_statuses"],
            ),
            (
                "max_leaked_upstream_streams",
                "<=",
                0.0,
                manifest_endurance_thresholds["max_leaked_upstream_streams"],
            ),
            ("min_segments", ">=", float(segment_count), float(segment_count)),
            ("workload_coverage", ">=", 1.0, 1.0),
            (
                "max_rss_growth_kib",
                "<=",
                0.0,
                manifest_endurance_thresholds["max_rss_growth_kib"],
            ),
            (
                "max_settled_socket_excess",
                "<=",
                0.0,
                manifest_endurance_thresholds["max_settled_socket_excess"],
            ),
            (
                "max_rss_drift_kib_per_hour",
                "<=",
                0.0,
                manifest_endurance_thresholds["max_rss_drift_kib_per_hour"],
            ),
            (
                "max_socket_drift_per_hour",
                "<=",
                0.0,
                manifest_endurance_thresholds["max_socket_drift_per_hour"],
            ),
            (
                "max_fd_drift_per_hour",
                "<=",
                0.0,
                manifest_endurance_thresholds["max_fd_drift_per_hour"],
            ),
        ):
            raw_result["verdicts"].append(
                {
                    "threshold": threshold,
                    "comparison": comparison,
                    "value": value,
                    "bound": bound,
                    "passed": True,
                }
            )
        raw.write_text(json.dumps(raw_result), encoding="utf-8")
        compact = copy.deepcopy(full_endurance)
        compact_row = compact["observation"][0]
        compact_row["verdicts"] = len(raw_result["verdicts"])
        for field, directory_path in (
            ("request_identities", request_dir),
            ("correlations", correlation_dir),
        ):
            claim = stateful_ledger_claim(
                directory_path,
                f"endurance self-test: {field}",
                field,
                raw_result["reconciliation"][field],
                schema_label="endurance schema 4",
                digest_domain=b"axond-stateful-ledger-v1\0",
            )
            for claim_field, claim_value in claim.items():
                compact_row[f"{field}_{claim_field}"] = claim_value
        sample = resource_sample_claim([samples_path], "endurance self-test samples")
        for claim_field, claim_value in sample.items():
            compact_row[f"samples_{claim_field}"] = claim_value
        compact_row["artifact_sha256"] = hashlib.sha256(
            raw.read_bytes()
        ).hexdigest()
        verify_promotion_artifacts(compact, Path(directory))

        # Stateless schema 4 keeps the pre-existing v1 digest contract and
        # rejects the stateful-only concurrent-ending code even when a compact
        # record is rehashed to match the forged shard.
        legacy_claim = stateful_ledger_claim(
            correlation_dir,
            "endurance legacy-domain self-test",
            "correlations",
            raw_result["reconciliation"]["correlations"],
            schema_label="endurance schema 4",
            digest_domain=b"axond-stateful-ledger-v1\0",
        )
        stateful_domain_claim = stateful_ledger_claim(
            correlation_dir,
            "endurance stateful-domain self-test",
            "correlations",
            raw_result["reconciliation"]["correlations"],
            schema_label="stateful-endurance schema 3",
            digest_domain=b"axond-stateful-ledger-v2\0",
        )
        assert legacy_claim["sha256"] != stateful_domain_claim["sha256"]
        expected_shard = correlation_dir / "expected-shard-01.bin"
        original_expected_row = expected_shard.read_bytes()
        expected_shard.write_bytes(original_expected_row[:16] + bytes([4]))
        forged_claim = stateful_ledger_claim(
            correlation_dir,
            "endurance code-4 self-test",
            "correlations",
            raw_result["reconciliation"]["correlations"],
            schema_label="endurance schema 4",
            digest_domain=b"axond-stateful-ledger-v1\0",
        )
        for claim_field, claim_value in forged_claim.items():
            compact_row[f"correlations_{claim_field}"] = claim_value
        expect_refusal(
            "stateless concurrent-ending code",
            lambda: verify_promotion_artifacts(compact, artifact_dir),
        )
        expected_shard.write_bytes(original_expected_row)
        for claim_field, claim_value in legacy_claim.items():
            compact_row[f"correlations_{claim_field}"] = claim_value

        original_samples = samples_path.read_bytes()
        samples_path.write_bytes(original_samples + original_samples.splitlines()[0] + b"\n")
        expect_refusal(
            "mutated endurance resource samples",
            lambda: verify_promotion_artifacts(compact, artifact_dir),
        )
        samples_path.write_bytes(original_samples)

        original_request_shard = (request_dir / "request-shard-01.bin").read_bytes()
        (request_dir / "request-shard-01.bin").write_bytes(original_request_shard * 2)
        expect_refusal(
            "mutated endurance request ledger",
            lambda: verify_promotion_artifacts(compact, artifact_dir),
        )
        (request_dir / "request-shard-01.bin").write_bytes(original_request_shard)

        coverage_verdict = next(
            verdict
            for verdict in raw_result["verdicts"]
            if verdict["threshold"] == "workload_coverage"
        )
        coverage_verdict["bound"] = 0.0
        raw.write_text(json.dumps(raw_result), encoding="utf-8")
        compact_row["artifact_sha256"] = hashlib.sha256(raw.read_bytes()).hexdigest()
        expect_refusal(
            "forged endurance verdict",
            lambda: verify_promotion_artifacts(compact, artifact_dir),
        )
        coverage_verdict["bound"] = 1.0
        raw.write_text(json.dumps(raw_result), encoding="utf-8")
        compact_row["artifact_sha256"] = hashlib.sha256(raw.read_bytes()).hexdigest()

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
