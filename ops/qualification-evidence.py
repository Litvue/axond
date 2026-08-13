#!/usr/bin/env python3
"""Turn a capacity run's result artifacts into a retained evidence record.

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

Every field written here comes from the artifacts; nothing is supplied by hand
except the runner classification and its note, which say where the run happened
and are the reader's warning about what may be compared with what.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def toml_string(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def number(value: float, digits: int = 1) -> str:
    return f"{value:.{digits}f}"


def load_results(directory: Path) -> list[dict]:
    results = [json.loads(path.read_text(encoding="utf-8")) for path in sorted(directory.glob("*.json"))]
    if not results:
        raise SystemExit(f"{directory}: no result artifacts to retain")
    return results


def shared(results: list[dict], reader) -> str:
    values = {reader(result) for result in results}
    if len(values) != 1:
        raise SystemExit(f"the artifacts disagree about their provenance: {sorted(values)}")
    return values.pop()


def render(results: list[dict], runner: str, note: str) -> str:
    environment = results[0]["environment"]
    hardware = environment["hardware"]
    tier = shared(results, lambda result: result["profile"]["tier"])
    lines = [
        "# A retained capacity run: what one replica did, and on what.",
        "#",
        "# Written by ops/qualification-evidence.py from the artifacts of a single",
        "# run; see docs/operations/qualification.md for how a record is read and",
        "# docs/operations/capacity.md for what the numbers mean. Two records may",
        "# only be compared when their provenance matches — the digests and the",
        "# hardware below are what makes that checkable rather than assumed.",
        "",
        "schema_version = 1",
        'slice = "capacity"',
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
        f"sha256 = {toml_string(environment['binary']['sha256'])}",
        f"version = {toml_string(environment['binary']['version'])}",
        f"cargo_profile = {toml_string(environment['toolchain']['cargo_profile'])}",
        f"rustc = {toml_string(environment['toolchain']['rustc'])}",
        "",
        "[inputs]",
        f"manifest = {toml_string(environment['manifest']['path'])}",
        f"manifest_sha256 = {toml_string(environment['manifest']['sha256'])}",
        f"config_sha256 = {toml_string(environment['config']['sha256'])}",
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
        lines += [
            "",
            "[[profile]]",
            f"id = {toml_string(profile['id'])}",
            f"concurrency = {profile['concurrency']}",
            f"requests = {profile['requests']}",
            f"offered = {throughput['offered']}",
            f"accepted = {throughput['accepted']}",
            f"rejected = {throughput['rejected']}",
            f"errors = {throughput['errors']}",
            f"accepted_rps = {number(throughput['accepted_rps'])}",
            f"latency_p50_ms = {number(latency['p50'], 2)}",
            f"latency_p95_ms = {number(latency['p95'], 2)}",
            f"latency_p99_ms = {number(latency['p99'], 2)}",
        ]
        if ttft:
            lines.append(f"ttft_p95_ms = {number(ttft['p95'], 2)}")
        lines += [
            f"peak_rss_kib = {resources['rss_kib']['peak']}",
            f"rss_growth_kib = {max(resources['rss_kib']['peak'], resources['rss_kib']['settled']) - resources['rss_kib']['baseline']}",
            f"peak_sockets = {resources['sockets']['peak']}",
            f"cpu_seconds = {number(resources['cpu_seconds'], 2)}",
            f"missing_usage_records = {usage['missing']}",
            f"leaked_upstream_streams = {result['upstream']['streams_open_at_end']}",
            f"verdicts = {len(result['verdicts'])}",
            f"passed = {str(all(verdict['passed'] for verdict in result['verdicts'])).lower()}",
        ]

    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("results", type=Path, help="a directory of result artifacts")
    parser.add_argument("--out", type=Path, required=True, help="the record to write")
    parser.add_argument(
        "--runner",
        required=True,
        choices=["local", "github-actions"],
        help="where the run happened, which is what bounds who may compare it",
    )
    parser.add_argument("--note", required=True, help="one line about the machine and build")
    arguments = parser.parse_args()

    record = render(load_results(arguments.results), arguments.runner, arguments.note)
    arguments.out.parent.mkdir(parents=True, exist_ok=True)
    arguments.out.write_text(record, encoding="utf-8")
    print(f"wrote {arguments.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
