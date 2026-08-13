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

Two runs are refused rather than written, because a record carrying either is
worse than no record: artifacts that disagree about their provenance — `target/`
survives across commits, so a leftover result sorts in beside a fresh one — and
a run whose provenance the harness could not determine, which would otherwise be
rendered as a null and read back as broken TOML.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Callable


def toml_string(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def number(value: float, digits: int = 1) -> str:
    return f"{value:.{digits}f}"


def load_results(directory: Path) -> list[dict]:
    results = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in sorted(directory.glob("*.json"))
    ]
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
