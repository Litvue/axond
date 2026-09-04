#!/usr/bin/env python3
"""Public Rust API compatibility gate for published library crates.

Axond ships as a binary: `gateway-core` and `gateway-transport` are unpublished
workspace members, and `axond`'s library target is empty outside `--cfg fuzzing`.
An empty published-library set is therefore success — there is no Rust API for
`cargo-semver-checks` to compare. If a workspace member is later marked
publishable and exports a library, this gate covers it the day it is added.

When such a crate exists, `cargo-semver-checks` compares its public API against
the version already on crates.io and reports whether the change requires a
version bump larger than the one this branch declares. Under the `0.x` policy
(ADR 0015) a break is a minor bump, so on a branch that has not bumped anything,
any break fails.

The gate is blocking. The only way past a break is an entry in
`ops/api-compat-overrides.toml` naming the crate, the exact published baseline
the break is measured against, and the review that accepted it. An override is
honoured only for that one baseline: once a release moves the baseline forward,
the entry is inert and cannot mask a later break.

This runs on every Python the contributor flow supports — 3.10 and newer, the
floor `tests/compat/requirements.txt` is compiled for — so it deliberately does
not import `tomllib`, which only exists from 3.11 on: crate discovery comes from
`cargo metadata`, and the override file is read by `parse_overrides` below.

Usage:
    ops/api-compat.py                 # check every published library crate
    ops/api-compat.py some-lib        # check one crate
    ops/api-compat.py --self-test     # exercise the override parser only
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
OVERRIDES = ROOT / "ops/api-compat-overrides.toml"
OVERRIDE_KEYS = {"crate", "baseline", "justification", "reviewed_in"}
LIBRARY_KINDS = {"lib", "rlib", "dylib", "cdylib", "proc-macro"}
# A crate-level gate that removes the entire library outside a fuzzing build.
FUZZ_ONLY_LIBRARY = re.compile(r"^#!\[cfg\(fuzzing\)\]\s*$", re.MULTILINE)
CHECKING = re.compile(r"^\s*Checking (\S+) v(\S+) -> v(\S+)", re.MULTILINE)
CSI = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")
TABLE = re.compile(r"\[\[break\]\]\s*(?:#.*)?")
KEY_VALUE = re.compile(r'([A-Za-z0-9_-]+)\s*=\s*"([^"]*)"\s*(?:#.*)?')


def published_library_crates() -> list[str]:
    """Workspace members that publish a library API.

    An empty list is success, not an error: Axond's crates.io product is the
    `axond` binary, whose compatibility surface is HTTP, config, and telemetry,
    which `docs/compatibility.md` governs and other CI lanes exercise. Asking
    Cargo rather than keeping a hard-coded list means a new published library
    crate is covered the day it is added, and Cargo — not this script — decides
    what counts as a library target or an inherited `publish` setting.

    A library target that is `cfg`-ed away for every build the workspace performs
    (`#![cfg(fuzzing)]`, which only the out-of-tree `fuzz/` project sets) exports
    nothing and so has no API to compare. `axond` has one of those, as the seam
    the fuzz targets link against, and stays binary-only here.
    """
    completed = subprocess.run(
        ["cargo", "metadata", "--no-deps", "--format-version", "1", "--locked"],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if completed.returncode != 0:
        raise SystemExit(f"cargo metadata failed:\n{completed.stderr.strip()}")
    crates: list[str] = []
    for package in json.loads(completed.stdout)["packages"]:
        # `publish` is null when unrestricted and [] for `publish = false`.
        if package.get("publish") == []:
            continue
        libraries = [
            target
            for target in package["targets"]
            if not LIBRARY_KINDS.isdisjoint(target["kind"])
        ]
        if not libraries or all(exports_nothing(target) for target in libraries):
            continue
        crates.append(package["name"])
    return sorted(crates)


def exports_nothing(target: dict) -> bool:
    """Whether a library target compiles to nothing outside a fuzzing build.

    A crate-level `#![cfg(fuzzing)]` removes the whole module tree, so the target
    is an empty crate for every consumer: there is no API to compare against
    crates.io, and asking `cargo-semver-checks` to try fails on a baseline that
    has no library at all.
    """
    try:
        source = Path(target["src_path"]).read_text(encoding="utf-8")
    except OSError:
        return False
    return FUZZ_ONLY_LIBRARY.search(source) is not None


def parse_overrides(text: str, name: str) -> list[dict[str, str]]:
    """Read the override file without a TOML library.

    Its grammar is fixed by policy and tiny: comments, blank lines, and
    `[[break]]` tables of `key = "value"` pairs. Anything else — another table,
    a key outside a table, an unquoted value — is rejected rather than guessed
    at, which is stricter than a general TOML parser and keeps this gate
    runnable on the oldest Python the contributor flow supports.
    """
    entries: list[dict[str, str]] = []
    current: dict[str, str] | None = None
    for number, raw in enumerate(text.splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        where = f"{name}:{number}"
        if line.startswith("["):
            if not TABLE.fullmatch(line):
                raise SystemExit(f"{where}: only [[break]] tables are allowed")
            current = {}
            entries.append(current)
            continue
        matched = KEY_VALUE.fullmatch(line)
        if not matched:
            raise SystemExit(
                f'{where}: expected key = "value" with a double-quoted string'
            )
        if current is None:
            raise SystemExit(f"{where}: keys must belong to a [[break]] table")
        key, value = matched.group(1), matched.group(2)
        if key in current:
            raise SystemExit(f"{where}: duplicate key {key!r}")
        current[key] = value
    return entries


def load_overrides(crates: list[str]) -> list[dict[str, str]]:
    if not OVERRIDES.exists():
        raise SystemExit(f"missing override policy file: {OVERRIDES.name}")
    overrides = parse_overrides(OVERRIDES.read_text(encoding="utf-8"), OVERRIDES.name)
    for entry in overrides:
        missing = OVERRIDE_KEYS - set(entry)
        extra = set(entry) - OVERRIDE_KEYS
        if missing or extra:
            raise SystemExit(
                f"{OVERRIDES.name}: an override entry must carry exactly "
                f"{sorted(OVERRIDE_KEYS)} (missing {sorted(missing)}, "
                f"unexpected {sorted(extra)})"
            )
        if entry["crate"] not in crates:
            raise SystemExit(
                f"{OVERRIDES.name}: {entry['crate']!r} is not a published "
                f"library crate ({', '.join(crates)})"
            )
        if not entry["justification"].strip() or not entry["reviewed_in"].strip():
            raise SystemExit(
                f"{OVERRIDES.name}: the override for {entry['crate']} needs a "
                "justification and the reviewed pull request or ADR"
            )
    return overrides


def baseline_of(output: str) -> str | None:
    """The published version `cargo-semver-checks` actually compared against.

    It is read from the progress line rather than from crates.io, so that an
    override names the same version the gate reports. Cargo renders that line for
    a terminal and CI asks it to keep doing so (`CARGO_TERM_COLOR: always`), so
    every CSI sequence comes off first — colour, and the erase/cursor moves
    progress rendering also emits — and a carriage return counts as a line start
    like a newline. With any of those left in, the line does not match and a real
    break looks like a comparison that never happened, which no override can name.
    """
    matched = CHECKING.search(CSI.sub("", output).replace("\r", "\n"))
    return matched.group(2) if matched else None


def check(crate: str) -> tuple[bool, str, str]:
    """Run the semver check for one crate; return (passed, baseline, output)."""
    completed = subprocess.run(
        [
            "cargo",
            "semver-checks",
            "check-release",
            "--all-features",
            "--package",
            crate,
        ],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    print(completed.stdout, end="")
    baseline = baseline_of(completed.stdout)
    if baseline is None:
        # Either the tool failed before it could compare anything (no network, a
        # build error, an unpublished crate), or it compared and this gate could
        # not read which version against. Neither is an override-able break, and
        # a pass whose baseline is unknown is not evidence: an override could
        # never name that baseline.
        raise SystemExit(
            f"cargo semver-checks did not report a published baseline for "
            f"{crate}; fix the invocation rather than overriding it"
        )
    return completed.returncode == 0, baseline, completed.stdout


def self_test() -> int:
    """Prove the override parser on whatever interpreter is running it.

    The gate must work on the Python floor the contributor flow documents, and
    that floor predates `tomllib`, so this runs first in `just api-compat` and in
    CI — on the floor as well as on the default interpreter.
    """
    parsed = parse_overrides(
        '# a comment\n\n[[break]]\ncrate = "gateway-core"  # trailing\n'
        'baseline = "0.1.0"\njustification = "why # not"\n'
        'reviewed_in = "https://example.invalid/pull/1"\n',
        "self-test",
    )
    assert parsed == [
        {
            "crate": "gateway-core",
            "baseline": "0.1.0",
            "justification": "why # not",
            "reviewed_in": "https://example.invalid/pull/1",
        }
    ], parsed
    assert parse_overrides("# only a comment\n", "self-test") == []
    # The baseline an override has to name is read from styled output too, which
    # is what CI produces.
    for line in (
        "    Checking gateway-core v0.3.21 -> v0.3.22 (minor change)\n",
        # Colour, which `CARGO_TERM_COLOR: always` keeps in the CI log.
        "\x1b[1m\x1b[32m    Checking\x1b[0m gateway-core v0.3.21 -> v0.3.22\n",
        # Progress rendering: erase the line, redraw it after a carriage return.
        "\x1b[2K\r\x1b[32m    Checking\x1b[0m gateway-core v0.3.21 -> v0.3.22\r",
        "\x1b[1A\x1b[2K    Checking gateway-core v0.3.21 -> v0.3.22\n",
    ):
        assert baseline_of(line) == "0.3.21", line
    assert baseline_of("error: no such package\n") is None
    for bad in (
        '[[allow]]\ncrate = "gateway-core"\n',  # a table that is not a break
        'crate = "gateway-core"\n',  # a key outside any table
        "[[break]]\ncrate = gateway-core\n",  # an unquoted value
        '[[break]]\ncrate = "a"\ncrate = "b"\n',  # a duplicated key
    ):
        try:
            parse_overrides(bad, "self-test")
        except SystemExit:
            continue
        raise AssertionError(f"the parser accepted an invalid policy: {bad!r}")
    # The committed policy file has to satisfy the same grammar.
    parse_overrides(OVERRIDES.read_text(encoding="utf-8"), OVERRIDES.name)
    print(f"api-compat: parser self-test passed on Python {sys.version.split()[0]}")
    return 0


def main(argv: list[str]) -> int:
    if argv and argv[0] == "--self-test":
        if argv[1:]:
            raise SystemExit("--self-test takes no other arguments")
        return self_test()
    # Every run proves the parser on the interpreter in use, so a contributor on
    # the Python floor finds out here rather than from a confusing parse error.
    self_test()
    crates = published_library_crates()
    requested = argv or crates
    unknown = [crate for crate in requested if crate not in crates]
    if unknown:
        known = ", ".join(crates) if crates else "none"
        raise SystemExit(
            f"not published library crates: {', '.join(unknown)} "
            f"(known: {known})"
        )
    overrides = load_overrides(crates)
    if not requested:
        print("api-compat: no published library crates; axond has no public Rust API")
        print("api-compat passed (0 published library crates)")
        return 0

    failures: list[str] = []
    notes: list[str] = []
    used: list[dict[str, str]] = []
    for crate in requested:
        passed, baseline, _ = check(crate)
        if passed:
            notes.append(f"{crate}: compatible with the published v{baseline}")
            continue
        applicable = [
            entry
            for entry in overrides
            if entry["crate"] == crate and entry["baseline"] == baseline
        ]
        if not applicable:
            stale = [entry for entry in overrides if entry["crate"] == crate]
            hint = ""
            if stale:
                hint = (
                    f" (an override for {crate} exists, but for baseline "
                    f"v{stale[0]['baseline']}, not the published v{baseline})"
                )
            failures.append(
                f"{crate}: public API break against the published v{baseline} "
                f"with no reviewed override{hint}"
            )
            continue
        used.extend(applicable)
        for entry in applicable:
            notes.append(
                f"{crate}: break against v{baseline} accepted by a reviewed "
                f"override ({entry['reviewed_in']}): {entry['justification']}"
            )

    for entry in overrides:
        if entry not in used and entry["crate"] in requested:
            # Inert, not fatal: the break it covered is now part of the
            # baseline, so the entry protects nothing and should be deleted.
            notes.append(
                f"note: the override for {entry['crate']} v{entry['baseline']} "
                "no longer applies and can be removed"
            )

    for note in notes:
        print(f"api-compat: {note}")
    for failure in failures:
        print(f"api-compat failed: {failure}", file=sys.stderr)
    if failures:
        print(
            "\nIntentional break? Add an entry to ops/api-compat-overrides.toml "
            "and explain it in the pull request; see "
            "docs/maintainers/releasing.md#public-api-compatibility.",
            file=sys.stderr,
        )
        return 1
    print(f"api-compat passed ({len(requested)} published library crates)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
