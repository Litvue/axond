#!/usr/bin/env python3
"""Public Rust API compatibility gate for the published library crates.

`cargo-semver-checks` compares each crate's public API against the version
already on crates.io — the API real consumers hold — and reports whether the
change requires a version bump larger than the one this branch declares. Under
the `0.x` policy (ADR 0015) a break is a minor bump, so on a branch that has not
bumped anything, any break fails.

The gate is blocking. The only way past it is an entry in
`ops/api-compat-overrides.toml` naming the crate, the exact published baseline
the break is measured against, and the review that accepted it. An override is
honoured only for that one baseline: once a release moves the baseline forward,
the entry is inert and cannot mask a later break.

Usage:
    ops/api-compat.py                 # check every published library crate
    ops/api-compat.py gateway-core    # check one crate
"""

from __future__ import annotations

import re
import subprocess
import sys
import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
OVERRIDES = ROOT / "ops/api-compat-overrides.toml"
OVERRIDE_KEYS = {"crate", "baseline", "justification", "reviewed_in"}
CHECKING = re.compile(r"^\s*Checking (\S+) v(\S+) -> v(\S+)", re.MULTILINE)


def published_library_crates() -> list[str]:
    """Workspace members that publish a library API, in manifest order.

    A binary-only member (`axond`) has no public Rust API to break: its
    compatibility surface is HTTP, config, and telemetry, which
    `docs/compatibility.md` governs and other CI lanes exercise. Reading this
    from the manifests rather than a hard-coded list means a new published
    library crate is covered the day it is added.
    """
    workspace = tomllib.loads((ROOT / "Cargo.toml").read_text(encoding="utf-8"))
    crates: list[str] = []
    for member in workspace["workspace"]["members"]:
        manifest = tomllib.loads(
            (ROOT / member / "Cargo.toml").read_text(encoding="utf-8")
        )
        package = manifest["package"]
        library = "lib" in manifest or (ROOT / member / "src/lib.rs").exists()
        if package.get("publish") is False or not library:
            continue
        crates.append(package["name"])
    if not crates:
        raise SystemExit("no published library crates found in the workspace")
    return crates


def load_overrides(crates: list[str]) -> list[dict[str, str]]:
    if not OVERRIDES.exists():
        raise SystemExit(f"missing override policy file: {OVERRIDES.name}")
    document = tomllib.loads(OVERRIDES.read_text(encoding="utf-8"))
    unexpected = set(document) - {"break"}
    if unexpected:
        raise SystemExit(
            f"{OVERRIDES.name}: unexpected top-level keys {sorted(unexpected)}"
        )
    overrides = document.get("break", [])
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


def check(crate: str) -> tuple[bool, str | None, str]:
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
    matched = CHECKING.search(completed.stdout)
    baseline = matched.group(2) if matched else None
    if completed.returncode != 0 and baseline is None:
        # The tool failed before it could compare anything (no network, a build
        # error, an unpublished crate). That is not an override-able break.
        raise SystemExit(
            f"cargo semver-checks could not compare {crate} against a published "
            "baseline; fix the invocation rather than overriding it"
        )
    return completed.returncode == 0, baseline, completed.stdout


def main(argv: list[str]) -> int:
    crates = published_library_crates()
    requested = argv or crates
    unknown = [crate for crate in requested if crate not in crates]
    if unknown:
        raise SystemExit(
            f"not published library crates: {', '.join(unknown)} "
            f"(known: {', '.join(crates)})"
        )
    overrides = load_overrides(crates)

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
