#!/usr/bin/env python3
"""Pin policy for the TypeScript provider-SDK compatibility lane.

The point of that lane is that a provider SDK *release* is what changed, not the
pin, so nothing in it may float: every dependency is an exact version, the
lockfile agrees with `package.json` and carries an integrity hash for each
package, and the Node runtime is declared in one place that CI and a contributor
both read.

Runs on any `python3` from 3.10 up — the same floor as the other gates — and
reads JSON only, so it needs neither `node` nor a network.

Usage:
    ops/compat-ts-pins.py              # check the committed lane
    ops/compat-ts-pins.py --self-test  # exercise the version-range comparison only
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PROJECT = ROOT / "tests/compat-ts"
MANIFEST = PROJECT / "package.json"
LOCKFILE = PROJECT / "package-lock.json"
NODE_VERSION_FILE = PROJECT / ".nvmrc"
EXACT = re.compile(r"^\d+\.\d+\.\d+$")
# The SDKs whose compatibility this lane exists to assert, as opposed to the
# toolchain that builds it.
REQUIRED_SDKS = {"openai", "@anthropic-ai/sdk"}


def check() -> list[str]:
    failures: list[str] = []
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    lock = json.loads(LOCKFILE.read_text(encoding="utf-8"))

    direct: dict[str, str] = {
        **manifest.get("dependencies", {}),
        **manifest.get("devDependencies", {}),
    }
    missing = REQUIRED_SDKS - direct.keys()
    if missing:
        failures.append(f"package.json: the provider SDKs are not depended on: {sorted(missing)}")

    for name, version in sorted(direct.items()):
        if not EXACT.match(version):
            failures.append(f"package.json: {name} is not pinned exactly: {version!r}")

    packages = lock.get("packages", {})
    for name, version in sorted(direct.items()):
        entry = packages.get(f"node_modules/{name}")
        if entry is None:
            failures.append(f"package-lock.json: no entry for {name}")
        elif entry.get("version") != version:
            failures.append(
                f"package-lock.json: {name} is locked at {entry.get('version')!r},"
                f" not the pinned {version!r}"
            )

    for path, entry in sorted(packages.items()):
        if path and not entry.get("integrity") and not entry.get("link"):
            failures.append(f"package-lock.json: {path} has no integrity hash")

    node = str(manifest.get("engines", {}).get("node", ""))
    if not node:
        failures.append("package.json: engines.node does not declare the Node runtime")
    declared = NODE_VERSION_FILE.read_text(encoding="utf-8").strip()
    if not EXACT.match(declared):
        failures.append(f"{NODE_VERSION_FILE.name}: {declared!r} is not an exact Node version")
    elif not _satisfies(declared, node):
        failures.append(
            f"{NODE_VERSION_FILE.name} pins Node {declared}, which engines.node ({node}) excludes"
        )
    return failures


def _satisfies(version: str, engines: str) -> bool:
    """Whether an exact version clears a `>=X <Y` engines range.

    Only the two comparators this lane's manifest uses are understood; anything
    else is reported rather than assumed to pass.
    """
    parsed = tuple(int(part) for part in version.split("."))
    for clause in engines.split():
        match = re.fullmatch(r"(>=|<)(\d+(?:\.\d+){0,2})", clause)
        if match is None:
            return False
        bound = tuple(int(part) for part in match.group(2).split("."))
        bound += (0,) * (3 - len(bound))
        if match.group(1) == ">=" and parsed < bound:
            return False
        if match.group(1) == "<" and parsed >= bound:
            return False
    return True


def self_test() -> int:
    """Prove the range comparison, which decides whether a Node pin is allowed.

    The committed manifest only ever exercises the passing case, so without this
    the interesting ones — a pin under the floor, one at the ceiling, a
    comparator the parser does not understand — are never executed.
    """
    allowed = [("22.12.0", ">=22.12.0 <23"), ("22.20.1", ">=22.12.0 <23")]
    refused = [
        ("22.11.0", ">=22.12.0 <23"),
        ("23.0.0", ">=22.12.0 <23"),
        ("22.12.0", ">=20.0.0 <22.12.0"),
        ("22.12.0", "^22.12.0"),
    ]
    for version, engines in allowed:
        if not _satisfies(version, engines):
            raise AssertionError(f"{version} should satisfy {engines!r}")
    for version, engines in refused:
        if _satisfies(version, engines):
            raise AssertionError(f"{version} should not satisfy {engines!r}")
    print(f"compat-ts pins: range self-test passed on Python {sys.version.split()[0]}")
    return 0


def main() -> int:
    if "--self-test" in sys.argv[1:]:
        return self_test()
    failures = check()
    for failure in failures:
        print(failure, file=sys.stderr)
    if failures:
        return 1
    print("compat-ts pins: exact versions, locked hashes, and a pinned Node runtime")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
