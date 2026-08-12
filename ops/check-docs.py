#!/usr/bin/env python3
"""Fast, dependency-free documentation drift checks."""

from __future__ import annotations

import re
import sys
from pathlib import Path
from urllib.parse import unquote


ROOT = Path(__file__).resolve().parent.parent


def markdown_files() -> list[Path]:
    files = [ROOT / "README.md", ROOT / "RELEASE.md", ROOT / "CONTRIBUTING.md"]
    files.extend((ROOT / "docs").rglob("*.md"))
    files.extend((ROOT / "crates").rglob("README.md"))
    files.extend((ROOT / "tests").rglob("README.md"))
    return sorted(set(files))


def check_relative_links(files: list[Path]) -> list[str]:
    failures: list[str] = []
    link_pattern = re.compile(r"!?(?:\[[^]]*\])\(([^)]+)\)")
    for source in files:
        text = source.read_text(encoding="utf-8")
        for raw_target in link_pattern.findall(text):
            target = raw_target.strip().strip("<>")
            if not target or target.startswith(("#", "http://", "https://", "mailto:")):
                continue
            path_text = unquote(target.split("#", 1)[0].split("?", 1)[0])
            if not path_text:
                continue
            resolved = (source.parent / path_text).resolve()
            if not resolved.exists():
                failures.append(
                    f"{source.relative_to(ROOT)}: missing relative link target {target!r}"
                )
    return failures


def workspace_version() -> str:
    cargo = (ROOT / "Cargo.toml").read_text(encoding="utf-8")
    match = re.search(
        r"\[workspace\.package\]\s+version\s*=\s*\"([^\"]+)\"", cargo
    )
    if not match:
        raise RuntimeError("could not read workspace.package.version")
    return match.group(1)


def check_release_markers() -> list[str]:
    version = workspace_version()
    failures: list[str] = []
    candidates = [ROOT / "README.md", ROOT / "docker-compose.yml"]
    candidates.extend((ROOT / "docs").rglob("*"))
    candidates.extend((ROOT / "deploy").rglob("*"))
    found = 0
    for path in candidates:
        if not path.is_file():
            continue
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if "x-release-please-version" not in line:
                continue
            found += 1
            if version not in line:
                failures.append(
                    f"{path.relative_to(ROOT)}:{number}: release marker does not contain {version}"
                )
    if found < 7:
        failures.append(f"expected at least 7 release-managed documentation markers, found {found}")
    return failures


def check_stale_claims(files: list[Path]) -> list[str]:
    forbidden = {
        "open dev mode": "authentication has no open/keyless mode",
        "future Tier 1 `jti` denylist": "precise JTI revocation has shipped",
        "Precise per-token revocation remains future": "precise revocation has shipped",
        "the upload itself is untested": "crates.io publication has shipped",
        "no version has been published yet": "crates.io publication has shipped",
        "`POST /v1/responses`), `400 unsupported_wire`": "Responses is implemented",
    }
    failures: list[str] = []
    for source in files + [ROOT / "axond.example.toml", ROOT / ".agents/skills/testing-axond/SKILL.md"]:
        text = source.read_text(encoding="utf-8")
        for phrase, reason in forbidden.items():
            if phrase in text:
                failures.append(
                    f"{source.relative_to(ROOT)}: stale phrase {phrase!r} ({reason})"
                )
    return failures


def check_route_contract() -> list[str]:
    source = (ROOT / "crates/gateway/src/routes.rs").read_text(encoding="utf-8")
    documented = (ROOT / "docs/compatibility.md").read_text(encoding="utf-8")
    registered = set(re.findall(r'path:\s*"(/[^"]+)"', source))
    return [
        f"docs/compatibility.md: registered route {route!r} is not documented"
        for route in sorted(registered)
        if route not in documented
    ]


def check_front_door_size() -> list[str]:
    failures: list[str] = []
    for relative, limit in (("README.md", 260), ("docs/deployment.md", 220)):
        count = len((ROOT / relative).read_text(encoding="utf-8").splitlines())
        if count > limit:
            failures.append(f"{relative}: {count} lines exceeds front-door limit {limit}")
    return failures


def main() -> int:
    files = markdown_files()
    failures = []
    failures.extend(check_relative_links(files))
    failures.extend(check_release_markers())
    failures.extend(check_stale_claims(files))
    failures.extend(check_route_contract())
    failures.extend(check_front_door_size())
    if failures:
        for failure in failures:
            print(f"documentation check failed: {failure}", file=sys.stderr)
        return 1
    print(f"documentation checks passed ({len(files)} Markdown files)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
