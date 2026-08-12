#!/usr/bin/env python3
"""Fast, dependency-free documentation drift checks."""

from __future__ import annotations

import re
import sys
from pathlib import Path
from urllib.parse import unquote


ROOT = Path(__file__).resolve().parent.parent


def markdown_files() -> list[Path]:
    files = [
        ROOT / "README.md",
        ROOT / "RELEASE.md",
        ROOT / "CONTRIBUTING.md",
        ROOT / "SECURITY.md",
    ]
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
        "the stateful surface is not implemented yet": "stateful mode parsing and bootstrap validation have shipped",
        "no stateful mode, `/admin/v1` route, or durable schema ships yet": "stateful bootstrap configuration ships; the control plane does not",
    }
    failures: list[str] = []
    for source in files + [
        ROOT / "axond.example.toml",
        ROOT / "axond.stateful.example.toml",
        ROOT / ".agents/skills/testing-axond/SKILL.md",
    ]:
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


def check_operating_mode_contract() -> list[str]:
    """Every stateful bootstrap key the parser accepts is documented and shown.

    Bootstrap is the only surface a stateful operator can edit, so an
    undocumented key there is worse than an undocumented inference key: there is
    no `/admin/v1` alternative for it.
    """
    source = (ROOT / "crates/gateway/src/config.rs").read_text(encoding="utf-8")
    reference = (ROOT / "docs/configuration.md").read_text(encoding="utf-8")
    example = (ROOT / "axond.stateful.example.toml").read_text(encoding="utf-8")
    failures: list[str] = []
    sections = (
        ("[control_plane]", "ControlPlane"),
        ("[secret_store]", "SecretStore"),
        ("[[admin_breakglass]]", "AdminBreakglass"),
    )
    for section, struct in sections:
        body = re.search(rf"pub struct {struct} \{{(.*?)\n\}}", source, re.DOTALL)
        if body is None:
            failures.append(f"crates/gateway/src/config.rs: {struct} not found")
            continue
        for field in re.findall(r"\n    pub (\w+):", body.group(1)):
            if f"`{field}`" not in reference:
                failures.append(
                    f"docs/configuration.md: {section} key {field!r} is not documented"
                )
        for path, text in (("docs/configuration.md", reference), ("axond.stateful.example.toml", example)):
            if section not in text:
                failures.append(f"{path}: stateful bootstrap section {section} is missing")
    for mode in ("stateless", "stateful"):
        if f'mode = "{mode}"' not in reference:
            failures.append(f"docs/configuration.md: `mode = \"{mode}\"` is not documented")
    return failures


def check_msrv_documented() -> list[str]:
    """The published MSRV policy names the versions the manifests actually pin.

    `ops/msrv-gate.sh` keeps the manifests, the Dockerfile, and the pinned
    toolchain consistent with each other; this keeps the operator-facing
    statement of the policy consistent with them.
    """
    cargo = (ROOT / "Cargo.toml").read_text(encoding="utf-8")
    msrv = re.search(r"^rust-version\s*=\s*\"([^\"]+)\"", cargo, re.MULTILINE)
    if msrv is None:
        return ["Cargo.toml: [workspace.package] rust-version is not declared"]
    pinned = re.search(
        r"^channel\s*=\s*\"([^\"]+)\"",
        (ROOT / "rust-toolchain.toml").read_text(encoding="utf-8"),
        re.MULTILINE,
    )
    if pinned is None:
        return ["rust-toolchain.toml: no [toolchain] channel is pinned"]

    heading = "### The Rust version floor (MSRV)"
    document = (ROOT / "docs/compatibility.md").read_text(encoding="utf-8")
    if heading not in document:
        return [f"docs/compatibility.md: {heading!r} section is missing"]
    start = document.index(heading)
    end = document.find("\n### ", start + len(heading))
    section = document[start : end if end != -1 else len(document)]
    return [
        f"docs/compatibility.md: the MSRV section does not mention {value!r}"
        for value in (msrv.group(1), pinned.group(1))
        if value not in section
    ]


def check_front_door_size() -> list[str]:
    failures: list[str] = []
    for relative, limit in (("README.md", 260), ("docs/deployment.md", 220)):
        count = len((ROOT / relative).read_text(encoding="utf-8").splitlines())
        if count > limit:
            failures.append(f"{relative}: {count} lines exceeds front-door limit {limit}")
    return failures


def check_review_trigger_tests() -> list[str]:
    """Every test the threat-model trigger page names still exists.

    The page is only useful if its named tests are the *real* floor, and a
    documented test name rots silently: a rename in the crates leaves the page
    pointing at coverage nobody has. Every backticked lowercase identifier with
    three or more underscores in it is treated as a test function and must be
    declared somewhere under `crates/`. Shorter identifiers are configuration
    keys and field names (`allow_platform_fallback`, `credential_source`), which
    other checks and the configuration reference already cover.
    """
    page = ROOT / "docs/security/threat-model-review.md"
    text = page.read_text(encoding="utf-8")
    declared = set()
    for source in (ROOT / "crates").rglob("*.rs"):
        declared.update(
            re.findall(r"\bfn\s+([a-z0-9_]+)", source.read_text(encoding="utf-8"))
        )
    named = {
        candidate
        for candidate in re.findall(r"`([a-z0-9_]+)`", text)
        if candidate.count("_") >= 3
    }
    failures = [
        f"docs/security/threat-model-review.md: named test {name!r} does not exist under crates/"
        for name in sorted(named - declared)
    ]
    if len(named) < 40:
        failures.append(
            "docs/security/threat-model-review.md: only "
            f"{len(named)} named tests remain; the triggers are meant to name the "
            "existing floor for each area"
        )
    return failures


def main() -> int:
    files = markdown_files()
    failures = []
    failures.extend(check_relative_links(files))
    failures.extend(check_release_markers())
    failures.extend(check_operating_mode_contract())
    failures.extend(check_stale_claims(files))
    failures.extend(check_route_contract())
    failures.extend(check_msrv_documented())
    failures.extend(check_review_trigger_tests())
    failures.extend(check_front_door_size())
    if failures:
        for failure in failures:
            print(f"documentation check failed: {failure}", file=sys.stderr)
        return 1
    print(f"documentation checks passed ({len(files)} Markdown files)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
