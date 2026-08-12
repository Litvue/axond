#!/usr/bin/env python3
"""Deterministic release-artifact configuration checks.

The release matrix is only exercised for real at a tag, when a mistake is
already published and a `latest` tag or a dropped target cannot be taken back.
So the shape of the release configuration is asserted here on every change,
without a YAML dependency: the published binary targets, the published image
platforms, the archive extension per target, the integrity and smoke gates each
lane must carry, the release-success aggregate, and the absence of any
`latest`-tag requirement.

The expectations are written out rather than derived from the workflow, so
removing a supported target or dropping a gate fails this check instead of
silently shrinking the release.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
WORKFLOW = ROOT / ".github/workflows/release-please.yml"

# target -> archive extension published for it.
BINARY_TARGETS = {
    "x86_64-unknown-linux-gnu": "tar.gz",
    "x86_64-unknown-linux-musl": "tar.gz",
    "aarch64-unknown-linux-gnu": "tar.gz",
    "aarch64-unknown-linux-musl": "tar.gz",
    "aarch64-apple-darwin": "tar.gz",
    "x86_64-pc-windows-msvc": "zip",
}
UNIX_INSTALLER_TARGETS = {
    target for target in BINARY_TARGETS if not target.endswith("-pc-windows-msvc")
}
WINDOWS_INSTALLER_TARGET = "x86_64-pc-windows-msvc"
IMAGE_PLATFORMS = {"linux/amd64", "linux/arm64"}
# The last release published as a single `linux/amd64` image — the release that
# was current when ARM support landed, so it must be bumped alongside a rebase
# onto a newer amd64-only release. The Compose
# quickstart pins a release tag, so while that tag is at or below this version it
# must keep an explicit `linux/amd64` default: dropping the pin would leave ARM
# hosts unable to pull an image that has no ARM child yet. The moment
# release-please bumps the pinned tag past it, the fallback is wrong and this
# check demands the unpinned form, so the transition cannot be forgotten.
LAST_AMD64_ONLY_VERSION = (0, 3, 15)
# Paths the repair dispatch needs in the tag it rebuilds. A tag lacking any of
# them cannot publish or verify the image index, so the preflight refuses.
REPAIR_REQUIRED_PATHS = (
    "Dockerfile",
    "ops/docker-smoke.sh",
    "ops/publish-image-index.sh",
    "ops/verify-image-evidence.sh",
)
AMD64_FALLBACK_PLATFORM = "platform: ${AXOND_PLATFORM-linux/amd64}"
NATIVE_PLATFORM = "platform: ${AXOND_PLATFORM-}"
# Documentation that must name every target and platform an operator can pick.
PLATFORM_DOCS = ("docs/installation.md", "docs/compatibility.md")


def workflow_text() -> str:
    return WORKFLOW.read_text(encoding="utf-8")


def job_block(text: str, job: str) -> str | None:
    """The body of one top-level workflow job, by name."""
    match = re.search(
        rf"\n  {re.escape(job)}:\n(.*?)(?=\n  [A-Za-z0-9_-]+:\n|\Z)", text, re.DOTALL
    )
    return None if match is None else match.group(1)


def check_binary_matrix(text: str) -> list[str]:
    failures: list[str] = []
    block = job_block(text, "release-binaries")
    if block is None:
        return ["release-please.yml: release-binaries job not found"]
    entries = re.findall(
        r"- os: (\S+)\n\s+target: (\S+)\n\s+archive: (\S+)", block
    )
    found = {target: archive for _os, target, archive in entries}
    if len(entries) != len(found):
        failures.append("release-please.yml: duplicate target in the binary matrix")
    for target, archive in BINARY_TARGETS.items():
        if target not in found:
            failures.append(f"release-please.yml: binary matrix is missing {target}")
        elif found[target] != archive:
            failures.append(
                f"release-please.yml: {target} publishes .{found[target]}, expected .{archive}"
            )
    for target in sorted(set(found) - set(BINARY_TARGETS)):
        failures.append(
            f"ops/check-release-config.py: undeclared binary target {target}; document it first"
        )
    # An aarch64 Linux archive built on an x86_64 runner would be cross-compiled
    # and could not be booted by the smoke gate below.
    for os_label, target, _archive in entries:
        if target.startswith("aarch64-unknown-linux") and not os_label.endswith("-arm"):
            failures.append(
                f"release-please.yml: {target} builds on {os_label}, not an arm64 runner"
            )
    return failures


def check_binary_gates(text: str) -> list[str]:
    block = job_block(text, "release-binaries")
    if block is None:
        return []
    required = {
        "checksum sidecar": "shasum -a 256",
        "windows checksum sidecar": "Get-FileHash",
        "SBOM": "anchore/sbom-action@v0",
        "provenance attestation": "Attest binary provenance",
        "SBOM attestation": "Attest binary SBOM",
        "static-link assertion": "Assert the musl binary is statically linked",
        "boot smoke": "ops/tier0-gate.sh",
        # The release must not depend on namespace creation being permitted on a
        # hosted runner: the smoke gate degrades there instead of failing, while
        # CI keeps proving the hermetic guarantee on every change.
        "sandbox-restriction tolerance": 'AXOND_TIER0_ALLOW_NO_NETNS: "1"',
    }
    failures = [
        f"release-please.yml: release-binaries lacks a {label} gate ({needle!r})"
        for label, needle in required.items()
        if needle not in block
    ]
    ci = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    if "AXOND_TIER0_ALLOW_NO_NETNS" in ci:
        failures.append(
            "ci.yml: the CI lanes must not tolerate a missing namespace; the "
            "hermetic Tier 0 guarantee is what they exist to prove"
        )
    return failures


def check_image_matrix(text: str) -> list[str]:
    failures: list[str] = []
    for job in ("release-image", "release-image-index-smoke"):
        block = job_block(text, job)
        if block is None:
            failures.append(f"release-please.yml: {job} job not found")
            continue
        entries = re.findall(r"- os: (\S+)\n\s+platform: (\S+)\n\s+arch: (\S+)", block)
        platforms = {platform for _os, platform, _arch in entries}
        if platforms != IMAGE_PLATFORMS:
            failures.append(
                f"release-please.yml: {job} publishes {sorted(platforms)}, "
                f"expected {sorted(IMAGE_PLATFORMS)}"
            )
        for os_label, platform, arch in entries:
            if platform != f"linux/{arch}":
                failures.append(
                    f"release-please.yml: {job} pairs platform {platform} with arch {arch}"
                )
            if arch == "arm64" and not os_label.endswith("-arm"):
                failures.append(
                    f"release-please.yml: {job} builds arm64 on {os_label}, not an arm64 runner"
                )
    # The image builds every published platform from one Dockerfile, so each
    # architecture must map to a Rust target there too.
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    if "ARG TARGETARCH" not in dockerfile:
        failures.append(
            "Dockerfile: TARGETARCH is not declared; the build is not platform-aware"
        )
    for platform in sorted(IMAGE_PLATFORMS):
        arch = platform.split("/", 1)[1]
        mapping = rf"\n\s+{re.escape(arch)}\) rust_target=\S+-unknown-linux-musl ;;"
        if not re.search(mapping, dockerfile):
            failures.append(f"Dockerfile: {arch} does not select a static musl Rust target")
    # The index script is the one place the platform list is applied at runtime.
    script = (ROOT / "ops/publish-image-index.sh").read_text(encoding="utf-8")
    declared = re.search(r"PLATFORMS=\(([^)]*)\)", script)
    if declared is None:
        failures.append("ops/publish-image-index.sh: PLATFORMS list not found")
    elif set(declared.group(1).split()) != IMAGE_PLATFORMS:
        failures.append(
            "ops/publish-image-index.sh: PLATFORMS "
            f"{sorted(declared.group(1).split())} does not match {sorted(IMAGE_PLATFORMS)}"
        )
    # Descriptors without a platform must be classified, not skipped: an ignored
    # descriptor is an unreviewed manifest inside the deployed reference.
    for label, needle in {
        "attestation descriptors": "attestation-manifest",
        "rejection of unclassified descriptors": "is not an attestation manifest",
        "attestation subject check": "vnd.docker.reference.digest",
    }.items():
        if needle not in script:
            failures.append(
                f"ops/publish-image-index.sh: no explicit handling of {label} ({needle!r})"
            )
    return failures


def check_image_gates(text: str) -> list[str]:
    failures: list[str] = []
    per_platform = job_block(text, "release-image")
    index = job_block(text, "release-image-index")
    smoke = job_block(text, "release-image-index-smoke")
    if per_platform is None or index is None or smoke is None:
        return ["release-please.yml: an OCI image job is missing"]
    for label, needle in {
        "published-image smoke": "ops/docker-smoke.sh",
        "SBOM": "anchore/sbom-action@v0",
        "provenance attestation": "Attest image provenance",
        "SBOM attestation": "Attest image SBOM",
        "keyless signature": "cosign sign --yes",
        "evidence verification": "ops/verify-image-evidence.sh",
        "per-architecture digest asset": ".digest",
    }.items():
        if needle not in per_platform:
            failures.append(
                f"release-please.yml: release-image lacks a {label} gate ({needle!r})"
            )
    for label, needle in {
        "index assembly and platform assertion": "ops/publish-image-index.sh",
        "keyless signature": "cosign sign --yes",
        "provenance attestation": "Attest index provenance",
        "evidence verification": "ops/verify-image-evidence.sh",
        "index digest asset": '"axond-image-${RELEASE_VERSION}.digest"',
    }.items():
        if needle not in index:
            failures.append(
                f"release-please.yml: release-image-index lacks a {label} gate ({needle!r})"
            )
    if "ops/docker-smoke.sh" not in smoke:
        failures.append(
            "release-please.yml: release-image-index-smoke does not smoke the index digest"
        )
    if "docker image inspect" not in smoke:
        failures.append(
            "release-please.yml: release-image-index-smoke does not assert the resolved architecture"
        )
    # SBOM attestations are per-architecture by decision (ADR 0004): an index has
    # no filesystem, so an index SBOM would be a child's document under a subject
    # it does not describe. Publishing one anyway is allowed, but only together
    # with the documentation that tells operators where SBOMs live.
    if "anchore/sbom-action" in index:
        compatibility = (ROOT / "docs/compatibility.md").read_text(encoding="utf-8")
        if "SBOM attestations are per-architecture" in compatibility:
            failures.append(
                "release-please.yml: release-image-index now generates an SBOM, but "
                "docs/compatibility.md and ADR 0004 still say SBOMs are "
                "per-architecture only; update both together"
            )
    elif any(claim in index for claim in ("Attest index SBOM", "SBOM_PATH", ".spdx.json")):
        failures.append(
            "release-please.yml: release-image-index attests or names an SBOM it "
            "does not generate; the index carries signature and provenance only"
        )
    return failures


def check_release_success(text: str) -> list[str]:
    block = job_block(text, "release-success")
    if block is None:
        return ["release-please.yml: release-success job not found"]
    failures: list[str] = []
    for lane in (
        "release-binaries",
        "release-image",
        "release-image-index",
        "release-image-index-smoke",
    ):
        if f"- {lane}\n" not in block:
            failures.append(f"release-please.yml: release-success does not need {lane}")
        if f"needs['{lane}'].result }}}}\" = success" not in block:
            failures.append(f"release-please.yml: release-success does not require {lane}")
    return failures


def check_compose_platform(notes: list[str]) -> list[str]:
    """The quickstart's platform default must match what its pinned tag can serve.

    Only one direction can be an error. While the pinned tag is amd64-only,
    removing the fallback breaks ARM hosts outright, so its presence is required.
    Once the tag is multi-architecture the fallback is merely suboptimal — ARM
    keeps working, emulated — and it cannot be a failure: release-please bumps
    that tag inside its own generated release pull request and never touches the
    `platform:` line, so failing here would fail the release PR with no edit that
    could have landed beforehand. It is reported as a note instead, and
    docs/maintainers/releasing.md carries it as a post-release step.
    """
    failures: list[str] = []
    compose = (ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    pinned = re.search(r"image: \$\{AXOND_IMAGE:-ghcr\.io/litvue/axond:([0-9.]+)\}", compose)
    if pinned is None:
        return ["docker-compose.yml: could not read the pinned quickstart image tag"]
    version = tuple(int(part) for part in pinned.group(1).split("."))
    if version > LAST_AMD64_ONLY_VERSION:
        if AMD64_FALLBACK_PLATFORM in compose:
            notes.append(
                f"docker-compose.yml: the pinned tag {pinned.group(1)} publishes a "
                "multi-architecture image, so the amd64 fallback now only forces "
                f"emulation on ARM hosts; switch to `{NATIVE_PLATFORM}` and bump "
                f"LAST_AMD64_ONLY_VERSION to {pinned.group(1)}"
            )
        elif NATIVE_PLATFORM not in compose:
            failures.append(
                "docker-compose.yml: the quickstart platform default is neither "
                f"`{AMD64_FALLBACK_PLATFORM}` nor `{NATIVE_PLATFORM}`"
            )
    elif AMD64_FALLBACK_PLATFORM not in compose:
        failures.append(
            f"docker-compose.yml: the pinned tag {pinned.group(1)} is "
            "amd64-only, so the quickstart must keep "
            f"`{AMD64_FALLBACK_PLATFORM}` for ARM hosts to run it at all"
        )
    # A source build is never limited by the last release's platforms.
    build_overlay = (ROOT / "docker-compose.build.yml").read_text(encoding="utf-8")
    if NATIVE_PLATFORM not in build_overlay:
        failures.append(
            f"docker-compose.build.yml: source builds must use `{NATIVE_PLATFORM}`, "
            "not the quickstart's amd64 fallback"
        )
    compose_docs = (ROOT / "docs/deployment/docker-compose.md").read_text(encoding="utf-8")
    if "AXOND_PLATFORM" not in compose_docs:
        failures.append(
            "docs/deployment/docker-compose.md: AXOND_PLATFORM is not documented"
        )
    return failures


def check_no_latest_tag(text: str) -> list[str]:
    failures: list[str] = []
    if "flavor: latest=false" not in text:
        failures.append("release-please.yml: OCI metadata does not disable the latest tag")
    if re.search(r"type=raw,value=latest|:latest\b", text):
        failures.append("release-please.yml: a latest tag is published")
    candidates = [
        ROOT / "docker-compose.yml",
        ROOT / "docker-compose.build.yml",
        ROOT / "docker-compose.stateful.yml",
        ROOT / "install.sh",
        ROOT / "install.ps1",
    ]
    candidates.extend(sorted((ROOT / "deploy").rglob("*.yaml")))
    candidates.extend(sorted((ROOT / "deploy").rglob("*")))
    for path in candidates:
        if not path.is_file():
            continue
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if re.search(r"axond:latest|/axond:\s*latest", line):
                failures.append(
                    f"{path.relative_to(ROOT)}:{number}: deployment example requires a latest tag"
                )
    return failures


def check_documented_matrix() -> list[str]:
    failures: list[str] = []
    docs = {
        relative: (ROOT / relative).read_text(encoding="utf-8") for relative in PLATFORM_DOCS
    }
    for target in BINARY_TARGETS:
        if not any(target in text for text in docs.values()):
            failures.append(
                f"{' / '.join(PLATFORM_DOCS)}: release target {target} is not documented"
            )
    container = (ROOT / "docs/deployment/container.md").read_text(encoding="utf-8")
    for platform in sorted(IMAGE_PLATFORMS):
        for relative, text in list(docs.items()) + [("docs/deployment/container.md", container)]:
            if platform not in text:
                failures.append(f"{relative}: image platform {platform} is not documented")
    installer = (ROOT / "install.sh").read_text(encoding="utf-8")
    allowlist = re.search(r"\ncase \"\$target\" in\n(.*?)\n  \*\)", installer, re.DOTALL)
    if allowlist is None:
        failures.append("install.sh: target allowlist not found")
    else:
        allowed = set(re.findall(r"[A-Za-z0-9_.]+-[A-Za-z0-9_.-]+", allowlist.group(1)))
        for target in sorted(UNIX_INSTALLER_TARGETS):
            if target not in allowed:
                failures.append(f"install.sh: prebuilt target {target} is not accepted")
    if "Linux/aarch64|Linux/arm64" not in installer:
        failures.append("install.sh: Linux arm64 is not detected by uname")
    windows_installer = (ROOT / "install.ps1").read_text(encoding="utf-8")
    if WINDOWS_INSTALLER_TARGET not in windows_installer:
        failures.append(f"install.ps1: {WINDOWS_INSTALLER_TARGET} is not accepted")
    return failures


def check_repair_preflight(text: str) -> list[str]:
    """A tag that cannot be repaired from `main` must say so, and say what to do.

    The image lanes publish and verify an index, so a tag whose tree predates
    those scripts cannot be rebuilt by the current workflow. That is a deliberate
    explicit failure rather than a silent partial release — but an operator
    reading the log has to be told the remediation, and the runbook has to name
    the same prerequisites.
    """
    block = job_block(text, "release-metadata")
    if block is None:
        return ["release-please.yml: release-metadata job not found"]
    failures: list[str] = []
    for required in REPAIR_REQUIRED_PATHS:
        if required not in block:
            failures.append(
                f"release-please.yml: the repair preflight does not require {required}"
            )
    if "Remediation: dispatch this workflow from" not in block:
        failures.append(
            "release-please.yml: the repair preflight fails without naming the "
            "remediation; an operator should not have to find the runbook"
        )
    runbook = (ROOT / "docs/maintainers/releasing.md").read_text(encoding="utf-8")
    for required in REPAIR_REQUIRED_PATHS:
        if required not in runbook:
            failures.append(
                f"docs/maintainers/releasing.md: repair prerequisite {required} is "
                "not documented"
            )
    return failures


def main() -> int:
    text = workflow_text()
    notes: list[str] = []
    failures: list[str] = []
    failures.extend(check_binary_matrix(text))
    failures.extend(check_binary_gates(text))
    failures.extend(check_image_matrix(text))
    failures.extend(check_image_gates(text))
    failures.extend(check_release_success(text))
    failures.extend(check_compose_platform(notes))
    failures.extend(check_no_latest_tag(text))
    failures.extend(check_documented_matrix())
    failures.extend(check_repair_preflight(text))
    for note in notes:
        print(f"release configuration note: {note}", file=sys.stderr)
    if failures:
        for failure in failures:
            print(f"release configuration check failed: {failure}", file=sys.stderr)
        return 1
    print(
        "release configuration checks passed "
        f"({len(BINARY_TARGETS)} binary targets, {len(IMAGE_PLATFORMS)} image platforms)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
