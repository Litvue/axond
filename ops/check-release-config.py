#!/usr/bin/env python3
"""Deterministic release-artifact configuration checks.

Usage:
    ops/check-release-config.py              # every check against the committed tree
    ops/check-release-config.py --self-test  # decision and release-order mutations

The release matrix is only exercised for real at a tag, when a mistake is
already published and a `latest` tag or a dropped target cannot be taken back.
So the shape of the release configuration is asserted here on every change,
without a YAML dependency: the published binary targets, the published image
platforms, the archive extension per target, the integrity and smoke gates each
lane must carry, fail-closed commit-CI ordering before every publication, the
release-success aggregate, and the absence of any `latest`-tag requirement.

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
# was current when ARM support landed. It is a fixed historical fact, not a
# pointer at the current release: it only moves if the quickstart is ever pinned
# back onto a newer amd64-only release. The Compose quickstart pins a release
# tag, so while that tag is at or below this version it must keep an explicit
# `linux/amd64` default: dropping the pin would leave ARM hosts unable to pull an
# image that has no ARM child yet. The moment release-please bumps the pinned tag
# past it, the fallback is wrong and this check asks for the unpinned form, so the
# transition cannot be forgotten.
LAST_AMD64_ONLY_VERSION = (0, 3, 17)
# Paths the repair dispatch needs in the tag it rebuilds. A tag lacking any of
# them cannot publish or verify the image index, so the preflight refuses.
REPAIR_REQUIRED_PATHS = (
    "Dockerfile",
    "ops/docker-smoke.sh",
    "ops/publish-image-index.sh",
    "ops/verify-image-evidence.sh",
)
INDEX_TRANSITION_PHRASE = "from the next release onward"
# The operator-facing pages that promise the index; each must date that promise
# while the newest published release is still amd64-only.
INDEX_TRANSITION_PAGES = (
    "README.md",
    "docs/installation.md",
    "docs/compatibility.md",
    "docs/deployment.md",
    "docs/deployment/container.md",
    "docs/deployment/docker-compose.md",
)
AMD64_FALLBACK_PLATFORM = "platform: ${AXOND_PLATFORM-linux/amd64}"
NATIVE_PLATFORM = "platform: ${AXOND_PLATFORM-}"
# Documentation that must name every target and platform an operator can pick.
PLATFORM_DOCS = ("docs/installation.md", "docs/compatibility.md")


def workflow_text() -> str:
    return WORKFLOW.read_text(encoding="utf-8")


def format_version(version: tuple[int, ...]) -> str:
    return ".".join(str(part) for part in version)


def platform_default(
    version: tuple[int, ...], last_amd64_only: tuple[int, ...] = LAST_AMD64_ONLY_VERSION
) -> str:
    """The quickstart `platform:` default a pinned release tag calls for.

    A tag newer than the last amd64-only release resolves a native child on both
    architectures, so it wants no pin at all; anything at or below that release
    has no ARM child and must keep the explicit `linux/amd64` fallback.

    So `last_amd64_only` only ever names a release *older* than the pinned tag:
    setting it to the pinned tag would ask the quickstart for the fallback again,
    which is exactly what the multi-architecture release removed.
    """
    return NATIVE_PLATFORM if version > last_amd64_only else AMD64_FALLBACK_PLATFORM


def job_block(text: str, job: str) -> str | None:
    """The body of one top-level workflow job, by name."""
    match = re.search(
        rf"\n  {re.escape(job)}:\n(.*?)(?=\n  [A-Za-z0-9_-]+:\n|\Z)", text, re.DOTALL
    )
    return None if match is None else match.group(1)


def job_configuration(block: str) -> str:
    """The declarative part of a job before its executable steps."""
    return block.partition("\n    steps:\n")[0]


def workflow_jobs(text: str) -> dict[str, str]:
    """All top-level jobs, preserving their complete bodies."""
    names = re.findall(r"^  ([A-Za-z0-9_-]+):$", text, re.MULTILINE)
    return {
        name: block
        for name in names
        if (block := job_block(text, name)) is not None
    }


def check_ci_success_poll(block: str, commit_expression: str, label: str) -> list[str]:
    """Require an exact-commit, success-only, fail-closed CI Success poll."""
    failures: list[str] = []
    required = {
        "checks: read permission": "      checks: read\n",
        "exact commit identity": f"COMMIT_SHA: {commit_expression}",
        "strict shell failure handling": "set -euo pipefail",
        "exact CI aggregate name": 'select(.name == "CI Success")',
        "completed-status distinction": 'elif .status != "completed" then "pending"',
        "missing-check distinction": 'if . == null then "missing"',
        "success-only terminal arm": '              success)\n',
        "retry-only pending/missing arm": '              pending | missing)\n',
        "immediate non-success failure arm": '              *)\n',
        "bounded timeout refusal": "within 30 minutes",
    }
    for description, needle in required.items():
        if needle not in block:
            failures.append(f"release-please.yml: {label} lacks {description}")

    retry = re.search(
        r"\n\s+pending \| missing\)\n(.*?)(?=\n\s+\*\))", block, re.DOTALL
    )
    if retry is None or "sleep 30" not in retry.group(1):
        failures.append(
            f"release-please.yml: {label} does not retry only missing or pending checks"
        )
    elif any(
        outcome in retry.group(0)
        for outcome in ("neutral", "skipped", "cancelled", "completed")
    ):
        failures.append(
            f"release-please.yml: {label} retries a completed non-success conclusion"
        )
    success = re.search(
        r"\n\s+success\)\n(.*?)(?=\n\s+pending \| missing\))", block, re.DOTALL
    )
    if success is None or "exit 0" not in success.group(1):
        failures.append(
            f"release-please.yml: {label} does not terminate successfully on success"
        )
    refusal = re.search(
        r"\n\s+\*\)\n(.*?)(?=\n\s+;;)", block, re.DOTALL
    )
    if refusal is None or "exit 1" not in refusal.group(1) or "sleep" in refusal.group(1):
        failures.append(
            f"release-please.yml: {label} does not immediately fail a non-success conclusion"
        )
    if not re.search(
        r"done\n\s+echo \"CI Success did not conclude .* within 30 minutes; refusing .*\" >&2\n\s+exit 1",
        block,
    ):
        failures.append(
            f"release-please.yml: {label} does not fail closed after its polling timeout"
        )

    # `set -e` makes an API error in the unguarded command substitution fatal.
    # An `||` fallback would turn an authorization/outage failure into a retry
    # and eventually obscure the actual refusal reason.
    api_line = re.search(r"^\s+gh api .+$", block, re.MULTILINE)
    if api_line is None:
        failures.append(f"release-please.yml: {label} does not query check runs")
    elif "||" in api_line.group(0):
        failures.append(
            f"release-please.yml: {label} masks a check-runs API failure"
        )
    return failures


def check_release_ordering(text: str) -> list[str]:
    """Prove release maintenance and publication follow exact-commit CI success."""
    failures: list[str] = []
    jobs = workflow_jobs(text)
    main_gate = jobs.get("main-ci-success")
    release_gate = jobs.get("release-ci-success")
    release_please = jobs.get("release-please")

    if main_gate is None:
        failures.append("release-please.yml: main-ci-success job not found")
    else:
        failures.extend(
            check_ci_success_poll(main_gate, "${{ github.sha }}", "main-ci-success")
        )
        config = job_configuration(main_gate)
        if "if: github.event_name == 'push'" not in config:
            failures.append(
                "release-please.yml: main-ci-success is not bound to main pushes"
            )

    if release_gate is None:
        failures.append("release-please.yml: release-ci-success job not found")
    else:
        failures.extend(
            check_ci_success_poll(
                release_gate,
                "${{ needs['release-metadata'].outputs.commit_sha }}",
                "release-ci-success",
            )
        )
        config = job_configuration(release_gate)
        for needle, description in (
            ("needs: release-metadata", "direct release-metadata dependency"),
            (
                "needs['release-metadata'].result == 'success'",
                "successful metadata condition",
            ),
            (
                "needs['release-metadata'].outputs.release_created == 'true'",
                "release-created condition",
            ),
        ):
            if needle not in config:
                failures.append(
                    f"release-please.yml: release-ci-success lacks {description}"
                )

    if release_please is None:
        failures.append("release-please.yml: release-please job not found")
    else:
        config = job_configuration(release_please)
        for needle, description in (
            ("needs: main-ci-success", "direct main-ci-success dependency"),
            ("github.event_name == 'push'", "push maintenance condition"),
            (
                "needs['main-ci-success'].result == 'success'",
                "successful main-CI condition",
            ),
        ):
            if needle not in config:
                failures.append(
                    f"release-please.yml: release-please lacks {description}"
                )
        if "release_created" in config.partition("    concurrency:")[0]:
            failures.append(
                "release-please.yml: release-please maintenance is limited to a "
                "created release instead of every green main push"
            )

    publishing_jobs = (
        "release-binaries",
        "release-image",
        "release-image-index",
        "release-image-index-promote",
        "release-crates",
    )
    for job in publishing_jobs:
        block = jobs.get(job)
        if block is None:
            failures.append(f"release-please.yml: {job} job not found")
            continue
        config = job_configuration(block)
        if "      - release-ci-success\n" not in config:
            failures.append(
                f"release-please.yml: {job} does not directly need release-ci-success"
            )
        if "needs['release-ci-success'].result == 'success'" not in config:
            failures.append(
                f"release-please.yml: {job} does not require successful release-ci-success"
            )

    aggregate = jobs.get("release-success")
    if aggregate is None:
        failures.append("release-please.yml: release-success job not found")
    else:
        config = job_configuration(aggregate)
        if "      - release-ci-success\n" not in config:
            failures.append(
                "release-please.yml: release-success does not directly need release-ci-success"
            )
        if 'test "${{ needs[\'release-ci-success\'].result }}" = success' not in aggregate:
            failures.append(
                "release-please.yml: release-success does not aggregate release-ci-success"
            )

    crates = jobs.get("release-crates")
    if crates is not None:
        if "checks: read" in crates:
            failures.append(
                "release-please.yml: release-crates retains the obsolete crates-only checks permission"
            )
        if "check-runs?" in crates or "Require CI to be green" in crates:
            failures.append(
                "release-please.yml: release-crates retains the obsolete crates-only CI poll"
            )

    publication_primitives = {
        "release-please action": "googleapis/release-please-action@",
        "GitHub release creation": "gh release create",
        "GitHub release upload": "gh release upload",
        "image build publication": "docker/build-push-action@",
        "image-index publication": "run: bash ops/publish-image-index.sh",
        "keyless signing": "cosign sign",
        "GitHub attestation": "actions/attest@",
        "registry attestation": "push-to-registry: true",
        "crates publication script": "run: bash ops/publish-crates.sh",
        "direct crates publication": "cargo publish",
    }
    for job, block in jobs.items():
        primitives = [
            description
            for description, needle in publication_primitives.items()
            if needle in block
        ]
        if not primitives:
            continue
        config = job_configuration(block)
        if job == "release-please":
            gated = (
                "needs: main-ci-success" in config
                and "needs['main-ci-success'].result == 'success'" in config
            )
        else:
            gated = (
                "      - release-ci-success\n" in config
                and "needs['release-ci-success'].result == 'success'" in config
            )
        if not gated:
            failures.append(
                f"release-please.yml: {job} contains ungated publication primitive(s): "
                + ", ".join(primitives)
            )
    return failures


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
        # Matched without a ref: the Actions are pinned to commit SHAs
        # (ADR 0034), so only the repository name is stable here.
        "SBOM": "anchore/sbom-action@",
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
    # Promotion must retag the smoked digest instead of reassembling the index
    # from the child tags: a tag, once applied, cannot be retracted by the
    # assertion that follows it. ops/check-index-promotion.sh proves the ordering
    # against a stubbed registry; this keeps the shape of it in place.
    if 'apply_tags "${IMAGE_NAME}@${index_digest}"' not in script:
        failures.append(
            "ops/publish-image-index.sh: promotion does not retag the expected "
            "index digest itself, so it can publish an operator-facing tag before "
            "the digest is checked"
        )
    # The mode must be explicit. Inferring promotion from a non-empty
    # EXPECT_INDEX_DIGEST means an empty job output silently becomes a staging run
    # that publishes the operator-facing tags before asserting anything.
    if '"${INDEX_MODE:?' not in script:
        failures.append(
            "ops/publish-image-index.sh: INDEX_MODE is not required, so the mode "
            "can be inferred from a possibly-empty variable"
        )
    if 'INDEX_MODE=promote requires EXPECT_INDEX_DIGEST' not in script:
        failures.append(
            "ops/publish-image-index.sh: promotion does not fail on an empty "
            "EXPECT_INDEX_DIGEST"
        )
    if "cannot apply the operator-facing tag" not in script:
        failures.append(
            "ops/publish-image-index.sh: staging does not refuse the "
            "operator-facing tags, so a mislabelled run can publish them before "
            "the index is asserted"
        )
    promotion = script.partition('if [[ "$INDEX_MODE" == promote ]]; then')[2]
    promotion = promotion.partition("\nelse\n")[0]
    if not promotion:
        failures.append("ops/publish-image-index.sh: no promotion branch found")
    else:
        assertion = promotion.find('assert_index_contents "$index_digest"')
        tagging = promotion.find("apply_tags ")
        if assertion < 0 or tagging < 0 or assertion > tagging:
            failures.append(
                "ops/publish-image-index.sh: promotion applies tags before "
                "asserting the index contents; a registry tag cannot be retracted "
                "by a later failure"
            )
    if "${child_refs[@]}" in promotion:
        failures.append(
            "ops/publish-image-index.sh: promotion reassembles the index from the "
            "child references; if a child tag moved since staging, the release "
            "tags would point at an index no smoke lane booted"
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
    promote = job_block(text, "release-image-index-promote")
    if per_platform is None or index is None or smoke is None or promote is None:
        return ["release-please.yml: an OCI image job is missing"]
    for label, needle in {
        "published-image smoke": "ops/docker-smoke.sh",
        "SBOM": "anchore/sbom-action@",
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
    if "ops/publish-image-index.sh" not in index:
        failures.append(
            "release-please.yml: release-image-index lacks the index assembly and "
            "platform assertion gate ('ops/publish-image-index.sh')"
        )
    # Staging must not publish an operator-facing reference, and must not sign,
    # attest, or advertise one: those belong after the smoke lanes, or the tags
    # the documentation names would exist before anything booted the index.
    staged_tags = re.search(r"INDEX_TAGS: (.+)", index)
    if staged_tags is None:
        failures.append(
            "release-please.yml: release-image-index does not say which tag it stages"
        )
    else:
        staged = staged_tags.group(1)
        if "version" in staged or staged.strip().startswith("${{"):
            failures.append(
                "release-please.yml: release-image-index stages the index under "
                f"{staged!r}, which is the operator-facing reference; stage it "
                "under a separate tag and promote after the smoke lanes"
            )
    for label, needle in {
        "keyless signature": "cosign sign --yes",
        "provenance attestation": "Attest index provenance",
        "release digest asset": '"axond-image-${RELEASE_VERSION}.digest"',
    }.items():
        if needle in index:
            failures.append(
                f"release-please.yml: release-image-index applies the {label} "
                f"({needle!r}) before the index is smoked; that belongs to "
                "release-image-index-promote"
            )
    # Each job must name its own mode: the script refuses to guess, and a job that
    # omits it fails at the tag rather than in the registry.
    if "INDEX_MODE: stage" not in index:
        failures.append(
            "release-please.yml: release-image-index does not set INDEX_MODE: stage"
        )
    for label, needle in {
        "explicit promotion mode": "INDEX_MODE: promote",
        "index retag from the smoked digest": "ops/publish-image-index.sh",
        "smoked-digest assertion": "EXPECT_INDEX_DIGEST",
        "keyless signature": "cosign sign --yes",
        "provenance attestation": "Attest index provenance",
        "evidence verification": "ops/verify-image-evidence.sh",
        "index digest asset": '"axond-image-${RELEASE_VERSION}.digest"',
    }.items():
        if needle not in promote:
            failures.append(
                "release-please.yml: release-image-index-promote lacks a "
                f"{label} gate ({needle!r})"
            )
    # Promotion is what makes the index operator-facing, so it may only run after
    # the native boot lanes succeeded.
    for needle in ("- release-image-index-smoke\n", "needs['release-image-index-smoke'].result == 'success'"):
        if needle not in promote:
            failures.append(
                "release-please.yml: release-image-index-promote does not depend on "
                f"release-image-index-smoke ({needle!r}); the release tags would be "
                "published before either architecture booted the index"
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
    index_lanes = index + promote
    if "anchore/sbom-action" in index_lanes:
        compatibility = (ROOT / "docs/compatibility.md").read_text(encoding="utf-8")
        if "SBOM attestations are per-architecture" in compatibility:
            failures.append(
                "release-please.yml: release-image-index now generates an SBOM, but "
                "docs/compatibility.md and ADR 0004 still say SBOMs are "
                "per-architecture only; update both together"
            )
    elif any(claim in index_lanes for claim in ("Attest index SBOM", "SBOM_PATH", ".spdx.json")):
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
        "release-image-index-promote",
    ):
        if f"- {lane}\n" not in block:
            failures.append(f"release-please.yml: release-success does not need {lane}")
        if f"needs['{lane}'].result }}}}\" = success" not in block:
            failures.append(f"release-please.yml: release-success does not require {lane}")
    return failures


def cosign_installer_steps(text: str) -> list[str]:
    """Every step that installs cosign, however the step happens to be written.

    A step's `uses:` is not necessarily its first line — `- name:` above it is
    just as valid — so the installer is found by its `uses:` line and the whole
    enclosing list item is returned. Keying on `- uses:` instead would read a
    named step as no step at all and pass a lane that pins nothing.
    """
    lines = text.splitlines()
    marks = [
        index
        for index, line in enumerate(lines)
        if re.match(r"\s*(- )?uses: sigstore/cosign-installer@", line)
    ]
    steps: list[str] = []
    for mark in marks:
        start = mark
        while start >= 0 and not lines[start].lstrip().startswith("- "):
            start -= 1
        if start < 0:
            continue
        indent = len(lines[start]) - len(lines[start].lstrip())
        end = start + 1
        while end < len(lines):
            line = lines[end]
            if not line.strip():
                end += 1
                continue
            depth = len(line) - len(line.lstrip())
            if depth < indent or (depth == indent and line.lstrip().startswith("- ")):
                break
            end += 1
        steps.append("\n".join(lines[start:end]))
    return steps


def check_cosign_pin(text: str) -> list[str]:
    """Signing must name the cosign binary, and stay on the format consumers verify.

    The installer's default cosign version moves with the installer's own major
    version, so an action bump silently changes what `cosign sign` writes.
    cosign 3 defaults to the protobuf bundle format stored as an OCI 1.1
    referring artifact; a consumer running cosign 2.x looks for the
    `sha256-<digest>.sig` tag and reports no signature at all. The release only
    verifies its own output with the version it just signed with, so that break
    reaches operators rather than CI. docs/installation.md hands out a bare
    `cosign verify`, so the format is part of the published contract: the binary
    is pinned here, and moving it off the 2.x line is a documented migration
    rather than a dependency bump.
    """
    failures: list[str] = []
    steps = cosign_installer_steps(text)
    if not steps:
        return ["release-please.yml: no cosign-installer step found; the release must sign"]
    for body in steps:
        pin = re.search(r"cosign-release:\s*(\S+)", body)
        if pin is None:
            failures.append(
                "release-please.yml: a sigstore/cosign-installer step does not pin "
                f"`cosign-release`, so the cosign binary — and the signature format "
                f"docs/installation.md tells operators to verify — follows the "
                "installer's default"
            )
        elif not re.fullmatch(r"v2\.\d+\.\d+", pin.group(1)):
            failures.append(
                "release-please.yml: a sigstore/cosign-installer step pins "
                f"cosign-release: {pin.group(1)}, off the 2.x line the published "
                "verification instructions assume; move the docs and this check "
                "together with the signing format"
            )
    return failures


def check_cosign_format_lane(text: str, ci: str) -> list[str]:
    """The lane that exercises cosign must install the binary the release signs with.

    `check_cosign_pin` only reads YAML, and the pin's risk is an upstream one:
    whether that installer still accepts `cosign-release` and still resolves 2.x
    assets. `ops/check-cosign-format.sh` answers it by signing with the real
    binary, which is evidence about the release only while the CI lane and the
    release lanes install the same thing — otherwise a bump moves the release and
    leaves the test proving something about a version nobody ships.
    """
    release = {
        (
            re.search(r"uses: (sigstore/cosign-installer@\S+)", step).group(1),
            re.search(r"cosign-release:\s*(\S+)", step).group(1),
        )
        for step in cosign_installer_steps(text)
        if re.search(r"cosign-release:\s*(\S+)", step)
    }
    tested = {
        (
            re.search(r"uses: (sigstore/cosign-installer@\S+)", step).group(1),
            re.search(r"cosign-release:\s*(\S+)", step).group(1),
        )
        for step in cosign_installer_steps(ci)
        if re.search(r"cosign-release:\s*(\S+)", step)
    }
    failures: list[str] = []
    if "ops/check-cosign-format.sh" not in ci:
        failures.append(
            "ci.yml: no lane runs ops/check-cosign-format.sh, so nothing proves the "
            "installer still delivers the pinned cosign or that it writes the "
            "signature format docs/installation.md documents"
        )
    if not tested:
        failures.append(
            "ci.yml: no cosign-installer step pins `cosign-release`, so the signing "
            "format is exercised with a different binary than the release uses"
        )
    elif tested != release:
        failures.append(
            f"ci.yml installs cosign as {sorted(tested)} while release-please.yml "
            f"signs with {sorted(release)}; the compatibility lane must install what "
            "the release installs or it proves nothing about the release"
        )
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
    if platform_default(version) == NATIVE_PLATFORM:
        if AMD64_FALLBACK_PLATFORM in compose:
            notes.append(f"docker-compose.yml: {fallback_obsolete_note(pinned.group(1))}")
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
    # Every page that promises an index must say when it starts existing: these
    # pages are on main from the moment the change merges, while the newest
    # published release is still amd64-only, and a reader who pins `:<version>`
    # or pulls `-arm64` on that tag gets nothing. Once the pinned tag is an
    # index the caveat is stale instead, so it is reported for removal with the
    # Compose fallback rather than left to rot.
    for page in INDEX_TRANSITION_PAGES:
        # Prose wraps, so the phrase is matched against a single-spaced copy.
        text = " ".join((ROOT / page).read_text(encoding="utf-8").split())
        if "multi-architecture index" not in text and "arm64` index" not in text:
            continue
        if platform_default(version) == NATIVE_PLATFORM:
            if INDEX_TRANSITION_PHRASE in text:
                notes.append(
                    f"{page}: the pinned tag {pinned.group(1)} is a "
                    "multi-architecture index, so drop the "
                    f"\"{INDEX_TRANSITION_PHRASE}\" caveat"
                )
        elif INDEX_TRANSITION_PHRASE not in text:
            failures.append(
                f"{page}: promises a multi-architecture index without saying it "
                f"starts \"{INDEX_TRANSITION_PHRASE}\"; the newest release "
                f"({pinned.group(1)}) is amd64-only, so a reader pinning that tag "
                "finds one platform and no -arm64 reference"
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


def fallback_obsolete_note(pinned: str) -> str:
    """What the check says once the pinned tag outgrows the amd64 fallback.

    The runbook quotes this note verbatim, and `check_platform_transition_guidance`
    holds the quote to it, so the instruction an operator reads cannot drift from
    the one the check emits.
    """
    return (
        f"the pinned tag {pinned} publishes a multi-architecture image, so the "
        "amd64 fallback now only forces emulation on ARM hosts; switch to "
        f"`{NATIVE_PLATFORM}`. Leave LAST_AMD64_ONLY_VERSION alone: it names the "
        f"last amd64-only release ({format_version(LAST_AMD64_ONLY_VERSION)}), and "
        "raising it to this tag re-asserts the fallback this note asks to drop"
    )


def check_platform_transition_guidance() -> list[str]:
    """The post-release follow-up must not ask for `LAST_AMD64_ONLY_VERSION` to move.

    The constant names the last release published as a single amd64 image, and
    `platform_default` compares the pinned tag against it. Bumping it to the
    release that first published an index puts the pin back *at* the constant, so
    the gate demands the amd64 fallback that release just made unnecessary: the
    follow-up would fail the check it exists to satisfy.
    """
    runbook = " ".join(
        (ROOT / "docs/maintainers/releasing.md").read_text(encoding="utf-8").split()
    )
    failures: list[str] = []
    if re.search(r"bump(?:ing)?\s+`?LAST_AMD64_ONLY_VERSION", runbook, re.IGNORECASE):
        failures.append(
            "docs/maintainers/releasing.md: tells the operator to bump "
            "LAST_AMD64_ONLY_VERSION; setting it to the released version makes "
            f"`{AMD64_FALLBACK_PLATFORM}` required again"
        )
    if "LAST_AMD64_ONLY_VERSION" not in runbook:
        failures.append(
            "docs/maintainers/releasing.md: the multi-architecture follow-up does "
            "not name LAST_AMD64_ONLY_VERSION, so an operator cannot tell whether "
            "it has to change"
        )
    elif format_version(LAST_AMD64_ONLY_VERSION) not in runbook:
        failures.append(
            "docs/maintainers/releasing.md: the follow-up does not name the last "
            f"amd64-only release ({format_version(LAST_AMD64_ONLY_VERSION)}), the "
            "value LAST_AMD64_ONLY_VERSION holds"
        )
    # The quoted note carries the instruction, so a reworded check must not leave
    # the runbook quoting advice the check no longer gives. The pinned tag is the
    # one part that legitimately differs: the runbook quotes the release that first
    # published an index, while a live run names whatever tag is pinned today. It
    # is cut out through a sentinel rather than a phrase from the note, so nothing
    # here breaks when the note itself is reworded — which is when this must work.
    quoted_tail = fallback_obsolete_note("\x00").split("\x00", 1)[1]
    if quoted_tail not in runbook:
        failures.append(
            "docs/maintainers/releasing.md: the quoted release-configuration note "
            "no longer matches the one check_compose_platform emits, so the runbook "
            "shows the operator an instruction the check does not give"
        )
    return failures


def self_test() -> int:
    """Prove decisions and rejection paths, not only that the tree passes today.

    The decision is what the post-release cleanup got wrong once: the pinned tag
    is compared against the *last amd64-only release*, so the constant trails the
    pin and never equals it.
    """
    last = (0, 3, 17)
    # A tag newer than the last amd64-only release resolves both architectures.
    assert platform_default((0, 3, 18), last) == NATIVE_PLATFORM
    assert platform_default((0, 4, 0), last) == NATIVE_PLATFORM
    # The amd64-only releases themselves, and anything older, keep the fallback:
    # dropping it there leaves an ARM host with nothing to pull.
    assert platform_default((0, 3, 17), last) == AMD64_FALLBACK_PLATFORM
    assert platform_default((0, 3, 16), last) == AMD64_FALLBACK_PLATFORM
    # The regression: bumping the constant to the first multi-architecture
    # release asks for the fallback that release removed.
    assert platform_default((0, 3, 18), (0, 3, 18)) == AMD64_FALLBACK_PLATFORM

    # The cosign pin: an installer step without one, or one off the 2.x line,
    # changes the signature format operators verify and must be reported.
    signed = (
        "jobs:\n  release-image:\n    steps:\n"
        "      - uses: sigstore/cosign-installer@abc # v4.1.2\n"
        "        with:\n          cosign-release: v2.5.2\n"
        "      - run: cosign sign --yes ghcr.io/o/r@sha256:x\n"
    )
    assert check_cosign_pin(signed) == []
    assert check_cosign_pin(signed.replace("          cosign-release: v2.5.2\n", "")) != []
    assert check_cosign_pin(signed.replace("v2.5.2", "v3.0.6")) != []
    assert check_cosign_pin("jobs:\n  release-image:\n    steps: []\n") != []
    # A named step is the same step: reading it as no step at all would let an
    # unpinned lane pass, which is the regression this gate exists to catch.
    named = signed.replace(
        "      - uses: sigstore/cosign-installer@abc # v4.1.2\n",
        "      - name: Install cosign\n        uses: sigstore/cosign-installer@abc # v4.1.2\n",
    )
    assert len(cosign_installer_steps(named)) == 1
    assert check_cosign_pin(named) == []
    assert check_cosign_pin(named.replace("          cosign-release: v2.5.2\n", "")) != []
    # Two lanes, one of them unpinned: the pinned one must not hide the other.
    both = signed + signed.replace(
        "        with:\n          cosign-release: v2.5.2\n", ""
    ).replace("release-image", "release-image-index-promote")
    assert len(cosign_installer_steps(both)) == 2
    assert len(check_cosign_pin(both)) == 1

    # The lane that signs for real must install what the release installs: a
    # bump applied to one side only leaves the evidence describing a version
    # nobody ships.
    lane = (
        "jobs:\n  cosign-format:\n    steps:\n"
        "      - uses: sigstore/cosign-installer@abc # v4.1.2\n"
        "        with:\n          cosign-release: v2.5.2\n"
        "      - run: bash ops/check-cosign-format.sh\n"
    )
    assert check_cosign_format_lane(signed, lane) == []
    assert check_cosign_format_lane(signed, lane.replace("v2.5.2", "v2.4.0")) != []
    assert check_cosign_format_lane(signed, lane.replace("@abc", "@def")) != []
    assert check_cosign_format_lane(
        signed, lane.replace("      - run: bash ops/check-cosign-format.sh\n", "")
    ) != []
    assert check_cosign_format_lane(signed, "jobs:\n  fmt:\n    steps: []\n") != []

    # Release ordering is security policy, so prove the checker rejects each
    # bypass class rather than merely confirming that today's workflow happens
    # to contain the expected job names.
    ordered = workflow_text()
    assert check_release_ordering(ordered) == []

    def replace_in_job(source: str, job: str, old: str, new: str) -> str:
        block = job_block(source, job)
        assert block is not None, f"release-order fixture lacks {job}"
        assert old in block, f"release-order fixture lacks {old!r} in {job}"
        return source.replace(block, block.replace(old, new, 1), 1)

    def rejected(candidate: str, label: str) -> None:
        assert check_release_ordering(candidate), (
            f"release-order checker accepted mutation: {label}"
        )

    rejected(
        replace_in_job(ordered, "release-please", "    needs: main-ci-success\n", ""),
        "release-please lost its main CI dependency",
    )
    rejected(
        replace_in_job(
            ordered,
            "release-please",
            "needs['main-ci-success'].result == 'success'",
            "always()",
        ),
        "release-please ignored the main CI result",
    )
    rejected(
        replace_in_job(
            ordered,
            "main-ci-success",
            "COMMIT_SHA: ${{ github.sha }}",
            "COMMIT_SHA: ${{ github.ref }}",
        ),
        "main preflight checked a ref instead of github.sha",
    )
    for outcome in ("neutral", "skipped", "cancelled", "completed"):
        rejected(
            replace_in_job(
                ordered,
                "main-ci-success",
                "              success)\n",
                f"              success | {outcome})\n",
            ),
            f"main preflight accepted non-success outcome {outcome}",
        )
    rejected(
        replace_in_job(
            ordered,
            "release-ci-success",
            "COMMIT_SHA: ${{ needs['release-metadata'].outputs.commit_sha }}",
            "COMMIT_SHA: ${{ github.sha }}",
        ),
        "repair preflight checked the workflow SHA instead of the tag commit",
    )
    rejected(
        replace_in_job(
            ordered, "release-binaries", "      - release-ci-success\n", ""
        ),
        "publishing job lost its direct release CI dependency",
    )
    rejected(
        replace_in_job(
            ordered,
            "release-binaries",
            "      needs['release-ci-success'].result == 'success' &&\n",
            "",
        ),
        "publishing job ignored the release CI result",
    )
    release_gate_removed = re.sub(
        r"\n  release-ci-success:\n.*?(?=\n  release-binaries:\n)",
        "",
        ordered,
        count=1,
        flags=re.DOTALL,
    )
    assert release_gate_removed != ordered
    release_gate_removed = replace_in_job(
        release_gate_removed,
        "release-crates",
        "    steps:\n",
        "    steps:\n"
        "      - name: Crates-only CI check\n"
        "        run: gh api check-runs?ref=tag\n",
    )
    rejected(release_gate_removed, "CI was checked only inside release-crates")
    rejected(
        ordered
        + "\n  rogue-publisher:\n"
        + "    runs-on: ubuntu-latest\n"
        + "    steps:\n"
        + "      - run: gh release upload v9.9.9 payload\n",
        "a publication primitive moved into an ungated job",
    )
    rejected(
        replace_in_job(
            ordered, "release-success", "      - release-ci-success\n", ""
        ),
        "release-success lost the CI gate dependency",
    )
    rejected(
        replace_in_job(
            ordered,
            "release-success",
            '          test "${{ needs[\'release-ci-success\'].result }}" = success\n',
            "",
        ),
        "release-success stopped aggregating the CI gate",
    )
    rejected(
        replace_in_job(
            ordered,
            "release-please",
            "      needs['main-ci-success'].result == 'success'\n",
            "      needs['main-ci-success'].result == 'success' &&\n"
            "      needs['release-please'].outputs.release_created == 'true'\n",
        ),
        "release-please maintenance was limited to release-created runs",
    )

    # The committed constant is deliberately not asserted here: it legitimately
    # moves if the quickstart is ever pinned back onto a newer amd64-only release,
    # and a tree where it disagrees with the pinned tag is already reported by
    # check_compose_platform as a readable failure rather than a traceback.

    print("check-release-config: decision and release-order self-tests passed")
    return 0


def main(argv: list[str]) -> int:
    if argv == ["--self-test"]:
        return self_test()
    if argv:
        raise SystemExit(
            f"usage: check-release-config.py [--self-test], not {' '.join(argv)}"
        )
    # The decision the notes and the runbook rest on is proven on every run, so a
    # refactor that inverts it cannot pass by leaving the tree untouched.
    self_test()
    text = workflow_text()
    notes: list[str] = []
    failures: list[str] = []
    failures.extend(check_binary_matrix(text))
    failures.extend(check_binary_gates(text))
    failures.extend(check_image_matrix(text))
    failures.extend(check_image_gates(text))
    failures.extend(check_release_ordering(text))
    failures.extend(check_release_success(text))
    failures.extend(check_cosign_pin(text))
    failures.extend(
        check_cosign_format_lane(
            text, (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
        )
    )
    failures.extend(check_compose_platform(notes))
    failures.extend(check_platform_transition_guidance())
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
    raise SystemExit(main(sys.argv[1:]))
