# 4. CI and release pipeline

Date: 2026-08-04

## Status

Accepted

## Context

Axond is developed as a public open-source project under the Litvue org and is
meant to be dogfooded by the company. Two sibling Rust services — `actord` and
`custodian` — already run a mature GitHub Actions setup (release-please, a
lane-per-check CI matrix with an aggregate gate, a `cargo-deny` policy, and a
signed/attested release fan-out). Reusing that shape keeps the three repos
consistent and avoids reinventing supply-chain posture per project.

Axond differs from those two in scope: it ships **one** binary (`axond`) and
**one** container image, not a multi-service pipeline. So the pattern is ported,
not copied — the release fan-out is sized to a single-binary gateway.

## Decision

**Release automation is release-please, driven by Conventional Commits.** Merges
to `main` keep a release PR open; merging it tags `v<major>.<minor>.<patch>`,
updates `CHANGELOG.md`, and bumps the single `workspace.package.version` via a
`toml` extra-file. Pre-1.0 uses `bump-minor-pre-major` +
`bump-patch-for-minor-pre-major`, matching the siblings, so the first automated
release off `0.0.0` is a patch. `Cargo.lock` is re-synced on the release PR so
`--locked` builds stay green after the version bump. The release-please/
lockfile-sync job for a ref is serialized by a queueing concurrency group, and
lockfile sync re-bases onto the remote branch tip with a bounded retry instead
of force-pushing.

**CI is one job per concern** (`fmt`, `clippy`, `build`, `tests`,
`stateful-tests`, `docs`, `dependency-policy`, `static-binary`, `docker-smoke`,
and `publish-dry-run` per [ADR 0025](./0025-crates-io-publication.md))
behind a single required `CI-Success` gate. The hermetic `tests` lane remains
service-free; `stateful-tests` runs the gated Redis/Postgres tests in pinned
service containers with `AXOND_TEST_REQUIRE_SERVICES=1`, so a missing service
configuration fails loudly instead of silently skipping. All Rust invocations
are `--locked`; the toolchain is pinned to the `rust-toolchain.toml` channel.
The `static-binary` lane asserts the musl build is actually static (accepting
`static-pie linked`, the modern musl default, as well as `statically linked`),
and `docker-smoke` boots the image against the example config and probes
`/healthz` + `/v1/models`.

**Release artifacts, per tagged commit:**

- Cross-platform binaries (`x86_64` gnu + static musl Linux, `aarch64` macOS,
  `x86_64` Windows), each with a SHA-256 checksum and an SPDX SBOM.
- A single-arch OCI image at `ghcr.io/litvue/axond`, tagged by version and short
  SHA (no `latest`).
- SLSA build provenance and SBOM **attestations** for every binary and for the
  image, plus a keyless **cosign** signature on the image digest. The release
  job verifies the signature and provenance before completing, and the image is
  smoke-tested before it is signed.
- The `gateway-core`, `gateway-transport`, and `axond` crates.io versions, added
  by [ADR 0025](./0025-crates-io-publication.md), published last because they are
  the only irreversible artifact.

**Supply-chain policy (`deny.toml`)** is enforced in CI and re-run daily against
`main`. Advisories and unmaintained crates fail; the license allowlist is the
permissive set plus `CDLA-Permissive-2.0` (Mozilla's CA bundle in
`webpki-roots`). `wildcards` was originally `"allow"`, because the workspace's
own path crates read as wildcards; **[ADR 0025](./0025-crates-io-publication.md)
supersedes that** with `"deny"`, since the internal crates now carry exact
version requirements and crates.io rejects a wildcard dependency on publish.

**PR titles are gated to Conventional Commits** so release-please can classify
every merge.

Release publication is tied to a release-please-created release or an explicit
`workflow_dispatch` against an existing, matching tag — never an untagged or
dirty worktree — and the signing identity is pinned to this workflow on
`refs/heads/main` or a `refs/tags/v*` ref.

## Consequences

- Versioning and changelog are mechanical and tied to commit discipline; a
  sloppy commit type produces a wrong bump, so the PR-title gate is load-bearing.
- The hermetic and stateful test lanes cover both no-service developer runs and
  real Redis/Postgres behavior; the required-services guard prevents CI from
  passing with those tests skipped.
- Consumers can verify provenance and signatures for both the binaries and the
  image, and reconstruct the dependency set from the attached SBOMs.
- The release PR only triggers downstream CI when the org-wide release GitHub
  App (`RELEASE_PLEASE_APP_ID`/`RELEASE_PLEASE_APP_PRIVATE_KEY`, shared with
  `actord` and `custodian`) authors it; if the repo is outside the App's scope
  the `GITHUB_TOKEN` fallback leaves the release PR un-CI-validated until merge.
- The pipeline currently publishes a single `linux/amd64` image; multi-arch is a
  later addition, not a rewrite.

## Amendment (2026-08-12): ARM64 archives and a multi-architecture image

The original decision left multi-arch as "a later addition, not a rewrite". This
is that addition, and it is additive in exactly that sense: nothing above is
withdrawn.

Two Linux `aarch64` archives (glibc and static musl) join the four existing
binary targets, and the image is published for `linux/arm64` alongside
`linux/amd64`. Every Linux archive and every published manifest is built on a
runner of its own architecture, so the release never ships an artifact that was
only cross-compiled and never executed: each Linux archive is booted through the
Tier 0 gate, and each single-platform image is smoke-tested before it is signed.

The two signed, attested child images are then joined — by digest, not rebuilt —
into an OCI index that the existing `<version>` and `sha-<short>` tags point at,
so a digest-pinned deployment resolves on either architecture. The index digest
is itself signed and carries a provenance attestation, and it is what
`axond-image-<version>.digest` names on the release. SBOM attestations stay on
the per-architecture children, where the packages are: an index has no
filesystem, so an "index SBOM" could only be one child's document under a
subject it does not describe. Single-platform references remain first-class as
`<version>-<arch>` and `sha-<short>-<arch>`; per-architecture SBOM and digest
assets are attached per architecture, since their contents genuinely differ.
Still no `latest` tag.

Keyless signatures remain image-only. Archives are covered by their SHA-256
sidecar and GitHub provenance/SBOM attestations, as before; signing archives
would be a separate decision with its own verification story for installers.

The Compose quickstart pins a release tag, so it cannot become multi-architecture
before the release does. Its `platform:` default therefore stays
`linux/amd64` — overridable through `AXOND_PLATFORM` — while the pinned tag is an
amd64-only release, so an ARM host keeps running it under emulation instead of
failing to pull an image with no ARM child. The validator below requires that
fallback while the pinned tag is amd64-only, and asks for its removal — as a
note, not a failure — once the tag moves past the last amd64-only version:
release-please rewrites that tag inside its own release pull request and never
touches the `platform:` line, so a failure there would block the release itself.
Dropping the fallback is therefore a documented post-release step.

Because the release matrix is only exercised for real at a tag,
`ops/check-release-config.py` asserts its shape on every change: the published
targets and archive extensions, the image platforms, that ARM lanes run on ARM
runners, that each lane keeps its checksum/SBOM/provenance/signature/smoke gate,
that `release-success` requires every lane, and that no `latest` tag is
introduced anywhere.
