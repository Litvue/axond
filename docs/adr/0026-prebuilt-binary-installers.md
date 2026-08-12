# 26. Prebuilt binary installers

Date: 2026-08-11

## Status

Accepted

Extends the release-artifact and verification decisions in
[ADR 0004](./0004-ci-and-release-pipeline.md).

## Context

Axond releases already build executable archives, checksums, SBOMs, and GitHub
provenance attestations, but the primary installation guidance was
`cargo install axond`. A Cargo installation downloads source and compiles it on
the user's machine. That is useful for Rust developers, but it obscures the
prebuilt artifacts and requires a Rust toolchain, native build dependencies,
and compilation time from operators who only want to run the gateway.

A first-party installer makes the release binaries discoverable and gives users
one stable command across supported platforms. It also creates a security
boundary: a piped installer downloads executable code, and a checksum stored
beside an archive detects corruption but is not independent proof that GitHub
Actions built the archive from this repository.

## Decision

The repository ships `install.sh` for Linux x86-64 and macOS Apple Silicon and
`install.ps1` for Windows x86-64. The scripts install an existing GitHub release
archive; they do not compile Axond. Linux defaults to the static musl target and
allows an explicit GNU target. Unsupported architectures continue to use Cargo
or a source build until they join the release matrix.

The README presents the prebuilt installer as the normal local installation
path while retaining `cargo install axond` as the source-build alternative.
Installers resolve the latest release through GitHub's public release redirect,
accept an explicit version and destination for reproducible automation, and
reject unsupported targets and malformed settings before downloading assets.

Every install verifies the release archive's same-origin SHA-256 sidecar. This
is the availability-oriented default: installation does not require GitHub CLI,
authentication, the GitHub API, or the transparency log. The installer warns
that this check proves integrity rather than publisher provenance.

Production automation can opt into fail-closed provenance verification with
`AXOND_REQUIRE_ATTESTATION=1` or PowerShell's `-RequireAttestation`. Strict mode
requires an authenticated GitHub CLI with attestation support and runs
`gh attestation verify` against `Litvue/axond`; missing tooling, authentication,
network access, or a failed verification aborts installation. Boolean-like
environment values are parsed explicitly, and unknown values fail rather than
silently weakening verification.

The installers themselves remain auditable repository files. Documentation
shows the convenient piped command and also recommends downloading and
inspecting the script first where that trust model is inappropriate.

### State tier

Tier 0. The installers are distribution tooling and introduce no runtime state
or service dependency. They do not raise the tier of an existing deployment;
Tier 0, Tier 1 (Redis), and Tier 2 (Postgres) deployments run the same binary and
retain their existing state choices.

## Consequences

- Operators can install a released binary without a Rust toolchain or local
  compilation, while Rust users can continue to use Cargo.
- Supported installer platforms are deliberately limited to artifacts exercised
  by the release matrix; adding an architecture requires both a release target
  and installer detection.
- The default checksum path remains available during GitHub API, CLI, or
  transparency-log outages, but it does not independently authenticate the
  publisher.
- Environments that require publisher provenance must explicitly enable strict
  verification and accept its GitHub CLI, authentication, and network
  dependencies.
- Piped installation is convenient but places trust in the current default
  branch; high-assurance users should inspect or pin the installer and release.
