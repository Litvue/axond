# Maintainer release runbook

Axond uses release-please with Conventional Commit PR titles. Merges to `main`
update a release PR; merging that PR tags the release and runs the artifact
pipeline.

## Release order

1. Resolve the immutable tag/version/commit.
2. Require the tagged commit's `CI Success` aggregate.
3. Build four binary targets with checksums, SPDX SBOMs, and provenance/SBOM
   attestations.
4. Build and publish the `linux/amd64` image.
5. Smoke the published image before signing it.
6. Generate image SBOM/provenance attestations, sign the digest keylessly, and
   verify signature plus attestations.
7. Require every artifact lane.
8. Publish `gateway-core`, `gateway-transport`, then `axond` to crates.io.

crates.io is last because a published version cannot be replaced. The publish
script is idempotent: an existing aligned package is skipped, and a partial
release resumes at the first missing crate.

## Required GitHub configuration

- Release Please GitHub App ID/private key, or the documented `GITHUB_TOKEN`
  fallback.
- `CARGO_REGISTRY_TOKEN` in the protected `crates-io` environment.
- Actions permission to create pull requests.
- `CI Success` required on `main`.
- `packages: write`, `id-token: write`, and attestation permissions in the
  release jobs.

The crates.io token owner must have a verified email address.

## Normal release

1. Ensure the release PR is current and green.
2. Review version, changelog, Cargo metadata/lockfile, and release-managed docs
   version markers.
3. Merge the release PR.
4. Observe the tag/release workflow.
5. Verify artifacts externally with `docs/installation.md`.
6. Confirm all three crates are visible and `cargo install axond --locked`
   succeeds.

## Repair an existing tag

When a workflow fix lands after a release tag, dispatch **Release** from `main`
with the existing tag as `release_tag`. The reviewed workflow definition comes
from `main`, while every artifact lane checks out and verifies the immutable
tag.

The preflight requires the tag's `Dockerfile` and `ops/docker-smoke.sh`.
crates.io publication is enabled only if the tag also contains
`ops/publish-crates.sh`; older tags skip that lane explicitly.

```bash
gh workflow run .github/workflows/release-please.yml \
  --repo Litvue/axond --ref main -f release_tag=<existing-tag>
```

Do not retag or publish artifacts from a mutated checkout.

## Partial crates.io failure

Identify which immutable versions are already present, fix the account/token or
network issue, and re-dispatch the same tag. `ops/publish-crates.sh` checks in
dependency order and resumes safely.

Never modify a packaged tarball at an existing version. If a published version
is broken, yank all affected crates in reverse dependency order and ship a new
patch release.

## Verification

```bash
cargo install axond --version <version> --locked
```

In a scratch crate:

```bash
cargo add gateway-core@<version> gateway-transport@<version>
cargo build
```

Also verify release checksums, GitHub attestations, the public OCI pull, cosign
identity, image provenance, and the release evidence assets.
