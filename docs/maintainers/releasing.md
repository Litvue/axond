# Maintainer release runbook

Axond uses release-please with Conventional Commit PR titles. Merges to `main`
update a release PR; merging that PR tags the release and runs the artifact
pipeline.

## Release order

1. Resolve the immutable tag/version/commit.
2. Require the tagged commit's `CI Success` aggregate.
3. Build six binary targets with checksums, SPDX SBOMs, and provenance/SBOM
   attestations, each Linux archive booted through the Tier 0 gate on a runner
   of its own architecture.
4. Build and publish the `linux/amd64` and `linux/arm64` images, each on a native
   runner, under their `<version>-<arch>` and `sha-<short>-<arch>` tags.
5. Smoke each published single-platform image before signing it.
6. Generate image SBOM/provenance attestations, sign the digest keylessly, and
   verify signature plus attestations, per architecture.
7. Join the signed child digests into the multi-architecture index that
   `<version>` and `sha-<short>` point at, assert every descriptor in it is a
   supported platform child (or an attestation manifest for one), sign the index
   digest, attest its provenance, and attach it to the release as
   `axond-image-<version>.digest`. SBOM attestations and SPDX assets stay
   per-architecture, on the children.
8. Pull that index digest on each architecture and smoke it natively.
9. Require every artifact lane.
10. Publish `gateway-core`, `gateway-transport`, then `axond` to crates.io.

crates.io is last because a published version cannot be replaced. The publish
script is idempotent: an existing aligned package is skipped, and a partial
release resumes at the first missing crate.

## Required GitHub configuration

- Release Please GitHub App ID/private key, or the documented `GITHUB_TOKEN`
  fallback.
- `CARGO_REGISTRY_TOKEN` in the protected `crates-io` environment.
- Actions permission to create pull requests.
- `CI Success` required on `main`.
- Private vulnerability reporting enabled on the repository, so the
  [`SECURITY.md`](../../SECURITY.md) advisory link works and reports do not
  arrive as public issues.
- `packages: write`, `id-token: write`, and attestation permissions in the
  release jobs.

The crates.io token owner must have a verified email address.

The `GITHUB_TOKEN` fallback can create the release PR only when organization or
enterprise policy permits Actions-created pull requests. If that policy is
disabled, use the GitHub App path for full automation. The workflow still
updates the conventional release branch and synchronizes `Cargo.lock` after a
denied PR creation, so a maintainer can open the prepared branch manually:

```bash
gh api repos/Litvue/axond/git/matching-refs/heads/release-please--branches--main \
  --jq '.[].ref'
gh pr create --repo Litvue/axond \
  --base main \
  --head <generated-branch> \
  --title "chore(main): release <version>"
```

Do not repeatedly rerun a denied release-please job without changing the token
or policy; it will fail at the same PR-creation boundary.

## Normal release

1. Ensure the release PR is current and green.
2. Review version, changelog, Cargo metadata/lockfile, and release-managed docs
   version markers.
3. Merge the release PR.
4. Observe the tag/release workflow.
5. Verify artifacts externally with `docs/installation.md`.
6. Confirm all three crates are visible and `cargo install axond --locked`
   succeeds.

## After the first multi-architecture release

The quickstart still forces `linux/amd64` so that ARM hosts can run the last
amd64-only image at all. The first release that publishes a `linux/amd64` +
`linux/arm64` index makes that fallback obsolete, and
`ops/check-release-config.py` says so on every subsequent run:

```
release configuration note: docker-compose.yml: the pinned tag 0.3.17 publishes a
multi-architecture image, so the amd64 fallback now only forces emulation on ARM
hosts; switch to `platform: ${AXOND_PLATFORM-}` and bump
LAST_AMD64_ONLY_VERSION to 0.3.17
```

It is a note, not a failure, because release-please bumps the pinned tag inside
its own generated release PR and never rewrites the `platform:` line — failing
there would block the release. Land the two-line follow-up once that release is
published: set `platform: ${AXOND_PLATFORM-}` in `docker-compose.yml` and bump
`LAST_AMD64_ONLY_VERSION` in `ops/check-release-config.py` to the released
version. `ops/check-compose-platform.sh` then proves native resolution, and the
note disappears.

## Repair an existing tag

When a workflow fix lands after a release tag, dispatch **Release** from `main`
with the existing tag as `release_tag`. The reviewed workflow definition comes
from `main`, while every artifact lane checks out and verifies the immutable
tag.

```bash
gh workflow run .github/workflows/release-please.yml \
  --repo Litvue/axond --ref main -f release_tag=<existing-tag>
```

### What a repair from `main` requires of the tag

Repairing from `main` mixes two refs on purpose — the workflow definition from
`main`, the artifacts from the tag — so it only works where the tag can satisfy
the definition. Every artifact lane runs *the tag's own copy* of the scripts it
invokes, so the preflight refuses up front, naming the path, rather than failing
several minutes later inside a lane:

| Path required at the tag | Why | Missing at the tag |
| --- | --- | --- |
| `Dockerfile`, `ops/docker-smoke.sh` | the image lane builds and smokes the published image from the tag | the dispatch fails in the preflight |
| `ops/binary-smoke.py` | each binary lane boots the binary it archived, before attestation, signing, and upload | the dispatch fails in the preflight |
| `ops/publish-image-index.sh`, `ops/verify-image-evidence.sh` | the image lanes assemble the multi-architecture index from the per-platform child digests and verify the evidence attached to both | the dispatch fails in the preflight |
| `ops/publish-crates.sh` | crates.io upload | the crates lane is skipped explicitly and the rest of the repair proceeds |

The difference in the last row is deliberate: a skipped optional lane leaves the
release incomplete but every published artifact still gated, while skipping a
boot gate would attest and attach binaries nobody started — the property the
`binary-smoke` lanes exist to hold. A gate is not allowed to degrade quietly.

### Repairing a tag that predates a required path

A tag cut before `ops/binary-smoke.py` or the multi-architecture index scripts
landed cannot be repaired from `main`. Every tag published before ARM support is
in that position, so call it out in the release notes for the release that
introduces it: a maintainer with `--ref main -f release_tag=<older-tag>` in their
shell history should learn the new form from the changelog rather than from a
failed dispatch. The preflight failure also prints the remediation itself, so it
does not depend on this page being read first.

Dispatch the same workflow from the tag instead — the preflight's required-path
check runs only for `refs/heads/main`, and a tag ref runs that tag's own
definition and its own gates, which is the only self-consistent way to rebuild an
older release:

```bash
gh workflow run .github/workflows/release-please.yml \
  --repo Litvue/axond --ref refs/tags/<existing-tag> \
  -f release_tag=<existing-tag>
```

The dispatch is rejected from any other ref, and when dispatched from a tag ref
the preflight also verifies that the ref's commit is the tag's commit.

If the repair needs the *fix* on `main` — a workflow bug that the tag's own
definition still has — the answer is a new patch release, not a mixed-ref
dispatch: cut a release from `main` so the artifacts and the definition that
produced them are the same reviewed tree. Do not retag, and do not publish
artifacts from a mutated checkout.

## Partial crates.io failure

Identify which immutable versions are already present, fix the account/token or
network issue, and re-dispatch the same tag. `ops/publish-crates.sh` checks in
dependency order and resumes safely.

Never modify a packaged tarball at an existing version. If a published version
is broken, yank all affected crates in reverse dependency order and ship a new
patch release.

## Public API compatibility

The `api-compat` lane runs [`ops/api-compat.py`](../../ops/api-compat.py), which
compares each published *library* crate — `gateway-core` and
`gateway-transport`; the binary-only `axond` has no Rust API surface — against
the version already on crates.io with `cargo-semver-checks` (pinned in
`ci.yml`). It is blocking, and there are no allow rules or per-lint bypasses.
The crate list comes from `cargo metadata`, so a newly added published library
crate is covered without touching the script, and the script itself needs only
`python3` 3.10 or newer — the same floor as the provider-SDK lockfile — which CI
proves with `ops/api-compat.py --self-test` on 3.10.

Run it before pushing an API change:

```bash
just api-compat            # or: ops/api-compat.py gateway-core
```

When the break is unintended, fix the API. When it is intended:

1. Confirm the break is worth it against
   [`docs/compatibility.md`](../compatibility.md#the-published-rust-api) — pre-1.0,
   a break is a **minor** bump, so it cannot ride in a patch.
2. Add one entry to
   [`ops/api-compat-overrides.toml`](../../ops/api-compat-overrides.toml) with the
   crate, the published `baseline` exactly as the gate reports it, the
   justification, and `reviewed_in` pointing at this pull request or an ADR.
   Reviewers are approving that entry, not a switch.
3. Use a `feat!:`/`fix!:` (breaking) Conventional Commit title so release-please
   takes the minor bump, and describe the migration in the changelog body.
4. After the release, delete the entry — the gate prints that it no longer
   applies, because the break is now part of the baseline. An override only ever
   matches the one baseline it names, so a forgotten entry cannot mask the next
   break.

If the lane fails without reaching a comparison (no registry access, a build
error, a crate that has never been published), it reports that instead of a
break; fix the invocation rather than adding an override.

## Rust version floor

The MSRV is `rust-version` in `[workspace.package]`, and the `msrv` lane runs
[`ops/msrv-gate.sh`](../../ops/msrv-gate.sh): it builds the workspace on the
first patch of that minor and refuses drift between `Cargo.toml`,
`rust-toolchain.toml`, the `Dockerfile`, and the crate manifests. The pinned
`1.97.1` lanes are unchanged by it — the floor is proved in its own lane rather
than by weakening the stable one. The build needs `rustup`, because that is the
only way to select the floor: without it the lane fails instead of compiling with
an ambient newer compiler and calling the MSRV verified. Use
`AXOND_MSRV_CHECK_ONLY_POLICY=1 ops/msrv-gate.sh` to run the declaration checks
alone.

To raise the floor, in one PR: bump `rust-version`, bump `rust-toolchain.toml`
and the `Dockerfile` stage if they now trail it, update the `toolchain:` pins in
the workflows, update the table in
[`docs/compatibility.md`](../compatibility.md#the-rust-version-floor-msrv), and
land it as a **minor** release with a changelog entry that says why. To raise
only the pinned developer toolchain, bump `rust-toolchain.toml` and the workflow
pins and leave `rust-version` alone; the `msrv` lane then keeps proving the older
floor still builds.

## Security releases

Before the release, the changes it carries should already have been reviewed
against the [threat-model review triggers](../security/threat-model-review.md):
each trigger names the release-impact statement it owes, which is what the
changelog, the migration notes, and the
[compatibility contract](../compatibility.md) are assembled from. A release
containing a token-format, namespace-key, schema, artifact, or signer-identity
change without that statement is a release whose upgrade notes are guesswork.

A security fix uses this same runbook — same required CI, signed artifacts, and
attestations — with the additions in [`SECURITY.md`](../../SECURITY.md): a
regression test that failed before the fix, a backport to the previous supported
minor when it is affected, the GitHub Security Advisory published with a
requested CVE and the affected/fixed ranges, and a changelog entry referencing
the advisory. Do not fold a security fix into an unrelated large release; ship it
on its own so the advisory maps to one version.

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
