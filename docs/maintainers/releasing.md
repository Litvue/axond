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
7. Join the signed child digests into a multi-architecture index, staged under
   `sha-<short>-index`, and assert every descriptor in it is a supported platform
   child (or an attestation manifest for one).
8. Pull that index digest on each architecture and smoke it natively.
9. Promote the smoked index (`INDEX_MODE=promote`, which requires the smoked
   digest and refuses an empty one — staging cannot apply these tags at all):
   assert that digest still holds exactly the release children, retag *that
   digest* as `<version>` and `sha-<short>` — nothing is reassembled, so no check
   can run after a tag exists — sign it keylessly,
   attest its provenance, verify both, and attach it to the release as
   `axond-image-<version>.digest`. SBOM attestations and SPDX assets stay
   per-architecture, on the children.
10. Require every artifact lane.
11. Publish `gateway-core`, `gateway-transport`, then `axond` to crates.io.

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
- The `area:operations` label, which
  [`.github/dependabot.yml`](../../.github/dependabot.yml) applies to its pin
  bumps. Dependabot rejects its whole configuration on an unknown label and
  reports it only on the repository's Dependabot page, so a renamed label would
  stop the Action pins from being refreshed silently —
  [`ops/dependabot-labels.sh`](../../ops/dependabot-labels.sh) fails the
  `workflow-policy` lane instead.

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

0.3.18 was that release, and this cleanup has landed: the quickstart no longer
defaults `platform:` at all, and the operator-facing pages no longer date the
index to a future release. It is recorded here because the same steps apply if
the quickstart pin is ever moved back onto an amd64-only release.

While the pinned tag is amd64-only, the quickstart must force `linux/amd64` so
ARM hosts can pull it at all, and `ops/check-release-config.py` *fails* without
it. The first release publishing a `linux/amd64` + `linux/arm64` index makes the
fallback obsolete, and the check then says so on every subsequent run:

```
release configuration note: docker-compose.yml: the pinned tag 0.3.18 publishes a
multi-architecture image, so the amd64 fallback now only forces emulation on ARM
hosts; switch to `platform: ${AXOND_PLATFORM-}`
```

It is a note, not a failure, because release-please bumps the pinned tag inside
its own generated release PR and never rewrites the `platform:` line — failing
there would block the release. So the follow-up lands once that release is
published: set `platform: ${AXOND_PLATFORM-}` in `docker-compose.yml` and drop
the `from the next release onward` caveat from the pages that promise the index.
`ops/check-compose-platform.sh` then proves native resolution, and the notes
disappear.

`LAST_AMD64_ONLY_VERSION` is *not* part of that follow-up. It names the last
release published as a single `linux/amd64` image — 0.3.17 — so it moves only
when the pin is rebased onto a newer amd64-only release. Bumping it to the
multi-architecture release instead re-asserts that the pinned tag is amd64-only
and fails the check that the follow-up just satisfied.

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

## Workflow Action pins

The policy behind this section is
[ADR 0033](../adr/0033-pinned-github-actions.md). The release jobs can reach the
release GitHub App token, `CARGO_REGISTRY_TOKEN`, and the keyless signing
identity, so a third-party Action running there is as privileged as this runbook.
`owner/action@v3` is a pointer the upstream owner can move, so every `uses:` in
`.github/workflows` names a full commit SHA with the version in a trailing
comment:

```yaml
- uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
```

The required `workflow-policy` lane runs
[`ops/workflow-policy.py`](../../ops/workflow-policy.py), which rejects a tag or
branch ref, a short SHA, a pin with no readable version comment, a workflow with
no `permissions:` block, `permissions: write-all`, an unanchored
`SIGNER_IDENTITY`, and a `cosign verify` that does not restrict the certificate
identity and issuer. It also requires one reviewed pin per action across all
workflows, so two lanes cannot silently run different builds of the same Action.
The lane proves it still rejects those with `ops/workflow-policy.py --self-test`
before it checks the repository, and then lints the workflows with
[`ops/actionlint.sh`](../../ops/actionlint.sh). That script runs one pinned
`actionlint` version, reached three ways: a matching binary already on `PATH`,
otherwise the checksum-verified release archive for this host (Linux and macOS,
x86-64 and arm64, each with its own checksum from the release's
`checksums.txt`), otherwise the same version as a digest-pinned container image —
which is the path GitHub-hosted runners take, because they answer 503 for the
release asset, and the path any other host takes rather than downloading a binary
it cannot execute. Bumping `actionlint` means updating the version, all four
checksums, and the image digest together. Locally:

```bash
just workflow-policy   # pins, permissions, signer restrictions, Dependabot labels
just actionlint        # workflow linting; downloads the pinned actionlint
```

Everything in that lane is offline except the label check
([`ops/dependabot-labels.sh`](../../ops/dependabot-labels.sh)), which needs an
authenticated `gh` and reports that it skipped when there is none. On CI it uses
the job's own `github.token` for a read-only label query and prints what it
verified, so the lane carries its own evidence:

```
dependabot labels exist on Litvue/axond (area:operations)
```

The job keeps the workflow's `contents: read` and adds nothing: listing labels is
Issues-scoped, and GitHub serves it without any permission while the repository is
public. If `axond` ever becomes private, that query starts returning 403 and the
check says so and fails rather than reporting a verified label — the fix is
`issues: read` on this job alone, not a wider workflow grant.

Its `--self-test` (also run in the lane) covers the ways the check could pass
while verifying nothing: a neighbouring `- package-ecosystem:` entry read as a
label, a config that moved or was renamed, and a `labels:` key written in a shape
the reader cannot see (an inline `[…]` list). All three fail rather than report
success.

If a host has neither a supported release archive nor `docker`, the script says
so and fails instead of linting with an unpinned version.

[`.github/dependabot.yml`](../../.github/dependabot.yml) opens one grouped
`ci(deps):` pull request a week that moves the pins and their comments forward
together. Reviewing a pin bump — Dependabot's or your own — means checking that
the SHA is the commit the claimed tag points at in the upstream repository, not
just that the comment reads plausibly:

```bash
gh api repos/actions/checkout/commits/v7.0.1 --jq .sha
```

Bumping a pin by hand follows the same rule: resolve the tag to its SHA, write
both, and let the lane confirm the format. Where an action publishes no usable
release tag the comment names the upstream branch the SHA was taken from
(`dtolnay/rust-toolchain@… # stable`), which is also the pin most likely to need
a manual refresh. Dependabot only ever rewrites a SHA pin to another SHA and
edits the comment to match — it cannot turn a pin into a `@v1` tag ref, and the
lane would reject one if it did — but for that branch pin it may propose the SHA
behind the upstream `v1` tag with the comment rewritten to `# v1`. That is still
immutable, and it is a deliberate change of which upstream ref the pin follows:
take it only if tracking the tag is what you want, and keep `# stable` otherwise.
A proposed *major* bump is not a pin change: read the upstream
release notes for renamed inputs and changed defaults before taking it, because
the pinned SHA is what CI will run until someone changes it again.

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
