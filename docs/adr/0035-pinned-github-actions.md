# 35. Pinned GitHub Actions and an enforced pin policy

Date: 2026-08-12

## Status

Accepted

## Context

[ADR 0004](./0004-ci-and-release-pipeline.md) puts the release fan-out in
GitHub Actions: the release job mints a GitHub App token, publishes to crates.io
with `CARGO_REGISTRY_TOKEN`, and signs the image digest keylessly with an OIDC
identity bound to `release-please.yml`. Every third-party Action in that job runs
with the same reach as the job itself.

Until now those Actions were referenced by tag or branch — `actions/checkout@v7`,
`dtolnay/rust-toolchain@stable`. A tag is a pointer the upstream owner can move,
so the code that runs beside the release credentials was whatever had most
recently been pushed there. That is the shape of the `tj-actions/changed-files`
compromise: a retagged release, not a new dependency, and nothing in the
repository changed.

Pinning by itself decays. A repository full of SHAs nobody refreshes runs
year-old Actions and, worse, invites the next contributor to add a tag ref
because the convention is not mechanically visible.

## Decision

**Every `uses:` in `.github/workflows` names a full 40-character commit SHA with
the released version in a trailing comment.** The SHA is what runs; the comment
is what a reviewer reads:

```yaml
- uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
```

Where an Action publishes no usable release tag, the comment names the upstream
branch the SHA was taken from (`# stable`). Actions committed to this repository
(`./.github/actions/...`) are exempt: they move with the commit being tested.

**`ops/workflow-policy.py` enforces it in a required CI lane.** The gate is
line-based and dependency-free, so it runs on the same `python3` floor as the
other `ops/` checks. It rejects a ref that is not a full SHA, a pin without a
version comment, a comment that names neither a version nor a branch, the same
Action pinned to two different SHAs anywhere in the workflows — including twice
within one file — and a `docker://` step.

The gate also guards the two properties that make the pins worth having, because
both live in the same files and neither is visible in a diff without knowing to
look:

- every workflow and no job may widen beyond declared least privilege
  (`permissions:` must exist; `permissions: write-all` is rejected);
- every `SIGNER_IDENTITY:` declaration stays an anchored regular expression, and
  every `cosign verify` keeps `--certificate-identity-regexp` and
  `--certificate-oidc-issuer`. The check is per occurrence, so an unanchored
  job-level `env:` override cannot shadow an anchored workflow-level value. The
  `cosign verify` half also reads the shell under `ops/`, because the release
  verifies its images through
  [`ops/verify-image-evidence.sh`](../../ops/verify-image-evidence.sh) rather
  than an inline `run:` block; a gate that only read the workflows would pass
  while the real verification accepted any Fulcio certificate. One file is
  exempt from the certificate flags and it is named in the checker:
  [`ops/check-cosign-format.sh`](../../ops/check-cosign-format.sh) verifies a
  throwaway image against a key pair it mints itself, because a pull request has
  no OIDC identity to verify against. The exemption is scoped to that file and to
  keys generated in it, so a script cannot opt out of the identity restriction by
  minting a pair next to the verify it wants excused, and widening it is a review
  of the list.

`ops/workflow-policy.py --self-test` runs the checker against in-memory fixtures
for each rejection class and runs first in the lane, so a pass means the gate is
still capable of failing. The lane also runs `actionlint` at one pinned version
via `ops/actionlint.sh`.

**Bumps arrive as review, not as drift.** `.github/dependabot.yml` opens one
grouped `ci(deps)` pull request a week that moves the SHAs and their comments
together. Cargo stays out of Dependabot: `deny.toml`, the `dependency-policy`
lane, and `dependency-audit.yml` already cover it, and a lockfile bump belongs to
`cargo update` and the full matrix rather than a per-crate PR.

A bump cannot quietly trade immutability for convenience. Dependabot's
github-actions updater rewrites a SHA-pinned `uses:` to another SHA and edits the
trailing version comment to match — it updates comments *only* for pins that are
already SHA refs, and it never replaces a SHA with a tag ref
([`dependabot-core#5951`](https://github.com/dependabot/dependabot-core/pull/5951),
[changelog](https://github.blog/changelog/2022-10-31-dependabot-now-updates-comments-in-github-actions-workflows-referencing-action-versions/)).
The pin for `dtolnay/rust-toolchain`, whose SHA comes from the `stable` branch
rather than a release tag, may therefore be proposed as the SHA of the upstream
`v1` tag with the comment rewritten to `# v1`: still an immutable SHA, but now
following a different upstream ref. That is a reviewer's decision on a diff, and
the property that matters holds either way — the gate rejects any `uses:` that is
not a full SHA, and `--self-test` asserts that rejection with both a tag fixture
(`actions/checkout@v7`) and a branch fixture (`dtolnay/rust-toolchain@stable`), so
no update path can put a movable ref back into a workflow.

Container base images are out of scope here; they are pinned by tag in
`Dockerfile` and tracked separately.

### Alternatives considered

- **Tags plus review discipline.** Rejected: the compromise leaves no trace in
  the repository, so there is nothing for a reviewer to catch.
- **Pins without a gate.** Rejected: a convention that only lives in a document
  is reintroduced by the first contributor who copies an upstream README snippet.
- **Renovate instead of Dependabot.** Renovate resolves pins with version
  comments well and offers finer scheduling, but it needs an app installation and
  its own config surface. Dependabot is already available to the org, and the pin
  refresh is a small weekly diff either way. Revisit if the grouped PR becomes
  noisy.
- **A vendored copy of each Action.** Rejected: full control over what runs, but
  the maintenance cost and the loss of upstream security fixes are worse than the
  pin.
- **`actionlint` in a required lane via an installer Action.**
  `taiki-e/install-action` has no `actionlint` manifest and falls through to
  `cargo-binstall`, which fails because `actionlint` is not a crate. The script
  therefore owns acquisition, and its version is pinned two ways (release
  checksum, image digest).
- **PyYAML for the gate.** Rejected: real parsing would be more robust than
  line matching, but it would make a required lane depend on a Python package
  where every other `ops/` check runs on the interpreter alone. Line numbers in
  failures are also more useful for a `SIGNER_IDENTITY` override than a parsed
  mapping would be.

### State tier

Tier 0 (config-only). This decision covers CI and release automation only: no
runtime component, no Redis, and no Postgres. It does not change the tier of any
deployment.

## Consequences

- A moved upstream tag cannot change what runs in a job that can reach the
  release token, the crates.io token, or the signing identity. Adopting an
  upstream fix is an explicit, reviewable commit.
- A new mutable ref fails CI with the SHA-pinned form in the error message, so
  the convention does not depend on a reviewer noticing.
- Reviewing a Dependabot pin bump means reading upstream release notes, not just
  the diff — the pinned SHA is what CI runs until someone changes it again. A
  major bump can rename inputs or change defaults.
- Pins for Actions without release tags (`# stable`) need manual refreshes; they
  are the ones most likely to go stale.
- The gate parses lines, not YAML. It is intentionally strict about the one-line
  `uses:` form every workflow here uses, and would need extending for exotic
  formatting.
