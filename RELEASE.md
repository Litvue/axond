# Beta release readiness — go/no-go

Dated review of whether axond is ready to publish its first `0.x` beta.
Reviewed at `main` (`1dd3a14`) plus the changes in the PR that adds this file.

**Date: 2026-08-05**
**Recommendation: GO**, conditional on two maintainer decisions that this
document cannot make (below). No blocking gap was found; the release pipeline is
correctly wired and has been exercised as far as it can be without a tag.

Beta here means the stability milestone the epic defines — the surfaces are
stable and documented, the failure modes are understood, the supply chain is
signed — not completion of the roadmap. `/v1/responses` and cross-provider
translation stay deferred, deliberately and in writing
([compatibility contract](./docs/compatibility.md)).

## Exit criteria

| # | Criterion | Status | Evidence |
| --- | --- | --- | --- |
| 1 | Streaming, failover, native `/v1/messages`, OTel, a durable usage sink, and a shared budget backend are implemented and covered by tests | **Met** | Streaming [ADR 0005](./docs/adr/0005-streaming-relay.md) (`crates/gateway/src/streaming.rs`); failover [ADR 0008](./docs/adr/0008-target-failover-and-circuit-scope.md); native routes [ADR 0012](./docs/adr/0012-native-provider-routes.md); telemetry [ADR 0007](./docs/adr/0007-telemetry-model.md); usage sinks [ADR 0009](./docs/adr/0009-durable-usage-sinks.md) (Postgres + OTLP); budgets [ADR 0010](./docs/adr/0010-shared-budget-backends-and-charging-policy.md) (Redis + Postgres, fail-closed). Whole workspace green under gate 3 below. |
| 2 | The supported-provider/compatibility contract and the usage schema are documented and versioned | **Met** | [`docs/compatibility.md`](./docs/compatibility.md) (routes, provider kinds, deferrals, `0.x` promise); [`docs/usage-schema.md`](./docs/usage-schema.md) + `UsageRecord::SCHEMA_VERSION = 1` + [`ops/postgres/usage_v1.sql`](./ops/postgres/usage_v1.sql); policy of record in [ADR 0015](./docs/adr/0015-zero-dot-x-compatibility-policy.md). |
| 3 | Security review complete with no known secret-exposure paths | **Met** | [`docs/security-review-2026-08-05.md`](./docs/security-review-2026-08-05.md). One finding (transport errors echoed the upstream URL's query/userinfo) found **and fixed** in the same PR, with a regression test. Two residual items recorded as accepted risk with follow-ups; neither is a known exposure. |
| 4 | Deployment guide + config reference + runbook published | **Met** | [`docs/deployment.md`](./docs/deployment.md), [`docs/configuration.md`](./docs/configuration.md), [`docs/observability.md`](./docs/observability.md) — each cross-checked against `config.rs`, `routes.rs`, and the telemetry module rather than the roadmap. |
| 5 | Compatibility, record/replay, and SSE soak suites pass in CI | **Met** | [ADR 0014](./docs/adr/0014-compatibility-and-soak-harness.md); `crates/gateway/tests/{compat,replay,soak}.rs` and `tests/compat/` (vendor SDKs pinned to `openai==2.50.0`, `anthropic==0.120.0`). Both lanes are required by the `CI-Success` aggregate in `.github/workflows/ci.yml`. |
| 6 | A signed, attested `0.x` beta release is published and its artifacts verify; go/no-go recorded | **Not met — by design** | The pipeline is verified as far as is possible without cutting a tag (see below). Publishing is the maintainer's act: tagging requires merging a release PR, and this PR deliberately does not seed a version. This document is the recorded go/no-go. |

Criterion 6 is the only open one, and it is open because it *should* be: it can
only be closed by the maintainer merging the release PR that release-please
opens once this lands.

## Release pipeline verification

Read end to end in `.github/workflows/release-please.yml`, plus what could be
executed locally.

| Stage | Verified | How |
| --- | --- | --- |
| Version + changelog | Wired | `release-please-config.json`: `release-type: simple`, `bump-minor-pre-major`, `bump-patch-for-minor-pre-major`, Cargo workspace version in `extra-files`, `CHANGELOG.md` sections per Conventional Commit type. A GitHub App token (falling back to `GITHUB_TOKEN`) makes the release PR trigger CI; the lockfile is re-synced onto the release PR so `--locked` stays green. |
| Release fan-out | Wired | `release-metadata` resolves the automatic path and the `workflow_dispatch` repair path, and rejects a dispatch whose ref is not the requested tag. |
| Binaries | Wired | Four targets — `x86_64-unknown-linux-gnu`, `x86_64-unknown-linux-musl`, `aarch64-apple-darwin`, `x86_64-pc-windows-msvc` — each built at the pinned toolchain from the tagged commit, packaged with a SHA-256 sidecar, an SPDX SBOM, and provenance + SBOM attestations. |
| Image | Wired **and exercised** | `docker build` + `ops/docker-smoke.sh` run locally against the built image: `healthz: ok`, `/v1/models` (probed with the platform gateway key) returns the example catalogue, `axond image smoke passed`. In the release job the same smoke runs against the *published* image **before** it is signed. |
| Signing | Wired | Keyless cosign over the digest, verified in-job against `SIGNER_IDENTITY`, which is anchored to this workflow file at `refs/heads/main` or `refs/tags/v<semver>` only. `gh attestation verify` then checks SLSA provenance. A broken chain fails the release rather than shipping quietly. |
| Supply chain | Wired | `deny.toml` fails on any advisory, yanked crate, unlisted license, or non-crates.io source, with no ignores; `dependency-audit.yml` re-runs it on a schedule. Gate 5 below is green. |
| crates.io | Wired **and dry-run** | `release-crates` runs last, gated on the tagged commit's `CI Success` and on every artifact lane, and publishes `gateway-core` → `gateway-transport` → `axond` through `ops/publish-crates.sh`. `cargo package --locked` and `cargo publish --dry-run --locked` pass in that order locally and in the `crates.io packaging` CI lane. The upload itself is untested until a token exists, and the first workflow-driven publish is the next release tag — `v0.3.0` predates the lane. See [Publishing to crates.io](#publishing-to-cratesio). |

No configuration error was found that warranted a fix. Two observations, both
maintainer calls rather than defects, are in the decisions section.

## Gates

All of these run locally at the reviewed tree:

| Gate | Result |
| --- | --- |
| `cargo fmt --all -- --check` | pass |
| `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` | pass |
| `cargo test --workspace --all-features --locked` | pass |
| `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features --locked` | pass |
| `cargo deny --locked --all-features check` | pass |
| `just docker-smoke` | pass |
| `just publish-dry-run` (package + publish dry-run, dependency order) | pass |

## Decisions the maintainer owns

1. **The first tag's version.** `.release-please-manifest.json` says `0.0.0` and
   `release-please-config.json` says `initial-version: 0.1.0`. Because the
   manifest already has an entry, `initial-version` does not apply: with
   `bump-patch-for-minor-pre-major`, the accumulated `feat` commits will produce
   **`v0.0.1`**, and only a commit marked breaking would produce `v0.1.0`.
   `v0.1.0` reads far better as "first beta" and matches the stated intent of
   `initial-version`, but getting it requires seeding the manifest — which this
   PR deliberately does not touch. Decide, then either accept `v0.0.1` or seed
   `0.1.0` in a separate, explicit commit.
2. **`bootstrap-sha: "HEAD"`.** The literal string is not a commit SHA, so it
   will not match and release-please will consider the full history for the
   first changelog. For a repo this young that is probably the desired outcome —
   the first `CHANGELOG.md` describes everything in the beta — but it is worth a
   conscious yes rather than an accident. Left unchanged.
3. **Publishing itself.** Merging this PR opens (or updates) the release PR.
   Merging *that* tags, builds, signs, and publishes. Neither is done here.

## After the tag

Close criterion 6 by verifying the published artifacts from outside CI, exactly
as an adopter would — the commands are in the
[deployment guide](./docs/deployment.md#running-the-container-image):

```bash
cosign verify --certificate-identity-regexp … --certificate-oidc-issuer … ghcr.io/litvue/axond@<digest>
gh attestation verify oci://ghcr.io/litvue/axond@<digest> --repo Litvue/axond --predicate-type https://slsa.dev/provenance/v1
sha256sum -c axond-<version>-x86_64-unknown-linux-musl.tar.gz.sha256
```

Then append the result and the date to this file.

## Publishing to crates.io

The decision of record is
[ADR 0025](./docs/adr/0025-crates-io-publication.md), which supersedes
[ADR 0004](./docs/adr/0004-ci-and-release-pipeline.md)'s wildcard rule and
extends its artifact list. This section is the runbook.

The workspace is published as **one release at one version**, in dependency
order:

1. `gateway-core`
2. `gateway-transport`
3. `axond`

The order is not cosmetic. Each package's registry dependency on the previous
one has to resolve for `cargo publish`'s own verification build and for any
external consumer, so `gateway-transport` cannot go up before `gateway-core`,
and `axond` — which depends on both — goes last. Publishing only the binary
package is not a release: its library dependencies would be unresolvable.

Internal dependencies therefore carry both a `path` and a `version` in
`[workspace.dependencies]`. release-please rewrites those two version strings
alongside `[workspace.package].version` (`release-please-config.json`
`extra-files`), so a bump cannot leave the registry requirements behind.
`ops/publish-crates.sh` refuses to publish if any package version or internal
requirement disagrees with the release version.

Those `extra-files` paths are load-bearing and fail quietly: release-please
resolves them with jsonpath-plus and its TOML updater only *warns*
(`No entries modified in …`) when a path matches nothing, so a mistyped path — or
a fourth publishable crate nobody registered — would leave a version behind and
break the release at the tag. Hyphenated keys do resolve in dot notation
(verified against the `release-please` version the pinned `@v5` action installs,
by running its own `GenericToml` updater over this `Cargo.toml`), and
`ops/publish-crates.sh` asserts on every run that each configured path resolves
to the release version and that every internal `path` + `version` dependency has
an entry.

**The shipped DDL is duplicated on purpose.** `ops/postgres/*.sql` stays the
operator contract — every doc, ADR, and runbook `psql -f` points there, and a
shipped file is never edited in place. Because `ops/` is outside
`crates/gateway/`, the packaged crate carries byte-identical copies under
`crates/gateway/sql/`, which is what the binary `include_str!`s. Change the
operator-facing file, then copy it across; `crates/gateway/tests/shipped_ddl.rs`
and `ops/publish-crates.sh` both fail on any drift or on a file that exists in
only one of the two directories, so a new `budget_v2.sql` is covered without
touching either gate.

**Names.** `gateway-core`, `gateway-transport`, and `axond` were all unclaimed on
crates.io on 2026-08-11 (`curl -s https://crates.io/api/v1/crates/<name>` →
`does not exist` for each). Registry names are first-come, so re-check
immediately before the first publish; no placeholder release has been made to
reserve them, deliberately.

### Gates before an upload

`release-crates` in `.github/workflows/release-please.yml` runs only when:

- a release was created for the tag (or the tag was passed to the
  `workflow_dispatch` repair path);
- the tagged commit's `CI Success` aggregate has concluded **success** — the job
  polls for it and refuses after 30 minutes rather than publishing blind;
- `release-success` is green, i.e. every binary target, the signed and attested
  image, and the release evidence all shipped.

It is the last lane on purpose: binaries and images can be rebuilt or replaced,
while a crates.io version is immutable — it can only be yanked, never replaced
or deleted.

### The first publish is the next tag

The already-released tags, `v0.3.0` included, predate this lane: their trees
contain no `ops/publish-crates.sh` and no `release-crates` job, and the
`workflow_dispatch` repair path checks the tag out before running it. So
**dispatching `v0.3.0` cannot publish these crates** — the job it would need does
not exist at that commit, and the packaging fixes this release depends on are not
in that tree either.

The bootstrap is therefore the *next* release tag cut after this lands: merge the
release PR, and the crates lane runs for the first time on that tag, after the
same CI and artifact gates as every later release. That is deliberate — there is
no "publish from main" shortcut, because a registry version that does not
correspond to a tag cannot be reproduced or re-verified afterwards.

If a release has to be bootstrapped before then, do it by hand from a clean
checkout of the tag, with the same script the workflow uses and the release
owner's own token:

```bash
git clone --depth 1 --branch <tag> https://github.com/Litvue/axond && cd axond
CARGO_REGISTRY_TOKEN=… ops/publish-crates.sh "${tag#v}"
```

The script's refusals are the gate in that case: misaligned versions, drifted
DDL copies, a missing token, or an unreadable registry all stop it before an
upload. Note that this only works for a tag whose tree contains the script — in
practice, this release or later.

### Token ownership and rotation

| Item | Value |
| --- | --- |
| Secret | `CARGO_REGISTRY_TOKEN`, scoped to the `crates-io` GitHub Actions environment on `Litvue/axond` |
| Owner | The Axond release owner (the maintainer who owns the crates.io `Litvue` team/owners entry) |
| Scope | `publish-new` and `publish-update` for these three crates only — never `yank`, never account-wide |
| Used by | `ops/publish-crates.sh`, invoked only from the `release-crates` job |
| Rotation | Every 12 months, on owner change, and immediately on any suspected exposure |

Crate ownership on crates.io should be a **team**, not a person: add the
organisation team as an owner (`cargo owner --add github:litvue:axond-release
<crate>`) for each of the three crates as soon as they exist, so a single
departing account cannot orphan the release.

To rotate:

1. Create a new scoped token on <https://crates.io/settings/tokens> (crate scope:
   the three crates; endpoint scope: publish only).
2. Update the `CARGO_REGISTRY_TOKEN` secret in the `crates-io` environment.
3. Revoke the old token on crates.io.
4. Confirm the next release publishes, or re-dispatch the release workflow for a
   tag that contains the crates lane — the publish is idempotent, so a re-run
   with the new token is safe even if nothing is left to upload.

The token never appears in a workflow run's logs: it is passed only as the
`CARGO_REGISTRY_TOKEN` environment variable of the publish step, and no step
echoes it. Because it lives in an environment, an environment protection rule
(required reviewer) can be added without touching the workflow.

### Recovering a partial crates.io release

A publish that dies between packages leaves the release half-shipped — say
`gateway-core` uploaded and the other two not. Nothing about that state can be
undone, so recovery is *resumption*, not repair:

1. Re-dispatch **Release** (`workflow_dispatch`) from the release tag, passing
   that tag as `release_tag`. Dispatching from any other ref is rejected, and
   the tag must be one whose tree contains this lane (see above).
2. `ops/publish-crates.sh` asks crates.io for each `name@version` first and
   skips what is already there, so the run uploads exactly the packages that are
   missing and reports the rest as `already present`.
3. If a publish call fails *after* the upload was accepted (a dropped response,
   two runs racing), the script re-checks the registry and treats a version that
   is now present as done rather than failing the release.

The same script is the manual path when Actions itself is unavailable: from a
clean checkout of the tag, `CARGO_REGISTRY_TOKEN=… ops/publish-crates.sh
<version>`. It is safe to re-run.

What *not* to do: never bump the version to "get past" a partially published
release, and never publish a mutated tarball at a version that already exists —
it is rejected, and it desynchronises the tag from the registry. If a published
version is genuinely broken, `cargo yank --version <version> <crate>` (yank all
three, in reverse dependency order) and ship a fix as the next patch release.

### Verifying a published release

From outside CI, as an adopter would:

```bash
cargo install axond --version <version> --locked
cargo add gateway-core@<version> gateway-transport@<version>   # in a scratch crate, then `cargo build`
```

## Known gaps carried into beta

None of these blocks the release; all are stated so adopters are not surprised.

- `/readyz` reports process liveness only — it does not probe the usage sink,
  the budget store, or any provider. Documented in the
  [deployment guide](./docs/deployment.md#health-and-readiness).
- Inbound key material is held as a `String` rather than a `SecretString`; no
  exposure path exists today, and hardening is a follow-up (same section).
- Only `linux/amd64` images are published; `arm64` is post-beta.
- `/v1/responses` and cross-provider translation remain typed deferrals.
- The crates.io upload path is wired, gated, and dry-run, but **no version has
  been published yet**: it needs `CARGO_REGISTRY_TOKEN` in the `crates-io`
  environment, and the first workflow-driven publish is the next release tag
  cut after this lane lands — `v0.3.0` predates it. Until that secret exists the
  `release-crates` job fails loudly with that reason instead of silently
  skipping, so a release cannot appear to have published when it did not.
