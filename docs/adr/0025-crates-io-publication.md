# 25. Publishing the workspace to crates.io

Date: 2026-08-11

## Status

Accepted historically.

Superseded in part on 2026-09-04: crates.io ships **only** the `axond` binary
crate. `gateway-core` and `gateway-transport` remain unpublished workspace
members. Historical versions already on the registry are not yanked. The rest of
this record — duplicated shipped DDL, an idempotent publish script, a packaging
dry-run in CI, publish-last-because-immutable — still holds for that one crate.

Supersedes the supply-chain wildcard rule and extends the release-artifact list
of [ADR 0004](./0004-ci-and-release-pipeline.md).

## Context

ADR 0004 sized the release fan-out to a single binary and a single image: the
artifacts of a tagged commit were the cross-platform binaries, the signed image,
and their attestations. Consumers were expected to take one of those.

That leaves two audiences unserved. `cargo install axond` is the idiomatic way to
get a Rust binary, and `gateway-core` and `gateway-transport` are library crates —
I/O-free provider logic and the transport layer — that other programs have reason
to depend on directly. Neither is reachable from a GitHub release.

Publishing them makes the release *irreversible* for the first time. A container
tag can be re-pushed and a GitHub release edited, but a crates.io version can only
be yanked, never replaced or reused. That single property drives every decision
below, and it also breaks two things ADR 0004 recorded as settled:

* **Wildcards.** ADR 0004 chose `wildcards = "allow"` because the workspace's own
  path dependencies carry no version and read as wildcards. crates.io rejects a
  wildcard dependency on publish, so that policy is no longer merely lax — it
  hides a condition that fails the release at the tag.
* **Package boundaries.** `axond` embeds its Postgres DDL with `include_str!` from
  `ops/postgres/`, which is outside the crate directory and therefore outside the
  `.crate` tarball. The workspace build resolves it; a published package cannot.
  But `ops/postgres/` is an operator interface — the deployment guide points there,
  ADR 0009 forbids editing shipped DDL in place, and #126 requires
  `ops/postgres/budget_v2.sql` — so it cannot move.

## Decision

**One package:** `axond`. Every git workspace member, including the `axond`
crate directory, is `publish = false`. `cargo publish -p axond` from the
workspace therefore refuses. The registry tarball is assembled by
`ops/stage-axond-crate.py`, which flattens unpublished internals into a
standalone crate, and `ops/publish-crates.sh` publishes that. Direct registry
dependencies in that tarball are pinned to the versions this workspace already
locked; transitives resolve at pack time. `cargo install axond --locked`
therefore compiles from that package alone.

**Internal path dependencies omit a `version`.** A version here would let an
accidental `cargo publish -p axond` from the workspace rewrite the dep to a
stale crates.io copy. Consequently **`wildcards = "deny"`** remains in
`deny.toml` for registry dependencies, with **`allow-wildcard-paths = true`**
for those unpublished internals, replacing ADR 0004's `"allow"`. release-please
bumps `[workspace.package].version` from one release, and because its TOML
updater only *warns* when a JSONPath matches nothing, `ops/publish-crates.sh`
asserts that path is covered and that no internal path crate has reacquired a
version pin.

**The shipped DDL is duplicated, not moved.** `ops/postgres/*.sql` remains the
operator-facing contract and the target of every doc link;
`crates/gateway/sql/*.sql` holds byte-identical copies that exist only so the
package can embed them, and is what `include_str!` reads. The duplication is a
drift hazard, so two deterministic gates enforce byte identity over the *union*
of the two directories — `crates/gateway/tests/shipped_ddl.rs` in the test lane,
and `ops/publish-crates.sh` before any packaging or upload. Both cover a newly
added file, such as `budget_v2.sql`, without being extended. The rule for
maintainers is one-directional: edit `ops/postgres/`, then copy.

**Publication is one script, and it is idempotent.** `ops/publish-crates.sh`
queries the registry for `axond` and skips a version that is already present,
re-checks after a failed upload (an accepted upload with a lost response is done,
not failed), and refuses to proceed on a misaligned version, drifted DDL, an
unknown registry status, or a missing token. That makes it the resume path for a
release that failed after the other artifacts, rather than a command that must
not be run twice.

**Release artifacts of a tagged commit therefore also include** the `axond`
crates.io version, published by a `release-crates` job that runs *last*: after
`release-success` (all binaries, plus the signed and attested image) and after
polling the tagged commit's own `CI Success` check. `CARGO_REGISTRY_TOKEN` lives
in a protected `crates-io` GitHub environment and is passed only to the publish
step. **CI gains a `crates.io packaging` lane** (`ops/publish-crates.sh
--dry-run`, required by `CI Success`), so packaging breakage surfaces on the pull
request that causes it and not at the tag.

Tags cut before this lane exists — `v0.3.0` and earlier — check out a tree with
neither the job nor the script, so the bootstrap is the next release tag, gated
identically. There is deliberately no publish-from-main path: a registry version
with no corresponding tag cannot be reproduced afterwards.

### State tier

Tier 0. This is build-and-release tooling; it adds no runtime dependency and does
not change what a deployment requires. The DDL that Tier 2 deployments apply is
unchanged and still applied from `ops/postgres/`.

## Consequences

- The release becomes partially irreversible: a wrong version can be yanked but
  never replaced, so the gates before the upload matter more than the ones after
  it, and the publish lane is ordered last for that reason.
- Two copies of every shipped DDL file exist. Editing the packaged copy directly
  is a mistake the gates catch, but the duplication is real and permanent for as
  long as `axond` is published from a crate that cannot reach `ops/`.
- Deleting or renaming a published crate name is no longer possible, so `axond`
  (and the historical `gateway-core` / `gateway-transport` names already on the
  registry) stay part of the public interface even though only `axond` is still
  published.
- A partial release is recoverable by re-dispatching the release for the tag,
  which is only true for tags whose tree contains the lane.
- Until the `crates-io` environment holds a token, `release-crates` fails loudly
  rather than skipping, so a release cannot appear published when it is not.
