# 41. Fuzzing the untrusted-input parsers

Date: 2026-08-12

## Status

Accepted

## Context

Axond parses input it does not control on two paths. An operator's TOML is parsed
at boot and again on every reload ([ADR 0011](./0011-config-hot-reload.md)), and a
caller's credential and query string are parsed on every request before anything
authenticates them ([ADR 0013](./0013-inbound-auth-fails-closed.md),
[ADR 0016](./0016-minted-inbound-identity-and-principal-stores.md)). The unit
tests around those parsers are example-based: they assert the refusals we thought
of. Nothing looked for the input nobody thought of, which for a parser reached
before authentication is the interesting half — a panic there is a remote
denial of service on a request that was never entitled to anything.

[ADR 0014](./0014-compatibility-and-soak-harness.md) built the harness for what
the *process* does with well-formed traffic. This is the other axis: what a
parser does with traffic that is not.

Coverage-guided fuzzing does not fit the shape of the existing CI. It needs a
nightly toolchain and sanitizer runtimes, its dependencies are not ones the
published crates should acquire, and a run is unbounded by construction, which is
the opposite of what a required pull-request lane can be.

## Decision

A fuzzing program in two halves, over three targets: `Config::from_toml_str` plus
its validation graph, `axt1.` token verification, and the credential-query
parser.

### An out-of-tree workspace, not a workspace member

`fuzz/` is its own Cargo workspace with its own lockfile. Nothing at the
repository root builds, lints, packages, or audits libFuzzer, and no fuzzing
dependency can reach a published crate. The cost is that root `--workspace`
commands do not reach `fuzz/`, so the fuzz lane runs `cargo fmt --check` and
`cargo clippy -- -D warnings` inside it explicitly, and the release lockfile sync
updates both lockfiles.

### A seam gated on a cfg, not a Cargo feature

The targets need private items. They reach them through a second library target
over the same module sources (`crates/gateway/src/fuzz_seam.rs`), named
`axond_fuzz_seam` rather than `axond` — a library sharing the binary's crate name
takes its place in `cargo doc`, which would leave every module under `src/`
unlinted while the documentation lane still passed, and `doc = false` removes the
crate from the documentation altogether. `ops/check-docs.py` keeps the names
apart. It is gated
`#![cfg(fuzzing)]` — set by `fuzz/.cargo/config.toml` and by `cargo fuzz`, and by
nothing at the repository root:

```rust
#![cfg(fuzzing)]                      // empty crate for every other consumer
pub fn config_from_toml_str(input: &str) -> Result<ConfigShape, Rejection>
pub fn credentials_query_namespaces(q: Option<&str>) -> Result<Option<String>, Rejection>
pub fn verify_token(credential: &str) -> Result<Option<VerifiedToken>, Rejection>
```

A Cargo feature was the obvious alternative and is worse in the way that matters:
a feature is switchable by any dependant and is compiled by `--all-features`, so
the seam would become part of what the crate offers. A cfg the published crate
never sets keeps the published API exactly as it was — the library target is an
empty crate everywhere except under fuzzing — at the price of two consequences we
accept: `ops/api-compat.py` skips a library whose crate-level `#![cfg(fuzzing)]`
removes it, because there is no API to compare and no library in the crates.io
baseline to compare against; and the seam must declare the same modules as
`main.rs`, which `tests/fuzz_seam.rs` holds
(`the_fuzz_seam_declares_every_module_the_binary_does`) so drift fails a test
rather than silently fuzzing a different crate than the one shipped. A cfg also
hides the seam from every lint lane — the root clippy compiles it as an empty
library and the fuzz workspace skips `axond` as a path dependency — so the `Fuzz
smoke` lane lints it with the cfg on (`just fuzz-seam-clippy`), and the seam is
held to the same `-D warnings` as the code it drives.

The seam exposes a small typed `Rejection` rather than the gateway's own error
types, so internal error refactoring does not ripple into the fuzz project.

### A required bounded replay, and a scheduled unbounded run

The pull-request lane (`Fuzz smoke`, required) replays every committed seed and
fixed derivations of it on the pinned stable toolchain, under a per-input and
total time budget and a heap cap enforced by its own global allocator, so an
uncontrolled allocation is a diagnosis rather than an OOM kill. Each target also
declares how many distinct outcome classes its corpus must still reach, so the
lane cannot go green because a parser began refusing everything at the door.
Nine token scenarios are minted *at replay time*, because a committed token is
expired by the time it is replayed and every claim check behind expiry would
otherwise be unreachable in the required lane.

The scheduled lane (`fuzz.yml`, nightly and dispatchable) runs `cargo fuzz` per
target on nightly with a wall-clock budget and libFuzzer's own RSS and
malloc limits, restores and saves the accumulated corpus, and retains the corpus
and any reproducer as artifacts on success and failure alike.

### The cfg is graph-wide, so verification is proven rather than assumed

Cargo has no per-crate rustflags, so `--cfg fuzzing` reaches every dependency —
exactly as it does under `cargo fuzz`. A crate is allowed to change behaviour
under that cfg, and some weaken cryptography deliberately to help fuzzers, which
would make every `token_verify` assertion vacuous. An audit of `fuzz/Cargo.lock`
says none of ours do, and an audit is not a gate: the required smoke therefore
begins with `assert_signature_verification_is_real`, which mints a token through
the seam, verifies it, and requires a refusal for the same token with a bit
flipped in each of its three JWS segments. A dependency bump that stubbed the
verifier fails the lane rather than quietly hollowing it out.

### Hermetic, with synthetic material

The seam's verifiers come from a configuration compiled into the target with
synthetic key material and an `.invalid` audience. No target opens a socket, reads
a file, or holds real key material, which is also what makes the corpora
publishable.

### State tier

Tier 0. The fuzz targets construct parsers and verifiers in-process from
compiled-in values; no target reaches Redis or Postgres, and none is reachable
from a deployed binary. No deployment's tier changes.

## Consequences

- A panic, hang, or unbounded allocation in a parser under test is a merge
  blocker rather than an incident, and a fixed finding becomes a committed seed,
  so the required lane replays that exact input from then on.
- The published `axond` crate carries a library target that is an empty crate.
  Nothing consumes it, `cargo doc` documents nothing from it, and the
  compatibility gate ignores it — but it exists, which is a cost of not widening
  the API by feature instead.
- Two lockfiles must move together at a release. The release lockfile sync does
  it; if it stops, the required fuzz lane fails `--locked` on every pull request.
- A new module in the binary must be added to the seam. The drift test says so at
  test time, which is the point.
- Nightly toolchain and `cargo-fuzz` are pinned in the scheduled lane only, so
  the required lanes stay on the pinned stable toolchain.

## Alternatives considered

- **Fuzz targets inside the root workspace.** Simplest to wire, and it puts
  libFuzzer into the graph the published crates and every supply-chain gate see.
- **Make the parsers public.** Zero machinery, and it makes a request-path detail
  a compatibility surface — the opposite of
  [ADR 0015](./0015-zero-dot-x-compatibility-policy.md)'s narrow-surface stance.
- **Coverage-guided fuzzing on pull requests.** Either too short to explore or
  too long to require, and non-deterministic either way. The corpus replay is the
  deterministic residue of the scheduled runs, which is what a required lane can
  be.
