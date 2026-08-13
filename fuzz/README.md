# Fuzzing Axond

Axond parses two kinds of untrusted input: what an operator's configuration file
says, and what a caller puts in a request. This project fuzzes both, on the
parsers the process actually runs.

| Target | What it drives | Why |
| --- | --- | --- |
| `config_toml` | `Config::from_toml_str` and the whole validation graph | Boot and every `SIGHUP` reload re-parse a file the gateway does not control |
| `token_verify` | `axt1.` JWS decoding, key selection, signature, and every claim check | A minted token is the one credential an attacker can shape freely |
| `credentials_query` | `GET /v1/credentials/status?namespaces=…` parsing | Hand-rolled percent-decoding, so malformed escapes, duplicate keys, empty values, and oversized inputs all land here |

The properties each target asserts live in [`src/lib.rs`](./src/lib.rs): a
parser returns rather than panicking, a refusal is a typed value the gateway
could answer with, and what it accepts stays inside the bounds the request path
relies on — including that a signature from one namespace's signer never
verifies into another.

Runs are hermetic. The seam the targets call
([`crates/gateway/src/fuzz_seam.rs`](../crates/gateway/src/fuzz_seam.rs)) builds
its verifiers from a configuration compiled into the binary with synthetic key
material, so nothing here opens a socket, reads a file, or holds a real secret.

## Layout

- `fuzz_targets/` — the `cargo fuzz` entry points, one per target.
- `src/lib.rs` — the target bodies and their assertions, shared with the smoke.
- `src/bin/smoke.rs` — the bounded, deterministic seed replay CI requires.
- `seeds/<target>/` — the committed corpus. `corpus/` and `artifacts/` are
  generated and ignored.

This is deliberately its own Cargo workspace: the targets need nightly and a
sanitizer runtime, and they reach `axond`'s parsers through a seam only
`--cfg fuzzing` compiles, which no other consumer can switch on. Nothing at the
repository root builds it, so this project runs its own `cargo fmt --check` and
`cargo clippy -- -D warnings` in the same CI lane as the smoke.
[ADR 0048](../docs/adr/0048-fuzzing-the-untrusted-input-parsers.md) records the
decision and what it costs.

That flag reaches every dependency rather than just `axond` — Cargo has no
per-crate rustflags, and it is what `cargo fuzz` sets anyway. Since a crate is
allowed to weaken cryptography under it, the smoke's first act is
`assert_signature_verification_is_real`: the seam mints a token, verifies it, and
must then refuse that token with a bit flipped in each of its three JWS segments.
That is the durable check; [`.cargo/config.toml`](./.cargo/config.toml) records
the lockfile audit behind it.

## The smoke (stable, bounded, on every pull request)

```bash
just fuzz-smoke
```

Proves signature verification is live, then replays every seed plus fixed
derivations — truncations, single-byte flips, one oversized repetition — and a set
of tokens minted at replay time. It fails on a panic, on an input slower than its
budget, or on an allocation past a hard cap enforced by the binary's own global
allocator. It also fails if the corpus stops reaching a spread of outcome
classes, so the lane cannot go green by refusing everything at the door.

Every command here runs `--locked`, and this workspace has its own lockfile that
records the gateway crates through the path dependency on `axond`. So a pull
request that adds or bumps a dependency of any `crates/` member has to refresh it:

```bash
just fuzz-lock   # cargo fetch in fuzz/, recording the change and nothing else
```

The lane also lints the seam (`just fuzz-seam-clippy`), because
`crates/gateway/src/fuzz_seam.rs` is `#![cfg(fuzzing)]`: the root clippy builds it
as an empty library, and this workspace's clippy skips `axond` as a path
dependency, so nothing else here has it under `-D warnings`. And it checks the
lockfile first and says so, rather than leaving a bare `--locked`
error to interpret. Releases are handled for you: `release-please.yml` syncs both
lockfiles when the workspace version changes, using this same command for this
one.

## Coverage-guided runs (nightly toolchain)

```bash
cargo install cargo-fuzz --locked
just fuzz config_toml         # one target, a bounded local run
just fuzz-all                 # every target, the way the scheduled lane does
```

`just fuzz` copies `seeds/<target>/` into `corpus/<target>/` first, so a local
run starts where the committed corpus left off.

## When a run finds something

1. `cargo fuzz tmin <target> artifacts/<target>/<crash>` to shrink it.
2. Commit the minimized reproducer to `seeds/<target>/` with a name that says
   what it is. The smoke then covers it on every pull request.
3. Fix the parser. A finding is a bug in the parser, not in the fuzzer, unless
   the assertion itself was wrong — in which case fix the assertion and say why
   in the commit.

Scheduled runs keep their corpus between runs and upload both the corpus and any
artifact, so a finding is reproducible from the workflow run alone.
