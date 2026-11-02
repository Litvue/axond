# 69. Retire the Rust gateway

Date: 2026-11-02

## Status

Accepted. Supersedes section 1 of [ADR 0066](./0066-typescript-hono-extension-contract.md).

## Context

ADR 0066 kept the crates under `crates/` as the conformance oracle "until a later minor makes the TypeScript build the release binary and the release image." That minor never shipped, but the TypeScript gateway already serves Litvue production: Litvue/litvue#2974 (2026-10-21) points the production AI gateway at the `axond-typescript` app, and the Rust app runs only as a rollback.

The project now carries two implementations of one contract. Since 2026-10-15 several commits on `main` were paired upkeep: Rust advisories (rustls, webpki-roots, tokio) on one side and npm audit fixes on the other. Releases build two images, the README and crates.io still advertise Rust, and operators see a different binary than the one Litvue runs.

Production traffic is too small to argue for Rust on performance. Over 2026-10-19 to 2026-11-02, `litvue-production-traces` records 136 inference requests on the TypeScript gateway (p50 about 0.8 to 1.1 s, dominated by the upstream provider) and 453 management requests (p50 10 ms, p95 25 ms).

## Decision

1. **TypeScript is the only implementation.** The last Rust release is the 0.7.x line. The next minor makes the TypeScript build the release binary (`bun build --compile`) and the release image.
2. **The oracle is the test suite, not a second gateway.** `tests/compat`, `tests/compat-ts`, and the byte-faithful streaming fixtures define parity. They run against the TypeScript gateway only.
3. **Rust code leaves `main`.** `crates/`, `fuzz/`, the Cargo workspace files, the Rust Dockerfile, and the Rust-only CI, soak, capacity, endurance, and release steps are removed after the TypeScript artifact ships. The Rust tree stays in git history and in the 0.7.x tags.
4. **crates.io stops.** No new `axond` crate versions are published. Existing versions are not yanked: a yank breaks existing lockfiles and withdraws nothing.
5. **The Postgres DDL follows TypeScript.** The TypeScript Store schema becomes the shipped schema. `ops/postgres/*.sql` is replaced or removed with the Rust tree.

Sections 2 to 7 of ADR 0066 (host-agnostic library, extension authors, shared state, settlement, SQLite, admission) are unchanged.

### State tier

No change. The TypeScript gateway already requires the same Tier 2 Store (SQLite or Postgres) as Rust. No existing deployment's tier rises.

## Consequences

- One implementation to patch, release, and document. Rust advisories stop being Axond work.
- Rollback to Rust after the Litvue infra change means pinning the unmaintained 0.7.x Rust image. Litvue removes its Rust container app and the `axond` service database in a separate Litvue/litvue change.
- The compiled TypeScript binary is larger than the static musl Rust binary. Container deployments are unaffected. The size gap has not been measured.
- `cargo install axond` stops receiving updates. Operators move to the release binary, the image, or the library.
- Behavior that only Rust had (the per-replica `tenant_concurrency_exceeded` limiter, `axt1.` rejection wording) is now defined by the TypeScript gateway and the parity contract. Litvue/axond#562 (Rust vs TypeScript byte differences) becomes moot once Rust is gone.

## Open questions

- Does the TypeScript image take over the `ghcr.io/litvue/axond` name, or keep `axond-typescript`? Litvue's `infra/modules/axond/variables.tf` validates both names, so either needs a matching Litvue change.
- Do `fuzz/` targets get TypeScript equivalents, or does the conformance suite cover untrusted-input parsing well enough?
