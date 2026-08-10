# Contributing to Axond

Thanks for your interest. Axond is early — the architecture is settling, so
opening an issue to discuss a change before a large PR is appreciated.

## Development

Requires the toolchain pinned in [`rust-toolchain.toml`](./rust-toolchain.toml).

```bash
just check      # fmt --check, clippy -D warnings, tests — the CI gates
just run        # run against ./axond.toml
```

Or without `just`:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
```

The Redis/Postgres-backed tests skip when their services are not configured.
To run them locally, start pinned service containers and set the same variables
as CI:

```bash
docker run -d --name axond-test-redis -p 6399:6379 redis:7.4.2-alpine
docker run -d --name axond-test-postgres -e POSTGRES_PASSWORD=axond-ci \
  -p 55432:5432 postgres:17.6-alpine
AXOND_TEST_REDIS_URL=redis://127.0.0.1:6399 \
AXOND_TEST_POSTGRES_DSN=postgres://postgres:axond-ci@127.0.0.1:55432/postgres \
AXOND_TEST_REQUIRE_SERVICES=1 cargo test -p axond --all-features --locked
```

## Conventions

- **Warnings are errors.** CI runs clippy with `-D warnings`; keep it clean.
- **Never commit secrets.** Credentials are referenced by env-var name in
  config; real keys and `axond.toml` are gitignored.
- **Keep `gateway-core` I/O-free.** No HTTP client, no runtime, no config, no
  secrets in the core crate — that is what keeps provider-wire logic testable in
  isolation.
- **Significant decisions get an ADR** in `docs/adr` in the same PR. Start from
  the [ADR template](./docs/adr/template.md), including its required state-tier
  declaration.

## License

By contributing, you agree that your contributions are dual-licensed under
Apache-2.0 and MIT, matching the project (inbound = outbound).
