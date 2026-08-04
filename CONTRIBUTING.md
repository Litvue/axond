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

## Conventions

- **Warnings are errors.** CI runs clippy with `-D warnings`; keep it clean.
- **Never commit secrets.** Credentials are referenced by env-var name in
  config; real keys and `axond.toml` are gitignored.
- **Keep `gateway-core` I/O-free.** No HTTP client, no runtime, no config, no
  secrets in the core crate — that is what keeps provider-wire logic testable in
  isolation.
- **Significant decisions get an ADR** in `docs/adr` in the same PR.

## License

By contributing, you agree that your contributions are dual-licensed under
Apache-2.0 and MIT, matching the project (inbound = outbound).
