# Contributing to Axond

Thanks for your interest. Axond is early — the architecture is settling, so
opening an issue to discuss a change before a large PR is appreciated.

Found a vulnerability? Do not open an issue or a pull request — follow
[`SECURITY.md`](./SECURITY.md) and report it privately.

Changing authentication, namespace scoping, secret handling, model entitlement,
durable schema or telemetry, or the release workflows? Start from the
[threat-model review triggers](./docs/security/threat-model-review.md): they name
the regression tests your change owes, when it needs an ADR or a threat-model
update, and what to say about release impact.

## Development

Requires the toolchain pinned in [`rust-toolchain.toml`](./rust-toolchain.toml).
That pin is newer than the project's minimum supported Rust version, which is
`rust-version` in [`Cargo.toml`](./Cargo.toml); `just msrv` builds the workspace
on that floor the way CI does.

```bash
just check      # fmt --check, clippy -D warnings, tests, the fuzz smoke, docs,
                # supply chain, packaging, MSRV, public-API compatibility, and
                # workflow policy — the CI gates
just run        # run against ./axond.toml
just compat     # run the Python SDK compatibility lane
just compat-ts  # run the TypeScript SDK compatibility lane
just fuzz-smoke # replay the committed fuzz corpora (see fuzz/README.md)
just msrv       # build on the declared minimum supported Rust version
just api-compat # semver-check the published library crates against crates.io
just workflow-policy
                # check the Action pins, workflow permissions, and the release
                # signer restriction
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

The SDK compatibility lane uses the hash-pinned lockfile at
[`tests/compat/requirements.txt`](./tests/compat/requirements.txt). Refresh it
with `just compat-lock`; the recipe resolves from
[`requirements.in`](./tests/compat/requirements.in) while excluding releases
published less than seven days ago. The refresh requires
[`uv`](https://docs.astral.sh/uv/getting-started/installation/) on `PATH`.
Review the generated diff, then run `just compat`.

The same claim is made through the vendors' Node SDKs by
[`tests/compat-ts`](./tests/compat-ts), which pins the SDKs, the TypeScript
toolchain, and the Node runtime exactly and type-checks the calls before running
them. `ops/compat-ts-pins.py` (`just compat-ts-pins`) enforces those pins; how to
bump an SDK is in [that lane's README](./tests/compat-ts/README.md).

Touching a parser that reads untrusted input — configuration, minted tokens, or
a query string? [`fuzz/`](./fuzz/README.md) is a separate Cargo workspace, so the
root checks skip it; `just fuzz-smoke` runs the required pull-request replay on
stable, and `just fuzz <target>` runs a bounded coverage-guided run once
[`cargo-fuzz`](https://github.com/rust-fuzz/cargo-fuzz) and a nightly toolchain
are installed. A reproducer belongs in `fuzz/seeds/<target>/` in the same PR as
the fix. Because that workspace locks its own graph and depends on `axond` by
path, adding or bumping a dependency of any `crates/` member leaves
`fuzz/Cargo.lock` stale: run `just fuzz-lock` and commit it, which the `Fuzz
smoke` lane asks for by name before it replays anything.

Changing anything public in `gateway-core` or `gateway-transport`? Run the
compatibility gate, which compares the crates against the versions on crates.io
(needs [`cargo-semver-checks`](https://github.com/obi1kenobi/cargo-semver-checks)
and network access):

```bash
cargo install cargo-semver-checks --locked
just api-compat
```

The gate itself runs on `python3` 3.10 or newer, so `just api-compat-self-test`
works offline and without `cargo-semver-checks`; `just msrv` needs `rustup`, and
fails rather than measuring the floor with a newer compiler.

A break fails CI. If it is intentional, add a reviewed entry to
[`ops/api-compat-overrides.toml`](./ops/api-compat-overrides.toml) in the same PR
and follow
[the release runbook](./docs/maintainers/releasing.md#public-api-compatibility);
raising the MSRV follows
[the same runbook](./docs/maintainers/releasing.md#rust-version-floor).

## Conventions

- **Warnings are errors.** CI runs clippy with `-D warnings`; keep it clean.
- **Public API breaks and MSRV bumps are minor releases.** Both are covered by
  [the compatibility contract](./docs/compatibility.md) and gated in CI; neither
  is a patch.
- **Report vulnerabilities privately.** See [`SECURITY.md`](./SECURITY.md).
- **Workflow steps are pinned to commit SHAs.** Every `uses:` names a full commit
  SHA with the version in a trailing comment; a tag or branch ref fails the
  `workflow-policy` lane. Dependabot proposes the bumps — see
  [ADR 0035](./docs/adr/0035-pinned-github-actions.md) and
  [the runbook](./docs/maintainers/releasing.md#workflow-action-pins).
- **Never commit secrets.** Credentials are referenced by env-var name in
  config; real keys and `axond.toml` are gitignored.
- **Keep `gateway-core` I/O-free.** No HTTP client, no runtime, no config, no
  secrets in the core crate — that is what keeps provider-wire logic testable in
  isolation.
- **Significant decisions get an ADR** in `docs/adr` in the same PR. Start from
  the [ADR template](./docs/adr/template.md), including its required state-tier
  declaration.
- **A fired security trigger is answered in the PR body.** Which trigger, the
  tests that hold the property, whether the threat model or an ADR changed, and
  the release impact — see the
  [threat-model review triggers](./docs/security/threat-model-review.md).
  "No trigger fired" is a fine answer; an unstated one is not.

## License

By contributing, you agree that your contributions are dual-licensed under
Apache-2.0 and MIT, matching the project (inbound = outbound).
