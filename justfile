# Axond developer commands. Run `just` to list.

default:
    @just --list

# Format, lint (warnings = errors), test, docs, supply-chain, and release
# packaging — the CI gates.
check: fmt-check clippy test docs deny publish-dry-run msrv api-compat workflow-policy

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all -- --check

clippy:
    cargo clippy --workspace --all-targets --all-features --locked -- -D warnings

test:
    cargo test --workspace --all-features --locked

docs:
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features --locked

# Validate documentation links, release markers, route coverage, the release
# artifact matrix, and Compose variants.
docs-check:
    python3 ops/check-docs.py --self-test
    python3 ops/check-docs.py
    python3 ops/check-release-config.py --self-test
    python3 ops/check-release-config.py
    sh -n install.sh
    bash -n ops/publish-image-index.sh
    bash -n ops/verify-image-evidence.sh
    bash ops/check-compose-platform.sh
    bash ops/check-installer-download.sh
    bash ops/check-index-promotion.sh
    AXOND_VERSION=0.0.0 AXOND_TARGET=x86_64-unknown-linux-musl AXOND_INSTALL_DRY_RUN=1 sh install.sh | grep -F 'axond-0.0.0-x86_64-unknown-linux-musl.tar.gz'
    AXOND_VERSION=0.0.0 AXOND_TARGET=aarch64-unknown-linux-musl AXOND_INSTALL_DRY_RUN=1 sh install.sh | grep -F 'axond-0.0.0-aarch64-unknown-linux-musl.tar.gz'
    docker compose --env-file ops/compose/env.example config --quiet
    docker compose --env-file ops/compose/env.example -f docker-compose.yml -f docker-compose.build.yml config --quiet
    AXOND_QUICKSTART_CONFIG=./ops/compose/axond.stateful.toml docker compose --env-file ops/compose/env.example -f docker-compose.yml -f docker-compose.stateful.yml --profile stateful config --quiet

# The declared MSRV floor: policy consistency, then a build on that toolchain.
# Installs the floor toolchain through rustup; the pinned newer toolchain in
# rust-toolchain.toml is untouched.
msrv:
    ops/msrv-gate.sh

# Public Rust API compatibility for the published library crates, against the
# versions on crates.io. Needs cargo-semver-checks and network. Runs on any
# python3 from 3.10 up, the same floor as the provider-SDK lockfile.
api-compat:
    ops/api-compat.py

# The parts of the API gate that need neither network nor cargo-semver-checks.
api-compat-self-test:
    ops/api-compat.py --self-test

# Workflow supply-chain policy: SHA-pinned Actions, least-privilege permissions,
# and the release signer restriction. `actionlint` additionally lints the
# workflow definitions; it is downloaded at a pinned, checksummed version.
workflow-policy:
    ops/workflow-policy.py --self-test
    ops/workflow-policy.py
    ops/dependabot-labels.sh --self-test
    ops/dependabot-labels.sh

actionlint:
    ops/actionlint.sh --self-test
    ops/actionlint.sh

# Supply-chain policy: advisories, licenses, sources (see deny.toml).
deny:
    cargo deny --locked --all-features check

# Package and publish-dry-run the three crates in dependency order. No upload,
# no token; the real publish only ever runs from the tagged release workflow.
publish-dry-run:
    ops/publish-crates.sh --dry-run

# Provider-SDK compatibility: the vendors' own Python SDKs against a real
# axond and the committed fixtures. Offline; needs python3.
compat:
    cargo build -p axond --locked
    python3 -m venv target/compat-venv
    target/compat-venv/bin/pip install --quiet --require-virtualenv --require-hashes -r tests/compat/requirements.txt
    AXOND_BIN="$(pwd)/target/debug/axond" target/compat-venv/bin/python -m pytest tests/compat -q

# The same compatibility claim through the vendors' Node SDKs, which also
# type-checks the calls against the SDKs' own definitions. Offline; needs the
# Node version pinned in tests/compat-ts/.nvmrc.
compat-ts:
    cargo build -p axond --locked
    ops/compat-ts-pins.py
    cd tests/compat-ts && npm ci --ignore-scripts
    cd tests/compat-ts && AXOND_BIN="$(git rev-parse --show-toplevel)/target/debug/axond" npm test

# Pin policy for the TypeScript lane: exact versions, locked hashes, pinned Node.
compat-ts-pins:
    ops/compat-ts-pins.py --self-test
    ops/compat-ts-pins.py

# Refresh the hash-pinned provider-SDK lockfile, excluding releases newer than a week.
compat-lock:
    uv pip compile --generate-hashes --universal --python-version 3.10 --exclude-newer "$(python3 -c 'from datetime import datetime, timedelta, timezone; print((datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ"))')" -o tests/compat/requirements.txt tests/compat/requirements.in

# The heavy SSE soak: hundreds of concurrent streams with cancels and drops.
# The short subset runs in `just test`; this is the long one.
soak:
    AXOND_SOAK=1 cargo test --locked --all-features --test soak -- --nocapture

# The heavy capacity profiles, writing result artifacts to target/capacity/heavy.
# The reduced tier of the same driver runs in `just test` (ADR 0033).
# One tier at a time: two tiers offering load at once measure each other.
capacity:
    AXOND_CAPACITY=1 cargo test --locked --all-features --test capacity -- --nocapture --test-threads=1

# The twelve-hour mixed-workload endurance soak, writing its result and time
# series to target/endurance/soak. The smoke tier of the same driver runs in
# `just test`. Pass a shorter duration in milliseconds to dispatch a shorter run:
# `just endurance 2400000`.
# The soak test by name: the rest of the binary — the smoke tier, the sequential
# regression, and the deterministic checks — belongs to `just test`, and running
# it again here would offer the dispatched hours a second workload to share.
endurance duration_ms="":
    AXOND_ENDURANCE=1 AXOND_ENDURANCE_DURATION_MS={{ duration_ms }} cargo test --locked --all-features --test endurance -- the_endurance_soak_tier_qualifies_and_publishes_its_evidence --exact --nocapture --test-threads=1

# The heavy rollout scenarios, writing result artifacts to target/rollout/heavy.
# The reduced tier of the same driver runs in `just test` (ADR 0036).
# Set AXOND_TEST_POSTGRES_DSN to also evaluate the forward-only rollback fence;
# without it the artifact records the fence as skipped rather than as passing.
rollout:
    AXOND_ROLLOUT=1 cargo test --locked --all-features --test rollout -- --nocapture --test-threads=1

# Run the gateway against ./axond.toml (copy axond.example.toml first).
run:
    cargo run -p axond

# Static single-binary release build (musl).
build-static:
    cargo build --release --target x86_64-unknown-linux-musl -p axond

# Build the static binary if needed and prove the Tier 0 default is hermetic.
tier0:
    cargo build --release --target x86_64-unknown-linux-musl -p axond
    ops/tier0-gate.sh target/x86_64-unknown-linux-musl/release/axond

# Boot a release binary and prove it serves, the way CI does for every released
# target. Defaults to $AXOND_BIN, then a release build, then a debug build.
binary-smoke *binary:
    python3 ops/binary-smoke.py {{ binary }}

# Build the distroless container image.
docker:
    docker build -t axond:dev .

# Build the image and prove it boots and serves /healthz.
docker-smoke:
    ops/docker-smoke.sh "$(docker build -q .)"

# Run the five-minute Docker Compose quickstart against placeholder providers.
quickstart-smoke:
    ops/compose-smoke.sh
