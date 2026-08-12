# Axond developer commands. Run `just` to list.

default:
    @just --list

# Format, lint (warnings = errors), test, docs, supply-chain, and release
# packaging — the CI gates.
check: fmt-check clippy test docs deny publish-dry-run

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
    python3 ops/check-docs.py
    python3 ops/check-release-config.py
    sh -n install.sh
    bash -n ops/publish-image-index.sh
    bash -n ops/verify-image-evidence.sh
    bash ops/check-compose-platform.sh
    AXOND_VERSION=0.0.0 AXOND_TARGET=x86_64-unknown-linux-musl AXOND_INSTALL_DRY_RUN=1 sh install.sh | grep -F 'axond-0.0.0-x86_64-unknown-linux-musl.tar.gz'
    AXOND_VERSION=0.0.0 AXOND_TARGET=aarch64-unknown-linux-musl AXOND_INSTALL_DRY_RUN=1 sh install.sh | grep -F 'axond-0.0.0-aarch64-unknown-linux-musl.tar.gz'
    docker compose --env-file ops/compose/env.example config --quiet
    docker compose --env-file ops/compose/env.example -f docker-compose.yml -f docker-compose.build.yml config --quiet
    AXOND_QUICKSTART_CONFIG=./ops/compose/axond.stateful.toml docker compose --env-file ops/compose/env.example -f docker-compose.yml -f docker-compose.stateful.yml --profile stateful config --quiet

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

# Refresh the hash-pinned provider-SDK lockfile, excluding releases newer than a week.
compat-lock:
    uv pip compile --generate-hashes --universal --python-version 3.10 --exclude-newer "$(python3 -c 'from datetime import datetime, timedelta, timezone; print((datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ"))')" -o tests/compat/requirements.txt tests/compat/requirements.in

# The heavy SSE soak: hundreds of concurrent streams with cancels and drops.
# The short subset runs in `just test`; this is the long one.
soak:
    AXOND_SOAK=1 cargo test --locked --all-features --test soak -- --nocapture

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

# Build the distroless container image.
docker:
    docker build -t axond:dev .

# Build the image and prove it boots and serves /healthz.
docker-smoke:
    ops/docker-smoke.sh "$(docker build -q .)"

# Run the five-minute Docker Compose quickstart against placeholder providers.
quickstart-smoke:
    ops/compose-smoke.sh
