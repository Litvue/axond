# Axond developer commands. Run `just` to list.

default:
    @just --list

# Format, lint (warnings = errors), test, docs, and supply-chain — the CI gates.
check: fmt-check clippy test docs deny

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

# Supply-chain policy: advisories, licenses, sources (see deny.toml).
deny:
    cargo deny --locked --all-features check

# Provider-SDK compatibility: the vendors' own Python SDKs against a real
# axond and the committed fixtures. Offline; needs python3.
compat:
    cargo build -p axond --locked
    python3 -m venv target/compat-venv
    target/compat-venv/bin/pip install --quiet --require-virtualenv -r tests/compat/requirements.txt
    AXOND_BIN="$(pwd)/target/debug/axond" target/compat-venv/bin/python -m pytest tests/compat -q

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
