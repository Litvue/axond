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

# Run the gateway against ./axond.toml (copy axond.example.toml first).
run:
    cargo run -p axond

# Static single-binary release build (musl).
build-static:
    cargo build --release --target x86_64-unknown-linux-musl -p axond

# Build the distroless container image.
docker:
    docker build -t axond:dev .

# Build the image and prove it boots and serves /healthz.
docker-smoke:
    ops/docker-smoke.sh "$(docker build -q .)"
