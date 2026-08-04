# Axond developer commands. Run `just` to list.

default:
    @just --list

# Format, lint (warnings = errors), and test — the same gates CI runs.
check: fmt-check clippy test

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all -- --check

clippy:
    cargo clippy --all-targets --all-features -- -D warnings

test:
    cargo test --all-features

# Run the gateway against ./axond.toml (copy axond.example.toml first).
run:
    cargo run -p axond

# Static single-binary release build (musl).
build-static:
    cargo build --release --target x86_64-unknown-linux-musl -p axond

# Build the distroless container image.
docker:
    docker build -t axond:dev .
