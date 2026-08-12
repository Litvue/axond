#!/usr/bin/env bash
# Prove the workspace still compiles on its declared minimum supported Rust
# version, and that every other toolchain declaration in the repository agrees
# with it.
#
# `rust-version` in `[workspace.package]` is the single source of truth. The MSRV
# is interpreted as the *first* patch of that minor (`1.97` means `1.97.0`),
# because that is what Cargo enforces for a consumer: `cargo add axond` on
# 1.97.0 must work. The pinned developer/CI toolchain in `rust-toolchain.toml`
# is deliberately newer — it fixes lint and format output — so this lane is the
# only place the floor itself is exercised.
set -euo pipefail

cd "$(dirname "$0")/.."

read_toml_value() {
    # First `key = "value"` occurrence in the file; enough for these manifests.
    sed -n -E "s/^[[:space:]]*$2[[:space:]]*=[[:space:]]*\"([^\"]+)\".*/\1/p" "$1" \
        | head -n 1
}

msrv="$(read_toml_value Cargo.toml rust-version)"
[[ -n $msrv ]] || {
    echo "Cargo.toml: [workspace.package] rust-version is not declared" >&2
    exit 1
}

# `1.97` is the policy statement; `1.97.0` is the toolchain that must build it.
msrv_full="$msrv"
[[ $msrv_full == *.*.* ]] || msrv_full="$msrv_full.0"

pinned="$(read_toml_value rust-toolchain.toml channel)"
[[ -n $pinned ]] || {
    echo "rust-toolchain.toml: no [toolchain] channel is pinned" >&2
    exit 1
}

# A pinned toolchain older than the MSRV would mean nobody ever builds the floor;
# an unpinned one would make fmt and clippy output drift release to release.
if [[ $(printf '%s\n%s\n' "$msrv_full" "$pinned" | sort -V | head -n 1) != "$msrv_full" ]]; then
    echo "rust-toolchain.toml: pinned $pinned is older than the MSRV $msrv_full" >&2
    exit 1
fi

# The release image must not be built by a compiler older than the floor either.
docker_channel="$(sed -n -E 's/^FROM rust:([0-9.]+).*/\1/p' Dockerfile | head -n 1)"
[[ -n $docker_channel ]] || {
    echo "Dockerfile: no pinned rust: build stage found" >&2
    exit 1
}
if [[ $docker_channel != "$msrv" && $docker_channel != "$msrv_full" ]]; then
    echo "Dockerfile: rust:$docker_channel does not track the MSRV $msrv" >&2
    exit 1
fi

# Every published crate inherits the workspace floor rather than declaring its
# own, so a crate can never be published with a quieter MSRV than the policy.
for manifest in crates/*/Cargo.toml; do
    grep -Fq 'rust-version.workspace = true' "$manifest" || {
        echo "$manifest: does not inherit rust-version from the workspace" >&2
        exit 1
    }
done

echo "MSRV policy: $msrv (building with $msrv_full; pinned toolchain $pinned)"

if [[ ${AXOND_MSRV_CHECK_ONLY_POLICY:-0} == 1 ]]; then
    exit 0
fi

if command -v rustup >/dev/null 2>&1; then
    rustup toolchain install "$msrv_full" --profile minimal --no-self-update
    export RUSTUP_TOOLCHAIN="$msrv_full"
fi

# `--locked` matters: the committed lockfile has to resolve on the floor too, so
# a dependency bump that raises its own MSRV fails here instead of in a
# consumer's build.
cargo check --workspace --all-targets --all-features --locked
