#!/usr/bin/env bash
# Static analysis of the workflow definitions themselves: expression syntax,
# unknown contexts, bad `needs`/`if` references, and shellcheck over every
# `run:` script.
#
# The linter is fetched at a pinned version and verified against a pinned
# SHA-256 before it is executed, for the same reason the workflows pin their
# Actions: a lint step that resolves "latest" at run time is arbitrary upstream
# code running in CI.
set -euo pipefail

ACTIONLINT_VERSION=1.7.12
# Same linter, same version, pinned by manifest digest rather than by archive
# checksum. GitHub-hosted runners answer 503 for the release asset, so the image
# is the path that actually works there; it also carries the shellcheck and
# pyflakes that actionlint shells out to.
IMAGE="docker.io/rhysd/actionlint@sha256:b1934ee5f1c509618f2508e6eb47ee0d3520686341fec936f3b79331f9315667"

# From actionlint_1.7.12_checksums.txt. A host outside this table has no pinned
# archive and goes straight to the image, rather than downloading the amd64 Linux
# binary it cannot execute.
select_platform() {
    case "$1" in
    Linux/x86_64)
        PLATFORM=linux_amd64
        ARCHIVE_SHA256=8aca8db96f1b94770f1b0d72b6dddcb1ebb8123cb3712530b08cc387b349a3d8
        ;;
    Linux/aarch64 | Linux/arm64)
        PLATFORM=linux_arm64
        ARCHIVE_SHA256=325e971b6ba9bfa504672e29be93c24981eeb1c07576d730e9f7c8805afff0c6
        ;;
    Darwin/x86_64)
        PLATFORM=darwin_amd64
        ARCHIVE_SHA256=5b44c3bc2255115c9b69e30efc0fecdf498fdb63c5d58e17084fd5f16324c644
        ;;
    Darwin/arm64)
        PLATFORM=darwin_arm64
        ARCHIVE_SHA256=aba9ced2dee8d27fecca3dc7feb1a7f9a52caefa1eb46f3271ea66b6e0e6953f
        ;;
    *)
        PLATFORM=
        ARCHIVE_SHA256=
        ;;
    esac
}

# `--self-test` asserts the host table rather than the linting, so a refactor of
# the acquisition paths cannot quietly send a developer machine at the wrong
# archive; it is what CI runs before the lint itself.
self_test() {
    local host expected_platform problems=0
    while read -r host expected_platform; do
        select_platform "$host"
        if [[ $PLATFORM != "$expected_platform" ]]; then
            echo "self-test: $host selected '${PLATFORM:-<image>}', expected '${expected_platform}'" >&2
            problems=1
        fi
        if [[ -n $PLATFORM && ${#ARCHIVE_SHA256} -ne 64 ]]; then
            echo "self-test: $host has no pinned checksum for $PLATFORM" >&2
            problems=1
        fi
        if [[ -z $PLATFORM && -n $ARCHIVE_SHA256 ]]; then
            echo "self-test: $host has a checksum but no archive" >&2
            problems=1
        fi
    done <<'HOSTS'
Linux/x86_64 linux_amd64
Linux/aarch64 linux_arm64
Linux/arm64 linux_arm64
Darwin/x86_64 darwin_amd64
Darwin/arm64 darwin_arm64
Linux/riscv64
MINGW64_NT-10.0/x86_64
HOSTS
    if ((problems)); then
        return 1
    fi
    echo "actionlint host-selection self-test passed"
}

if [[ ${1-} == --self-test ]]; then
    self_test
    exit
fi

select_platform "$(uname -s)/$(uname -m)"
ARCHIVE="actionlint_${ACTIONLINT_VERSION}_${PLATFORM}.tar.gz"
URL="https://github.com/rhysd/actionlint/releases/download/v${ACTIONLINT_VERSION}/${ARCHIVE}"

cd "$(dirname "$0")/.."
repository="$PWD"

run_pinned_image() {
    if ! command -v docker >/dev/null 2>&1; then
        echo "no usable pinned actionlint: docker is unavailable for $IMAGE" >&2
        return 1
    fi
    # Not `exec`: that would replace this shell and drop the cleanup trap,
    # leaving any partial download behind.
    docker run --rm --network none \
        --volume "$repository:/repo:ro" --workdir /repo "$IMAGE" -color
}

verify_sha256() {
    # macOS ships shasum rather than the coreutils tool.
    if command -v sha256sum >/dev/null 2>&1; then
        echo "$2  $1" | sha256sum --check --quiet
    else
        echo "$2  $1" | shasum -a 256 --check --status
    fi
}

# A binary named `actionlint` on PATH is not a pinned build: nothing but its own
# `--version` string vouches for it, so trusting it would make the acquisition
# below decorative on any machine that has one. It is used only when the caller
# names it explicitly, which is an assertion that they know where it came from.
if [[ -n ${AXOND_ACTIONLINT-} ]]; then
    installed="$("$AXOND_ACTIONLINT" --version | head -n 1)"
    if [[ $installed != "$ACTIONLINT_VERSION" ]]; then
        echo "AXOND_ACTIONLINT is actionlint $installed; this gate pins $ACTIONLINT_VERSION" >&2
        exit 1
    fi
    echo "using the caller-supplied actionlint $installed ($AXOND_ACTIONLINT)"
    exec "$AXOND_ACTIONLINT" -color
fi

if [[ -z $PLATFORM ]]; then
    echo "no pinned actionlint build for $(uname -s)/$(uname -m); using the image"
    run_pinned_image
    exit
fi

workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT

# The release asset is the first choice because it needs no container runtime.
# Both paths land on the same pinned version, so a fallback cannot change what
# the gate runs.
if curl --fail --silent --show-error --location \
    --retry 3 --retry-delay 2 --retry-connrefused --retry-all-errors \
    --output "$workdir/$ARCHIVE" "$URL"; then
    verify_sha256 "$workdir/$ARCHIVE" "$ARCHIVE_SHA256"
    tar -C "$workdir" -xzf "$workdir/$ARCHIVE" actionlint
    cd "$repository"
    "$workdir/actionlint" -color
    exit
fi

echo "$URL is unreachable; running the pinned actionlint image instead"
run_pinned_image
