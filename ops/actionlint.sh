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
ACTIONLINT_SHA256=8aca8db96f1b94770f1b0d72b6dddcb1ebb8123cb3712530b08cc387b349a3d8
ARCHIVE="actionlint_${ACTIONLINT_VERSION}_linux_amd64.tar.gz"
URL="https://github.com/rhysd/actionlint/releases/download/v${ACTIONLINT_VERSION}/${ARCHIVE}"
# Same linter, same version, pinned by manifest digest rather than by archive
# checksum. GitHub-hosted runners answer 503 for the release asset, so the image
# is the path that actually works there; it also carries the shellcheck and
# pyflakes that actionlint shells out to.
IMAGE="docker.io/rhysd/actionlint@sha256:b1934ee5f1c509618f2508e6eb47ee0d3520686341fec936f3b79331f9315667"

cd "$(dirname "$0")/.."
repository="$PWD"

if command -v actionlint >/dev/null 2>&1; then
    installed="$(actionlint --version | head -n 1)"
    if [[ $installed == "$ACTIONLINT_VERSION" ]]; then
        exec actionlint -color
    fi
    echo "ignoring actionlint $installed on PATH; this gate pins $ACTIONLINT_VERSION"
fi

workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT

# The release asset is the first choice because it needs no container runtime.
# Both paths land on the same pinned version, so a fallback cannot change what
# the gate runs.
if curl --fail --silent --show-error --location \
    --retry 3 --retry-delay 2 --retry-connrefused --retry-all-errors \
    --output "$workdir/$ARCHIVE" "$URL"; then
    echo "$ACTIONLINT_SHA256  $workdir/$ARCHIVE" | sha256sum --check --quiet
    tar -C "$workdir" -xzf "$workdir/$ARCHIVE" actionlint
    cd "$repository"
    "$workdir/actionlint" -color
    exit 0
fi

if ! command -v docker >/dev/null 2>&1; then
    echo "cannot reach $URL and docker is unavailable for the pinned image" >&2
    exit 1
fi

echo "$URL is unreachable; running the pinned actionlint image instead"
# Not `exec`: that would replace this shell and drop the cleanup trap, leaving
# the partial download behind.
docker run --rm --network none \
    --volume "$repository:/repo:ro" --workdir /repo "$IMAGE" -color
