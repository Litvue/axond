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

# The release CDN answers an occasional 503, and a lint lane that fails on one
# is a false red, so retry before giving up. The checksum below is what makes
# the download trustworthy, not the transport.
curl --fail --silent --show-error --location \
    --retry 5 --retry-delay 2 --retry-connrefused --retry-all-errors \
    --output "$workdir/$ARCHIVE" "$URL"
echo "$ACTIONLINT_SHA256  $workdir/$ARCHIVE" | sha256sum --check --quiet
tar -C "$workdir" -xzf "$workdir/$ARCHIVE" actionlint

cd "$repository"
"$workdir/actionlint" -color
