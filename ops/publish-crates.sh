#!/usr/bin/env bash
# Publish the Axond workspace to crates.io in dependency order, idempotently.
#
# The three packages are a single release at one version: `gateway-core`, then
# `gateway-transport`, then `axond`. Order matters because each package's
# registry dependency on the previous one must already be resolvable — for
# `cargo publish`'s own verification build and for any external consumer.
#
# A crates.io version is immutable, so a re-run after a partial release must not
# re-upload what is already there: each package is skipped when the exact
# version is already on the registry. That makes this script the resume path for
# a release that failed halfway (see RELEASE.md), and it is why the release
# workflow can be re-dispatched for an existing tag.
#
# Usage:
#   ops/publish-crates.sh <version>              # real publish; needs CARGO_REGISTRY_TOKEN
#   ops/publish-crates.sh [<version>] --dry-run  # package + publish dry-run, no token, no upload
#
# A real publish demands the version explicitly: the release workflow passes the
# tag's version so a mismatched checkout fails instead of shipping whatever the
# manifest happens to say. The dry-run defaults to the workspace version, which
# is what CI checks on every pull request.
#
# `--dry-run` never uploads and never consults the registry for the skip check.
# It packages and verifies the whole workspace in one cargo invocation, because
# a per-package dry-run of `gateway-transport` or `axond` cannot resolve a
# sibling that is not on crates.io yet; `--workspace` lets cargo satisfy the
# registry requirements from the local packages, in dependency order.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

# Dependency order. Do not reorder: `gateway-transport` depends on
# `gateway-core`, and `axond` depends on both.
packages=(gateway-core gateway-transport axond)

usage() {
  echo "Usage: ops/publish-crates.sh [<version>] [--dry-run]" >&2
}

version=""
mode="publish"
for argument in "$@"; do
  case "$argument" in
    --dry-run) mode="dry-run" ;;
    -h | --help)
      usage
      exit 0
      ;;
    -*)
      usage
      exit 2
      ;;
    *)
      if [[ -n "$version" ]]; then
        usage
        exit 2
      fi
      version="$argument"
      ;;
  esac
done

fail() {
  echo >&2
  echo "PUBLISH FAILED: $*" >&2
  exit 1
}

workspace_version() {
  cargo metadata --no-deps --format-version 1 --locked |
    python3 -c '
import json, sys
metadata = json.load(sys.stdin)
print(next(p["version"] for p in metadata["packages"] if p["name"] == "axond"))
'
}

if [[ -z "$version" ]]; then
  if [[ "$mode" == publish ]]; then
    fail "a real publish requires the version explicitly, e.g. ops/publish-crates.sh 0.3.0"
  fi
  version="$(workspace_version)"
  echo "dry-run against the workspace version $version"
fi
if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
  fail "'$version' is not a semantic version"
fi

# Every package ships at the single workspace version. A mismatch means the
# release is not coherent, so refuse before touching the registry rather than
# publishing a half-aligned set that cannot be unpublished.
require_aligned_versions() {
  local manifest_versions
  manifest_versions="$(
    cargo metadata --no-deps --format-version 1 --locked |
      python3 -c '
import json, sys
metadata = json.load(sys.stdin)
for package in sorted(metadata["packages"], key=lambda p: p["name"]):
    print(package["name"], package["version"])
'
  )"
  local name found
  while read -r name found; do
    [[ -n "$name" ]] || continue
    if [[ "$found" != "$version" ]]; then
      fail "package $name is at $found but the release is $version; align every workspace version before publishing"
    fi
  done <<<"$manifest_versions"

  local declared
  for name in gateway-core gateway-transport; do
    declared="$(
      python3 - "$name" <<'PY'
import re, sys
name = sys.argv[1]
manifest = open("Cargo.toml", encoding="utf-8").read()
match = re.search(rf'^{re.escape(name)}\s*=\s*\{{(.*)\}}\s*$', manifest, re.MULTILINE)
if not match:
    raise SystemExit(f"no workspace dependency entry for {name}")
version = re.search(r'version\s*=\s*"([^"]+)"', match.group(1))
print(version.group(1) if version else "")
PY
    )"
    if [[ "$declared" != "$version" ]]; then
      fail "[workspace.dependencies] $name declares version '$declared', not $version; external consumers would resolve the wrong release"
    fi
  done
  echo "versions: all workspace packages and internal dependency requirements are at $version"
}

# 200 = this exact version is already published (immutable, so skip it).
# 404 = absent. Anything else is an unknown registry state: refuse rather than
# guess, because guessing "absent" risks a duplicate upload attempt mid-release.
published() {
  local name="$1" status
  status="$(
    curl --silent --show-error --location --max-time 30 --retry 3 --retry-delay 2 \
      --output /dev/null --write-out '%{http_code}' \
      --header 'User-Agent: axond-release (https://github.com/Litvue/axond)' \
      "https://crates.io/api/v1/crates/${name}/${version}" || echo 000
  )"
  case "$status" in
    200) return 0 ;;
    404) return 1 ;;
    *) fail "crates.io returned HTTP $status for ${name}@${version}; refusing to publish against an unknown registry state" ;;
  esac
}

if [[ "$mode" == publish && -z "${CARGO_REGISTRY_TOKEN:-}" ]]; then
  fail "CARGO_REGISTRY_TOKEN is not set; a real crates.io publish needs the release token (see RELEASE.md)"
fi

require_aligned_versions

if [[ "$mode" == dry-run ]]; then
  echo
  echo "=== packaging the workspace at $version"
  cargo package --locked --workspace
  echo
  echo "=== publish dry-run in dependency order: ${packages[*]}"
  cargo publish --dry-run --locked --workspace
  echo
  echo "publish dry-run passed in dependency order: ${packages[*]}"
  exit 0
fi

published_now=()
skipped=()

for name in "${packages[@]}"; do
  echo
  echo "=== ${name}@${version}"

  if published "$name"; then
    echo "skip: ${name}@${version} is already on crates.io (immutable); nothing to re-upload"
    skipped+=("$name")
    continue
  fi

  # cargo blocks until the uploaded version is visible in the index, so the
  # next package in the order can resolve it.
  if cargo publish --locked --package "$name"; then
    published_now+=("$name")
    continue
  fi

  # A publish can fail after the upload is accepted (a dropped response, a
  # concurrent run). Re-check the registry: if the version is there, the release
  # step succeeded and the retry is a no-op rather than a failure.
  if published "$name"; then
    echo "recovered: ${name}@${version} is on crates.io despite the failed publish call; continuing"
    skipped+=("$name")
    continue
  fi
  fail "could not publish ${name}@${version}; re-run this script (or re-dispatch the release) to resume from here"
done

echo
echo "published now: ${published_now[*]:-none}"
echo "already present: ${skipped[*]:-none}"
echo "crates.io release $version complete in dependency order: ${packages[*]}"
