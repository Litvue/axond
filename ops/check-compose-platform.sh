#!/usr/bin/env bash
# Prove the Compose architecture selection behaves in every mode operators use.
#
# The quickstart pins a release tag. While that tag is an amd64-only release it
# has no ARM child, so dropping the platform default outright would turn
# `docker compose up` on an ARM host from "slow, emulated" into "cannot pull". So
# the default is deliberately overridable rather than absent, and both ends of
# that transition are asserted here — the unpinned resolution a
# multi-architecture tag needs to run natively, and the fallback that keeps an
# amd64-only tag runnable.
#
# Whether the default *should* currently be the fallback or unpinned is
# ops/check-release-config.py's decision, keyed to the pinned tag; this script
# takes the default from the file and checks the mechanism around it.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

compose=(docker compose --env-file ops/compose/env.example)
build_overlay=(-f docker-compose.yml -f docker-compose.build.yml)

configured_platform() {
  # Prints the effective platform, or nothing when the service is unpinned.
  # `env` rather than a prefix assignment, so "unset" and "set but empty" stay
  # distinguishable and neither leaks into the next case.
  #
  # A Compose failure must not read as "unpinned": both produce no `platform:`
  # line, and two of the assertions below expect exactly that. So the render is
  # captured and its status checked before anything is matched.
  local mode="$1"
  shift
  local rendered
  if [[ "$mode" == unset ]]; then
    rendered="$(env -u AXOND_PLATFORM "$@" config 2>&1)" || {
      printf 'compose platform check failed: compose config errored with AXOND_PLATFORM unset:\n%s\n' \
        "$rendered" >&2
      return 1
    }
  else
    rendered="$(env AXOND_PLATFORM="$mode" "$@" config 2>&1)" || {
      printf 'compose platform check failed: compose config errored with AXOND_PLATFORM=%s:\n%s\n' \
        "$mode" "$rendered" >&2
      return 1
    }
  fi
  printf '%s\n' "$rendered" |
    sed -n 's/^[[:space:]]*platform:[[:space:]]*\(.*\)$/\1/p'
}

expect_platform() {
  # Runs the render itself rather than taking its output: a command substitution
  # used as an argument discards its exit status, so a failing Compose invocation
  # would arrive here as an empty string and pass the two "unpinned" cases.
  local label="$1" expected="$2" mode="$3"
  shift 3
  local actual
  actual="$(configured_platform "$mode" "$@")"
  if [[ "$actual" != "$expected" ]]; then
    printf 'compose platform check failed: %s resolved to %s, expected %s\n' \
      "$label" "${actual:-<unpinned>}" "${expected:-<unpinned>}" >&2
    exit 1
  fi
  printf 'compose platform: %s -> %s\n' "$label" "${actual:-unpinned (native)}"
}

# The declared line is read before its default, because an absent or reshaped
# `platform:` line yields the same empty string as the unpinned default and would
# make the assertions below vacuous: two of them expect exactly that.
# shellcheck disable=SC2016  # the Compose interpolation is matched literally
declared_line="$(
  sed -n 's/^[[:space:]]*\(platform: \${AXOND_PLATFORM-.*}\)$/\1/p' docker-compose.yml
)"
if [[ "$(printf '%s' "$declared_line" | grep -c .)" != 1 ]]; then
  # shellcheck disable=SC2016  # the Compose interpolation is quoted literally
  printf 'compose platform check failed: docker-compose.yml does not declare exactly one overridable `platform: ${AXOND_PLATFORM-...}` default\n' >&2
  exit 1
fi
declared_default="${declared_line#platform: \$\{AXOND_PLATFORM-}"
declared_default="${declared_default%\}}"

# 1. Unset: the file's own default applies. With a multi-architecture pinned tag
#    that default is empty, so every host resolves its own child of the index;
#    with an amd64-only tag it is `linux/amd64`, which an ARM host runs under
#    emulation instead of failing to pull.
if [[ -z "$declared_default" ]]; then
  expect_platform "quickstart, AXOND_PLATFORM unset (multi-arch native)" "" \
    unset "${compose[@]}"
else
  expect_platform "quickstart, AXOND_PLATFORM unset (amd64 fallback)" \
    "$declared_default" unset "${compose[@]}"
fi

# 2. Set but empty: no pin, so Docker resolves the native child of an index. This
#    is what an ARM host uses once a multi-architecture tag or digest is pinned.
expect_platform "quickstart, AXOND_PLATFORM= (multi-arch native)" "" \
  "" "${compose[@]}"

# 3. Explicit: force one platform, emulated if the host differs.
for platform in linux/amd64 linux/arm64; do
  expect_platform "quickstart, AXOND_PLATFORM=$platform" "$platform" \
    "$platform" "${compose[@]}"
done

# 4. A source build is not limited by the last release's platforms: the
#    Dockerfile builds either architecture natively, so it must not inherit the
#    quickstart's fallback.
expect_platform "source build, AXOND_PLATFORM unset" "" \
  unset "${compose[@]}" "${build_overlay[@]}"
expect_platform "source build, AXOND_PLATFORM=linux/arm64" "linux/arm64" \
  linux/arm64 "${compose[@]}" "${build_overlay[@]}"

echo "compose platform checks passed"
