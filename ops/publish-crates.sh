#!/usr/bin/env bash
# Publish the axond binary crate to crates.io, idempotently.
#
# `gateway-core` and `gateway-transport` are unpublished workspace members.
# The registry tarball is a self-contained flatten of those sources into
# `axond` (`ops/stage-axond-crate.py`), so `cargo install axond` compiles
# from that package alone and does not resolve the internals from crates.io.
#
# A crates.io version is immutable, so a re-run after a partial release must not
# re-upload what is already there: `axond` is skipped when the exact version is
# already on the registry. That makes this script the resume path for a release
# that failed after the other artifacts (see RELEASE.md), and it is why the
# release workflow can be re-dispatched for an existing tag.
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
# It stages the flattened crate and packages that, not the workspace members:
# `cargo publish -p axond` from this workspace cannot produce a registry
# tarball that compiles without the unpublished internals.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

package=axond

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

# Every workspace member stays at the single workspace version so a release is
# coherent even though only axond is uploaded. Internal path crates must not
# carry a version requirement: that is what would let an accidental
# `cargo publish -p axond` from this workspace resolve stale crates.io copies.
require_aligned_versions() {
  python3 - "$version" <<'PY' || fail "workspace versions are not aligned (see above)"
import json, subprocess, sys

release_version = sys.argv[1]
metadata = json.loads(
    subprocess.check_output(
        ["cargo", "metadata", "--no-deps", "--format-version", "1", "--locked"],
        text=True,
    )
)
problems = []
publishable = []
by_name = {}
for package in sorted(metadata["packages"], key=lambda p: p["name"]):
    by_name[package["name"]] = package
    flag = package.get("publish")
    print(f"{package['name']} version={package['version']} publish={flag!r}")
    if package["version"] != release_version:
        problems.append(
            f"package {package['name']} is at {package['version']} but the release is {release_version}"
        )
    if flag != []:
        publishable.append(package["name"])
for name in ("gateway-core", "gateway-transport"):
    package = by_name.get(name)
    if package is None:
        problems.append(f"workspace is missing internal member {name}")
        continue
    if package.get("publish") != []:
        problems.append(f"{name} is publishable; it must stay an unpublished workspace member")
unexpected = [name for name in publishable if name != "axond"]
if unexpected:
    problems.append(
        "publishable workspace crates besides axond: "
        + ", ".join(unexpected)
        + "; crates.io ships only the axond binary"
    )
print("---")
print("workspace publishable:", publishable if publishable else "(none)")
print("registry package: axond")
print("internals unpublished: gateway-core, gateway-transport")
for problem in problems:
    print(f"  {problem}", file=sys.stderr)
raise SystemExit(1 if problems else 0)
PY
  echo "versions: every workspace package is at $version; registry package is axond; internals are unpublished"

  python3 - "$version" <<'PY' || fail "internal path crates must not carry a registry version"
import sys
import tomllib

release_version = sys.argv[1]
manifest = tomllib.loads(open("Cargo.toml", "rb").read().decode("utf-8"))
problems = []
for name in ("gateway-core", "gateway-transport"):
    dep = manifest["workspace"]["dependencies"].get(name)
    if not isinstance(dep, dict) or "path" not in dep:
        problems.append(f"[workspace.dependencies] {name} is not a path crate")
        continue
    if "version" in dep:
        problems.append(
            f"[workspace.dependencies] {name} declares version {dep['version']!r}; "
            "omit it so cargo cannot rewrite the dep to crates.io"
        )
for problem in problems:
    print(f"  {problem}", file=sys.stderr)
raise SystemExit(1 if problems else 0)
PY
}

# The shipped DDL is an operator interface and lives in `ops/postgres/`, which is
# outside `crates/gateway/` and therefore outside the published package. The
# gateway embeds package-local copies under `crates/gateway/sql/`. Publishing a
# copy that has drifted from the operator-facing file would ship a gateway that
# builds a different table than the runbook does, so refuse — and refuse before
# the first upload, since a published version cannot be replaced.
# `crates/gateway/tests/shipped_ddl.rs` is the same gate in the test suite.
require_ddl_copies_match() {
  local operator_dir=ops/postgres packaged_dir=crates/gateway/sql name
  local -a names=()
  for name in "$operator_dir"/*.sql "$packaged_dir"/*.sql; do
    [[ -e "$name" ]] || fail "no shipped DDL found in $operator_dir and $packaged_dir"
    names+=("$(basename "$name")")
  done
  while read -r name; do
    [[ -f "$operator_dir/$name" ]] ||
      fail "$packaged_dir/$name has no $operator_dir/$name; operators applying the schema by hand would never see it"
    [[ -f "$packaged_dir/$name" ]] ||
      fail "$operator_dir/$name has no packaged copy at $packaged_dir/$name; the published axond package cannot embed it"
    cmp --silent "$operator_dir/$name" "$packaged_dir/$name" ||
      fail "$operator_dir/$name and $packaged_dir/$name differ; copy the operator-facing file over the packaged one"
  done < <(printf '%s\n' "${names[@]}" | sort -u)
  echo "shipped DDL: every ops/postgres file has a byte-identical packaged copy"
}

# release-please bumps the manifest strings through `extra-files` JSONPaths, and
# its TOML updater *warns* rather than fails when a path matches nothing: an
# unregistered or mistyped path silently leaves a version behind, and the first
# symptom would be an incoherent release at the tag. So assert here that every
# version string this release depends on is both reachable by a configured path
# and already at $version — and that no internal `path` + `version` dependency
# has crept back in, which is the way a second publishable crate would
# reintroduce a silent no-op.
require_release_config_bumps_every_version() {
  python3 - "$version" <<'PY' || fail "release-please-config.json does not cover every version string (see above)"
import json
import re
import sys
import tomllib

release_version = sys.argv[1]
manifest = tomllib.loads(open("Cargo.toml", "rb").read().decode("utf-8"))
config = json.load(open("release-please-config.json", encoding="utf-8"))


def segments(jsonpath):
    """The member names of a plain `$.a.b` / `$['a']['b']` JSONPath.

    release-please resolves these with jsonpath-plus, which accepts a hyphenated
    key in dot notation as well as in brackets. Only the plain forms are used
    here, so anything with a wildcard or filter is reported rather than guessed
    at.
    """
    if not re.fullmatch(r"\$(\.[A-Za-z0-9_-]+|\['[^']+'\])+", jsonpath):
        return None
    matches = re.findall(r"\.([A-Za-z0-9_-]+)|\['([^']+)'\]", jsonpath)
    return [dotted or bracketed for dotted, bracketed in matches]


def resolve(names):
    node = manifest
    for name in names:
        if not isinstance(node, dict) or name not in node:
            return None
        node = node[name]
    return node


problems = []
covered = set()
for entry in config["packages"]["."]["extra-files"]:
    if entry.get("type") != "toml" or entry.get("path") != "Cargo.toml":
        continue
    jsonpath = entry["jsonpath"]
    names = segments(jsonpath)
    if names is None:
        problems.append(f"{jsonpath} is not a plain member path; this gate cannot verify it")
        continue
    found = resolve(names)
    if found is None:
        problems.append(f"{jsonpath} matches nothing in Cargo.toml; release-please would only warn and leave the version unbumped")
        continue
    if found != release_version:
        problems.append(f"{jsonpath} is '{found}', not {release_version}")
    covered.add(tuple(names))

if ("workspace", "package", "version") not in covered:
    problems.append("$.workspace.package.version has no extra-files entry; the workspace version would not be bumped")

for name, dependency in manifest["workspace"]["dependencies"].items():
    if not isinstance(dependency, dict) or "path" not in dependency or "version" not in dependency:
        continue
    if ("workspace", "dependencies", name, "version") not in covered:
        problems.append(
            f"[workspace.dependencies] {name} pins a version but no extra-files entry bumps it; "
            f"add $.workspace.dependencies.{name}.version to release-please-config.json"
        )

for problem in problems:
    print(f"  {problem}", file=sys.stderr)
raise SystemExit(1 if problems else 0)
PY
  echo "release config: every version string is bumped by an extra-files path and is at $version"
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

stage_axond_crate() {
  local stage="$1"
  python3 ops/stage-axond-crate.py --self-test
  python3 ops/stage-axond-crate.py --out "$stage"
  python3 - "$stage/Cargo.toml" <<'PY' || fail "staged axond Cargo.toml still depends on unpublished internals"
import pathlib, sys, tomllib
manifest = tomllib.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
deps = set(manifest.get("dependencies", {}))
internal = {"gateway-core", "gateway-transport"} & deps
if internal:
    raise SystemExit("staged Cargo.toml depends on " + ", ".join(sorted(internal)))
print("staged manifest: no crates.io dependency on gateway-core or gateway-transport")
PY
}

if [[ "$mode" == publish && -z "${CARGO_REGISTRY_TOKEN:-}" ]]; then
  fail "CARGO_REGISTRY_TOKEN is not set; a real crates.io publish needs the release token (see RELEASE.md)"
fi

require_aligned_versions
require_release_config_bumps_every_version
require_ddl_copies_match

stage="$(mktemp -d "${TMPDIR:-/tmp}/axond-crate.XXXXXX")"
cleanup() { rm -rf "$stage"; }
trap cleanup EXIT
stage_axond_crate "$stage"

if [[ "$mode" == dry-run ]]; then
  echo
  echo "=== packaging flattened ${package} at $version"
  cargo package --locked --manifest-path "$stage/Cargo.toml"
  packaged="$stage/target/package/${package}-${version}/Cargo.toml"
  [[ -f "$packaged" ]] || fail "cargo package did not unpack ${package}-${version}"
  python3 - "$packaged" <<'PY' || fail "packaged axond still depends on unpublished internals"
import pathlib, sys, tomllib
path = pathlib.Path(sys.argv[1])
manifest = tomllib.loads(path.read_text(encoding="utf-8"))
deps = manifest.get("dependencies", {})
internal = {"gateway-core", "gateway-transport"} & set(deps)
print(f"packaged {path}:")
for name in sorted(deps):
    print(f"  {name} = {deps[name]!r}")
if internal:
    raise SystemExit("packaged Cargo.toml depends on " + ", ".join(sorted(internal)))
PY
  echo
  echo "=== publish dry-run of ${package} (internals are not packages)"
  cargo publish --dry-run --locked --manifest-path "$stage/Cargo.toml"
  echo
  echo "publish dry-run passed: ${package} only"
  exit 0
fi

echo
echo "=== ${package}@${version}"

if published "$package"; then
  echo "skip: ${package}@${version} is already on crates.io (immutable); nothing to re-upload"
  echo
  echo "published now: none"
  echo "already present: ${package}"
  echo "crates.io release $version complete: ${package}"
  exit 0
fi

if cargo publish --locked --manifest-path "$stage/Cargo.toml"; then
  echo
  echo "published now: ${package}"
  echo "already present: none"
  echo "crates.io release $version complete: ${package}"
  exit 0
fi

if published "$package"; then
  echo "recovered: ${package}@${version} is on crates.io despite the failed publish call"
  echo
  echo "published now: none"
  echo "already present: ${package}"
  echo "crates.io release $version complete: ${package}"
  exit 0
fi

fail "could not publish ${package}@${version}; re-run this script (or re-dispatch the release) to resume from here"
