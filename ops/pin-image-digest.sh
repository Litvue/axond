#!/usr/bin/env bash
# Resolve the production overlay's image digest, or refuse the unresolved one.
#
# The committed overlay pins an all-zero sentinel digest: an image reference that
# cannot be pulled. That is deliberate — a tag in a production manifest is a name
# that can be repointed at different bytes after the review that approved it, and
# a real digest committed to the repository is stale the next release. So the
# digest is resolved at deploy time from the release the operator verified, and
# this script is both halves of that:
#
#   ops/pin-image-digest.sh --check          # exit non-zero while unresolved
#   ops/pin-image-digest.sh 0.3.21           # resolve that release into the overlay
#   ops/pin-image-digest.sh --print 0.3.21   # only print the digest
#
# `--check` is what a rollout gate runs: it fails on a working tree that would
# apply the placeholder. Resolution insists on the multi-architecture index, so a
# digest that names one architecture's child image — schedulable only onto that
# architecture — is refused rather than pinned. Verify the evidence chain for the
# digest with `ops/verify-image-evidence.sh` before applying it; this script
# resolves a reference and makes no claim about its signature.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
overlay="${repo_root}/deploy/kubernetes/overlays/production/kustomization.yaml"
image="${AXOND_IMAGE:-ghcr.io/litvue/axond}"
sentinel="sha256:0000000000000000000000000000000000000000000000000000000000000000"
required_platforms=(linux/amd64 linux/arm64)

usage() {
  echo "usage: ops/pin-image-digest.sh [--check | [--print] [VERSION]]" >&2
  exit 2
}

current_digest() {
  sed -n 's/^ *digest: *\(sha256:[0-9a-f]\{64\}\) *$/\1/p' "$overlay" | head -n 1
}

workspace_version() {
  sed -n 's/^version = "\([^"]*\)"$/\1/p' "${repo_root}/Cargo.toml" | head -n 1
}

mode=resolve
case "${1:-}" in
  --check)
    mode=check
    shift
    ;;
  --print)
    mode=print
    shift
    ;;
  -h | --help) usage ;;
  # A mistyped flag must not become the version to resolve: `--chek` would
  # otherwise look up an image tagged with the typo and read as a registry
  # failure rather than as the check never having run.
  --*) usage ;;
esac
[[ $# -le 1 ]] || usage
[[ "$mode" != check || $# -eq 0 ]] || usage

if [[ "$mode" == check ]]; then
  digest="$(current_digest)"
  [[ -n "$digest" ]] || {
    echo "no image digest found in ${overlay#"$repo_root"/}" >&2
    exit 1
  }
  if [[ "$digest" == "$sentinel" ]]; then
    echo "the production overlay still pins the unresolved sentinel digest." >&2
    echo "run: ops/pin-image-digest.sh <version>   (then verify it with ops/verify-image-evidence.sh)" >&2
    exit 1
  fi
  echo "production overlay pins ${image}@${digest}"
  exit 0
fi

version="${1:-$(workspace_version)}"
[[ -n "$version" ]] || {
  echo "could not determine a version to resolve" >&2
  exit 1
}
reference="${image}:${version}"

# `docker buildx imagetools inspect` reads the registry without pulling, and
# `crane` does the same where buildx is not installed. Both the index body and
# the registry's own descriptor digest for it are read here: hashing the body
# locally would depend on how the tool and this shell handled trailing bytes,
# and `ops/publish-image-index.sh` already takes the descriptor as the truth.
command -v jq >/dev/null 2>&1 || {
  echo "need jq to read the registry's manifest for ${reference}" >&2
  exit 1
}
if command -v docker >/dev/null 2>&1 && docker buildx version >/dev/null 2>&1; then
  raw="$(docker buildx imagetools inspect --raw "$reference")"
  digest="$(docker buildx imagetools inspect --format '{{json .Manifest}}' "$reference" | jq -r '.digest // empty')"
elif command -v crane >/dev/null 2>&1; then
  raw="$(crane manifest "$reference")"
  digest="$(crane digest "$reference")"
else
  echo "need docker buildx or crane to read ${reference} from the registry" >&2
  exit 1
fi

media_type="$(jq -r '.mediaType // empty' <<<"$raw")"
case "$media_type" in
  application/vnd.oci.image.index.v1+json | application/vnd.docker.distribution.manifest.list.v2+json) ;;
  *)
    echo "${reference} is not a multi-architecture index (mediaType: ${media_type:-unknown})." >&2
    echo "pinning an architecture's child digest would schedule onto that architecture only." >&2
    exit 1
    ;;
esac

for platform in "${required_platforms[@]}"; do
  os="${platform%%/*}"
  arch="${platform##*/}"
  jq -e --arg os "$os" --arg arch "$arch" \
    '.manifests | map(select(.platform.os == $os and .platform.architecture == $arch)) | length == 1' \
    <<<"$raw" >/dev/null || {
    echo "${reference} does not contain exactly one ${platform} manifest" >&2
    exit 1
  }
done

[[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]] || {
  echo "the registry did not report a usable digest for ${reference}: ${digest:-none}" >&2
  exit 1
}

if [[ "$mode" == print ]]; then
  echo "$digest"
  exit 0
fi

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT
sed "s|^\( *digest: \)sha256:[0-9a-f]\{64\}|\1${digest}|" "$overlay" >"$tmp"
grep -Fq "$digest" "$tmp" || {
  echo "failed to write the digest into ${overlay#"$repo_root"/}" >&2
  exit 1
}
cat "$tmp" >"$overlay"
echo "pinned ${image}@${digest} (${reference}) in ${overlay#"$repo_root"/}"
echo "verify it before applying: SIGNER_IDENTITY=... GITHUB_REPOSITORY=Litvue/axond ops/verify-image-evidence.sh ${image}@${digest}"
