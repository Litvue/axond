#!/usr/bin/env bash
# Resolve the production overlays' image digests, or refuse the unresolved ones.
#
# The committed overlay pins an all-zero sentinel digest: an image reference that
# cannot be pulled. That is deliberate — a tag in a production manifest is a name
# that can be repointed at different bytes after the review that approved it, and
# a real digest committed to the repository is stale the next release. So the
# digest is resolved at deploy time from the release the operator verified, and
# this script is both halves of that:
#
#   ops/pin-image-digest.sh --check                       # every shipped overlay
#   ops/pin-image-digest.sh --check overlays/production   # only that overlay
#   ops/pin-image-digest.sh 0.3.21           # resolve that release into the overlay
#   ops/pin-image-digest.sh --print 0.3.21   # only print the digest
#
# `--check` is what a rollout gate runs: it fails on a working tree that would
# apply the placeholder. Bare, it answers for the whole shipped fleet, which is
# what a repository gate wants. An operator rolling out one overlay wants an
# answer about that overlay: naming it scopes the check, so an unresolved
# sentinel in an overlay nobody is applying does not fail the rollout that is.
# Resolution insists on the multi-architecture index, so a
# digest that names one architecture's child image — schedulable only onto that
# architecture — is refused rather than pinned. Verify the evidence chain for the
# digest with `ops/verify-image-evidence.sh` before applying it; this script
# resolves a reference and makes no claim about its signature.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Every overlay that carries the sentinel, not just the stateless one: the
# stateful overlays pin the migration Job as well as their workload, and a
# `--check` blind to one would report a resolved fleet while that overlay still
# names an image no node can pull.
overlays=(
  "${repo_root}/deploy/kubernetes/overlays/production/kustomization.yaml"
  "${repo_root}/deploy/kubernetes/overlays/production-stateful/kustomization.yaml"
  "${repo_root}/deploy/kubernetes/overlays/production-stateful-persistent/kustomization.yaml"
  "${repo_root}/deploy/kubernetes/overlays/production-stateful-blob/kustomization.yaml"
)
image="${AXOND_IMAGE:-ghcr.io/litvue/axond}"
sentinel="sha256:0000000000000000000000000000000000000000000000000000000000000000"
required_platforms=(linux/amd64 linux/arm64)

usage() {
  echo "usage: ops/pin-image-digest.sh [--check [OVERLAY...] | [--print] [VERSION]]" >&2
  echo "       OVERLAY is one of: ${overlay_names[*]}" >&2
  exit 2
}

# An operator names an overlay the way the repository does — `overlays/production`,
# or the path to its kustomization — not by the array index it happens to have.
resolve_overlay() {
  local wanted="${1#"$repo_root"/}"
  wanted="${wanted#deploy/kubernetes/}"
  wanted="${wanted%/kustomization.yaml}"
  wanted="${wanted%/}"
  local index
  for index in "${!overlay_names[@]}"; do
    if [[ "$wanted" == "${overlay_names[$index]}" ]]; then
      echo "${overlays[$index]}"
      return 0
    fi
  done
  echo "unknown overlay: $1 (known: ${overlay_names[*]})" >&2
  return 1
}

current_digest() {
  sed -n 's/^ *digest: *\(sha256:[0-9a-f]\{64\}\) *$/\1/p' "$1" | head -n 1
}

workspace_version() {
  sed -n 's/^version = "\([^"]*\)"$/\1/p' "${repo_root}/Cargo.toml" | head -n 1
}

overlay_names=()
for overlay in "${overlays[@]}"; do
  name="${overlay#"$repo_root"/deploy/kubernetes/}"
  overlay_names+=("${name%/kustomization.yaml}")
done

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
[[ "$mode" == check || $# -le 1 ]] || usage

if [[ "$mode" == check ]]; then
  checked=()
  if [[ $# -eq 0 ]]; then
    checked=("${overlays[@]}")
  else
    for requested in "$@"; do
      checked+=("$(resolve_overlay "$requested")")
    done
  fi
  for overlay in "${checked[@]}"; do
    digest="$(current_digest "$overlay")"
    [[ -n "$digest" ]] || {
      echo "no image digest found in ${overlay#"$repo_root"/}" >&2
      exit 1
    }
    if [[ "$digest" == "$sentinel" ]]; then
      echo "${overlay#"$repo_root"/} still pins the unresolved sentinel digest." >&2
      echo "run: ops/pin-image-digest.sh <version>   (then verify it with ops/verify-image-evidence.sh)" >&2
      exit 1
    fi
    echo "${overlay#"$repo_root"/} pins ${image}@${digest}"
  done
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
  reader=buildx
  digest="$(docker buildx imagetools inspect --format '{{json .Manifest}}' "$reference" | jq -r '.digest // empty')"
elif command -v crane >/dev/null 2>&1; then
  reader=crane
  digest="$(crane digest "$reference")"
else
  echo "need docker buildx or crane to read ${reference} from the registry" >&2
  exit 1
fi

[[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]] || {
  echo "the registry did not report a usable digest for ${reference}: ${digest:-none}" >&2
  exit 1
}

# The index body is read back by digest rather than by tag. A tag is a name the
# registry may repoint at any moment, so validating the body behind the tag and
# then pinning the digest behind the tag are two reads of two possibly different
# artifacts: the platform set that gets checked would not be the one that gets
# pinned. `${image}@${digest}` is immutable, so what is validated below is the
# artifact this script writes into the overlay.
pinned="${image}@${digest}"
if [[ "$reader" == buildx ]]; then
  raw="$(docker buildx imagetools inspect --raw "$pinned")"
else
  raw="$(crane manifest "$pinned")"
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

if [[ "$mode" == print ]]; then
  echo "$digest"
  exit 0
fi

tmp="$(mktemp)"
trap 'rm -f "$tmp"' EXIT
for overlay in "${overlays[@]}"; do
  sed "s|^\( *digest: \)sha256:[0-9a-f]\{64\}|\1${digest}|" "$overlay" >"$tmp"
  grep -Fq "$digest" "$tmp" || {
    echo "failed to write the digest into ${overlay#"$repo_root"/}" >&2
    exit 1
  }
  cat "$tmp" >"$overlay"
  echo "pinned ${image}@${digest} (${reference}) in ${overlay#"$repo_root"/}"
done
echo "verify it before applying: SIGNER_IDENTITY=... GITHUB_REPOSITORY=Litvue/axond ops/verify-image-evidence.sh ${image}@${digest}"
