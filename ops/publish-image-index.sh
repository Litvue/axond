#!/usr/bin/env bash
# Join the per-architecture release images into the multi-architecture index
# that `<version>` and `sha-<short>` point at, then prove what it contains.
#
# The children are the images the per-platform lanes already smoked, signed, and
# attested; the index only references them by digest, so no artifact is rebuilt
# here and nothing unattested can enter it. The index is asserted to contain
# exactly the supported platforms — an extra, missing, or unexpected child is a
# release failure, not a surprise for whoever pulls it on the other
# architecture. There is deliberately no `latest` tag.
#
# Inputs (environment): IMAGE_NAME, RELEASE_VERSION, RELEASE_SHORT_SHA,
# RELEASE_COMMIT_SHA, GITHUB_REPOSITORY. Writes `digest=<index-digest>` to
# GITHUB_OUTPUT when it is set.
set -euo pipefail

: "${IMAGE_NAME:?IMAGE_NAME must be set}"
: "${RELEASE_VERSION:?RELEASE_VERSION must be set}"
: "${RELEASE_SHORT_SHA:?RELEASE_SHORT_SHA must be set}"
: "${RELEASE_COMMIT_SHA:?RELEASE_COMMIT_SHA must be set}"
: "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY must be set}"

# The supported platforms, as `<os>/<arch>`. ops/check-release-config.py keeps
# this list, the release workflow's image matrix, and the documentation in step.
PLATFORMS=(linux/amd64 linux/arm64)

child_refs=()
declare -A child_digests=()
for platform in "${PLATFORMS[@]}"; do
  arch="${platform#*/}"
  arch_ref="${IMAGE_NAME}:${RELEASE_VERSION}-${arch}"
  descriptor="$(docker buildx imagetools inspect "$arch_ref" --format '{{json .Manifest}}')"
  media_type="$(jq -r '.mediaType' <<<"$descriptor")"
  case "$media_type" in
    application/vnd.oci.image.manifest.v1+json | application/vnd.docker.distribution.manifest.v2+json) ;;
    *)
      echo "$arch_ref is $media_type, not a single-platform image manifest" >&2
      exit 1
      ;;
  esac
  digest="$(jq -r '.digest' <<<"$descriptor")"
  [[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]] || {
    echo "could not resolve a digest for $arch_ref: ${digest:-none}" >&2
    exit 1
  }
  child_digests["$platform"]="$digest"
  child_refs+=("${IMAGE_NAME}@${digest}")
  echo "$platform child: $digest"
done

docker buildx imagetools create \
  --annotation "index:org.opencontainers.image.source=https://github.com/${GITHUB_REPOSITORY}" \
  --annotation "index:org.opencontainers.image.revision=${RELEASE_COMMIT_SHA}" \
  --annotation "index:org.opencontainers.image.version=${RELEASE_VERSION}" \
  --tag "${IMAGE_NAME}:${RELEASE_VERSION}" \
  --tag "${IMAGE_NAME}:sha-${RELEASE_SHORT_SHA}" \
  "${child_refs[@]}"

index_digest="$(
  docker buildx imagetools inspect "${IMAGE_NAME}:${RELEASE_VERSION}" \
    --format '{{json .Manifest}}' | jq -r '.digest'
)"
[[ "$index_digest" =~ ^sha256:[0-9a-f]{64}$ ]] || {
  echo "could not resolve the index digest: ${index_digest:-none}" >&2
  exit 1
}

index="$(docker buildx imagetools inspect --raw "${IMAGE_NAME}@${index_digest}")"
index_media_type="$(jq -r '.mediaType' <<<"$index")"
case "$index_media_type" in
  application/vnd.oci.image.index.v1+json | application/vnd.docker.distribution.manifest.list.v2+json) ;;
  *)
    echo "${IMAGE_NAME}@${index_digest} is $index_media_type, not a multi-architecture index" >&2
    exit 1
    ;;
esac

# Every descriptor is classified, and anything that is neither an expected
# platform child nor an attestation manifest attached to one is a failure. A
# silent skip here would let an unrelated or unattested manifest ride along
# inside the reference operators deploy.
platform_children=()
while IFS= read -r descriptor; do
  [[ -n "$descriptor" ]] || continue
  digest="$(jq -r '.digest' <<<"$descriptor")"
  os="$(jq -r '.platform.os // ""' <<<"$descriptor")"
  architecture="$(jq -r '.platform.architecture // ""' <<<"$descriptor")"
  if [[ "$os" == unknown || "$architecture" == unknown || -z "$os" || -z "$architecture" ]]; then
    # Buildx marks attestation manifests with the unknown/unknown platform and
    # names the manifest they describe. Accept exactly that, referring to a
    # child this script itself resolved.
    reference_type="$(
      jq -r '.annotations["vnd.docker.reference.type"] // ""' <<<"$descriptor"
    )"
    referenced="$(
      jq -r '.annotations["vnd.docker.reference.digest"] // ""' <<<"$descriptor"
    )"
    if [[ "$reference_type" != attestation-manifest ]]; then
      echo "index descriptor $digest has no platform and is not an attestation manifest" >&2
      exit 1
    fi
    matched=0
    for platform in "${PLATFORMS[@]}"; do
      if [[ "${child_digests[$platform]}" == "$referenced" ]]; then
        matched=1
      fi
    done
    if [[ "$matched" != 1 ]]; then
      echo "index attestation $digest describes ${referenced:-nothing}, which is not a release child" >&2
      exit 1
    fi
    echo "attestation descriptor $digest for $referenced"
    continue
  fi
  platform_children+=("$os/$architecture=$digest")
done < <(jq -c '.manifests[]' <<<"$index")

actual="$(printf '%s\n' ${platform_children[@]+"${platform_children[@]}"} | sort)"
expected="$(
  for platform in "${PLATFORMS[@]}"; do
    printf '%s=%s\n' "$platform" "${child_digests[$platform]}"
  done | sort
)"
if [[ "$actual" != "$expected" ]]; then
  printf 'index platforms do not match the release matrix\nexpected:\n%s\nactual:\n%s\n' \
    "$expected" "$actual" >&2
  exit 1
fi

# The tag the short SHA advertises must be the same index, not a stale pointer.
sha_tag_digest="$(
  docker buildx imagetools inspect "${IMAGE_NAME}:sha-${RELEASE_SHORT_SHA}" \
    --format '{{json .Manifest}}' | jq -r '.digest'
)"
if [[ "$sha_tag_digest" != "$index_digest" ]]; then
  echo "sha-${RELEASE_SHORT_SHA} points at $sha_tag_digest, not the index $index_digest" >&2
  exit 1
fi

printf 'multi-architecture index %s contains:\n%s\n' "$index_digest" "$actual"
if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
  echo "digest=$index_digest" >> "$GITHUB_OUTPUT"
fi
