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
# The tags are an input because the operator-facing ones are applied last. The
# index is first published under a staging tag and booted on both architectures
# by digest; only then is this script run again with `<version>`, `sha-<short>`,
# and EXPECT_INDEX_DIGEST set to the digest that was booted, so the reference the
# documentation tells people to deploy never exists before something has
# actually started it.
#
# In that promotion mode nothing is assembled: the smoked index is asserted and
# then retagged from its own digest. Reassembling it from the child tags and
# comparing digests afterwards would be too late — a child tag that moved since
# staging would already have published `<version>` pointing at an index nothing
# booted, and a registry tag cannot be retracted. So every check that can reject
# a promotion runs strictly before the first tag is applied.
#
# The mode is explicit, never inferred: INDEX_MODE=stage assembles under
# non-operator-facing tags, INDEX_MODE=promote retags a smoked digest. An empty
# EXPECT_INDEX_DIGEST therefore fails the promotion instead of quietly turning it
# into an assemble-then-tag run, and staging refuses to apply an operator-facing
# tag at all.
#
# Inputs (environment): INDEX_MODE, IMAGE_NAME, RELEASE_VERSION,
# RELEASE_SHORT_SHA, RELEASE_COMMIT_SHA, GITHUB_REPOSITORY, INDEX_TAGS
# (space-separated), and EXPECT_INDEX_DIGEST when INDEX_MODE=promote. Writes
# `digest=<index-digest>` to GITHUB_OUTPUT when it is set.
set -euo pipefail

: "${IMAGE_NAME:?IMAGE_NAME must be set}"
: "${RELEASE_VERSION:?RELEASE_VERSION must be set}"
: "${RELEASE_SHORT_SHA:?RELEASE_SHORT_SHA must be set}"
: "${RELEASE_COMMIT_SHA:?RELEASE_COMMIT_SHA must be set}"
: "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY must be set}"
: "${INDEX_TAGS:?INDEX_TAGS must be set}"
: "${INDEX_MODE:?INDEX_MODE must be stage or promote}"

case "$INDEX_MODE" in
  stage | promote) ;;
  *)
    echo "INDEX_MODE is $INDEX_MODE, not stage or promote" >&2
    exit 1
    ;;
esac

# The supported platforms, as `<os>/<arch>`. ops/check-release-config.py keeps
# this list, the release workflow's image matrix, and the documentation in step.
PLATFORMS=(linux/amd64 linux/arm64)

read -r -a index_tags <<<"$INDEX_TAGS"
[[ "${#index_tags[@]}" -gt 0 ]] || {
  echo "INDEX_TAGS names no tag" >&2
  exit 1
}

child_refs=()
amd64_child_digest=""
arm64_child_digest=""
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
  case "$platform" in
    linux/amd64) amd64_child_digest="$digest" ;;
    linux/arm64) arm64_child_digest="$digest" ;;
  esac
  child_refs+=("${IMAGE_NAME}@${digest}")
  echo "$platform child: $digest"
done

child_digest_for_platform() {
  case "$1" in
    linux/amd64) printf '%s\n' "$amd64_child_digest" ;;
    linux/arm64) printf '%s\n' "$arm64_child_digest" ;;
    *)
      echo "unsupported release platform: $1" >&2
      return 1
      ;;
  esac
}

assert_index_contents() {
  # Every descriptor is classified, and anything that is neither an expected
  # platform child nor an attestation manifest attached to one is a failure. A
  # silent skip here would let an unrelated or unattested manifest ride along
  # inside the reference operators deploy.
  local digest="$1" index index_media_type descriptor os architecture
  local reference_type referenced matched platform
  index="$(docker buildx imagetools inspect --raw "${IMAGE_NAME}@${digest}")"
  index_media_type="$(jq -r '.mediaType' <<<"$index")"
  case "$index_media_type" in
    application/vnd.oci.image.index.v1+json | application/vnd.docker.distribution.manifest.list.v2+json) ;;
    *)
      echo "${IMAGE_NAME}@${digest} is $index_media_type, not a multi-architecture index" >&2
      return 1
      ;;
  esac
  local platform_children=()
  local child
  while IFS= read -r descriptor; do
    [[ -n "$descriptor" ]] || continue
    child="$(jq -r '.digest' <<<"$descriptor")"
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
        echo "index descriptor $child has no platform and is not an attestation manifest" >&2
        return 1
      fi
      matched=0
      for platform in "${PLATFORMS[@]}"; do
        if [[ "$(child_digest_for_platform "$platform")" == "$referenced" ]]; then
          matched=1
        fi
      done
      if [[ "$matched" != 1 ]]; then
        echo "index attestation $child describes ${referenced:-nothing}, which is not a release child" >&2
        return 1
      fi
      echo "attestation descriptor $child for $referenced"
      continue
    fi
    platform_children+=("$os/$architecture=$child")
  done < <(jq -c '.manifests[]' <<<"$index")

  INDEX_CONTENTS="$(printf '%s\n' ${platform_children[@]+"${platform_children[@]}"} | sort)"
  local expected
  expected="$(
    for platform in "${PLATFORMS[@]}"; do
      printf '%s=%s\n' "$platform" "$(child_digest_for_platform "$platform")"
    done | sort
  )"
  if [[ "$INDEX_CONTENTS" != "$expected" ]]; then
    printf 'index platforms do not match the release matrix\nexpected:\n%s\nactual:\n%s\n' \
      "$expected" "$INDEX_CONTENTS" >&2
    return 1
  fi
}

apply_tags() {
  # $1 is the source: the child references to assemble, or an already-published
  # index digest to retag.
  local tag tag_args=()
  for tag in "${index_tags[@]}"; do
    tag_args+=(--tag "${IMAGE_NAME}:${tag}")
  done
  docker buildx imagetools create "${tag_args[@]}" "$@"
}

assert_tags_resolve_to() {
  # A registry tag cannot be withdrawn, so this only ever confirms what was just
  # applied; nothing may depend on it to prevent a bad tag.
  local digest="$1" tag tag_digest
  for tag in "${index_tags[@]}"; do
    tag_digest="$(
      docker buildx imagetools inspect "${IMAGE_NAME}:${tag}" \
        --format '{{json .Manifest}}' | jq -r '.digest'
    )"
    if [[ "$tag_digest" != "$digest" ]]; then
      echo "$tag points at $tag_digest, not the index $digest" >&2
      return 1
    fi
    echo "$tag points at the index $digest"
  done
}

if [[ "$INDEX_MODE" == promote ]]; then
  # Promotion. Nothing is assembled: the smoked index already exists, so it is
  # asserted first and then retagged from its own digest, which cannot yield a
  # different one. Assembling from the child tags again and checking the digest
  # afterwards would be too late — if a child tag had moved since staging, the
  # operator-facing tags would already point at an index nothing booted, and no
  # later failure can retract them.
  #
  # An unset or empty EXPECT_INDEX_DIGEST is a failure, not a fallback: the job
  # output it comes from could be empty after a workflow edit or a partial
  # re-dispatch, and inferring staging from that would publish `<version>` before
  # anything asserted it.
  index_digest="${EXPECT_INDEX_DIGEST:-}"
  [[ "$index_digest" =~ ^sha256:[0-9a-f]{64}$ ]] || {
    echo "INDEX_MODE=promote requires EXPECT_INDEX_DIGEST to name the smoked index; got: ${index_digest:-empty}" >&2
    exit 1
  }
  assert_index_contents "$index_digest"
  apply_tags "${IMAGE_NAME}@${index_digest}"
  assert_tags_resolve_to "$index_digest"
else
  # Staging. The tags here are not operator-facing, so assembling first and
  # asserting the result is safe: a failure strands a staging tag, not a release.
  # That safety is the reason an operator-facing tag is refused outright — it may
  # only be applied by a promotion, after the digest has been asserted.
  for tag in "${index_tags[@]}"; do
    if [[ "$tag" == "$RELEASE_VERSION" || "$tag" == "sha-${RELEASE_SHORT_SHA}" ]]; then
      echo "INDEX_MODE=stage cannot apply the operator-facing tag $tag; promote the smoked digest instead" >&2
      exit 1
    fi
  done
  [[ -z "${EXPECT_INDEX_DIGEST:-}" ]] || {
    echo "INDEX_MODE=stage was given EXPECT_INDEX_DIGEST; use INDEX_MODE=promote to retag a smoked index" >&2
    exit 1
  }
  apply_tags \
    --annotation "index:org.opencontainers.image.source=https://github.com/${GITHUB_REPOSITORY}" \
    --annotation "index:org.opencontainers.image.revision=${RELEASE_COMMIT_SHA}" \
    --annotation "index:org.opencontainers.image.version=${RELEASE_VERSION}" \
    "${child_refs[@]}"
  index_digest="$(
    docker buildx imagetools inspect "${IMAGE_NAME}:${index_tags[0]}" \
      --format '{{json .Manifest}}' | jq -r '.digest'
  )"
  [[ "$index_digest" =~ ^sha256:[0-9a-f]{64}$ ]] || {
    echo "could not resolve the index digest: ${index_digest:-none}" >&2
    exit 1
  }
  assert_index_contents "$index_digest"
  assert_tags_resolve_to "$index_digest"
fi

printf 'multi-architecture index %s contains:\n%s\n' "$index_digest" "$INDEX_CONTENTS"
if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
  echo "digest=$index_digest" >> "$GITHUB_OUTPUT"
fi
