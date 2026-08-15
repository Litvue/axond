#!/usr/bin/env bash
# Prove that the cosign the release installs still writes the signature format
# the published verification instructions tell operators to look for.
#
# `.github/workflows/release-please.yml` pairs a major-4 `cosign-installer` with
# `cosign-release: v2.5.2`, which is only meaningful if that installer still
# accepts the input and still resolves 2.x assets. That is an upstream API
# question: no amount of YAML inspection answers it, and the release itself
# verifies its own output with the binary that produced it, so a break surfaces
# in an operator's terminal rather than in CI. This runs the real installed
# binary against a throwaway registry — an architecture image and a multi-arch
# index, the two things the release signs — and asserts the signature lands on
# the `sha256-<digest>.sig` tag that cosign 2.x consumers resolve. cosign 3
# defaults to a protobuf bundle stored as an OCI 1.1 referring artifact, so it
# fails here on the tag assertion rather than in public.
#
# Signing is key-based: keyless needs an OIDC identity that a pull request does
# not have, and the storage format under test is the same either way.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

expected="$(
  grep -oE 'cosign-release:[[:space:]]*v[0-9]+\.[0-9]+\.[0-9]+' \
    .github/workflows/release-please.yml | head -n1 | awk '{print $2}'
)"
if [[ -z "$expected" ]]; then
  echo "check-cosign-format: release-please.yml pins no cosign-release" >&2
  exit 1
fi

# The installer resolved the input rather than silently falling back to its own
# default, which is the whole reason the pin is written down.
installed="$(cosign version 2>&1 | awk '/GitVersion:/ {print $2}')"
if [[ "$installed" != "$expected" ]]; then
  echo "check-cosign-format: release-please.yml pins cosign-release: ${expected}," \
    "but the installed binary reports ${installed:-<none>}; the installer no" \
    "longer honours the pin and the signature format is no longer the one" \
    "docs/installation.md documents" >&2
  exit 1
fi
echo "check-cosign-format: cosign ${installed} matches the release pin"

work="$(mktemp -d)"
registry=""
registry_port="${COSIGN_REGISTRY_PORT:-5001}"
registry_host="localhost:${registry_port}"
# The trap runs under `set -e`, so a failing removal must not take the rest of
# the cleanup — or the script's exit status — with it.
cleanup() {
  if [[ -n "$registry" ]]; then
    docker rm --force "$registry" >/dev/null 2>&1 || true
  fi
  rm -rf "$work"
}
trap cleanup EXIT

registry="cosign-format-check-$$"
docker run --detach --name "$registry" --publish "${registry_port}:5000" \
  registry:3@sha256:1be55279f18a2fe1a74edf2664cac61c1bea305b7b4642dab412e7affdcb3e33 \
  >/dev/null
for _ in $(seq 1 60); do
  curl --silent --fail "http://${registry_host}/v2/" >/dev/null && break
  sleep 1
done
curl --silent --fail "http://${registry_host}/v2/" >/dev/null

export COSIGN_PASSWORD=""
cosign generate-key-pair --output-key-prefix "$work/canary" >/dev/null
key="$work/canary.key"
pub="$work/canary.pub"

# The build context is its own directory so the private key is never among the
# files handed to the builder, whatever a later Dockerfile here copies.
context="$work/context"
mkdir "$context"
printf 'FROM scratch\nCOPY canary.txt /canary.txt\n' > "$context/Dockerfile"
printf 'cosign format canary\n' > "$context/canary.txt"

# Any tag the registry rejects would fail the push rather than the assertion, so
# the reference is fixed and the digest is read back from the registry.
image="${registry_host}/cosign-format-canary"

# A signature is stored under a tag derived from the digest it covers; a cosign
# 3 default-format signature is not reachable there at all.
assert_signature_tag() {
  local repository="$1" digest="$2" what="$3"
  local tag="${digest/:/-}.sig"
  if ! curl --silent --fail --output /dev/null \
    --header 'Accept: application/vnd.oci.image.manifest.v1+json' \
    --header 'Accept: application/vnd.docker.distribution.manifest.v2+json' \
    "http://${registry_host}/v2/${repository}/manifests/${tag}"; then
    echo "check-cosign-format: cosign ${installed} signed the ${what} but wrote no" \
      "${tag}; the signature is not where a cosign 2.x consumer following" \
      "docs/installation.md looks for it" >&2
    return 1
  fi
  echo "check-cosign-format: ${what} signature published as ${tag}"
}

# The registry reports the digest it stored the tag under, which is what the
# release signs; asking the local daemon instead can answer with a different
# descriptor than the one that was pushed.
digest_of() {
  curl --silent --head --fail \
    --header 'Accept: application/vnd.oci.image.index.v1+json' \
    --header 'Accept: application/vnd.oci.image.manifest.v1+json' \
    --header 'Accept: application/vnd.docker.distribution.manifest.list.v2+json' \
    --header 'Accept: application/vnd.docker.distribution.manifest.v2+json' \
    "http://${registry_host}/v2/cosign-format-canary/manifests/$1" |
    tr -d '\r' | awk '/^[Dd]ocker-[Cc]ontent-[Dd]igest:/ {print $2}'
}

for platform in amd64 arm64; do
  docker buildx build --provenance=false --sbom=false "--platform=linux/${platform}" \
    --tag "${image}:${platform}" --push "$context" >/dev/null
done
arch_digest="$(digest_of amd64)"
other_digest="$(digest_of arm64)"
test -n "$arch_digest"
test -n "$other_digest"
cosign sign --yes --key "$key" --tlog-upload=false "${image}@${arch_digest}"
assert_signature_tag cosign-format-canary "$arch_digest" "architecture image"
cosign verify --key "$pub" --insecure-ignore-tlog=true \
  "${image}@${arch_digest}" >/dev/null

# The release signs the promoted index as well, and an index is a different
# descriptor: a format change could reach one lane and not the other. The index
# is assembled the way ops/publish-image-index.sh assembles it.
docker buildx imagetools create --tag "${image}:index" \
  "${image}@${arch_digest}" "${image}@${other_digest}" >/dev/null
index_digest="$(digest_of index)"
test -n "$index_digest"
# Two children so the result is an index rather than a passed-through manifest:
# with one source, buildx may return the source descriptor itself, and the
# assertion below would then re-check the architecture image's signature and
# report the index as covered without ever signing one.
if [[ "$index_digest" == "$arch_digest" || "$index_digest" == "$other_digest" ]]; then
  echo "check-cosign-format: imagetools returned a child manifest (${index_digest})" \
    "instead of an index, so the index half of this check would prove nothing" >&2
  exit 1
fi
cosign sign --yes --key "$key" --tlog-upload=false "${image}@${index_digest}"
assert_signature_tag cosign-format-canary "$index_digest" "multi-arch index"
cosign verify --key "$pub" --insecure-ignore-tlog=true \
  "${image}@${index_digest}" >/dev/null

echo "check-cosign-format: signing format matches the published verification contract"
