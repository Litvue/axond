#!/usr/bin/env bash
# Verify the published evidence for one image digest: keyless signature, SLSA
# provenance, and — when an SBOM is named — its SBOM attestation.
#
# Every published manifest goes through this, the per-architecture images and
# the multi-architecture index alike, so no digest can reach the release with a
# weaker chain than another. SIGNER_IDENTITY is the release workflow's trusted
# certificate identity pattern; refusing to run without it keeps a missing
# variable from silently degrading `cosign verify` into an unpinned check.
#
# The SBOM predicate type carries the SPDX version, so it is read out of the
# document instead of being hardcoded: an SBOM tool that starts emitting a
# different SPDX version must not silently turn the SBOM gate into a no-op.
#
# Usage: ops/verify-image-evidence.sh <image-ref-with-digest> [sbom-path]
set -euo pipefail

image_ref="${1:?usage: ops/verify-image-evidence.sh <image-ref-with-digest> [sbom-path]}"
sbom_path="${2:-}"

: "${SIGNER_IDENTITY:?SIGNER_IDENTITY must name the trusted signer certificate identity}"
: "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY must be set}"

case "$image_ref" in
  *@sha256:*) ;;
  *)
    echo "refusing to verify a mutable reference: $image_ref" >&2
    exit 1
    ;;
esac

cosign verify \
  --certificate-identity-regexp "$SIGNER_IDENTITY" \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  "$image_ref" >/dev/null
echo "signature verified: $image_ref"

gh attestation verify "oci://${image_ref}" \
  --repo "$GITHUB_REPOSITORY" \
  --predicate-type https://slsa.dev/provenance/v1 >/dev/null
echo "provenance verified: $image_ref"

if [[ -n "$sbom_path" ]]; then
  [[ -s "$sbom_path" ]] || {
    echo "SBOM $sbom_path is missing or empty" >&2
    exit 1
  }
  spdx_version="$(jq -r '.spdxVersion // empty' "$sbom_path")"
  [[ "$spdx_version" =~ ^SPDX-[0-9]+\.[0-9]+$ ]] || {
    echo "SBOM $sbom_path does not declare an SPDX version: ${spdx_version:-none}" >&2
    exit 1
  }
  gh attestation verify "oci://${image_ref}" \
    --repo "$GITHUB_REPOSITORY" \
    --predicate-type "https://spdx.dev/Document/v${spdx_version#SPDX-}" >/dev/null
  echo "SBOM attestation verified (${spdx_version}): $image_ref"
fi
