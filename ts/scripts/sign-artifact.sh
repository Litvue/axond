#!/usr/bin/env bash
# Sign one artifact with cosign 2.x and verify that signature.
#
# Key mode (default) creates an ephemeral keypair, signs without uploading to
# the public transparency log, verifies, then deletes the private key. The
# public key stays next to the signature.
#
# Keyless mode (COSIGN_MODE=keyless) uses ambient OIDC, which GitHub Actions
# provides on a tag. COSIGN_IDENTITY_REGEXP must match the Fulcio identity.
set -euo pipefail

artifact="${1:?artifact path}"
out="${2:-$(dirname "$artifact")}"
cosign="${COSIGN:-cosign}"
base="$(basename "$artifact")"
mkdir -p "$out"

sha256="$(sha256sum "$artifact" | awk '{print $1}')"
printf '%s  %s\n' "$sha256" "$base" > "$out/$base.sha256"

if [[ "${COSIGN_MODE:-key}" == "keyless" ]]; then
  identity="${COSIGN_IDENTITY_REGEXP:?set COSIGN_IDENTITY_REGEXP for keyless verify}"
  "$cosign" sign-blob --yes \
    --output-signature "$out/$base.sig" \
    --output-certificate "$out/$base.pem" \
    "$artifact"
  "$cosign" verify-blob \
    --certificate "$out/$base.pem" \
    --signature "$out/$base.sig" \
    --certificate-oidc-issuer https://token.actions.githubusercontent.com \
    --certificate-identity-regexp "$identity" \
    "$artifact"
  printf 'signed %s keyless sha256 %s\n' "$base" "$sha256"
  exit 0
fi

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
export COSIGN_PASSWORD="${COSIGN_PASSWORD:-$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')}"
"$cosign" generate-key-pair --output-key-prefix "$work/cosign"
"$cosign" sign-blob --yes --tlog-upload=false \
  --key "$work/cosign.key" \
  --output-signature "$out/$base.sig" \
  "$artifact"
cp "$work/cosign.pub" "$out/$base.pub"
"$cosign" verify-blob --key "$out/$base.pub" \
  --signature "$out/$base.sig" \
  --insecure-ignore-tlog=true \
  "$artifact"
printf 'signed %s sha256 %s\n' "$base" "$sha256"
