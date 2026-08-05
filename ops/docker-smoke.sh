#!/usr/bin/env bash
# Boot a built Axond image against the example config and prove it serves.
#
# The image is stateless and ships no config, so the example config is mounted
# in and pointed at with AXOND_CONFIG. A gateway that will not answer /healthz
# is not shippable, so a failed probe fails the whole smoke.
#
# Boot refuses a credential or gateway key whose env var is unset, so every env
# var the example config references is supplied with a placeholder. Nothing here
# is dispatched upstream and the probed routes are unauthenticated, so the values
# are never used as keys. Two gateway keys may not share a secret, so the inbound
# placeholders differ.
#
# Usage: ops/docker-smoke.sh <image-ref>
set -euo pipefail

image="${1:?usage: ops/docker-smoke.sh <image-ref>}"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
container="axond-smoke-$$"
host_port=18080

cleanup() {
  docker rm -f "$container" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker run -d --name "$container" \
  -p "${host_port}:8080" \
  -e AXOND_CONFIG=/etc/axond/axond.toml \
  -e GW_PLATFORM_OPENAI_API_KEY=smoke-placeholder \
  -e GW_PLATFORM_OPENAI_API_KEY_OVERFLOW=smoke-placeholder \
  -e GW_PLATFORM_ANTHROPIC_API_KEY=smoke-placeholder \
  -e GW_PLATFORM_AZURE_OPENAI_API_KEY=smoke-placeholder \
  -e GW_ACME_OPENAI_API_KEY=smoke-placeholder \
  -e GW_INBOUND_PLATFORM_KEY=smoke-placeholder-platform \
  -e GW_INBOUND_ACME_KEY=smoke-placeholder-acme \
  -v "${repo_root}/axond.example.toml:/etc/axond/axond.toml:ro" \
  "$image" >/dev/null

for attempt in $(seq 1 30); do
  if curl --fail --silent "http://127.0.0.1:${host_port}/healthz" >/tmp/axond-healthz; then
    body="$(cat /tmp/axond-healthz)"
    if [[ "$body" == "ok" ]]; then
      echo "healthz: $body"
      curl --fail --silent "http://127.0.0.1:${host_port}/v1/models" | tee /tmp/axond-models
      echo
      echo "axond image smoke passed"
      exit 0
    fi
    echo "unexpected /healthz body: $body" >&2
    break
  fi
  sleep 1
done

echo "axond did not answer /healthz; container logs:" >&2
docker logs "$container" >&2 || true
exit 1
