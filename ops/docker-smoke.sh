#!/usr/bin/env bash
# Boot a built Axond image against the example config and prove it serves.
#
# The image is stateless and ships no config, so the example config is mounted
# in and pointed at with AXOND_CONFIG. A gateway that will not answer /healthz
# is not shippable, so a failed probe fails the whole smoke.
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
