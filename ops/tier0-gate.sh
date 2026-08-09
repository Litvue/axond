#!/usr/bin/env bash
# Boot and serve axond in a kernel-enforced, network-denied namespace.
#
# This is the mechanical Tier 0 guarantee from ADR 0017 and ADR 0002: the
# default gateway starts and serves without a datastore or outbound network.
# Loopback remains available by design, so a local fake provider can exercise
# the real serving path; this does not prove that a same-namespace datastore
# could never be reached, which is why the usual Redis and Postgres ports are
# asserted empty as well.
#
# Usage: ops/tier0-gate.sh [axond-binary]
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ -z "${AXOND_TIER0_NETNS:-}" ]]; then
  if ! command -v unshare >/dev/null 2>&1; then
    echo "TIER 0 INVARIANT FAILED: unshare is required; refusing to run with networking enabled." >&2
    exit 1
  fi
  namespace_body='ip link set lo up || { echo "TIER 0 INVARIANT FAILED: could not enable namespace loopback." >&2; exit 1; }; export AXOND_TIER0_NETNS=1; exec "$@"'
  if unshare --user --map-root-user --net --fork true >/dev/null 2>&1; then
    exec unshare --user --map-root-user --net --fork bash -c \
      "$namespace_body" bash "$0" "$@"
  fi
  echo "unprivileged user/network namespace unavailable; trying passwordless sudo fallback" >&2
  if ! sudo -n unshare --net --fork bash -c "$namespace_body" bash "$0" "$@"; then
    echo "TIER 0 INVARIANT FAILED: neither unprivileged unshare nor sudo network namespace creation worked; refusing to run with networking enabled." >&2
    exit 1
  fi
  exit 0
fi

bin="${1:-${AXOND_BIN:-}}"
if [[ -z "$bin" ]]; then
  if [[ -x "$repo_root/target/x86_64-unknown-linux-musl/release/axond" ]]; then
    bin="$repo_root/target/x86_64-unknown-linux-musl/release/axond"
  else
    bin="$repo_root/target/debug/axond"
  fi
fi
if [[ ! -x "$bin" ]]; then
  echo "TIER 0 INVARIANT FAILED: axond binary is not executable: $bin" >&2
  exit 1
fi

config="$repo_root/tests/tier0/axond.tier0.toml"
upstream="$repo_root/tests/tier0/fake_upstream_serve.py"
gateway_log="$(mktemp "${TMPDIR:-/tmp}/axond-tier0-gateway.XXXXXX.log")"
upstream_log="$(mktemp "${TMPDIR:-/tmp}/axond-tier0-upstream.XXXXXX.log")"
gateway_pid=""
upstream_pid=""

failure() {
  echo >&2
  echo "TIER 0 INVARIANT FAILED: $*" >&2
  echo "ADR 0017/0002 require config-only axond to have no datastore or network dependency at boot or on the serving path." >&2
  echo "If a feature added this dependency, gate it behind an opt-in higher tier." >&2
  echo "--- gateway log ($gateway_log) ---" >&2
  cat "$gateway_log" >&2 || true
  echo "--- fake-upstream log ($upstream_log) ---" >&2
  cat "$upstream_log" >&2 || true
  exit 1
}

cleanup() {
  for pid in "$gateway_pid" "$upstream_pid"; do
    [[ -n "$pid" ]] || continue
    kill "$pid" 2>/dev/null || true
    for _ in $(seq 1 20); do
      kill -0 "$pid" 2>/dev/null || break
      sleep 0.1
    done
    kill -KILL "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  done
  rm -f "$gateway_log" "$upstream_log"
}
trap cleanup EXIT

echo "Tier 0: checking network namespace denial"
if timeout 2 bash -c '</dev/tcp/1.1.1.1/443' >/dev/null 2>&1; then
  failure "network namespace permits outbound TCP (sandbox silently degraded to network-enabled; the gate is worthless)"
fi
if getent hosts example.com >/dev/null 2>&1; then
  failure "network namespace permits public DNS resolution (sandbox silently degraded to network-enabled; the gate is worthless)"
fi
echo "sandbox: outbound TCP denied; public DNS denied"

for port in 6379 5432; do
  if timeout 1 bash -c "</dev/tcp/127.0.0.1/$port" >/dev/null 2>&1; then
    failure "datastore invariant violated: 127.0.0.1:$port is reachable (Tier 0 forbids Redis/Postgres)"
  fi
done
echo "datastore ports: Redis 6379 and Postgres 5432 unreachable"

python3 "$upstream" --port 18082 >"$upstream_log" 2>&1 &
upstream_pid=$!

for _ in $(seq 1 30); do
  upstream_status="$(curl --silent --output /dev/null --write-out '%{http_code}' \
    --max-time 1 "http://127.0.0.1:18082/" || true)"
  if [[ "$upstream_status" != 000 ]]; then
    break
  fi
  sleep 0.1
done
[[ "${upstream_status:-000}" != 000 ]] ||
  failure "local fake-upstream did not bind; Tier 0 serving-path check cannot proceed"

AXOND_CONFIG="$config" \
GW_TIER0_UPSTREAM_KEY=tier0-upstream-placeholder \
GW_TIER0_INBOUND_KEY=tier0-gateway-key \
GW_TIER0_VERIFIER=tier0-verifier-secret-012345678901234567890123 \
RUST_LOG=warn \
"$bin" >"$gateway_log" 2>&1 &
gateway_pid=$!

base_url="http://127.0.0.1:18081"
ready=0
for _ in $(seq 1 60); do
  if curl --silent --show-error --max-time 1 "$base_url/healthz" 2>/dev/null | grep -qx 'ok'; then
    ready=1
    break
  fi
  if ! kill -0 "$gateway_pid" 2>/dev/null; then
    failure "gateway exited before /healthz; Tier 0 must boot without a datastore or network"
  fi
  sleep 0.1
done
[[ "$ready" == 1 ]] || failure "gateway did not serve /healthz; Tier 0 boot is not available"

health="$(curl --fail --silent "$base_url/healthz")"
ready_body="$(curl --fail --silent "$base_url/readyz")"
[[ "$health" == ok ]] || failure "/healthz was not ok: $health"
[[ "$ready_body" == ready ]] || failure "/readyz was not ready: $ready_body"

models="$(curl --fail --silent -H 'Authorization: Bearer tier0-gateway-key' "$base_url/v1/models")"
grep -q '"id":"fixture-chat"' <<<"$models" || failure "authenticated /v1/models omitted configured alias fixture-chat"

unauth_status="$(curl --silent --output /dev/null --write-out '%{http_code}' "$base_url/v1/models")"
[[ "$unauth_status" == 401 ]] || failure "unauthenticated /v1/models returned $unauth_status instead of 401"

unknown_body="$(mktemp "${TMPDIR:-/tmp}/axond-tier0-unknown.XXXXXX")"
unknown_status="$(curl --silent --output "$unknown_body" --write-out '%{http_code}' \
  -H 'Authorization: Bearer tier0-gateway-key' -H 'content-type: application/json' \
  -d '{"model":"does-not-exist","messages":[{"role":"user","content":"hello"}]}' \
  "$base_url/v1/chat/completions")"
grep -q '"type":"unknown_model"' "$unknown_body" || failure "unknown model response lacked typed unknown_model error"
[[ "$unknown_status" == 404 ]] || failure "unknown model returned $unknown_status instead of 404"
rm -f "$unknown_body"

fixture_body="$(mktemp "${TMPDIR:-/tmp}/axond-tier0-fixture.XXXXXX")"
fixture_status="$(curl --silent --output "$fixture_body" --write-out '%{http_code}' \
  -H 'Authorization: Bearer tier0-gateway-key' -H 'content-type: application/json' \
  -d '{"model":"fixture-chat","messages":[{"role":"user","content":"What is the capital of France?"}]}' \
  "$base_url/v1/chat/completions")"
grep -Eq '"object"[[:space:]]*:[[:space:]]*"chat.completion"' "$fixture_body" ||
  failure "fake-upstream response was not fixture-shaped"
[[ "$fixture_status" == 200 ]] || failure "local fake-upstream request returned $fixture_status instead of 200"
rm -f "$fixture_body"

echo "healthz: $health"
echo "readyz: $ready_body"
echo "models: fixture-chat"
echo "auth: unauthenticated /v1/models -> 401"
echo "errors: unknown model -> 404 unknown_model"
echo "serving: local fixture upstream -> 200 chat.completion"
echo "Tier 0 hermetic boot and serve passed"
