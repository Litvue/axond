#!/usr/bin/env bash
# Boot and serve axond in a kernel-enforced, network-denied namespace.
#
# This is the mechanical Tier 0 guarantee from ADR 0017 and ADR 0002: the
# default gateway starts and serves without a datastore or outbound network.
# Loopback remains available by design, so a local fake provider can exercise
# the real serving path. The namespace itself excludes every external
# datastore; an external Redis or Postgres dependency therefore fails boot or
# serving. After boot, the gate also requires exactly its gateway and
# fake-upstream listeners, catching a datastore or sidecar started in-namespace.
#
# Usage: ops/tier0-gate.sh [axond-binary]
#
# AXOND_TIER0_ALLOW_NO_NETNS=1 permits a degraded run when the host denies
# namespace creation outright. Boot and the entire serving path are still
# asserted; only the two guarantees the namespace itself provides — egress denial
# and the listener set — are skipped, loudly. It exists for the release lanes,
# where a sandbox restriction on a hosted runner must not fail an otherwise valid
# release. CI leaves it unset, so the hermetic guarantee is enforced on every
# change.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

requested_bin="${1:-${AXOND_BIN:-}}"
if [[ -z "$requested_bin" ]]; then
  if [[ -x "$repo_root/target/x86_64-unknown-linux-musl/release/axond" ]]; then
    requested_bin="$repo_root/target/x86_64-unknown-linux-musl/release/axond"
  else
    requested_bin="$repo_root/target/debug/axond"
  fi
fi
if [[ ! -x "$requested_bin" ]]; then
  echo "TIER 0 INVARIANT FAILED: axond binary is not executable: $requested_bin" >&2
  exit 1
fi
bin="$(realpath -e "$requested_bin")"
requested_tmpdir="${2:-${TMPDIR:-/tmp}}"
tmpdir="$(realpath -e "$requested_tmpdir")"
if [[ ! -d "$tmpdir" ]]; then
  echo "TIER 0 INVARIANT FAILED: temporary directory is not a directory: $tmpdir" >&2
  exit 1
fi

allow_no_netns=0
case "${AXOND_TIER0_ALLOW_NO_NETNS:-0}" in
  1 | true | yes | on) allow_no_netns=1 ;;
  0 | false | no | off | '') allow_no_netns=0 ;;
  *)
    echo "TIER 0 INVARIANT FAILED: AXOND_TIER0_ALLOW_NO_NETNS must be 1/0, true/false, yes/no, or on/off" >&2
    exit 1
    ;;
esac

no_namespace() {
  # Either refuse — the hermetic guarantee is the point of this gate — or, when
  # the caller has opted in, keep the boot and serving assertions and drop the
  # two the namespace was providing.
  if [[ "$allow_no_netns" != 1 ]]; then
    echo "TIER 0 INVARIANT FAILED: $1; refusing to run with networking enabled." >&2
    echo "Set AXOND_TIER0_ALLOW_NO_NETNS=1 to accept a degraded run that still boots and serves but cannot prove egress denial." >&2
    exit 1
  fi
  echo "TIER 0 DEGRADED: $1; AXOND_TIER0_ALLOW_NO_NETNS=1, so boot and the serving path are still asserted while egress denial and the listener invariant are not." >&2
}

if [[ -z "${AXOND_TIER0_NETNS:-}" ]]; then
  namespace_body='ip link set lo up || { echo "TIER 0 INVARIANT FAILED: could not enable namespace loopback." >&2; exit 1; }; export AXOND_TIER0_NETNS=1; exec "$@"'
  if ! command -v unshare >/dev/null 2>&1; then
    no_namespace "unshare is not installed"
    export AXOND_TIER0_DEGRADED=1
  elif unshare --user --map-root-user --net --fork true >/dev/null 2>&1; then
    exec unshare --user --map-root-user --net --fork bash -c \
      "$namespace_body" bash "$0" "$bin" "$tmpdir"
  else
    echo "unprivileged user/network namespace unavailable; trying passwordless sudo fallback" >&2
    if sudo -n unshare --net --fork true >/dev/null 2>&1; then
      if sudo -n unshare --net --fork bash -c "$namespace_body" bash "$0" "$bin" "$tmpdir"; then
        exit 0
      else
        status=$?
        exit "$status"
      fi
    fi
    no_namespace "neither unprivileged unshare nor sudo network namespace creation worked"
    export AXOND_TIER0_DEGRADED=1
  fi
fi

degraded="${AXOND_TIER0_DEGRADED:-0}"
if [[ "$degraded" == 1 ]]; then
  # Outside a namespace the fixed ports are the host's, so a stale listener would
  # be mistaken for the gateway or the fake upstream.
  for port in 18081 18082; do
    if ss -H -ltn "sport = :$port" | grep -q .; then
      echo "TIER 0 INVARIANT FAILED: port $port is already in use; a degraded run needs both fixed ports free." >&2
      exit 1
    fi
  done
fi

config="$repo_root/tests/tier0/axond.tier0.toml"
stateful_config="$repo_root/tests/tier0/axond.stateful-bootstrap.toml"
upstream="$repo_root/tests/tier0/fake_upstream_serve.py"
gateway_log="$(mktemp "$tmpdir/axond-tier0-gateway.XXXXXX.log")"
upstream_log="$(mktemp "$tmpdir/axond-tier0-upstream.XXXXXX.log")"
gateway_pid=""
upstream_pid=""
health_probe_body=""
ready_probe_body=""
models_probe_body=""
unknown_body=""
fixture_body=""
stateful_log=""

failure() {
  echo >&2
  echo "TIER 0 INVARIANT FAILED: $*" >&2
  echo "ADR 0017/0002 require config-only axond to have no datastore or network dependency at boot or on the serving path." >&2
  echo "If a feature added this dependency, gate it behind an opt-in higher tier." >&2
  echo "--- gateway log ($gateway_log) ---" >&2
  cat "$gateway_log" >&2 || true
  echo "--- fake-upstream log ($upstream_log) ---" >&2
  cat "$upstream_log" >&2 || true
  if [[ -n "$stateful_log" ]]; then
    echo "--- stateful bootstrap log ($stateful_log) ---" >&2
    cat "$stateful_log" >&2 || true
  fi
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
  rm -f "$gateway_log" "$upstream_log" "$health_probe_body" "$ready_probe_body" \
    "$models_probe_body" "$unknown_body" "$fixture_body" "$stateful_log"
}
trap cleanup EXIT

if [[ "$degraded" == 1 ]]; then
  echo "sandbox: DEGRADED, egress denial not checked"
else
  echo "Tier 0: checking network namespace denial"
  if timeout 2 bash -c '</dev/tcp/1.1.1.1/443' >/dev/null 2>&1; then
    failure "network namespace permits outbound TCP (sandbox silently degraded to network-enabled; the gate is worthless)"
  fi
  if getent hosts example.com >/dev/null 2>&1; then
    failure "network namespace permits public DNS resolution (sandbox silently degraded to network-enabled; the gate is worthless)"
  fi
  echo "sandbox: outbound TCP denied; public DNS denied"
fi

listener_ports() {
  ss -H -ltn | awk '{print $4}' | awk -F: '{print $NF}' | sort -n | uniq
}
baseline_listeners="$(listener_ports)"

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

env -u OTEL_EXPORTER_OTLP_ENDPOINT -u OTEL_EXPORTER_OTLP_PROTOCOL \
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

# The listener set is only an invariant inside the namespace: on a shared host any
# unrelated service would break it, which is why a degraded run cannot assert it.
listeners="$(listener_ports)"
expected_listeners="$(printf '%s\n18081\n18082\n' "$baseline_listeners" | sed '/^$/d' | sort -n | uniq)"
if [[ "$degraded" == 1 ]]; then
  echo "listeners: DEGRADED, in-namespace listener invariant not checked"
elif [[ "$listeners" != "$expected_listeners" ]]; then
  failure "listener invariant violated: namespace must contain its baseline listeners plus only gateway 18081 and fake upstream 18082; unexpected listener set (${listeners//$'\n'/, }), expected (${expected_listeners//$'\n'/, }). This includes any Redis 6379 or Postgres 5432 listener. External datastore dependencies are excluded by the network namespace and would instead appear as boot or serving failure."
fi
if [[ "$degraded" != 1 ]]; then
  echo "namespace listeners: baseline (${baseline_listeners//$'\n'/, }) plus gateway 18081 and fake upstream 18082 only; external Redis 6379/Postgres 5432 are excluded by namespace egress denial"
fi

health_probe_body="$(mktemp "$tmpdir/axond-tier0-healthz.XXXXXX")"
health_status="$(curl --silent --show-error --max-time 5 --output "$health_probe_body" \
  --write-out '%{http_code}' "$base_url/healthz" || true)"
[[ "$health_status" == 200 ]] ||
  failure "/healthz returned HTTP $health_status instead of 200"
health="$(cat "$health_probe_body")"
[[ "$health" == ok ]] || failure "/healthz body was not ok: $health"
rm -f "$health_probe_body"
health_probe_body=""

ready_probe_body="$(mktemp "$tmpdir/axond-tier0-readyz.XXXXXX")"
ready_status="$(curl --silent --show-error --max-time 5 --output "$ready_probe_body" \
  --write-out '%{http_code}' "$base_url/readyz" || true)"
[[ "$ready_status" == 200 ]] ||
  failure "/readyz returned HTTP $ready_status instead of 200"
ready_body="$(cat "$ready_probe_body")"
[[ "$ready_body" == ready ]] || failure "/readyz body was not ready: $ready_body"
rm -f "$ready_probe_body"
ready_probe_body=""

models_probe_body="$(mktemp "$tmpdir/axond-tier0-models.XXXXXX")"
models_status="$(curl --silent --show-error --max-time 5 --output "$models_probe_body" \
  --write-out '%{http_code}' -H 'Authorization: Bearer tier0-gateway-key' \
  "$base_url/v1/models" || true)"
[[ "$models_status" == 200 ]] ||
  failure "authenticated /v1/models returned HTTP $models_status instead of 200"
models="$(cat "$models_probe_body")"
grep -q '"id":"fixture-chat"' <<<"$models" || failure "authenticated /v1/models omitted configured alias fixture-chat"
rm -f "$models_probe_body"
models_probe_body=""

unauth_status="$(curl --silent --max-time 5 --output /dev/null \
  --write-out '%{http_code}' "$base_url/v1/models" || true)"
[[ "$unauth_status" == 401 ]] || failure "unauthenticated /v1/models returned $unauth_status instead of 401"

unknown_body="$(mktemp "$tmpdir/axond-tier0-unknown.XXXXXX")"
unknown_status="$(curl --silent --max-time 5 --output "$unknown_body" \
  --write-out '%{http_code}' \
  -H 'Authorization: Bearer tier0-gateway-key' -H 'content-type: application/json' \
  -d '{"model":"does-not-exist","messages":[{"role":"user","content":"hello"}]}' \
  "$base_url/v1/chat/completions" || true)"
[[ "$unknown_status" == 404 ]] || failure "unknown model returned $unknown_status instead of 404"
grep -q '"type":"unknown_model"' "$unknown_body" || failure "unknown model response lacked typed unknown_model error"
rm -f "$unknown_body"

fixture_body="$(mktemp "$tmpdir/axond-tier0-fixture.XXXXXX")"
fixture_status="$(curl --silent --max-time 5 --output "$fixture_body" \
  --write-out '%{http_code}' \
  -H 'Authorization: Bearer tier0-gateway-key' -H 'content-type: application/json' \
  -d '{"model":"fixture-chat","messages":[{"role":"user","content":"What is the capital of France?"}]}' \
  "$base_url/v1/chat/completions" || true)"
[[ "$fixture_status" == 200 ]] || failure "local fake-upstream request returned $fixture_status instead of 200"
grep -Eq '"object"[[:space:]]*:[[:space:]]*"chat.completion"' "$fixture_body" ||
  failure "fake-upstream response was not fixture-shaped"
rm -f "$fixture_body"

# Stateful bootstrap validates without a database: the same namespace that
# denies egress is where a config-parse connection attempt would fail, so a
# clean refusal here is evidence that parsing connects to nothing. The process
# must also refuse to serve while the control plane is unimplemented, and its
# diagnostic must name references rather than values (ADR 0027).
stateful_log="$(mktemp "$tmpdir/axond-tier0-stateful.XXXXXX.log")"
stateful_status=0
env -u OTEL_EXPORTER_OTLP_ENDPOINT -u OTEL_EXPORTER_OTLP_PROTOCOL \
  -u GW_TIER0_CONTROL_PLANE_DSN -u GW_TIER0_SECRET_STORE_KEK \
  -u GW_TIER0_ADMIN_BREAKGLASS \
  AXOND_CONFIG="$stateful_config" RUST_LOG=warn \
  timeout 10 "$bin" >"$stateful_log" 2>&1 || stateful_status=$?
[[ "$stateful_status" != 0 ]] ||
  failure "a stateful process started while the control plane is unimplemented; it must refuse rather than serve an empty snapshot"
[[ "$stateful_status" != 124 ]] ||
  failure "a stateful process kept running instead of refusing at boot"
grep -q 'stateful' "$stateful_log" ||
  failure "stateful refusal did not explain the mode"
if grep -Eq 'postgres(ql)?://|dbname=' "$stateful_log"; then
  failure "stateful diagnostics must name references, never a resolved DSN"
fi
rm -f "$stateful_log"
stateful_log=""

echo "healthz: $health"
echo "readyz: $ready_body"
echo "models: fixture-chat"
echo "auth: unauthenticated /v1/models -> 401"
echo "errors: unknown model -> 404 unknown_model"
echo "serving: local fixture upstream -> 200 chat.completion"
echo "stateful: bootstrap validates with no datastore, then refuses to serve"
if [[ "$degraded" == 1 ]]; then
  echo "Tier 0 boot and serve passed (DEGRADED: no namespace, so egress denial and the listener invariant were not proven)"
else
  echo "Tier 0 hermetic boot and serve passed"
fi
