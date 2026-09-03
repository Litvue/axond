#!/usr/bin/env bash
# Boot and serve axond in a kernel-enforced, network-denied namespace.
#
# ADR 0063 requires a Store. This gate boots a temp SQLite file and serves
# `/ns/{ns}/v1`. It is not a no-datastore promise. The namespace still excludes
# external Redis or Postgres: an outbound datastore dependency fails boot or
# serving. After boot, the gate requires exactly its gateway and fake-upstream
# listeners, catching a sidecar started in-namespace.
#
# Usage: ops/tier0-gate.sh [axond-binary]
#
# AXOND_TIER0_ALLOW_NO_NETNS=1 permits a degraded run when the host denies
# namespace creation outright. Boot and the entire serving path are still
# asserted; only the two guarantees the namespace itself provides — egress denial
# and the listener set — are skipped, loudly. It exists for the release lanes,
# where a sandbox restriction on a hosted runner must not fail an otherwise valid
# release. CI leaves it unset, so the network-isolation guarantee is enforced on
# every change.
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

# Every tool the gate needs is named here rather than discovered halfway through a
# run. `curl` is not negotiable: without it nothing can be probed at all. `ss` and
# `python3` each back one assertion, so when the caller has accepted a degraded run
# that assertion is skipped rather than failing an otherwise valid release.
missing_tool() {
  if [[ "$allow_no_netns" != 1 ]]; then
    echo "TIER 0 INVARIANT FAILED: $1 is required by the Tier 0 gate." >&2
    exit 1
  fi
  echo "TIER 0 DEGRADED: $1 is unavailable, so $2 is not checked." >&2
  return 1
}
command -v curl >/dev/null 2>&1 || {
  echo "TIER 0 INVARIANT FAILED: curl is required by the Tier 0 gate; nothing can be probed without it." >&2
  exit 1
}
check_listeners=1
command -v ss >/dev/null 2>&1 ||
  missing_tool ss "the listener invariant" ||
  check_listeners=0
check_serving=1
command -v python3 >/dev/null 2>&1 ||
  missing_tool python3 "the fixture serving path" ||
  check_serving=0
# Outside a namespace the listener set is the host's, so it is not an invariant.
[[ "$degraded" != 1 ]] || check_listeners=0

committed_config="$repo_root/tests/tier0/axond.tier0.toml"
runtime_config="$(mktemp "$tmpdir/axond-sqlite-boot.XXXXXX.toml")"
sqlite_path="$(mktemp "$tmpdir/axond-store.XXXXXX.sqlite")"
rm -f "$sqlite_path"
sed "s|^path = \"axond-tier0.sqlite\"$|path = \"${sqlite_path}\"|" \
  "$committed_config" >"$runtime_config"
grep -Fq "path = \"${sqlite_path}\"" "$runtime_config" || {
  echo "TIER 0 INVARIANT FAILED: could not bind the gate to a temp SQLite file." >&2
  exit 1
}

if [[ "$degraded" == 1 ]]; then
  # Outside a namespace the fixed ports are the host's, so a stale listener would
  # be mistaken for the gateway or the fake upstream. The gateway always binds
  # 18081; 18082 is only bound when the fixture upstream runs.
  required_free=(18081)
  [[ "$check_serving" != 1 ]] || required_free+=(18082)
  if command -v ss >/dev/null 2>&1; then
    for port in "${required_free[@]}"; do
      if ss -H -ltn "sport = :$port" | grep -q .; then
        echo "TIER 0 INVARIANT FAILED: port $port is already in use; a degraded run needs its fixed ports free." >&2
        exit 1
      fi
    done
  else
    # Never let a missing probe read as a free port: without `ss` the answer is
    # unknown, and a conflict would otherwise surface as a confusing boot failure.
    echo "TIER 0 DEGRADED: ss is unavailable, so port availability for ${required_free[*]} is not checked; a conflict will appear as a boot or bind failure." >&2
  fi
fi

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

failure() {
  echo >&2
  echo "TIER 0 INVARIANT FAILED: $*" >&2
  echo "ADR 0063 requires a Store: this gate boots a temp SQLite file and serves /ns/{ns}/v1. It is not a no-datastore promise." >&2
  echo "External Redis or Postgres must not be required for this single-replica boot." >&2
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
  rm -f "$gateway_log" "$upstream_log" "$health_probe_body" "$ready_probe_body" \
    "$models_probe_body" "$unknown_body" "$fixture_body" "$runtime_config" \
    "$sqlite_path" "${sqlite_path}-wal" "${sqlite_path}-shm"
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
baseline_listeners=""
if [[ "$check_listeners" == 1 ]]; then
  baseline_listeners="$(listener_ports)"
fi

if [[ "$check_serving" == 1 ]]; then
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
fi

env -u OTEL_EXPORTER_OTLP_ENDPOINT -u OTEL_EXPORTER_OTLP_PROTOCOL \
AXOND_CONFIG="$runtime_config" \
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
    failure "gateway exited before /healthz; a temp SQLite file must be enough to boot"
  fi
  sleep 0.1
done
[[ "$ready" == 1 ]] || failure "gateway did not serve /healthz; SQLite boot is not available"

# The listener set is only an invariant inside the namespace: on a shared host any
# unrelated service would break it, which is why a degraded run cannot assert it.
if [[ "$check_listeners" != 1 ]]; then
  echo "listeners: DEGRADED, in-namespace listener invariant not checked"
else
  listeners="$(listener_ports)"
  # Expect exactly the ports this run actually starts: 18082 belongs to the
  # fixture upstream, which a run without `python3` never launched.
  if [[ "$check_serving" == 1 ]]; then
    expected_ports=$'18081\n18082'
    expected_description="gateway 18081 and fake upstream 18082"
  else
    expected_ports='18081'
    expected_description="gateway 18081 (the fixture upstream was not started)"
  fi
  expected_listeners="$(printf '%s\n%s\n' "$baseline_listeners" "$expected_ports" | sed '/^$/d' | sort -n | uniq)"
  if [[ "$listeners" != "$expected_listeners" ]]; then
    failure "listener invariant violated: namespace must contain its baseline listeners plus only $expected_description; unexpected listener set (${listeners//$'\n'/, }), expected (${expected_listeners//$'\n'/, }). This includes any Redis 6379 or Postgres 5432 listener. External datastore dependencies are excluded by the network namespace and would instead appear as boot or serving failure."
  fi
  echo "namespace listeners: baseline (${baseline_listeners//$'\n'/, }) plus $expected_description only; external Redis 6379/Postgres 5432 are excluded by namespace egress denial"
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

# ADR 0063: no budget row is 429 budget_exceeded. Litvue PUTs on create.
budget_body="$(mktemp "$tmpdir/axond-tier0-budget.XXXXXX")"
budget_status="$(curl --silent --show-error --max-time 5 --output "$budget_body" \
  --write-out '%{http_code}' \
  -X PUT -H 'Authorization: Bearer tier0-gateway-key' -H 'content-type: application/json' \
  -d '{"limit_microdollars":1000000000000}' \
  "$base_url/api/v1/namespaces/platform/budgets/tier0" || true)"
[[ "$budget_status" == 200 || "$budget_status" == 201 ]] ||
  failure "PUT platform budget returned HTTP $budget_status: $(cat "$budget_body")"
rm -f "$budget_body"
budget_body=""

models_probe_body="$(mktemp "$tmpdir/axond-tier0-models.XXXXXX")"
models_status="$(curl --silent --show-error --max-time 5 --output "$models_probe_body" \
  --write-out '%{http_code}' -H 'Authorization: Bearer tier0-gateway-key' \
  "$base_url/ns/platform/v1/models" || true)"
[[ "$models_status" == 200 ]] ||
  failure "authenticated /ns/platform/v1/models returned HTTP $models_status instead of 200"
models="$(cat "$models_probe_body")"
grep -Eq '"object"[[:space:]]*:[[:space:]]*"list"' <<<"$models" ||
  failure "authenticated /ns/platform/v1/models was not a JSON object list"
rm -f "$models_probe_body"
models_probe_body=""

unauth_status="$(curl --silent --max-time 5 --output /dev/null \
  --write-out '%{http_code}' "$base_url/ns/platform/v1/models" || true)"
[[ "$unauth_status" == 401 ]] || failure "unauthenticated /ns/platform/v1/models returned $unauth_status instead of 401"

unknown_body="$(mktemp "$tmpdir/axond-tier0-unknown.XXXXXX")"
unknown_status="$(curl --silent --max-time 5 --output "$unknown_body" \
  --write-out '%{http_code}' \
  -H 'Authorization: Bearer tier0-gateway-key' -H 'content-type: application/json' \
  -d '{"model":"does-not-exist","messages":[{"role":"user","content":"hello"}]}' \
  "$base_url/ns/platform/v1/chat/completions" || true)"
[[ "$unknown_status" == 400 ]] || failure "unprefixed model returned $unknown_status instead of 400"
grep -q '"type":"model_unprefixed"' "$unknown_body" || failure "unprefixed model response lacked typed model_unprefixed error"
rm -f "$unknown_body"

if [[ "$check_serving" == 1 ]]; then
  fixture_body="$(mktemp "$tmpdir/axond-tier0-fixture.XXXXXX")"
  fixture_status="$(curl --silent --max-time 5 --output "$fixture_body" \
    --write-out '%{http_code}' \
    -H 'Authorization: Bearer tier0-gateway-key' -H 'content-type: application/json' \
    -d '{"model":"fake-openai/fixture-chat","messages":[{"role":"user","content":"What is the capital of France?"}]}' \
    "$base_url/ns/platform/v1/chat/completions" || true)"
  [[ "$fixture_status" == 200 ]] || failure "local fake-upstream request returned $fixture_status instead of 200"
  grep -Eq '"object"[[:space:]]*:[[:space:]]*"chat.completion"' "$fixture_body" ||
    failure "fake-upstream response was not fixture-shaped"
  rm -f "$fixture_body"
  fixture_body=""
fi

echo "healthz: $health"
echo "readyz: $ready_body"
echo "models: JSON object list"
echo "auth: unauthenticated /ns/platform/v1/models -> 401"
echo "errors: unprefixed model -> 400 model_unprefixed"
echo "store: temp SQLite file $sqlite_path"
if [[ "$check_serving" == 1 ]]; then
  echo "serving: local fixture upstream -> 200 chat.completion on /ns/platform/v1"
else
  echo "serving: DEGRADED, fixture upstream path not checked"
fi
if [[ "$degraded" == 1 || "$check_listeners" != 1 || "$check_serving" != 1 ]]; then
  echo "SQLite boot and serve passed (DEGRADED: a runner prerequisite was unavailable, so the assertions marked DEGRADED above were not proven)"
else
  echo "SQLite boot and serve passed"
fi
