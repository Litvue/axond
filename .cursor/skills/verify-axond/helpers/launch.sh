#!/usr/bin/env bash
# Launch an isolated axond + fixture upstream for verification.
# Usage: helpers/launch.sh [run-id]
# Prints AXOND_VERIFY_RUN and writes run.env. Does not publish a budget.

set -euo pipefail

HELPERS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${HELPERS}/lib.sh"

cd "$VERIFY_AXOND_REPO"

RUN_ID="${1:-${AXOND_VERIFY_RUN:-}}"
if [[ -z "$RUN_ID" ]]; then
  RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
fi
if [[ ! "$RUN_ID" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "verify-axond: run id must be [A-Za-z0-9._-]+" >&2
  exit 1
fi

RUN_DIR="$(verify_axond_run_dir "$RUN_ID")"
EVIDENCE_DIR="$(verify_axond_evidence_dir "$RUN_ID")"
if [[ -e "${RUN_DIR}/axond.pid" ]] && verify_axond_pid_alive "$(cat "${RUN_DIR}/axond.pid" 2>/dev/null || true)"; then
  echo "verify-axond: run '${RUN_ID}' is already serving. Reuse it or cleanup first." >&2
  exit 1
fi

mkdir -p "$RUN_DIR" "$EVIDENCE_DIR"
chmod 700 "$RUN_DIR"

if [[ ! -x "$VERIFY_AXOND_BINARY" ]]; then
  echo "verify-axond: building axond (cargo build -p axond --locked)" >&2
  cargo build -p axond --locked
fi
if [[ ! -x "$VERIFY_AXOND_BINARY" ]]; then
  echo "verify-axond: missing ${VERIFY_AXOND_BINARY}" >&2
  exit 1
fi

GATEWAY_PORT="$(verify_axond_claim_port)"
UPSTREAM_PORT="$(verify_axond_claim_port)"
BIND="127.0.0.1:${GATEWAY_PORT}"
UPSTREAM_URL="http://127.0.0.1:${UPSTREAM_PORT}"
SQLITE="${RUN_DIR}/axond.sqlite"
CONFIG="${RUN_DIR}/axond.toml"
VERSION="$("$VERIFY_AXOND_BINARY" --version)"

cat >"$CONFIG" <<EOF
# Written by verify-axond helpers/launch.sh. Verification scaffolding; not a
# production config. Isolation: unique bind, sqlite path, and placeholder keys.
[server]
bind = "${BIND}"

[storage]
backend = "sqlite"
path = "${SQLITE}"

[shutdown]
drain_grace_ms = 0
deadline_ms = 1000
flush_timeout_ms = 1000

[[namespace]]
id = "platform"
default = true

[[provider]]
id = "fixture-openai"
kind = "openai"
base_url = "${UPSTREAM_URL}"

[[provider]]
id = "fixture-anthropic"
kind = "anthropic"
base_url = "${UPSTREAM_URL}"

[[credential]]
id = "verify-openai"
namespace = "platform"
provider = "fixture-openai"
env = "GW_VERIFY_UPSTREAM_KEY"

[[credential]]
id = "verify-anthropic"
namespace = "platform"
provider = "fixture-anthropic"
env = "GW_VERIFY_UPSTREAM_KEY"

[[gateway_key]]
env = "GW_VERIFY_INBOUND_KEY"
namespace = "platform"

[[price]]
provider = "fixture-openai"
model = "*"
input_microdollars_per_million = 2500000
output_microdollars_per_million = 10000000

[[price]]
provider = "fixture-anthropic"
model = "*"
input_microdollars_per_million = 2500000
output_microdollars_per_million = 10000000
EOF

INBOUND_KEY="VERIFY-INBOUND-KEY"
UPSTREAM_KEY="VERIFY-UPSTREAM-KEY"

python3 "${HELPERS}/fake-upstream.py" --port "$UPSTREAM_PORT" --log "${RUN_DIR}/upstream.jsonl" \
  >"${RUN_DIR}/upstream.log" 2>&1 &
UPSTREAM_PID=$!
echo "$UPSTREAM_PID" >"${RUN_DIR}/upstream.pid"

export AXOND_CONFIG="$CONFIG"
export GW_VERIFY_INBOUND_KEY="$INBOUND_KEY"
export GW_VERIFY_UPSTREAM_KEY="$UPSTREAM_KEY"
unset OTEL_EXPORTER_OTLP_ENDPOINT OTEL_EXPORTER_OTLP_PROTOCOL || true
export RUST_LOG="${RUST_LOG:-warn}"

"$VERIFY_AXOND_BINARY" >"${RUN_DIR}/axond.log" 2>&1 &
AXOND_PID=$!
echo "$AXOND_PID" >"${RUN_DIR}/axond.pid"

cleanup_failed_launch() {
  if verify_axond_pid_alive "$AXOND_PID"; then
    kill -TERM "$AXOND_PID" 2>/dev/null || true
    wait "$AXOND_PID" 2>/dev/null || true
  fi
  if verify_axond_pid_alive "$UPSTREAM_PID"; then
    kill -TERM "$UPSTREAM_PID" 2>/dev/null || true
    wait "$UPSTREAM_PID" 2>/dev/null || true
  fi
}

deadline=$((SECONDS + 60))
while (( SECONDS < deadline )); do
  if ! verify_axond_pid_alive "$AXOND_PID"; then
    echo "verify-axond: axond exited before /healthz. Log:" >&2
    cat "${RUN_DIR}/axond.log" >&2 || true
    cleanup_failed_launch
    exit 1
  fi
  if curl -fsS --max-time 1 "http://${BIND}/healthz" >/dev/null 2>&1; then
    break
  fi
  sleep 0.05
done
if ! curl -fsS --max-time 1 "http://${BIND}/healthz" >/dev/null 2>&1; then
  echo "verify-axond: /healthz did not answer within 60s. Log:" >&2
  cat "${RUN_DIR}/axond.log" >&2 || true
  cleanup_failed_launch
  exit 1
fi

BASE_URL="http://${BIND}"
python3 - "$RUN_DIR" "$RUN_ID" "$BASE_URL" "$VERSION" "$AXOND_PID" "$UPSTREAM_PID" \
  "$GATEWAY_PORT" "$UPSTREAM_PORT" "$CONFIG" "$SQLITE" "$VERIFY_AXOND_BINARY" \
  "$EVIDENCE_DIR" "$INBOUND_KEY" "$UPSTREAM_KEY" "$BIND" "$UPSTREAM_URL" <<'PY'
import json, shlex, sys

(
    run_dir, run_id, base_url, version, axond_pid, upstream_pid,
    gateway_port, upstream_port, config, sqlite, binary, evidence,
    inbound_key, upstream_key, bind, upstream_url,
) = sys.argv[1:]

payload = {
    "run_id": run_id,
    "base_url": base_url,
    "bind": bind,
    "gateway_port": int(gateway_port),
    "upstream_port": int(upstream_port),
    "version": version,
    "axond_pid": int(axond_pid),
    "upstream_pid": int(upstream_pid),
    "config": config,
    "sqlite": sqlite,
    "binary": binary,
    "evidence_dir": evidence,
    "namespace": "platform",
    "chat_model": "fixture-openai/fixture-chat",
    "inbound_key_env": "GW_VERIFY_INBOUND_KEY",
}
with open(f"{run_dir}/run.json", "w", encoding="utf-8") as handle:
    json.dump(payload, handle, indent=2)
    handle.write("\n")

env = {
    "AXOND_VERIFY_RUN": run_id,
    "AXOND_VERIFY_BASE_URL": base_url,
    "AXOND_VERIFY_BIND": bind,
    "AXOND_VERIFY_GATEWAY_PORT": gateway_port,
    "AXOND_VERIFY_UPSTREAM_PORT": upstream_port,
    "AXOND_VERIFY_UPSTREAM_URL": upstream_url,
    "AXOND_VERIFY_GATEWAY_KEY": inbound_key,
    "AXOND_VERIFY_UPSTREAM_KEY": upstream_key,
    "AXOND_VERIFY_NAMESPACE": "platform",
    "AXOND_VERIFY_CHAT_MODEL": "fixture-openai/fixture-chat",
    "AXOND_VERIFY_MESSAGES_MODEL": "fixture-anthropic/fixture-messages",
    "AXOND_VERIFY_BINARY": binary,
    "AXOND_VERIFY_VERSION": version,
    "AXOND_VERIFY_CONFIG": config,
    "AXOND_VERIFY_SQLITE": sqlite,
    "AXOND_VERIFY_RUN_DIR": run_dir,
    "AXOND_VERIFY_EVIDENCE_DIR": evidence,
    "AXOND_VERIFY_AXOND_PID": axond_pid,
    "AXOND_VERIFY_UPSTREAM_PID": upstream_pid,
    "AXOND_CONFIG": config,
    "GW_VERIFY_INBOUND_KEY": inbound_key,
    "GW_VERIFY_UPSTREAM_KEY": upstream_key,
}
with open(f"{run_dir}/run.env", "w", encoding="utf-8") as handle:
    for key, value in env.items():
        handle.write(f"{key}={shlex.quote(str(value))}\n")
PY

echo "AXOND_VERIFY_RUN=${RUN_ID}"
echo "AXOND_VERIFY_BASE_URL=${BASE_URL}"
echo "AXOND_VERIFY_EVIDENCE_DIR=${EVIDENCE_DIR}"
echo "export AXOND_VERIFY_RUN=${RUN_ID}"
echo "source ${HELPERS}/env.sh"
