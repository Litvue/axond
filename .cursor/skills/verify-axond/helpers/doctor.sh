#!/usr/bin/env bash
# Read-only check: is this launched instance worth driving?
# Usage: helpers/doctor.sh [run-id]

set -euo pipefail

HELPERS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${HELPERS}/lib.sh"

RUN_ID="$(verify_axond_run_id "${1:-}")"
verify_axond_load_run "$RUN_ID"

fail() {
  echo "verify-axond doctor: $*" >&2
  exit 1
}

if [[ "${AXOND_VERIFY_BASE_URL}" != http://127.0.0.1:* ]]; then
  fail "base URL is not loopback: ${AXOND_VERIFY_BASE_URL}"
fi

verify_axond_pid_alive "$AXOND_VERIFY_AXOND_PID" || fail "axond pid ${AXOND_VERIFY_AXOND_PID} is not running"
verify_axond_pid_alive "$AXOND_VERIFY_UPSTREAM_PID" || fail "upstream pid ${AXOND_VERIFY_UPSTREAM_PID} is not running"

if [[ -r "/proc/${AXOND_VERIFY_AXOND_PID}/exe" ]]; then
  exe="$(readlink -f "/proc/${AXOND_VERIFY_AXOND_PID}/exe" || true)"
  expected="$(readlink -f "$AXOND_VERIFY_BINARY")"
  # Rust binaries may show "(deleted)" after a rebuild; still require the path prefix.
  if [[ "$exe" != "$expected" && "$exe" != "${expected} (deleted)" ]]; then
    fail "pid ${AXOND_VERIFY_AXOND_PID} exe is ${exe}, expected ${expected}"
  fi
fi

verify_axond_pid_owns_tcp "$AXOND_VERIFY_AXOND_PID" "$AXOND_VERIFY_GATEWAY_PORT" \
  || fail "pid ${AXOND_VERIFY_AXOND_PID} does not own 127.0.0.1:${AXOND_VERIFY_GATEWAY_PORT}"

cmdline="$(tr '\0' ' ' <"/proc/${AXOND_VERIFY_AXOND_PID}/cmdline" || true)"
if [[ "$cmdline" == *pkill* || "$cmdline" == *curl* ]]; then
  fail "pid ${AXOND_VERIFY_AXOND_PID} cmdline is not axond: ${cmdline}"
fi

if [[ -r "/proc/${AXOND_VERIFY_AXOND_PID}/environ" ]]; then
  env_config="$(tr '\0' '\n' <"/proc/${AXOND_VERIFY_AXOND_PID}/environ" | awk -F= '/^AXOND_CONFIG=/{print substr($0,13); exit}')"
  if [[ -n "$env_config" && "$env_config" != "$AXOND_VERIFY_CONFIG" ]]; then
    fail "pid ${AXOND_VERIFY_AXOND_PID} AXOND_CONFIG=${env_config} != ${AXOND_VERIFY_CONFIG}"
  fi
fi

health="$(curl -sS --max-time 2 "${AXOND_VERIFY_BASE_URL}/healthz" || true)"
[[ "$health" == "ok" ]] || fail "/healthz answered ${health@Q}, expected ok"

ready="$(curl -sS --max-time 2 "${AXOND_VERIFY_BASE_URL}/readyz" || true)"
[[ "$ready" == "ready" ]] || fail "/readyz answered ${ready@Q}, expected ready"

observed="$("$AXOND_VERIFY_BINARY" --version)"
[[ "$observed" == "$AXOND_VERIFY_VERSION" ]] || fail "binary version ${observed@Q} != launched ${AXOND_VERIFY_VERSION@Q}"

echo "verify-axond doctor: ok"
echo "  run        ${AXOND_VERIFY_RUN}"
echo "  version    ${AXOND_VERIFY_VERSION}"
echo "  bind       ${AXOND_VERIFY_BIND} pid=${AXOND_VERIFY_AXOND_PID}"
echo "  upstream   127.0.0.1:${AXOND_VERIFY_UPSTREAM_PORT} pid=${AXOND_VERIFY_UPSTREAM_PID}"
echo "  sqlite     ${AXOND_VERIFY_SQLITE}"
echo "  evidence   ${AXOND_VERIFY_EVIDENCE_DIR}"
echo "  healthz    ok"
echo "  readyz     ready"
