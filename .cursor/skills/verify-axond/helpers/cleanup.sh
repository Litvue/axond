#!/usr/bin/env bash
# Tear down the instance this run launched. Never kills by process name.
# Evidence under target/verify-axond/evidence/<run-id>/ is kept.

set -euo pipefail

HELPERS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${HELPERS}/lib.sh"

RUN_ID="$(verify_axond_run_id "${1:-}")"
verify_axond_require_run "$RUN_ID"
verify_axond_load_run "$RUN_ID"

stop_pid() {
  local pid="$1"
  local label="$2"
  if [[ -z "$pid" ]]; then
    return 0
  fi
  if ! verify_axond_pid_alive "$pid"; then
    echo "verify-axond cleanup: ${label} pid ${pid} already exited"
    return 0
  fi
  kill -TERM "$pid" 2>/dev/null || true
  local i=0
  while verify_axond_pid_alive "$pid" && (( i < 20 )); do
    sleep 0.1
    i=$((i + 1))
  done
  if verify_axond_pid_alive "$pid"; then
    echo "verify-axond cleanup: ${label} pid ${pid} still alive after SIGTERM; sending SIGKILL" >&2
    kill -KILL "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
  echo "verify-axond cleanup: stopped ${label} pid ${pid}"
}

stop_pid "${AXOND_VERIFY_AXOND_PID}" "axond"
stop_pid "${AXOND_VERIFY_UPSTREAM_PID}" "upstream"

mkdir -p "$AXOND_VERIFY_EVIDENCE_DIR"
{
  echo "cleaned_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "run_id=${AXOND_VERIFY_RUN}"
  echo "axond_pid=${AXOND_VERIFY_AXOND_PID}"
  echo "upstream_pid=${AXOND_VERIFY_UPSTREAM_PID}"
} >"${AXOND_VERIFY_EVIDENCE_DIR}/cleanup.txt"
cp -a "${AXOND_VERIFY_RUN_DIR}/axond.log" "${AXOND_VERIFY_EVIDENCE_DIR}/axond.log" 2>/dev/null || true
cp -a "${AXOND_VERIFY_RUN_DIR}/upstream.jsonl" "${AXOND_VERIFY_EVIDENCE_DIR}/upstream.jsonl" 2>/dev/null || true
cp -a "${AXOND_VERIFY_RUN_DIR}/run.json" "${AXOND_VERIFY_EVIDENCE_DIR}/run.json" 2>/dev/null || true

rm -rf "$AXOND_VERIFY_RUN_DIR"
echo "verify-axond cleanup: removed ${AXOND_VERIFY_RUN_DIR}"
echo "verify-axond cleanup: kept evidence at ${AXOND_VERIFY_EVIDENCE_DIR}"
