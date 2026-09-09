#!/usr/bin/env bash
# Shared paths and guards for verify-axond helpers. Source this; do not run it.

set -euo pipefail

_verify_axond_this="${BASH_SOURCE[0]:-$0}"
VERIFY_AXOND_HELPERS="$(cd "$(dirname "$_verify_axond_this")" && pwd)"
VERIFY_AXOND_SKILL="$(cd "$VERIFY_AXOND_HELPERS/.." && pwd)"
VERIFY_AXOND_REPO="$(cd "$VERIFY_AXOND_SKILL/../../.." && pwd)"
VERIFY_AXOND_ROOT="${VERIFY_AXOND_REPO}/target/verify-axond"
VERIFY_AXOND_BINARY="${VERIFY_AXOND_REPO}/target/debug/axond"

verify_axond_run_id() {
  if [[ -n "${AXOND_VERIFY_RUN:-}" ]]; then
    printf '%s\n' "$AXOND_VERIFY_RUN"
    return 0
  fi
  if [[ $# -ge 1 && -n "${1:-}" ]]; then
    printf '%s\n' "$1"
    return 0
  fi
  echo "verify-axond: set AXOND_VERIFY_RUN to a launch id (or pass it as \$1)." >&2
  echo "Do not drive a shared axond on :8080. Launch an isolated instance first." >&2
  return 1
}

verify_axond_run_dir() {
  local id="$1"
  printf '%s\n' "${VERIFY_AXOND_ROOT}/runs/${id}"
}

verify_axond_evidence_dir() {
  local id="$1"
  printf '%s\n' "${VERIFY_AXOND_ROOT}/evidence/${id}"
}

verify_axond_require_run() {
  local id="$1"
  local dir
  dir="$(verify_axond_run_dir "$id")"
  if [[ ! -f "${dir}/run.env" || ! -f "${dir}/run.json" ]]; then
    echo "verify-axond: run '${id}' is not a launched instance (missing ${dir}/run.env)." >&2
    echo "Refuse to drive any other process. Run helpers/launch.sh first." >&2
    return 1
  fi
}

verify_axond_load_run() {
  local id="$1"
  verify_axond_require_run "$id"
  # shellcheck disable=SC1091
  set -a
  # shellcheck disable=SC1090
  source "$(verify_axond_run_dir "$id")/run.env"
  set +a
}

verify_axond_claim_port() {
  python3 - <<'PY'
import socket
sock = socket.socket()
sock.bind(("127.0.0.1", 0))
print(sock.getsockname()[1])
sock.close()
PY
}

verify_axond_pid_alive() {
  local pid="$1"
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

verify_axond_pid_owns_tcp() {
  local pid="$1"
  local port="$2"
  python3 - "$pid" "$port" <<'PY'
import os, struct, socket, sys
from pathlib import Path

pid, port = sys.argv[1], int(sys.argv[2])


def parse_local(cell: str) -> tuple[str, int]:
    ip_hex, port_hex = cell.split(":")
    ip = socket.inet_ntoa(struct.pack("<I", int(ip_hex, 16)))
    return ip, int(port_hex, 16)


inodes = set()
for table in (Path("/proc/net/tcp"), Path("/proc/net/tcp6")):
    if not table.is_file():
        continue
    for line in table.read_text().splitlines()[1:]:
        cols = line.split()
        if len(cols) < 10 or cols[3] != "0A":
            continue
        ip, listen_port = parse_local(cols[1])
        if listen_port == port and ip in {"127.0.0.1", "0.0.0.0"}:
            inodes.add(cols[9])
if not inodes:
    sys.exit(1)
fd_dir = Path(f"/proc/{pid}/fd")
try:
    for fd in fd_dir.iterdir():
        try:
            target = os.readlink(fd)
        except OSError:
            continue
        for inode in inodes:
            if target == f"socket:[{inode}]":
                sys.exit(0)
except FileNotFoundError:
    sys.exit(1)
sys.exit(1)
PY
}
