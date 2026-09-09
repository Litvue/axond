#!/usr/bin/env bash
# Drive the launched instance over HTTP and capture request + response.
# Usage:
#   helpers/drive.sh --name STEP [--auth|--no-auth] [--method GET|POST|PUT|DELETE]
#                    [--json BODY] [--header 'K: V'] PATH
# PATH is appended to AXOND_VERIFY_BASE_URL. Evidence survives cleanup.

set -euo pipefail

HELPERS="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC1091
source "${HELPERS}/lib.sh"

NAME=""
AUTH=1
METHOD=""
JSON=""
HEADERS=()
POSITIONAL=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --name) NAME="${2:-}"; shift 2 ;;
    --auth) AUTH=1; shift ;;
    --no-auth) AUTH=0; shift ;;
    --method) METHOD="${2:-}"; shift 2 ;;
    --json) JSON="${2:-}"; shift 2 ;;
    --header) HEADERS+=("$2"); shift 2 ;;
    --) shift; POSITIONAL+=("$@"); break ;;
    -*) echo "verify-axond drive: unknown flag $1" >&2; exit 2 ;;
    *) POSITIONAL+=("$1"); shift ;;
  esac
done

if [[ -z "$NAME" ]]; then
  echo "verify-axond drive: --name STEP is required" >&2
  exit 2
fi
if [[ ${#POSITIONAL[@]} -ne 1 ]]; then
  echo "verify-axond drive: exactly one PATH is required" >&2
  exit 2
fi
PATH_SUFFIX="${POSITIONAL[0]}"

if [[ -z "$METHOD" ]]; then
  if [[ -n "$JSON" ]]; then
    METHOD="POST"
  else
    METHOD="GET"
  fi
fi

RUN_ID="$(verify_axond_run_id)"
verify_axond_load_run "$RUN_ID"

if [[ "$PATH_SUFFIX" != /* ]]; then
  echo "verify-axond drive: PATH must start with /" >&2
  exit 2
fi

URL="${AXOND_VERIFY_BASE_URL}${PATH_SUFFIX}"
STEP_DIR="${AXOND_VERIFY_EVIDENCE_DIR}/drive/${NAME}"
mkdir -p "$STEP_DIR"

CURL_ARGS=(-sS -D "${STEP_DIR}/response.headers" -o "${STEP_DIR}/response.body" -w '%{http_code}' --max-time 30 -X "$METHOD")
if [[ "$AUTH" -eq 1 ]]; then
  CURL_ARGS+=(-H "Authorization: Bearer ${AXOND_VERIFY_GATEWAY_KEY}")
fi
if [[ -n "$JSON" ]]; then
  CURL_ARGS+=(-H "content-type: application/json" --data "$JSON")
fi
for header in "${HEADERS[@]+"${HEADERS[@]}"}"; do
  CURL_ARGS+=(-H "$header")
done

STATUS="$(curl "${CURL_ARGS[@]}" "$URL" || true)"
printf '%s\n' "$STATUS" >"${STEP_DIR}/status"

{
  echo "method=${METHOD}"
  echo "url=${URL}"
  echo "auth=$([[ "$AUTH" -eq 1 ]] && echo bearer || echo none)"
  if [[ -n "$JSON" ]]; then
    echo "json=${JSON}"
  fi
} >"${STEP_DIR}/request.txt"

python3 - "$STEP_DIR" "$NAME" "$METHOD" "$URL" "$AUTH" "$STATUS" <<'PY'
import json, pathlib, sys, time
step_dir, name, method, url, auth, status = sys.argv[1:]
record = {
    "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "name": name,
    "method": method,
    "url": url,
    "auth": "bearer" if auth == "1" else "none",
    "status": int(status) if status.isdigit() else status,
}
path = pathlib.Path(step_dir).parent.parent / "index.jsonl"
with path.open("a", encoding="utf-8") as handle:
    handle.write(json.dumps(record) + "\n")
print(f"{name}: {method} {url} -> {status}")
print(f"  evidence: {step_dir}")
PY
