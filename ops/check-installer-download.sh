#!/usr/bin/env bash
# A failed download must say which kind of failure it was. The installer's
# older-release explanation ("this version may predate prebuilt <target>
# archives") is only true for a 404: on a proxy, DNS, TLS, or timeout failure it
# would send someone to change AXOND_VERSION or build from source over an outage
# they should simply retry. This exercises all three answers against a local
# server, for the archive and for its checksum sidecar.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work="$(mktemp -d "${TMPDIR:-/tmp}/axond-installer-check.XXXXXX")"
server_pid=""
cleanup() {
  [[ -z "$server_pid" ]] || kill "$server_pid" 2>/dev/null || true
  rm -rf "$work"
}
trap cleanup EXIT

command -v python3 >/dev/null 2>&1 || {
  echo "installer download check needs python3" >&2
  exit 1
}

cat >"$work/server.py" <<'PY'
import http.server
import socket
import sys


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        # The path segment picks the answer, so one server covers every case.
        if "/nosidecar/" in self.path:
            # The archive is served and only its checksum is absent.
            status = 404 if self.path.endswith(".sha256") else 200
        elif "/missing/" in self.path:
            status = 404
        elif "/gone/" in self.path:
            status = 410
        elif "/boom/" in self.path:
            status = 500
        else:
            status = 200
        body = b"not a real archive\n"
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
with open(sys.argv[1], "w", encoding="utf-8") as handle:
    handle.write(str(server.server_address[1]))
server.serve_forever()
PY

python3 "$work/server.py" "$work/port" &
server_pid=$!
for _ in $(seq 1 50); do
  [[ -s "$work/port" ]] && break
  sleep 0.1
done
[[ -s "$work/port" ]] || {
  echo "installer download check: the local server did not report a port" >&2
  exit 1
}
port="$(cat "$work/port")"

# The installer only speaks https, which a local server cannot offer without a
# certificate the installer would rightly reject. Allowing http in the copy under
# test keeps the status handling — the thing being tested — exactly as shipped.
installer_under_test() {
  local base_url="$1"
  local script="$work/install.sh"
  sed -e "s#^base_url=.*#base_url=\"$base_url\"#" \
    -e "s#--proto '=https'#--proto '=http,https'#" \
    "$root/install.sh" >"$script"
  # A rename in install.sh must not turn this check into a no-op.
  grep -qF "base_url=\"$base_url\"" "$script" ||
    {
      echo "installer download check: base_url override did not apply" >&2
      exit 1
    }
  grep -qF -e "--proto '=http,https'" "$script" ||
    {
      echo "installer download check: proto override did not apply" >&2
      exit 1
    }
  printf '%s\n' "$script"
}

expect_message() {
  local label="$1"
  local base_url="$2"
  local expected="$3"
  local forbidden="${4-}"
  local script output
  script="$(installer_under_test "$base_url")"
  output="$(AXOND_VERSION=0.0.0 AXOND_TARGET=aarch64-unknown-linux-musl \
    AXOND_INSTALL_DIR="$work/bin" sh "$script" 2>&1 || true)"
  if ! printf '%s' "$output" | grep -qF "$expected"; then
    printf 'installer download check failed (%s): expected %s\ngot: %s\n' \
      "$label" "$expected" "$output" >&2
    exit 1
  fi
  if [[ -n "$forbidden" ]] && printf '%s' "$output" | grep -qF "$forbidden"; then
    printf 'installer download check failed (%s): must not say %s\ngot: %s\n' \
      "$label" "$forbidden" "$output" >&2
    exit 1
  fi
  echo "installer download check: $label"
}

predates="may predate prebuilt aarch64-unknown-linux-musl archives"
transfer="This is a transfer problem"

expect_message "404 names the release and target, not a transfer error" \
  "http://127.0.0.1:$port/missing" "$predates" "$transfer"
expect_message "410 is treated as a missing asset too" \
  "http://127.0.0.1:$port/gone" "$predates" "$transfer"
expect_message "a 500 reports the status instead of blaming the release" \
  "http://127.0.0.1:$port/boom" "failed with HTTP 500" "$predates"
expect_message "an unresolvable host reports a retryable transfer failure" \
  "https://axond-installer-check.invalid/releases" "$transfer" "$predates"
# The checksum sidecar is fetched by the same helper, so a served archive with a
# missing sidecar must still be reported as a missing sidecar.
expect_message "a missing checksum sidecar names the sidecar" \
  "http://127.0.0.1:$port/nosidecar" "does not contain" "$predates"

echo "installer download checks passed"
