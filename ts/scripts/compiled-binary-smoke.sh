#!/usr/bin/env bash
# Boot a compiled axond binary, load one .ts extension from disk, and refuse apiVersion 2.
set -euo pipefail

bin="${1:?compiled axond binary}"
echo "binary_bytes $(wc -c < "$bin" | tr -d ' ')"

pid=""
cleanup() {
  if [[ -n "${pid}" ]] && kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
}
trap cleanup EXIT

free_port() {
  python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

wait_health() {
  local url="$1"
  local err="$2"
  local attempt
  for attempt in $(seq 1 40); do
    if curl -fsS "$url"; then
      echo
      return 0
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      echo "compiled binary exited before ${url}" >&2
      cat "$err" >&2 || true
      return 1
    fi
    sleep 0.2
  done
  echo "compiled binary did not become healthy: ${url}" >&2
  cat "$err" >&2 || true
  return 1
}

write_config() {
  local dir="$1"
  local port="$2"
  cat > "${dir}/axond.toml" <<EOF
[server]
bind = "127.0.0.1:${port}"

[storage]
backend = "sqlite"
path = "${dir}/axond.sqlite"

[[namespace]]
id = "platform"
default = true

[[provider]]
id = "fake-openai"
kind = "openai"
base_url = "http://127.0.0.1:9"

[[credential]]
namespace = "platform"
provider = "fake-openai"
env = "GW_FAKE_OPENAI_KEY"

[[gateway_key]]
env = "GW_INBOUND_KEY"
namespace = "platform"

[[price]]
provider = "fake-openai"
model = "*"
input_microdollars_per_million = 1
output_microdollars_per_million = 1
EOF
}

stop_child() {
  if [[ -n "${pid}" ]]; then
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    pid=""
  fi
}

dir="$(mktemp -d)"
port="$(free_port)"
write_config "$dir" "$port"
err="${dir}/stderr"
AXOND_EXTENSIONS_DIR= AXOND_CONFIG="${dir}/axond.toml" \
  GW_INBOUND_KEY=test-inbound-key GW_FAKE_OPENAI_KEY=upstream \
  "$bin" >"${dir}/stdout" 2>"$err" &
pid=$!
wait_health "http://127.0.0.1:${port}/healthz" "$err"
echo "healthz_ok"
stop_child
rm -rf "$dir"

dir="$(mktemp -d)"
port="$(free_port)"
cat > "${dir}/probe.ts" <<'EOF'
export default {
  name: "probe",
  apiVersion: 1,
  stage: "pre-auth",
  async middleware() {
    return new Response("loaded");
  },
};
EOF
write_config "$dir" "$port"
err="${dir}/stderr"
AXOND_CONFIG="${dir}/axond.toml" AXOND_EXTENSIONS_DIR="$dir" \
  GW_INBOUND_KEY=test-inbound-key GW_FAKE_OPENAI_KEY=upstream \
  "$bin" >"${dir}/stdout" 2>"$err" &
pid=$!
wait_health "http://127.0.0.1:${port}/healthz" "$err"
body="$(curl -fsS -H "authorization: Bearer test-inbound-key" "http://127.0.0.1:${port}/ns/platform/v1/models")"
if [[ "$body" != "loaded" ]]; then
  echo "extension response was not loaded: ${body}" >&2
  cat "$err" >&2 || true
  exit 1
fi
echo "extension_loaded"
stop_child
rm -rf "$dir"

dir="$(mktemp -d)"
cat > "${dir}/future.ts" <<'EOF'
export default {
  name: "future",
  apiVersion: 2,
  stage: "pre-auth",
  async middleware() {
    return new Response("nope");
  },
};
EOF
write_config "$dir" 9
err="${dir}/stderr"
AXOND_CONFIG="${dir}/axond.toml" AXOND_EXTENSIONS_DIR="$dir" \
  GW_INBOUND_KEY=test-inbound-key GW_FAKE_OPENAI_KEY=upstream \
  "$bin" >"${dir}/stdout" 2>"$err" &
pid=$!
attempt=0
while kill -0 "$pid" 2>/dev/null; do
  attempt=$((attempt + 1))
  if [[ "$attempt" -gt 50 ]]; then
    echo "apiVersion 2 extension did not exit" >&2
    cat "$err" >&2 || true
    exit 1
  fi
  sleep 0.2
done
set +e
wait "$pid"
code=$?
set -e
pid=""
if [[ "$code" -ne 1 ]]; then
  echo "expected exit 1 for apiVersion 2, got ${code}" >&2
  cat "$err" >&2 || true
  exit 1
fi
if ! grep -q "future.ts apiVersion 2 is not supported (want 1)" "$err"; then
  echo "missing apiVersion refusal" >&2
  cat "$err" >&2 || true
  exit 1
fi
echo "api_version_refused"
rm -rf "$dir"

root="$(cd "$(dirname "$0")/.." && pwd)"
bun_bin="${BUN:-bun}"
if ! command -v "$bun_bin" >/dev/null 2>&1; then
  echo "bun is required to bundle an extension that imports a package" >&2
  exit 1
fi

dir="$(mktemp -d)"
cat > "${dir}/dep.ts" <<'EOF'
import { parse } from "smol-toml";
const parsed = parse("n = 7") as { n: number };
export default {
  name: "dep",
  apiVersion: 1,
  stage: "pre-auth",
  async middleware() {
    return new Response(String(parsed.n));
  },
};
EOF
write_config "$dir" 9
err="${dir}/stderr"
AXOND_CONFIG="${dir}/axond.toml" AXOND_EXTENSIONS_DIR="$dir" \
  GW_INBOUND_KEY=test-inbound-key GW_FAKE_OPENAI_KEY=upstream \
  "$bin" >"${dir}/stdout" 2>"$err" &
pid=$!
attempt=0
while kill -0 "$pid" 2>/dev/null; do
  attempt=$((attempt + 1))
  if [[ "$attempt" -gt 50 ]]; then
    echo "unbundled extension import did not exit" >&2
    cat "$err" >&2 || true
    exit 1
  fi
  sleep 0.2
done
set +e
wait "$pid"
code=$?
set -e
pid=""
if [[ "$code" -eq 0 ]]; then
  echo "unbundled extension import was accepted" >&2
  cat "$err" >&2 || true
  exit 1
fi
if ! grep -q "Cannot find package 'smol-toml'" "$err"; then
  echo "missing unresolved package refusal" >&2
  cat "$err" >&2 || true
  exit 1
fi
echo "unbundled_package_refused"
rm -rf "$dir"

dir="$(mktemp -d)"
cat > "${dir}/dep.ts" <<'EOF'
import { parse } from "smol-toml";
const parsed = parse("n = 7") as { n: number };
export default {
  name: "dep",
  apiVersion: 1,
  stage: "pre-auth",
  async middleware() {
    return new Response(String(parsed.n));
  },
};
EOF
ln -s "${root}/node_modules" "${dir}/node_modules"
"$bun_bin" build "${dir}/dep.ts" --outfile "${dir}/dep.js"
rm -f "${dir}/dep.ts" "${dir}/node_modules"
port="$(free_port)"
write_config "$dir" "$port"
err="${dir}/stderr"
AXOND_CONFIG="${dir}/axond.toml" AXOND_EXTENSIONS_DIR="$dir" \
  GW_INBOUND_KEY=test-inbound-key GW_FAKE_OPENAI_KEY=upstream \
  "$bin" >"${dir}/stdout" 2>"$err" &
pid=$!
wait_health "http://127.0.0.1:${port}/healthz" "$err"
body="$(curl -fsS -H "authorization: Bearer test-inbound-key" "http://127.0.0.1:${port}/ns/platform/v1/models")"
if [[ "$body" != "7" ]]; then
  echo "bundled extension response was not 7: ${body}" >&2
  cat "$err" >&2 || true
  exit 1
fi
echo "bundled_package_loaded"
stop_child
rm -rf "$dir"
