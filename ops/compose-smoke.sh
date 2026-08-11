#!/usr/bin/env bash
# Boot the documented Docker Compose quickstart and prove its request paths.
#
# The default config is Tier 0 and does not need Redis or Postgres. Health,
# readiness, namespace-scoped catalogues, and unauthenticated rejection are
# hard assertions. The dispatch step forces the committed placeholder key and
# only asserts a successful response or a typed provider/transport error; it
# does not assert a particular provider status or completion body.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
[[ -f .env ]] || { echo "missing .env; run: cp ops/compose/env.example .env" >&2; exit 1; }
set -a
# shellcheck disable=SC1091
source .env
set +a

project_name="axond-quickstart-smoke-$$"
smoke_port="${AXOND_QUICKSTART_SMOKE_PORT:-8080}"
base_url="http://127.0.0.1:${smoke_port}"
probe_dir="$(mktemp -d "${TMPDIR:-/tmp}/axond-compose-smoke.XXXXXX")"
healthz_file="${probe_dir}/healthz"
readyz_file="${probe_dir}/readyz"
platform_models_file="${probe_dir}/platform-models"
acme_models_file="${probe_dir}/acme-models"
unauth_file="${probe_dir}/unauth"
chat_file="${probe_dir}/chat"
# The gateway default is 30,000 ms (crates/gateway/src/config.rs). Keep the
# client deadline five seconds beyond it so typed transport errors can surface.
gateway_overall_timeout_ms=30000
chat_timeout_seconds=$((gateway_overall_timeout_ms / 1000 + 5))
compose=(env "AXOND_QUICKSTART_HOST_PORT=127.0.0.1:${smoke_port}" docker compose --project-name "$project_name")

cleanup() {
  "${compose[@]}" down -v >/dev/null 2>&1 || true
  rm -rf "$probe_dir"
}
trap cleanup EXIT

GW_PLATFORM_OPENAI_API_KEY=placeholder-openai-key \
AXOND_QUICKSTART_CONFIG=./ops/compose/axond.quickstart.toml \
  "${compose[@]}" up -d --build
healthy=false
for attempt in $(seq 1 60); do
  if curl --fail --silent "${base_url}/healthz" >"$healthz_file"; then
    healthy=true
    break
  fi
  sleep 1
done
if [[ "$healthy" != true ]]; then
  echo "axond did not answer /healthz; container logs:" >&2
  "${compose[@]}" logs >&2 || true
  exit 1
fi

printf 'healthz: '
curl --fail --silent "${base_url}/healthz" >"$healthz_file"
cat "$healthz_file"
echo
printf 'readyz: '
curl --fail --silent "${base_url}/readyz" >"$readyz_file"
cat "$readyz_file"
echo
printf 'platform models: '
curl --fail --silent \
  -H "Authorization: Bearer ${GW_INBOUND_PLATFORM_KEY}" \
  "${base_url}/v1/models" >"$platform_models_file"
cat "$platform_models_file"
echo
printf 'acme models: '
curl --fail --silent \
  -H "Authorization: Bearer ${GW_INBOUND_ACME_KEY}" \
  "${base_url}/v1/models" >"$acme_models_file"
cat "$acme_models_file"
echo
printf 'unauthenticated models: '
unauth_status="$(curl --silent --show-error --output "$unauth_file" \
  --write-out '%{http_code}' "${base_url}/v1/models")"
[[ "$unauth_status" == 401 ]]
printf '%s ' "$unauth_status"
cat "$unauth_file"
echo
printf 'placeholder chat/completions: '
: >"$chat_file"
if chat_status="$(curl --silent --show-error --output "$chat_file" \
    --connect-timeout 5 --max-time "$chat_timeout_seconds" \
    --write-out '%{http_code}' \
    -H "Authorization: Bearer ${GW_INBOUND_PLATFORM_KEY}" \
    -H 'content-type: application/json' \
    -d '{"model":"gpt-4o","messages":[{"role":"user","content":"hello"}]}' \
    "${base_url}/v1/chat/completions")"; then
  :
else
  curl_status=$?
  chat_status="curl_exit_${curl_status}"
fi
printf '%s ' "$chat_status"
cat "$chat_file"
echo
if [[ "$chat_status" == 200 ]]; then
  echo "chat/completions: provider success"
elif [[ "$chat_status" =~ ^[45][0-9][0-9]$ ]] \
  && grep -q '"error"' "$chat_file" \
  && grep -q '"type"' "$chat_file"; then
  echo "chat/completions: typed provider/transport error"
else
  echo "unexpected chat/completions result: HTTP $chat_status" >&2
  exit 1
fi
