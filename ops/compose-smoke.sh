#!/usr/bin/env bash
# Boot the documented Docker Compose quickstart and prove its request paths.
#
# The default config is Tier 0 and does not need Redis or Postgres. The
# placeholder provider key is intentionally not a real credential, so the
# final request proves the typed upstream error path rather than a completion.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
[[ -f .env ]] || { echo "missing .env; run: cp ops/compose/env.example .env" >&2; exit 1; }
set -a
# shellcheck disable=SC1091
source .env
set +a

cleanup() {
  docker compose down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker compose up -d --build
for attempt in $(seq 1 60); do
  if curl --fail --silent http://127.0.0.1:8080/healthz >/tmp/axond-compose-healthz; then
    break
  fi
  sleep 1
done

printf 'healthz: '
curl --fail --silent http://127.0.0.1:8080/healthz
echo
printf 'readyz: '
curl --fail --silent http://127.0.0.1:8080/readyz
echo
printf 'platform models: '
curl --fail --silent \
  -H "Authorization: Bearer ${GW_INBOUND_PLATFORM_KEY}" \
  http://127.0.0.1:8080/v1/models
echo
printf 'acme models: '
curl --fail --silent \
  -H "Authorization: Bearer ${GW_INBOUND_ACME_KEY}" \
  http://127.0.0.1:8080/v1/models
echo
printf 'unauthenticated models: '
unauth_status="$(curl --silent --show-error --output /tmp/axond-compose-unauth \
  --write-out '%{http_code}' http://127.0.0.1:8080/v1/models)"
[[ "$unauth_status" == 401 ]]
printf '%s ' "$unauth_status"
cat /tmp/axond-compose-unauth
echo
printf 'placeholder chat/completions: '
chat_status="$(curl --silent --show-error --output /tmp/axond-compose-chat \
  --write-out '%{http_code}' \
  -H "Authorization: Bearer ${GW_INBOUND_PLATFORM_KEY}" \
  -H 'content-type: application/json' \
  -d '{"model":"gpt-4o","messages":[{"role":"user","content":"hello"}]}' \
  http://127.0.0.1:8080/v1/chat/completions)"
[[ "$chat_status" == 502 ]]
grep -q '"type":"invalid_request"' /tmp/axond-compose-chat
printf '%s ' "$chat_status"
cat /tmp/axond-compose-chat
echo
