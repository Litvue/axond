# Docker Compose

The repository ships three composable files:

| File | Purpose |
| --- | --- |
| `docker-compose.yml` | Pull-first Tier 0 quickstart using the public release image. |
| `docker-compose.build.yml` | Development/CI overlay that builds the checked-out tree. |
| `docker-compose.stateful.yml` | Redis/Postgres dependency and health-gating overlay. |

Configuration lives in `ops/compose/*.toml`; secrets and DSNs live in `.env`.
The service explicitly selects `linux/amd64`, matching the current release
image and allowing Docker Desktop to emulate the source build on Apple Silicon.

## Pull-first Tier 0

```bash
cp ops/compose/env.example .env
docker compose up -d
curl --fail http://127.0.0.1:8080/healthz
curl --fail \
  -H 'Authorization: Bearer quickstart-platform-key' \
  http://127.0.0.1:8080/v1/models
```

The service intentionally has no restart policy. A missing secret or invalid
configuration leaves an `Exited (1)` container whose final log line explains
the boot failure, instead of hiding it in a restart loop.

Override the image or host binding through `.env` or the command environment:

```dotenv
AXOND_IMAGE=ghcr.io/litvue/axond:0.3.12 # x-release-please-version
AXOND_QUICKSTART_HOST_PORT=127.0.0.1:18080
```

Production should use an image digest instead of the version tag.

## Build current source

```bash
docker compose \
  -f docker-compose.yml -f docker-compose.build.yml \
  up -d --build
```

`just quickstart-smoke` uses this overlay so CI always tests the checked-out
code rather than the last public release.

## Stateful profile

Select the stateful config and add the dependency overlay:

```bash
export AXOND_QUICKSTART_CONFIG=./ops/compose/axond.stateful.toml
docker compose \
  -f docker-compose.yml -f docker-compose.stateful.yml \
  --profile stateful up -d
```

Use the same files and profile on every follow-up command. The stateful example
enables:

- Redis-backed shared budgets;
- Redis-backed in-flight rate limits;
- Postgres durable usage with schema creation at boot.

Exercise the request path:

```bash
curl http://127.0.0.1:8080/v1/chat/completions \
  -H 'Authorization: Bearer quickstart-platform-key' \
  -H 'content-type: application/json' \
  -d '{"model":"gpt-4o","messages":[{"role":"user","content":"hello"}]}'
```

Usage writes are batched. Poll Postgres rather than assuming the row is visible
immediately:

```bash
for attempt in $(seq 1 12); do
  count="$(docker compose \
    -f docker-compose.yml -f docker-compose.stateful.yml \
    --profile stateful exec -T postgres \
    psql -U postgres -d axond -Atc 'select count(*) from axond_usage;')"
  if [ "$count" -ge 1 ]; then
    printf '%s\n' "$count"
    break
  fi
  sleep 1
done
```

To prove Redis participates in admission, stop it and make a dispatch request:

```bash
docker compose \
  -f docker-compose.yml -f docker-compose.stateful.yml \
  --profile stateful stop redis
```

The dispatch path returns `503 rate_limit_unavailable` or
`budget_unavailable` under the default fail-closed policy, while `/v1/models`
still answers. After Redis restarts, allow its connection manager several
seconds to recover before treating a transient `503` as a defect.

## Teardown

Keep `.env` until after teardown; Compose performs required-variable
interpolation even for `down`.

```bash
docker compose down -v

docker compose \
  -f docker-compose.yml -f docker-compose.stateful.yml \
  --profile stateful down -v
```

The examples use public placeholder gateway keys. Replace them before binding
outside loopback or exposing the service through another network.
