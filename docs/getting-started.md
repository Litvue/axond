# Getting started

This walkthrough boots Axond with no datastore, proves its public and
authenticated surfaces, and optionally sends a real provider request. No real
provider credential is needed until the final step.

## Prerequisites

- Docker Engine or another Docker Compose-compatible runtime.
- `curl`.
- A real OpenAI or Anthropic key only if you want a successful upstream call.

## 1. Start the released image

```bash
git clone https://github.com/Litvue/axond.git
cd axond
cp ops/compose/env.example .env
docker compose up -d
```

The base Compose file pulls the current public image and mounts
`ops/compose/axond.quickstart.toml`. It is Tier 0: Redis and Postgres are not
started and Axond writes one JSON usage record per completed request to its
container log.

The values in `.env` are intentionally public placeholders. They are safe only
for local evaluation.

## 2. Prove boot and authentication

```bash
curl --fail http://127.0.0.1:8080/healthz
curl --fail http://127.0.0.1:8080/readyz
curl --fail \
  -H 'Authorization: Bearer quickstart-platform-key' \
  http://127.0.0.1:8080/v1/models
curl --silent --output /dev/null --write-out '%{http_code}\n' \
  http://127.0.0.1:8080/v1/models
```

Expected results are `ok`, `ready`, an alias catalogue, and `401` for the
unauthenticated catalogue request. Only `/healthz` and `/readyz` are public.

## 3. Exercise the dispatch path

```bash
curl http://127.0.0.1:8080/v1/chat/completions \
  -H 'Authorization: Bearer quickstart-platform-key' \
  -H 'content-type: application/json' \
  -d '{"model":"gpt-4o","messages":[{"role":"user","content":"Say hello in one word."}]}'
```

With the committed placeholder key, this returns either a typed provider error
or `upstream_transport` in an air-gapped environment. That is expected: Axond
has authenticated the caller, resolved the alias and credential, attempted the
provider, and rendered the failure through its typed error envelope.

For a successful completion, edit `.env`:

```dotenv
GW_PLATFORM_OPENAI_API_KEY=sk-your-real-key
```

Then recreate the container and repeat the request:

```bash
docker compose up -d --force-recreate
```

## 4. Inspect credential state and logs

```bash
curl --fail \
  -H 'Authorization: Bearer quickstart-platform-key' \
  http://127.0.0.1:8080/v1/credentials
docker compose logs axond
```

Credential status exposes labels and replica-local `healthy`, `parked`, or
`probe` state, never secret values. Logs are structured JSON and include the
canonical usage record.

## 5. Stop the stack

```bash
docker compose down -v
```

Keep `.env` until after teardown; required-variable interpolation happens
before every Compose command.

## Try the stateful stack

The stateful example adds Redis-backed budgets/rate limits and a Postgres usage
sink:

```bash
export AXOND_QUICKSTART_CONFIG=./ops/compose/axond.stateful.toml
docker compose \
  -f docker-compose.yml -f docker-compose.stateful.yml \
  --profile stateful up -d
```

Use those same `-f` and `--profile` flags on every follow-up command. See the
[Compose guide](./deployment/docker-compose.md) for the durable-row probe and
failure tests.

## Run current source instead

The source-build overlay keeps development and CI on the checked-out tree:

```bash
docker compose \
  -f docker-compose.yml -f docker-compose.build.yml \
  up -d --build
```

Or build the Rust binary directly:

```bash
cargo build -p axond --locked
AXOND_CONFIG=ops/compose/axond.quickstart.toml \
GW_PLATFORM_OPENAI_API_KEY=placeholder-openai-key \
GW_PLATFORM_ANTHROPIC_API_KEY=placeholder-anthropic-key \
GW_ACME_OPENAI_API_KEY=placeholder-acme-openai-key \
GW_INBOUND_PLATFORM_KEY=quickstart-platform-key \
GW_INBOUND_ACME_KEY=quickstart-acme-key \
  target/debug/axond
```

Next, choose an [installation path](./installation.md), connect an
[OpenAI](./clients/openai.md) or [Anthropic](./clients/anthropic.md) client, and
review the [production checklist](./deployment/production-checklist.md).
