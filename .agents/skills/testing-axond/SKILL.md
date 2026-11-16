---
name: testing-axond
description: How to run and black-box test the axond HTTP gateway locally (boot from a TOML config, hit the unauthenticated liveness probes and the authenticated catalogue/typed-error routes, exercise transport failures) without any provider credentials.
---

# Testing axond end to end

axond is a headless HTTP AI gateway. There is **no frontend** — test it with `curl`,
not a browser, and do not record a screen session for shell-only runs.

## Build and run

```bash
(cd ts && npm ci)                    # Node 22.14.0
AXOND_CONFIG=/path/to/axond.toml \
GW_PLATFORM_OPENAI_API_KEY=dummy-openai \
GW_INBOUND_PLATFORM_KEY=dummy-inbound \
  ts/bin/axond
```

- `AXOND_CONFIG` defaults to `./axond.toml`. `AXOND_<SECTION>__<KEY>` overrides a config
  scalar (e.g. `AXOND_SERVER__BIND=127.0.0.1:18081`) — handy for running several instances
  from one config file.
- **No provider credentials are needed** for most testing: every referenced env var only
  has to be set and non-empty at boot, and nothing is dispatched unless you send a
  chat/embeddings/messages request. Placeholder values are enough.
- Boot fails closed. Every `[[credential]] env`, `[[gateway_key]] env`, and `dsn_env` the
  config names must be set and non-empty, exactly one deployment-wide `[[gateway_key]]` must exist. All of this happens **before** the socket
  is bound, so a boot-failure test can assert connection-refused on the port.
- Start from `axond.example.toml`; `ops/docker-smoke.sh` lists every env var that file
  needs.

## Useful probes

```bash
curl -s http://127.0.0.1:8080/healthz     # -> ok      (unauthenticated)
curl -s http://127.0.0.1:8080/readyz      # -> ready   (unauthenticated)
# /ns/{ns}/v1/models is authenticated: pass the deployment gateway key.
curl -s -H "Authorization: Bearer <gateway key value>" \
  http://127.0.0.1:8080/ns/platform/v1/models
curl -s http://127.0.0.1:8080/ns/platform/v1/models   # -> 401 unauthorized (no key)
```

Every route except the `/healthz` and `/readyz` liveness probes needs
`Authorization: Bearer <gateway key value>` or `x-api-key: <value>` (the value of
the env var named by `[[gateway_key]] env`, not the env var name).

Typed errors are `{"error":{"type":...,"message":...}}`; useful ones you can trigger with
no upstream: `401 unauthorized`, `404 unknown_namespace`, and `400 model_unprefixed` (omit the provider prefix
from a model). Inference routes live under `/ns/{ns}/v1`, including native
`/responses` and `/messages`; unprefixed `/v1` routes are unmounted.

### Black-box testing `/ns/{ns}/v1/credentials`

Observed behaviour worth reusing (all reproducible offline, no provider keys):

- Authority: one deployment-wide static key authenticates namespace paths.
  `?namespaces=all` requests the operator view; the key is not tenant-scoped.
- `?namespaces=` is the caller's *own* namespace only when omitted entirely. Any other
  value — including `""`, `ALL`, a real other namespace, or `all,platform` — is typed
  `400 bad_request`. Unknown params (`?foo=bar`) are ignored.
- A repeated `namespaces` param is deliberately rejected with a typed `400 bad_request`
  (for example, `?namespaces=all&namespaces=beta`); it never exposes a query deserializer
  error or bypasses the gateway's typed-error envelope.
- A namespace with no credentials and fallback off answers `200 {"data":[]}` — an empty
  list, not an error.
- The list is sorted by `(namespace, provider, credential_id)`, with omitted ids sorting
  as empty values. When diffing repeated bodies, compare the *ordering* separately from
  the `state` values: an interleaved dispatch legitimately flips `probe` → `parked`,
  which makes a naive whole-body `md5sum` diff look like an ordering bug.
- A fallback tenant sees the platform credential's presence and state, but its default
  env-derived `credential_id` is omitted. Set explicit `id`s when a test needs a stable
  non-secret label; explicit ids remain visible.

### Driving `parked` / `probe` on a live process

`is_credential_exhausted` only counts upstream **429**s — a connection-refused `base_url`
never parks a credential. Serve a local fake upstream that returns `429` *only for one
credential's key value* (check the `authorization` / `x-api-key` header) and `200`
otherwise; that parks exactly one entry of a multi-credential pool and proves per-credential
independence. With `[credential_pool] failure_threshold = 2, cooldown_seconds = 5`:

1. Send ~4 `POST /ns/platform/v1/chat/completions`; each returns `200` (the healthy credential serves)
   while the 429 credential accumulates failures — with round-robin, only about half the
   requests touch it, so count the fake upstream's 429 log lines rather than the requests.
2. Status then reports that credential `parked`; a request during the cooldown produces no
   new upstream hit for it (it is skipped, not retried).
3. After the cooldown it reports `probe`, and polling `/ns/platform/v1/credentials` repeatedly leaves it
   `probe` and generates zero upstream traffic (the read is pure). The next real request
   consumes the probe (one new 429 line) and the state re-arms to `parked`.

A fake upstream log line per request (`{"key_tail": ..., "status": ...}`) is the cheapest
evidence for all of this; `key_tail` keeps the secret out of the log.

### Making a "no secret material" assertion non-vacuous

Give every `[[credential]]` / `[[gateway_key]]` env var a distinctive marker value
(`SEKRETAAA-…`, `GWKEY…`), tee every response body to one file, and grep the file for each
marker plus the bare prefixes. Set explicit credential `id`s in the test config when
checking labels: without explicit ids, fallback entries omit the env-derived
`credential_id`, which weakens a naive leak grep. Never use the env-var name as the
secret marker.

## Exercising the upstream/transport path with no provider

Point a provider's `base_url` at an unreachable address, e.g. `http://127.0.0.1:1/v1`, then
send `POST /ns/platform/v1/chat/completions`. You get `502` / `upstream_transport` with the fixed
message `upstream transport failure`: the caller is never told the endpoint the attempt
failed against, so this is a cheap way to test failover, circuit breakers, and that the
answer names no provider host — the reason itself is on the replica's `upstream attempt
failed on the transport` warn (`open stream failed on the transport` mid-stream), which is
where redaction is worth checking. Note the upstream URL is built by **string
concatenation** of `base_url`
+ route path, so a `base_url` with a query string or trailing junk will produce a mangled
URL — keep test `base_url`s path-only unless that is what you are testing.

## File-backed key material

Minted `axt1.` tokens are not inbound identity ([ADR 0063](../../../docs/adr/0063-stateful-only-namespaced-gateway.md)):
`authenticate` returns `401` for that prefix, `POST /v1/tokens` is unmounted, and
the binary has no `keygen`/`mint` subcommands. The only inbound credential is the
one static `[[gateway_key]]`, which takes **exactly one** of `env = "NAME"` or
`file = "/path"`. There is no hot reload: changing key material needs a restart.

- Whitespace: static gateway-key files are **exact bytes**. A key file that ends in a
  newline is effectively unusable over HTTP, because header values cannot carry a
  trailing newline (curl strips it) — expect 401 and use `printf %s`.
- Booting several configs on different ports: `AXOND_SERVER__BIND=127.0.0.1:180xx`.

## Current checks

```bash
(cd ts && npm test)
just check
python3 ops/binary-smoke.py ts/bin/axond
```

The smoke owns a free loopback port and local fixture upstream; it needs no
provider credentials. Use a unique config, port and SQLite path for manual
runs. Stop only the PIDs you launched, never `pkill -f axond`.
Record request, response and durable state before drawing conclusions.

Rust static builds, Tier 0, fuzzing and cross-binary comparison were retired
under ADR 0069. There is no current Rust build prerequisite.

## Devin Secrets Needed

None. All of the above runs offline with placeholder values.

## Testing the Docker Compose quickstart

The repo ships a Compose quickstart (`docker-compose.yml`, `docker-compose.stateful.yml`
overlay, `ops/compose/*.toml`, `ops/compose/env.example`, `ops/compose-smoke.sh`,
`just quickstart-smoke`) documented in `docs/deployment.md#5-minute-quickstart`.

```bash
cp ops/compose/env.example .env      # compose uses `${VAR:?set it in .env}` — no .env, no boot
docker compose up -d --build         # warm image cache: ~45s; cold TS build: minutes
curl http://localhost:8080/healthz   # -> ok
docker compose down -v               # keep .env until after this command
just quickstart-smoke                # tear down first; own project name, needs host 8080 free
```

- Every compose command (including `docker compose down`) needs `.env` to exist, because
  the required-variable interpolation runs first. Deleting `.env` before teardown leaves
  containers running with a confusing "required variable ... is missing a value" error.
- `just quickstart-smoke` publishes on host port 8080 by default
  (`AXOND_QUICKSTART_SMOKE_PORT=18080` overrides). If a quickstart stack is already up,
  tear it down first or use the override.
- The stateful path needs both files plus the config override, and the same flags on every
  follow-up command:

```bash
export AXOND_QUICKSTART_CONFIG=./ops/compose/axond.stateful.toml
docker compose -f docker-compose.yml -f docker-compose.stateful.yml --profile stateful up -d
```

- Postgres usage rows are batched: `select count(*) from axond_usage` immediately after a
  request returns `0` and flips to `1` a few seconds later. Always poll before asserting,
  as shown in the deployment guide.
- With the committed placeholder provider key and network egress, dispatch returns `502`
  `invalid_request` carrying OpenAI's "Incorrect API key provided: placehol**********-key"
  text. That body depends on reaching api.openai.com; air-gapped runs get
  `upstream_transport` instead, so assert "typed error" rather than that exact string.
- Boot/config failures stay visible: the service sets no restart policy, so a bad config
  leaves an `Exited (1)` container and the error is the last line of `docker compose logs`.
