---
name: testing-axond
description: How to run and black-box test the axond HTTP gateway locally (boot from a TOML config, hit the unauthenticated liveness probes and the authenticated catalogue/typed-error routes, exercise transport failures) without any provider credentials.
---

# Testing axond end to end

axond is a headless HTTP AI gateway. There is **no frontend** — test it with `curl`,
not a browser, and do not record a screen session for shell-only runs.

## Build and run

```bash
cargo build -p axond --locked          # toolchain is pinned in rust-toolchain.toml
AXOND_CONFIG=/path/to/axond.toml \
GW_PLATFORM_OPENAI_API_KEY=dummy-openai \
GW_INBOUND_PLATFORM_KEY=dummy-inbound \
  target/debug/axond
```

- `AXOND_CONFIG` defaults to `./axond.toml`. `AXOND_<SECTION>__<KEY>` overrides a config
  scalar (e.g. `AXOND_SERVER__BIND=127.0.0.1:18081`) — handy for running several instances
  from one config file.
- **No provider credentials are needed** for most testing: every referenced env var only
  has to be set and non-empty at boot, and nothing is dispatched unless you send a
  chat/embeddings/messages request. Placeholder values are enough.
- Boot fails closed. Every `[[credential]] env`, `[[gateway_key]] env`, and `dsn_env` the
  config names must be set and non-empty, at least one `[[gateway_key]]` must exist, and
  two gateway keys may not hold the same value. All of this happens **before** the socket
  is bound, so a boot-failure test can assert connection-refused on the port.
- Start from `axond.example.toml`; `ops/docker-smoke.sh` lists every env var that file
  needs.

## Useful probes

```bash
curl -s http://127.0.0.1:8080/healthz     # -> ok      (unauthenticated)
curl -s http://127.0.0.1:8080/readyz      # -> ready   (unauthenticated)
# /v1/models is authenticated and namespace-scoped: pass a gateway key, and it
# lists only aliases whose targets that key's namespace holds a credential for.
curl -s -H "Authorization: Bearer <gateway key value>" \
  http://127.0.0.1:8080/v1/models         # -> {"object":"list","data":[{"id":"<alias>",...}]}
curl -s http://127.0.0.1:8080/v1/models   # -> 401 unauthorized (no key)
```

Every route except the `/healthz` and `/readyz` liveness probes needs
`Authorization: Bearer <gateway key value>` or `x-api-key: <value>` (the value of
the env var named by `[[gateway_key]] env`, not the env var name).

Typed errors are `{"error":{"type":...,"message":...}}`; useful ones you can trigger with
no upstream: `401 unauthorized`, `404 unknown_model`, `501 not_implemented`
(`POST /v1/responses`), `400 unsupported_wire` (send an OpenAI-kind alias to `/v1/messages`
or an Anthropic-kind alias to `/v1/chat/completions`).

## Exercising the upstream/transport path with no provider

Point a provider's `base_url` at an unreachable address, e.g. `http://127.0.0.1:1/v1`, then
send `POST /v1/chat/completions`. You get `502` / `upstream_transport` and the transport
error message, which is a cheap way to test error rendering, redaction, failover, and
circuit breakers. Note the upstream URL is built by **string concatenation** of `base_url`
+ route path, so a `base_url` with a query string or trailing junk will produce a mangled
URL — keep test `base_url`s path-only unless that is what you are testing.

## Before/after comparisons

For behaviour-change PRs, build the base branch in a throwaway worktree and run the same
scenario against both binaries — this is what makes a "secret is not leaked" style
assertion non-vacuous:

```bash
git worktree add /tmp/axond-main origin/main
(cd /tmp/axond-main && cargo build -p axond --locked)
# ... run both binaries against the same config/port, diff the outputs ...
git worktree remove /tmp/axond-main --force
```

## Gotchas

- Do **not** run `pkill -f '...axond...'` from the exec tool: the pattern matches the
  tool's own shell command line and kills your shell. Use `pkill -x axond`, or put the
  kill in a script file and run `bash script.sh`.
- Backgrounding a server and curling it works fine from the exec tool; give it ~3-4s to
  bind. Logs are JSON on stdout; `RUST_LOG=debug` adds `reqwest`/`hyper` lines, which is
  what you want when asserting that something is *absent* from logs.
- `just docker-smoke` builds the distroless image and probes `/healthz`; it needs docker
  and takes a few minutes on a cold cache — run it backgrounded with a long timeout.
- `just check` runs the full CI gate set (fmt, clippy, test, rustdoc, cargo-deny).

## Devin Secrets Needed

None. All of the above runs offline with placeholder values.
