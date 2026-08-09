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

## Minted tokens and file-backed key material

`[[gateway_verifier]]` and `[[gateway_key]]` take **exactly one** of `env = "NAME"` or
`file = "/path"`. File material is re-read whenever a reload candidate is built, so
`SIGHUP` rotates it without a restart.

```bash
# Ed25519 signer: writes the private PKCS#8 base64 (0600, trailing newline) to the
# path and prints the public key as an `export NAME='...'` line plus a TOML snippet.
axond keygen --private-key /tmp/kt/sign.key --kid my-kid \
  --env GW_VERIFY --namespace platform --max-ttl 15m
# Verifier from a file: write just the public base64 into the configured path.
printf %s "$PUB" > /tmp/kt/verifier.pub

# Mint reads the *signing* material from an env var name; --config lets it infer
# alg/audience/max_ttl from a matching verifier entry.
export SIGN_KEY="$(cat /tmp/kt/sign.key)"
axond mint --kid my-kid --key-env SIGN_KEY --namespace platform \
  --subject tester --ttl 15m --config /tmp/kt/axond.toml   # prints axt1.<jws>
```

A verifier requires `[gateway_token] audience`, and at least one static
`[[gateway_key]]` is always mandatory (breakglass). Send the minted token as
`Authorization: Bearer axt1....`.

Rotation / negative testing:

- Replace material under the same `kid` (`printf %s … > f.next && mv f.next f`) then
  `kill -HUP $PID`. The applied `"config reloaded"` JSON line carries
  `gateway_verifiers: "+[] -[] ~[<kid>]"` plus `gateway_verifier_fingerprints` /
  `gateway_key_fingerprints` (16 hex chars, SHA-256 prefix, never the material) — diff
  the fingerprint across reloads to prove the re-read happened. Tokens signed by the
  retired key then return `401 token_invalid_signature`.
- A bad candidate (empty, deleted, non-UTF-8, corrupt base64, HS256 < 32 bytes) logs
  `"config reload rejected; the running config keeps serving"` at ERROR with the path,
  does not emit an applied line, and leaves the previous snapshot serving 200.
- Whitespace: Ed25519 base64 is `trim()`ed (trailing newline fine); HS256 secrets and
  static gateway-key files are **exact bytes**. Note a static key whose file ends in a
  newline is effectively unusable over HTTP, because header values cannot carry a
  trailing newline (curl strips it) — expect 401 and use `printf %s`.
- Booting several configs on different ports: `AXOND_SERVER__BIND=127.0.0.1:180xx`.

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
