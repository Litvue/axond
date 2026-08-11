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

### Black-box testing `/v1/credentials`

Observed behaviour worth reusing (all reproducible offline, no provider keys):

- Scope matrix: scope-less principals (static `[[gateway_key]]` **and** a minted token
  with no `scope` claim) get `200` on the own-namespace view and `403
  token_scope_insufficient` naming `credentials:all` on `?namespaces=all`. A scoped token
  needs `credentials` for the route; `credentials:all` alone is denied with `credentials`.
- `?namespaces=` is the caller's *own* namespace only when omitted entirely. Any other
  value — including `""`, `ALL`, a real other namespace, or `all,platform` — is typed
  `400 bad_request`. Unknown params (`?foo=bar`) are ignored.
- A repeated `namespaces` param is deliberately rejected with a typed `400 bad_request`
  (for example, `?namespaces=all&namespaces=beta`); it never exposes a query deserializer
  error or bypasses the gateway's typed-error envelope.
- `credentials:all` is a pure capability check, not an operator-namespace check: a token
  minted for a *tenant* namespace that carries `credentials:all` sees every namespace.
  Operators must mint it only into operator tokens; the verifier's `namespaces = [...]`
  allowlist does not constrain this capability.
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

1. Send ~4 `POST /v1/chat/completions`; each returns `200` (the healthy credential serves)
   while the 429 credential accumulates failures — with round-robin, only about half the
   requests touch it, so count the fake upstream's 429 log lines rather than the requests.
2. Status then reports that credential `parked`; a request during the cooldown produces no
   new upstream hit for it (it is skipped, not retried).
3. After the cooldown it reports `probe`, and polling `/v1/credentials` repeatedly leaves it
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

## Minted inbound identity (`keygen` / `mint` / `[[gateway_verifier]]`)

Minted tokens can be exercised fully offline. Working recipe:

```bash
axond keygen --private-key ./sign.key --kid k1 --env GW_VERIFY_K1 \
  --namespace acme --max-ttl 15m          # stdout = public key export + verifier snippet
# config needs: [gateway_token] audience, [[gateway_verifier]], AND >=1 [[gateway_key]]
export GW_VERIFY_K1='<from keygen stdout>'   # must be in the env BEFORE the gateway starts
export GW_SIGN_K1="$(cat ./sign.key)"
TOKEN=$(axond mint --config ./axond.toml --kid k1 --alg EdDSA --key-env GW_SIGN_K1 \
  --namespace acme --subject agent-1 --ttl 10m)
curl -s -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8080/v1/models
```

- Mint-time enforcement only happens when a config is loaded. Dropping `--config` (and
  `AXOND_CONFIG`) and passing `--alg` plus `--audience` explicitly makes `mint` enforce
  just the 24h ceiling, which is how you produce a token the gateway must reject
  (`token_invalid_lifetime`, `token_signer_not_permitted`) — the cheapest way to test
  verify-side checks without hand-forging a JWS.
- Verify-side rejections are typed: `token_unknown_key` (kid removed/absent),
  `token_invalid_signature` (key material swapped), `token_invalid_lifetime`
  (`exp - iat > max_ttl`), `token_signer_not_permitted` (403, `ns` not in the verifier's
  `namespaces`), `token_wrong_audience`.
- `signer_kid` appears in the JSON usage record on stdout only for minted callers; static
  `[[gateway_key]]` records use the env var *name* as `subject` and omit `signer_kid`. To
  emit a record with no provider, point the provider `base_url` at `http://127.0.0.1:1/v1`
  and POST `/v1/chat/completions` (502) — the usage record is still written.
- **A running process's environment cannot gain a new variable.** SIGHUP re-reads
  `std::env::vars()` of the *same* process, so adding a `[[gateway_verifier]]` whose `env`
  was exported after the gateway started makes the reload fail with
  "references env var `X`, which is unset or empty" and the old config keeps serving.
  Any new-`kid` rotation test must pre-export every verifier env var before boot, or
  restart the process. Same-`kid` key-material swaps likewise need a restart.
- The reload summary line renders the verifier delta as
  `gateway_verifiers="+[new] -[old] ~[changed-definition]"`; key material is never part of
  the diff, so a material-only change reports `gateway_verifiers="unchanged", changed=false`.
- EdDSA base64 is trimmed on both mint and verify sides (a trailing `\n` in either env var
  still works). HS256 secrets are *not* trimmed — a trailing newline on the signing side
  yields `token_invalid_signature`, which is the expected, documented behaviour.

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
- `just tier0` builds the static binary and runs `ops/tier0-gate.sh`. The gate
  re-execs in a network-denied namespace, asserts the post-boot listener set is the
  captured baseline plus only 18081/18082 (so no in-namespace Redis/Postgres), and
  boots `tests/tier0/axond.tier0.toml` against the committed local fixture
  upstream. To use an already-built binary, run
  `AXOND_BIN=target/debug/axond ops/tier0-gate.sh`. Budget ~4-5s for the gate script
  itself; the cold musl release build dominates (~40s on 8 cores).

## Static musl builds (`just build-static`, `just tier0`)

Both recipes need the musl toolchain, which is *not* installed by the blueprint:

```bash
sudo apt-get install -y musl-tools
# Add the target to the PINNED toolchain from rust-toolchain.toml, not the default one:
rustup target add --toolchain "$(grep -oP 'channel = "\K[^"]+' rust-toolchain.toml)-x86_64-unknown-linux-gnu" \
  x86_64-unknown-linux-musl
```

A bare `rustup target add x86_64-unknown-linux-musl` installs it on the *default*
toolchain, so the build still dies with `error[E0463]: can't find crate for 'core' ...
the x86_64-unknown-linux-musl target may not be installed` even though
`rustup target list --installed` looks right — check
`rustup target list --installed --toolchain <pinned>`. CI is unaffected (it installs the
target through `dtolnay/rust-toolchain` with the pinned toolchain); this is local-dev only.

### Testing the namespace plumbing of the Tier-0 gate

The gate prefers `unshare --user --map-root-user --net --fork` and falls back to
`sudo -n unshare --net --fork`. To exercise the branches without editing the script, put
shims first on `PATH` in a script file (not inline in the exec tool):

- sudo fallback: shim `unshare` to `exit 1` when any argument is `--user`, else
  `exec /usr/bin/unshare "$@"`.
- "no namespace at all" loud failure: shim `unshare` to always fail **and** shim `sudo` to
  fail — `sudo` resolves `unshare` via `secure_path`, so a PATH shim alone does not reach it.
- missing-`unshare` branch: run with `PATH` set to a directory holding only symlinks to
  `bash` and `realpath`, so `command -v unshare` fails before anything else is needed.

To force a *gate* (not sandbox) failure for red-path testing, pass a binary that exits
immediately, e.g. `ops/tier0-gate.sh /bin/true` — you should get
`TIER 0 INVARIANT FAILED: gateway exited before /healthz`. For a late-stage failure that
exercises the temp-file cleanup trap, temporarily make `tests/compat/fake_upstream.py`
`_buffered` respond `500` (revert afterwards and confirm `git status` is clean).

## Devin Secrets Needed

None. All of the above runs offline with placeholder values.

## Testing the Docker Compose quickstart

The repo ships a Compose quickstart (`docker-compose.yml`, `docker-compose.stateful.yml`
overlay, `ops/compose/*.toml`, `ops/compose/env.example`, `ops/compose-smoke.sh`,
`just quickstart-smoke`) documented in `docs/deployment.md#5-minute-quickstart`.

```bash
cp ops/compose/env.example .env      # compose uses `${VAR:?set it in .env}` — no .env, no boot
docker compose up -d --build         # warm image cache: ~45s; cold musl build: minutes
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

- Proving Redis is really in admission (not silently ignored): `docker compose ... stop redis`,
  then POST `/v1/chat/completions` → `503` `rate_limit_unavailable` (or `budget_unavailable`).
  `/v1/models` still answers `200`, so only the dispatch path is gated. After
  `start redis`, expect ~10-15s of transient `503`s before requests flow again — do not
  read the first failure after a restart as a bug.
- Redis holds no keys for a request that never spends (cost 0), so `redis-cli KEYS '*'`
  being empty is not evidence Redis is unwired; use the fail-closed probe above instead.
- Postgres usage rows are batched: `select count(*) from axond_usage` immediately after a
  request returns `0` and flips to `1` a few seconds later. Always poll before asserting,
  as shown in the deployment guide.
- With the committed placeholder provider key and network egress, dispatch returns `502`
  `invalid_request` carrying OpenAI's "Incorrect API key provided: placehol**********-key"
  text. That body depends on reaching api.openai.com; air-gapped runs get
  `upstream_transport` instead, so assert "typed error" rather than that exact string.
- Boot/config failures stay visible: the service sets no restart policy, so a bad config
  leaves an `Exited (1)` container and the error is the last line of `docker compose logs`.
