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
