---
name: verify-axond
description: Verify the axond HTTP AI gateway by launching an isolated replica, driving real /healthz /api/v1 and /ns/{ns}/v1 routes with curl, and capturing request/response plus Store and upstream evidence. Use when proving boot, authentication, budgets, namespaces, or inference dispatch without provider credentials.
---

# Verify axond

Axond is a headless HTTP AI gateway. There is no product UI. Drive it with `curl` (via the helpers below), not a browser, and do not record a screen session for these runs.

Secondary surfaces that this skill does **not** drive: the Astro docs site under `website/`, Docker Compose on host `:8080`, and withdrawn `/admin/v1` / unprefixed `/v1/...` routes. Operator CLI (`axond check preflight`, `axond mint`) is not inbound identity.

## Launch

From the repository root:

```bash
.cursor/skills/verify-axond/helpers/launch.sh
# optional: .cursor/skills/verify-axond/helpers/launch.sh <run-id>
```

Ready when stdout prints `AXOND_VERIFY_BASE_URL=http://127.0.0.1:<port>` and `GET /healthz` on that URL returns `ok` (the helper waits up to 60s). Source the run:

```bash
export AXOND_VERIFY_RUN=<id-from-stdout>
source .cursor/skills/verify-axond/helpers/env.sh
.cursor/skills/verify-axond/helpers/doctor.sh
```

What launch actually does:

1. `cargo build -p axond --locked` if `target/debug/axond` is missing (toolchain is `rust-toolchain.toml`).
2. Claims two free loopback ports (gateway + fixture upstream). Never binds `:8080`.
3. Writes a throwaway TOML + SQLite under `target/verify-axond/runs/<run-id>/`.
4. Starts `helpers/fake-upstream.py` (committed wire fixtures from `tests/fixtures/`) then `target/debug/axond`.
5. Does **not** publish a period budget. Inference is fail-closed until a recipe `PUT`s one.

Placeholder env (always these values; they are not production secrets):

| Env | Value | Role |
| --- | --- | --- |
| `GW_VERIFY_INBOUND_KEY` | `VERIFY-INBOUND-KEY` | deployment gateway key |
| `GW_VERIFY_UPSTREAM_KEY` | `VERIFY-UPSTREAM-KEY` | fixture provider key |

Teardown is `helpers/cleanup.sh` (see Cleanup). Do not `pkill -f axond`.

Isolation: two launched runs may share a host. Each has its own bind, sqlite file, PIDs, and `AXOND_VERIFY_RUN`. Never drive `http://127.0.0.1:8080` or a Compose stack from this skill — that is someone else's session. If `doctor.sh` fails, stop; do not fall back to another listener.

## Doctor

```bash
.cursor/skills/verify-axond/helpers/doctor.sh
# or: .cursor/skills/verify-axond/helpers/doctor.sh <run-id>
```

Read-only. Passes only when all of these hold:

- `target/verify-axond/runs/<id>/run.env` exists (this run was launched by `launch.sh`).
- The recorded axond PID is alive and its `/proc/<pid>/exe` is `target/debug/axond`.
- That PID owns `127.0.0.1:<gateway-port>` in listen state.
- `AXOND_CONFIG` in the process environment is this run's TOML.
- `GET /healthz` → body `ok`; `GET /readyz` → body `ready`.
- `axond --version` matches the version recorded at launch.

If anything looks off, run doctor before guessing. A green Compose stack on `:8080` is not this instance.

## Drive

Harness: `helpers/drive.sh` wrapping `curl` against `AXOND_VERIFY_BASE_URL`. Stable handles are **route paths**, HTTP status, and typed JSON `error.type` — not DOM, coordinates, or tab order.

Every authenticated route except `/healthz` and `/readyz` needs `Authorization: Bearer $AXOND_VERIFY_GATEWAY_KEY` (also accepted as `x-api-key`). Inference and catalogue live under `/ns/{namespace}/v1/...`. Unprefixed `/v1/...` is unmounted. `/admin/v1` is unmounted.

```bash
# Probes (no auth)
.cursor/skills/verify-axond/helpers/drive.sh --name healthz --no-auth /healthz
.cursor/skills/verify-axond/helpers/drive.sh --name readyz --no-auth /readyz

# Authenticated catalogue
.cursor/skills/verify-axond/helpers/drive.sh --name models \
  /ns/platform/v1/models

# Management
.cursor/skills/verify-axond/helpers/drive.sh --name put-budget --method PUT \
  --json '{"limit_microdollars":1000000000000}' \
  /api/v1/namespaces/platform/budgets/verify

.cursor/skills/verify-axond/helpers/drive.sh --name chat --method POST \
  --json '{"model":"fixture-openai/fixture-chat","messages":[{"role":"user","content":"hello"}]}' \
  /ns/platform/v1/chat/completions
```

`drive.sh` writes request, status, headers, and body under `target/verify-axond/evidence/<run-id>/drive/<name>/`. Read `status` and `response.body` after each step; do not trust stdout alone.

Feature recipes: [features/README.md](features/README.md). Drive the entry points the map lists. A proof that only hits `/healthz` does not cover budgets or dispatch.

Raw `curl` is allowed if it uses `$AXOND_VERIFY_BASE_URL` and `$AXOND_VERIFY_GATEWAY_KEY` from `run.env`. Still copy the exchange into the evidence directory.

## Evidence

Proof lives in `target/verify-axond/evidence/<run-id>/` and **must survive cleanup**.

Required for a passing proof:

- The real user path: HTTP to the listening replica, not internal setters, not `cargo test`, not test-only routers.
- The action and the resulting state: `drive/<step>/request.txt` plus `status` plus `response.body`.
- Side effects alongside the HTTP body:
  - Store: `python3 .cursor/skills/verify-axond/helpers/store-get.py --sqlite "$AXOND_VERIFY_SQLITE" --namespaces` or `--budget <ns> <period>` (tables `axond_namespace`, `axond_store_budget`).
  - Upstream: `target/verify-axond/runs/<id>/upstream.jsonl` (copied to evidence on cleanup). Each line is `{path, model, status, key_tail}` — never the full key.
  - Logs: `axond.log` JSON lines (usage records after a completed request).
- Secret markers `VERIFY-INBOUND-KEY` and `VERIFY-UPSTREAM-KEY` must be absent from every captured response body.

Do not mock the gateway. The fixture upstream is the production isolation boundary for providers (same fixtures as `tests/compat`). Do not call api.openai.com.

`ops/binary-smoke.py` is a CI cousin, not this harness: it does not leave evidence and it auto-publishes a budget.

## Cleanup

```bash
.cursor/skills/verify-axond/helpers/cleanup.sh
# or: .cursor/skills/verify-axond/helpers/cleanup.sh <run-id>
```

Sends SIGTERM to the recorded axond PID, then the fixture-upstream PID. SIGKILL only if SIGTERM does not exit within ~2s. Removes `target/verify-axond/runs/<id>/`. Copies `axond.log`, `upstream.jsonl`, and `run.json` into the evidence directory first.

Never `pkill -f axond` (the pattern matches the invoking shell). Never kill by binary name. Never delete `target/verify-axond/evidence/`.

After cleanup, confirm:

```bash
test -d "target/verify-axond/evidence/${AXOND_VERIFY_RUN}"
test -f "target/verify-axond/evidence/${AXOND_VERIFY_RUN}/index.jsonl"
```

## Helpers

All scripts are executable. Run from the repo root, or via the paths below.

| Script | Invocation |
| --- | --- |
| Launch | `.cursor/skills/verify-axond/helpers/launch.sh [run-id]` |
| Doctor | `.cursor/skills/verify-axond/helpers/doctor.sh [run-id]` |
| Drive | `.cursor/skills/verify-axond/helpers/drive.sh --name STEP [--no-auth] [--method PUT] [--json '{...}'] /path` |
| Store read | `python3 .cursor/skills/verify-axond/helpers/store-get.py --sqlite "$AXOND_VERIFY_SQLITE" --namespaces` |
| Fixture upstream | started by launch: `python3 .cursor/skills/verify-axond/helpers/fake-upstream.py --port PORT --log PATH` |
| Cleanup | `.cursor/skills/verify-axond/helpers/cleanup.sh [run-id]` |

`helpers/env.sh` only sources `run.env` for the current `AXOND_VERIFY_RUN`.
