# TypeScript conversion regression gates

The conversion is not ready to replace the Rust release solely because its test
count is green. A release needs evidence for the supported contract, an explicit
decision on the differences below, and deployment evidence for its chosen target.
The detailed behavioral specification remains [the parity contract](typescript-parity.md)
and [the operator compatibility contract](../compatibility.md).

## Feature coverage matrix

`Differential` means the same fixture scenario runs through two real processes.
`TypeScript` means a behavior test asserts the contract independently; it does not
prove every combination agrees with Rust. Each test file below is under `ts/`.

| Feature / contract | Differential coverage | Additional TypeScript evidence | Remaining limit |
| --- | --- | --- | --- |
| Liveness and readiness | `/healthz`, `/readyz`, expected 200 | `packages/cli/src/argv.test.ts`, `packages/gateway/src/app-*.test.ts` shutdown tests | Drain behavior is tested independently, not in the differential runner |
| Static authentication and ordering | Missing/wrong key, `axt1.` refusal, auth before namespace lookup, management auth | `unknown_gateway_key_is_rejected_before_namespace_lookup` | Extension authentication is an additional TS capability |
| Namespace isolation and canonical URLs | Unknown and encoded namespace, create/read/replace/delete/repeated delete, config-owned delete refusal | `namespace_create_uses_the_rust_identifier_messages`, scoped-store and extension tests | Concurrent incarnation tests remain TS/store tests |
| Chat and embeddings | Buffered chat, chunked chat SSE, embeddings | `app-*.test.ts`, `streaming.test.ts`, both vendor SDK lanes | Differential runner uses successful provider fixtures |
| Native Responses and Messages | Buffered and streamed routes; streaming bytes must match exactly | `responses-sequence.test.ts`, `native-messages.test.ts`, SDK lanes | Continuation affinity and malformed event sequences have independent TS tests |
| Model and credential discovery | Namespace models, credential views, all-namespaces view, cached provider models | `query.test.ts`, `cli/src/discovery.test.ts` | Live vendor discovery is not exercised |
| Blocklists and routing refusal | Unprefixed model, namespace blocklist, malformed JSON/model, unsupported content type | `behavior-*.test.ts`, `app-*.test.ts` | See catalogue validation difference below |
| Namespace pagination and management validation | Paginated list, duplicate create, unknown budget field | `management_query_matches_the_rust_deserializer`, strict JSON tests | Not every malformed-input case is replayed through Rust |
| Fixed and monthly budgets | Set/read limits, exhausted budget 429, set/read both cadence policies | `namespace.test.ts`, `budget_policy_follows_the_cadence_period` | Timezone boundaries are TS tests, not a clock-controlled two-process replay |
| Usage and spend | Seven expected settlements; compare namespace, period, model, status, nullable cost; usage summary and durable budgets | `ten settlements of one request_id charge once`, SQLite/Postgres stale-incarnation and lock tests | Request IDs and recorded timestamps are deliberately excluded; Rust mints IDs |
| Restart recovery | API-created namespace, usage, limits, spend, active periods, cadence survive graceful restart | SQLite store tests and compiled-binary smoke | Crash/kill recovery and sustained fault qualification still need release evidence |
| Admission and settlement bounds | No new differential stress scenarios | `behavior-*.test.ts` saturation, queue, pending-settlement and settlement-deadline tests | Per-tenant admission differs; see below |
| Provider errors, rotation, timeout, cancellation | Covered by independent suites | `behavior-*.test.ts`, `socket-close.test.ts`, Worker cancellation tests | Differential fault replay remains a gap |
| Config and CLI compatibility | Both processes load the same valid config | `config-*.test.ts`, `argv.test.ts`, `config-file-*.test.ts` | Detailed Figment/TOML cases remain TS tests |
| Telemetry and secret omission | No collector in the differential runner | `otel.test.ts`, request/provider log tests, `usage-delivery.test.ts`, `check:alerts` | Protocol and metric differences below require an operator decision |
| OpenAPI | Standalone `ops/check-openapi.py` plus gateway OpenAPI tests | Management route/schema presence | Full schema equivalence against Rust has not been established |
| Extensions | TS-only: auth, scoping, migration prefixes, startup loading, version refusal | `extension-load.test.ts`, SDK packages, compiled smoke | Reviewed operator code; no untrusted-code isolation boundary |

## Running the gates

From the repository root, install `ts/` and `tests/compat-ts/` with `npm ci`, and
build the Rust oracle with the pinned Rust toolchain:

```sh
cargo build -p axond --locked
cd ts
AXOND_TEST_POSTGRES="$TEST_POSTGRES_DSN" npm test
npm run test:bun
npm run test:workerd
npm run test:parity -- --rust ../target/debug/axond
```

Set `AXOND_TEST_POSTGRES` for Bun and workerd too. Use an isolated test database
per concurrently running suite: the store tests reset their tables. A release
report must list skips, and a database lane with a skipped required test is not
passing evidence. Run `ops/typescript-compat.sh` for both vendor SDK lanes,
OpenAPI, Node tests, and host-independent imports; also run `check:alerts`,
`check:npm`, and the compiled-binary smoke.

The existing `Rust shadow compare` CI job runs the expanded runner automatically.
It now exercises 49 scenarios, checks expected status independently for each
process, compares contract headers and bodies, waits for the expected usage count,
and checks durable state before and after restart. It fails on any unexplained
difference. Its comparison tests also run in the Node and Bun test commands.

JSON object key order and SSE JSON spacing may differ. Numeric digits above
2^53 must remain significant. Responses and Messages streaming bytes are compared
without normalization. Header normalization only permits MIME parameter casing
and order, an optional UTF-8 JSON charset, and the Node adapter's content type on
empty 204/404 responses. It does not ignore response status, retry hints, cache
control, error types, error messages, event ordering, or usage/spend differences.

## Declared differences that block an unqualified parity claim

Review each row against the intended deployment. ADR 0066 already accepts some
changes; that is evidence of a design decision, not evidence that existing
operators can upgrade without reviewing it.

| Difference | Required release disposition |
| --- | --- |
| Per-tenant concurrency is absent from core; the rate-limit extension has different semantics | Retain the limiter or document and approve the replacement, including its shared/isolate scope |
| Billing-grade usage journal is absent and enabling it fails boot | Confirm consumers accept the usage-insert idempotency boundary and possible under-billing; otherwise implement the journal |
| Redis budgets and hot reload are absent | Inventory affected deployments; provide a supported replacement or an explicit migration |
| OTLP JSON replaces protobuf/gRPC; connection-pool and target-circuit metrics differ | Verify collectors, alerts and dashboards against the TS deployment |
| Bun/Workers have no separate connect timeout | Verify their header and overall bounds meet operational requirements |
| The models.dev importer accepts parsed JSON without Rust's full schema validation | Establish equivalent validation or explicitly approve the weaker import contract |
| Postgres DDL adds TS tables/columns; Rust DDL remains the shipped schema | Qualify provisioning, non-owner access, upgrade and rollback on the intended database |

Alias-level failover and minted-token CLI commands were already withdrawn by
ADR 0063. Their absence is not a new TS regression. Token and redaction
extensions add capabilities without restoring those withdrawn core contracts.

## Release decision and canary

Record the exact source SHA, runtime versions, artifact digest, Rust oracle SHA,
commands, test counts/skips, and every accepted difference in the release packet.
Re-run CI on the actual merge/release commit, not only an earlier branch head.
Require the TypeScript gateway, Rust comparison, compiled-binary and Bun jobs in
the release decision. Inspect repository protection separately; the presence of
a workflow does not prove GitHub requires it.

Before production cutover, run the Worker against real Hyperdrive with caching
disabled and the intended PlanetScale role/schema. Local workerd/Postgres proves
the runtime path, not Cloudflare account configuration or PlanetScale behavior.
Run signing on a release tag. Keep the current Rust artifact deployable and
exercise rollback on the target database before directing production traffic.

Start with an opted-in test namespace and controlled provider fixture traffic.
Then canary a small set of actual requests with one implementation owning each
request. Never replay billable production inference through a second gateway.
Reconcile each request's usage, spend, namespace and outcome; inspect typed error
rates, cancellations, readiness and latency against the existing qualification
envelope. An unexplained contract mismatch, duplicate charge, lost state, or
failed rollback stops expansion. Broaden only after the agreed traffic sample
and recovery checks pass. This audit does not deploy a production canary.
