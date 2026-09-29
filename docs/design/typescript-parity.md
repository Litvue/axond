# TypeScript gateway parity contract

This is the contract for `ts/` against the post-[ADR 0063](../adr/0063-stateful-only-namespaced-gateway.md) Rust gateway. [ADR 0066](../adr/0066-typescript-hono-extension-contract.md) records the decisions. The conformance launcher is `ops/typescript-compat.sh`.

## Same wire

| Behavior | Evidence |
| --- | --- |
| `GET /healthz` is `200` `ok`. `GET /readyz` is `ready`, or `503` `draining` as soon as SIGTERM starts shutdown. New `/api` and `/ns` requests stay admitted for `shutdown.drain_grace_ms`. Admission then closes and a request still accepted is `503` `draining` with `Retry-After: 0` before authentication. | CLI |
| Exactly one `[[gateway_key]]`. `Authorization: Bearer` wins over `x-api-key`. Missing or wrong key is `401` `unauthorized` before namespace lookup. | compat lanes |
| Inference is `/ns/{ns}/v1/...`. The raw path is checked before percent-decoding, so `/ns/%70latform/...` is `400` `invalid_namespace` and the identifier is not echoed. | compat lanes |
| Unknown namespace is `404` `unknown_namespace`. Unprefixed model is `400` `model_unprefixed`. | compat lanes |
| OpenAI chat, embeddings, and Responses, buffered and streamed. Anthropic Messages, including thinking and tool-use bytes. Model rewritten to the bare id. Provider credential injected. Gateway key stripped. | `tests/compat`, `tests/compat-ts` |
| Streamed fixtures in `tests/fixtures/**/*.sse` are relayed byte-for-byte, including across chunk splits. A fast fixture does not gain a `: keepalive` comment. | gateway streaming tests |
| No budget row, or `spent >= limit`, is `429` `budget_exceeded`. A failed budget read is `503` `budget_unavailable` unless `[storage] on_unavailable = "allow"`, which serves the request and does not charge. Other Store failures are `503` `store_unavailable`. The driver message is not returned. Charge is one usage row per `request_id`. Delete bumps incarnation so a late settle does not charge. | store tests |
| File namespaces do not inherit platform credentials unless `allow_platform_fallback`. API-created namespaces do. Config namespaces cannot be deleted (`409`). | gateway |
| A provider `429` rotates to the next credential in the pool and counts toward `credential_pool.failure_threshold` (default 2). A `500` stays on that credential and does not park it. An OpenAI chat stream that sends a rate-limit event before any content rotates the same way and does not forward that event. A rate-limit event after content stays on that stream and counts as one credential failure, so the stream end does not clear the streak. A rate-limit event after the terminal frame does not. Responses and native Messages do not rotate on a mid-stream rate-limit event; that event counts as one credential failure. After `cooldown_seconds` (default 30) the status is `probe` and one request is allowed through; handing out that probe rearms the cooldown. A success clears the streak. Responses stay pinned to the first credential. `0` for the threshold or cooldown fails boot. | gateway |
| Management routes and the OpenAPI 3.1 document match `ops/check-openapi.py`. | `ts/openapi.json` |
| Withdrawn config sections fail boot by name and do not echo secret values. `transport.max_response_bytes` and `admission.max_request_bytes` load from the file; `0` is rejected. An oversized body is `413` `request_too_large` and is not echoed. `transport.max_error_bytes` (default 65536) truncates a provider error body; the provider status still reaches the caller. `0`, or a value above `max_response_bytes`, fails boot. `admission.max_prompt_tokens` (default `1000000`) and `admission.max_output_tokens` (default `200000`) load from the file; `0` disables that ceiling. Estimated input over the prompt ceiling is `413` `prompt_too_large`. A requested output allowance over the output ceiling is `400` `output_limit_exceeded`. Neither message echoes the request. | config tests |
| A per-request spend cap above the pre-dispatch estimate is `403` `request_cost_ceiling_exceeded` before the provider is called. The estimate is the JSON byte length divided by four, plus the requested output allowance or 1024. Embeddings estimate no output. An equal estimate is served, and settlement charges measured usage. | gateway |
| `GET /ns/{ns}/v1/credentials` reads the raw query. A repeated `namespaces` parameter, or a component that is not percent-encoded UTF-8, is `400` `bad_request`. `%61ll` is `all`. `+` is a space. An empty or other value is `400`. Unknown keys are ignored. Rows are sorted by namespace, provider, and credential id. `source` is `platform` or `byok`. A fallback tenant sees platform pools it does not override, and an env-derived platform label is omitted. | gateway |
| `failover.overall_timeout_ms` (default 30000) tightens the header and buffered-body waits. A budget already spent before the next credential is opened is `504` `upstream_timeout`. An open stream is not cut by that budget. After a terminal SSE event, `transport.stream_terminal_grace_ms` (default 1000) closes the body; bytes that arrive inside the grace are relayed. `transport.connect_timeout_ms` (default 5000) is enforced on Node. `0` fails boot. | gateway |

## Gaps, on purpose

| Rust behavior | TypeScript |
| --- | --- |
| Per-replica tenant admission and `tenant_concurrency_exceeded` | Not in core. Use the rate-limit extension. |
| Alias failover across providers | Absent, as in ADR 0063. Credential rotation within one provider retries `provider_dependency_failed` on the next pool member. Responses stay pinned to the first credential. |
| Minted tokens, revocation, redaction | Extensions (`@axond/tokens`, `@axond/redact`). Core still rejects an `axt1.` credential that no extension accepts. |
| Billing-grade usage journal (ADR 0049) | Not built. The usage insert is the charge key. |
| Redis budget backend | Not an SDK dependency. |
| Hot reload of config | Withdrawn. Extensions load at process start. |
| OTLP/HTTP protobuf traces, metrics, and usage logs | OTLP/HTTP JSON traces and metrics when `OTEL_EXPORTER_OTLP_ENDPOINT` is set. `grpc` and `http/protobuf` fail boot. Request logs are one JSON object on stdout and omit prompts, completions, and credentials. |
| Separate connect timeout on the Bun binary and on Workers | The header and failover budgets still bound the attempt. Node applies `connect_timeout_ms` with its HTTP client. |

## Distribution

| Target | How |
| --- | --- |
| Node process | `ts/bin/axond` |
| Container | `ts/Dockerfile`. The repository-root `Dockerfile` remains the Rust release image. |
| Worker | `ts/packages/worker`. Hyperdrive caching disabled. Not executed against PlanetScale here. |
| Compiled binary | `bun build --compile` of `ts/packages/cli/src/main.ts`, when Bun is installed. The Node wrapper is the path CI runs. |

## Shadow

`ts/scripts/shadow-compare.ts` sends the fixture suite through the Rust binary and `ts/bin/axond`. Status, parsed JSON, and the charged `(model, status, cost)` rows match. Rust re-encodes some JSON objects with sorted keys. TypeScript relays the upstream bytes, which is what the streaming fixtures require. Each process mints its own `request_id`.
