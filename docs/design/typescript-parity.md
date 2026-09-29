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
| No budget row, or `spent >= limit`, is `429` `budget_exceeded`. Charge is one usage row per `request_id`. Delete bumps incarnation so a late settle does not charge. | store tests |
| File namespaces do not inherit platform credentials unless `allow_platform_fallback`. API-created namespaces do. Config namespaces cannot be deleted (`409`). | gateway |
| Management routes and the OpenAPI 3.1 document match `ops/check-openapi.py`. | `ts/openapi.json` |
| Withdrawn config sections fail boot by name and do not echo secret values. | config tests |

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

## Distribution

| Target | How |
| --- | --- |
| Node process | `ts/bin/axond` |
| Container | `ts/Dockerfile`. The repository-root `Dockerfile` remains the Rust release image. |
| Worker | `ts/packages/worker`. Hyperdrive caching disabled. Not executed against PlanetScale here. |
| Compiled binary | `bun build --compile` of `ts/packages/cli/src/main.ts`, when Bun is installed. The Node wrapper is the path CI runs. |

## Shadow

`ts/scripts/shadow-compare.ts` sends the fixture suite through the Rust binary and `ts/bin/axond`. Status, parsed JSON, and the charged `(model, status, cost)` rows match. Rust re-encodes some JSON objects with sorted keys. TypeScript relays the upstream bytes, which is what the streaming fixtures require. Each process mints its own `request_id`.
