# Troubleshooting

Axond fails closed and returns typed JSON errors. Start with the process log,
HTTP status, and `error.type`; do not infer the cause from status alone.

## The process never listens

Axond validates configuration, resolves every declared secret reference, and
connects configured backends before binding the socket.

| Log message shape | Likely cause | Action |
| --- | --- | --- |
| `failed to load config` | Missing file, invalid TOML, invalid graph, or unsupported combination. | Check `AXOND_CONFIG`, then compare against the configuration reference. |
| `references env var ... unset or empty` | A credential, gateway key, verifier, or DSN reference is absent from the process environment. | Set it on the actual service/container and restart. |
| `at least one gateway key` | No static `[[gateway_key]]` exists. | Add a breakglass key; minted verifiers are additive. |
| `hold the same secret` | Two gateway-key entries resolve to one value. | Give each principal a distinct value. |
| `usage sink configuration failed` | Postgres or OTLP configuration/connectivity failed, or the usage outbox could not connect to or read its tables. | Verify DSN/endpoint, DNS, TLS, schema, and credentials; for `[usage_journal]`, that `ops/postgres/usage_outbox_v1.sql` is applied in the named schema and the role can read it. |
| `budget configuration failed` | Budget backend unavailable or layout migration incomplete. | Restore the backend or complete the named migration. |
| `rate-limit configuration failed` | Redis is unavailable or invalid. | Verify URL, TLS, DNS, and connectivity. |
| `revocation configuration failed` | Redis/Postgres revocation store is unavailable or missing schema. | Apply DDL or restore connectivity. |

Boot errors name references and identifiers, not secret values.

## HTTP decision table

| Status / type | Meaning | First check |
| --- | --- | --- |
| `401 unauthorized` | No presented credential matched a static key or valid minted token. | Header value, key namespace, token audience/signature/expiry. |
| `403 token_scope_insufficient` | Auth succeeded but route/operator authority did not. | Token scope and whether the action requires the default-namespace static operator key. |
| `403 token_signer_not_permitted` | Verifier does not permit the token namespace. | Verifier `namespaces` and token `ns`. |
| `404 unknown_model` | Alias is absent or unavailable to the caller namespace. | `/v1/models`, credential coverage, alias spelling. |
| `400 unsupported_wire` | Route and alias provider family differ. | Keep every alias target in one wire family and use the matching route. |
| `400 bad_request` | Invalid request or query shape. | Error message; repeated/invalid `namespaces` values are rejected deliberately. |
| `429 budget_exceeded` | Subject or namespace spend cap cannot admit the estimate. | Budget metrics and namespace-denial metric. |
| `429 rate_limit_exceeded` | In-flight concurrency cap reached. | Caller concurrency and limiter metrics. |
| `429 tenant_concurrency_exceeded` | The caller's namespace is at `admission.max_in_flight_per_tenant` on this replica. | `axond.admission.in_flight`; whether the tenant's own concurrency, not the replica, is the cause. |
| `413 request_too_large` / `413 prompt_too_large` | Body over `admission.max_request_bytes`, or estimated input over `admission.max_prompt_tokens`. | The caller's payload size; raise the bound only if the workload genuinely needs it. |
| `200` + SSE `error` typed `upstream_stream_error` | A stream hit `admission.max_stream_duration_ms` or `admission.max_stream_bytes`; the bounds cannot change a status already sent. | The event's message names the bound; the usage record settles with what was relayed. |
| `415 unsupported_media_type` | The request did not declare `content-type: application/json`. | The caller's `Content-Type` header. |
| `400 output_limit_exceeded` | The request asked for more output tokens than `admission.max_output_tokens`. | The request's `max_tokens`/`max_completion_tokens`/`max_output_tokens`. |
| `503 gateway_overloaded` / `503 stream_capacity_exhausted` | The replica is at `admission.max_in_flight` or `max_in_flight_streams`. | `axond.admission.rejections` by resource, replica count, and whether the ceilings match what one process can hold. |
| `503 admission_queue_full` / `503 admission_queue_timeout` | Queueing is enabled and the queue is full, or a queued request outlived `admission.queue_wait_ms`. | Whether queueing is helping at all: sustained shedding here means the replica is under-provisioned, not bursty. |
| `503 admission_tenant_capacity_exhausted` | More distinct namespaces in flight than `admission.max_tenants`. | Namespace count in the deployment; retrying will not clear it, so no `Retry-After` is sent. |
| `502 upstream_transport` | Axond could not establish/complete the provider transport. The caller's answer is worded `upstream transport failure` and names no endpoint; the reason is in the replica's log, on the `upstream attempt failed on the transport` warn (`open stream failed on the transport` mid-stream). | Provider URL, DNS, TLS, egress, proxy, timeout — from that warn, not from the caller's body. |
| `504 upstream_timeout` | A transport bound fired before a response could be served. | `axond.timeout` on the attempt span names the phase: `connect`, `response_headers`, `buffered_body`, `stream_idle`, or `overall`. Tune that `[transport]` bound, or `failover.overall_timeout_ms` for `overall`. |
| `502 upstream_body_too_large` | A buffered provider body exceeded `transport.max_response_bytes`. | Whether the workload really returns bodies that size; otherwise treat the target as misbehaving. |
| `502 invalid_request` | Provider returned a non-retryable request/auth error. | Provider credential, model deployment, and provider body. |
| `503 usage_not_durable` | Billing-grade delivery is on and the request's usage event could not be made durable, so the gateway will not report success for a request it cannot bill. | `axond.usage.journal.appends` by outcome, and depth against capacity: a full outbox usually means delivery has stalled, not that appends are too fast ([usage outbox](./usage-outbox.md#when-a-request-is-refused)). |
| `503 budget_unavailable` | Shared budget backend failed under fail-closed policy. | Redis/Postgres health and latency. |
| `503 rate_limit_unavailable` | Redis limiter failed under fail-closed policy. | Redis health, invoke saturation, connection recovery. |
| `503 revocation_unavailable` | JTI store failed under fail-closed policy. | Redis/Postgres health and configured policy. |
| `503 continuation_affinity_unavailable` | A request carrying `previous_response_id` cannot safely use its pinned first target or credential. | First-target circuit and first-credential state; retry later. |

All error bodies use `{"error":{"type":...,"message":...}}`.

## A `/v1/responses` request fails while chat on the same alias succeeds

Expected. Every Responses request — initial calls included — uses only the
alias's first target and first credential, so it neither fails over nor rotates;
chat on the same alias still walks the remaining targets and keys. This is what
keeps a response id continuable without gateway state
([ADR 0023](../adr/0023-openai-responses-passthrough.md)).

An **initial** Responses request reports the ordinary cause —
`503 all_provider_circuits_open` when the first target's circuit is open,
`no_credential` when the first credential is missing, or the upstream error
itself — because nothing was continued. Only a request with a non-empty
`previous_response_id` reports `continuation_affinity_unavailable`. Check the
first target and first credential of the alias; do not expect a later target to
absorb the failure, and do not reorder `targets` or the credential pool to route
around it, because that strands response ids created under the previous order.

## Health is green but requests fail

`/healthz` and `/readyz` report a serving process, not continuous provider or
datastore health. Backends were connected before boot, but may fail later.
Inspect typed errors, `axond.*.unavailable_denials`, upstream attempt spans, and
provider/network telemetry.

## Credential status

```bash
curl --fail \
  -H "Authorization: Bearer $GW_INBOUND_PLATFORM_KEY" \
  http://127.0.0.1:8080/v1/credentials
```

- `healthy`: available to selection.
- `parked`: recent provider `429`s crossed the credential threshold.
- `probe`: cooldown elapsed; the next real request may test it.

Status reads are pure and do not consume a probe. A connection-refused provider
does not park a credential; only provider `429` exhaustion does.

The all-namespace view requires a scope-less static key in the configured
default namespace:

```bash
curl --fail \
  -H "Authorization: Bearer $GW_INBOUND_PLATFORM_KEY" \
  'http://127.0.0.1:8080/v1/credentials?namespaces=all'
```

Minted tokens cannot access this operator view, even if they carry a forged
`credentials:all` claim.

## Reload did not apply

A rejected candidate leaves the old snapshot serving. Look for
`config reload rejected` and fix the complete error before retrying.

- A new environment variable cannot be injected into a running process.
- File-backed key material can be replaced and re-read.
- `[server]`, `[transport]`, `[[usage_sink]]`, and `[budget]` changes require
  restart; a changed `[transport]` is validated and warned about, because the
  upstream HTTP client is already pooled.
- ConfigMap/projected-volume updates require `[reload] watch = true` or an
  explicit `SIGHUP`.

## A request hangs, or ends sooner than expected

Every upstream phase is bounded, so a hang is a bound that is too wide and an
early `504` is one that is too tight. Read `axond.timeout` first — the phase is
the diagnosis:

| Phase | What was waiting | Usual cause |
| --- | --- | --- |
| `connect` | TCP + TLS to the provider | Egress policy, DNS, or a proxy swallowing the connection. |
| `response_headers` | Time to first byte after dispatch | An overloaded provider or a queued request; long-thinking models legitimately need a wider bound. |
| `buffered_body` | The rest of a non-streamed body | A provider trickling a large completion. Consider streaming instead. |
| `stream_idle` | The next chunk of an **open** stream | A half-dead connection or a provider that stopped mid-answer. |
| `overall` | Nothing — no attempt was dispatched | `failover.overall_timeout_ms` was already spent when the walk reached this target. |

`axond.timeout.bound` says which bound ended the wait: `phase` for the
`[transport]` bound, `walk_budget` for what was left of
`failover.overall_timeout_ms`. A run of `walk_budget` on `response_headers` means
attempts are being cut short by the walk rather than by the phase bound — widen
`failover.overall_timeout_ms` or expect fewer targets per walk. The phase is still
blamed on the target in that case, so a black-holing target does trip its circuit;
only `overall`, where no target was called at all, is excluded from target health.

Remember that `response_header_timeout_ms` and `buffered_body_timeout_ms` cover a
non-streamed model's whole thinking time, since no headers arrive until the
completion exists. Tightening them below `failover.overall_timeout_ms` caps a
single attempt so later targets get a turn; it also refuses slow completions the
walk still had time for.

Two consequences are deliberate and not bugs:

- A slow *productive* stream is never cut off by `failover.overall_timeout_ms`.
  Only silence longer than `stream_idle_timeout_ms` ends it.
- A stream that stalls after bytes were already relayed terminates in band on
  the already-`200` response and is **not** retried; retrying would splice a
  second completion into one answer. The usage record still settles exactly
  once.

## Streams fail through a proxy

Test the same stream directly against Axond and through ingress. If direct works:

- disable response buffering;
- raise idle and total request timeouts;
- verify chunked/SSE transfer is preserved;
- inspect proxy retries, which must not replay a committed stream;
- verify client disconnects reach Axond so usage/finalization can complete.

See the [observability runbook](../observability.md) for metric names and alert
recommendations.
