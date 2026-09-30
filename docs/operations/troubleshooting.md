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
| `exactly one \`[[gateway_key]]\`` | Zero or more than one static key. | Declare exactly one deployment-wide key. |
| `mode is withdrawn` | `mode` is set. | Remove the `mode` key. |
| `usage sink configuration failed` | Postgres or OTLP configuration/connectivity failed, or the usage outbox could not connect to or read its tables. | Verify DSN/endpoint, DNS, TLS, schema, and credentials; for `[usage_journal]`, that `ops/postgres/usage_outbox_v1.sql` is applied in the named schema and the role can read it. |
| `budget configuration failed` | Budget backend unavailable or layout migration incomplete. | Restore the backend or complete the named migration. |

Boot errors name references and identifiers, not secret values.

## HTTP decision table

| Status / type | Meaning | First check |
| --- | --- | --- |
| `401 unauthorized` | No presented credential matched the static gateway key. | Header value. |
| `404 unknown_namespace` | Path namespace missing or deleted. | `GET /api/v1/namespaces/{ns}`; same body for never-existed and deleted. |
| `400 model_unprefixed` / `unknown_provider` | Request `model` is not `provider-id/model-id`. | Prefix with a configured provider id. |
| `400 unsupported_wire` | Route and alias provider family differ. | Keep every alias target in one wire family and use the matching route. |
| `400 bad_request` | Invalid request or query shape. | Error message; repeated/invalid `namespaces` values are rejected deliberately. |
| `429 budget_exceeded` | Namespace spend cap: `spent >= limit`, or no budget row. | `GET /api/v1/namespaces/{ns}/budgets/{period}`. |
| `429 tenant_concurrency_exceeded` | The caller's namespace is at `admission.max_in_flight_per_tenant` on this replica. | `axond.admission.in_flight`; whether the tenant's own concurrency, not the replica, is the cause. |
| `413 request_too_large` / `413 prompt_too_large` | Inbound body over `admission.max_request_bytes`, or estimated input over `admission.max_prompt_tokens`. | The caller's payload size; raise the bound only if the workload genuinely needs it. |
| `200` + SSE `error` typed `upstream_stream_error` | A stream hit `admission.max_stream_duration_ms` or `admission.max_stream_bytes`; the bounds cannot change a status already sent. | The event's message names the bound; the usage record settles with what was relayed. |
| `415 unsupported_media_type` | The request did not declare `content-type: application/json`. | The caller's `Content-Type` header. |
| `400 output_limit_exceeded` | The request asked for more output tokens than `admission.max_output_tokens`. | The request's `max_tokens`/`max_completion_tokens`/`max_output_tokens`. |
| `503 gateway_overloaded` / `503 stream_capacity_exhausted` | The replica is at `admission.max_in_flight` or `max_in_flight_streams`. | `axond.admission.rejections` by resource, replica count, and whether the ceilings match what one process can hold. |
| `503 admission_queue_full` / `503 admission_queue_timeout` | Queueing is enabled and the queue is full, or a queued request outlived `admission.queue_wait_ms`. | Whether queueing is helping at all: sustained shedding here means the replica is under-provisioned, not bursty. |
| `503 admission_tenant_capacity_exhausted` | More distinct namespaces in flight than `admission.max_tenants`. | Namespace count in the deployment; retrying will not clear it, so no `Retry-After` is sent. |
| `503 settlement_capacity_exhausted` | The replica is carrying `admission.max_pending_settlements` charges its Store has not yet settled; new requests are refused rather than admitted charges dropped. | The Store, not the replica: `axond.settlement.oldest_pending_age`, `axond.settlement.in_flight` by stage, and the budget backend's health. `axond.settlement.failures` says whether charges are being abandoned as well as delayed. The TypeScript process also writes JSON `msg` `settlement_failure` with `reason` `queue_timeout` or `execution_timeout` and `waited_ms`, or `charge_failed` when the Store write throws. That line omits the driver text. |
| `502 upstream_transport` | Axond could not establish/complete the provider transport. The caller's answer is worded `upstream transport failure` and names no endpoint; the reason is in the replica's log, on the `upstream attempt failed on the transport` warn (`open stream failed on the transport` mid-stream). The TypeScript process writes the same incident as JSON `msg` `upstream_transport` with `phase` `request`, `stream`, or `closing` and `reason` `dns`, `refused`, `reset`, `tls`, or `other`, and that line omits the endpoint. | Provider URL, DNS, TLS, egress, proxy, timeout — from that warn, not from the caller's body. |
| `504 upstream_timeout` | A transport bound fired before a response could be served. | `axond.timeout` on the attempt span names the phase: `connect`, `response_headers`, `buffered_body`, `stream_idle`, or `overall`. Tune that `[transport]` bound, or `failover.overall_timeout_ms` for `overall`. |
| `502 upstream_body_too_large` | A buffered provider body exceeded `transport.max_response_bytes`. | Whether the workload really returns bodies that size; otherwise treat the target as misbehaving. |
| `502 invalid_request` | Provider returned a non-retryable request/auth error. | Provider credential, model deployment, and provider body. |
| `503 usage_not_durable` | Billing-grade delivery is on and the request's usage event could not be made durable, so the gateway will not report success for a request it cannot bill. | `axond.usage.journal.appends` by outcome, and depth against capacity: a full outbox usually means delivery has stalled, not that appends are too fast — or a retired `consumer` name is still registered and holding retention open ([usage outbox](./usage-outbox.md#when-a-request-is-refused)). |
| `503 budget_unavailable` | The Store failed under `[storage] on_unavailable = "deny"`. The TypeScript process writes JSON `msg` `budget_unavailable` with `stance` `deny`. `allow` writes the same `msg` with `stance` `allow`, serves the request, and does not charge. That line omits the driver text. | SQLite/Postgres health and latency. |
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
  http://127.0.0.1:8080/ns/platform/v1/credentials
```

- `healthy`: available to selection.
- `parked`: recent provider `429`s crossed the credential threshold.
- `probe`: cooldown elapsed; the next real request may test it.

Status reads are pure and do not consume a probe. A connection-refused provider
does not park a credential; only provider `429` exhaustion does.

The all-namespace view requires the static key whose `namespace` is the
configured default namespace:

```bash
curl --fail \
  -H "Authorization: Bearer $GW_INBOUND_PLATFORM_KEY" \
  'http://127.0.0.1:8080/ns/platform/v1/credentials?namespaces=all'
```

## A config change did not apply

There is no hot reload. Config, environment, and file-backed key material are
read at boot, so a change takes a restart or a rollout.

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
  Before its semantic terminal event, only silence longer than
  `stream_idle_timeout_ms` ends it. A byte-faithful Native or Responses body
  still open after completion closes successfully at
  `stream_terminal_grace_ms`; trailing extension chunks do not reset that grace.
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
