# Observability and runbook

What axond emits, and how each failure mode looks when it happens. The design
rationale is [ADR 0007](./adr/0007-telemetry-model.md); this is the operational
view.

## Turning telemetry on

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4318   # OTLP/HTTP only
```

- **Unset** (the default): JSON logs on stdout and nothing else. No exporter,
  tracer, meter, or propagator is installed, and the recording helpers return
  before they build a single attribute — the request path does no exporter work
  at all. This is a supported production posture, not a degraded one.
- **Set**: traces, metrics, and (with the OTLP usage sink) usage logs are
  exported with `service.name = axond`. Only `OTEL_EXPORTER_OTLP_PROTOCOL` of
  `http/protobuf` is supported; anything else is a boot error rather than a
  silent no-op.

Logs are always JSON on stdout, filtered by `RUST_LOG` (default
`info,axond=info`).

## Traces

One `http.server.request` span per request, with one `axond.upstream.attempt`
child per upstream call — so an ordered-failover walk reads as N attempt spans
under one server span, the last carrying the status the caller saw. Each
attempt contains one `axond.credential.lease` child for every attempted or
parked credential.

| Span | Key attributes |
| --- | --- |
| `http.server.request` | `http.request.method`, `http.route`, `http.response.status_code`, `axond.request_id`, `axond.namespace`, `axond.subject`, `gen_ai.request.model`, `axond.target.*`, `axond.credential_source`, `axond.status`, `axond.retry_count`, `gen_ai.usage.*`, `axond.cost_microdollars`, `axond.latency_ms`, `axond.ttft_ms` |
| `axond.upstream.attempt` | `axond.attempt` (zero-based), `axond.target.provider`, `axond.target.model`, `axond.credential_source`, `axond.status`, `axond.latency_ms`, `axond.ttft_ms`, `axond.timeout` (which transport bound fired, when one did) |
| `axond.credential.lease` | `axond.credential.id`, `axond.credential_source`, `axond.credential.index`, `axond.status` (`served`, `rate_limited`, `error`, `parked`) |
| `axond.config.reload` | `axond.reload.trigger`, `axond.reload.outcome`, `axond.config.generation` |

An inbound `traceparent` is **joined**, not replaced, and the context is
injected into the upstream request, so a caller's trace runs end to end. Spans
never carry credentials, prompts, or completions.

A streamed response outlives its server span: the span records where the stream
was routed before dispatch, and the final tokens/cost land on the metrics and
the usage record instead.

## Metrics

`axond.http.*` covers every HTTP request — including ones that never reach a
provider — with low-cardinality dimensions. `axond.request.*` /
`axond.upstream.*` are emitted from the single canonical usage record, so a
metric and a usage row can never disagree.

| Instrument | Type | Dimensions | Use it for |
| --- | --- | --- | --- |
| `axond.http.server.requests` | counter | `http.request.method`, `http.route`, `http.response.status_code` | Overall RPS and error rate, including rejected requests. |
| `axond.http.server.duration` | histogram (ms) | same | Served latency. |
| `axond.request.count` | counter | `axond.namespace`, `gen_ai.request.model`, `axond.target.provider`, `axond.target.model`, `axond.credential_source`, `axond.status` | Per-tenant / per-model volume and outcome mix. |
| `axond.request.duration` | histogram (ms) | same | End-to-end gateway latency. |
| `axond.request.time_to_first_token` | histogram (ms) | same | TTFT — the number streaming users feel. |
| `axond.tokens.input` | counter | same | Non-cached prompt remainder. |
| `axond.tokens.cache_read` | counter | same | Prompt tokens served from cache. |
| `axond.tokens.cache_write` | counter | same | Prompt tokens written to cache. |
| `axond.tokens.output` | counter | same | Completion token volume. |
| `axond.cost.microdollars` | counter (µUSD) | same | Spend, priced from the target catalogue. |
| `axond.upstream.errors` | counter | same | Upstream failure rate by target. |
| `axond.upstream.timeouts` | counter | `axond.target.provider`, `axond.target.model`, `axond.timeout` | Which transport bound is being hit: `connect`, `response_headers`, `buffered_body`, `stream_idle`, or `overall`. |
| `axond.upstream.circuit_state` | gauge | `axond.target.provider`, `axond.target.model` | `0` closed, `1` half-open, `2` open. |
| `axond.usage.records_written` | counter | `axond.usage_sink` | Records a sink acknowledged. |
| `axond.usage.records_dropped` | counter | `axond.usage_sink`, `axond.drop_reason` | Records discarded rather than delaying a request. |
| `axond.config.reloads` | counter | `axond.reload.trigger`, `axond.reload.outcome` | Reload attempts and whether they applied. |
| `axond.config.generation` | gauge | — | `0` at boot, `+1` per applied reload. |
| `axond.budget.capacity_denials` | counter | — | In-memory admissions denied because the ledger bound was exhausted. |
| `axond.budget.namespace_denials` | counter | — | Admissions denied by `namespace_limit_microdollars` rather than by the subject's own cap. Both answer `429`. |
| `axond.budget.retained_subjects` | gauge | — | In-memory ledgers retained after capacity-pressure pruning; watch against `max_subjects`. |
| `axond.rate_limit.denials` | counter | — | Inbound concurrency admissions rejected. |
| `axond.rate_limit.capacity_denials` | counter | — | In-memory admissions rejected because the bounded subject map is full. |
| `axond.rate_limit.unavailable_denials` | counter | — | Redis rate-limit admissions denied because the store was unavailable. |

### What to alert on

| Alert | Signal | Why |
| --- | --- | --- |
| Usage is being lost | `axond.usage.records_dropped` rate > 0, sustained | Spend data is gone and will not come back. Buffer or destination is undersized. |
| A target is out | `axond.upstream.circuit_state = 2`, sustained | Every request is failing over (or failing) for that target. |
| Budget denials | `axond.http.server.requests{status=429}` rising | Tenants are hitting their cap. |
| Inbound concurrency denials | `axond.rate_limit.denials` rising | Authenticated callers are reaching their per-replica in-flight limit. |
| Budget store down | `axond.http.server.requests{status=503}` rising | Fail-closed denial: fix the store, or the whole tenant is refused. |
| Config drift across the fleet | `axond.config.generation` differs between replicas | A replica missed a reload and is serving stale routing or keys. |
| Rejected reloads | `axond.config.reloads{outcome="rejected"}` > 0 | Someone edited the config into an invalid state; the old one is still serving. |
| Budget capacity exhausted | `axond.budget.capacity_denials` > 0 | The replica is refusing unseen subjects; investigate subject churn and the in-memory bound. |
| Budget ledger pressure | `axond.budget.retained_subjects` near configured `max_subjects` | Leading indicator that the bound is approaching; watch it before capacity denials occur. |
| Namespace budget exhausted | `axond.budget.namespace_denials` > 0 | The whole namespace is out of budget, so *every* subject in it is being denied — not one noisy caller. Raise `namespace_limit_microdollars` or investigate what is spending. |
| TTFT regression | `axond.request.time_to_first_token` p95 | Provider degradation shows here before total latency moves. |
| Upstream stalls | `axond.upstream.timeouts` rising | Split by `axond.timeout`: `connect` is egress or DNS, `response_headers` is an overloaded provider, `stream_idle` is a half-dead connection, and `overall` means the failover budget is too tight for how slow the target became. |

## Usage records

One record per terminated request — including failures, cancellations, and
partial streams. With no `[[usage_sink]]` configured it is one JSON line on
stdout. Fields, versioning, and delivery guarantees are the published contract
in [`docs/usage-schema.md`](./usage-schema.md).

Records carry the credential's **label** (`credential_id`) and the gateway key's
**env-var name** (`subject`) — never a secret.

## Failure modes

Every route always exists; unavailable behaviour answers with a typed error
rather than a bare 404 that would be indistinguishable from a wrong `base_url`.
Error bodies are `{"error": {"type": …, "message": …}}`.

| Status | `type` | What happened | What to do |
| --- | --- | --- | --- |
| `401` | `unauthorized` | No `Authorization: Bearer` / `x-api-key`, or the token is not in the key table. | Check the caller's key and that its `[[gateway_key]]` is declared and its env var set. There is no keyless mode. |
| `404` | `unknown_model` | The alias is not configured. | Add a `[[model]]`, or fix the caller. `/v1/models` lists the aliases the caller can invoke. |
| `400` | `unsupported_wire` | The alias's target (or one of its failover targets) does not speak this route's wire — e.g. an OpenAI-only alias on `/v1/messages`. Raised **before** anything is reserved or dispatched. | Fix the alias's targets; no route translates between wires. See the [compatibility contract](./compatibility.md). |
| `400` | `invalid_request`, `context_window_exceeded`, `bad_request` | The provider (or the gateway) rejected the request shape. | Caller-side fix; retrying will not help. |
| `429` | `budget_exceeded` | The `(namespace, subject)` cap is spent — settled spend plus live holds leaves no room. With `namespace_limit_microdollars` set, the same code and body also cover the namespace-wide cap being spent; `axond.budget.namespace_denials` is what tells them apart. | Raise `limit_microdollars` (or `namespace_limit_microdollars`) or wait. This is the tenant's own cap, not a provider rate limit. |
| `503` | `budget_unavailable` | The budget store could not be reached and `on_unavailable = "deny"` (the default). | Fix Redis/Postgres. **Distinguish this from `429`:** `429` is the tenant over budget, `503` is *your* dependency down. |
| `503` | `rate_limit_unavailable` | The Redis rate-limit store could not be reached and `on_unavailable = "deny"` (the default). | Fix Redis or deliberately choose `on_unavailable = "allow"`. |
| `503` | `all_provider_circuits_open` | Every target for the alias has a tripped circuit. | The upstreams are down or the thresholds are too tight; check `axond.upstream.circuit_state`. |
| `502` | `no_credential` | The namespace has no credential for the resolved provider and no platform fallback. | Add a `[[credential]]`, or set `allow_platform_fallback` deliberately. |
| `502` | `upstream_transport`, `provider_dependency_failed`, `model_unavailable`, `invalid_stream` | The upstream failed after the failover walk was exhausted. | Check the provider's status and the attempt spans; `attempts` on the usage record says how hard the gateway tried. |
| `504` | `upstream_timeout` | A transport bound fired before a response could be served: connecting, waiting for headers, reading a buffered body, waiting for the next chunk of an open stream, or `failover.overall_timeout_ms` cancelling the in-flight attempt. | `axond.upstream.timeouts{axond.timeout}` and the attempt span's `axond.timeout` name the phase. Tune the matching `[transport]` bound, or `overall_timeout_ms` when the phase is `overall`. |
| `502` | `upstream_body_too_large` | A buffered provider response exceeded `transport.max_response_bytes`, so it was refused instead of held in memory. | Raise `max_response_bytes` if the workload legitimately returns bodies that size; otherwise treat it as a misbehaving target. |

Mid-stream failures are different by construction. Native passthrough streams
and OpenAI-normalized streams that have already queued downstream bytes remain
terminal: the relay emits an SSE `error` event on the already-`200` response,
and the usage record settles as `partial` or `upstream_error`. An
OpenAI-normalized stream may instead rotate to the next pooled credential when
an explicit upstream rate-limit event arrives before anything is queued
downstream; the additional lease span remains under the original upstream
attempt and request trace. Rotation does not create another upstream attempt
span: there is one attempt span per target attempt, while `attempts` and
`axond.retry_count` remain target-scoped. The target-open attempt can be
`ok` while a later lease child is `rate_limited`.
Rotation uses the same `failover.overall_timeout_ms` deadline as target
failover. A long time-to-first-token stream can therefore remain terminal
instead of rotating once that deadline expires; the attempt span is closed
with the target's terminal status and no later lease span is emitted.

An open stream is bounded by `transport.stream_idle_timeout_ms` rather than by
the failover deadline: a stream that keeps producing runs to completion however
long it takes, while one that goes silent for longer than the idle bound is
terminated in band on the already-`200` response. Nothing is retried there, and
no second completion is spliced in — the usage record settles once, as `partial`
or `upstream_error`, and `axond.upstream.timeouts{axond.timeout="stream_idle"}`
is what distinguishes a stalled provider from one that ended early
([ADR 0027](./adr/0027-transport-phase-bounds.md)).

A `504` whose phase is `overall` reports the gateway's own spent failover
budget, so it is attributed to the request and the target's metrics but does not
count against the target's circuit breaker; the per-phase bounds do.

### Boot failures

The process exits before binding the socket, with a message naming the
offending *reference*. What an operator sees is one of these shapes behind a
prefix — `Error: config resolution failed: …` for a resolution failure, or
`Error: failed to load config from <path>: invalid config: …` for one caught
while parsing and validating the file:

| Message shape | Cause |
| --- | --- |
| `gateway_key for namespace … references env var …, which is unset or empty` | A declared inbound key's variable is missing. |
| `at least one [[gateway_key]] is required` | Fail-closed auth: a keyless config is not servable. |
| `… hold the same secret, so the caller's namespace would be ambiguous` | Two gateway keys with one value. |
| `credential … references env var …, which is unset or empty` | A declared provider credential's variable is missing. |
| `model … targets undefined provider …` / `has no targets` | A dangling or empty alias. |
| `exactly one namespace must set default = true` | Zero or several defaults. |
| `usage sink configuration failed: …` / `budget configuration failed: …` | A DSN reference is unset, or the datastore did not accept a connection at boot. |

None of these messages contain a secret value — only env-var names, namespaces,
and provider ids.

### Reloads

`SIGHUP` (and `[reload] watch`) re-run the same validation. A rejected candidate
logs at `error` with the reason and leaves the previous config serving; the
counter still increments with `outcome="rejected"`, so "someone tried and
failed" is visible. An applied reload logs an added/removed diff of namespaces,
providers, aliases, credential labels, and gateway-key env-var names, and bumps
`axond.config.generation`.

If a replica's generation lags the fleet, it missed a reload — its file or its
process environment differs. Restarting it is always safe: it is stateless.
