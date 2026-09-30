# Observability and runbook

What axond emits, and how each failure mode looks when it happens. The design
rationale is [ADR 0007](./adr/0007-telemetry-model.md); this is the operational
view.

## Turning telemetry on

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4318   # OTLP/HTTP only
export AXOND_INSTANCE_ID=axond-replica-a                  # optional, unique per replica
```

- **Unset** (the default): JSON logs on stdout and nothing else. No exporter,
  tracer, meter, or propagator is installed, and the recording helpers return
  before they build a single attribute — the request path does no exporter work
  at all. A valid inbound W3C trace id is still retained as optional usage
  correlation; validating that bounded header does not install or invoke an
  exporter. This is a supported production posture, not a degraded one.
- **Set**: traces, metrics, and (with the OTLP usage sink) usage logs are
  exported with `service.name = axond`. If `AXOND_INSTANCE_ID` is set, the same
  bounded deployment identity is exported as the OTLP resource attribute
  `service.instance.id`; the shipped collector converts it to the
  `service_instance_id` Prometheus label so a fleet alert can identify one
  replica. It must contain only ASCII letters, digits, `.`, `_`, or `-`, and be
  at most 128 bytes. It is never populated from tenant, model, caller, or
  credential data. Only `OTEL_EXPORTER_OTLP_PROTOCOL` of `http/protobuf` is
  supported; anything else is a boot error rather than a silent no-op.

Logs are always JSON on stdout, filtered by `RUST_LOG` (default
`info,axond=info`).

## Health surfaces

Two production surfaces answer two different questions
([ADR 0031](./adr/0031-bounded-status-contract.md)).

| Surface | Authentication | Question it answers |
| --- | --- | --- |
| `GET /healthz` | none | *Is the process alive?* Answers `ok` throughout, including the shutdown drain. Restart it if this fails. |
| `GET /readyz` | none | *Should traffic be sent here?* `ready`, or `503 draining` once termination begins. Point the load balancer here. |

Neither `/healthz` nor `/readyz` observes a dependency. A Store outage on the
budget path is `503 budget_unavailable` (default `[storage].on_unavailable =
deny`). Dependency health is in typed errors, logs, and metrics. The
[observability runbook](./operations/observability-runbook.md) says what to do
about each signal, and the shipped dashboard and alert assets under
[`ops/observability/`](../ops/observability/) are the fleet-wide view.

## Traces

One `http.server.request` span per request, with one `axond.upstream.attempt`
child per upstream call — so an ordered-failover walk reads as N attempt spans
under one server span, the last carrying the status the caller saw. Each
attempt contains one `axond.credential.lease` child for every attempted or
parked credential.

| Span | Key attributes |
| --- | --- |
| `http.server.request` | `http.request.method`, `http.route`, `http.response.status_code`, `axond.request_id`, `axond.namespace`, `axond.subject`, `gen_ai.request.model`, `axond.target.*`, `axond.credential_source`, `axond.status`, `axond.retry_count`, `gen_ai.usage.*`, `axond.cost_microdollars`, `axond.latency_ms`, `axond.ttft_ms` |
| `axond.upstream.attempt` | `axond.attempt` (zero-based), `axond.target.provider`, `axond.target.model`, `axond.credential_source`, `axond.status`, `axond.latency_ms`, `axond.ttft_ms`, `axond.upstream.status` (provider HTTP refusal code), `axond.upstream.message` (bounded failure diagnostic), `axond.timeout` (which phase stalled, when one did), `axond.timeout.bound` (`phase` or `walk_budget`) |
| `axond.credential.lease` | `axond.credential.id`, `axond.credential_source`, `axond.credential.index`, `axond.status` (`served`, `rate_limited`, `error`, `parked`) |
| `axond.revision.converge` | `axond.revision.trigger` (`boot`, `polled`, `notified`, or `pricing-boundary`), outcome, active/desired revision, lag, and generation |

An inbound `traceparent` is **joined**, not replaced, and the context is
injected into the upstream request, so a caller's trace runs end to end.

Failed attempt spans have OpenTelemetry error status. Their `axond.status`
is `error` (a success is `ok`); the caller-visible type stays on the HTTP
body. For a provider HTTP
refusal, `axond.upstream.status` is the original upstream code, even when the
gateway maps it to a different response status. `axond.upstream.message` holds
the provider's extracted error message, capped at 4 KiB on a UTF-8 boundary.
The outbound API key is removed before classification and truncation. These
diagnostics travel with traces and do not require OTLP log export. Transport
failures without a response have no upstream HTTP status.

Axond does not attach request bodies, authentication headers, or successful
response bodies to spans. A provider's error message may echo caller input;
trace access and retention should follow the deployment's diagnostic-data
policy. Diagnostics are span attributes, never metric labels.

Provider request-validation and context-window errors return HTTP 400 on both
buffered requests and stream-open failures. Upstream 401/403 credential refusals
remain HTTP 502 even though the provider parser labels them `invalid_request`;
these require an operator to correct provider access. Provider dependency and model
availability failures keep their gateway error classification; their original
HTTP status is available on the attempt span. Once a stream has opened, errors
remain in-band SSE events because its HTTP status has already been sent.

A streamed response outlives its server span: the span records where the stream
was routed before dispatch, and the final tokens/cost land on the metrics and
the usage record instead.

## Metrics

`axond.http.*` covers every HTTP request — including ones that never reach a
provider — with low-cardinality dimensions. `axond.request.*` /
`axond.upstream.*` are emitted from the single canonical usage record, so a
metric never reports a different value than the usage row it came from. They
count what the upstream actually did, which is also what the budget was charged:
a billing-grade request whose event could not be journaled is counted here and
refused to the caller, so `axond.request.count` can exceed the usage rows a
destination receives by exactly what the refusals in
`axond.usage.journal.appends` and `axond.usage.journal.lost` report.

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
| `axond.upstream.timeouts` | counter | `axond.target.provider`, `axond.target.model`, `axond.timeout`, `axond.timeout.bound` | Which phase stalled — `connect`, `response_headers`, `buffered_body`, `stream_idle`, or `overall` (nothing was dispatched) — and whether the `phase` bound or the remaining `walk_budget` ended the wait. |
| `axond.upstream.time_to_first_token` | histogram (ms) | `axond.target.provider`, `axond.target.model` | Provider TTFT measured at the first decoded stream event. |
| `axond.upstream.circuit_state` | gauge | `axond.target.provider`, `axond.target.model` | `0` closed, `1` half-open, `2` open. |
| `axond.usage.records_written` | counter | `axond.usage_sink` | Records a sink acknowledged. In billing-grade mode the delivery worker emits it, for records a destination accepted. |
| `axond.usage.records_dropped` | counter | `axond.usage_sink`, `axond.drop_reason` | Records discarded rather than delaying a request. `shutdown` means the termination flush could not write them. Billing-grade mode has no buffer to drop from: a failed write stays journaled, so watch `axond.usage.journal.lost` there instead. |
| `axond.usage.flushes` | counter | `axond.usage_sink`, `axond.flush_outcome` | Termination flushes of a buffered sink: `flushed`, `failed`, or `timeout`. |
| `axond.usage.journal.appends` | counter | `axond.usage_journal`, `axond.journal.outcome` | Billing-grade appends. Anything but `accepted` / `already_present` is a request refused or an event lost. |
| `axond.usage.index.appends` | counter | `axond.index.outcome` | Management usage-index events (`axond_store_usage`, what `GET .../usage` reads). `accepted` landed; the rest are best-effort losses of the summary index, not of billing: `saturated` (the bounded queue was full, dropped without waiting), `closed` (the index worker is not running), `failed` (the Store refused the batch), `timeout` (the batch write missed its deadline). See the migration note below. |
| `axond.usage.index.batches` | counter | `axond.index.outcome` | Store writes the index worker made, one transaction each; only `accepted`, `failed`, `timeout` occur. `appends / batches` is the achieved batch size. |
| `axond.usage.index.batch_size` | histogram | — | Events per index write. Buckets are powers of two up to the `4096` ceiling, so an unbatched deployment reads as all-ones. |
| `axond.usage.index.queue_age` | histogram (ms) | — | How long the oldest event of each index write waited in the queue before the write began. The queue-pressure signal: rising age with `saturated` drops means the Store cannot keep up at `[storage.usage_index]`'s batch size. |
| `axond.usage.index.queue.depth` | histogram | — | Occupied slots in the bounded usage-index queue at each accepted enqueue. Label-free so it keeps short-lived peaks between exports; a peak at `buffer_capacity` means the next record is dropped from the index, not from billing. |
| `axond.usage.index.queue.wait` | histogram (ms) | — | How long an enqueued record waited before the index worker took it. Grows when the Store is slow or contended by the request path — read it with `axond.store.acquire_wait`. |
| `axond.usage.journal.deliveries` | counter | `axond.usage_journal`, `axond.usage_journal.consumer`, `axond.journal.delivery` | Journaled events handed to their destinations. |
| `axond.usage.journal.depth` | gauge | `axond.usage_journal`, `axond.usage_journal.consumer` | Events awaiting delivery. Read against `axond.usage.journal.capacity`. |
| `axond.usage.journal.in_flight` | gauge | same | Events under an unexpired lease. |
| `axond.usage.journal.oldest_pending_age` | gauge (s) | same | How far behind delivery is; a depth alone does not say. |
| `axond.usage.journal.capacity` | gauge | `axond.usage_journal` | Configured `max_events`, so depth is readable as a fraction. |
| `axond.usage.journal.quarantined` | counter | `axond.usage_journal`, `axond.usage_journal.consumer`, `axond.journal.poison_reason` | Events set aside as poison: `malformed`, `rejected`, `attempts_exhausted`. |
| `axond.usage.journal.quarantined_events` | gauge | `axond.usage_journal`, `axond.usage_journal.consumer` | Quarantined events still retained, each waiting on a human. |
| `axond.usage.journal.undeliverable` | counter | `axond.usage_journal`, `axond.journal.reason` | Rows this build declined to deliver: `schema_ahead` (a newer build wrote it) or `corrupt`. |
| `axond.usage.journal.lost` | counter | `axond.usage_journal`, `axond.journal.loss_reason` | Events a billing-grade deployment gave up: served under `on_undurable = "serve"`, dropped for capacity, terminal, or refused after the caller had already hung up. The only data-loss counter of this mode. |
| `axond.shutdown.phase` | gauge | — | `0` serving, `1` draining (readiness fails, still admitting), `2` admission closed. The TypeScript process writes JSON `msg` `shutdown` with `phase` `requested`, `second_signal`, `admission_closed`, `signal_ignored`, or `deadline_expired`. When the settle share ends with a spawned charge or an admitted request still open, it also writes `phase` `spend_unsettled` with `settlements_queued`, `settlements_executing`, `settlements_reserved`, and `oldest_settlement_ms`. That line names the signal or the stage counts and omits the bind address, the store path, and the gateway key. |
| `axond.shutdown.rejected_requests` | counter | — | Requests refused with `503 draining` after admission closed. |
| `axond.shutdown.abandoned_requests` | counter | — | Requests still in flight when the shutdown deadline cut them. |
| `axond.shutdown.abandoned_settlements` | counter | — | Settlements still queued or executing when the shutdown settle share ran out. Spend this replica served and will never record; the shutdown log line breaks the leftovers down by stage. |
| `axond.shutdown.abandoned_index` | counter | — | Management usage-index events still queued when the graceful drain reported leftovers. In-memory only: they will not appear on `GET .../usage` after this replica exits. Not incremented when the worker did not report (unknown is a log line, not a zero), and leftovers are not also counted as `saturated`/`closed` on `axond.usage.index.appends`. Crash exit never drains and never increments this. |
| `axond.settlement.in_flight` | up-down counter | `axond.settlement.stage` | Settlement capacity held right now, by stage: `reserved` (admitted requests whose settlement has not been spawned), `queued` (spawned, waiting for an execution slot), `executing` (charging the Store). The sum is what `admission.max_pending_settlements` bounds; `executing` is what `max_in_flight_settlements` bounds. |
| `axond.settlement.queue_wait` | histogram (ms) | — | Time a spawned settlement waited for an execution slot. Sustained growth means the Store is slower than the charge rate. |
| `axond.settlement.oldest_pending_age` | gauge (ms) | — | Age of the oldest spawned settlement not yet finished. A rising age with a flat depth is a stalled Store; a rising depth with a flat age is a burst. |
| `axond.settlement.failures` | counter | `axond.settlement.reason` | Settlements that missed a bound: `queue_timeout` (outlived `settlement_queue_wait_ms` and never started — spend not recorded), `execution_timeout` (outlived `settlement_timeout_ms`; the slot stays occupied until non-cancellable Store work ends so charge and usage stay together), `panicked`, `cancelled` (the task was aborted or the runtime stopped under it), `refused` (background work that carried no admission reservation met a saturated replica). The TypeScript process also counts `charge_failed` when the Store write throws, and writes JSON `msg` `settlement_failure` with that reason. That line omits the driver text. None is retried, because a budget charge is not idempotent. |
| `axond.budget.capacity_denials` | counter | — | In-memory admissions denied because the ledger bound was exhausted. |
| `axond.budget.namespace_denials` | counter | — | Admissions denied by `namespace_limit_microdollars` rather than by the subject's own cap. Both answer `429`. |
| `axond.budget.retained_subjects` | gauge | — | In-memory ledgers retained after capacity-pressure pruning; watch against `max_subjects`. |
| `axond.store.acquire_wait` | histogram (ms) | `axond.store.backend`, `axond.store.operation` | Time a Store operation waited for a connection before any SQL ran: the dispatch slot (then the connection mutex) on `sqlite`, the pool permit on `postgres` plus a fresh connect when nothing idle was available. SQLite waiters queue on that slot rather than occupying one blocking thread each. Recorded for every outcome, including a saturated dispatch. SQLite `usage_summary` waits on a **separate** read-only lane, so a long fold does not occupy the inference slot. |
| `axond.store.query_duration` | histogram (ms) | same | Time the operation held the connection: the statement or transaction itself, after the wait. On SQLite this is captured inside the blocking closure, before the mutex is released, so runtime delay polling a finished `spawn_blocking` join is not counted as execution. Recorded for `ok` and `error`; a `saturated` operation never reached the connection and has no execution sample. |
| `axond.store.operations` | counter | `axond.store.backend`, `axond.store.operation`, `axond.store.outcome` | Store operations by outcome: `ok`, `error`, or `saturated` (Postgres pool permit or SQLite `spawn_blocking` slot not granted within its wait bound). The count behind the two histograms. |
| `axond.store.connections_opened` | counter | `axond.store.backend` | Successful Postgres sessions. A failed `connect()` does not increment. SQLite opens once and does not count. After idle retention matches the pool size, this should rise at cold start and after dead sessions, not on every traffic burst. |
| `axond.store.connections_reused` | counter | `axond.store.backend` | Postgres checkouts that took a healthy idle session instead of connecting. |
| `axond.store.connections_discarded` | counter | `axond.store.backend` | Postgres sessions dropped rather than returned idle: a closed backend at checkin, a closed client found in the idle list, a cancelled checkout, or a typed error that refuses reuse. A rise that tracks request bursts after idle retention matches the pool size is a reconnect storm, not expected drain. |
| `axond.store.pool.sessions` | gauge | `axond.store.backend`, `axond.store.pool.state` | Postgres occupancy right now: `live` is checked out, `idle` is retained. The sum is the session count the semaphore bounds (32 per replica). SQLite does not record this gauge. |
| `axond.admission.queue.depth` | histogram | — | Exact server-side queue depth observed when a request acquires a bounded queue slot. The label-free histogram retains short-lived peaks between export intervals; compare its maximum with `queue_capacity`. |
| `axond.admission.in_flight` | up-down counter | `axond.admission.resource` | Admission capacity held right now, by resource: `request`, `stream`, `tenant`, `queue`. Bounded label set — no tenant, subject, or request identity. |
| `axond.admission.rejections` | counter | `axond.admission.resource`, `axond.error.type` | Requests shed by admission control, by resource and stable error type. |
| `axond.catalog.refusals` | counter | `axond.catalog.reason` | Catalogue imports refused, by typed reason: `unreachable`, `denied`, `oversized`, `not_json`, `schema`, `id_mismatch`, `identifier`, `unknown_status`, `unknown_modality`, `price`, `unknown_tier_type`, `duplicate_tier`, `neutral_price`, `uncanonicalizable_text`, `ambiguous_model_key`, `content`, `unsupported_endpoint`, `unknown`. A refusal keeps the previous catalogue active, so nothing else moves when one happens. The JSON Pointer and message the refusal also carries are logged, never labelled. |
| `axond.catalog.active_age` | gauge (ms) | — | How long since the active catalogue was last confirmed current — admitted, or answered `304`. Absent, not zero, before a first import. |
| `axond.catalog.consecutive_refusals` | gauge | — | Imports refused in a row. Reset by any admitted or confirmed-unchanged import. |

**`axond.index.outcome` migration.** Before the usage index was batched, a full
index queue was counted as `timeout` although nothing had waited, and a missing
worker as `failed`. Both values keep their names and now carry only their literal
meaning: `timeout` is an elapsed write deadline, `failed` a Store error. Two new
values carry what used to be folded in: `saturated` (queue full) and `closed`
(worker not running). An alert that read `timeout` as "the index is overloaded"
should select `saturated` instead, or `outcome != "accepted"` for any loss.

Every instrument and label above is declared in a catalogue inside the binary,
and tests fail if the code builds an instrument or records a label the catalogue
does not declare. Labels are classified: closed vocabularies are enumerated, and
deployment-defined dimensions such as `axond.namespace` and the model dimensions
are legitimate only on the instruments listed above — they are refused as default
labels attached to everything. Tenant, subject, credential id, alias, revision id,
request id, and jti are refused as metric dimensions outright, so tenancy is never
readable from a scrape endpoint
([ADR 0031](./adr/0031-bounded-status-contract.md)).

### What to alert on

The table below is the reasoning; [`ops/observability/alerts/axond-alerts.yml`](../ops/observability/alerts/axond-alerts.yml)
is the same content as Prometheus rules, each carrying a `runbook_url` into the
[observability runbook](./operations/observability-runbook.md). A test validates
every expression in the shipped rules and dashboards against the catalogue, so a
renamed instrument cannot leave an alert silently matching nothing.

| Alert | Signal | Why |
| --- | --- | --- |
| Usage is being lost | `axond.usage.records_dropped` rate > 0, sustained | Spend data is gone and will not come back. Buffer or destination is undersized. |
| Spend lost at termination | `axond.usage.records_dropped{axond.drop_reason="shutdown"}` > 0, or `axond.usage.flushes{axond.flush_outcome!="flushed"}` > 0 | A replica exited before its buffered records landed. Raise `shutdown.flush_timeout_ms` (and the stopping timeout with it), or check the sink. |
| A billing-grade deployment lost usage | `axond.usage.journal.lost` > 0 | **Page.** A billable event is gone: either `on_undurable = "serve"` or `capacity_policy = "drop-oldest"` was exercised, or a terminal path could not journal. |
| Billing-grade requests are being refused | `axond.usage.journal.appends{axond.journal.outcome!="accepted",!="already_present"}` rising | Callers are getting `503 usage_not_durable`. Usually the outbox is full because delivery has stalled, not because appends are too fast. |
| The outbox is filling | `axond.usage.journal.depth` above ~half `axond.usage.journal.capacity`, or `oldest_pending_age` beyond minutes | Delivery is falling behind, and at capacity the default policy starts refusing requests. Check the destinations before raising `max_events`. |
| Usage events need reconciliation | `axond.usage.journal.quarantined_events` > 0 | Poison left the delivery path so it would stop blocking its ordering key; the rows are on disk waiting for a decision ([usage outbox](./operations/usage-outbox.md#recovery)). |
| Rollouts are cutting streams | `axond.shutdown.abandoned_requests` > 0 per rollout | Callers hold streams longer than `shutdown.deadline_ms`; their responses end mid-stream. |
| A replica is stuck draining | `axond.shutdown.phase` ≥ 1 for longer than `drain_grace_ms + deadline_ms + flush_timeout_ms` | The orchestrator sent `SIGTERM` but the process is not going away; expect a `SIGKILL` and lost buffered usage. |
| A target is out | `axond.upstream.circuit_state = 2`, sustained | Every request is failing over (or failing) for that target. |
| Budget denials | `axond.http.server.requests{status=429}` rising | Tenants are hitting their cap. |
| Load shedding | `axond.admission.rejections` rising | Split by `axond.admission.resource`: `request` means the replica's own ceiling (scale out or raise it), `tenant` means one namespace's ceiling (the tenant's own traffic), `queue` means queueing is absorbing more than a burst. |
| Admission saturation | `axond.admission.in_flight{axond.admission.resource="request"}` near `admission.max_in_flight` | Leading indicator of shedding; watch it before the rejections start. |
| Settlement falling behind | `axond.settlement.in_flight` summed over stages approaching `admission.max_pending_settlements`, or `axond.settlement.oldest_pending_age` beyond seconds | The Store is charging slower than the replica is serving. At the ceiling new requests are refused `503 settlement_capacity_exhausted` (`axond.admission.rejections{axond.admission.resource="settlement"}`) rather than an admitted charge being dropped. Fix the Store before raising the ceiling. |
| Spend not recorded by settlement | `axond.settlement.failures` > 0 | A settlement outlived its queue wait or execution timeout, panicked, or was cut by shutdown; its charge is not retried and this replica never recorded it. Reconcile against the usage sink for the window. |
| Rollouts are abandoning charges | `axond.shutdown.abandoned_settlements` > 0 per rollout | Settlements were still queued or executing when the settle share of `shutdown.flush_timeout_ms` ran out. Lengthen the flush budget, or look at why the Store was slow during the drain. |
| Rollouts are dropping the usage index | `axond.shutdown.abandoned_index` > 0 per rollout | Graceful shutdown ran out of flush budget before the Store usage-index worker wrote queued events. Billing is elsewhere; `GET .../usage` on this replica will be short. Lengthen `shutdown.flush_timeout_ms`, or look at Store contention (`axond.store.acquire_wait`). |
| Budget store down | `axond.http.server.requests{status=503}` rising | Fail-closed denial: fix the store, or the whole tenant is refused. |
| Budget capacity exhausted | `axond.budget.capacity_denials` > 0 | The replica is refusing unseen subjects; investigate subject churn and the in-memory bound. |
| Budget ledger pressure | `axond.budget.retained_subjects` near configured `max_subjects` | Leading indicator that the bound is approaching; watch it before capacity denials occur. |
| Namespace budget exhausted | `axond.budget.namespace_denials` > 0 | The whole namespace is out of budget, so *every* subject in it is being denied — not one noisy caller. Raise `namespace_limit_microdollars` or investigate what is spending. |
| TTFT regression | `axond.request.time_to_first_token` p95 | Provider degradation shows here before total latency moves. |
| Catalogue has stopped advancing | `axond.catalog.consecutive_refusals` >= 2 | Refusals have persisted across more than one import, so this is not one bad minute upstream: model metadata is frozen at whatever was last admitted, and `axond.catalog.active_age` is how far behind it now is. Runbook below. Not an availability alert — requests are unaffected. |
| Upstream stalls | `axond.upstream.timeouts` rising | Split by `axond.timeout`: `connect` is egress or DNS, `response_headers` is an overloaded provider, `stream_idle` is a half-dead connection, and `overall` means the failover budget was spent before the attempt was dispatched. Then split by `axond.timeout.bound`: `walk_budget` means `failover.overall_timeout_ms` is too tight for how slow the target became. |

### Runbook: refused catalogue imports

A refused import is durable by design: the last successfully imported catalogue
stays active, so nothing is served from a half-parsed document and no request
path changes. That also means the only evidence is the signals above — hence the
alert at two consecutive refusals rather than one.

The count is a property of the catalogue rather than of whatever drives it:
`LastKnownGoodCatalog::record_refresh` takes the whole refresh outcome, so a
fetch that never reached a parse still counts and a confirmed `304` still ends
the run, and the scheduler slice has no separate bookkeeping to get wrong. Every
refusal it counts is also handed back with its reason — on the error for a failed
refresh, and as `Refreshed::Refused` for the one refusal that produces no error —
so the counter split by reason can never fall behind the run gauge.

None of this is an availability incident. Catalogue freshness is not an
admission, entitlement, billing, readiness, or liveness dependency: a stale
catalogue degrades metadata quality (a new model or a changed published price is
not yet known), and observed prices are metadata that never activate billing on
their own ([ADR 0043](./adr/0043-catalogue-source-imports.md)). Do not fail a
replica out, roll back, or restart on this alert; a restart imports nothing new
and loses the active snapshot.

1. **Read the reason.** Split `axond.catalog.refusals` by `axond.catalog.reason`.
   `unreachable` / `denied` / `oversized` are the fetch: egress, a mirror, an
   auth-ing proxy, or a body past the ceiling. `unsolicited_unchanged` is the one
   reason with no error and no pointer behind it: the source answered "not
   modified" to a request that carried no validator for it to check against —
   nothing imported yet, content held that stated no `ETag`, or an
   unconditional refresh — which an intermediary answering `304` unconditionally
   will do. Look at the cache in front of the source, not at the payload.
   Everything else is the document itself — upstream published something this
   schema refuses.
2. **Read what is active.** The authenticated deployment-scope status response
   carries `catalogue.content_id` (the short digest of the content actually
   being served), `catalogue.active_age_ms`, `catalogue.consecutive_refusals`,
   `catalogue.last_refusal`, and `catalogue.last_diff` when a content-changing
   import has succeeded. `last_diff` contains only bounded counts for provider,
   model, offering, lifecycle, capability, metadata, and observed-price changes;
   it contains no model ids or price amounts. Tenant-scoped responses do not: a
   tenant sees only that the `catalogue` component is healthy or degraded.
3. **Find the location.** The refusal's log line carries the typed error the
   parser produced, and a JSON Pointer into the payload whenever the refusal was
   decided at one location: that pointer is the whole diagnosis for a `price`,
   `identifier`, `id_mismatch`, or modality/status refusal, and for a `schema`
   refusal decided inside a record — a field that went missing or changed type
   is named at the record or field it happened at, not by a byte offset. Only a
   `not_json` refusal, a `schema` refusal of the document root, and a `content`
   refusal name no single location; the message text is the lead there. The
   pointer is deliberately absent from metrics and from the status response,
   where it would be unbounded.
4. **Decide by age, not by the alert.** Age is when this gateway last confirmed
   the content current — an import or a `304`, not a retrieval time the document
   claims — so a freshly imported offline seed reads as fresh
   (`LastKnownGoodCatalog::admit_as_of` stamps that for imports which never came
   from a refresh, seeding at boot being the one that exists). `active_age_ms`
   against your own tolerance for stale model metadata is the actual decision.
   Hours are usually uninteresting; days mean pricing and capability facts are
   drifting from upstream.
5. **Fix the source, not the gateway.** Restore reachability, or pin/mirror a
   payload that parses. A refusal that reflects genuine upstream drift is a
   parser change, and the pointer from step 3 is what the change is written
   against.

## Usage records

One record per terminated request — including failures, cancellations, and
partial streams. With no `[[usage_sink]]` configured it is one JSON line on
stdout. Fields, versioning, and delivery guarantees are the published contract
in [`docs/usage-schema.md`](./usage-schema.md).

Records carry the credential's **label** (`credential_id`) and the gateway key's
**env-var name** (`subject`) — never a secret.

Delivery is telemetry-grade by default: a stalled destination drops records with
a count rather than delaying a request, so a missing record is possible and
visible. `[usage_journal] backend = "postgres"` opts into billing-grade delivery
instead — durable before the response, replayed until the destinations
acknowledge it — with its own metrics above and its own runbook in
[billing-grade usage outbox](./operations/usage-outbox.md).

## Failure modes

Every route always exists; unavailable behaviour answers with a typed error
rather than a bare 404 that would be indistinguishable from a wrong `base_url`.
Error bodies are `{"error": {"type": …, "message": …}}`.

| Status | `type` | What happened | What to do |
| --- | --- | --- | --- |
| `401` | `unauthorized` | No `Authorization: Bearer` / `x-api-key`, or the token is not in the key table. | Check the caller's key and that its `[[gateway_key]]` is declared and its env var set. There is no keyless mode. |
| `404` | `unknown_namespace` | Path namespace is missing or deleted. | `POST /api/v1/namespaces` or fix the URL. |
| `400` | `unknown_provider` / `model_unprefixed` | Request `model` is not `provider-id/model-id`. | Prefix with a configured `[[provider]] id`. `GET /ns/{ns}/v1/models` lists cached ids. |
| `400` | `unsupported_wire` | The alias's target (or one of its failover targets) does not speak this route's wire — e.g. an OpenAI-only alias on `/v1/messages`. Raised **before** anything is reserved or dispatched. | Fix the alias's targets; no route translates between wires. See the [compatibility contract](./compatibility.md). |
| `400` | `invalid_request`, `context_window_exceeded`, `bad_request` | The provider (or the gateway) rejected the request shape. | Caller-side fix; retrying will not help. |
| `429` | `budget_exceeded` | The namespace's active period is at or over cap (`spent >= limit`), or no budget row exists. In-flight requests are not reserved against remaining. | Raise `limit_microdollars` or wait. This is the tenant's own cap, not a provider rate limit. |
| `503` | `budget_unavailable` | The Store could not be reached and `[storage].on_unavailable = "deny"` (the default). The TypeScript process also writes JSON `msg` `budget_unavailable` with `stance` `deny`. `allow` writes `stance` `allow`, serves the request, and does not charge. The line omits the driver text. | Fix SQLite/Postgres. **Distinguish this from `429`:** `429` is the tenant over budget, `503` is *your* dependency down. |
| `503` | `draining` | The replica is terminating and has closed admission; `Retry-After: 0`. Expected during a rollout, on the requests that arrive after the readiness drain window. The TypeScript process writes JSON `msg` `shutdown` with `phase` `admission_closed` when admission closes, and `signal_ignored` for a later signal. That line omits the bind address and the gateway key. | Nothing on the gateway: the caller (or load balancer) should retry, and another replica should answer. Sustained volume means callers are not honoring readiness — check endpoint removal and `shutdown.drain_grace_ms`. |
| `503` | `all_provider_circuits_open` | Every target the request could consider has a tripped circuit. That is all of the alias's targets on every route except `/v1/responses`, which considers only its pinned first target — so a Responses request can raise this while the alias's later targets are healthy. | The upstreams are down or the thresholds are too tight; check `axond.upstream.circuit_state`. On `/v1/responses`, read it as *the first target* being down, not the whole alias, and do not alert on it as an alias-wide outage. |
| `502` | `no_credential` | The namespace has no credential for the resolved provider and no platform fallback. | Add a `[[credential]]`, or set `allow_platform_fallback` deliberately. |
| `502` | `upstream_transport`, `provider_dependency_failed`, `model_unavailable`, `invalid_stream` | The upstream failed after the failover walk was exhausted. | Check the provider's status and the attempt spans; `attempts` on the usage record says how hard the gateway tried. |
| `504` | `upstream_timeout` | A transport bound fired before a response could be served: connecting, waiting for headers, reading a buffered body, waiting for the next chunk of an open stream, or the walk's budget running out. | `axond.upstream.timeouts{axond.timeout}` and the attempt span's `axond.timeout` name the phase; `axond.timeout.bound` names the bound. Tune the matching `[transport]` bound, or `overall_timeout_ms` when the bound is `walk_budget`. |
| `502` | `upstream_body_too_large` | A buffered provider response exceeded `transport.max_response_bytes`, so it was refused instead of held in memory. | Raise `max_response_bytes` if the workload legitimately returns bodies that size; otherwise treat it as a misbehaving target. |
| `429` | `tenant_concurrency_exceeded` | The caller's namespace is at `admission.max_in_flight_per_tenant` on this replica. The caller's own concurrency is the cause, so it is a `429` rather than a `503`. | Raise the per-tenant ceiling, or have the caller lower its concurrency. Carries `Retry-After: 1`. |
| `503` | `gateway_overloaded`, `stream_capacity_exhausted` | The replica is at `admission.max_in_flight` (or `max_in_flight_streams`). Raised after authentication and before the budget check and the provider, so a shed request costs nothing. | Scale out, or raise the ceilings to what one process can actually hold. `axond.admission.in_flight` says which resource ran out. |
| `503` | `admission_queue_full`, `admission_queue_timeout` | Queueing is enabled and the queue is full, or a queued request outlived `admission.queue_wait_ms`. | Sustained shedding here means under-provisioning rather than burstiness; queueing only helps short bursts. |
| `503` | `admission_tenant_capacity_exhausted` | More distinct namespaces were in flight than `admission.max_tenants`, so the admission table itself is full. | Raise `max_tenants`. No `Retry-After` is sent: waiting will not change it. |
| `503` | `settlement_capacity_exhausted` | The replica is carrying `admission.max_pending_settlements` charges its Store has not yet settled, so it refuses to admit another rather than serve it with a charge nowhere to go. Raised at admission, before the budget check and the provider, so the refused request costs nothing. | The Store is slow or down: check `axond.settlement.oldest_pending_age` and the budget backend's own signals. Raising the ceiling only buys memory; the charge rate is the Store's. Carries `Retry-After: 1`. |
| `413` | `request_too_large`, `prompt_too_large` | The inbound body exceeded `admission.max_request_bytes` (refused by the router before buffering) or the estimated input exceeded `admission.max_prompt_tokens`. | Caller-side or content-policy fix, or raise the bound if the workload needs it. Neither message echoes the request. |
| `415` | `unsupported_media_type` | The request did not declare `content-type: application/json`. Unchanged in status from earlier releases; only the body is now the typed JSON envelope. | Caller-side fix: send a JSON content type. |
| `400` | `output_limit_exceeded` | The request asked for more output tokens than `admission.max_output_tokens`. Refused rather than clamped. | Lower the caller's output allowance or raise the ceiling. |
| `503` | `continuation_affinity_unavailable` | A request carrying `previous_response_id` could not use the alias's pinned first target or credential, and continuity forbids substituting another. | Restore the first target/credential; retry later. An *initial* Responses request in the same state reports the ordinary error above instead. |

`/v1/responses` records exactly one upstream attempt per request: it is pinned to
the alias's first target and first credential whether or not it continues a
stored response, so `attempts` is always `1` and no rotation lease appears
([ADR 0023](./adr/0023-openai-responses-passthrough.md)). A Responses request
failing while chat on the same alias succeeds is that pin, not a routing bug.

Mid-stream failures are different by construction. Native passthrough streams
and OpenAI-normalized streams that have already queued downstream bytes remain
terminal: the relay emits an SSE `error` event on the already-`200` response,
and the usage record settles as `partial` or `upstream_error`. A stream ended by
`admission.max_stream_duration_ms` or `admission.max_stream_bytes` arrives the
same way — an already-`200` response, an SSE `error` event typed
`upstream_stream_error` naming the bound, and a settled usage record — because
its first bytes were committed before the bound fired. Alert on that event's
type rather than on a status code. An
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
([ADR 0028](./adr/0028-transport-phase-bounds.md)).

Byte-faithful Native and Responses relays may forward provider extension bytes
after the semantic terminal event, but only for the fixed
`transport.stream_terminal_grace_ms`. Expiry closes the completed response
successfully and releases its request, stream, and caller capacity; it is not an
upstream timeout and does not increment `axond.upstream.timeouts`. The TypeScript
process writes JSON `msg` `terminal_remain` with `bound` `grace` and `grace_ms`
when that grace elapses while the socket is still open, and `bound` `duration`
when the total stream bound elapses first. A clean EOF writes neither line.
The line omits the endpoint.

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

