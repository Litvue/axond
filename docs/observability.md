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
  at all. This is a supported production posture, not a degraded one.
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

Three surfaces answer three different questions, and none of them substitutes for
another ([ADR 0031](./adr/0031-bounded-status-contract.md)):

| Surface | Authentication | Question it answers |
| --- | --- | --- |
| `GET /healthz` | none | *Is the process alive?* Answers `ok` throughout, including the shutdown drain. Restart it if this fails. |
| `GET /readyz` | none | *Should traffic be sent here?* `ready`, or `503 draining` once termination begins. Point the load balancer here. |
| `GET /admin/v1/status` | any gateway credential; a scoped token also needs the `status` capability | *Which dependencies is this replica talking to?* Cached component states with an observation age. Answers throughout the shutdown, including `closing`, and on a replica that refuses inference. Bounded by its own diagnostic ceilings — eight concurrent answers, seventy-two concurrent authentications — rather than by `admission.max_in_flight`. |

Neither `/healthz` nor `/readyz` observes a dependency. A store outage must not
remove healthy replicas from service, so dependency state lives only on the
authenticated surface — and that surface answers from a **cache**: a background
refresher observes each enabled component on its own cadence, and a read takes an
in-memory snapshot. A status request never probes a backend, never takes a budget
or rate-limit permit, and cannot slow inference down.

Read the age, not just the state. Each component reports `ok`, `degraded`,
`unavailable`, or `disabled` (nothing is configured for it) with the age of the
observation behind it; an observation older than the staleness budget reports
`degraded` with reason `stale`, because a replica serving a valid snapshot through
an observation outage is stale rather than down. A component that is enabled but
has never been observed reports `unavailable`, never `ok`.

Reasons come from a closed list — `unavailable`, `unreachable`, `timeout`,
`authentication_rejected`, `permission_denied`, `schema_incompatible`,
`payload_corrupt`, `validation_rejected`, `projection_rejected`,
`snapshot_rejected`, `pricing_rejected`, `clock_unsynchronised`,
`policy_rejected`, `secret_unresolved`, `stale`, `not_configured`, `draining`,
`capacity_exhausted`, `unknown` — and treat an unrecognised one as opaque, since
codes are added additively. There is deliberately no free-text field: connection
strings, tokens, raw backend errors, and rejected-revision details are logged for
the operator and cannot appear in a response. A caller without deployment-wide
authority additionally sees only the components its own traffic depends on,
reasons coarsened to `unavailable`, ages rounded to whole seconds, and no revision
summary.

A component reports `disabled` when this deployment has no such dependency — that
is the correct answer, not a degraded one — so a replica that configured nothing
durable reports `disabled` everywhere, observes nothing, and produces no
`axond.status.*` series at all.

The components a replica does observe are the ones its own configuration opened:
**the control plane** in `mode = "stateful"`, and the **budget**, **rate-limit**,
and **revocation** stores wherever those are backed by Redis or PostgreSQL rather
than by `none`/`in-memory`. Each is observed on the connection the administrative
or request path already uses, rather than a second pool of its own: a diagnostic
that probed a path no real request takes is how status reports `ok` throughout an
outage of the thing being asked about. The exception is PostgreSQL, whose
request-path client is serialised behind a mutex — a probe queued there would
delay inference to answer a status page, so it opens its own short-lived session
and runs `SELECT 1`.

A probe asks only for reachability: a `PING` or a `SELECT 1`, with no tenant, key,
or `jti` in it, never a `reserve`, an `acquire`, or a revocation lookup. A store
that answers and refuses (a rotated credential) is `degraded`, not `unavailable`,
which keeps the unreachability alert for an outage.

Each component is probed under its own backend's configured bounds — a probe that
gave up before the backend's own bounds elapsed would report an outage it had
caused — while the shared refresh cadence is the slowest enabled component's, and
a request-path store is never observed faster than the metric export interval that
reads it. Backends with no reachability seam yet (the secret store, the usage
sink, the catalogue, and provider credentials) stay `disabled` until the slice
that owns each one exposes one; neither the response shape nor the metric names
change when they do:

```bash
curl -sS -H "Authorization: Bearer $AXOND_KEY" http://localhost:8080/admin/v1/status
```

What to reach for, in order: `/readyz` says whether traffic belongs here,
`/admin/v1/status` says which dependency is impaired and how fresh that knowledge
is, and the [observability runbook](./operations/observability-runbook.md) says
what to do about it. The shipped dashboard and alert assets under
[`ops/observability/`](../ops/observability/) are the fleet-wide view of the same
signals.

## Traces

One `http.server.request` span per request, with one `axond.upstream.attempt`
child per upstream call — so an ordered-failover walk reads as N attempt spans
under one server span, the last carrying the status the caller saw. Each
attempt contains one `axond.credential.lease` child for every attempted or
parked credential.

| Span | Key attributes |
| --- | --- |
| `http.server.request` | `http.request.method`, `http.route`, `http.response.status_code`, `axond.request_id`, `axond.namespace`, `axond.subject`, `gen_ai.request.model`, `axond.target.*`, `axond.credential_source`, `axond.status`, `axond.retry_count`, `gen_ai.usage.*`, `axond.cost_microdollars`, `axond.latency_ms`, `axond.ttft_ms` |
| `axond.upstream.attempt` | `axond.attempt` (zero-based), `axond.target.provider`, `axond.target.model`, `axond.credential_source`, `axond.status`, `axond.latency_ms`, `axond.ttft_ms`, `axond.timeout` (which phase stalled, when one did), `axond.timeout.bound` (`phase` or `walk_budget`) |
| `axond.credential.lease` | `axond.credential.id`, `axond.credential_source`, `axond.credential.index`, `axond.status` (`served`, `rate_limited`, `error`, `parked`) |
| `axond.config.reload` | `axond.reload.trigger`, `axond.reload.outcome`, `axond.config.generation` |
| `axond.revision.converge` | `axond.revision.trigger` (`boot`, `polled`, `notified`, or `pricing-boundary`), outcome, active/desired revision, lag, and generation |

An inbound `traceparent` is **joined**, not replaced, and the context is
injected into the upstream request, so a caller's trace runs end to end. Spans
never carry credentials, prompts, or completions.

A streamed response outlives its server span: the span records where the stream
was routed before dispatch, and the final tokens/cost land on the metrics and
the usage record instead.

When request-path middleware is registered, the prompt estimate, request
ceilings, and budget reservation are computed from the body after the chain has
run. A captured inbound request may therefore be smaller than the input named
by a usage row: the row describes the body that reached the provider. For
stream cancellation or a provider response without authoritative input usage,
the relay's fallback input count is the same post-middleware estimate used for
the hold.

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
| `axond.upstream.circuit_state` | gauge | `axond.target.provider`, `axond.target.model` | `0` closed, `1` half-open, `2` open. |
| `axond.usage.records_written` | counter | `axond.usage_sink` | Records a sink acknowledged. In billing-grade mode the delivery worker emits it, for records a destination accepted. |
| `axond.usage.records_dropped` | counter | `axond.usage_sink`, `axond.drop_reason` | Records discarded rather than delaying a request. `shutdown` means the termination flush could not write them. Billing-grade mode has no buffer to drop from: a failed write stays journaled, so watch `axond.usage.journal.lost` there instead. |
| `axond.usage.flushes` | counter | `axond.usage_sink`, `axond.flush_outcome` | Termination flushes of a buffered sink: `flushed`, `failed`, or `timeout`. |
| `axond.usage.journal.appends` | counter | `axond.usage_journal`, `axond.journal.outcome` | Billing-grade appends. Anything but `accepted` / `already_present` is a request refused or an event lost. |
| `axond.usage.journal.deliveries` | counter | `axond.usage_journal`, `axond.usage_journal.consumer`, `axond.journal.delivery` | Journaled events handed to their destinations. |
| `axond.usage.journal.depth` | gauge | `axond.usage_journal`, `axond.usage_journal.consumer` | Events awaiting delivery. Read against `axond.usage.journal.capacity`. |
| `axond.usage.journal.in_flight` | gauge | same | Events under an unexpired lease. |
| `axond.usage.journal.oldest_pending_age` | gauge (s) | same | How far behind delivery is; a depth alone does not say. |
| `axond.usage.journal.capacity` | gauge | `axond.usage_journal` | Configured `max_events`, so depth is readable as a fraction. |
| `axond.usage.journal.quarantined` | counter | `axond.usage_journal`, `axond.usage_journal.consumer`, `axond.journal.poison_reason` | Events set aside as poison: `malformed`, `rejected`, `attempts_exhausted`. |
| `axond.usage.journal.quarantined_events` | gauge | `axond.usage_journal`, `axond.usage_journal.consumer` | Quarantined events still retained, each waiting on a human. |
| `axond.usage.journal.undeliverable` | counter | `axond.usage_journal`, `axond.journal.reason` | Rows this build declined to deliver: `schema_ahead` (a newer build wrote it) or `corrupt`. |
| `axond.usage.journal.lost` | counter | `axond.usage_journal`, `axond.journal.loss_reason` | Events a billing-grade deployment gave up: served under `on_undurable = "serve"`, dropped for capacity, terminal, or refused after the caller had already hung up. The only data-loss counter of this mode. |
| `axond.shutdown.phase` | gauge | — | `0` serving, `1` draining (readiness fails, still admitting), `2` admission closed. |
| `axond.shutdown.rejected_requests` | counter | — | Requests refused with `503 draining` after admission closed. |
| `axond.shutdown.abandoned_requests` | counter | — | Requests still in flight when the shutdown deadline cut them. |
| `axond.config.reloads` | counter | `axond.reload.trigger`, `axond.reload.outcome` | Reload attempts and whether they applied. |
| `axond.config.generation` | gauge | — | `0` at boot, `+1` per applied reload. |
| `axond.budget.capacity_denials` | counter | — | In-memory admissions denied because the ledger bound was exhausted. |
| `axond.budget.namespace_denials` | counter | — | Admissions denied by `namespace_limit_microdollars` rather than by the subject's own cap. Both answer `429`. |
| `axond.budget.retained_subjects` | gauge | — | In-memory ledgers retained after capacity-pressure pruning; watch against `max_subjects`. |
| `axond.middleware.capacity_wait` | histogram (ms) | — | Time content middleware waits for one of the bounded blocking-executor slots. Sustained growth means late synchronous invocations are retaining capacity. |
| `axond.middleware.capacity_timeouts` | counter | — | Requests whose middleware deadline expired while waiting for blocking capacity. Alert on any sustained increase alongside `middleware_unavailable` responses. |
| `axond.rate_limit.denials` | counter | — | Inbound concurrency admissions rejected. |
| `axond.rate_limit.capacity_denials` | counter | — | In-memory admissions rejected because the bounded subject map is full. |
| `axond.rate_limit.unavailable_denials` | counter | — | Redis rate-limit admissions denied because the store was unavailable. |
| `axond.policy.unenforceable_denials` | counter | `axond.policy.condition`, `axond.policy.store` | Admissions denied because this replica holds no policy it can enforce for the namespace: `ungoverned` (no published document governs it) or `layout` (the published cap disagrees with the key layout the store booted on). The store is healthy in both cases, so these are counted apart from the unavailable-denial counters; the explanatory log line is sampled, this count is not. `axond.policy.store` names the responsibility as well as the backend — `budget:redis`, `budget:postgres` or `rate_limit:redis` — because a namespace with neither a published spend cap nor a published concurrency ceiling is denied by two stores that are commonly the same Redis, and the two are fixed separately. |
| `axond.admission.in_flight` | up-down counter | `axond.admission.resource` | Admission capacity held right now, by resource: `request`, `stream`, `tenant`, `queue`, `diagnostic` (status reads being answered, ceiling eight), `diagnostic_auth` (status reads being authenticated, ceiling seventy-two, split forty-eight for minted tokens, sixteen for credentials that resolve in memory, and eight for callers presenting none — a separate dimension because one read holds one of each and the two ceilings differ). Bounded label set — no tenant, subject, or request identity. |
| `axond.admission.rejections` | counter | `axond.admission.resource`, `axond.error.type` | Requests shed by admission control, by resource and stable error type. |
| `axond.status.component_state` | gauge | `axond.status.component` | Last observed dependency state: `0` disabled, `1` ok, `2` degraded, `3` unavailable — a severity ladder, so `>= 2` is trouble and the stateless posture (`disabled` everywhere) sits below `ok` rather than above `unavailable`. Bounded label set — no tenant, subject, or credential identity. |
| `axond.status.observation_age` | gauge (ms) | `axond.status.component` | Age of the cached observation behind that state; a rising age means the refresher, not the dependency, is the problem. |
| `axond.status.refreshes` | counter | `axond.status.component`, `axond.status.outcome` | Background refresh attempts and how they ended. |
| `axond.catalog.refusals` | counter | `axond.catalog.reason` | Catalogue imports refused, by typed reason: `unreachable`, `denied`, `oversized`, `not_json`, `schema`, `id_mismatch`, `identifier`, `unknown_status`, `unknown_modality`, `price`, `unknown_tier_type`, `duplicate_tier`, `neutral_price`, `uncanonicalizable_text`, `ambiguous_model_key`, `content`, `unsupported_endpoint`, `unknown`. A refusal keeps the previous catalogue active, so nothing else moves when one happens. The JSON Pointer and message the refusal also carries are logged, never labelled. |
| `axond.catalog.active_age` | gauge (ms) | — | How long since the active catalogue was last confirmed current — admitted, or answered `304`. Absent, not zero, before a first import. |
| `axond.catalog.consecutive_refusals` | gauge | — | Imports refused in a row. Reset by any admitted or confirmed-unchanged import. |

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
| Inbound concurrency denials | `axond.rate_limit.denials` rising | Authenticated callers are reaching their per-replica in-flight limit. |
| Load shedding | `axond.admission.rejections` rising | Split by `axond.admission.resource`: `request` means the replica's own ceiling (scale out or raise it), `tenant` means one namespace's ceiling (the tenant's own traffic), `queue` means queueing is absorbing more than a burst. |
| Admission saturation | `axond.admission.in_flight{axond.admission.resource="request"}` near `admission.max_in_flight` | Leading indicator of shedding; watch it before the rejections start. |
| Budget store down | `axond.http.server.requests{status=503}` rising | Fail-closed denial: fix the store, or the whole tenant is refused. |
| Config drift across the fleet | `axond.config.generation` differs between replicas | A replica missed a reload and is serving stale routing or keys. |
| Rejected reloads | `axond.config.reloads{outcome="rejected"}` > 0 | Someone edited the config into an invalid state; the old one is still serving. |
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
| `404` | `unknown_model` | The alias is not configured. | Add a `[[model]]`, or fix the caller. `/v1/models` lists the aliases the caller can invoke. |
| `400` | `unsupported_wire` | The alias's target (or one of its failover targets) does not speak this route's wire — e.g. an OpenAI-only alias on `/v1/messages`. Raised **before** anything is reserved or dispatched. | Fix the alias's targets; no route translates between wires. See the [compatibility contract](./compatibility.md). |
| `400` | `invalid_request`, `context_window_exceeded`, `bad_request` | The provider (or the gateway) rejected the request shape. | Caller-side fix; retrying will not help. |
| `400` | `middleware_refused` | Request middleware rejected the request as invalid before provider dispatch. The bounded message never echoes request content or middleware diagnostics. | Correct the request; retrying the same body will not help. |
| `403` | `middleware_refused` | A content-policy middleware guardrail denied the authenticated request before provider dispatch. | Check the policy selected for the caller's namespace; changing provider health or retrying unchanged input will not help. |
| `429` | `budget_exceeded` | The `(namespace, subject)` cap is spent — settled spend plus live holds leaves no room. With `namespace_limit_microdollars` set, the same code and body also cover the namespace-wide cap being spent; `axond.budget.namespace_denials` is what tells them apart. | Raise `limit_microdollars` (or `namespace_limit_microdollars`) or wait. This is the tenant's own cap, not a provider rate limit. |
| `503` | `budget_unavailable` | The budget store could not be reached and `on_unavailable = "deny"` (the default). | Fix Redis/Postgres. **Distinguish this from `429`:** `429` is the tenant over budget, `503` is *your* dependency down. |
| `503` | `rate_limit_unavailable` | The Redis rate-limit store could not be reached and `on_unavailable = "deny"` (the default). | Fix Redis or deliberately choose `on_unavailable = "allow"`. |
| `503` | `middleware_unavailable` | A fail-closed content middleware invocation failed, exceeded its end-to-end deadline, or could not acquire bounded blocking capacity before that deadline. | Check middleware failure logs plus `axond.middleware.capacity_wait` and `axond.middleware.capacity_timeouts`; restore the implementation or capacity before retrying. |
| `503` | `model_not_priced` | Every target the alias could route to is bound to a catalogue offering the serving snapshot has no **approved** price for — the book is a draft, or it does not price the offering. On a route that pins its destination (`/v1/responses`), the pinned target alone decides this: an unpriced pin is refused this way even where a later target of the alias is chargeable. Raised before admission or a rate-limit permit is spent, so a refused request costs nothing. The alias stays listed by `/v1/models`: it is discoverable but not chargeable. | Approve a price book covering the offering, or drop the target's `catalog` binding to charge the configured `price` again. The response body carries a stable, redacted reason ("no price is in force for this model") and never names the price book or its approval state — those are control-plane facts, and they go to the gateway log instead, on the `alias has no approved price` warning ([ADR 0056](./adr/0056-request-path-pricing.md)). |
| `503` | `draining` | The replica is terminating and has closed admission; `Retry-After: 0`. Expected during a rollout, on the requests that arrive after the readiness drain window. | Nothing on the gateway: the caller (or load balancer) should retry, and another replica should answer. Sustained volume means callers are not honoring readiness — check endpoint removal and `shutdown.drain_grace_ms`. |
| `503` | `all_provider_circuits_open` | Every target the request could consider has a tripped circuit. That is all of the alias's targets on every route except `/v1/responses`, which considers only its pinned first target — so a Responses request can raise this while the alias's later targets are healthy. | The upstreams are down or the thresholds are too tight; check `axond.upstream.circuit_state`. On `/v1/responses`, read it as *the first target* being down, not the whole alias, and do not alert on it as an alias-wide outage. |
| `502` | `no_credential` | The namespace has no credential for the resolved provider and no platform fallback. | Add a `[[credential]]`, or set `allow_platform_fallback` deliberately. |
| `502` | `upstream_transport`, `provider_dependency_failed`, `model_unavailable`, `invalid_stream` | The upstream failed after the failover walk was exhausted. | Check the provider's status and the attempt spans; `attempts` on the usage record says how hard the gateway tried. |
| `504` | `upstream_timeout` | A transport bound fired before a response could be served: connecting, waiting for headers, reading a buffered body, waiting for the next chunk of an open stream, or the walk's budget running out. | `axond.upstream.timeouts{axond.timeout}` and the attempt span's `axond.timeout` name the phase; `axond.timeout.bound` names the bound. Tune the matching `[transport]` bound, or `overall_timeout_ms` when the bound is `walk_budget`. |
| `502` | `upstream_body_too_large` | A buffered provider response exceeded `transport.max_response_bytes`, so it was refused instead of held in memory. | Raise `max_response_bytes` if the workload legitimately returns bodies that size; otherwise treat it as a misbehaving target. |
| `429` | `tenant_concurrency_exceeded` | The caller's namespace is at `admission.max_in_flight_per_tenant` on this replica. The caller's own concurrency is the cause, so it is a `429` rather than a `503`. | Raise the per-tenant ceiling, or have the caller lower its concurrency. Carries `Retry-After: 1`. |
| `503` | `gateway_overloaded`, `stream_capacity_exhausted` | The replica is at `admission.max_in_flight` (or `max_in_flight_streams`). Raised after authentication and before the rate-limit store, the budget reservation, and the provider, so a shed request costs nothing. | Scale out, or raise the ceilings to what one process can actually hold. `axond.admission.in_flight` says which resource ran out. |
| `503` | `admission_queue_full`, `admission_queue_timeout` | Queueing is enabled and the queue is full, or a queued request outlived `admission.queue_wait_ms`. | Sustained shedding here means under-provisioning rather than burstiness; queueing only helps short bursts. |
| `503` | `diagnostic_concurrency_exceeded` | Eight diagnostic reads (`GET /admin/v1/status`) were already being answered on this replica, or the share of the seventy-two pre-authentication permits its credential's shape may hold was already full — forty-eight for minted tokens, sixteen for credentials that resolve in memory, eight for callers presenting none, so neither a slow revocation store nor a credential-less flood can refuse a static key. That second ceiling sits outside authentication, which the first cannot bound without letting anonymous callers hold it closed. A fixed ceiling of its own, separate from `max_in_flight`: served traffic at its ceiling never makes the replica unanswerable, and polling the diagnostic never consumes served capacity. | Poll less often. It is not configurable, and a diagnostic answers from memory, so eight concurrent is a bound on abuse rather than a capacity dial. Carries `Retry-After: 1`. |
| `503` | `admission_tenant_capacity_exhausted` | More distinct namespaces were in flight than `admission.max_tenants`, so the admission table itself is full. | Raise `max_tenants`. No `Retry-After` is sent: waiting will not change it. |
| `413` | `request_too_large`, `prompt_too_large` | The body exceeded `admission.max_request_bytes` (refused by the router before it was buffered), or the estimated input exceeded `admission.max_prompt_tokens`. | Caller-side fix, or raise the bound if the workload needs it. Neither message echoes the request. |
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

The applied reload log also carries `catalog_changed` and
`restart_required`. When `catalog_changed` is `true`, the
`[catalog]` edit was validated but the boot-owned importer, and the serving
snapshot's catalogue settings, remain unchanged; the warning immediately after
the log line tells the operator to restart.
