# Observability runbook

The first-response page for a production incident. Each failure mode below names
the **signal** that shows it, the **alert** that fires on it, and the **first
response** — including, where it matters, what *not* to do.

[`docs/observability.md`](../observability.md) is the reference for what axond
emits; this page is what to do when one of those signals moves. The shipped
dashboards and alert rules under [`ops/observability/`](../../ops/observability/)
are the same signals as assets you can import, and every rule's `runbook_url`
points at a section of this page.

Three things to hold onto before reading on:

- **A dependency outage is not a fleet outage.** `/readyz` reports lifecycle
  drain state and nothing else, so a Store or provider
  outage never removes healthy replicas from service. If your orchestrator's
  readiness probe is doing that, the probe is wrong, not the gateway.
- **Restarting a replica is safe and rarely the fix.** A replica holds nothing
  durable outside the Store, but a restart clears circuit state and credential
  health, so it destroys the evidence for whatever
  you were about to diagnose.

## Where to look, in order

1. `axond_http_server_requests` and `axond_http_server_duration` — is the caller
   experience actually affected?
2. The failure mode below that matches, for the response.

## Failure modes

### Usage records are being lost

**Signal.** `axond_usage_records_dropped` increasing, split by
`axond_drop_reason` (`buffer_full`, `sink_error`, `shutdown`) and
`axond_usage_sink`.

**Alert.** `AxondUsageRecordsDropped`.

**First response.** Dropped records are spend and audit data that no retry will
recover, so treat a sustained rate as data loss rather than as a latency
symptom. `sink_error` is the sink itself; `buffer_full` means the sink cannot
keep up with request volume; `shutdown` means the termination flush could not
write what it held — check `axond_usage_flushes{axond_flush_outcome="timeout"}`
and give the drain longer. Requests are deliberately never delayed to keep the
sink honest, so this signal is the only thing that reports the loss.

### A billing-grade deployment is losing usage

**Signal.** `axond_usage_journal_lost` increasing, split by
`axond_journal_loss_reason`.

**Alert.** `AxondUsageJournalLoss`.

**First response.** Page. In billing-grade mode a served request is only reported
as successful once its event is in the outbox, so this counter is the one signal
that a billable fact exists nowhere. The reason says which trade was taken:
`at_capacity`, `backend`, `conflict`, and `invalid_event` are appends that failed
where the caller could not be refused — `on_undurable = "serve"`, a terminated
request, or a caller that hung up before its refusal could be delivered — and
`capacity_drop` is `capacity_policy = "drop-oldest"` discarding the oldest
undelivered event to stay inside `max_events`. Reconcile from the destination,
then fix what was full or unreachable; the sections below are the two causes.

### The usage outbox is filling or refusing

**Signal.** `axond_usage_journal_oldest_pending_age` climbing,
`axond_usage_journal_depth` approaching `axond_usage_journal_capacity`, or
`axond_usage_journal_appends` with an outcome other than `accepted` and
`already_present`.

**Alert.** `AxondUsageJournalBacklogAging`, `AxondUsageJournalRefusingAppends`.

**First response.** A backlog is not loss: the events are durable and the next
process claims them. What it becomes at `max_events` is refusal — callers get
`503 usage_not_durable` — so treat an ageing backlog as the warning for that.
Almost always the destination is stalled rather than the appends being too fast,
so check the sinks the delivery worker writes to and
`axond_usage_journal_deliveries{axond_journal_delivery="failed"}` before raising
`max_events`. A `backend` append outcome is the outbox database itself, not
capacity: the request path and the worker share it.
Full procedure: [usage outbox](./usage-outbox.md#recovery).

### Usage events are quarantined

**Signal.** `axond_usage_journal_quarantined_events` above zero, with
`axond_usage_journal_quarantined` naming the reason.

**Alert.** `AxondUsageJournalQuarantined`.

**First response.** Quarantine is deliberate: an event the destination refuses on
its own account, or one this build cannot decode, is set aside so it stops
blocking its ordering key and its siblings keep flowing. Nothing retries it
again, and retention will not prune it, so it holds part of `max_events` until
somebody decides what it is worth. The rows are on disk with their reason and
their `request_id`; reconcile them and delete the event row, following
[usage outbox](./usage-outbox.md#recovery) — deleting the delivery row alone
leaves the event unprunable.

### A provider target is out

**Signal.** `axond_upstream_circuit_state == 2` (open) for a
`axond_target_provider`/`axond_target_model` pair, with
`axond_upstream_errors` and `axond_request_count{axond_status="upstream_error"}`
rising on the same pair.

**Alert.** `AxondProviderCircuitOpen`.

**First response.** Requests to that target are failing over where the route
allows it — every route except `/v1/responses`, which is pinned to its alias's
first target. Check the provider's own status page, then whether the aliases that
name it have a healthy failover target. A single open circuit with flat overall
error rate is failover doing its job and is not a caller-visible incident.

**Do not** treat this as a fleet problem: provider outages must not drain
replicas, and a `503 all_provider_circuits_open` on `/v1/responses` means *the
pinned first target*, not the whole alias.

### Upstream requests are timing out

**Signal.** `axond_upstream_timeouts` split by `axond_timeout` (`connect`,
`response_headers`, `buffered_body`, `stream_idle`, `overall`) and
`axond_timeout_bound` (`phase`, `walk_budget`).

**Alert.** `AxondUpstreamTimeouts`.

**First response.** The phase names the cause: `connect` is egress or DNS,
`response_headers` is an overloaded provider, `stream_idle` is a half-dead
connection, and `overall` means the failover budget was spent before an attempt
was even dispatched. `axond_timeout_bound="walk_budget"` means
`failover.overall_timeout_ms` is too tight for how slow the target has become —
tune that rather than the phase bound.

### Served errors are elevated

**Signal.** The `5xx` share of `axond_http_server_requests`, with
`axond_http_server_duration` for the latency side, split by `http_route`.

**Alert.** `AxondServedErrorRateHigh`.

**First response.** This is the caller-experience signal and the one to page on;
every other mode on this page is a cause. Split by `http_route` and status, then
follow the matching mode: `502`/`504` are upstream, `503` is a fail-closed
dependency or admission, and `429` is a budget or concurrency ceiling rather
than an error to fix on the gateway.

### The replica is shedding load

**Signal.** `axond_admission_rejections` split by `axond_admission_resource`
(`request`, `stream`, `tenant`, `queue`, `settlement`) and
`axond_error_type`, with
`axond_admission_in_flight` as the leading indicator.

**Alert.** `AxondAdmissionShedding`, `AxondAdmissionSaturated`.

**First response.** `request` is the replica's own ceiling — scale out, or raise
`admission.max_in_flight` to what one process can actually hold. `tenant` is one
namespace's ceiling and is the caller's own concurrency. Sustained `queue`
rejections mean under-provisioning rather than burstiness: queueing only absorbs
short bursts. Shed requests are refused before the budget check and the provider, so
shedding costs nothing upstream.

### A replica is stuck draining

**Signal.** `axond_shutdown_phase >= 1` for longer than
`shutdown.drain_grace_ms` plus the request budget, with
`axond_shutdown_rejected_requests` and `axond_shutdown_abandoned_requests`
increasing.

**Alert.** `AxondReplicaStuckDraining`.

**First response.** A replica in phase `1` fails readiness while still admitting,
and phase `2` has closed admission and answers `503 draining`. Persisting there
means something is holding requests open — usually a long stream — or the
orchestrator is not removing the endpoint. Sustained
`axond_shutdown_rejected_requests` volume means callers are not honouring
readiness; abandoned requests mean the deadline cut work, so lengthen the drain
or shorten `max_stream_duration_ms`. A rising `axond_shutdown_abandoned_settlements`
is a different cut: the requests ended, but their charges were still queued or
executing against the Store when the settle share of `flush_timeout_ms` ran out,
and that spend was never recorded. The shutdown log line breaks the leftovers
down by stage (`settlements_queued`, `settlements_executing`,
`settlements_reserved`) and names the oldest one's age. A rising
`axond_shutdown_abandoned_index` is a third cut: the Store usage-index worker
still had queued events when the flush budget ended, so `GET .../usage` on this
replica is short. Billing is not this counter; crash exit never increments it.



## Bounded drill-down

Dashboards and alerts drill down along four dimensions and no others:

| Dimension | Label | Bound |
| --- | --- | --- |
| Tenant | `axond_namespace` | The namespaces you declare |
| Alias | `gen_ai_request_model` | The `[[model]]` aliases you declare |
| Provider | `axond_target_provider` | The `[[provider]]` ids you declare |
| Target model | `axond_target_model` | The target models you declare |

Those four are *configured* cardinality: they grow with your configuration, which
is why they are legitimate on the request-derived instruments and refused as
default labels attached to everything.

Everything finer is deliberately unavailable from metrics. Subject, credential
id, request id, and anything secret-shaped is refused as a metric dimension
outright, so no scrape endpoint reveals per-request identity. Per-request attribution
lives on the usage record and on spans, which are per-event rather than
multiplied into stored series — drill down to a *tenant* on a dashboard, then
switch to usage records or traces for a *request*.

Two conventions follow from that, and the shipped assets obey both:

- **Dashboard variables are label queries, never free text.** A drill-down
  variable is populated from `label_values(...)` over one of the four labels
  above, so a dashboard cannot ask a question the metrics cannot answer.
- **Alerts group by bounded labels only.** An alert that grouped by an unbounded
  dimension would multiply its own series, so rules aggregate to the component,
  the target, or the resource.

## The shipped assets

| Asset | What it is |
| --- | --- |
| [`ops/observability/dashboards/axond-fleet.json`](../../ops/observability/dashboards/axond-fleet.json) | Grafana dashboard: served traffic, providers, capacity, and usage delivery across the fleet |
| [`ops/observability/dashboards/axond-tenancy.json`](../../ops/observability/dashboards/axond-tenancy.json) | Grafana dashboard: per-namespace, per-alias, per-target volume, latency, spend, and outcome mix |
| [`ops/observability/alerts/axond-alerts.yml`](../../ops/observability/alerts/axond-alerts.yml) | Prometheus rule group, one rule per failure mode above, each carrying a `runbook_url` into this page |
| [`ops/observability/otel-collector.yaml`](../../ops/observability/otel-collector.yaml) | The collector pipeline the assets assume: OTLP in, Prometheus out |

Both dashboards import with a `DS_PROMETHEUS` datasource input and no other
editing, and neither hard-codes a datasource uid.

**The metric names in these assets are checked against the binary's canonical
catalogue.** A rule or panel referencing a metric axond does not emit, a label an
instrument does not declare, or a closed-vocabulary value that is not in the
vocabulary fails `cargo test`, so an asset cannot drift away from what the
gateway actually exports. The same gate checks that every `runbook_url` anchor in
the rules resolves to a section of this page, and that every failure mode on this
page has at least one rule.

### The name translation these assets assume

axond exports OTLP only, so the Prometheus-side names come from your collector.
The assets assume the exporter is configured with `add_metric_suffixes: false`,
which makes the translation exactly:

| OTLP | Prometheus |
| --- | --- |
| `axond.request.count` | `axond_request_count` |
| `axond.request.duration` (histogram) | `axond_request_duration_bucket`, `_sum`, `_count` |
| `axond.namespace` (label) | `axond_namespace` |

Dots become underscores and nothing else is appended: no `_total` on counters and
no unit suffix. With the exporter's default `add_metric_suffixes: true` you get
`axond_request_count_total` and `axond_request_duration_milliseconds_bucket`
instead, and the shipped assets will match nothing — set the flag as
[the shipped pipeline](../../ops/observability/otel-collector.yaml) does, or
rewrite the queries once on import.

## Related

- [Observability reference](../observability.md) — every span, metric, and typed
  error.
- [Troubleshooting](./troubleshooting.md) — symptom and typed-error decision
  tree for a single failing request.
- [Upgrades and rollback](./upgrades.md) — mixed-version rules and schema
  ordering.
