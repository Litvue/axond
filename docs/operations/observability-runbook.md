# Observability runbook

The first-response page for a production incident. Each failure mode below names
the **signal** that shows it, the **alert** that fires on it, and the **first
response** — including, where it matters, what *not* to do.

[`docs/observability.md`](../observability.md) is the reference for what axond
emits; this page is what to do when one of those signals moves. The shipped
dashboards and alert rules under [`ops/observability/`](../../ops/observability/)
are the same signals as assets you can import, and every rule's `runbook_url`
points at a section of this page.

**What is live today.** Two families are not produced by any release yet, because
nothing in `serve` constructs the loop that produces them.

* **Status.** No status refresher is constructed, so
  `axond_status_component_state`, `axond_status_observation_age`, and
  `axond_status_refreshes` are absent on every deployment — stateless or not —
  and `GET /admin/v1/status` answers with every component `disabled`.
* **Revision convergence.** No reconciler is constructed either (the
  `convergence` module is contract-only until a projection from resource bodies
  to a servable config lands), so every `axond_revision_*` series — `lag`,
  `converged`, `consecutive_failures`, `rejections`, `attempts`,
  `last_known_good`, `convergence_duration` — is absent too, and the `revision`
  summary in an operator's `GET /admin/v1/status` is `null` on every replica.
  When the reconciler lands it must hand its own status handle to both the
  status page and the administrative surface, or one replica would answer two
  convergence stories.

Eight shipped rules are therefore inert until those slices land:
`AxondDependencyImpaired`, `AxondStatusObservationsStale`,
`AxondStatusRefreshesFailing`, `AxondStatusRefresherStalled`, and
`AxondControlPlaneUnreachable` on the status side; `AxondRevisionLagAboveTarget`,
`AxondRevisionRejectionsSustained`, and `AxondRevisionConvergenceSplit` on the
convergence side. So are the *Dependency state*, *Observation age*, *Refresh
outcomes*, *Revision lag*, *Revision rejections*, *Convergence attempts*, and
*Convergence duration* panels on the fleet dashboard. Read the two convergence
failure modes below as the contract they will report against, not as coverage you
have now.

`AxondFleetRevisionSplit` is the one rule in the convergence group that works
today, because `axond_config_generation` comes from the file/environment reload
path rather than from a reconciler — but that gauge is first recorded on a
replica's first reload, so a fleet that has never reloaded emits no series and
the rule stays silent until one does.

The stall rule is inert rather than perpetually firing on purpose: it asks for an
absent refresh series **and** a status gauge that has existed within the last six
hours, so "never wired" is silent and only "was observing, then stopped" pages.
Nothing here needs to be disabled at import. Everything else — served traffic,
latency, config reloads, providers, capacity, usage delivery, lifecycle — is live
now.

So do not read a flat dependency panel as a healthy dependency: until that
wiring lands, use `axond_config_reloads`, upstream health
(`axond_upstream_circuit_state`, `axond_upstream_timeouts`), and the fail-closed
denial counters (`axond_rate_limit_unavailable_denials`,
`axond_revocation_unavailable_denials`, `axond_budget_capacity_denials`) as the
evidence that a dependency is impaired. Importing the assets now keeps them
versioned with your deployment and makes the drift gate meaningful; see
[stateful backends](../deployment/stateful-backends.md) for which components a
deployment enables.

Three things to hold onto before reading on:

- **A dependency outage is not a fleet outage.** `/readyz` reports lifecycle
  drain state and nothing else, so a control-plane, budget-store, or provider
  outage never removes healthy replicas from service. If your orchestrator's
  readiness probe is doing that, the probe is wrong, not the gateway.
- **Read the observation age, not just the state.** Dependency state is a
  *cached* observation. A component reporting `degraded`/`stale` means the
  refresher has not learned anything recently — which may be a broken refresher
  rather than a broken dependency.
- **Restarting a replica is safe and rarely the fix.** A stateless replica holds
  no state worth preserving, but a restart clears in-memory budget ledgers,
  circuit state, and the status cache, so it destroys the evidence for whatever
  you were about to diagnose.

## Where to look, in order

1. `axond_http_server_requests` and `axond_http_server_duration` — is the caller
   experience actually affected?
2. `GET /admin/v1/status` on an affected replica — which dependency is impaired,
   and how fresh is that knowledge?
3. The failure mode below that matches, for the response.

```bash
curl -sS -H "Authorization: Bearer $AXOND_KEY" \
  http://replica.internal:8080/admin/v1/status | jq
```

This answers throughout a shutdown — it is a diagnostic rather than served work,
so admission closing does not close it — and on a replica that refuses inference
because it cannot compile a revision yet, which is the one you are most likely to
be asking about. The caller's authority decides the
answer: an operator's scope-less static
`[[gateway_key]]` in the default namespace sees every component, exact ages, and
the revision summary, while any other caller sees only request-path components
with reasons coarsened to `unavailable`. Use an operator key when triaging, and
do not add a query parameter hoping to widen the view — there is none.

Use a *static* operator key rather than a minted token, and not only for the
wider view. The handler reads a cache, but authenticating a minted token means
checking its `jti` against the revocation store, which fails closed: during a
revocation-store outage a minted token gets `503 revocation_unavailable` from
every route including this one, while a static key — which has no `jti` to check
— still answers.

## Failure modes

### A dependency is impaired

**Signal.** `axond_status_component_state >= 2` (`0` disabled, `1` ok, `2`
degraded, `3` unavailable), split by `axond_status_component`. `GET
/admin/v1/status` names the same component with a reason code.

**Alert.** `AxondDependencyImpaired`.

**First response.** Read the reason: `unreachable` and `timeout` are the network
or the backend, `authentication_rejected` and `permission_denied` are the
credential the gateway presents, `schema_incompatible` means the deployed binary
and the store's schema disagree (see
[upgrades](./upgrades.md)), and `secret_unresolved` means the secret store
answered but the material was not there. Fix the named dependency; the gateway
recovers on its own once the next observation succeeds.

**Do not** remove the replica from service. Which components even matter depends
on the deployment: a stateless replica reports every component `disabled` and
that is the correct answer, not an incident.

### Dependency observations have gone stale

**Signal.** `axond_status_refreshes` no longer being recorded at all,
`axond_status_refreshes{axond_status_outcome="failed"}` increasing while the
component's state does not move, or `axond_status_observation_age` past the
staleness budget.

**Alert.** `AxondStatusRefresherStalled`, `AxondStatusRefreshesFailing`,
`AxondStatusObservationsStale`.

**First response.** Read the three signals as different failures, because the age
gauge alone cannot tell them apart. Every round publishes an observation for
every probe — an abandoned probe publishes a synthetic `timeout` observation too
— and each publish restamps the age, so:

- **the refresher stopped.** Nothing is published, so the age *freezes* at a
  small value rather than climbing, and the refresh series simply stops. That is
  what `AxondStatusRefresherStalled` watches for. Everything the status surface
  reports is now as old as the stall, including the `ok` components. (A replica
  that never wired a refresher emits no status instruments at all, and the rule's
  second arm keeps it silent there rather than paging for a stall that never
  happened.)
- **probes keep failing.** Refreshes are recorded with
  `axond_status_outcome="failed"`, the state is honest, and the dependency — not
  the replica — is the thing to fix.
- **rounds are slower than the budget.** The age exceeds the budget while still
  being republished, which means a round takes longer than the budget allows —
  the probe timeout or the refresh interval is above the staleness budget. Those
  three are fields of the registry's internal settings, not configuration: there
  is no `[status]` section to edit, so this is a report for whoever wires the
  refresher rather than a knob you can turn. What you can establish from here is
  whether the replica is saturated (`axond_admission_in_flight`) or the probes
  are timing out (`axond_status_outcome="failed"`).

A stale observation is deliberately reported as `degraded`/`stale` rather than as
`ok` or as a readiness failure, so treat it as "I no longer know", not as "the
dependency is down".

### The control plane is unreachable

**Signal.** `axond_status_component_state{axond_status_component="control_plane"} >= 2`,
`axond_revision_rejections{axond_revision_reason="unavailable"}` increasing, and
`axond_revision_lag` growing.

**Alert.** `AxondControlPlaneUnreachable`.

**First response.** Running replicas keep serving their active snapshot; what
they lose is the ability to learn about new revisions. Fix PostgreSQL and the
fleet converges by itself with no intervention and no restart. Expect
`axond_revision_last_known_good{axond_revision_outcome="restored"}` to increase
if replicas are also being *created* during the outage — a cold boot from the
signed cache is the designed behaviour, and it means those replicas may be
serving something older than desired.

**Do not** restart the fleet or fail readiness. Both convert a control-plane
outage into an inference outage, which is exactly what the cache and the narrow
readiness contract exist to prevent. See
[revision convergence](./revision-convergence.md#during-a-control-plane-outage).

### A replica will not converge

**Signal.** `axond_revision_lag` above your convergence target on a *specific*
replica, `axond_revision_converged == 0`, `axond_revision_consecutive_failures`
rising, and `axond_revision_rejections` split by `axond_revision_reason`.

**Alert.** `AxondRevisionLagAboveTarget`, `AxondRevisionRejectionsSustained`.

**First response.** The rejection reason is the triage key, and
[revision convergence](./revision-convergence.md#when-a-replica-will-not-converge)
lists what each one means. `validation` and `projection` will refuse on *every*
replica and need a corrected or older-compatible revision; `secret` and
`snapshot` are frequently replica-specific — one replica missing an environment
variable while its siblings converge looks exactly like this. A refused revision
is never partly applied: the replica is serving exactly what it served before.

### The fleet is split across revisions

**Signal.** `max(axond_config_generation) - min(axond_config_generation) > 0`
sustained, or the same spread over `axond_revision_converged`. Both are
per-replica gauges with no labels of their own, so the fleet view is the spread
across the series your scrape distinguishes by `instance`.

That distinction has to come from your pipeline, because axond's OTLP resource
carries only `service.name`. A per-replica collector — a sidecar, or a DaemonSet
scraped per pod — gives each replica its own series and the spread works. A
*single shared* collector receiving OTLP from every replica collapses them into
one series, and then every spread in this section is permanently zero and
`AxondFleetRevisionSplit` can never fire. Either run the collector per replica,
or add a replica-identifying resource attribute in the pipeline and turn
`resource_to_telemetry_conversion` on so it reaches the series.

**Alert.** `AxondFleetRevisionSplit`.

**First response.** A brief split is convergence working: replicas converge
independently and there is no fleet-wide barrier. A persistent split is one
replica stuck — take that replica's identity from the series labels your scrape
adds (`instance`, `pod`), read its `GET /admin/v1/status` revision summary (once
convergence is wired — it is `null` until then), and
treat it as the previous failure mode. A replica whose generation lags after a
file reload rather than a revision has a different file or environment than its
siblings; restarting *that* replica is safe and usually the fix.

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

### A fail-closed dependency is denying requests

**Signal.** `axond_rate_limit_unavailable_denials` or
`axond_revocation_unavailable_denials` increasing, and `503` responses on
`axond_http_server_requests`. `GET /admin/v1/status` reports the owning
component `unavailable`.

**Alert.** `AxondFailClosedDependencyDenials`.

**First response.** These denials are the configured stance, not a bug: with
`on_unavailable = "deny"` an unreachable store denies rather than admits, so a
budget or revocation outage cannot become unmetered or un-revoked traffic. Fix
the store. Choosing `on_unavailable = "allow"` is a deliberate availability
trade you make in advance, in config — not a change to make during an incident.

Distinguish it from the tenant's own limits: `429 budget_exceeded` is a spent
cap, `503 budget_unavailable` is *your* dependency down.

### Budget or rate-limit capacity is exhausted

**Signal.** `axond_budget_capacity_denials`, `axond_budget_namespace_denials`,
`axond_budget_retained_subjects` approaching the configured `max_subjects`, and
`axond_rate_limit_capacity_denials`.

**Alert.** `AxondBudgetCapacityExhausted`, `AxondNamespaceBudgetExhausted`.

**First response.** `namespace_denials` means the whole namespace is out of
budget, so *every* subject in it is denied — not one noisy caller. Capacity
denials and rising retained subjects are the in-memory bound instead: the replica
is refusing subjects it has no room to track, which is subject churn or an
under-sized `max_subjects`, and it is per replica by design.

### The replica is shedding load

**Signal.** `axond_admission_rejections` split by `axond_admission_resource`
(`request`, `stream`, `tenant`, `queue`, `diagnostic`) and `axond_error_type`, with
`axond_admission_in_flight` as the leading indicator.

**Alert.** `AxondAdmissionShedding`, `AxondAdmissionSaturated`.

**First response.** `request` is the replica's own ceiling — scale out, or raise
`admission.max_in_flight` to what one process can actually hold. `tenant` is one
namespace's ceiling and is the caller's own concurrency. Sustained `queue`
rejections mean under-provisioning rather than burstiness: queueing only absorbs
short bursts. Shed requests are refused before the rate-limit store, the budget
reservation, and the provider, so shedding costs nothing upstream. `diagnostic`
is a different animal: it is the fixed ceiling on `GET /admin/v1/status` — eight
reads being answered, sixty-four being authenticated — it is not sized by
`admission.max_in_flight`, and it means
something is polling the diagnostic rather than that the replica is out of
capacity — served traffic is unaffected either way.

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
or shorten `max_stream_duration_ms`.

`GET /admin/v1/status` still answers in both phases and reports the phase it is
in, including `closing`: it authenticates but sits outside admission, and takes
no served in-flight slot, so polling a stuck replica neither is refused nor
extends the drain it is describing. It is not unbounded, though — eight
concurrent diagnostic reads per replica, and sixty-four concurrent
authentications of them, refused beyond either with `503
diagnostic_concurrency_exceeded`. The second ceiling is the wider one because it
sits *outside* authentication, where an anonymous caller can reach it: it bounds
the signature checks and revocation lookups a flood would otherwise spend, while
the narrow one is inside, where only a caller that proved it may ask can spend a
slot. Neither is configurable. Poll serially when scripting a fleet sweep.

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
id, alias-level request id, jti, revision id, and anything secret-shaped is
refused as a metric dimension outright, so no scrape endpoint reveals tenancy
that the redacted status response is careful not to. Per-request attribution
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
| [`ops/observability/dashboards/axond-fleet.json`](../../ops/observability/dashboards/axond-fleet.json) | Grafana dashboard: served traffic, dependency status, convergence, and usage delivery across the fleet |
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
- [Revision convergence](./revision-convergence.md) — convergence targets, what a
  replica reports, and the last-known-good cache.
- [Upgrades and rollback](./upgrades.md) — mixed-version rules and schema
  ordering.
