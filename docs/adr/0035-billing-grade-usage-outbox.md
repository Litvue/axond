# 35. Billing-grade usage delivery: an opt-in durable outbox

Date: 2026-08-12

## Status

Accepted

Extends the usage sinks of [ADR 0009](./0009-durable-usage-sinks.md) rather than
replacing them, and keeps the stateless default of
[ADR 0002](./0002-stateless-by-default-stateful-by-opt-in.md) and the tier rules
of [ADR 0017](./0017-state-tiers-and-optional-backends.md) intact.

## Context

Usage records are telemetry-grade today, by construction and on purpose: a
settled request hands its record to a fan-out that buffers behind a bounded queue
and drops with a counter when a destination stalls
([ADR 0009](./0009-durable-usage-sinks.md)). That is the right trade for
dashboards — a request is never delayed by a metrics pipeline — and the wrong one
for money. A dropped record is an invoice line that never existed, and nothing in
the request path can tell whether the row it produced survived.

Three gaps make the existing `kind = "postgres"` sink unusable as a billing
source even though it writes to a database:

- **Acceptance is not durability.** The sink accepts a record into memory and
  returns; the request is answered before any row is written. A replica that is
  `SIGKILL`ed, evicted, or OOM-killed between the two loses every buffered
  record, and a full buffer drops silently by design.
- **Nothing replays.** There is no record of what was written, so a restart
  cannot tell delivered records from lost ones. Recovery consists of hoping.
- **The consumer cannot deduplicate.** Retrying anything therefore risks double
  billing, which is why nothing retries.

A gateway that also wants to *charge* for what it relayed needs the opposite
default for exactly one class of deployment, without changing the behaviour, cost,
or state tier of any deployment that does not ask for it.

## Decision

Add a second delivery mode behind one type, `UsageDelivery`, so the request path
asks for an event to be recorded and learns whether it was, and *which* guarantee
it got is a property of the configuration rather than of the route:

- **Telemetry-grade** (default, unchanged): the existing fan-out. Best effort,
  off the request path, lossy under overload, drops counted.
- **Billing-grade** (`[usage_journal] backend = "postgres"`): the event is
  appended to a durable outbox *before* the request is answered, and a bounded
  delivery worker replays it to the configured sinks until they acknowledge it.

The parts of that decision worth stating as boundaries:

- **Identity is minted once, at admission.** A `request_id` is a UUIDv7 rendered
  `req_<uuid>` and is captured after the request is admitted and before it is
  dispatched upstream, so the id in the response's telemetry, the id in the
  outbox row, and the id every redelivery carries are the same string. It is the
  key a destination deduplicates on, and it is globally unique rather than
  per-replica: nothing coordinates, and two replicas cannot mint the same id.
- **The outbox is a separate contract, not the usage table.** `UsageJournal` is
  append / claim / ack / quarantine / stats / maintain over
  `ops/postgres/usage_outbox_v1.sql`. The `axond_usage` sink rows stay exactly
  what they are — the outbox is what guarantees they eventually exist. Keeping
  them apart is also what keeps this slice independent of the unfinished tenancy,
  admin, and catalogue work: it stores a `UsageRecord`, and knows nothing else
  about the deployment.
- **Append is idempotent on the event's identity.** The same fact appended twice
  is stored once (`AlreadyPresent`); the same identity with *different* content
  is a `Conflict` rather than an overwrite, because two different facts claiming
  one request id is a bug, and silently keeping either one produces a wrong bill.
- **Delivery is at-least-once, and says so.** A claim leases a bounded batch; an
  acknowledgement is written only after the destination write returned. A worker
  that dies in between leaves the lease to expire and the event to be delivered
  again — a *new* `DeliveryId`, the *same* `request_id`. Exactly-once is not
  offered, so the deduplication key is documented instead of implied.
- **Ordering is per `(namespace, subject)`, not global.** At most one event per
  ordering key is in flight, so one caller's events reach a destination in the
  order they happened; a caller whose events are stuck cannot hold up anybody
  else's. A global order would make one slow row a global stall.
- **A full outbox refuses by default, and a request whose usage is not durable is
  not answered `200`.** `capacity_policy = "refuse"` and
  `on_undurable = "refuse"` (both defaults) turn storage exhaustion into
  `503 usage_not_durable` — the caller learns the request was not recorded and
  can retry it. `drop-oldest` and `serve` exist for deployments that would rather
  serve than account, and both count what they lost. Quarantined events are never
  a drop candidate: deleting the evidence an operator was asked to look at to
  make room is not a bound, it is a cover-up.
- **Poison leaves the delivery path instead of blocking its key.** A row this
  build cannot decode, or one that exhausted `max_delivery_attempts`, is
  quarantined with a reason and stays on disk for an operator. A row written by a
  *newer* build is skipped untouched — no attempt spent, no verdict — so a
  rolling upgrade's older replicas leave it for the replicas that can read it.
- **Shutdown is bounded and honest.** The worker stops claiming, spends the
  remaining shutdown budget on what is deliverable, and reports the rest as
  *undelivered durable work* rather than as lost usage — because it is still in
  the outbox, and the next process claims it.

### State tier

Tier 2 (Postgres), and only for a deployment that opts in.
`[usage_journal] backend` defaults to `none`, which is Tier 0 and byte-for-byte
the behaviour that shipped before this decision: no outbox, no worker, no
datastore, telemetry-grade delivery. No existing configuration changes tier,
because every existing configuration omits the section.

There is no Tier 1 implementation and deliberately so: a billing-grade guarantee
on Redis would depend on its persistence configuration, and a store whose
durability is the operator's `appendfsync` setting cannot promise that an
acknowledged append survives.

### Security review trigger

Trigger 5, [persistence, migrations, telemetry, and
usage](../security/threat-model-review.md#5-persistence-migrations-telemetry-and-usage),
fires: new shipped DDL, a new durable store on the request path, new metrics, and
a changed delivery guarantee for usage records.

- The outbox stores a `UsageRecord` as `jsonb` plus the columns it is indexed and
  ordered by. It is the same non-secret shape as `axond_usage`
  ([`docs/usage-schema.md`](../usage-schema.md)) — `credential_id` and
  `credential_source` are references, never material — so no new class of data
  becomes durable, and no field is emitted that a sink did not already carry.
- `namespace` and `subject` are stored because ordering is scoped to them. They
  are the same values the usage row and the budget key already carry; the outbox
  performs no cross-namespace read, and a claim is filtered by consumer, not by
  namespace, so nothing here is an authorization boundary it could get wrong.
- The DSN is referenced by env-var *name* (`dsn_env`), never inlined in config,
  and no error, log, or metric carries a connection string or password.
  `[usage_journal] schema` reaches `SET search_path` and is therefore validated as
  one unqualified identifier, exactly as `[control_plane] schema` is
  ([ADR 0032](./0032-operator-preflight-and-forward-only-migrations.md)).
- New shipped SQL exists in both locations and is gated by the existing
  byte-identity tests; a row-shape change is a new `usage_outbox_v<N>.sql` rather
  than an edit ([ADR 0009](./0009-durable-usage-sinks.md)).
- The new metric labels are closed sets or an operator-configured consumer name;
  no caller input becomes an attribute.
- Availability coupling is the deliberate consequence: with the defaults, an
  outbox that is full or unreachable refuses requests. That is a fail-closed
  choice about money, it is opt-in, and `on_undurable = "serve"` is the documented
  escape for deployments that rank availability higher.

No threat-model update is owed: no new state tier for an existing deployment, no
new emitted field, and no new trust boundary — an operator-visible release
impact only, which the [usage outbox
guide](../operations/usage-outbox.md) documents.

## Consequences

A deployment that opts in can reconstruct every billable request: a settled
request is durable before it is answered, survives `SIGKILL`, replays after
restart, and reaches its destinations at-least-once with a stable deduplication
key. Storage exhaustion, corruption, poison, version skew, and an incomplete
drain are each an explicit, measured behaviour rather than a silent drop.

Costs, all of them borne only by deployments that ask for it: a database write is
on the request path, so a slow outbox is added latency and an unavailable one is
`503` under the default policy. The outbox is bounded, so an operator owns its
capacity, its retention window, and the alert on its depth and oldest pending
age. Destinations must deduplicate on `request_id` — a destination that treats
every delivery as new will double-count under redelivery. Retention must exceed
the longest retry horizon a caller has, because pruning an acknowledged event
forgets its idempotency key and a much later retry of the same request would then
append a second copy. And a `drop-oldest` or `serve` deployment has knowingly
traded the guarantee for availability; the counters say how often that trade was
taken.
