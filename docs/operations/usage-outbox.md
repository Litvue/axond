# Billing-grade usage outbox

Usage records are telemetry-grade by default: a settled request hands its record
to a bounded, buffered fan-out and is answered immediately, and a stalled
destination drops records with a counter rather than delaying a request
([ADR 0009](../adr/0009-durable-usage-sinks.md)). That is the right trade for
dashboards and the wrong one for an invoice.

This page is the operator's view of the other mode. With
`[usage_journal] backend = "postgres"`, every settled usage event is appended to
a durable outbox **before the request is answered**, and a delivery worker
replays it into the configured sinks until they acknowledge it. Nothing here is
constructed unless you turn it on; the rationale and its boundaries are
[ADR 0035](../adr/0035-billing-grade-usage-outbox.md).

## The two modes

| | Telemetry-grade (default) | Billing-grade (opt-in) |
| --- | --- | --- |
| Configuration | `[usage_journal]` omitted, or `backend = "none"` | `backend = "postgres"` |
| State tier | Tier 0 (Tier 2 if a `postgres` *sink* is configured) | Tier 2 |
| When the record becomes durable | Possibly never | Before the response is sent |
| A record on a stalled destination | Buffered, then dropped with a count | Retained in the outbox and retried |
| A `SIGKILL`ed replica | Loses everything buffered | Loses nothing that was answered |
| Delivery | At-most-once | At-least-once, replayed after restart |
| Full or unreachable store | Drops | `503 usage_not_durable` (default) |
| On the request path | No | Yes: one `INSERT` |

Switching modes changes what a request can fail on. Read
[When a request is refused](#when-a-request-is-refused) before enabling it.

## What is stored

Three tables plus a loss counter, from
[`ops/postgres/usage_outbox_v1.sql`](../../ops/postgres/usage_outbox_v1.sql):

| Table | Holds |
| --- | --- |
| `axond_usage_outbox` | One appended event: `request_id`, the record as `jsonb`, its schema version, the `(namespace, subject)` it is ordered by, and when it was observed |
| `axond_usage_outbox_consumer` | Consumers that have claimed. Retention waits on every registered one |
| `axond_usage_outbox_delivery` | Per-consumer delivery state: attempts, lease expiry, and the acknowledgement or quarantine verdict |
| `axond_usage_outbox_loss` | Events lost to `capacity_policy = "drop-oldest"`, durably counted |

The event body is the same non-secret record a sink already receives
([`docs/usage-schema.md`](../usage-schema.md)): `credential_id` and
`credential_source` are references, never key material. The table names are fixed
— the outbox is the gateway's delivery state rather than an interface a billing
query reads — so a second outbox on one database means a second schema.

`request_id` is the event identity: `req_` followed by a canonical UUIDv7, minted
once when the request is admitted. It is `UNIQUE`, it is what every redelivery
carries, and it is the key your destinations must deduplicate on.

## Setup

Order matters: the schema exists before the writer runs.

1. **Create the role and database.** The gateway's role needs
   `SELECT, INSERT, UPDATE, DELETE` on the four tables and `USAGE` on the
   sequence. It does not need DDL rights unless you use `create_schema = true`.
2. **Apply the DDL**, into its own schema if you keep the outbox separate from
   your billing tables:

   ```bash
   psql "$AXOND_USAGE_JOURNAL_DSN" \
     -c 'CREATE SCHEMA IF NOT EXISTS billing' \
     -c 'SET search_path TO billing' \
     -f ops/postgres/usage_outbox_v1.sql
   ```

3. **Configure the gateway.** The outbox needs at least one sink to deliver
   *into*; in billing-grade mode those sinks are written through synchronously by
   the worker rather than buffered.

   ```toml
   [[usage_sink]]
   kind = "postgres"
   dsn_env = "AXOND_USAGE_DSN"

   [usage_journal]
   backend = "postgres"
   dsn_env = "AXOND_USAGE_JOURNAL_DSN"
   schema = "billing"
   ```

4. **Restart the replicas.** `[usage_journal]` is boot-only, like every other
   store section. A replica logs its mode at startup:

   ```text
   INFO usage delivery mode=billing-grade durable=true journal=postgres on_undurable=refuse
   ```

The outbox connects **at boot** and checks that its tables are readable, so a bad
DSN, a missing schema, or a role without the right grants refuses to start rather
than failing every request later. `kind = "otlp"` is rejected as a billing-grade
destination: the OTel SDK's batch processor acknowledges nothing, so the worker
would have no answer to acknowledge on.

Every key is in the [configuration
reference](../configuration.md#usage_journal--billing-grade-usage-delivery-opt-in-tier-2).

## Guarantees, stated precisely

- **Durable before answered.** In billing-grade mode the append happens before
  the response is acknowledged. A request answered `200` has a durable usage
  event; a request whose event could not be journaled is not answered `200`
  (under the default `on_undurable = "refuse"`).
- **At-least-once, not exactly-once.** A worker that writes to a destination and
  dies before recording the acknowledgement leaves the lease to expire, and the
  event is delivered again. The redelivery is a new delivery attempt of the
  *same* `request_id`. **Your destinations must deduplicate on `request_id`.**
  The shipped `axond_usage` table does not do this for you.
- **Ordered per `(namespace, subject)`.** At most one event per key is in flight,
  so one caller's events reach a destination in the order they happened. Ordering
  is *not* global: a stuck key never holds up another caller's events.
- **Idempotent append.** The same event appended twice — a retry after an unknown
  outcome — is stored once. The same `request_id` with *different* content is
  refused as a conflict rather than overwriting either fact.
- **Bounded.** `max_events` bounds the outbox, `claim_batch` bounds a claim,
  `max_delivery_attempts` bounds retries, and `retain_acknowledged_seconds`
  bounds retention. Nothing here grows without a limit you set.

## When a request is refused

`503 usage_not_durable` means exactly one thing: the request was served upstream
but its usage event could not be made durable, so the gateway refuses to report
success for a request it cannot bill. Causes, in the order they are worth
checking:

| Cause | Signal |
| --- | --- |
| The outbox is full | `axond.usage.journal.appends{axond.journal.outcome="at_capacity"}`, and `axond.usage.journal.depth` at `axond.usage.journal.capacity` |
| Postgres is unreachable or slow past `operation_timeout_ms` | `axond.usage.journal.appends{axond.journal.outcome="backend"}` |
| A second, different record claimed a live `request_id` | `axond.journal.outcome="conflict"` — a bug, not an operational condition |

A full outbox is almost always a delivery problem, not an append problem: events
are arriving and nothing is acknowledging them. Look at
`axond.usage.journal.oldest_pending_age` and the sinks before raising
`max_events`.

Two policies change this, and both are a deliberate trade against accounting:

- `on_undurable = "serve"` — answer the request anyway and count the event as
  lost (`axond.usage.journal.lost`). Availability over accounting.
- `capacity_policy = "drop-oldest"` — bound the outbox by discarding the oldest
  undelivered event, counted durably in `axond_usage_outbox_loss`. A quarantined
  event is never discarded to make room, so an outbox whose whole backlog is
  poison still refuses.

Requests that are already terminal cannot be refused: a stream whose bytes were
relayed, or a cancellation, is still appended durably first, but a failure there
can only be counted (`axond.usage.journal.lost`), because there is no answer left
to change.

## Recovery

**Restart, crash, or eviction.** Delivery state is rows, so a new process resumes
rather than replaying everything: acknowledged events are not redelivered,
unacknowledged ones are. An event whose lease is held by a process that no longer
exists becomes claimable when the lease expires (`lease_seconds`), which is the
only recovery step — there is nothing to run by hand.

**A stuck destination.** Events accumulate; nothing is lost until `max_events` is
reached. Fix the destination and the backlog drains on its own. Watch
`axond.usage.journal.oldest_pending_age`.

**Poison.** A row this build cannot decode, and a row that exhausted
`max_delivery_attempts`, is quarantined with a reason
(`axond.usage.journal.quarantined`) so it stops blocking its ordering key. It
stays on disk for you:

```sql
SELECT e.request_id, d.consumer, d.poison_reason, d.attempts, e.record
FROM axond_usage_outbox_delivery d
JOIN axond_usage_outbox e USING (position)
WHERE d.quarantined_at IS NOT NULL;
```

Quarantine is terminal for that consumer: the gateway will not deliver it again,
and retention will not prune it while a consumer still has it quarantined. Decide
what the event is worth, reconcile it by hand, and delete the delivery row (or
the event) when you are done. `poison_reason = "malformed"` means the stored JSON
is not a record this build can read — the row is your evidence of corruption, and
the record's own `request_id` ties it to the request it came from.

**Replaying into a new destination.** Add the sink and give the worker a new
`consumer` name: delivery state is per consumer, so a fresh name starts from the
beginning of everything still retained. That is a replay of the retained window,
not of all history, and the destination sees each event as a first delivery.

## Upgrades and version skew

The outbox row carries the `schema_version` it was written at.

- **A row a newer build wrote is skipped untouched** by an older replica — no
  attempt spent, no verdict, no lease. During a rolling upgrade the replicas on
  the new version deliver it. Mixed versions are therefore safe, and no event is
  delivered by a build that cannot read it.
- **The reverse is not true of the schema itself.** The DDL is forward-only and
  never edited in place: a row-shape change ships as a new
  `ops/postgres/usage_outbox_v<N>.sql`, applied before the writers that emit it,
  exactly like the usage table ([ADR 0009](../adr/0009-durable-usage-sinks.md)).
- **Drain before you downgrade.** Rolling back to a build that predates a row
  version leaves those rows undeliverable — reported as
  `axond.usage.journal.undeliverable{axond.journal.reason="schema_ahead"}` rather than silently
  dropped, but nothing will deliver them until that build is running again.

## Shutdown

On shutdown the worker stops claiming new work, spends the remaining shutdown
budget on what is deliverable, and reports the rest:

```text
INFO shutdown complete usage_journal_drained=false ...
```

Events it could not deliver are **undelivered durable work, not lost usage**:
they are still in the outbox, and the next process claims them. An incomplete
drain is normal for a large backlog and is not a data-loss event — the only
data-loss counter is `axond.usage.journal.lost`. Size `[shutdown]` for the drain
you want, but do not treat the bound as a correctness requirement.

## Metrics and alerts

All under `axond.usage.journal.*`, alongside the existing usage metrics
([observability](../observability.md)):

| Metric | Kind | Read it as |
| --- | --- | --- |
| `appends` | counter | Appends by outcome. Anything but `accepted` / `already_present` is a request that was refused or an event that was lost |
| `deliveries` | counter | Events handed to their destinations, by consumer and outcome |
| `depth` | gauge | Events awaiting delivery |
| `in_flight` | gauge | Events under an unexpired lease |
| `oldest_pending_age` | gauge (s) | How far behind delivery is |
| `capacity` | gauge | The configured `max_events`, so depth is readable as a fraction |
| `quarantined` / `quarantined_events` | counter / gauge | Poison, by reason |
| `undeliverable` | counter | Rows this build declined to deliver (`schema_ahead`, `corrupt`) |
| `lost` | counter | Events a billing-grade deployment gave up |

Worth alerting on:

- `lost > 0` — **page.** A billing-grade deployment lost a billable event.
- `appends{axond.journal.outcome != "accepted", != "already_present"}` rising —
  requests are being refused, or events dropped.
- `depth / capacity` above ~0.5, or `oldest_pending_age` beyond a few minutes —
  delivery is falling behind and will start refusing requests.
- `quarantined_events > 0` — reconciliation work is waiting for a human.
- `undeliverable{axond.journal.reason="corrupt"} > 0` — investigate the store.

## Not enabling it

Omit `[usage_journal]`. You keep exactly the behaviour that shipped before it
existed: no outbox, no worker, no extra datastore, no new failure mode on the
request path — and usage records that are telemetry, not accounting.
