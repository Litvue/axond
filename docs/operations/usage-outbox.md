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
[ADR 0049](../adr/0049-billing-grade-usage-outbox.md).

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
   INFO usage delivery mode=billing_grade durable=true journal=postgres on_undurable=refuse
   ```

The outbox connects **at boot** and checks that its tables are readable, so a bad
DSN, a missing schema, or a role without the right grants refuses to start rather
than failing every request later. `kind = "otlp"` is not a billing-grade
destination: the OTel SDK's batch processor acknowledges nothing, so the worker
would have no answer to acknowledge on. Declare it beside a storing sink and it
keeps exporting anyway — the worker hands it the events a destination that
answers has already accepted, so the export is exactly as best-effort as it was
in telemetry-grade mode and its failures never hold an event in the outbox. A
journal whose destinations are *all* `otlp` refuses to boot.

A journal whose only destination is `kind = "stdout"` boots with a warning. It is
allowed — it is how the mode is tried out, and a log pipeline that collects the
line is a real destination — but an acknowledgement then means "a line was
written", and once every destination has acknowledged an event the outbox forgets
it when `retain_acknowledged_seconds` expires. Point billing-grade delivery at a
destination that stores the row.

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

`max_events` bounds the outbox itself, not each replica's share of it: admission
reads the outbox's position span on every append, which every replica's appends
move, and subtracts the positions it knows are vacant. Only deletions — retention,
reclamation, drop-oldest — are cached, for at most a second, and a stale one makes
a replica read the outbox as *fuller* than it is. So the limit can refuse an
append a second early after another replica prunes; it cannot be overshot. A
deletion that removes the outbox's *lowest* row — deleting a quarantined event by
hand, below — is the one case where a cached vacancy count would read the other
way, because the span collapses past holes that are still counted; a span smaller
than the one those holes were counted against is therefore discarded rather than
reused, and that append counts.

An append pays two index probes for that span and, below the limit, nothing else:
the span can only overstate how many rows it covers, so a span under `max_events`
is already proof there is room. Only an outbox whose positions span its limit
counts rows, at most once per second per replica and never past `max_events + 1`
rows — the largest number the decision can tell apart. So the request path never
pays for the size of the backlog, which is exactly when it could least afford to.

What the request path *does* pay for is a connection. The pool has two lanes and
they do not overlap: the last connection belongs to the delivery worker, and the
rest serve appends, so a claim waiting on a slow destination can never hold a
connection a request needs. `connections` defaults to `8`, which is seven
concurrent appends per replica, and the floor is `2`. A connection serves one
operation at a time, so appends beyond the lane's width queue and throughput
becomes the round trip to the outbox: raise `connections` towards the concurrency
you actually serve, inside the share of your Postgres `max_connections` this
replica may hold, before concluding the mode is slow.

## What enabling it changes about your sinks

In billing-grade mode the outbox *is* the buffer, and the worker writes each
claimed batch through to the sinks synchronously. This is not an implementation
detail: a delivery may only be acknowledged once a destination has accepted the
records, and a queue that accepted a record on the destination's behalf would let
the outbox forget an event no one has stored.

A claim is also *acknowledged* as one set, in a single statement, rather than a
transaction per event. That is throughput, not semantics — each event still gets
its own verdict, a quarantined or already-resolved delivery is still answered
individually, and a repeated acknowledgement still changes nothing — but a round
trip per event would cap delivery at a fraction of the rate the request path
appends at, and a worker that falls permanently behind fills the outbox until
requests are refused with `usage_not_durable`.

So, per `[[usage_sink]]`:

| Key | In telemetry-grade mode | With the journal enabled |
| --- | --- | --- |
| `buffer_capacity` | The sink's in-process queue | **Not used.** The outbox holds the backlog, bounded by `max_events` |
| `max_batch` | Records per write | **Not used.** Replaced by `[usage_journal] claim_batch` |
| `flush_interval_ms` | How long a partial batch waits | **Not used.** Replaced by `[usage_journal] poll_interval_ms` |

A replica that has explicitly set any of them logs which ones stopped applying,
rather than leaving a tuned number that means nothing:

```text
WARN the usage journal owns sink batching; these `[[usage_sink]]` keys no longer
     apply and `[usage_journal]` claim_batch/poll_interval_ms replace them
     keys="buffer_capacity, flush_interval_ms" claim_batch=256
```

The two shared sink metrics keep their meaning but change what they can count:
`usage.records_written` still counts records a destination accepted, now emitted
by the delivery worker; `usage.records_dropped` no longer counts a failed write,
because a failed write is a journaled event that will be retried rather than a
lost one. In billing-grade mode the loss and backlog signals are the journal's
own: `axond.usage.journal.lost`, `.quarantined`, and `.depth`.

## The cost of a claim

Acknowledged events stay for `retain_acknowledged_seconds` after the request was
observed — not after the acknowledgement, because the window is there to cover a
caller's retry horizon and that horizon starts at the request — so the retained
history is normally much larger than the backlog. An event that only lands after
a long delivery outage is therefore prunable sooner than a promptly delivered
one, and one older than the window is pruned as soon as it is acknowledged.

A claim must not pay for that history, and it does not: each consumer row
carries a `resolved_through` floor — the
position below which everything is acknowledged, quarantined, or gone — and
maintenance raises it after each retention pass. Both sides of the claim's
selection are floored on it.

The floor stops short of anything appended in the last five minutes. `position`
comes from a sequence, so it is taken before the appending transaction commits and
a later position can become visible while an earlier one is still in flight; a
floor that stepped over that earlier event would leave it durable and never
delivered. The margin is far longer than `operation_timeout_ms` can let an append
run, so the only thing it costs is that the floor trails the backlog by one
maintenance tick's worth of events.

The maintenance tick runs on its own interval whether or not the worker has
caught up: pruning, the floor, and the gauges happen between delivery batches, so
the replica furthest behind is not the one that stops pruning and stops saying how
far behind it is.

The gauges published each maintenance tick are floored the same way, for the same
reason: `depth`, `in_flight`, and the oldest pending age describe the backlog, so
they are read from the events above the floor. The poison count cannot be — a
quarantined event *is* resolved, so it sits below the floor — and is counted from
its own partial index instead, which holds only quarantined delivery rows.

Measured on PostgreSQL 17 with 200,000 acknowledged events inside their retention
window, 100 awaiting delivery across 64 ordering keys, and one consumer
(`EXPLAIN (ANALYZE, BUFFERS)` of the claim selection):

| Selection | Time | Buffers |
| --- | --- | --- |
| No floor | 70.9 ms | 4,086 — sequential scans of both tables |
| Floored (this is what runs) | 9.3 ms | 1,532 — the event side is a 100-row index range |

What remains proportional to the retained history is one scan of the *delivery*
rows for that consumer, which the planner still prefers over an index range while
that table is small and cached. It is bounded by your retention window, so keep
`retain_acknowledged_seconds` to what reconciliation actually needs — 24 hours by
default — rather than raising it to keep an archive. `axond.usage.journal.depth`
is the backlog, not the retained rows; the table size is what to watch for
retention.

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

Refusing is cheap, deliberately: a full outbox whose backlog holds nothing that
may be given up is remembered for a second, so the requests refused inside that
window answer without touching the outbox at all — the database is already the
bottleneck when this happens. Any room an append does manage to free is committed
even though that append is refused, so the next request does not repeat the work.

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
to change. The same holds for a caller that hangs up while its append is in
flight: the append finishes regardless, but a refusal it is no longer there to
receive cannot be retried away, so that failure is counted as lost rather than
returned.

## Recovery

**Restart, crash, or eviction.** Delivery state is rows, so a new process resumes
rather than replaying everything: acknowledged events are not redelivered,
unacknowledged ones are. An event whose lease is held by a process that no longer
exists becomes claimable when the lease expires (`lease_seconds`), which is the
only recovery step — there is nothing to run by hand.

**A stuck destination.** Events accumulate; nothing is lost until `max_events` is
reached. Fix the destination and the backlog drains on its own. Watch
`axond.usage.journal.oldest_pending_age`.

**Poison.** A row this build cannot decode, and a row the destination refuses on
its own account until it exhausts `max_delivery_attempts`, is quarantined with a
reason (`axond.usage.journal.quarantined`) so it stops blocking its ordering key.

A destination-wide outage is *not* poison and never quarantines anything, however
long it lasts. When a whole batch is refused the worker halves it and rewrites the
halves until the refusal is isolated to a single event, and only an event refused
while the same destination accepted its siblings spends an attempt. Several bad
rows in one batch are isolated the same way, one at a time, and their healthy
siblings still land. A destination that accepts nothing — from the whole batch
nor from any piece of it — has said nothing about any particular event, so the
attempt is given back and the lease retries the batch whole. That search is
bounded: a batch nothing has been accepted from stops being probed after 32
refused writes, so an outage is not beaten on once per event. A batch
of one is refused without a verdict for the same reason: with no sibling to judge
it against, "the destination is down" and "this row is bad" are the same
observation, so it is retried rather than condemned. A genuinely poisonous event
alone on its key therefore stalls that key — visible on
`axond.usage.journal.oldest_pending_age` — until traffic on another key gives the
destination a chance to accept something beside it. Rewriting halves means a
destination can see an event twice during that probe, which is the same duplicate
a lease expiry produces and the same idempotency on `request_id` absorbs it.

A quarantined event stays on disk for you:

```sql
SELECT e.request_id, d.consumer, d.poison_reason, d.attempts, e.record
FROM axond_usage_outbox_delivery d
JOIN axond_usage_outbox e USING (position)
WHERE d.quarantined_at IS NOT NULL;
```

Quarantine is terminal for that consumer: the gateway will not deliver it again,
and retention will not prune it while a consumer still has it quarantined. Decide
what the event is worth, reconcile it by hand, and delete the **event** row when
you are done — the delivery row cascades with it:

```sql
DELETE FROM axond_usage_outbox WHERE request_id = 'req_…';
```

Do not delete the delivery row on its own. Quarantining has already raised that
consumer's claim floor past the event, so nothing will hand it out again, and
without a delivery row nothing will prune it either: retention wants an
acknowledgement from every registered consumer, and an absent row is not one. The
event would then hold part of `max_events` forever while sitting below the floor
the gauges are read from, so no metric would say why the outbox is filling up.

`poison_reason = "malformed"` means the stored JSON
is not a record this build can read — the row is your evidence of corruption, and
the record's own `request_id` ties it to the request it came from.

**Replaying into a new destination.** Add the sink and give the worker a new
`consumer` name: delivery state is per consumer, so a fresh name starts from the
beginning of everything still retained. That is a replay of the retained window,
not of all history, and the destination sees each event as a first delivery.

**Retiring a consumer name.** Retention waits on *every* registered consumer, and
a consumer is registered by its first claim and never unregistered. So a name you
stop using — the old one after a rename, or a replay consumer you are finished
with — holds the retention window open forever: nothing is pruned, the outbox
fills to `max_events`, and the default `capacity_policy = "refuse"` then answers
`503 usage_not_durable` on the request path. Delete the retired row once no
replica is configured with that name:

```sql
DELETE FROM axond_usage_outbox_consumer WHERE consumer = 'old-name';
DELETE FROM axond_usage_outbox_delivery  WHERE consumer = 'old-name';
```

The worker says so once a maintenance tick when the outbox holds state for a
consumer this deployment is not running (`consumers this deployment is not
running`), because it cannot tell a retired name from a second fleet's live one —
so it names them and deletes nothing. `axond.usage.journal.depth` staying flat
while `deliveries` keeps rising is what this looks like before the outbox fills.

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
- **The DDL is idempotent.** Re-applying `usage_outbox_v1.sql` to a schema that
  already has it is a no-op, which is what makes the setup step safe to script.
  It is also *additive*: `CREATE TABLE IF NOT EXISTS` does nothing to a table
  that already exists, so the file states
  `axond_usage_outbox_consumer.resolved_through` as an `ALTER TABLE … ADD COLUMN
  IF NOT EXISTS` too, and re-applying it to a schema an earlier copy created is a
  complete upgrade rather than a no-op. If it is not applied, boot refuses: the
  schema check reads the columns this build needs by name, so a missing one is a
  startup failure naming the table rather than an error on the first claim. A
  released version is only ever superseded by a new `usage_outbox_v<N>.sql`.
- **Drain before you downgrade.** Rolling back to a build that predates a row
  version leaves those rows undeliverable — reported as
  `axond.usage.journal.undeliverable{axond.journal.reason="schema_ahead"}` rather than silently
  dropped, but nothing will deliver them until that build is running again.

## Shutdown

On shutdown the worker stops claiming new work, spends part of the remaining
shutdown budget on what is deliverable, and reports the rest:

```text
INFO shutdown complete usage_journal_drained=false ...
```

`usage_journal_drained` is absent rather than `false` when the worker stopped
cleanly but its closing backlog read did not answer in time: whether delivery was
caught up is then unknown, not "no".

Events it could not deliver are **undelivered durable work, not lost usage**:
they are still in the outbox, and the next process claims them. An incomplete
drain is normal for a large backlog and is not a data-loss event — the only
data-loss counter is `axond.usage.journal.lost`. Size `[shutdown]` for the drain
you want, but do not treat the bound as a correctness requirement.

The drain is given half of whatever is left of `flush_timeout_ms` when it starts,
less the one-second allowance for the batch the worker may already be writing:
the telemetry export runs after it inside the same budget, and
`drain_grace_ms + deadline_ms + flush_timeout_ms` stays the whole bound on
termination. With under a second left the worker is stopped without a wait, and
its report says nothing rather than zeros.

The worker checks the stop signal between claims, so it stops at its bound rather
than finishing a long backlog first. If it is stuck inside a destination write it
cannot be stopped at all, and shutdown abandons it: the backlog is then read from
the outbox and logged at `ERROR` *without* delivery counts, because nobody
reported them — a shutdown line missing `delivered` means "this run's totals are
unknown", never "nothing was pending".

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
