# Usage schema

One usage record is produced per terminated request — including failures,
cancellations, and partial streams — and fanned out to every configured sink.
This document is the reader-facing contract for that record: the columns, their
meaning, and how they are allowed to change. The design rationale is
[ADR 0009](./adr/0009-durable-usage-sinks.md).

**Current version: `2`** (`UsageRecord::SCHEMA_VERSION`, DDL in
[`ops/postgres/usage_v2.sql`](../ops/postgres/usage_v2.sql)).

## Fields

| Column | Type | Meaning |
| --- | --- | --- |
| `id` | `bigserial` | Surrogate key. Postgres-only; not part of the record. |
| `schema_version` | `integer` | Version of this row's shape. Always populated. |
| `request_id` | `text` | Identifies one request, and therefore one usage event. Globally unique: `req_` followed by a lowercase canonical UUIDv7 (`req_0192f5e1-2b3c-7def-8123-456789abcdef`). Opaque — treat it as a string, not as a parsed timestamp. |
| `trace_id` | `text` | W3C trace id of the caller's trace; NULL when the request was not traced. One trace usually spans many requests. |
| `namespace` | `text` | Tenant/namespace the request was served under. |
| `subject` | `text` | Authenticated caller — the gateway key's env-var label or file path for static authentication, or the token's `sub` claim for token authentication. Switching a static key from `env` to `file` changes this value and therefore the corresponding budget subject; file paths are emitted as written. |
| `signer_kid` | `text` | Configured JWS signer that vouched for a token caller; NULL for static gateway-key authentication. |
| `model` | `text` | Alias the caller asked for (`gpt-4o`). |
| `target_provider` | `text` | Provider that served it. |
| `target_model` | `text` | Concrete upstream model / deployment. |
| `credential_source` | `text` | `platform` or `byok`. |
| `credential_id` | `text` | Non-secret label of the credential in the pool that served the request. |
| `status` | `text` | `ok`, `upstream_error`, `client_cancelled`, `partial`, or `rejected`. |
| `input_tokens` | `bigint` | Non-cached prompt tokens billed at the regular input rate. Add `cache_read_tokens` to recover the provider's full prompt total. |
| `output_tokens` | `bigint` | Completion tokens billed upstream. |
| `reasoning_tokens` | `bigint` | Reserved; NULL today. |
| `cache_read_tokens` | `bigint` | Prompt tokens read from the provider cache, disjoint from `input_tokens`. |
| `cache_write_tokens` | `bigint` | Prompt tokens written to the provider cache. |
| `cost_microdollars` | `bigint` | Cost in micro-dollars, priced from the target's catalog entry. |
| `catalog_version` | `bigint` | Version of the pricing catalog the cost was computed against. |
| `latency_ms` | `bigint` | End-to-end gateway latency. |
| `attempts` | `bigint` | Upstream target attempts across the alias's targets; retry count is `attempts - 1`, and `1` means the first target served. |
| `started_at` | `timestamptz` | `recorded_at - latency_ms`. |
| `recorded_at` | `timestamptz` | When the gateway settled the request. Excludes the sink's own batching delay. |

The stdout and OTLP sinks carry the same fields, minus `id` and
`reasoning_tokens`: stdout emits the record as JSON (`snake_case`, `trace_id`
omitted when absent), and the OTLP sink emits it as an OTel log record with
`event_name = axond.usage` and `axond.*` / `gen_ai.*` attributes.

## Versioning policy

- Adding a **nullable** column, or populating a reserved one, is not a version
  bump: no existing reader changes behaviour.
- Additive nullable columns that were not reserved in the base DDL ship as
  ordered `usage_v1_<sequence>_<name>.sql` files alongside `usage_v1.sql`.
  Fresh installations apply `usage_v1.sql` followed by every additive file in
  filename order; existing installations apply only the new additive file
  before deploying a writer that emits its column. The writer does not probe
  for missing columns: deploying it first causes the existing sink error and
  dropped-record path.
- Removing or renaming a column, making one `NOT NULL`, or changing a unit or a
  vocabulary (e.g. a new `status` value is fine; redefining an existing one is
  not) **is** a bump: a new `ops/postgres/usage_v<N>.sql` plus a bump of
  `UsageRecord::SCHEMA_VERSION`. Shipped DDL files are never edited in place.
- Version 2 changes the meaning of `input_tokens` from the inclusive provider
  prompt total to the non-cached prompt remainder, so it is a version bump even
  though the cache columns were reserved in version 1.
- One table may hold rows written by several gateway versions. Read
  `schema_version` rather than assuming a deploy timeline.
- Making `request_id` globally unique is **not** a bump. The column keeps its
  type, its `req_` prefix, and its meaning ("identifies one request"); only the
  set of ids it can hold widened, and it was always documented as opaque. A
  reader that stored, compared, or grouped by it is unaffected; the only reader
  affected is one that parsed the old 16-hex-digit body, which was never part of
  the contract. Nothing else was strengthened in step: the shipped DDL declares
  no unique index on it (see below), so the promise is about what the writer
  emits, not about what an existing table enforces.

This schema versions independently of the gateway's own version; where it sits
among axond's other published interfaces is the
[compatibility contract](./compatibility.md), and the policy of record is
[ADR 0015](./adr/0015-zero-dot-x-compatibility-policy.md).

## Reading the rows

Writes are **at least once**: a batch whose commit outcome is unknown is retried,
so a duplicate row is possible. Deduplicate on `request_id` alone — it is
globally unique across replicas and restarts, so it is the whole key of a usage
event and nothing else needs to be added to make a join exact.

The shipped DDL does **not** declare a unique index on it, because a table may
already hold rows written by an older gateway whose ids were only unique per
process. Deployments with no such rows can add one:

```sql
CREATE UNIQUE INDEX CONCURRENTLY axond_usage_request_id_key
    ON axond_usage (request_id);
```

The sink's insert ends in `ON CONFLICT DO NOTHING`, which carries no target: it
does nothing on the shipped DDL, and with the unique index in place it absorbs
the duplicate a retry or an outbox redelivery presents, so the retry commits and
nothing is counted as
`records_dropped{axond.drop_reason="sink_error"}`. The index is therefore worth
adding wherever the rows allow it — it is what makes the table itself
duplicate-free rather than leaving every reader to deduplicate, and a
billing-grade deployment (`docs/operations/usage-outbox.md`), where redelivery is
routine, should have it.

Ids are time-ordered, but by **admission**, not settlement: one is minted when the
gateway accepts a request, and rows are written when requests end. A long stream's
row therefore lands after — and with a smaller id than — a short request that
started later. So the ordering is worth having for storage (an index on
`request_id` stays append-mostly) but `max(request_id)` is not a safe cursor on
its own: a naive one skips exactly the slow requests. Page on `recorded_at`, which
is settlement-ordered, and re-read a window at least as long as your longest
tolerated request. The embedded timestamp is not an interface either — read
`recorded_at` for time.

Rows written before this change carry a `req_` + 16-hex-digit id
(`req_0000000000000001`) and are unique only within the process that wrote them.
The two shapes are distinguishable by length, and both are just text to a reader.

Spend for a period, per namespace:

```sql
SELECT namespace,
       sum(cost_microdollars) / 1e6 AS usd,
       sum(input_tokens)            AS input_tokens,
       sum(output_tokens)           AS output_tokens
FROM axond_usage
WHERE recorded_at >= now() - interval '1 day'
  AND status IN ('ok', 'partial')
GROUP BY namespace
ORDER BY usd DESC;
```

## Delivery guarantees

Sinks are off the request path and a slow or failing sink drops rather than
stalls, so durability is best-effort by construction. What was lost is
observable:

- `axond.usage.records_written{axond.usage_sink}` — records a sink accepted.
- `axond.usage.records_dropped{axond.usage_sink, axond.drop_reason}` — records
  discarded, where the reason is `buffer_full` (the destination could not keep
  up), `sink_error` (it rejected the batch), or `shutdown`.

Alert on the second one. A sustained non-zero rate means the buffer
(`buffer_capacity`) or the destination is undersized for the offered load.

This is the **telemetry-grade** delivery mode and it is the default: a record the
fan-out accepted is not a record that was written, so these rows are telemetry
rather than an accounting source.

`[usage_journal] backend = "postgres"` opts into the **billing-grade** mode
instead ([ADR 0048](./adr/0048-billing-grade-usage-outbox.md)): the event is
appended to a durable outbox before the request is answered, replayed into these
same sinks until they acknowledge it, and a request whose event could not be made
durable is answered `503 usage_not_durable` rather than `200`. The row shape does
not change — the guarantee about whether the row eventually exists does. Delivery
is still at-least-once, so the deduplication advice above is unchanged and
becomes load-bearing: the outbox *will* redeliver after a crash, always with the
same `request_id`. Setup, recovery, poison handling, and alerts are in the
[usage outbox guide](./operations/usage-outbox.md).
