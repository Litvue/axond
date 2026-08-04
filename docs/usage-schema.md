# Usage schema

One usage record is produced per terminated request — including failures,
cancellations, and partial streams — and fanned out to every configured sink.
This document is the reader-facing contract for that record: the columns, their
meaning, and how they are allowed to change. The design rationale is
[ADR 0009](./adr/0009-durable-usage-sinks.md).

**Current version: `1`** (`UsageRecord::SCHEMA_VERSION`, DDL in
[`ops/postgres/usage_v1.sql`](../ops/postgres/usage_v1.sql)).

## Fields

| Column | Type | Meaning |
| --- | --- | --- |
| `id` | `bigserial` | Surrogate key. Postgres-only; not part of the record. |
| `schema_version` | `integer` | Version of this row's shape. Always populated. |
| `request_id` | `text` | Identifies one request. Unique **per gateway process**, not globally. |
| `trace_id` | `text` | W3C trace id of the caller's trace; NULL when the request was not traced. One trace usually spans many requests. |
| `namespace` | `text` | Tenant/namespace the request was served under. |
| `subject` | `text` | Authenticated caller — the gateway-key id, or `anonymous` in open dev mode. |
| `model` | `text` | Alias the caller asked for (`gpt-4o`). |
| `target_provider` | `text` | Provider that served it. |
| `target_model` | `text` | Concrete upstream model / deployment. |
| `credential_source` | `text` | `platform` or `byok`. |
| `credential_id` | `text` | Non-secret label of the credential in the pool that served the request. |
| `status` | `text` | `ok`, `upstream_error`, `client_cancelled`, `partial`, or `rejected`. |
| `input_tokens` | `bigint` | Prompt tokens billed upstream. |
| `output_tokens` | `bigint` | Completion tokens billed upstream. |
| `reasoning_tokens` | `bigint` | Reserved; NULL today. |
| `cache_read_tokens` | `bigint` | Reserved; NULL today. |
| `cache_write_tokens` | `bigint` | Reserved; NULL today. |
| `cost_microdollars` | `bigint` | Cost in micro-dollars, priced from the target's catalog entry. |
| `catalog_version` | `bigint` | Version of the pricing catalog the cost was computed against. |
| `latency_ms` | `bigint` | End-to-end gateway latency. |
| `attempts` | `bigint` | Upstream target attempts across the alias's targets; retry count is `attempts - 1`, and `1` means the first target served. |
| `started_at` | `timestamptz` | `recorded_at - latency_ms`. |
| `recorded_at` | `timestamptz` | When the gateway settled the request. Excludes the sink's own batching delay. |

The stdout and OTLP sinks carry the same fields, minus `id` and the reserved
token columns: stdout emits the record as JSON (`snake_case`, `trace_id` omitted
when absent), and the OTLP sink emits it as an OTel log record with
`event_name = axond.usage` and `axond.*` / `gen_ai.*` attributes.

## Versioning policy

- Adding a **nullable** column, or populating a reserved one, is not a version
  bump: no existing reader changes behaviour.
- Removing or renaming a column, making one `NOT NULL`, or changing a unit or a
  vocabulary (e.g. a new `status` value is fine; redefining an existing one is
  not) **is** a bump: a new `ops/postgres/usage_v<N>.sql` plus a bump of
  `UsageRecord::SCHEMA_VERSION`. Shipped DDL files are never edited in place.
- One table may hold rows written by several gateway versions. Read
  `schema_version` rather than assuming a deploy timeline.

## Reading the rows

Writes are **at least once**: a batch whose commit outcome is unknown is retried,
so a duplicate row is possible. Deduplicate on `(request_id, recorded_at)`.
Because `request_id` is unique per process rather than globally, a fleet of
replicas can in principle mint the same id — include `recorded_at` (and, when
present, `trace_id`) in any join that must be exact.

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
