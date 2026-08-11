# 9. Durable usage sinks: a versioned schema and a drop-not-stall contract

Date: 2026-08-04

## Status

Accepted

## Context

ADR 0002 put usage behind a `UsageSink` trait with a stdout default, so the
binary boots with no datastore. That is the right floor, but stdout is not an
answer for the two things operators actually do with usage: bill from it and
analyse it. Both want durable rows.

Three forces shape how durability lands:

- **The schema is the hard-to-change part.** A usage table lives in the
  adopter's own database, is read by their billing queries, and may be replicated
  into a warehouse. Once rows exist, a column rename is their migration, not
  ours. The row shape is therefore a published interface, and the interesting
  question is not "which columns" but "how does it change".
- **A sink must never be able to hurt a request.** The failure mode to avoid is
  the one ADR 0002 describes from a previous service: a datastore on the request
  path that turned an outage into an outage. A usage write is worth less than the
  request it describes.
- **Observability is already solved once.** ADR 0007 installs an OTLP/HTTP
  exporter stack over the gateway's own `reqwest` client, deliberately avoiding a
  second HTTP client in a single-static-binary product. A usage exporter must not
  undo that.

## Decision

**Postgres is the durable sink, behind an opt-in `[[usage_sink]]` entry.** With
no entry the behaviour is exactly what it was: one JSON line per record on
stdout. A configured sink connects at boot — and applies the DDL if asked to — so
a wrong DSN, an unreachable database, or a missing table refuses to start rather
than discarding records at request time.

**The schema is versioned, documented, and shipped as DDL.** Every row carries
`schema_version`; the DDL lives in [`ops/postgres/usage_v1.sql`](../../ops/postgres/usage_v1.sql)
and is documented in [`docs/usage-schema.md`](../usage-schema.md). The rules:

- An **additive, nullable** column is not a version bump. `reasoning_tokens`,
  `cache_read_tokens`, and `cache_write_tokens` are declared now and left NULL
  precisely so populating them later changes no reader.
- An additive nullable column that was not reserved ships as an ordered
  `usage_v1_<sequence>_<name>.sql` alongside the base DDL. Fresh installations
  apply the base file and then every additive file; existing installations
  apply only the new file before the writer. The writer does not probe for the
  column: migrate first or the sink drops its failed writes.
- Anything a reader can observe as a change in meaning — a dropped or renamed
  column, a widened `NOT NULL`, a changed unit or vocabulary — is a new
  `usage_v<N>.sql` and a bump of `UsageRecord::SCHEMA_VERSION`. The old file is
  not edited, because it describes rows that already exist.
- Version 2 applies this rule to the usage counters: `input_tokens` is now the
  non-cached prompt remainder, while the reserved cache counters are populated
  so readers can reconstruct the provider total.
- One table may hold rows of several versions. That is the point of the column:
  readers branch on it instead of guessing from a deploy timeline.

Writes are **at least once**. A batch whose commit outcome is unknown is retried
once, so an exactly-once reader deduplicates on `(request_id, recorded_at)`.
`request_id` is unique per *process*, not globally, so it is indexed and not
constrained — a globally unique id would be a change to the record's shape, which
this ADR deliberately does not make. Timestamps are stamped when the fan-out
observes the record, not when the batch flushes, so a sink's buffering never
appears as request latency; `started_at` is `recorded_at - latency_ms`.

**The durability-vs-latency contract: drop, count, and never stall.** Each
datastore sink owns a bounded buffer and a single flush task. The request path
enqueues with a non-blocking `try_send`; a full buffer discards the record and
increments `axond.usage.records_dropped{sink,reason}`
(`buffer_full` | `sink_error` | `shutdown`), while accepted batches increment
`axond.usage.records_written`. So: usage is durable when the destination keeps
up, lossy when it does not, and the loss is a number an operator can alert on
rather than a mystery. The buffer (default 10 000 records) and batch policy
(default 500 rows or 1 s, whichever comes first) are the knobs that trade memory
for tolerated stall.

Buffering belongs to the **sink**, not the fan-out: one slow destination must not
delay a fast one, and each keeps its own queue and its own drop count.

**OTLP usage export is a log record, on the existing exporter stack.** For
operators who want one backend, a `kind = "otlp"` sink emits each record as an
OTel log record (`event_name = axond.usage`) carrying the identifiers metrics
cannot afford — `request_id`, `subject`, `credential_id`, `trace_id` — with the
trace context set so the row joins the caller's trace. It reuses the endpoint,
client, and resource that telemetry already installed: one more signal, not a
second exporter. The SDK's batch log processor does the buffering, so this sink
is not wrapped in the batching queue, and it refuses to be configured when OTLP
export is off rather than emitting into a no-op provider.

**`UsageSink` stays separate from `BudgetStore`.** Nothing here touches the
request-path admission decision; a Postgres *budget* backend is a different trait
and a different ADR.

**Dependency:** `tokio-postgres` (MIT) with `tokio-postgres-rustls` (MIT) for TLS
against managed Postgres, and `rustls` (already in the tree via `reqwest`) to
install the process-default crypto provider. No new licence had to be allowed in
`deny.toml`. A full SQL toolkit (`sqlx`) was rejected: this sink issues one
statement shape, needs no query builder or compile-time checking, and the
gateway's value is being one small static binary. Rows are written with a
multi-row `INSERT`, chunked so a batch cannot exceed the protocol's 65535-bind
limit.

## Consequences

- Usage is durable only as far as the destination keeps up, and the gap is
  measured. Operators who need zero loss must provision for peak or raise the
  buffer; the gateway will not buy durability with request latency.
- The schema is now a compatibility surface with a stated change policy, so
  adding fields to the canonical record (reasoning/cache tokens, a globally
  unique request id) is a follow-up that this table already has room for.
- Tinybird and ClickHouse remain unimplemented. Both are HTTP ingest endpoints
  and fit the same `UsageSink` + batching seam without further design; they are
  post-beta.
- The Postgres sink holds one connection. A single flush task cannot use a pool,
  and adding one would only matter if writes were parallelised across
  connections — which would in turn weaken the ordering that makes the
  drop-accounting simple.
