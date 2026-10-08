# 67. TypeScript review hardening

Date: 2026-10-08

## Status

Proposed for review with the TypeScript conversion stack.

## Context

The conversion's automated reviews identified boundary, buffering, persistence,
and qualification defects. The Rust binary remains the release oracle under
ADR 0066. These changes qualify the TypeScript implementation; they do not
switch the shipped release binary.

## Decision

Extension authors remain reviewed operator code. The scoped SQL facade accepts
only simple SELECT projections from the namespace or extension tables with an
exact bound namespace predicate. Arbitrary SQL, including writes, requires
`trusted: true`. This strengthens the convenience facade; it creates no sandbox.
A pre-auth extension can intentionally authenticate a request under ADR 0066.
The token extension must bind claims to the request context's identity, validate
signed claim shapes, and restrict grants to named inference surfaces. Grants
cannot authenticate management routes. The optional mint route requires an
explicit administrative mint key different from the signing key; it is disabled
by default. Its default maximum validity is one hour. Mint authority remains
administrative authority to issue grants for the selected namespace, not a
permission conferred by an inference token.

Request bodies and transformed SSE events have incremental byte bounds and
cancel their sources at the limit. Usage delivery counts queued, scheduled, and
in-flight rows against capacity. Shutdown reports any drained insert failure and
bounds connection cleanup with the same deadline.

File-backed SQLite summaries use a separate read-only WAL snapshot and indexed
pages of at most 1024 rows, yielding between pages. Settlement uses the existing
connection and serialization. An in-memory SQLite store retains synchronous
reads. PostgreSQL aggregates summaries in SQL. Integer charges and JSON numeric
tokens retain their exact values; the established ledger saturation remains.

Metric exports are cumulative snapshots with a stable start timestamp, emitted
at most once per 30 seconds per recorder on request completion. Traces remain
per request. Exporter HTTP refusals are failures, not successful deliveries.

An existing extension table is insufficient evidence of an applied migration.
An owner must run and register the migration before a restricted role starts.
`storage.create_table = false` probes the schema and refuses missing objects
without issuing gateway DDL. Existing databases also need the usage index and
the cadence `period` and provider-model `source` columns.

### State tier

No existing deployment's state tier changes. Local coordination remains process
memory; SQLite remains local durable state and PostgreSQL remains Tier 2 shared
durable state. The extra SQLite reader does not introduce an external service.

## Consequences

For the TypeScript 0.x interface, changing mint configuration, accepted scoped
SQL, pattern syntax, timer ranges, or migration privileges is a minor release
change. Operators must explicitly configure the mint key, apply migrations with
an owner, and store Worker gateway keys as secrets. Redaction supports a bounded
pattern grammar: literals, dot, character classes, and quantifiers up to 64
characters per atom. Grouping, alternation, and anchors are rejected; ambiguous
accepted patterns use dynamic programming instead of exponential backtracking.
Runtime timer delays are clamped to the supported signed 32-bit millisecond
range, accounting for units. These differences must be reviewed before adopting
the TypeScript binary. Existing security conclusions about reviewed extension
code, static core authentication, secret delivery, and the Rust release artifact
remain in force. The review disposition report names the regression evidence.
