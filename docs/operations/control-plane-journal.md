# Control-plane revision journal

The durable desired state of a stateful deployment is a chain of immutable
revisions in PostgreSQL. This page is the operator's view of that journal: what
the schema is, how migrations are applied and checked, and what happens to a
running fleet when the database is unavailable.

Stateful mode is still being assembled: this is the storage layer
([#165](https://github.com/Litvue/axond/issues/165)). Hydrating a revision into a
runtime snapshot and converging replicas onto it are separate slices, so nothing
described here is constructed by `serve` yet, and no `/admin/v1` route writes to
it. Read [ADR 0027](../adr/0027-stateless-and-stateful-operating-modes.md) for the
mode as a whole.

## What is stored

Publishing a change never edits a row. It writes new resource versions and a new
manifest that references them, then advances a single head pointer — so every
earlier revision remains loadable exactly as it was published.

| Table | Holds |
| --- | --- |
| `axond_cp_schema_migration` | Applied migration versions, names, and a checksum of each file's text |
| `axond_cp_blob` | Content-addressed payload references: kind, SHA-256 digest, size |
| `axond_cp_resource_version` | One immutable version of one resource, shared by every revision that pins it |
| `axond_cp_resource_dependency` | The dependency edges between resource versions |
| `axond_cp_mutation` | One administrative change and its actor attribution |
| `axond_cp_revision` | A published revision: parent, mutation, whole-state checksum, publication order |
| `axond_cp_revision_entry` | The manifest: which resource version each revision pins |
| `axond_cp_revision_blob` | The blobs a revision declares |
| `axond_cp_audit_event` | The audit trail, written in the publishing transaction |
| `axond_cp_idempotency` | Per-caller retry records, which expire |
| `axond_cp_head` | One row naming the desired revision |

Two properties worth knowing before you plan capacity or retention:

- **Revisions share storage.** A resource version is written once and referenced
  by every revision that pins it; a blob digest pinned by a hundred revisions is
  one row. A revision is not a copy of the state.
- **Blobs are references, not payloads.** The journal stores kind, digest, and
  size. Payload bytes live in the blob store, and a revision is only usable when
  its digests resolve there.

Secret material is never in the journal. A credential resource's body carries an
opaque secret *reference*; plaintext stays in the secret store, and no body value
is logged.

## Applying migrations

Migrations are forward-only and versioned. An applied file is never edited and
never renumbered: a schema change is a new
`ops/postgres/control_plane_<NNNN>_<name>.sql`.

Apply migration 0001 by hand before pointing a gateway at a new database:

```bash
psql "$AXOND_CONTROL_PLANE_DSN" -f ops/postgres/control_plane_0001_initial.sql
```

Every object is created in the current schema, so a journal that must live beside
other tables can be applied after `SET search_path`.

A gateway configured to migrate does the same thing at boot, under a PostgreSQL
advisory lock held for the whole check-and-apply transaction, so a fleet starting
together against an empty database applies the DDL once rather than N times.

## Schema status, and what each state means

Boot compares what the database has applied against what the build requires and
refuses to serve anything it does not recognise.

| Status | Meaning | What to do |
| --- | --- | --- |
| Current | The applied history is exactly the required one | Nothing |
| Behind | Migrations are missing | Apply them, or allow the gateway to |
| Ahead | The database has a migration this build does not know | Deploy the newer build; do not downgrade the schema |
| Drifted | An applied migration's text no longer matches its recorded checksum | Restore the file, or add a new migration — never edit in place |

*Ahead* and *Drifted* are refusals, not warnings, and they are refusals a retry
cannot clear: a replica that served against a schema it did not write would be
writing rows a newer build defined differently.

Drift is also checked without a database. The packaged copy under
`crates/gateway/sql/` must be byte-identical to `ops/postgres/`
(`crates/gateway/tests/shipped_ddl.rs` and `ops/publish-crates.sh`), and the
migration text the build embeds must be the file it applies.

## PostgreSQL version

PostgreSQL 14 or newer. CI exercises 17. Boot reads `server_version_num` and
refuses an older server rather than failing later on a statement it cannot run.

## TLS and connectivity

Connectivity follows the same conventions as the usage and revocation backends:
the DSN decides TLS (`sslmode`), the connector is the shared Rustls one with the
webpki root bundle, connection and operation timeouts are bounded, and a
connection that an outage broke is discarded and re-established on the next
operation. A DSN that requests TLS against a server that cannot provide it fails
to connect; it is never silently downgraded to plaintext. The DSN is never
logged.

## During a Postgres outage

The journal is not on the inference path. A replica serves an immutable snapshot
it already holds, so an outage does not touch what it is serving:

- Inference keeps working on the loaded snapshot.
- Administrative writes fail with a retryable unavailable error, and a
  publication that cannot finish writes nothing: the manifest, the resource
  versions, the mutation, the audit event, the idempotency record, and the head
  move in one transaction or not at all.
- A replica that has not yet loaded a revision cannot start serving. Cold boot
  needs the control plane.

So a partially published revision is not a state the journal can be in, and
"roll back the failed change" is not an operator task.

## Concurrency and retries

- **One writer wins.** Publication reads the head row `FOR UPDATE`, so two
  administrators submitting against the same expected revision serialize: one
  commits, the other is told it conflicts and with what.
- **A retry replays its own outcome.** A repeated idempotency key that describes
  the same desired state returns the original manifest — even if the expectation
  has since gone stale — without recording a second mutation or a second audit
  event. The same key describing *different* state is refused.
- **Deduplication is scoped per caller and expires.** One administrator's
  `retry-1` neither replays nor blocks another's, and expiry closes a retry
  window without touching the revision or audit trail the record points at.

## When a revision will not load

Loading a revision reads its manifest, resolves every resource version it pins,
and verifies the result against the checksums recorded at publication. It either
returns the whole revision or fails; there is no partially loaded revision to
inspect or repair, and a failed load never becomes state a replica serves. Three
outcomes need different responses:

- **Unreadable (`stored revision … is unreadable`).** The rows no longer add up:
  a missing resource version, a dependency that leaves the revision, a body this
  build cannot decode, a checksum that no longer matches, or a reference that
  crosses a tenant boundary. The message names the resource. This is corruption,
  not an outage — retrying will not clear it — and it means something wrote to the
  journal outside the gateway, or a restore was partial. Compare against a backup;
  a healthy older revision still loads, so serving can continue from one while the
  damage is investigated.
- **Too large (`exceeds what hydration reads`).** The revision is intact but
  larger than this build's read bounds (resource versions, blobs, blob bytes,
  dependency edges or nesting, inline body bytes, or total candidate size); the
  message names which bound and, where useful, what was observed. Nothing is
  truncated to fit. Either raise that bound deliberately or split the change into
  smaller revisions.
- **Unavailable.** Postgres is unreachable. Retryable, and covered by
  [During a Postgres outage](#during-a-postgres-outage).

A revision that loads once loads forever: revisions are immutable, so a
successful load is repeatable, and history stays answerable after any number of
newer publications.

## Backup and retention

Back the journal up like any store of record; it is the authority on what the
fleet should be serving. Nothing here is safe to prune selectively: mutations and
audit events are referenced by revisions, and resource versions are referenced by
every manifest that pins them. Expired idempotency rows are the only
self-pruning data, and they are pruned on the write path that depends on them.

## Useful queries

```sql
-- What is desired right now, and when it was published.
SELECT h.revision_id, r.created_at, r.state_checksum
FROM axond_cp_head h JOIN axond_cp_revision r ON r.revision_id = h.revision_id;

-- The most recent changes, with attribution.
SELECT r.seq, r.revision_id, m.actor_kind, m.actor_subject, m.actor_component,
       a.event_kind, a.summary, a.recorded_at
FROM axond_cp_revision r
JOIN axond_cp_mutation m USING (mutation_id)
JOIN axond_cp_audit_event a ON a.revision_id = r.revision_id
ORDER BY r.seq DESC
LIMIT 20;

-- Applied migrations.
SELECT version, name, applied_at FROM axond_cp_schema_migration ORDER BY version;
```
