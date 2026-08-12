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

## Operator commands

Three commands run *before* replicas do, all with the same grammar —
`axond <command> <action> --config PATH`. `--config` may be omitted when
`AXOND_CONFIG` is set.

| Command | Writes? | Answers |
| --- | --- | --- |
| `axond check preflight --config PATH` | No | Would a replica boot against this? Config ownership and mode, every reference a boot resolves (control plane, secret-store KEK, breakglass, inbound keys and verifiers, provider credentials, opt-in stores), control-plane reachability, schema compatibility. |
| `axond migrate status --config PATH` | No | What schema does this database have, and what would an apply do? |
| `axond migrate apply --config PATH` | Yes | Apply the pending migrations, forward only. |

The command surface, the forward-only policy, and the refusal to migrate at boot
are [ADR 0031](../adr/0031-operator-preflight-and-forward-only-migrations.md).

`preflight` and `status` cannot change a database, and not merely by convention:
they open the control plane on a maintenance path that does not prepare a schema,
with migration permission forced off, and read the ledger inside a `READ ONLY`
transaction, so the *server* rejects a write. `apply` is the only mutation, and it
is explicit.

Exit codes make these usable as deployment gates: `preflight` exits non-zero if
any check failed, `status` exits non-zero while a migration is outstanding or the
schema is refused, and `apply` exits non-zero if the schema was refused. Output
names environment variables, never DSNs.

`apply` against a database that is already current is a no-op rather than a
re-run: it reports the current version and rolls its transaction back without
executing a migration file.

In stateless mode there is no control plane, so `preflight` reports the database
checks as skipped and `migrate` has nothing to do. Neither command requires
PostgreSQL to exist.

Until stateful serving is wired up, a stateful `preflight` **fails** on a
`serving` line and exits non-zero: `axond serve` still refuses
`mode = "stateful"` outright because the durable control plane is not wired to the
runtime yet, so no replica can boot against that config and a zero exit would
promise one that cannot. Every other check still runs and is still printed, so the
report is the same description of the database it would otherwise be — and
`axond migrate status` / `axond migrate apply` are separate commands with their own
exit codes, so preparing the database is not blocked by the serving refusal. The
line is printed from the refusal `serve` itself raises, not from a second copy of
the rule, so it disappears when that refusal does rather than outliving it.

Only the control-plane journal is migrated by these commands. It is the only store
with a ledger — recorded version, file name, checksum — so it is the only one where
"what has been applied?" is a question the database itself can answer. The usage,
budget, and revocation stores are applied by hand from `ops/postgres/`, as
[stateful backends](../deployment/stateful-backends.md#postgres) describes.

### Fresh install

The database must exist and the role must be able to create objects in the target
schema; the commands create neither the database nor the schema.

```bash
export GW_CONTROL_PLANE_DSN='postgres://axond@db/axond?sslmode=require'

axond migrate apply --config /etc/axond/axond.toml   # applies 0001 and onwards
axond check preflight --config /etc/axond/axond.toml # then verify a replica would boot
```

Then start replicas. Run `apply` once from one place; it is safe if that
accidentally becomes twice, or two places at once.

While stateful serving is unwired, that `preflight` exits non-zero on its
`serving` line even when the database is ready; read the rest of the
report, and gate the rollout on `axond migrate status` until the refusal is gone.

### Upgrade

```bash
axond migrate status --config /etc/axond/axond.toml  # with the NEW binary
axond migrate apply  --config /etc/axond/axond.toml  # fleet still on the old binary
axond check preflight --config /etc/axond/axond.toml
# roll replicas onto the new binary
```

Order matters in one direction only: migrations are additive and forward-only, so
the *new* binary's `apply` runs before the new binary serves, and the old binary
keeps running against the migrated schema until it is replaced. Never apply from
an older binary than the one you are deploying, and never roll a binary onto a
schema its own `status` reports as *Ahead*.

Migrations are forward-only and versioned. An applied file is never edited and
never renumbered: a schema change is a new
`ops/postgres/control_plane_<NNNN>_<name>.sql`, applied on top. Every object is
created in the current schema, so a journal that lives beside other tables works
with `[control_plane] schema` (or a DSN that sets `search_path`).

`apply` runs the whole read-and-write in one transaction under a PostgreSQL
advisory lock, so it is safe to run while replicas are starting and two
simultaneous applies are one migration. A replica configured with
`[control_plane] migrate = true` does the same thing at boot; the default is off,
because one apply before a rollout is the order that cannot have replicas
migrating a database their peers are reading.

Applying the DDL with `psql` still works and stays supported for operators who
own schema changes out of band:

```bash
psql "$GW_CONTROL_PLANE_DSN" -f ops/postgres/control_plane_0001_initial.sql
```

That path does not write the ledger row, so the journal is then reported as
*Unrecorded* — a ledger table that exists and records nothing — and both `status`
and `apply` refuse it. An empty ledger is indistinguishable from an untouched
database, and the ledger is the only record of what was applied, so migrating from
zero would replay every shipped file over objects that may already be there;
that survives only while every statement is `IF NOT EXISTS`. Either drop the
empty `axond_cp_schema_migration` table if nothing was applied, or state the
baseline the DDL corresponds to:

```bash
psql "$GW_CONTROL_PLANE_DSN" -c "INSERT INTO axond_cp_schema_migration (version, name, checksum)
  VALUES (1, 'control_plane_0001_initial', '<checksum>')"
```

The refusal prints the exact statement, checksum included, for every migration
this build ships. Prefer `axond migrate apply`, which records what it applied and
never leaves this state behind.

## Schema status, and what each state means

`axond migrate status` reports these, and boot refuses to serve anything but
*Current*. Each state is separate because each implies something different to do —
"the schema is wrong" would leave an operator to find out which of these it is.

| Status | Meaning | What to do |
| --- | --- | --- |
| Current | The applied history is exactly the required one | Nothing |
| Behind | Migrations are missing | `axond migrate apply` |
| Unrecorded | The ledger table exists and records nothing: the DDL was applied out of band, or every row was deleted | Drop the empty ledger if nothing was applied, or record the baseline the database corresponds to (the refusal prints the statement); never migrate it from zero |
| Ahead | The database records a migration this build does not know | Deploy the newer build; do not downgrade the schema |
| Drifted | An applied migration's recorded checksum is not this build's file | Restore the file, or add a new migration — never edit in place |
| Incomplete | The applied versions are not a complete prefix (`v3` without `v2`, or a deleted ledger row) | Find out what applied out of order or removed the row; the maximum version alone is not evidence the history is intact |
| Renamed | A version is recorded under a name this build does not ship it as | A migration was renumbered or renamed rather than added; restore the shipped numbering |
| Malformed | The ledger is not the one this build writes: a missing column, a version below 1, a duplicate version, another table under that name | Something else owns `axond_cp_schema_migration`, or a restore was partial |

Only *Behind* (and a database with no journal table at all) is migratable. Everything
else is a refusal a retry cannot clear, and `apply` refuses it rather than writing
more DDL over a history it cannot account for: a replica that served against a
schema it did not write would be writing rows a newer build defined differently.

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
  a missing resource version, a declared blob whose record is gone, a dependency
  that leaves the revision or closes a cycle, a body this build cannot decode, a
  checksum that no longer matches, or a reference that crosses a tenant boundary.
  The message names the resource or edge. This is corruption,
  not an outage — retrying will not clear it — and it means something wrote to the
  journal outside the gateway, or a restore was partial. Compare against a backup;
  a healthy older revision still loads, so serving can continue from one while the
  damage is investigated.
- **Too large (`exceeds what hydration reads`).** The revision is intact but
  larger than this build's read bounds (resource versions, blobs, blob bytes,
  dependency edges or nesting, inline body bytes, or total candidate size); the
  message names which bound and, where useful, what was observed. Nothing is
  truncated to fit. Either raise that bound deliberately or split the change into
  smaller revisions. A dependency *cycle* is never reported here — no bound could
  clear one — it is unreadable, above.
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
