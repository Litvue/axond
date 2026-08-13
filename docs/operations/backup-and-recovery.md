# Backup, restore, and point-in-time recovery

What axond keeps, how to get it back, and how long that is allowed to take. A
backup procedure nobody has restored from is a hypothesis, so the objectives on
this page have an executable form: `ops/restore-drill.sh` performs both recoveries
against the supported PostgreSQL and runs in CI on every change.

## What is durable, and what is not

| State | Where it lives | Loss costs |
| --- | --- | --- |
| Control-plane revisions, resource versions, blobs, mutations, audit events, idempotency records, head | PostgreSQL (`axond_cp_*`) | Configuration history and the audit trail. Not recoverable from anywhere else. |
| Schema version and migration ledger | PostgreSQL (`axond_cp_schema_migration`) | The record of what was applied; without it a database is [`Unrecorded`](./control-plane-journal.md#schema-status-and-what-each-state-means) and a replica refuses it. |
| Usage rows | PostgreSQL (`axond_usage`) | Billing and analytics history. |
| Namespace and subject spend, reservations | PostgreSQL (`axond_budget*`) when a Postgres budget backend is configured | Accumulated spend against caps. |
| Revocation entries | PostgreSQL (`axond_revocation`) | Denylisted token identifiers, which is a security regression, not just a data one. |
| Rate-limit windows, budget reservations, revocation cache | Redis | Accuracy and availability of enforcement in flight — **not history**. |

Redis is hot state and is deliberately outside the recovery objectives below. It
is a cache and a coordination surface, not a source of record: losing it makes
enforcement briefly less accurate (in-flight reservations vanish, windows reset)
and, under `on_unavailable = "deny"`, makes it a fail-closed outage — but nothing
that was true yesterday is lost with it. Restoring an RDB or AOF file is
therefore optional; if you do, restore the layout markers with it rather than the
counters alone, because a marker-less database reads as a different layout to the
next boot. See [Stateful backends](../deployment/stateful-backends.md#redis).

The gateway itself holds nothing durable. Replicas are interchangeable and a lost
replica is replaced, not recovered; a stateless deployment (`mode = "stateless"`)
has no durable state at all and nothing on this page applies to it beyond the
usage, budget, and revocation stores it opted into.

## Objectives

These are the objectives the drill demonstrates and the numbers a deployment is
expected to meet or explicitly revise. They are per PostgreSQL cluster.

| Objective | Target | What it takes to hold |
| --- | --- | --- |
| **RPO** — data a disaster may lose | **≤ 5 minutes** | Continuous WAL archiving with `archive_timeout = 300` (or streaming to a standby), plus a base backup no older than a week. Without WAL archiving the RPO is the age of the last dump, which is typically 24 hours. |
| **RTO** — time to serving again | **≤ 30 minutes** for the control plane | A base backup restorable in place, WAL reachable from the restoring host, and the recovery target chosen before the restore starts rather than during it. |
| **Audit and usage durability** | No committed revision, audit event, or usage row is lost inside the RPO window | The journal writes the manifest, resource versions, mutation, audit event, idempotency record, and head in one transaction, so a recovery never lands on a half-published revision. |

Inference keeps serving during a control-plane outage on the snapshot each replica
already holds, so the RTO above bounds *administrative* recovery, not availability
of the request path. The exception is a cold boot: a replica that has not yet
loaded a revision needs the control plane, so the RTO is a serving objective for
any replica that restarts during the incident. See
[during a Postgres outage](./control-plane-journal.md#during-a-postgres-outage).

## Backups

Two mechanisms, because they answer different questions. Take both.

### Continuous archiving — the RPO mechanism

```ini
# postgresql.conf
wal_level = replica
archive_mode = on
archive_command = 'test ! -f /archive/%f && cp %p /archive/%f'
archive_timeout = 300           # bounds the RPO at five minutes of idle WAL
```

```bash
pg_basebackup -h "$PGHOST" -U "$PGUSER" -D /backups/base-$(date -u +%FT%TZ) -Fp -Xs -c fast
```

The archive is what makes a recovery target selectable. Alert on
`pg_stat_archiver.last_failed_wal`: a failing archiver is a silent RPO
regression — the database keeps accepting writes, and the WAL needed to replay
them never leaves the host.

### Logical dumps — the migration and corruption mechanism

```bash
pg_dump "$GW_CONTROL_PLANE_DSN" -Fc -f /backups/control-plane-$(date -u +%F).dump
```

A dump is portable across major versions and across clusters, and it is the only
backup that survives a corrupt cluster the WAL would faithfully reproduce. It is
a point in time nobody chose, though, so it bounds the RPO at its own age.

Back up every database axond writes to, not only the control plane: usage,
budget, and revocation may live in databases of their own.

## Restoring

### From a logical dump

```bash
createdb axond_restored
pg_restore -d axond_restored --no-owner /backups/control-plane-2026-08-13.dump
GW_CONTROL_PLANE_DSN='postgres://axond@db/axond_restored?sslmode=require' \
  axond migrate status --config /etc/axond/axond.toml
```

`axond migrate status` is the acceptance test, not a formality: it is the same
check a replica makes before it serves, so a restore it rejects is a restore no
replica would have accepted either. A restored database whose ledger came back
empty is [`Unrecorded`](./control-plane-journal.md#schema-status-and-what-each-state-means) and needs an
explicit baseline; that is the adoption path, not a migration.

### To a point in time

```bash
cp -a /backups/base-2026-08-13T00:00:00Z /var/lib/postgresql/restored
cat >>/var/lib/postgresql/restored/postgresql.auto.conf <<'EOF'
restore_command = 'cp /archive/%f %p'
recovery_target_time = '2026-08-13 01:12:00+00'
recovery_target_action = 'promote'
EOF
touch /var/lib/postgresql/restored/recovery.signal
pg_ctl -D /var/lib/postgresql/restored start
```

Choose the target before starting, from the audit trail
(`SELECT recorded_at, event_kind, summary FROM axond_cp_audit_event ORDER BY recorded_at DESC`),
and choose it *before* the mutation being undone. Then verify what recovery
landed on, in this order:

1. `SELECT pg_is_in_recovery()` returns `f` — the cluster promoted rather than
   sitting in recovery waiting for WAL it cannot reach.
2. `axond migrate status` reports the schema this build requires.
3. `SELECT revision_id FROM axond_cp_head` is the revision you intended, and the
   revision published after the target is **absent**. This asymmetry is the whole
   point: a restore that replayed to the end of the WAL passes every "the rows are
   there" check and still contains the change the incident was about.
4. Roll replicas so they load the recovered head; a replica holding the
   post-target snapshot keeps serving it until it does.

Forward-only migrations mean a recovery to a point before an upgrade must be
served by the binary of that time. See
[Upgrades and rollback](./upgrades.md).

## The drill

```bash
ops/restore-drill.sh
```

About a minute, needs Docker and a `cargo` build. It starts the supported
PostgreSQL with WAL archiving, installs the schema with `axond migrate apply`,
applies the usage, budget, and revocation DDL, writes control-plane state and an
audit event, and then performs both recoveries:

- a `pg_dump`/`pg_restore` round trip, asserting the head, the revision
  checksums, and the audit trail come back;
- a `pg_basebackup` plus archived WAL recovery to a target between two published
  revisions, asserting the first revision is present, the second is **not**, and
  the usage, budget, and revocation schemas survived.

Both restored databases are accepted by `axond migrate status` or the drill
fails. The `Restore and PITR drill` CI lane runs it on every change, so the
procedure on this page cannot rot into something that no longer restores.

Run it against your own backups too: the drill proves the mechanism, while only
a restore of your archive proves your archive.

## See also

- [Control-plane revision journal](./control-plane-journal.md) — schema status,
  migrations, and outage behaviour.
- [Stateful backends](../deployment/stateful-backends.md) — supported versions,
  DDL, and Redis's role.
- [Upgrades and rollback](./upgrades.md) — forward-only migrations and rollback
  limits.
- [Production checklist](../deployment/production-checklist.md) — the review this
  page is the recovery half of.
- [Recovery qualification](./recovery-qualification.md) — the fleet-level
  contract its `backup-restore` and `point-in-time-recovery` scenarios rehearse
  this procedure under, once its harness exists.
- [ADR 0041](../adr/0041-recovery-objectives-and-supported-backends.md) — why
  these objectives are numbers, why Redis is outside them, and what changing them
  costs.
