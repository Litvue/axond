# Backup, restore, and point-in-time recovery

What axond keeps, how to get it back, and how long that is allowed to take.
RPO and RTO below are the operator contract. The recovery qualification drill
that used to execute them in CI was retired with the tier matrix
([ADR 0063](../adr/0063-stateful-only-namespaced-gateway.md) / #427).

## What is durable, and what is not

| State | Where it lives | Loss costs |
| --- | --- | --- |
| Namespaces and their incarnations | Store: SQLite file or PostgreSQL | Every tenant. Inference to `/ns/{ns}/v1` refuses an unknown namespace. |
| Namespace period spend, reservations, cadence budgets | Store (`axond_store_budget*`) | Accumulated spend against namespace caps. Leftover `axond_budget*` tables from the withdrawn budget backend are not this ledger and are not migrated. |
| Management usage index | Store (`axond_store_usage`) | Current-period `GET /api/v1/namespaces/{ns}/usage` summaries, not the billing warehouse. Operators prune rows older than 90 days (or whose `period` is no longer billed); the gateway does not auto-prune. |
| Usage rows | PostgreSQL (`axond_usage`) when a Postgres usage sink is configured | Billing and analytics history. |
| Usage outbox | PostgreSQL, when `[usage_journal]` is configured | Undelivered billing events. See [usage outbox](./usage-outbox.md). |

The HTTP process holds nothing durable besides its Store connection. Replicas
are interchangeable. SQLite is a single-replica file: back up that file, with
the process stopped or with `sqlite3 .backup`, so the WAL is included.

## Objectives

These are the numbers a Postgres deployment is expected to meet or explicitly
revise. They are per PostgreSQL cluster.

| Objective | Target | What it takes to hold |
| --- | --- | --- |
| **RPO**: data a disaster may lose | **≤ 5 minutes** | Continuous WAL archiving with `archive_timeout = 300` (or streaming to a standby), plus a base backup no older than a week. Without WAL archiving the RPO is the age of the last dump, which is typically 24 hours. |
| **RTO**: time to serving again | **≤ 30 minutes** | A base backup restorable in place, WAL reachable from the restoring host, and the recovery target chosen before the restore starts rather than during it. |

Every inference request reads its namespace from the Store, so the RTO bounds
serving, not only administration.

## Backups

Two mechanisms, because they answer different questions. Take both.

### Continuous archiving: the RPO mechanism

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
regression. The database keeps accepting writes, and the WAL needed to replay
them never leaves the host.

### Logical dumps: the migration and corruption mechanism

```bash
pg_dump "$AXOND_STORE_DSN" -Fc -f /backups/store-$(date -u +%F).dump
```

A dump is portable across major versions and across clusters, and it is the only
backup that survives a corrupt cluster the WAL would faithfully reproduce. It is
a point in time nobody chose, though, so it bounds the RPO at its own age.

Back up every database axond writes to, not only the Store: usage sinks and the
outbox may live in databases of their own.

## Restoring

### From a logical dump

```bash
createdb axond_restored
pg_restore -d axond_restored --no-owner /backups/store-2026-08-13.dump
```

Point `[storage] dsn_env` at the restored database and boot one replica. Check
that `GET /api/v1/namespaces` lists the namespaces you expect before rolling
the rest.

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

Choose the target before starting, and choose it *before* the change being
undone. Then check that `SELECT pg_is_in_recovery()` returns `f` (the cluster
promoted rather than waiting for WAL it cannot reach), and that the change
after the target is absent. A restore that replayed to the end of the WAL
passes every "the rows are there" check and still contains the change the
incident was about.

## Rehearsal

<a id="the-drill"></a>

Rehearse a logical dump and a point-in-time restore against *your* backups.
Only a restore of your archive proves your archive.

## See also

- [Store backends](../deployment/stateful-backends.md): supported versions and
  DDL.
- [Upgrades and rollback](./upgrades.md): forward-only migrations and rollback
  limits.
- [Production checklist](../deployment/production-checklist.md): the review this
  page is the recovery half of.
- [ADR 0044](../adr/0044-recovery-objectives-and-supported-backends.md): why
  these objectives are numbers.
