# Store backends

Axond keeps namespaces, budgets, and the management usage index in one Store,
configured under [`[storage]`](../configuration.md#storage--required-adr-0063)
([ADR 0063](../adr/0063-stateful-only-namespaced-gateway.md)). Pick one:

| Backend | Use it for |
| --- | --- |
| SQLite (WAL) | One replica. The file must live on a persistent volume. |
| Postgres | More than one replica, or when the database is already the operated backup unit. |

A durable usage sink or the billing-grade usage outbox may also use Postgres;
each has its own `dsn_env`.

## Postgres

The Store applies its own DDL at connect unless `[storage] create_table =
false`. A role without DDL grants applies the files by hand first:

```bash
psql "$AXOND_STORE_DSN" -f ops/postgres/store_namespace_incarnation_v1.sql
psql "$AXOND_STORE_DSN" -f ops/postgres/store_budget_v1.sql
psql "$AXOND_STORE_DSN" -f ops/postgres/store_budget_cadence_v1.sql
psql "$AXOND_STORE_DSN" -f ops/postgres/store_usage_v1.sql
```

Durable usage:

```toml
[[usage_sink]]
kind = "postgres"
dsn_env = "AXOND_USAGE_POSTGRES_DSN"
create_table = false
```

Apply the committed DDL under explicit schema ownership:

```bash
psql "$AXOND_USAGE_POSTGRES_DSN" -f ops/postgres/usage_v1.sql
psql "$AXOND_USAGE_POSTGRES_DSN" -f ops/postgres/usage_v1_001_add_signer_kid.sql
psql "$AXOND_USAGE_POSTGRES_DSN" -f ops/postgres/usage_v2.sql
psql "$AXOND_USAGE_POSTGRES_DSN" -f ops/postgres/usage_v2_001_add_price_identity.sql
```

Apply additive usage migrations in filename order **before** deploying a binary
that writes the new shape. The sink compares every column it binds against the
existing table while it connects, so a binary deployed ahead of any migration
refuses to boot and names the ordered files to apply rather than dropping
batches at insert time. Migrate the table in place and preserve its history; do
not recreate it merely because the refusal mentions the base DDL. The check
resolves an unqualified table through the connection's `search_path`, just like
the `INSERT`, so a DSN selecting `billing` is checked in `billing`, not
hard-coded `public`. A table that does not exist yet is not checked: with
`create_table = false` its creation is yours to sequence, and until it exists
the off-path sink drops rejected batches and increments the dropped-record
metric.

Use `sslmode=require` in production DSNs. Axond uses rustls and webpki roots.

## Supported versions

| Backend | Supported | Floor is enforced by |
| --- | --- | --- |
| PostgreSQL | 14, 15, 16, 17 | Nothing at boot. An older server fails its statements rather than its connection. |
| SQLite | the bundled library | The binary; there is no external server. |

Newer PostgreSQL majors are not refused and are not tested here. The DDL is
version-independent within the supported range, so a major upgrade
(`pg_upgrade` or dump and restore) needs no axond change. Verify a restore into
the new major with [the restore drill](../operations/backup-and-recovery.md).

## Availability and recovery

- Initial Store connectivity is validated before the listener binds.
- When the Store cannot reserve, `[storage] on_unavailable` decides: `deny`
  answers `503 budget_unavailable`, `allow` serves without a hold.
- Usage sinks are off the request path: full buffers or rejected writes drop
  with metrics rather than stall provider traffic.
- Recovery objectives and backup mechanisms are in
  [Backup, restore, and point-in-time recovery](../operations/backup-and-recovery.md).

See [Configuration](../configuration.md), [Observability](../observability.md),
and [Upgrades](../operations/upgrades.md) for exact fields and rollout checks.
