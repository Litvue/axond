# Stateful backends

Axond is config-only by default. Add Redis or Postgres only when a feature must
coordinate replicas or survive process replacement.

This guide covers the **stateless** operating mode: TOML owns every resource, and
a backend below is an opt-in for one capability. "Stateful backend" here does not
mean the stateful control plane accepted in
[ADR 0027](../adr/0027-stateless-and-stateful-operating-modes.md), which moves
resource ownership into Postgres behind
[`/admin/v1`](../operations/admin-api.md). Stateful inference is enabled only
after convergence publishes a complete serving snapshot; otherwise the gateway
remains fail-closed.
Redis is a hot-state backend in both modes and is never a durable store of
record.

## Capability matrix

| Need | Backend | Main cost |
| --- | --- | --- |
| Approximate per-replica budget | in-memory | A fleet of N replicas can admit roughly N times the configured cap. |
| Exact shared subject/namespace budget | Redis or Postgres | Backend participates in request admission. |
| Exact cross-replica in-flight rate limit | Redis | Redis participates in request admission. |
| Precise minted-token JTI revocation | Redis or Postgres | Revocation lookup participates in authenticated requests. |
| Durable usage rows | Postgres | Buffered writes, migrations, backup/restore ownership. |
| Usage in an observability backend | OTLP | Collector connectivity is required at boot, but no Axond datastore. |

The default shared-backend outage policy is `on_unavailable = "deny"`. That
turns dependency failure into a typed `503 budget_unavailable`,
`rate_limit_unavailable`, or `revocation_unavailable`. `allow` is an explicit
fail-open decision and should be documented as such in the deployment review.

## Redis

Typical shared configuration:

```toml
[budget]
backend = "redis"
limit_microdollars = 10_000_000
namespace_limit_microdollars = 100_000_000
dsn_env = "AXOND_BUDGET_REDIS_URL"
on_unavailable = "deny"

[rate_limit]
backend = "redis"
max_in_flight = 32
dsn_env = "AXOND_RATE_LIMIT_REDIS_URL"
on_unavailable = "deny"

[revocation]
backend = "redis"
dsn_env = "AXOND_REVOCATION_REDIS_URL"
on_unavailable = "deny"
```

Use TLS-capable Redis URLs in production. Give separate deployments separate
key prefixes, especially when namespace-cap migration state differs.

### Enabling a namespace-wide cap

`namespace_limit_microdollars` changes the Redis budget key layout. It is a
stop-the-fleet migration, not a live config flip:

1. Add the namespace cap to the configuration that will serve after migration.
2. Stop and drain every Axond replica using the key prefix.
3. Run the migration with that same config.
4. Start the new fleet.

```bash
AXOND_CONFIG=/etc/axond/axond.toml \
  axond budget migrate-redis
```

The migration is resumable and idempotent. Axond fences incomplete, mixed, or
old-layout state at boot rather than silently resetting spend. Re-run the
command after an interruption. Do not run old and new binaries together.

In-flight reservations are not migrated, which is why traffic must be stopped.
Turning the cap off also requires an explicit spend reset or a new key prefix;
there is no lossless un-migrate operation.

The migration refuses ambiguous old keys, including cases where removed or
prefix-overlapping namespace IDs prevent safe attribution. Resolve or explicitly
delete those keys before retrying. The complete invariants are in
[ADR 0010](../adr/0010-shared-budget-backends-and-charging-policy.md).

## Postgres

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
(including an older `usage_v1_001` migration) refuses to boot and names the
ordered files to apply rather than dropping batches at insert time. This is an
intentional fail-closed contract: migrate the table in place and preserve its
history; do not recreate it merely because the refusal mentions the base DDL.
The check resolves an unqualified table through the connection's `search_path`,
just like the `INSERT`, so a DSN selecting `billing` is checked in `billing`,
not hard-coded `public`. A table that does not exist yet is not checked: with
`create_table = false` its creation is yours to sequence, and until it exists
the off-path sink drops rejected batches and increments the dropped-record
metric.

Shared budgets start with `ops/postgres/budget_v1.sql`. Enabling an exact
namespace cap requires `ops/postgres/budget_v2.sql` while the fleet is stopped:

```bash
psql "$AXOND_BUDGET_POSTGRES_DSN" -f ops/postgres/budget_v1.sql
psql "$AXOND_BUDGET_POSTGRES_DSN" -f ops/postgres/budget_v2.sql
```

The v2 migration backfills namespace spend and installs fences that reject
writes from cap-unaware sessions. It takes an exclusive lock so a concurrent v1
settlement cannot disappear from the namespace total. Re-running it is
idempotent.

Precise Postgres revocation uses `ops/postgres/revocation_v1.sql` and the table
configured under `[revocation]`.

Tenant provider credentials live in `ops/postgres/secret_store_v1.sql`, in the
database `[secret_store]` references — normally the control plane's own. A booting
replica applies it itself unless `create_table = false`, which is the setting for a
deployment whose gateway role holds no DDL grant:

```bash
psql "$AXOND_CONTROL_PLANE_DSN" -f ops/postgres/secret_store_v1.sql
```

Rows hold ciphertext only: material is sealed under a per-version data key which is
sealed under the deployment KEK named by `kek_env` or `kek_file`, so this table is
safe to dump and useless without that key
([ADR 0039](../adr/0039-envelope-encrypted-secret-store-and-snapshot-time-resolution.md)).
Back the KEK up somewhere the database's backups are not: losing it makes every
stored version unrecoverable, and there is no recovery path but restaging material.
Nothing here is on the ordinary request path — the store is read while a
candidate revision is compiled. An outage stalls administration and ordinary
convergence, while replicas with an active snapshot keep serving. A replica
that cold-boots during the outage can use its encrypted compiled-serving cache
when one was written after a prior admitted revision.

The control-plane journal of `mode = "stateful"` is the exception to applying DDL
by hand: it keeps a migration ledger, so the binary can tell what a database
contains and move it forward.

```bash
axond migrate status --config /etc/axond/axond.toml   # read-only
axond migrate apply  --config /etc/axond/axond.toml   # forward-only, idempotent
axond migrate adopt  --config /etc/axond/axond.toml   # only after applying its DDL by hand
axond check preflight --config /etc/axond/axond.toml  # read-only boot rehearsal
```

Applying the journal's DDL with `psql` anyway leaves the ledger empty, which is a
refusal rather than a fresh database: `axond migrate adopt` records the baseline
the objects present account for, and refuses if they do not account for one.

The stores on this page have no ledger, so nothing above orchestrates them;
`axond check preflight` still checks that their `dsn_env` references resolve,
because an unset one is a boot failure whichever section it is in. See
[the control-plane journal](../operations/control-plane-journal.md#operator-commands).

Use `sslmode=require` in production DSNs. Axond uses rustls and webpki roots.

## Supported versions

| Backend | Supported | Exercised in CI | Floor is enforced by |
| --- | --- | --- | --- |
| PostgreSQL | 14, 15, 16, 17 | `postgres:17.6-alpine` | The control-plane backend: boot and `axond check preflight` read `server_version_num` and refuse an older server. A usage sink, budget backend, or revocation table on Postgres is not version-checked. |
| Redis | 6.2, 7.x, 8.x | `redis:7.4.2-alpine` | Nothing at boot; an older server fails the first enforcement write instead. |

The floors are the oldest servers the shipped statements can run on, not a
preference. PostgreSQL 14 is where the journal's identity columns and
`ON CONFLICT` against partial unique indexes arrive
(`MINIMUM_SERVER_VERSION_NUM`). Redis 6.2 is where `SET … PXAT` arrives, which
the revocation liveness write uses; on 6.0 that write is a command error, so a
revocation backend on an older Redis fails closed at the first probe rather than
silently. `ops/check-deploy-manifests.py` keeps this table, the enforced
PostgreSQL floor, and the CI service images from drifting apart.

The version refusal is the control plane's alone, because that is the only store
whose schema axond owns and migrates. The other Postgres-backed features run DDL
from the same supported range but read no `server_version_num`, so on an older
server they fail their statements rather than their connection — Redis's outcome,
one step later. A deployment that uses them without the control plane is on a
documented floor, not an enforced one, and should check the server itself.

Newer majors than the exercised ones are supported in the sense that nothing
refuses them, and are not tested here; a deployment on one is on its own
evidence. Upgrading a backend major is an operation on the backend, not on axond:
the DDL is version-independent within the supported range, so a PostgreSQL major
upgrade (`pg_upgrade` or dump and restore) needs no axond change and no
migration. Run `axond check preflight` afterwards — it re-reads the server
version and the schema, which is exactly the pair a major upgrade can move — and
verify a restore into the new major with
[the restore drill](../operations/backup-and-recovery.md#the-drill).

## Availability and recovery

- Initial backend connectivity is validated before the listener binds.
- Runtime failures follow each feature's `on_unavailable` policy.
- Usage sinks are off the request path: full buffers or rejected writes drop
  with metrics rather than stall provider traffic.
- Budget reservations have expiry/recovery behavior, but operators must still
  alert on backend latency and unavailable denials.
- Restore procedures must preserve schema versions and Redis layout markers,
  not only application rows/counters.
- Recovery objectives, backup mechanisms, and the executable drill are in
  [Backup, restore, and point-in-time recovery](../operations/backup-and-recovery.md).

See [Configuration](../configuration.md),
[Observability](../observability.md), and
[Upgrades](../operations/upgrades.md) for exact fields and rollout checks. For
the planned separation of control-plane availability from data-plane
availability, including the per-dependency outage matrix, read
[ADR 0027](../adr/0027-stateless-and-stateful-operating-modes.md).
