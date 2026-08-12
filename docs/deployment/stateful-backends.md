# Stateful backends

Axond is config-only by default. Add Redis or Postgres only when a feature must
coordinate replicas or survive process replacement.

This guide covers the **stateless** operating mode: TOML owns every resource, and
a backend below is an opt-in for one capability. "Stateful backend" here does not
mean the stateful control plane accepted in
[ADR 0027](../adr/0027-stateless-and-stateful-operating-modes.md), which moves
resource ownership into Postgres behind `/admin/v1` and is not implemented yet.
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
```

Apply additive usage migrations in filename order before deploying a binary
that writes the new shape. A missing column does not stop requests; the
off-path sink drops rejected batches and increments the dropped-record metric.

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

The control-plane journal of `mode = "stateful"` is the exception to applying DDL
by hand: it keeps a migration ledger, so the binary can tell what a database
contains and move it forward.

```bash
axond migrate status --config /etc/axond/axond.toml   # read-only
axond migrate apply  --config /etc/axond/axond.toml   # forward-only, idempotent
axond check preflight --config /etc/axond/axond.toml  # read-only boot rehearsal
```

The stores on this page have no ledger, so nothing above orchestrates them;
`axond check preflight` still checks that their `dsn_env` references resolve,
because an unset one is a boot failure whichever section it is in. See
[the control-plane journal](../operations/control-plane-journal.md#operator-commands).

Use `sslmode=require` in production DSNs. Axond uses rustls and webpki roots.

## Availability and recovery

- Initial backend connectivity is validated before the listener binds.
- Runtime failures follow each feature's `on_unavailable` policy.
- Usage sinks are off the request path: full buffers or rejected writes drop
  with metrics rather than stall provider traffic.
- Budget reservations have expiry/recovery behavior, but operators must still
  alert on backend latency and unavailable denials.
- Restore procedures must preserve schema versions and Redis layout markers,
  not only application rows/counters.

See [Configuration](../configuration.md),
[Observability](../observability.md), and
[Upgrades](../operations/upgrades.md) for exact fields and rollout checks. For
the planned separation of control-plane availability from data-plane
availability, including the per-dependency outage matrix, read
[ADR 0027](../adr/0027-stateless-and-stateful-operating-modes.md).
