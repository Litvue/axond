# Upgrades and rollback

Treat the configuration, binary/image, and Postgres schema as one
deployment unit. Read the release's `CHANGELOG.md` entry before every rollout.

## Compatibility policy

- A `0.x` patch release is intended to accept the previous patch's valid
  configuration and preserve documented HTTP/usage contracts.
- A breaking configuration or contract change requires a minor release and a
  migration note.
- Typed error `type` values are more stable than human-readable messages.
- The complete promise is in the [compatibility contract](../compatibility.md).

## Preflight

1. Verify the release artifact, signature, provenance, and SBOM.
2. Read every breaking-change and migration entry since the deployed version.
3. Validate the candidate configuration against the new binary in a staging or
   canary environment.
4. Apply additive Postgres usage migrations in filename order before deploying
   writers, including `ops/postgres/usage_outbox_v1.sql` before any replica that
   enables `[usage_journal] backend = "postgres"`. The usage sink checks every
   bound column at connection time and fails closed with the ordered migration
   remedy; it does not allow a writer to boot and silently drop rows.
5. Apply `ops/postgres/catalog_v1.sql` before any replica that sets
   `[catalog] store = "postgres"`. A deployment that configures no `[catalog]`
   section imports nothing and needs none of it; the DDL is additive and
   idempotent, and applying it early costs two empty tables
   ([ADR 0051](../adr/0051-durable-catalogue-snapshots-and-refresh-orchestration.md),
   [ADR 0055](../adr/0055-catalogue-imports-in-a-running-deployment.md)).
6. Apply `ops/postgres/store_budget_v1.sql` (or `create_table = true`) before
   any replica that sets `[storage] backend = "postgres"`. The Store ledger is
   `axond_store_budget*`. A leftover withdrawn-backend `axond_budget` (PK
   `(namespace, subject)`) is left in place; spend is not migrated (subject vs
   period). Connect may RENAME leftover draft Store tables (`axond_budget*`
   with a `period` column) to `axond_store_budget*`, including when empty new
   tables already exist from a hand-applied `store_budget_v1.sql` and the draft
   still has spend (empty new relations are dropped first; non-empty new
   tables are kept). That needs table-rename privilege; migration-only roles
   should run the rename out of band before boot. Apply
   `ops/postgres/store_namespace_incarnation_v1.sql` on every replica before
   DELETE `/api/v1/namespaces/{ns}` is used. Mixed old/new reservation rows
   are not supported. `create_table = false` probes
   `axond_namespace_incarnation` and reservation tombstones and fails closed
   if they are missing. After a draft rename, connect still ADD COLUMN
   IF NOT EXISTS incarnation on the reservation table.
7. Verify ingress streaming behavior and client retries.
8. Retain the old artifact and old configuration for rollback where compatible.

## Ordinary rolling upgrade

An upgrade with no state-layout or configuration break can roll replicas behind
a load balancer:

1. Start a new replica and wait for `/readyz`.
2. Add it to service.
3. Send `SIGTERM` to one old replica. Its `/readyz` starts failing at once, so a
   load balancer that watches readiness removes it without operator action.
4. Wait for it to exit — bounded by
   `drain_grace_ms + deadline_ms + flush_timeout_ms` — and continue.

The replica serves through the readiness drain, refuses new work afterwards with
a typed `503` (`draining`), cuts streams still open at the deadline while still
recording their partial spend, and flushes usage and telemetry before exiting.
Stopping timeouts (`terminationGracePeriodSeconds`, `TimeoutStopSec`) must stay
above that sum so no replica is killed mid-flush. Clients should retry requests
that end before response commitment.

Replica-local circuits and credential health start empty on replacement. The
Store and durable usage keep their state.

This sequence is executed on every change against a fleet of real replicas behind
a readiness-driven balancer, including the rollback limits below:
[Kubernetes deployment](../deployment/kubernetes.md).

A billing-grade replica also drains its usage outbox within that budget, and
reports what it could not deliver. Undelivered events are not lost — the
replacement replica claims them once the leases expire — so
`usage_journal_drained=false` is a backlog to watch, not an incident. Events an
older replica cannot read because a newer replica wrote them are skipped rather
than condemned, so a mixed-version fleet is safe in both directions of the roll
([usage outbox](./usage-outbox.md#upgrades-and-version-skew)).

## Schema migrations

Usage schema migrations are additive and must be applied before the new binary,
in filename order. This release adds
`ops/postgres/usage_v2_001_add_price_identity.sql` (nullable `price_book`,
`price_book_checksum`, `price_catalog`), which follows
`ops/postgres/usage_v1_001_add_signer_kid.sql`. A Postgres usage sink compares
every column the writer binds against the existing table while it connects, so a
replica started before either migration refuses to boot and names the ordered
files to apply rather than dropping rows. This is intentional fail-closed
behavior; apply the migrations in place and preserve existing usage history.
Mixed versions are safe in both directions: the
columns are nullable, and an older binary neither writes nor reads them. Rolling
back does not require dropping them.

The usage *outbox* is stricter still for a billing-grade deployment: with
`[usage_journal] backend = "postgres"` the outbox is on the request path, so a
missing or unreadable outbox table is `503 usage_not_durable` per request under
the default policy, not an off-path drop. Apply outbox DDL before the writers,
and drain the outbox before rolling back to a build that predates a row version.

## Rollback

Rollback is safe only when the old binary understands the current config and
state layout.

- An ordinary patch rollback with no migrations can use the retained image and
  configuration.
- Do not roll back to a binary that predates a Store table it now relies on,
  such as `axond_namespace_incarnation`.
- Published crates.io versions and release tags are immutable. Fix forward with
  a new patch release rather than replacing artifacts.

## Post-deploy verification

- `/healthz` and `/readyz` pass on every new replica.
- Authenticated `/ns/{ns}/v1/models` shows the expected models.
- A buffered and streamed request succeeds through production ingress.
- Usage reaches every configured sink.
- Budget and admission denial metrics have expected baselines.
- No replica reports a rejected config.
