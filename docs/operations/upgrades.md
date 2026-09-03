# Upgrades and rollback

Treat the configuration, binary/image, Redis layout, and Postgres schema as one
deployment unit. Read the release's `CHANGELOG.md` entry before every rollout.

## Compatibility policy

- A `0.x` patch release is intended to accept the previous patch's valid
  configuration and preserve documented HTTP/usage contracts.
- A breaking configuration or contract change requires a minor release and a
  migration note.
- Typed error `type` values are more stable than human-readable messages.
- The complete promise is in the [compatibility contract](../compatibility.md).

## v0.4.0 `gateway-core` API migration

v0.4.0 intentionally removes the unused public `governance` module and its
`Governance`, `GovernanceKey`, `GovernanceLimits`, and `Admission` exports.
Admission, rate limiting, and usage accounting are runtime concerns; embedders
must keep them in their host or use Axond's configured admission and durable
budget implementations. `DeterministicGuardrail` is a content-policy primitive,
not a replacement rate limiter.

The legacy inbound-only `Guardrail`, `RegexGuardrail`, `GuardrailPolicy`,
`GuardrailRequest`, and `GuardrailVerdict` API is also removed. Replace it with
`DeterministicGuardrail`, declared and invoked through the common `Middleware`
contract:

1. Build ordered `GuardrailRule` values with `GuardrailAction::Block` or
   `GuardrailAction::Redact`.
2. Create a fail-closed `MiddlewareDeclaration` with the request, response, and
   stream-event scopes needed by the host. Set `mutates_response = true` when
   any redaction rule can restore caller text into output.
3. Compile with a namespace-derived 32-byte key. Hosts with an inbound body
   ceiling should call `DeterministicGuardrail::compile_with_request_limit` and
   pass that exact serialized whole-request limit; `compile` retains a finite
   64 MiB fallback for standalone callers.
4. Import the `Middleware` trait and invoke `apply_for_surface` with the trusted
   `MiddlewareSurface` selected from the authenticated route. Preserve the
   request outcome's opaque `MiddlewareState` for response and stream callbacks,
   and call `finish_stream` after semantic completion and strict EOF.

Do not call the unscoped `apply` compatibility entry point for deterministic
redaction. It cannot safely infer Chat, Messages, Embeddings, or Responses from
provider-controlled JSON and therefore fails closed outside core's unit tests.
Unknown structures and matches in routing/protocol fields now refuse atomically
instead of being rewritten.

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
   should run the rename out of band before boot.
7. Complete any stop-the-fleet Redis/Postgres budget migration before starting
   namespace-cap-aware replicas.
8. Verify ingress streaming behavior and client retries.
9. Retain the old artifact and old configuration for rollback where compatible.

In stateful mode the new binary owns the control-plane half of that list. Run
these from the new artifact, with the fleet still on the old one:

```bash
axond migrate status --config /etc/axond/axond.toml   # read-only: what would apply?
axond migrate apply  --config /etc/axond/axond.toml   # forward-only, idempotent
axond check preflight --config /etc/axond/axond.toml  # read-only: would a replica boot?
```

`status` and `preflight` cannot write to a database, so they are safe against
production at any time, including from a canary host. `apply` is the only mutation,
and it is safe before replicas start and while they are starting. All three exit
non-zero when the deployment is not ready, so a rollout can gate on them. The
ordering and the full state table are in
[the control-plane journal](control-plane-journal.md#operator-commands): a schema
reported *Ahead*, *Drifted*, *Incomplete*, *Renamed*, or *Malformed* stops the
rollout rather than being migrated over. A schema reported *Unrecorded* — the
journal's DDL applied out of band, so the ledger exists and records nothing — stops
it too, and is resolved once with
[`axond migrate adopt`](control-plane-journal.md#applied-out-of-band-psql-then-adopt)
before the `apply`.

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

Replica-local circuits and credential health start empty on replacement. Shared
budgets, rate limits, revocation, and durable usage retain backend state.

The v0.4.0 guardrail release is a state-layout exception to this ordinary
sequence for replicas that use the persistent compiled-serving cache. Its cache
payload is layout v4, and neither side of the v3/v4 boundary is cold-restored by
the other. Keep the control plane and SecretStore reachable while replacing one
StatefulSet ordinal at a time. For each ordinal, wait for the admitted revision
to become active and verify a successful v4 last-known-good export on that
ordinal's PVC before deleting the next Pod. A Ready process whose cache export
failed may serve traffic, but its PVC is not qualified for outage recovery. Do
not replace the fleet during a control-plane outage using pre-upgrade caches;
after every ordinal has exported v4, repeat the documented cold-start outage
drill before treating the upgraded fleet as recoverable.

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

## Migrations that are not rolling

Exact namespace-wide budgets require a stopped fleet:

- Redis: configure the cap, stop all writers, run
  `axond budget migrate-redis`, then start the new fleet.
- Postgres: stop/drain, apply `ops/postgres/budget_v2.sql`, then start the
  cap-aware fleet.

Do not mix cap-aware and cap-unaware replicas. Both backends contain fences so
an unsafe mix fails loudly rather than undercounting spend.

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

Price-book bodies are now written as `axond.price-book.v2`; the new
`catalog_version` field is part of the immutable book identity. Retained v1 books
remain readable for compatibility but have no numeric catalogue version, so new
requests charged from one record `catalog_version = 0` until the book is
republished as v2.

The usage *outbox* is stricter still for a billing-grade deployment: with
`[usage_journal] backend = "postgres"` the outbox is on the request path, so a
missing or unreadable outbox table is `503 usage_not_durable` per request under
the default policy, not an off-path drop. Apply outbox DDL before the writers,
and drain the outbox before rolling back to a build that predates a row version.

## Rollback

For the shipped stateful Kubernetes overlay, use the
[stateful deployment runbook](./stateful-deployment-runbook.md). It separates
an /admin/v1 desired-state rollback (a new journal revision) from a compatible
image rollback (a Recreate replacement), and gives the updatedReplicas and
probe checks that apply while the fleet intentionally has no Ready Pods.

Rollback is safe only when the old binary understands the current config and
state layout.

- An ordinary patch rollback with no migrations can use the retained image and
  configuration.
- Do not roll a pre-namespace-cap binary onto a migrated Redis/Postgres layout.
  The boot/runtime fences reject it, and bypassing them would reset or
  undercount spend.
- Disabling a Redis namespace cap requires accepting a spend reset and deleting
  the migrated prefix/layout marker or moving to a new prefix.
- Disabling a Postgres namespace cap requires a controlled fleet stop and
  removal of the namespace-fence triggers; decide how spend semantics change.
- Published crates.io versions and release tags are immutable. Fix forward with
  a new patch release rather than replacing artifacts.

## Post-deploy verification

- `/healthz` and `/readyz` pass on every new replica.
- Authenticated `/v1/models` shows expected namespace aliases.
- A buffered and streamed request succeeds through production ingress.
- Usage reaches every configured sink.
- Budget, rate-limit, and revocation denial metrics have expected baselines.
- No replica reports a rejected config or backend-layout fence.
