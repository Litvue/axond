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

## Preflight

1. Verify the release artifact, signature, provenance, and SBOM.
2. Read every breaking-change and migration entry since the deployed version.
3. Validate the candidate configuration against the new binary in a staging or
   canary environment.
4. Apply additive Postgres usage migrations before deploying writers.
5. Complete any stop-the-fleet Redis/Postgres budget migration before starting
   namespace-cap-aware replicas.
6. Verify ingress streaming behavior and client retries.
7. Retain the old artifact and old configuration for rollback where compatible.

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

This sequence is executed on every change against a fleet of real replicas behind
a readiness-driven balancer, including the rollback limits below:
[rollout qualification](./rollout.md).

## Migrations that are not rolling

Exact namespace-wide budgets require a stopped fleet:

- Redis: configure the cap, stop all writers, run
  `axond budget migrate-redis`, then start the new fleet.
- Postgres: stop/drain, apply `ops/postgres/budget_v2.sql`, then start the
  cap-aware fleet.

Do not mix cap-aware and cap-unaware replicas. Both backends contain fences so
an unsafe mix fails loudly rather than undercounting spend.

Usage schema migrations are additive and should be applied before the new
binary. Missing usage columns cause off-path sink drops rather than request
failure, which still makes migration ordering operationally important.

## Rollback

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
