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

## Ordinary rolling upgrade

An upgrade with no state-layout or configuration break can roll replicas behind
a load balancer:

1. Start a new replica and wait for `/readyz`.
2. Add it to service.
3. Remove one old replica from service and allow the external drain window.
4. Stop it and continue.

The current binary does not implement application-level SIGTERM draining.
Endpoint removal and client retry behavior are therefore part of the rollout.

Replica-local circuits and credential health start empty on replacement. Shared
budgets, rate limits, revocation, and durable usage retain backend state.

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
