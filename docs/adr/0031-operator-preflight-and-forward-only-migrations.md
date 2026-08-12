# 31. Operator commands: stateful preflight and forward-only migration

Date: 2026-08-12

## Status

Accepted

Implements the operational half of the stateful mode chosen in
[ADR 0027](./0027-stateless-and-stateful-operating-modes.md), and keeps the
hermetic Tier 0 boot of [ADR 0018](./0018-tier-0-hermetic-boot-gate.md) intact.

## Context

Stateful mode gives a deployment a control-plane database, and therefore a
schema that must exist and match the binary before any replica serves traffic.
Two questions had no answer outside a running process: *would a replica boot
against this configuration?* and *what schema does this database have?* Both were
answered by starting a replica and reading a crash loop — the most expensive
possible place to find out, because the listener is already gone and the old
replica is already terminating.

A boot-time migration is not an answer either. A rolling deployment has several
replicas starting at once against one database, so "migrate on boot" means one
replica applying DDL while the others read the schema it is changing, with no
operator in the loop and no ordering guarantee between them.

## Decision

Three commands, run by an operator *before* replicas, with one grammar —
`axond <command> <action> --config PATH`:

- `axond check preflight` — the boot rehearsal: config parse and validation, the
  config file's ownership and mode, every reference a boot resolves, control-plane
  reachability, schema compatibility.
- `axond migrate status` — what schema this database has, and what an apply would
  do.
- `axond migrate apply` — apply pending migrations, forward only.

Their boundaries:

- **`preflight` and `status` cannot write**, enforced by the server rather than by
  convention: the control plane is opened on a maintenance path that does not
  prepare a schema, with migration permission forced off, and the ledger is read
  inside a `READ ONLY` transaction. `apply` is the only mutation, and it is a
  command an operator types on purpose.
- **Forward only, and only with a complete history.** The ledger
  (`axond_cp_schema_migration`: version, file name, checksum) is classified
  strictly — absent, current, behind, ahead of this build, checksum drift, a hole
  in the applied prefix, a renamed migration, or a table this build cannot read as
  the ledger. Only *behind* is applied; everything else refuses, because a
  disagreement about history is an operator's decision and no retry changes it.
  Applying is serialized by a transaction-scoped advisory lock at an explicit
  isolation level, so a second or concurrent `apply` is a no-op rather than a
  second migration.
- **`[control_plane] migrate` governs boot only.** It stays `false` by default: the
  supported order is one `apply` before any replica starts. A replica checks the
  schema either way and refuses one it does not recognise.
- **Errors are typed by what an operator should do**: an unreachable database is
  retryable, a refused schema is not, and no output or error ever carries a DSN.
- Only the control-plane journal is migrated. The usage, budget, and revocation
  stores have no ledger, so nothing here orchestrates them; preflight still checks
  that their references resolve, because an unset one is a boot failure whichever
  section it is in.

### State tier

Tier 0 (config-only) for the commands themselves. `preflight` on a stateless
config requires no datastore and reports the control-plane checks as skipped;
`migrate` has nothing to do. Tier 2 (Postgres) is only reached when the config
already selected it, so this decision does not raise the tier of any existing
deployment.

## Consequences

A schema disagreement, an unset reference, or an unreachable database becomes a
non-zero exit from a command before a rollout instead of a crash loop during one,
and the fresh-install and upgrade orders are checkable rather than conventional.

Costs: an operator step is added to a stateful upgrade — `apply` must run once,
from one place, with the new binary, before replicas roll onto it. Preflight is
deliberately stricter than boot about the config file's mode, so a config another
account can rewrite fails the gate while it would start a gateway. And until the
durable control plane is wired to the runtime, `serve` still refuses
`mode = "stateful"`; a stateful preflight reports that as a failure rather than
promising a boot that cannot happen, while the database checks it accompanies
still run. Preflight reads that refusal from `serve`'s own definition, so the two
cannot disagree and lifting the refusal removes the reported failure with it.
