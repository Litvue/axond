# 32. Operator commands: stateful preflight and forward-only migration

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
  strictly — absent, unrecorded, current, behind, ahead of this build, checksum
  drift, a hole in the applied prefix, a renamed migration, or a table this build
  cannot read as the ledger. Only *behind* is applied; everything else refuses,
  because a disagreement about history is an operator's decision and no retry
  changes it. Applying is serialized by a transaction-scoped advisory lock at an
  explicit isolation level, so a second or concurrent `apply` is a no-op rather
  than a second migration.
- **An empty ledger is a refusal, not a fresh database.** A ledger table that
  exists and records nothing is what applying the DDL out of band leaves behind,
  and it is indistinguishable from an untouched database with a hand-created
  ledger: the ledger is the only record of what ran. Migrating it from zero would
  replay every shipped file over objects that may already exist, which holds only
  while every statement is `IF NOT EXISTS` and breaks on the first `ALTER TABLE`
  or backfill. So the baseline is the operator's to state — by
  `axond migrate adopt`, per the amendment below — or the empty ledger is theirs to
  drop. An *absent* ledger stays a fresh install: nothing has run, so there is
  nothing to replay.
- **`[control_plane] migrate` governs boot only.** It stays `false` by default: the
  supported order is one `apply` before any replica starts. A replica checks the
  schema either way and refuses one it does not recognise.
- **Errors are typed by what an operator should do**: an unreachable database is
  retryable, a refused schema is not, and no output or error ever carries a DSN.
  A server-reported rejection of the migration DDL — a missing target schema
  (`3F000`) or a role that may not create objects (`42501`) — is a refusal too,
  because a rollout gate that retries it loops forever; only the transient
  SQLSTATE classes (connection, rollback, resources, prerequisite state, operator
  intervention, system) stay outages. The boot path classifies the same way, so a
  replica migrating at boot and an operator running `apply` disagree about nothing.
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

### Security review trigger

Trigger 5, [persistence, migrations, telemetry, and
usage](../security/threat-model-review.md#5-persistence-migrations-telemetry-and-usage),
fires: the journal's migration history gains a stricter classification, a command
applies its DDL, and `[control_plane] schema` reaches `SET search_path`. No
shipped SQL file changes, so the byte-identity gates hold unchanged; the schema
name must be one unqualified identifier; every message and report names
`$GW_CONTROL_PLANE_DSN` rather than a connection string. No threat-model update
is owed — no new state tier, store dependency, emitted field, or request-path
query — and the operator-visible release impact is the three optional
`[control_plane]` keys plus the refusal of an empty ledger, which an operator who
applied the DDL by hand resolves by dropping the table or recording the baseline
(`axond migrate adopt`, per the amendment above).

## Amendment (2026-08-12): adopting an empty ledger is an operation, not an `INSERT`

The decision above left the empty-ledger baseline as an operator's hand-written
`INSERT INTO axond_cp_schema_migration`, printed by the refusal. That is the right
*policy* with the wrong *mechanism*, and it has to be settled before the first
non-idempotent migration ships, because that is the release where a wrong baseline
stops being a bad report and starts being a corrupted database:

- It is unverified. A pasted `INSERT` states "v1 was applied" with nothing checking
  that it was. A ledger row is afterwards indistinguishable from one an `apply`
  wrote, so a mistake there is laundered into the history every later
  classification, boot check, and migration reads — and the mistake it invites most
  is recording a baseline for a database where the DDL half applied.
- It has no lock. Two operators, or an operator and a booting replica with
  `migrate = true`, can interleave a hand `INSERT` with an `apply`.
- It is unattractive enough that the tempting alternative is dropping the ledger
  and re-applying, which is the replay this build refuses.

**`axond migrate adopt` is added**, and it is the only operation that records a
ledger row for DDL it did not run:

- It applies to exactly one status, *unrecorded*. Every other status is refused,
  including *absent* (that is `apply`'s job) and every recorded history that
  disagrees with this build — adoption reconciles an unrecorded history and never
  repairs a recorded one. A ledger that already records a history is reported and
  left alone, so `adopt` is idempotent and a stray one in a rollout is not an edit.
- **It records only what the database accounts for.** The baseline is the longest
  run of shipped migrations from v1 whose every declared table is present, read
  from each migration's own embedded SQL rather than from a hand-maintained list
  — so a migration that adds a table cannot ship without adoption looking for it.
  No object present is a refusal (nothing was applied: drop the ledger and
  `apply`); a partly-present migration is a refusal naming the missing tables
  (neither applied nor unapplied is true); a migration that declares no table is a
  refusal, because an `ALTER`-only or backfill migration is precisely the one whose
  effect the catalogue cannot report, and adopting it would be recording a guess.
- **It executes no migration SQL at all**, which is what makes it safe against a
  database that already has the objects, whether or not the shipped statements are
  idempotent — the property the hand-`INSERT` path shared but the drop-and-replay
  alternative never had.
- It is one transaction under the same transaction-scoped advisory lock `apply`
  takes, with the status re-read under the lock and re-classified after the write,
  so a refusal leaves no partial baseline and concurrent adoptions are one
  adoption.
- An adopted baseline below the required version reports what is still pending and
  exits non-zero; `apply` then migrates forward from it, unchanged.

Unchanged: `preflight` and `status` still cannot write, the classifications are the
same, an empty ledger is still a refusal for `status`, `apply`, and boot, and
recording the baseline by hand still works and is classified identically. `adopt`
is the checked version of that `INSERT`, not a new policy — and stating a baseline
for an unobservable migration remains the operator's own `INSERT` to make.

State tier: unchanged. Tier 2 only where the config already selected it.

Security review trigger 5 fires again — the control-plane journal gains a write
path. No shipped SQL file changes, so the byte-identity gates hold; the write is
ledger rows only, under the journal's existing lock, with no DDL executed; output
and errors still name `$GW_CONTROL_PLANE_DSN` and never a DSN. No threat-model
update is owed: no new state tier, store dependency, emitted field, request-path
query, or trust boundary — the actor is the same operator who could already write
that row with `psql`, and the command narrows what they can assert rather than
widening it. Release impact is one new optional command; no existing invocation,
exit code, or configuration key changes.

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
