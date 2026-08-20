# 32. Operator commands: stateful preflight and forward-only migration

Date: 2026-08-12

## Status

Accepted for the PostgreSQL control-plane backend, partially superseded by
[ADR 0062](./0062-blob-backed-flat-namespace-control-plane.md).

Preflight remains required. Forward-only PostgreSQL DDL and its migration Job
are no longer requirements of the preferred stateful deployment; the blob
backend instead preflights protocol version, credentials, object limits, and a
live native conditional-write probe.

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
- **It records only what the database accounts for, statement by statement.** The
  baseline is the longest run of shipped migrations from v1 whose *every* statement
  is confirmed, parsed from each migration's own embedded SQL rather than from a
  hand-maintained list — so a statement cannot ship without adoption accounting for
  it. What is confirmable is what one catalogue question answers about one object:
  `CREATE TABLE` and `CREATE INDEX` by the relation being present; `INSERT ... ON
  CONFLICT DO NOTHING` by the target table not being empty (idempotent by
  construction, which is what makes a row usable as evidence); `ADD COLUMN` by
  `pg_attribute`; a **named** `ADD CONSTRAINT` by `pg_constraint`; `ENABLE`/`FORCE
  ROW LEVEL SECURITY` by `pg_class.relrowsecurity` and `relforcerowsecurity`, which
  are two separate answers because `ENABLE` alone enforces nothing against a table's
  owner; and `CREATE POLICY` by `pg_policy`, keyed by the table as well as the
  policy, since two tables can each carry an `..._isolation` policy. A `DROP` of any
  of those is confirmed by the thing being *gone*, which is real evidence (v2 drops
  a constraint v1 created) but never evidence *for* a version on its own: whether a
  version ran is read from what it left behind, so a version that only removes
  things is unconfirmable, and an untouched database is untouched rather than half
  way through the version above it. Nor is something the version *replaced*: a
  `DROP` and a `CREATE` of the same object in one file — how v2 rewrites v1's
  `..._actor_attribution` constraints, and how it re-declares every `..._isolation`
  policy — leaves a database that ran the earlier version holding the same object
  as one that ran this one, so its presence is required of an applied database and
  is never read as proof of which version wrote it. Otherwise every deployment
  hand-applied only as far as v1 would be refused as half-way through v2, and such
  a statement is also the one kind it is safe to reach a second time, which is what
  makes leaving that version to `apply` the right answer rather than a risk. Each
  form names its object unqualified, so an
  unnamed `CREATE INDEX ON t (c)`, an unnamed constraint PostgreSQL names for
  itself, or a schema-qualified `other.t` is unconfirmable rather than probed for
  under a name that is not one; an `ALTER TABLE`'s clause list is read clause by
  clause, and one clause outside that set voids the statement. The parse skips
  comments (`--`, nested `/* */`) and quoted regions (`'...'`, `$tag$ ... $tag$`) in both
  its statement split and its keywords, so prose or a function body cannot
  contribute the keywords a statement is judged by; a region that does not close
  where that parse says it does makes the file unconfirmable rather than a
  shorter list of statements. Evidence more than one migration declares — a
  shared seed target, or an object two files both `CREATE ... IF NOT EXISTS` —
  confirms at most one of them and so confirms none. Nothing confirmed is a
  refusal (nothing was applied: drop the ledger and `apply`); a partly-confirmed
  migration is a refusal naming what is missing (neither applied nor unapplied
  is true). Every probe is qualified to
  `current_schema()` — the schema an `apply` would create these objects in — rather
  than resolved down the DSN's `search_path`, so a second install's journal in
  `public` is not read as evidence about this one.
- **A dynamic `DO $$ ... $$` block is interpreted, never assumed.** v2 guards its
  chained tables with a `FOREACH` over a literal array executing `format()`
  templates, and adoption reads it by rendering the block's own templates for the
  block's own names and parsing each result with the same parser every other
  statement goes through. So a table added to that array is a policy adoption goes
  looking for, and the alternative — a list of the six tables written out beside the
  parser — is exactly the hand-maintained list this design refuses. Only that one
  shape is read: a condition, names from a query, a template argument that is not
  the loop variable (or the loop variable with a literal suffix, which is how
  `<table>_isolation` is spelled), a `%L`, a placeholder without an argument, or
  `EXECUTE` of anything but a `format()` template makes the block unconfirmable, and
  a rendered statement is then judged on its own merits like any other.
- **A migration containing one statement adoption cannot confirm blocks adoption of
  the database entirely,** wherever in the shipped history it sits, including the
  versions below it. That is fail-closed in both directions, and it is the position
  this decision commits to. Recording such a version on the strength of the objects
  it *does* create would launder a guess: a `psql -f` outside a transaction can
  abort between a file's `CREATE TABLE x` and its following `ALTER TABLE y`, leaving
  precisely the catalogue a table-only check would call applied, and `apply` would
  then never run the remainder. Recording only the prefix underneath is no better:
  the ledger would report the unconfirmable version as *pending*, so the next
  `apply` would run it over a schema that may already have had it applied out of
  band. A backfill, an `UPDATE`, a non-idempotent `INSERT`, or an `ALTER` clause the
  catalogue has no answer for is both the statement whose effect nothing can be
  asked about *and* the one whose rerun is destructive, so no ledger row both
  accounts for the objects and keeps `apply` away. Shipping such a migration
  therefore withdraws `adopt`, and stating that history stays the operator's own
  `INSERT`; the refusal says so, naming the version.
  `a_migrations_declared_tables_are_read_out_of_the_shipped_ddl` asserts every
  shipped statement is confirmable, so this arrives as a deliberate release decision
  rather than a surprise in the field;
  `the_tenancy_migrations_columns_constraints_and_policies_are_all_confirmable`
  holds the shipped v1+v2 history to it, object by object, including the six tables
  the tenancy migration's `DO` block guards;
  `a_statement_whose_effect_cannot_be_confirmed_makes_its_migration_unadoptable`
  pins the mixed create-and-alter shape specifically; and the two manual states an
  operator can actually be in are each adopted against a real database — both files
  applied by `a_hand_applied_schema_is_adopted_as_the_baseline_its_objects_prove`,
  v1 alone by
  `a_schema_hand_applied_only_as_far_as_v1_adopts_v1_and_leaves_v2_pending`, which
  records v1, reports v2 pending with a non-zero exit, and leaves v2 to `apply`.
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
- A chunk of a file with no statement in it — a trailing comment, a stray `;` — is
  not an unconfirmable statement and does not withdraw adoption.
- Seed evidence is a table having a row, which is one answer for the whole table,
  so a seed into a table the shipped history seeds more than once — in another
  migration or twice in the same file — is confirmed by whichever row is there.
  Every such seed is unconfirmable and refuses the history, rather than recording
  a version whose row `apply` would then never write.
- **Reading the evidence is classified like writing the ledger, not like an
  ordinary read.** Adoption's premise is DDL applied by somebody else, plausibly as
  another role, so `42501` on a probe is a realistic failure; class 42 and `3F000`
  are refusals naming the SQLSTATE rather than outages, because a rollout gate told
  to retry a grant loops forever.

Unchanged: `preflight` and `status` still cannot write, the classifications are the
same, an empty ledger is still a refusal for `status`, `apply`, and boot, and
recording the baseline by hand still works and is classified identically. `adopt`
is the checked version of that `INSERT`, not a new policy.

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

## Amendment (2026-08-13): a later version takes earlier ones' objects away

The deferred-constraint migration (v3) is the first shipped file that *replaces*
rules an earlier version installed rather than adding to them, and it does so in
three shapes the amendment above did not read. Adoption is extended to all three
rather than withdrawn, on the same terms: every effect confirmed by one catalogue
question, and anything outside the shape refused.

- **`DROP INDEX` is confirmed by the index being gone**, like every other `DROP`,
  and like them it is never proof on its own: a database that never had v2 has no
  such index either. It is why the prefix is now read *with the versions above it
  in view* — the last version to act on a thing decides whether it should be there,
  so the longest prefix the database accounts for is the baseline. An index v2
  created and v3 dropped being present says v2 ran and v3 did not; its absence with
  v3's constraints present says both ran; and the two states the prefixes disagree
  about — the index gone with the constraint never added — are a migration stopped
  in the middle, refused rather than recorded shorter.
- **An add guarded by `IF NOT EXISTS (SELECT ... )` is evidence only when the guard
  asks about the very thing it guards**, which is what v3's blocks do: the guard
  names the table and the constraint the branch adds, so whether the branch was
  taken or skipped, the constraint is there afterwards either way and its presence
  is the same answer. A guard about something else, a guard that is not a single
  catalogue read, or a guarded effect unconfirmable in its own right makes the block
  unconfirmable, exactly as a bare `IF` condition did.
- **A loop that drops what its own catalogue query names is summarised by that
  query.** v3 removes the constraints PostgreSQL named for itself when v1 and v2
  declared them inline — a name the file cannot state, which is the whole reason the
  loop exists — so the evidence is the query returning nothing afterwards, asked
  again by adoption as the file asked it. Strictly bounded: one `SELECT` over
  `pg_constraint`, no writing keyword, selecting the `conname` the loop drops, and a
  body that does nothing but `EXECUTE format('... DROP CONSTRAINT %I', v.conname)`
  for what the query named. It is absence, so never proof of a version. Two details
  make it honest rather than approximate: the probe runs with the search path pinned
  to this schema, like every other probe, because the query resolves its tables with
  `::regclass` and a neighbour's tables would otherwise answer for this one's; and
  the constraints the file goes on to declare *by name* are exempt from it, since
  v3's journal loop matches every check mentioning `actor_kind` and then adds one
  that does — "the query names nothing" would otherwise be false of every database
  that ran it.

Reading the longest prefix cuts both ways, so the search is bounded from above as
well: a prefix is the baseline only if no version *over* it is evidently applied —
everything that version leaves of its own being present means the files went on in a
different order, or one was skipped, and the prefix underneath is then a history this
database does not have. That is a refusal naming the later version rather than a
recorded prefix, because recording one leaves `apply` to run the skipped file over
objects that are already there. All of that version's own proof is required, not one
object of it: an earlier file can declare the same constraint inline in a
`CREATE TABLE`, which this parse reads as a table and nothing more, so a single
shared object is not evidence of what wrote it. And the body of a `DO $$ ... $$`
block goes through the same lexical check the file does before its statements are
read, since the file-level scan skips the region whole and a literal inside it that
does not close where this parse says would shorten the block's evidence instead of
voiding it.

The evidence-sharing rule is unchanged in substance and stated once more, because
the third shape widens what counts as sharing: a version needs one thing **more
than one shipped migration does not act on**. Two files creating the same object,
one dropping and re-adding what another created, and one taking away what another
left are the same ambiguity — the object's state proves at most one of them — and a
version every one of whose effects is shared that way is unadoptable and blocks the
history, like a statement no catalogue can answer.
`the_deferred_constraint_migrations_guards_and_cleanups_are_all_confirmable` holds
v3 to this object by object,
`a_guarded_add_and_a_cleanup_loop_are_read_only_in_the_shapes_they_summarise` pins
the fail-closed edge of each shape, and
`a_later_migration_taking_an_earlier_ones_object_away_is_read_as_the_prefix_it_is`
covers the four states a replacement leaves. No shipped SQL changes, so the
checksums and the byte-identity gates are untouched, and no probe writes.

## Consequences

A schema disagreement, an unset reference, or an unreachable database becomes a
non-zero exit from a command before a rollout instead of a crash loop during one,
and the fresh-install and upgrade orders are checkable rather than conventional.

Costs: an operator step is added to a stateful upgrade — `apply` must run once,
from one place, with the new binary, before replicas roll onto it. Preflight is
deliberately stricter than boot about the config file's mode, so a config another
account can rewrite fails the gate while it would start a gateway. And until a
revision compiles into a runtime snapshot, a `mode = "stateful"` replica boots and
serves `/admin/v1` but refuses *inference*; a stateful preflight reports that
refusal as a failure rather than promising a deployment that can serve traffic,
while the database checks it accompanies still run. Preflight reads that refusal
from `serve`'s own definition, so the two cannot disagree and lifting the refusal
removes the reported failure with it.
