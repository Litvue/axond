# Control-plane revision journal

The durable desired state of a stateful deployment is a chain of immutable
revisions in PostgreSQL. This page is the operator's view of that journal: what
the schema is, how migrations are applied and checked, and what happens to a
running fleet when the database is unavailable.

Stateful mode is still being assembled: this is the storage layer
([#165](https://github.com/Litvue/axond/issues/165)). Hydrating a revision into a
runtime snapshot and converging replicas onto it are separate slices, so nothing
described here is constructed by `serve` yet, and no `/admin/v1` route writes to
it. Read [ADR 0027](../adr/0027-stateless-and-stateful-operating-modes.md) for the
mode as a whole.

## What is stored

Publishing a change never edits a row. It writes new resource versions and a new
manifest that references them, then advances a single head pointer — so every
earlier revision remains loadable exactly as it was published.

| Table | Holds |
| --- | --- |
| `axond_cp_schema_migration` | Applied migration versions, names, and a checksum of each file's text |
| `axond_cp_blob` | Content-addressed payload references: kind, SHA-256 digest, size |
| `axond_cp_resource_version` | One immutable version of one resource, shared by every revision that pins it |
| `axond_cp_resource_dependency` | The dependency edges between resource versions |
| `axond_cp_mutation` | One administrative change and its actor attribution |
| `axond_cp_revision` | A published revision: parent, mutation, whole-state checksum, publication order |
| `axond_cp_revision_entry` | The manifest: which resource version each revision pins |
| `axond_cp_revision_blob` | The blobs a revision declares |
| `axond_cp_audit_event` | The audit trail, written in the publishing transaction |
| `axond_cp_idempotency` | Per-caller retry records, which expire |
| `axond_cp_head` | One row naming the desired revision |
| `axond_cp_tenant` | Projected tenants: slug, lifecycle, and the revision that wrote the row |
| `axond_cp_project` | Projected projects, owned by exactly one tenant |
| `axond_cp_principal` | Projected principals: an OIDC `(issuer, subject)` human, or a workload and its key *digest* |
| `axond_cp_principal_role` | The roles a principal holds |
| `axond_cp_access_denial` | Administrative actions that were refused, with the reason |

Two properties worth knowing before you plan capacity or retention:

- **Revisions share storage.** A resource version is written once and referenced
  by every revision that pins it; a blob digest pinned by a hundred revisions is
  one row. A revision is not a copy of the state.
- **Blobs are references, not payloads.** The journal stores kind, digest, and
  size. Payload bytes live in the blob store, and a revision is only usable when
  its digests resolve there.
- **The last five tables are a projection, not a second source of truth.** The
  published revision is what the gateway authorizes against; the transaction that
  publishes it also writes these rows so the database can enforce ownership on its
  own — a tenant-scoped row cannot name a tenant nothing declared, and a
  principal cannot be scoped into another tenant's project. Migration 0002 also
  adds row-level-security policies keyed on `axond.tenant_id`: a session that sets
  it sees deployment-wide rows and that tenant's, and a session that does not set
  it — the publisher — is unrestricted. Authorization decisions stay in the
  service layer; the policies are defence in depth.

  What is inside the wall: every table that names a tenant
  (`axond_cp_resource_version`, `axond_cp_tenant`, `axond_cp_project`,
  `axond_cp_principal`, `axond_cp_access_denial`, `axond_cp_mutation`), and every
  table that names one indirectly, filtered through the row that owns it — a grant
  through its principal, an audit event through its mutation, a manifest line and
  a dependency edge through the resource version they point at, a deduplication
  record through the mutation it replays. A refusal or a change that names *no*
  tenant is shared state, but its actor is not: a deployment-scoped row attributed
  to a workload is filtered by that workload's tenant too, so a pinned session
  cannot read which service accounts of other tenants attempted what. A row that
  names *this* tenant is this tenant's whoever attempted it — a refused
  cross-tenant attempt is the event the trail is for, and hiding it from the
  tenant it was aimed at would leave it readable by no one.

  The publication chain — `axond_cp_head`, `axond_cp_revision`,
  `axond_cp_revision_entry`, `axond_cp_revision_blob`, `axond_cp_blob`,
  `axond_cp_resource_dependency` — has no tenant column to key a policy on, since
  one revision is every tenant's desired state at one instant, so it is walled the
  only way it can be: readable by the unpinned publisher and by nothing else. A
  session pinned to a tenant reads its own rows through the tables above and none
  of the chain that published them.

  What is deliberately outside the wall is one table, `axond_cp_schema_migration`
  — the schema's own version, which belongs to no tenant and is what an operator
  reads to know which migrations ran. It is also exactly what a
  `pg_class.relrowsecurity` audit reports as unprotected, so the list and the
  database agree.

  Two preconditions before you apply 0002 and 0003 to a deployment that is
  already running. The policies are `FORCE`d so that they bind the table owner too — the
  single-role install is the common one, and enabling row-level security that the
  owning role bypasses would claim a wall that is not there — and `ALTER TABLE …
  FORCE ROW LEVEL SECURITY` requires the migrating role to *own* every table it
  names, so a deployment whose DDL is applied by a DBA role separate from the
  application role must run them as the owner rather than as the migrator. And any
  reader outside the gateway — a reporting job, a replica consumer — that already
  sets `axond.tenant_id` on its sessions for its own reasons starts seeing
  filtered rows the moment 0002 lands, silently and with no error: unset it there,
  or accept that the reader is now tenant-scoped.
- **A tenant a revision omits is retired, not left active.** A revision is the
  whole desired state, so a tenant it stops declaring keeps its row — with its
  projects, mutations, and audit trail — and that row is written to `lifecycle =
  "deleted"`. Nothing serves an undeclared tenant, and this is what keeps the
  column readable as the serving answer. Two consequences worth planning for: a
  partial configuration retires every tenant it leaves out, so publish complete
  desired state; and re-declaring the tenant in a later revision brings the row
  back to whatever that revision says, history intact.

  A revision that declares *no* tenant at all is the exception, and deliberately:
  that is what every pre-tenancy revision in an upgraded deployment's journal
  looks like, and rolling back to one republishes it. Reading that silence as a
  deletion would make a rollback the most destructive operation the control plane
  has, so a snapshot with no tenancy in it reconciles nothing. Emptying the tenant
  list is therefore an explicit `lifecycle = "deleted"` on the last tenant rather
  than an empty publication — which is also the only version of it an audit trail
  can attribute.
- **A retained project name stays taken.** A project a later revision stops
  declaring keeps its row and its name. Publishing a *different* project under a
  retained name is refused rather than reported as a temporary failure: no retry
  clears it, and the refusal names the conflict. Releasing a *tenant's* name is
  automatic once it is retired — the uniqueness constraint ignores deleted rows —
  and doing it while keeping the tenant declared means publishing it at
  `lifecycle = "deleted"` and at the next version number of its *last published*
  version — republishing a version number with different content is refused, so read the
  current number out of the journal (`SELECT version FROM
  axond_cp_resource_version WHERE resource_id = …`) rather than assuming it. Two
  owners may also exchange names within one revision, and two principals may
  exchange sign-ins or keys, since uniqueness is judged on the state a revision
  declares rather than on the order its rows are written.
- **A project's name has no release path yet.** A tenant's does, through
  `lifecycle = "deleted"`; a project has no lifecycle, so once a revision stops
  declaring a project its `(tenant, name)` stays taken for the life of the
  deployment and re-using that name in that tenant is refused. Renaming the
  retained project within the revision that drops it is the workaround: a project
  that is still declared can be renamed, and uniqueness is judged on the declared
  state. Giving a project the lifecycle a tenant has is a change to the tenancy
  contract (#191) that its downstream consumers read, so it is follow-up work
  rather than a widening smuggled into this slice.
- **A disabled tenant serves nothing.** Only an active tenant's projects become
  servable namespaces. Disabling is what stops traffic; the rows stay for the
  history that points at them.
- **Refusals are not revisions.** A denied administrative action publishes
  nothing, so it is recorded in `axond_cp_access_denial` rather than in the audit
  trail of a revision that does not exist. Denials are read per tenant, and the
  caller is told only that the action was forbidden — the reason lives in the row.
- **A lifecycle transition is an update.** Disabling or deleting a tenant changes
  `axond_cp_tenant.lifecycle`; it never deletes the tenant, its projects, or its
  history. Physical erasure is a separate compliance procedure.

Secret material is never in the journal. A credential resource's body carries an
opaque, exactly-versioned secret *reference* (`sct_…` plus a version) and the
lifecycle state of that material; plaintext stays in the secret store, and no body
value is logged. Rotation writes a new secret version and a new credential
version rather than editing either, so an earlier revision keeps pinning the exact
material it was published against — see
[provider credentials](./revision-convergence.md#provider-credentials-name-material-without-holding-it).

## Operator commands

Four commands run *before* replicas do, all with the same grammar —
`axond <command> <action> --config PATH`. `--config` may be omitted when
`AXOND_CONFIG` is set.

| Command | Writes? | Answers |
| --- | --- | --- |
| `axond check preflight --config PATH` | No | Would a replica boot against this? Config ownership and mode, every reference a boot resolves (control plane, secret-store KEK, breakglass, inbound keys and verifiers, provider credentials, opt-in stores — including a reference a store inherits from the Redis budget), control-plane reachability, schema compatibility. |
| `axond migrate status --config PATH` | No | What schema does this database have, and what would an apply do? |
| `axond migrate apply --config PATH` | Yes | Apply the pending migrations, forward only. |
| `axond migrate adopt --config PATH` | Yes (ledger rows only) | This schema was applied out of band — record the baseline its objects account for. Executes no migration file. |

The command surface, the forward-only policy, and the refusal to migrate at boot
are [ADR 0032](../adr/0032-operator-preflight-and-forward-only-migrations.md).

`preflight` and `status` cannot change a database, and not merely by convention:
they open the control plane on a maintenance path that does not prepare a schema,
with migration permission forced off, and read the ledger inside a `READ ONLY`
transaction, so the *server* rejects a write. `apply` and `adopt` are the only
mutations, both are explicit, and they write different things: `apply` runs
migration files and records them, `adopt` records ledger rows for migrations it
verified are already applied and runs no DDL at all.

Exit codes make these usable as deployment gates: `preflight` exits non-zero if
any check failed, `status` exits non-zero while a migration is outstanding or the
schema is refused, and `apply` exits non-zero if the schema was refused. `adopt`
exits non-zero if it refused, or if the baseline it recorded still leaves a
migration to apply. Output names environment variables, never DSNs.

`apply` against a database that is already current is a no-op rather than a
re-run: it reports the current version and rolls its transaction back without
executing a migration file. `adopt` against a database whose ledger already
records a history is the same kind of no-op — it reports that history and writes
nothing.

In stateless mode there is no control plane, so `preflight` reports the database
checks as skipped and `migrate` has nothing to do. Neither command requires
PostgreSQL to exist.

Until stateful serving is wired up, a stateful `preflight` **fails** on a
`serving` line and exits non-zero: `axond serve` still refuses
`mode = "stateful"` outright because the durable control plane is not wired to the
runtime yet, so no replica can boot against that config and a zero exit would
promise one that cannot. Every other check still runs and is still printed, so the
report is the same description of the database it would otherwise be — and
`axond migrate status` / `axond migrate apply` are separate commands with their own
exit codes, so preparing the database is not blocked by the serving refusal. The
line is printed from the refusal `serve` itself raises, not from a second copy of
the rule, so it disappears when that refusal does rather than outliving it.

Only the control-plane journal is migrated by these commands. It is the only store
with a ledger — recorded version, file name, checksum — so it is the only one where
"what has been applied?" is a question the database itself can answer. The usage,
budget, and revocation stores are applied by hand from `ops/postgres/`, as
[stateful backends](../deployment/stateful-backends.md#postgres) describes.

### Fresh install

The database must exist and the role must be able to create objects in the target
schema; the commands create neither the database nor the schema. A missing schema
or a role that may not create objects is reported as a refusal naming the SQLSTATE
rather than as an outage, so an automated gate stops instead of retrying.

```bash
export GW_CONTROL_PLANE_DSN='postgres://axond@db/axond?sslmode=require'

axond migrate apply --config /etc/axond/axond.toml   # applies 0001 and onwards
axond check preflight --config /etc/axond/axond.toml # then verify a replica would boot
```

Then start replicas. Run `apply` once from one place; it is safe if that
accidentally becomes twice, or two places at once.

While stateful serving is unwired, that `preflight` exits non-zero on its
`serving` line even when the database is ready; read the rest of the
report, and gate the rollout on `axond migrate status` until the refusal is gone.

### Upgrade

```bash
axond migrate status --config /etc/axond/axond.toml  # with the NEW binary
axond migrate apply  --config /etc/axond/axond.toml  # fleet still on the old binary
axond check preflight --config /etc/axond/axond.toml
# roll replicas onto the new binary
```

Order matters in one direction only: migrations are additive and forward-only, so
the *new* binary's `apply` runs before the new binary serves, and the old binary
keeps running against the migrated schema until it is replaced. Never apply from
an older binary than the one you are deploying, and never roll a binary onto a
schema its own `status` reports as *Ahead*.

Migrations are forward-only and versioned. An applied file is never edited and
never renumbered: a schema change is a new
`ops/postgres/control_plane_<NNNN>_<name>.sql`, applied on top. Every object is
created in the current schema, so a journal that lives beside other tables works
with `[control_plane] schema` (or a DSN that sets `search_path`).

`apply` runs the whole read-and-write in one transaction under a PostgreSQL
advisory lock, so it is safe to run while replicas are starting and two
simultaneous applies are one migration. A replica configured with
`[control_plane] migrate = true` does the same thing at boot; the default is off,
because one apply before a rollout is the order that cannot have replicas
migrating a database their peers are reading.

### Applied out of band: `psql`, then `adopt`

Applying the DDL with `psql` still works and stays supported for operators who own
schema changes out of band:

```bash
psql "$GW_CONTROL_PLANE_DSN" -f ops/postgres/control_plane_0001_initial.sql
psql "$GW_CONTROL_PLANE_DSN" -f ops/postgres/control_plane_0002_tenancy_access.sql
psql "$GW_CONTROL_PLANE_DSN" -f ops/postgres/control_plane_0003_tenancy_constraints.sql
axond migrate adopt  --config /etc/axond/axond.toml  # record the baseline that applied
axond migrate status --config /etc/axond/axond.toml  # now Current
```

0003 is where the deferrable name, identity, and ownership rules live, and it
replaces the immediately-checked ones 0002 created — a deployment that applied
0002 alone keeps refusing revisions in which two tenants trade names. It is a
forward migration rather than an edit to 0002 for the reason the *Drifted* row
below states: the ledger compares a recorded checksum against the shipped file,
so an applied migration is immutable. Re-applying it is a no-op.

The `psql` path creates the ledger table without recording anything in it, so the
journal is then reported as *Unrecorded* — a ledger that exists and records nothing
— and `status`, `apply`, and boot all refuse it. That refusal is the point. An
empty ledger is indistinguishable from an untouched database, and the ledger is the
only record of what was applied, so migrating from zero would replay every shipped
file over objects that may already be there. Today every statement is
`IF NOT EXISTS` and that replay would survive; the first `ALTER TABLE`, backfill,
or constraint change would not, and it would corrupt a database rather than fail.

`axond migrate adopt` is how that state is resolved deliberately. It writes ledger
rows and never DDL, and it records only what the database itself accounts for: the
longest run of shipped migrations, starting at v1, whose every statement is
confirmed — each table and index present, and each idempotent seed row written — in
the schema this configuration writes to (the one `[control_plane] schema` names, or
the first on the DSN's own search path; a second install's journal further down that
path is not evidence about this one). What it does instead of recording:

- **No object present.** Nothing was applied out of band, so there is no baseline.
  Refused, naming the schema it looked in and the way forward — drop the empty
  `axond_cp_schema_migration` table and run `axond migrate apply`. The schema is
  worth reading: a DSN whose `search_path` has more than one entry can have the
  ledger answer from a schema further down it while the objects are sought in the
  one this configuration writes to, which is where `apply` would create them.
- **A migration only partly applied.** Some of what one file declares is there and
  some is not — a table, an index, or the seed row the shipped file ends with, which
  a `psql -f` outside a transaction can stop just short of. Neither "applied" nor
  "not applied" is true, so it is refused, naming the repair for each thing: an
  object that is not present is reported as missing, and a table that is present and
  empty is reported as having no seeded row. Finish or undo that file by hand first.
- **A ledger that already records a history.** Nothing to adopt: the history is
  reported and nothing is written, so a stray `adopt` in a rollout is not a ledger
  edit.
- **A shipped migration containing a statement adoption cannot confirm.** Adoption
  confirms three statement forms: `CREATE TABLE` and `CREATE INDEX` by the object
  being present, and `INSERT ... ON CONFLICT DO NOTHING` by the target table not
  being empty — each of them naming its object unqualified, since the probe asks
  about the configured schema. An unnamed `CREATE INDEX ON t (c)` or a
  schema-qualified `other.t` is unconfirmable for the same reason as the rest.
  Comments (`--` and nested `/* */`) and quoted regions (`'...'`, `$tag$ ... $tag$`)
  are prose or data, never statements: a `CREATE TABLE` inside a function body or
  a block comment is not evidence that anything was created.
  Anything else — an `ALTER`, a backfill, a `DROP`, a non-idempotent `INSERT` —
  is both what no catalogue can report on and what a rerun would damage,
  and one such statement makes its whole migration unadoptable. A *second* seed into
  a table the shipped history already seeds counts as one of those, whether it is in
  another migration or in the same file: one row proves at most one of the inserts,
  so it proves nothing about the rest. Unadoptable holds even when the same file
  also creates a table: `psql -f` without a wrapping transaction can abort between
  `CREATE TABLE x` and a following `ALTER TABLE y`, and "x exists" is then not
  evidence the file finished. Adopting the versions below it is no better — the
  ledger would call it *pending*, so the next `apply` would run it over a schema
  that may already have it. So a release that ships one refuses every adoption
  while it is unrecorded — the whole history, not just that version — and the
  baseline goes back to being an
  `INSERT INTO axond_cp_schema_migration (version, name, checksum)` you write
  because you own the change that applied it. The refusal names the version and says
  this.

The role running `adopt` must be able to *read* the objects the out-of-band apply
created, not just the ledger: a rejected probe (`42501`, or a missing schema) is
reported as a refusal naming the SQLSTATE rather than as an outage, so a gate stops
and the grant gets made instead of the gate retrying.

It is one transaction under the same advisory lock `apply` takes, so a refusal
leaves no partial baseline and two simultaneous adoptions are one adoption.
Adopting a prefix below the required version reports what is still pending and
exits non-zero, exactly as `status` does — the baseline was recorded and the
database is still not one a replica may serve; the next command is
`axond migrate apply`, which migrates forward from the adopted baseline.

Adoption is deliberately narrow. It cannot repair a *Drifted*, *Incomplete*,
*Renamed*, *Ahead*, or *Malformed* history: each of those is a recorded history
that disagrees with this build, which is a different question from an unrecorded
one, and each stays a refusal. Writing the baseline rows by hand still works and is
classified identically — `adopt` is the checked version of that `INSERT`. Prefer
`axond migrate apply` for a fresh install: it records what it applied and never
leaves this state behind.

## Schema status, and what each state means

`axond migrate status` reports these, and boot refuses to serve anything but
*Current*. Each state is separate because each implies something different to do —
"the schema is wrong" would leave an operator to find out which of these it is.

| Status | Meaning | What to do |
| --- | --- | --- |
| Current | The applied history is exactly the required one | Nothing |
| Behind | Migrations are missing | `axond migrate apply` |
| Unrecorded | The ledger table exists and records nothing: the DDL was applied out of band, or every row was deleted | `axond migrate adopt` if the DDL was applied — it records the baseline the objects account for, and refuses if they do not account for one; drop the empty ledger and `apply` if nothing was applied; never migrate it from zero |
| Ahead | The database records a migration this build does not know | Deploy the newer build; do not downgrade the schema |
| Drifted | An applied migration's recorded checksum is not this build's file | Restore the file, or add a new migration — never edit in place |
| Incomplete | The applied versions are not a complete prefix (`v3` without `v2`, or a deleted ledger row) | Find out what applied out of order or removed the row; the maximum version alone is not evidence the history is intact |
| Renamed | A version is recorded under a name this build does not ship it as | A migration was renumbered or renamed rather than added; restore the shipped numbering |
| Malformed | The ledger is not the one this build writes: a missing column, a version below 1, a duplicate version, another table under that name | Something else owns `axond_cp_schema_migration`, or a restore was partial |

Only *Behind* (and a database with no journal table at all) is migratable. Everything
else is a refusal a retry cannot clear, and `apply` refuses it rather than writing
more DDL over a history it cannot account for: a replica that served against a
schema it did not write would be writing rows a newer build defined differently.

Drift is also checked without a database. The packaged copy under
`crates/gateway/sql/` must be byte-identical to `ops/postgres/`
(`crates/gateway/tests/shipped_ddl.rs` and `ops/publish-crates.sh`), and the
migration text the build embeds must be the file it applies.

## PostgreSQL version

PostgreSQL 14 or newer. CI exercises 17. Boot reads `server_version_num` and
refuses an older server rather than failing later on a statement it cannot run.
The supported range, and what a major upgrade of the backend requires, are in
[Supported versions](../deployment/stateful-backends.md#supported-versions).

## TLS and connectivity

Connectivity follows the same conventions as the usage and revocation backends:
the DSN decides TLS (`sslmode`), the connector is the shared Rustls one with the
webpki root bundle, connection and operation timeouts are bounded, and a
connection that an outage broke is discarded and re-established on the next
operation. A DSN that requests TLS against a server that cannot provide it fails
to connect; it is never silently downgraded to plaintext. The DSN is never
logged.

## During a Postgres outage

The journal is not on the inference path. A replica serves an immutable snapshot
it already holds, so an outage does not touch what it is serving:

- Inference keeps working on the loaded snapshot.
- Administrative writes fail with a retryable unavailable error, and a
  publication that cannot finish writes nothing: the manifest, the resource
  versions, the mutation, the audit event, the idempotency record, and the head
  move in one transaction or not at all.
- A replica that has not yet loaded a revision cannot start serving. Cold boot
  needs the control plane.

So a partially published revision is not a state the journal can be in, and
"roll back the failed change" is not an operator task.

## Concurrency and retries

- **One writer wins.** Publication reads the head row `FOR UPDATE`, so two
  administrators submitting against the same expected revision serialize: one
  commits, the other is told it conflicts and with what.
- **A retry replays its own outcome.** A repeated idempotency key that describes
  the same desired state returns the original manifest — even if the expectation
  has since gone stale — without recording a second mutation or a second audit
  event. The same key describing *different* state is refused.
- **Deduplication is scoped per caller and expires.** One administrator's
  `retry-1` neither replays nor blocks another's, and expiry closes a retry
  window without touching the revision or audit trail the record points at.

## When a revision will not load

Loading a revision reads its manifest, resolves every resource version it pins,
and verifies the result against the checksums recorded at publication. It either
returns the whole revision or fails; there is no partially loaded revision to
inspect or repair, and a failed load never becomes state a replica serves. Four
outcomes need different responses:

- **Unreadable (`stored revision … is unreadable`).** The rows no longer add up:
  a missing resource version, a declared blob whose record is gone, a dependency
  that leaves the revision or closes a cycle, a checksum that no longer matches,
  or a reference that crosses a tenant boundary. Tenancy ownership is re-read here
  too, so a project row edited to claim a tenant that does not own it does not
  hydrate, and neither does a project whose tenant row is gone — see
  [resource body schemas](./revision-convergence.md#resource-body-schemas). The
  message names the resource or edge. This is corruption,
  not an outage — retrying will not clear it — and it means something wrote to the
  journal outside the gateway, or a restore was partial. Compare against a backup;
  a healthy older revision still loads, so serving can continue from one while the
  damage is investigated.
- **Incompatible (`stored revision … is not compatible with this build`).** The
  rows add up and this build cannot read them: a body whose schema identifier or
  field set belongs to a newer release, a tenant, project, credential, or
  model-enablement body written before those bodies were typed, a credential
  naming a lifecycle state this build does not know, an enablement or alias naming
  a state or wire family it does not know, or a row — or a whole revision — naming a canonical
  encoding version this build does not write, whether or not this build knows that
  version's name, which a restored backup holding rows from two builds produces.
  (A serializer column naming no version of this encoding at all is unreadable
  storage, below.) **This is not corruption, and nothing about the database needs
  repairing.** It is reported separately for exactly that reason, and convergence
  reports it as reason `incompatible`. The replica keeps serving the revision it
  already holds. Roll the replica onto a build that reads the revision, or publish
  a revision the deployed build reads — which for a legacy tenant or project means
  republishing it from a build that writes typed bodies. Revisions older than that
  upgrade stay unreadable to this build by design and remain in the journal as
  history. A credential whose body contradicts itself or its envelope — an owner
  that is not its scope, two credentials claiming one tenant's secret for another,
  two states for one secret version, or two versions of one secret in service — is
  unreadable, above, not this outcome: those are refused at publication, so a
  stored revision holding one was written outside the gateway. The same holds for
  an enablement or alias that contradicts itself or its envelope — an owner that is
  not its scope, an undeclared catalogue snapshot, two enablements for one offering
  at one scope, or an alias target that is duplicated, dangling, cross-tenant, or of
  another wire family. An *alias* body written before these bodies were typed is
  the documented exception: it is skipped rather than reported, so neither outcome
  names it — see
  [resource body schemas](./revision-convergence.md#resource-body-schemas).
  A body that *declares* a schema this build reads and then is not one —
  a field gone, a field whose type changed — is not this outcome; that is
  unreadable, above, because no version skew produces it. Neither is a tenancy body
  that is not an inline record at all, or one under a kind it does not match: no
  release ever wrote one, typed or untyped, so the shape itself says the row was
  rewritten.
- **Too large (`exceeds what hydration reads`).** The revision is intact but
  larger than this build's read bounds (resource versions, blobs, blob bytes,
  dependency edges or nesting, inline body bytes, or total candidate size); the
  message names which bound and, where useful, what was observed. Nothing is
  truncated to fit. Either raise that bound deliberately or split the change into
  smaller revisions. A dependency *cycle* is never reported here — no bound could
  clear one — it is unreadable, above.
- **Unavailable.** Postgres is unreachable. Retryable, and covered by
  [During a Postgres outage](#during-a-postgres-outage).

A revision that loads once loads forever *on that build*: revisions are
immutable, so a successful load is repeatable, and history stays answerable after
any number of newer publications. Compatibility is the one axis that is not
immutable — a build reads the schemas and enforces the rules it knows — so an
upgrade can turn a revision that loaded into one reported as incompatible, above.
That is a property of the build, not a change to the journal.

## Backup and retention

Back the journal up like any store of record; it is the authority on what the
fleet should be serving. Nothing here is safe to prune selectively: mutations and
audit events are referenced by revisions, and resource versions are referenced by
every manifest that pins them. Expired idempotency rows are the only
self-pruning data, and they are pruned on the write path that depends on them.

The objectives that back that up (RPO, RTO), the archiving and dump mechanisms, a
point-in-time recovery procedure, and the drill that proves both restores work
are in
[Backup, restore, and point-in-time recovery](./backup-and-recovery.md).

## Useful queries

```sql
-- What is desired right now, and when it was published.
SELECT h.revision_id, r.created_at, r.state_checksum
FROM axond_cp_head h JOIN axond_cp_revision r ON r.revision_id = h.revision_id;

-- The most recent changes, with attribution.
SELECT r.seq, r.revision_id, m.actor_kind, m.actor_subject, m.actor_component,
       a.event_kind, a.summary, a.recorded_at
FROM axond_cp_revision r
JOIN axond_cp_mutation m USING (mutation_id)
JOIN axond_cp_audit_event a ON a.revision_id = r.revision_id
ORDER BY r.seq DESC
LIMIT 20;

-- Applied migrations.
SELECT version, name, applied_at FROM axond_cp_schema_migration ORDER BY version;
```
