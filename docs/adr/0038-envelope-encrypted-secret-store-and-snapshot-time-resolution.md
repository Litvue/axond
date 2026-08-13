# 38. An envelope-encrypted secret store, and material that lives exactly as long as a snapshot

Date: 2026-08-13

## Status

Accepted

Implements the runtime half of
[ADR 0034](./0034-typed-provider-credentials-and-secret-lifecycle.md), at the
`SnapshotCompilation` boundary
[ADR 0027](./0027-stateless-and-stateful-operating-modes.md) put `SecretStore` at,
and inside the compile-then-publish pipeline of
[ADR 0011](./0011-config-hot-reload.md).

## Context

ADR 0034 typed the credential domain and left one deliberate gap: the only
`SecretStore` was a test double. Nothing durable stored material, so a stateful
deployment could describe a tenant credential and never authenticate with one.
Closing that gap is not just "write a Postgres table" — three runtime questions
have to be answered together, because answering them separately is what produces
a store that is correct in isolation and unsafe in service:

- **Where does material live at rest, and what does a database dump disclose?**
  Storing plaintext in the control-plane database makes every backup, replica, and
  `pg_dump` a copy of every tenant's provider key, and makes the blast radius of a
  read-only database credential the whole fleet's material.
- **When is it unwrapped?** A store consulted while a request is in flight makes a
  secret-store outage an inference outage, and makes plaintext handling a
  request-path concern with request-path latency budgets and request-path error
  paths.
- **How long does plaintext stay in memory?** A process-wide cache keyed by secret
  id keeps revoked material resolvable and keeps rotated material alive with no
  bound anyone can state. "Zeroize on revocation" cannot be honest while a
  snapshot compiled against that version is still serving requests.

## Decision

**One production store, at one boundary: envelope-encrypted rows in PostgreSQL.**
Material is sealed in this process under a fresh per-version AES-256-GCM data key;
that key is sealed under the deployment KEK that `[secret_store]` *references*;
only the sealed bytes reach the database. The KEK is read once at boot, from an
env var or a file, so key material has a single load point and never becomes a
row, a manifest field, or a log line. An external key manager is a later adapter
behind the same trait, which is why the config key is an enum and why
`Capability::ExternalKeyManagement` is deliberately not declared here.

The seal's associated data binds the scheme, the owner, and the exact
`SecretRef`, so a ciphertext moved between rows, tenants, or versions in the
database does not open. Three storage rules make ADR 0034's domain enforceable
rather than aspirational:

- **A version is a row, and a row is written once.** The primary key is
  `(secret_id, version)`; rotation inserts the next version and never updates
  sealed bytes. Overlap is therefore structural: a revision compiled against
  version 2 keeps resolving version 2 while version 3 is staged and proven.
- **Ownership is checked on every read.** A statement keys on the reference and
  returns the row's owner columns, which are matched against the caller's tenant
  and project exactly; a row this owner does not own answers exactly as an absent
  one does — distinguishable only in this process's own logs, so the store cannot be
  used to enumerate other tenants' material.
- **Tombstoning destroys bytes in the transaction that records it.** The
  lifecycle move and the `NULL`ing of the sealed columns are one statement, and
  the shipped DDL's check constraint refuses any other combination, so
  "destroyed" cannot mean "relabelled".

**Resolution happens while a candidate is compiled, and nowhere else.** Compiling
a candidate revision resolves every exact version its resolvable credentials pin,
once per reference, through the store. This is the only asynchronous step in
compilation and the only place durable material enters the process. A store
outage therefore stalls administration and convergence while replicas keep
serving the snapshot they already hold, which is the same posture as every other
Tier 2 backend.

**A candidate whose material does not resolve is refused, and the previous
revision keeps serving.** Resolution failures are `ProjectionError::Secret`,
carrying the reference and the reason and never the material, and they are
reported under their own `secret` rejection label because "the candidate was fine
but a secret was not" is the first distinction an on-call engineer needs. Nothing
in compilation can touch the published snapshot, so last-known-good is structural
rather than a rule the reconciler remembers.

**Material's lifetime is the snapshot's lifetime.** Unwrapped material is owned by
the `ConfigSnapshot` compiled against it, and shared through a reference-counted
holder registered in a process-wide ledger of *references and counts* — never
material. A request loads one snapshot and keeps it for the request's or stream's
whole life ([ADR 0011](./0011-config-hot-reload.md)), so publishing a rotation
leaves both versions live, and the superseded one is dropped — and its
`SecretString` zeroized — when the last snapshot holding it goes, which is when
the last request compiled against it finishes. That is the strongest honest
statement available: revocation stops *new* candidates resolving a version
immediately, and stops the last in-flight request using it when that request ends.

**Stateless mode is untouched.** `ConfigSnapshot::build` is the same call with an
empty material set, so `[[credential]]`, `env:`, and `file:` references resolve
exactly as they did before any of this existed, and a deployment with no secret
store gains no dependency. A process with no store also refuses a revision that
carries typed credentials, rather than publishing it without its material.

### State tier

Tier 2, exactly as ADR 0027 anticipated: a stateful deployment adds `axond_secret`
to the database it already runs, and the store is reachable only from
`BackendPath::SnapshotCompilation`. No stateless deployment's tier is raised, and
no request path acquires a datastore dependency.

## Consequences

**A database compromise is no longer a material compromise.** The cost is that the
KEK is now a deployment-critical secret with no automated recovery: losing it
makes every stored version unrecoverable, and rotating it is a restage of every
secret rather than a config edit. That trade is deliberate — the alternative is a
store whose security is the database's access control.

**Rotation costs a convergence cycle, not a request.** Staging material, proving a
candidate compiles against it, and activating it are all off the request path, so
a rotation cannot fail an inference request; but material an operator stages is
not in service until a revision that pins it is published.

**Zeroization is bounded by in-flight work, and the bound is stated rather than
hidden.** An operator who needs "this key is unusable now" has revocation, which
takes effect for every future candidate immediately; material already in a serving
snapshot goes when that snapshot does. A shorter guarantee would require killing
in-flight requests, which is a different decision than this one.

**The shipped DDL is a contract, in two places.** `ops/postgres/secret_store_v1.sql`
is what an operator applies by hand and what the deployment docs point at;
`crates/gateway/sql/secret_store_v1.sql` is the copy the binary embeds for
`create_table = true`. They are byte-identical and a test enforces that. Per
[ADR 0009](./0009-durable-usage-sinks.md), a row-shape change is a new
`secret_store_v<N>.sql`, never an edit to this one.

**Security review is triggered and narrow.** This fires threat-model trigger 3
(`SecretStore`, credential delivery, rotation, redaction). Plaintext exists in two
places only — the sealing code and the material a compiled snapshot owns — and the
only way out of `SecretMaterial` remains one greppable `expose()`. No new
`expose_secret` call site reaches the request path, no error, log, metric, or
audit field carries material, and the store's own failures name references,
variables, and file paths only.
