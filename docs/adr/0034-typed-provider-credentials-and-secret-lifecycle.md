# 34. Typed provider credentials, opaque secret references, and a secret lifecycle

Date: 2026-08-12

## Status

Accepted

Types the credential half of the stateful mode chosen in
[ADR 0027](./0027-stateless-and-stateful-operating-modes.md), on the namespaced
BYOK model of [ADR 0003](./0003-namespaced-credentials-and-byok.md), and keeps
`SecretStore` at `SnapshotCompilation` where ADR 0027 put it.

## Context

Stateful mode gives a deployment durable provider credentials: a tenant brings
its own key, an operator publishes it, and a snapshot is compiled against it. The
resource that records this is the one resource in the system that must describe
material it is forbidden to hold, and until now its body was an untyped record.
Three questions had no answer in the domain:

- **Which material does a published revision mean?** A body naming a secret
  without a version means "whatever is stored under that name now", so a rotation
  silently changes what an already-published revision authorizes — and a rollback
  to that revision does not roll the material back.
- **Whose material is it?** Ownership lived on the resource envelope, while the
  reference inside the body was a bare string. Nothing stopped one tenant's
  credential from naming another tenant's material, and because a reference
  discloses nothing about what it points at, nothing downstream could notice.
- **What may be done with it?** "Delete the credential" was the only withdrawal,
  which conflates *stop using this key* with *forget it happened*, and gives an
  operator no way to stage material, prove a revision compiles against it, and
  only then put it in service.

The answers cannot be pushed to the request path: a request never unwraps a
secret. They have to hold at publication, and again when a replica hydrates a
stored revision, or the two boundaries disagree about the same rows.

## Decision

A provider credential's body is the typed, versioned record
`axond.provider-credential.v1`, and it carries a reference, an owner, and a
state — never material, and nothing derived from material:

```
schema, credential_id, tenant_id, project_id?, provider_id,
display_name, secret_id, secret_version, lifecycle
```

- **A reference is opaque and exact.** `SecretId` (`sct_…`) is its own type, not
  a `ResourceId`, so a credential's identity and the identity of the material it
  points at are not interchangeable — including in their text forms. A
  `SecretRef` always carries a one-based `SecretVersion` (`sct_…@v2`), so a
  revision pins the exact material it was compiled against and a later rotation
  cannot retroactively change what a published revision meant.
- **Rotation mints a version; it does not edit one.** Material is immutable per
  version. Rotating stages a *new* version under the same secret id and publishes
  a new resource version of the credential; the revision that pinned the old
  version keeps pinning it. Because one resource names one version, an
  uninterrupted rotation is two credential resources — the serving one untouched
  while the new one is staged and proven — and the old one is withdrawn after the
  new one is active. Repointing a single credential is the deliberate cut-over.
- **Ownership is the envelope's scope, exactly.** A `SecretOwner` is a tenant and
  optionally one of its projects, derived from the resource's scope rather than
  authored beside it, so the owner of the resource and the owner of the material
  cannot disagree. Ownership is not a hierarchy: a project's credential does not
  resolve its tenant's material. `SecretStore` takes the owner as an argument, so
  every resolution is scoped by construction rather than by a caller remembering
  to check.
- **Lifecycle is a total, deterministic relation.** `staged → active → disabled ⇄
  active`, any of those `→ revoked`, and `revoked → tombstoned`. Every ordered
  pair of states is a permitted move, an idempotent no-op, or a typed refusal;
  nothing depends on wall-clock time, on which administrator arrived first, or on
  how many times a request was retried, so republishing the same desired state is
  a no-op rather than a conflict. `staged` and `active` resolve; the rest do not.
  Revoked material never returns to service, and tombstoning is what destroys
  stored bytes.
- **Contradictions are refused before publication, and again on hydration.**
  `DesiredState::validate` reads every credential body, so one secret has one
  owner, a credential authenticates only to a provider its owner can reach (its
  own scope or its tenant's), two references to one version agree about that
  version's state, and at most one version of a secret is active. A revision that
  would make "which key authorizes this request" depend on iteration order does
  not publish.
- **`SecretStore` is a contract, and its only implementation is a test double.**
  `SecretResolver` (resolve, exists) is split from `SecretStore` (stage, rotate,
  transition, describe) so a resolving caller cannot mint or move material.
  `exists` answers what `resolve` would do rather than whether a row is present,
  so a check built on it cannot approve withdrawn material, and it answers
  identically for another owner's reference and an absent one. The in-memory fake
  states the contract executably and is not a selectable backend.

Stateless mode is untouched: `[[credential]]` material still comes from TOML,
`env:`, or `file:` through `crate::credentials`, which has no `SecretRef` in it
and no dependency on any of this.

### State tier

Tier 0. Nothing here selects a backend, opens a connection, or runs at boot: it
is domain types, publication-time validation, and a trait with a test-only
implementation. A stateless deployment publishes no revisions and therefore
reads no credential bodies. The durable `SecretStore` this contract is written
for stays Tier 2 and unimplemented, as
[ADR 0027](./0027-stateless-and-stateful-operating-modes.md) leaves it, and no
existing deployment's tier is raised.

## Consequences

**A published revision means one thing forever.** Pinning an exact version makes
rollback restore the material as well as the manifest. The cost is that rotation
is two deliberate steps — stage, then activate — and that a rotation an operator
expects to take effect on its own does not.

**Withdrawal has degrees, and one of them is irreversible.** `disabled` stops
resolution reversibly; `revoked` cannot return to service, and its only onward
move is `tombstoned`, which destroys the bytes. An operator who revokes in order
to pause has to publish a new secret version to serve again. That asymmetry is
deliberate: the reversible state exists so the irreversible one can mean what it
says.

**Untyped credential bodies stop hydrating on upgrade — as a compatibility
refusal, not as corruption.** A revision published before this schema existed
carries a credential body with no `schema` field. This build refuses it and says
so as `incompatible`, so storage is not implicated and the replica keeps serving
what it holds; republishing the affected credentials from this build converges the
fleet. A lifecycle identifier this build does not know is classified the same way,
so a newer release may add a state without older replicas reporting damage.

**A `provider_id` naming a row this revision does not declare stays readable.**
Requiring it would stop revisions already in the journal from hydrating, for the
reason the tenancy bodies record: such a reference is *unresolvable* at the
boundary that resolves it, which is not the same as unreadable here.

**Security review is narrower than it looks.** The body has no field a plaintext,
a fingerprint, a prefix, or a length could travel in, and adding one is a
disclosure change, since bodies are canonically encoded into a checksum an
operator reads in a manifest. This fires threat-model trigger 3 (`SecretStore`,
credential delivery, rotation, and redaction); no new `expose_secret` call site
reaches the request path.

## Alternatives considered

- **A `ResourceId` for secrets.** One id type is less machinery, but it makes a
  credential's own id and the id of its material substitutable in every call and
  every log line, and a mistake reads as a valid lookup against the wrong table.
- **A latest-version reference.** "Use whatever is current" is what an operator
  rotating a key usually wants, and it is exactly what makes a published revision
  ambiguous and a rollback partial. The two-step rotation buys immutability.
- **Lifecycle as a boolean, or as row deletion.** `enabled`/`disabled` cannot
  express "loaded but not yet in service" or "never usable again", and deletion
  destroys the record of a compromise at the moment it matters most.
- **Lifecycle on the envelope rather than in the body.** It would apply to every
  resource kind, where it means nothing, and it would put a security decision
  outside the body whose checksum an operator compares.
