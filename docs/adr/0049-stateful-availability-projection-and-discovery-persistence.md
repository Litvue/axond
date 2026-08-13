# 49. Stateful availability projection and discovery persistence

Date: 2026-08-12

## Status

Accepted

Fills the contract of
[ADR 0038](./0038-derived-availability-and-discovery-evaluation.md) with the
projection, persistence, and read surface a stateful deployment
([ADR 0027](./0027-stateless-and-stateful-operating-modes.md)) needs, reading the
durable enablement identities of
[ADR 0042](./0042-model-enablement-and-alias-contracts.md) and the
snapshot-time secret resolution of
[ADR 0039](./0039-envelope-encrypted-secret-store-and-snapshot-time-resolution.md).

## Context

ADR 0038 fixed what availability *means* and constructed nothing. A stateful
deployment now holds every input it named — a pinned catalogue snapshot, tenant
and project model enablements, provider connections, credentials with a
lifecycle and a resolved secret version, and policy documents — and has nowhere
to put the answer. The question an operator actually asks during an incident is
"why can this tenant not call this model", and the deployment can currently only
answer with the catalogue, which is precisely the answer ADR 0038 refuses:
catalogue presence is not proof that a particular account can call a model.

Three properties make this more than reading five fields.

**Evidence outlives revisions.** Discovery evidence is accumulated over hours;
revisions are published in seconds. Evidence held inside a `ConfigSnapshot`
would be destroyed by every price change, so a rollout would spend its duration
reporting `unknown` for models it had already established.

**Evidence outlives processes.** A replica that restarts during a provider
incident has no way to look again — the provider is the thing that is down — so
last-known-good state that lives only in memory is lost exactly when it is worth
having.

**The answer must survive the outage that prompts the question.** An operator
asks about availability when something is broken, which is frequently the
control plane. A read that consults Postgres to answer "what does this replica
think" fails at the only moment it matters.

## Decision

**The projection derives dimensions from a revision and never touches
evidence.** `AvailabilityProjection::project` reads the revision's catalogue
pins, enablements, provider connections, credentials, and policies, and files one
record per `(scope, target)`. It starts from the previous index's *evidence
alone*, so discovery evidence, the retained last-known-good look, and the
definitive watermark are carried across every publication while no dimension
outlives the revision that stated it: a key this revision no longer describes —
a rollback that dropped an enablement, a catalogue snapshot no longer in hand —
keeps its looks under fail-closed dimensions and reads `unavailable`, counted as
`undescribed`, and a key that held no evidence simply ceases to exist.
Each authority keeps its own column, each ignorant answer is a refusal or an
uncertainty, and an enablement whose offering
no catalogue listing in hand carries produces *no record at all* rather than a
permissive one — counted, so a projection that quietly stopped describing a
catalogue does not look like a tenant that enabled nothing.

**Entitlement is credential readiness, not credential existence.** A scope is
entitled when it has a provider connection matching the catalogue provider, a
credential of its own or its tenant's against that connection, that credential is
`active`, *and* the exact secret version it pins is among the material this
candidate resolved. Active-but-unresolved and staged are `unknown` — nothing has
established what the account may call — and disabled, revoked, and tombstoned
refuse. Only references are read; no material enters the projection.

**Catalogue identity and catalogue version stay separate from availability.** A
record is filed against the snapshot digest the enablement pinned. An enablement
pinned to a digest the deployment no longer holds is `withdrawn`, not `absent`
and not `available`, so a catalogue re-import cannot silently change what a
tenant may call, and skew is counted rather than resolved by guessing.

**Runtime health is overlaid, never derived or stored.** A compiled snapshot's
breaker has attempted nothing, so a projected record carries `unobserved`, and
this replica's circuits are joined to the index at the instant a verdict is
asked for. Two replicas answer honestly instead of one answering for the fleet,
and replica-local state never reaches a durable row.

**Evidence is durable, bounded, and detail-free.** Control-plane migration 3 adds
`axond_cp_availability_observation`: one row per record slot (`current`,
`last_known_good`), carrying tenant and project, provider and model, a bounded
result, completeness and source vocabulary, the instants observed, expiring, and
concluded, under the same row-level tenant isolation as every other
control-plane table. What is deliberately *not* stored: the probe's operator
detail — which may carry a provider error body, a URL bearing a key, an account
name — and every dimension the revision states. A restored record therefore
carries evidence and nothing else, and the next projection supplies the
authority; a replica cannot serve availability derived from a revision it is not
running.

**Restoring goes through the same ordering rule as observing.** Rows are
reassembled per record and declared through the retention path, so a stored
positive older than a conclusion the index has already reached is discredited
rather than resurrecting a target a complete listing dropped, and a row naming
another scope is refused and counted. A save replaces every row of the keys it
mentions rather than upserting, so a retained look a later conclusion discredited
stops existing durably.

**Both halves are off the request path.** Evidence is loaded once at boot and
written by whatever takes the looks; the projection runs inside compilation,
which already resolves secrets and is the one place durable material enters the
process. Inference reads neither.

**The read is scoped, redacted, and answered from memory.**
`GET /admin/v1/availability?tenant=&project=` requires a `read_availability`
grant that encloses the scope asked about, and must name a tenant — a
deployment-wide answer would be every tenant's entitlements in one document. It
is answered from the index the snapshot carries plus this replica's circuits, so
it reaches no store. A grant narrower than the deployment sees the namespace
projection of each verdict, which withholds the discovery source and coarsens the
reasons that describe the deployment's own machinery. A replica that derives no
view reports `deriving: false` rather than an empty target list, because "we
derive nothing" and "you may call nothing" are different answers.

### State tier

Tier 2. The observations table is Postgres-backed and tenant-isolated; the
projection and the read are in-memory. A Tier 0 deployment carries the empty
index and the same evaluation path, and no tier puts a catalogue, discovery,
Postgres, or `SecretStore` read on an inference request.

## Alternatives considered

- **Keep evidence in the snapshot.** Simplest, and every publication erases what
  discovery established — a price change would reset a provider's listing.
- **Persist the derived dimensions too.** Then a restarted replica can serve
  availability derived from a revision it is not running, and desired state has a
  second, staler copy.
- **Persist the probe's detail for support.** The one field that can carry a
  provider error body and a URL bearing a key, durable and readable by anyone with
  the database: a log line is not worth a durable disclosure surface.
- **Answer availability from the control plane.** It is the outage that prompts
  the question.
- **Recompute the watermark from the stored looks.** A complete listing that
  dropped a target retains nothing, so its instant is unrecoverable, and a stored
  positive would come back and resurrect the target the listing removed.
- **Upsert rows instead of replacing a key's set.** Leaves a discredited retained
  positive in the database for the next failed refresh to rest on.

## Not decided here

- No provider is polled and no probe is written: what *takes* a look, on what
  schedule, and with what budget is a separate slice, and a generation probe that
  costs money is not enabled by default.
- Admission and `/v1/models` are unchanged. Wiring availability into what a
  request may do fires the catalogue and model-entitlement trigger of the
  [security review checklist](../security/threat-model-review.md) again.
- Readiness does not read an index: a discovery outage degrades verdicts and must
  never fail a probe.

## Consequences

- An operator can ask which authority refused, per tenant, during an outage, and
  get an answer that names catalogue, enablement, entitlement, policy, discovery,
  or this replica's own health.
- Discovery evidence survives publications and restarts, and degrades on its own
  terms — `available (last_known_good)`, then `stale` — rather than becoming a
  denial or a readiness failure.
- The observations table is a new durable tenant-scoped surface; it inherits the
  row-level isolation and the migration ledger of the rest of the control plane,
  and carries no free text.
- `deriving`, the state and reason vocabularies, and the namespace projection are
  a compatibility surface once anything scripts against them; new codes are
  additive under the [0.x policy](./0015-zero-dot-x-compatibility-policy.md).
