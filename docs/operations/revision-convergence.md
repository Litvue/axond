# Revision convergence

In stateful mode, a change is *published* to the control plane and then
*converges* onto every replica. This page is the operator's view of that second
half: how fast it is, what a replica reports about itself, what happens when it
cannot converge, and how a replica boots while PostgreSQL is unavailable.

Stateful mode is still being assembled: this is the convergence layer
([#142](https://github.com/Litvue/axond/issues/142)), built on the revision
journal ([#165](https://github.com/Litvue/axond/issues/165)). The loop, its
telemetry, and the signed cache are complete and tested, but `serve` does not yet
construct them. Tenants and projects have durable schemas and a projection
([#191](https://github.com/Litvue/axond/issues/191)); the remaining resource
*bodies* (providers, credentials, catalogue models, prices, policies) belong to
the slices that own those schemas. Read
[ADR 0027](../adr/0027-stateless-and-stateful-operating-modes.md) for the mode as
a whole.

## How a change reaches a replica

1. An administrator publishes a revision. The journal advances one head pointer.
2. Every replica polls the head, independently. A PostgreSQL notification, when
   one arrives, only makes a replica poll sooner.
3. A replica that sees a head it is not serving hydrates the **whole** revision,
   projects it onto its configuration, runs the same whole-graph validation boot
   runs, and resolves every secret the result needs.
4. If all of that succeeds, the replica swaps in the new snapshot atomically and
   the next request is served from it.
5. If any of it fails, the replica keeps serving what it already had and reports
   why.

Two consequences worth internalising:

- **Replicas converge independently, so a fleet is briefly mixed.** Two replicas
  can serve different revisions for the length of one convergence. There is no
  fleet-wide barrier, and adding one would mean an unavailable replica could
  block everyone else's rollout.
- **A request never straddles a revision.** A request resolves its alias, its
  credential, and its circuit against the snapshot it started with, and a stream
  relays to completion against that same snapshot. A revision published
  mid-request does not change that request's routing, pricing, or authorisation.

## Convergence targets

| Situation | Time to serve the new revision |
| --- | --- |
| Notification delivered | Compile time (milliseconds), no poll wait |
| Notification lost or disabled | Up to one poll interval, plus compile time |
| Control plane unreachable | Not until it returns; the previous revision keeps serving |
| Revision refused | Never, until the revision is fixed or replaced |

Polling is the mechanism that makes convergence *correct*; notifications only
make it fast. A PostgreSQL `NOTIFY` delivered while a replica is reconnecting is
gone for good, so a replica that waited for one could sit on a stale snapshot
indefinitely. Disabling notifications costs latency, never convergence.

The **convergence target** is the divergence an operator is willing to treat as
normal. Divergence lasting longer than the target is an incident, not rollout in
progress, and that is the threshold to alert on — not the poll interval, and not
a revision-identifier comparison, which is noisy by design during a rollout.

## What a replica reports

Each replica reports three revision identifiers, and they answer different
questions:

| Reported | Means |
| --- | --- |
| `desired` | What the control plane says should be serving. The same on every healthy replica. |
| `loaded` | The newest revision this replica hydrated *and* compiled successfully. |
| `active` | What requests are actually served from. The only one that describes behaviour. |

Read them together:

- `desired == loaded == active` — converged.
- `desired != loaded` — this replica cannot *accept* the desired revision.
  Something refused it; the reported reason says which stage.
- `loaded != active` — a candidate compiled but is not serving. Transient by
  construction, since publication follows compilation immediately.

Alongside them:

- **lag** — how long `desired` has differed from `active`. This is the alerting
  signal. One second of lag is convergence working; ten minutes is an incident.
- **reason** — the stage that refused the last candidate: `unavailable`,
  `incompatible`, `corrupt`, `not_found`, `projection`, `validation`, `secret`,
  or `snapshot`.
- **source** — `control-plane` or `last-known-good`. A replica reporting
  `last-known-good` booted from its cache and may be serving something older
  than desired.
- **generation** — the local snapshot counter, incremented on every publication,
  which request logs correlate against.

### Metrics

| Metric | Use |
| --- | --- |
| `axond.revision.lag` (ms) | Alert when it exceeds the convergence target |
| `axond.revision.converged` | `1` converged, `0` diverged; alert on a *fleet* where this stays 0 for one replica |
| `axond.revision.rejections` (by `reason`) | Which stage is refusing, and whether it is the store or the state |
| `axond.revision.consecutive_failures` | Backoff depth; a rising value is a replica that keeps failing |
| `axond.revision.convergence_duration` (ms) | How long an accepted revision takes to compile and publish |
| `axond.revision.attempts` (by `trigger`, `outcome`) | Whether convergence is being driven by notifications or by polls |
| `axond.revision.desired_at` / `axond.revision.active_at` | Publication timestamps embedded in the revision identifiers, for comparing replicas |
| `axond.revision.last_known_good` (by `outcome`) | Cache exports, export failures, and cold-boot restores |
| `axond.config.generation` | Which snapshot generation a replica serves |

Alert on **lag above the convergence target**, and on
`axond.revision.last_known_good{outcome="restored"}`, which means a replica
started without reaching the control plane.

## Resource body schemas

A published resource carries a **body**: a record whose meaning is fixed by an
explicit schema identifier stored inside it, alongside the resource's identity,
scope, and slug. Two schemas exist today:

| Schema | Resource | Fields |
| --- | --- | --- |
| `axond.tenant.v1` | a deployment tenant | `schema`, `tenant_id`, `display_name` |
| `axond.project.v1` | a tenant-owned project | `schema`, `project_id`, `tenant_id`, `display_name` |

Five rules hold for every body schema, present and future:

- **The identifier is inside the checksummed body.** A replica reads the schema
  before it reads anything else, so a revision cannot be interpreted under a
  schema it did not declare.
- **An unknown schema, or an unknown field, is a refusal.** A body declaring
  `axond.tenant.v2`, or an extra field a newer release added, is rejected rather
  than read partially — as `projection` when a *candidate* is compiled, and as
  `incompatible` when a *stored* revision is hydrated. An older replica keeps
  serving the revision it already had instead of serving a half-understood newer
  one. Roll the replica forward, or publish a revision the deployed version
  understands.
- **A body written before its schema was typed is the same refusal, not
  corruption.** Revisions published before `axond.tenant.v1` and
  `axond.project.v1` existed carry tenant and project bodies with no `schema`
  field. This build does not read them, and it does not guess: hydrating one is
  `incompatible`, and nothing untyped is ever accepted *as* a typed tenancy
  resource. Storage is intact, so there is nothing to repair — republish the
  affected tenants and projects from a build that writes typed bodies, and the
  fleet converges onto the new revision. Older revisions in the journal stay
  unreadable to this build by design; they remain in the journal as history.
- **A body that declares a schema this build reads, and then is not one, is
  damage.** Past the identifier the field set is known, so a `v1` body missing a
  `v1` field, or carrying one whose type changed, is reported as `corrupt` and not
  as `incompatible`: nothing about a release skew can produce it, and the operator
  is pointed at storage rather than away from it. (A *display name* this build will
  not take is the exception, and is `incompatible`: validation rules can tighten
  within one schema — this build refuses an invisible byte-order mark an earlier
  one accepted.)
- **A change to a field's presence or meaning is a new identifier.** `v1` bodies
  never change shape, so a checksum computed by one release is computed the same
  way by every release that accepts it. Adding a field, renaming one, or changing
  what one means all produce `axond.<kind>.v2`.

Identity in a body is bound to its envelope rather than merely carried by it: a
tenant body's `tenant_id` *is* its resource identity, a project's `tenant_id` *is*
the tenant it is scoped to, and a mismatch is refused at publication and again at
hydration. Human-readable slugs live on the envelope, so renaming a tenant leaves
every body and every reference untouched.

### How a project becomes a namespace

The runtime's tenancy boundary is the namespace: keys bind to one, credential
pools are per `(namespace, provider)`, and usage is accounted against it. A
project *is* that boundary made durable, so each published project projects to
exactly one namespace.

Its projected id is **tenant-qualified**: `acme/core`, not `core`. A project slug
is unique within its tenant and only within it — two tenants may each have a
`core` — while a namespace id is deployment-wide. Qualifying is what keeps two
tenants' projects out of one budget, one credential pool, and one key binding, and
`/` is not a legal slug character, so the qualified form decomposes exactly one
way. A projected namespace whose id a bootstrap namespace already claims is
refused rather than merged.

A qualified id is the first namespace id no `axond.toml` could have written, and
nothing consumes one yet. Before the runtime slice wires this projection into
`serve`, `/` has to be checked against every place a namespace id is *used* rather
than declared — metric and trace label values, Redis and Postgres key composition,
and gateway-key bindings — and a config-level charset and uniqueness rule for
namespace ids belongs with that check. Until then the projection's own collision
refusal is the only guard, which is enough because no request path reads a
projected id.

What projection does *not* touch is everything the local file owns: listener,
transport bounds, admission, telemetry, datastore connectivity, and — until their
own slices land — providers, credentials, models, and prices. The bootstrap's
default namespace stays the default, and a projected project starts with no
platform fallback: it borrows no other namespace's credentials.

**No published project is ever made the deployment default.** A request that names
no namespace is served by whatever the file made default, and publishing a project
does not move that target — promoting one, even when it is the only one, would let
an unrelated publication silently redirect unnamed traffic. A bootstrap that
declares no default namespace is therefore refused with reason `projection`, and
the message says so. Since `[[namespace]]` is a control-plane-owned section that a
stateful file may not declare, that is the shape a stateful bootstrap has today:
**stateful serving stays gated until the runtime slice that selects a default from
desired state lands.** Nothing in `serve` constructs this projection yet, so the
refusal is a design boundary rather than an outage.

A stateless deployment is unaffected by all of this. Tenants and projects are
published, never declared in `axond.toml`, and a stateless config's namespace ids
are exactly the ids the file wrote.

## When a replica will not converge

The refusal reason is the triage key.

- **`unavailable`** — PostgreSQL is unreachable. The replica keeps serving,
  retries with exponential backoff, and converges by itself when the database
  returns. No action beyond fixing the database; lag will grow until it does. See
  [during an outage](#during-a-control-plane-outage).
- **`validation`** — the desired revision does not describe a servable
  configuration: an alias pointing at a provider nobody defines, no default
  namespace, a duplicate identifier. This is the same gate a file-configured boot
  applies, so the message is the same one a bad `axond.toml` produces. **Every**
  replica will refuse it. Publish a corrected revision; the fleet keeps serving
  the previous one meanwhile.
- **`secret`** or **`snapshot`** — the revision is fine but material it references
  could not be resolved: a missing environment variable, an unreadable key file,
  a secret the store does not hold. Messages name the *reference*, never the
  value. Frequently replica-specific — one replica missing an environment
  variable while its siblings converge looks exactly like this.
- **`projection`** — a candidate this build cannot project: a resource body it
  does not read, or a bootstrap that is missing something projection may not
  supply for it (today, a default namespace). Roll the replica forward, publish a
  revision the deployed version understands, or fix the bootstrap file.
- **`incompatible`** — a *stored* revision this build cannot read: a schema from a
  newer release, an unknown field, or a body written before that resource's schema
  was typed. Distinct from `corrupt` on purpose — storage is intact and there is
  nothing to repair, so this is an upgrade or a republication, never a database
  investigation. The replica keeps serving its last known good revision and does
  not retry into a different answer. During a rolling upgrade, expect it on
  replicas still running the older build. A body that *declares* a schema this
  build reads and then is not one is `corrupt` instead — see the rules above.
- **`corrupt`** / **`not_found`** — the journal itself does not add up. Retrying
  will not clear it; see
  [when a revision will not load](./control-plane-journal.md#when-a-revision-will-not-load).

A refused candidate is never partially applied. Fetch, hydration, validation,
compilation, and secret resolution all complete before anything is published, so
a replica that refuses a revision is serving exactly what it served before —
same aliases, same credentials, same circuits.

## Retry pacing

A failing replica retries on exponential backoff: doubling from a sub-second
first retry to a ceiling of tens of seconds, then holding at the ceiling. It does
not stop retrying, and it does not tighten under load. The point is that an
extended outage costs a bounded number of attempts per replica — the alternative,
retrying at the poll interval, turns a recovering database into a thundering herd
from every replica at once.

Backoff clears on the first success.

## During a control-plane outage

A **running** replica is unaffected in the only way that matters: it keeps
serving inference from its active snapshot. It cannot learn about new revisions,
so its lag grows and its rejection reason reads `unavailable`, and it converges
without intervention once PostgreSQL returns.

A **new** replica has nothing to serve, and this is where the signed
last-known-good cache matters.

### The signed last-known-good cache

Every replica writes the revision it just published to a local file, and a
replica that boots while the control plane is unreachable may restore that file
instead of failing to start. This is what keeps a database incident from also
freezing fleet size — otherwise an outage during a traffic spike means no
scale-out and no replacement of failed replicas.

What to know about it operationally:

- **It is authenticated, not just serialised.** The file carries an HMAC over its
  contents, verified before a single field is interpreted. Editing the file by
  hand produces a refusal to boot, not a gateway serving hand-written desired
  state. Its signing key is deployment-wide material to provision like any other
  secret; replicas sharing it can read each other's caches.
- **A restored revision is re-verified.** Checksums, scope rules, and references
  are re-checked after the signature passes, so a cache is not a way to smuggle
  in state the journal would refuse.
- **It holds no secrets.** Bodies are resource envelopes; every credential is a
  *reference* resolved through the secret store at compile time.
- **It may be stale.** A replica reports `source = last-known-good` exactly so
  this is visible. Once the control plane returns, the replica converges to
  desired state normally and stops reporting the cache as its source.
- **A cache is a cache, not a fallback for bad state.** It is consulted for the
  two refusals where cached state is the better answer: the control plane being
  *unreachable*, and a desired revision this build cannot *read* (`incompatible`,
  the mixed-version case above). Both leave storage intact and neither is repaired
  by a replica refusing to start — a replica added mid-rollout that would not boot
  withdraws capacity exactly when a rollback needs it added. Corruption, a revision
  past this build's bounds, and a revision that exists but does not compile are all
  fatal at boot: booting an older cached revision instead would hide damage, or
  silently serve state an operator already replaced.

  A replica that boots this way reports `source = last-known-good` and keeps
  reporting `incompatible` for the revision it will not read, so the mixed-version
  state is visible rather than papered over. Roll it forward, as above.

  A rollback that reuses the volume can find a cache the *newer* build exported.
  It is authentic and intact, and this build still cannot read it, so the boot
  refusal names the version skew rather than the cache file: the action is to roll
  the replica forward or repave the volume, not to hunt a disk fault. A cache that
  fails its signature, or holds rows that do not add up, is still reported as the
  cache's own failure.
- **An unwritable cache is a warning, not an outage.** A replica whose disk is
  full keeps serving and logs once; what it loses is the ability to cold-boot
  during an outage.

Without a cache — or with one that fails its signature — a replica that cannot
reach the control plane, or cannot read the revision it finds, refuses to become
ready. It never serves an empty or partial configuration while reporting itself
healthy.

## Related

- [Control-plane revision journal](./control-plane-journal.md) — what is stored,
  and why a revision will not load.
- [ADR 0027](../adr/0027-stateless-and-stateful-operating-modes.md) — the mode,
  its authority rules, and the request path's independence from the control
  plane.
- [ADR 0011](../adr/0011-config-hot-reload.md) — the immutable-snapshot
  publication this converges through, and the one-snapshot-per-request rule it
  inherits.
- [Observability and runbook](../observability.md) — traces, metrics, and boot
  failures.
