# Administering a stateful deployment

In `mode = "stateful"` ([ADR 0027](../adr/0027-stateless-and-stateful-operating-modes.md))
a deployment's tenants, projects, providers, credentials, catalogues, model
enablements, aliases, and policy are owned by the Postgres control plane and
changed through `/admin/v1` — the same surface `axond admin` calls. TOML still
owns bootstrap: the listener, transport and admission bounds, telemetry, the
control-plane and secret-store connections, and the breakglass credential. None
of those are publishable through this API, and no administrative read happens on
an inference request.

## What a stateful replica serves today

| Surface | Status |
| --- | --- |
| `/admin/v1` | Served, authenticated, backed by the control plane. |
| `/healthz` | `200` — the process is alive and administrable. |
| `/readyz` | `503` — never route inference traffic to it. |
| `/v1/...` | `503 inference_unavailable`. |

A published revision cannot be compiled into a runtime snapshot yet (the
projection is [revision convergence](./revision-convergence.md) work), so
inference is refused per request rather than answered from an empty
configuration — an empty configuration would look, to a caller, like a
deployment that is configured and simply lacks what was asked for. `axond check
preflight` reports the same refusal, from the same function, before a rollout
gates on it. **Serve inference from `mode = "stateless"` until convergence
ships**; use stateful mode to build and review desired state.

A stateless deployment answers every `/admin/v1` path with
`501 stateful_mode_required`, before authentication and without opening any
backend: the mode is the answer to the question that was asked, where a `404`
would be indistinguishable from an older build and a `401` would invite a caller
to find a credential that cannot exist.

### One path under the prefix is not this surface

`GET /admin/v1/status` is the replica diagnostic
([the observability runbook](observability-runbook.md)), not an administrative
route: it reads this process's cached component states, never the control plane,
and it answers in both modes. It therefore takes a *gateway* credential with the
`status` capability rather than a breakglass one, and it is the one path under
the prefix that a stateless deployment does not refuse.

That matters if you place the prefix behind a network boundary. A deployment
that firewalls `/admin/v1/*` to an administrative network, or fronts it with a
proxy that enforces breakglass authentication, will make the diagnostic
unreachable or reject the credential it expects — so route `/admin/v1/status`
with the inference listener and the rest of the prefix with the administrative
one. The method contract is uniform either way: a wrong method on it answers
`405 admin_method_not_allowed`, like every other path under the prefix.

## Authentication

`/admin/v1` has its own authentication layer and its own credentials. An
inference credential — an `axt1.` bearer token or an `x-api-key` — is not an
administrative one and is refused; there is no capability that would let one
administer, because the two route tables share no code path.

Today's administrative credential is breakglass, declared in bootstrap TOML:

```toml
[[admin_breakglass]]
env = "AXOND_ADMIN_BREAKGLASS"   # or `file = "/run/secrets/breakglass"`
```

It is presented as `Authorization: Bearer <credential>` and must be attributed:

| Header | Meaning |
| --- | --- |
| `x-axond-breakglass-operator` | Who is acting. Recorded on every revision. |
| `x-axond-breakglass-reason` | Why. Recorded on every revision. |

## Every mutation is a complete candidate

A write is a `POST` of one complete resource document, never a `PATCH`. The
service loads the complete current desired state, applies the document's edit to
it, validates the **whole** candidate — every reference, every ownership rule,
every wire-family constraint — and publishes it as one immutable revision. A
refused candidate publishes nothing at all.

Nothing on this surface removes a resource: a revision supersedes versions and
retains what history resolves against, so the only deletion there is is a
resource's own terminal lifecycle state — a tenant `deleted`, a credential
`revoked`, an enablement or alias `disabled`. `"mutation": "delete"` is
therefore accepted only for a document that states one of those, and refused
with `admin_request_invalid` for a document that leaves the resource serving:
an auditor filtering the trail for `delete` is asking what stopped serving, and
a rename wearing that label would answer wrongly.

A request body is bounded at 1 MiB, refused as `413 admin_request_too_large`.
The bound is the surface's own rather than the inference `max_request_bytes` an
operator tunes for their models: an administrative document is identifiers and a
summary, and the largest thing publishable is a catalogue *reference*, because a
snapshot's payload is content-addressed and never crosses this surface.

Two preconditions are required on every write:

| Header | Value |
| --- | --- |
| `idempotency-key` | A retry-safe key. Replaying it returns the original revision; reusing it for a *different* candidate is `409 idempotency_key_reused`. |
| `x-axond-expected-revision` | The revision the change was written against, or `empty` for a control plane that has never published. A stale value is `409 revision_conflict`, which names the current head. |
| `x-axond-dry-run` | Optional. `true` validates and diffs without publishing or recording anything. |

The response is the same shape whether it published, replayed, or dry-ran:

```json
{
  "result": "published",
  "revision": "rev_...",
  "base": "rev_...",
  "checksum": "...",
  "mode": "apply",
  "diff": { "changes": [ { "kind": "provider", "id": "...", "change": "added" } ] }
}
```

The diff is semantic — resources matched by kind and id — and never carries
secret material: a credential document names a secret *reference* and its
lifecycle, so there is no material in a candidate to leak into a diff, an audit
record, or a state read.

## Routes

| Route | Method | What it does |
| --- | --- | --- |
| `/admin/v1/state` | `GET` | The complete desired state at the current revision, without resource bodies. |
| `/admin/v1/history` | `GET` | Recent revisions, newest first. `?limit=` is 1–100. |
| `/admin/v1/audit/{revision}` | `GET` | One revision's actor, summary, and recorded changes. |
| `/admin/v1/convergence` | `GET` | What this replica has loaded and activated, from its own cached status — never a control-plane read. |
| `/admin/v1/availability` | `GET` | What this replica derives about one scope's models. `?tenant=` is required, `?project=` optional; answered from the snapshot it is serving and its own circuits — never a control-plane read. |
| `/admin/v1/catalogue` | `GET` | One tenant's management catalogue: what it has enabled, its first-class aliases and ordered targets, and why a model is not routable. `?tenant=` is required. |
| `/admin/v1/tenants` | `POST` | A tenant and its lifecycle. |
| `/admin/v1/projects` | `POST` | A project (namespace) inside a tenant. |
| `/admin/v1/providers` | `POST` | A provider connection: wire family and endpoint. |
| `/admin/v1/credentials` | `POST` | A provider credential: a secret reference and its lifecycle. |
| `/admin/v1/catalogs` | `POST` | A provider's model catalogue snapshot. |
| `/admin/v1/models` | `POST` | A model enablement and its observed price. |
| `/admin/v1/aliases` | `POST` | A routing alias and its ordered targets. |
| `/admin/v1/policies` | `POST` | Budgets, concurrency limits, and token epoch for a scope. |
| `/admin/v1/rollback` | `POST` | Republish an earlier revision's complete state as a *new* revision. |
| `/admin/v1/secrets` | `POST` | Store credential material as a new secret's first version, staged. |
| `/admin/v1/secrets/rotate` | `POST` | Store material as the *next* version of an existing secret, staged. |
| `/admin/v1/secrets/lifecycle` | `POST` | Move one version: `active`, `disabled`, `revoked`, `tombstoned`. |
| `/admin/v1/secrets/{secret}` | `GET` | Every version of one secret, with its state and whether it still resolves. |

The four `secrets` rows are the only place material crosses this boundary, and
it crosses one way: they take material and answer with a reference, an owner,
and a lifecycle state. No route — and no method on the store behind them —
returns material that was stored earlier, so an administrator who can rotate a
provider key cannot read the one in service.

They are also the only writes that carry no idempotency key and no expected
revision, because they publish no revision: storing material changes nothing a
request can observe until a credential document pins it. What stands in for the
preconditions is the shape of the operations — staging mints a fresh version
rather than overwriting one, and a lifecycle move to the state a version already
holds answers `"changed": false`, so a retry is not a second change.

The versions read is intentionally bounded to the caller-named tenant (and
project, when present) and secret. It returns store metadata only — opaque
references, version numbers, lifecycle, and lifecycle-only `resolvable` status;
it does not unwrap material, consult desired state, or report provider
reachability. An absent or foreign secret returns the same empty version list,
so this rotation-status view is not a cross-tenant existence or control-plane
calibration endpoint.

An availability read names which authority refused — the catalogue, the
enablement, the tenant's entitlement, policy, discovery, or this replica's own
health — and never claims a model is available because the catalogue carries it
([ADR 0053](../adr/0053-stateful-availability-projection-and-discovery-persistence.md)).
A replica that derives no view answers `"deriving": false` rather than an empty
list of targets, and a caller without deployment-wide authority sees a verdict
with the discovery source dropped and operator-only reasons coarsened — the
query always names a tenant, so it is the caller's authority that decides this,
not the scope asked about. Asking about a project answers with what the project
inherits from its tenant as well as what it overrides, because a project's
enablements are overrides rather than a catalogue of its own.

Because they publish no revision, they also write no `AuditEvent`: a successful
stage, rotation, activation, revocation, or destruction is recorded only as an
operational event on the `axond.admin.secrets` log target rather than in the
control plane. That is a process-log record, not a durable audit trail: its
delivery and retention depend on the deployment's logging pipeline, and if it is
dropped or expires there is no SecretStore audit history or backfill for
`/admin/v1/audit/{revision}` to recover. The event contains only actor, owner,
operation, opaque references, and lifecycle metadata. Refusals of authority are
the exception and still record durably as access denials; a storage or lifecycle
failure is not converted into an audit event either.

Refusals from these routes describe the shape a field wants and never quote what
was presented — a provider key pasted into `reference`, `secret`, `tenant`, or
`project` must not come back out in the response or an operator log line.

Budgets and limits are policy fields rather than a route of their own: they are
published as a `policies` document, and history is therefore one chain rather
than one per knob.

Rollback never rewinds the journal. It reads an earlier revision's complete
state and publishes it forward, so the history that produced an incident is
still readable after the incident is undone.

## The management catalogue

`GET /admin/v1/catalogue` answers what one tenant may use, read from the
published revision — never from a provider, and never from anything the request
path touches. It is the only read on this surface whose scope comes from the
query rather than from the grant: `?tenant=` is required, `?project=` narrows it
to a project, and the grant must cover the scope asked for. A tenant-scoped
administrator therefore reads its own tenant and gets `403` for another's, with
no evidence in either answer that another tenant's enablements exist.

| Parameter | Meaning |
| --- | --- |
| `tenant` | Required. The tenant whose catalogue is read. |
| `project` | Read the project's effective catalogue: its own overrides, and the tenant defaults it inherits. |
| `state` | `enabled` or `disabled`. |
| `wire_family` | The wire contract an offering speaks. |
| `offering` | One offering id, for a single-model read. |
| `billable` | `true` or `false`: whether an approved price makes the offering billable. |

An unknown parameter is refused with `400 admin_request_invalid` rather than
ignored. A caller that asked to narrow and was silently not narrowed would read
the answer as authoritative, so the filters this build cannot yet evaluate —
`provider`, `capability`, `modality`, `availability`, all of which need the
catalogue-import and availability slices' metadata — are refused by the same rule,
and the response names the same gap in `pending`:

The query is bounded to 2 KiB and six parameters (the required `tenant` plus the
five supported filters). Repeated parameters are rejected by the query parser;
there is no “last value wins” interpretation for a catalogue filter.

```json
{
  "revision": "rev_...",
  "scope": { "kind": "tenant", "tenant": "..." },
  "aliases": [
    {
      "alias": "res_...",
      "version": 1,
      "slug": "default",
      "scope": { "kind": "project", "tenant": "...", "project": "..." },
      "wire_family": "openai-chat",
      "state": "enabled",
      "routable": true,
      "unavailable": [],
      "targets": [
        { "enablement": "res_...", "version": 1 }
      ]
    }
  ],
  "entries": [
    {
      "offering": "...",
      "catalog_snapshot": "...",
      "enablement": "...",
      "version": 1,
      "slug": "gpt-4o",
      "scope": { "kind": "tenant", "tenant": "..." },
      "wire_family": "openai-chat",
      "state": "enabled",
      "effective": true,
      "routable": false,
      "billable": false,
      "aliases": ["default"],
      "unavailable": ["unpriced"]
    }
  ],
  "pending": ["offering-metadata", "availability"]
}
```

`unavailable` is why a caller of this tenant cannot route to the offering:
`disabled` (its lifecycle), `shadowed` (a project override replaces the tenant
default this entry reports), `unpriced` (no approved price, so no request can be
billed) or `unaliased` (no enabled alias names it). `routable` is the absence of
all of them; a read of a project reports the tenant default beside the override
that shadows it, so an operator can see both without a second request.

An active alias must have a non-empty, reachable target list in its own project
or tenant and all targets must share its wire family; disabled fallback entries
may remain in priority order while a later mutation retargets the name.
Disabling an alias is the withdrawal operation: it may publish with an empty
target list, retaining the alias identity and revision history without leaving an
active routing graph.

`aliases` is the alias resource projection, separate from the `aliases` name list
on each offering entry. It preserves the durable alias id and version, its
project scope and wire family, and the ordered enablement targets. The target
references are not provider-local model ids: resolving those requires the pinned
catalogue snapshot, and remains named in `pending` until that metadata reader is
attached. `routable` and `unavailable` are derived from desired state: an enabled
alias is routable when any exact target is enabled and billable. Project override
precedence does not rewrite an alias's explicit tenant-default target. The reasons
are `disabled`, `no-targets`, `disabled-target`, and `unpriced-target`; an
unusable fallback does not make the alias unavailable if a later target is usable.

## Conditional reads

Every read carries an `ETag` over the bytes of the response, and answers
`304 Not Modified` — with the same validator and no body — to a request whose
`If-None-Match` names it. A dashboard or a reconciler waiting for a revision to
converge can therefore poll without paying for a control-plane read and a
complete desired state on every tick.

The validator is a digest of the projection, not a revision id: `/convergence`
has no revision of its own to name, and a projection changes shape between
releases while the revision does not. Treat it as opaque — compare it and echo
it, and do not parse it. An `If-None-Match` this surface cannot read is treated
as absent and answered in full, because a `304` against a validator nobody
issued would hand an operator a stale answer mid-incident.

`/state`, `/history`, `/audit/{revision}` and `/secrets/{secret}` are validated
strongly, by their own bytes. `/convergence` is the exception: it reports how
long this replica has been behind, so while it is behind its bytes differ on
every read and a digest of them would never match — for exactly the caller that
wants it to. That read is validated over the convergence *state* — everything
but the growing `lag_ms`: the desired, loaded and active revisions, the snapshot
source, the generation, the last convergence duration, the failure count and the
rejection reason — and answers a weak validator, `W/"…"`.
A `304` there may therefore withhold a body whose `lag_ms` has moved on; when a
`200` arrives, the lag it reports is current.
`If-None-Match` is compared weakly, so a caller sends back whatever validator it
was given and needs no special handling.

Every read also carries `Cache-Control: private, no-cache` and
`Vary: Authorization`: these answers are per-caller — a tenant-scoped grant reads
a narrower projection of the same revision — so an intermediary may keep a
representation but must revalidate it, never serve it to another administrator.

A validator is not a credential. A conditional read authenticates and is
authorized exactly like an unconditional one, and a refusal carries no `ETag`.

## Scope

An administrator's grant is deployment-, tenant-, or project-scoped, and a
deployment grant contains its tenants, which contain their projects. Scope is
checked against the resources a mutation *touches*, not against the whole
candidate: a project administrator's candidate necessarily retains every tenant
it does not own, and retaining them is not a cross-tenant write. Naming another
tenant's resource is.

## Typed refusals

Every distinguishable outcome has a stable `code`, so a script branches on the
code rather than on prose:

```json
{ "error": { "code": "revision_conflict", "message": "..." } }
```

`stateful_mode_required`, `admin_unauthenticated`, `admin_forbidden`,
`revision_conflict`, `idempotency_key_reused`, `validation_failed`,
`admin_request_invalid`, `history_limit_invalid`, `control_plane_unavailable`,
and the rest of the vocabulary are listed in `AdminError::CODES`, which a test
holds in step with the enum. Operator detail — a backend's message, a store's
DSN — is logged, never serialized.

`name_taken` is the one refusal an operator most often meets by accident: a
`409` saying that the slug a tenant or project asked for is already projected to
another row. It is caller-fixable, and the two runbooks below are how.

## Runbooks

### Reclaiming a tenant or project name

Slugs are unique among *live* rows only. A tenant whose lifecycle is `deleted`
releases its slug — the projection's unique index is partial — so:

- **the name is free** when the row that holds it is `deleted`, and a deleted
  tenant may be reactivated under its old slug for as long as nothing else has
  claimed it in the meantime;
- **the name is held** by any tenant that is not `deleted`, including one no
  longer named by the current revision: desired state supersedes rows, it does
  not forget them, so a name is released by a lifecycle move and never by
  omission. Publish the holder as `deleted`, then publish the new row.
- **restoring a deleted tenant whose slug was taken** is refused with
  `name_taken`, naming the slug. Reactivate it under a different slug, or delete
  the tenant that took the name first; the gateway will not silently rename
  either row.

A **project** slug is unique within its tenant and is *not* released this way:
a project has no lifecycle, its projected row is retained so history keeps
resolving, and `UNIQUE (tenant_id, slug)` on that row is unconditional. There is
therefore no way to hand a retired project's name to a new one — publish the new
project under a different slug, or rename the retired project by publishing it
with the slug you want it to keep.

### Rotating a provider credential without a redeploy

Material and the document that points at it are separate changes on purpose: the
new version is stored and provable while the old one keeps serving, and the
cutover is one ordinary publication whose rollback is another.

```console
$ printf %s "$NEW_KEY" | axond admin secret rotate --tenant ten_01J... \
    --reference sct_01J...@v1                       # stages sct_01J...@v2
$ axond admin secret lifecycle --tenant ten_01J... --reference sct_01J...@v2 --state active
$ axond admin apply --resource credentials --file credential-v2.json \
    --idempotency-key rotate-openai-1 --expected-revision rev_01J...
$ axond admin secret lifecycle --tenant ten_01J... --reference sct_01J...@v1 --state disabled
$ axond admin secret versions --secret sct_01J... --tenant ten_01J...
```

Material is read from a file or standard input, never from a flag, for the same
reason `AXOND_ADMIN_TOKEN` is an environment variable.

The credential document names `secret` and `secret_version`; publishing it is
what makes the new version servable, because material is resolved while a
candidate snapshot is compiled and never on a request. A version that cannot be
resolved — disabled, revoked, destroyed, or unreachable — therefore **fails the
candidate** and leaves the last-known-good snapshot serving. Cutting over to a
version that was never activated is a failed publication, not an outage.

Rotating twice from the same base reference is refused with
`secret_version_exists` and `409`, naming the version that already holds
material: a version is immutable, so the second call's rotation had already
happened — by a retry of the same request, or by another administrator — and
overwriting it would change what a credential already pinning it resolves to.
The material presented with that call is never examined, so the refusal does not
report it as bad; re-read `axond admin secret versions` and rotate from the
current version.

Rolling a rotation back is publishing the previous credential document again, or
`axond admin rollback`: the old version is still there, still resolvable, until
it is withdrawn.

Withdrawal has two strengths:

- **`revoked` and `disabled`** stop the *next* candidate from resolving the
  version and are never gated: a leaked key must be withdrawable immediately.
  The snapshot serving requests at that moment holds its own resolved copy and
  keeps serving until a candidate replaces it — the same last-known-good
  behaviour every other convergence failure has.
- **`tombstoned`** destroys the material. It is refused with `secret_in_use`
  while the current revision still pins the version, so the order is: publish a
  credential that no longer resolves it, then destroy. The plaintext a running
  replica already resolved is zeroized once no active snapshot holds it.

### Refreshing a model catalogue

A catalogue resource *is* its snapshot: an enablement records the digest it read
an offering from, and that pin is immutable, because re-pointing it is how a
published alias's meaning would change underneath its callers. Re-importing
different content under a catalogue an enablement reads from is therefore
refused with `validation_failed` and the rule `pinned_snapshot_withdrawn`,
naming the enablement that holds the pin.

The refresh is published alongside the old snapshot instead:

```console
$ axond admin apply --resource catalogs --file catalogue-2026-08.json ...   # a new catalogue id
$ axond admin apply --resource models   --file retire-gpt-4o.json ...       # "state": "disabled"
$ axond admin apply --resource models   --file enable-gpt-4o-2026-08.json ...
$ axond admin apply --resource aliases  --file retarget-default.json ...
```

Disabling the old enablement is what frees the offering for its replacement: a
disabled enablement resolves to nothing, so it holds no offering, while every
revision that served it stays readable and auditable. Aliases keep pointing at
the enablement they name until they are retargeted, so the switchover is one
alias publication and its rollback is another.

## `axond admin`

The CLI is an HTTP client for the routes above and deliberately nothing more: it
never opens the control plane, so there is no command that can publish without
the same key, precondition, identity, and validation the API enforces.

```console
$ export AXOND_ADMIN_TOKEN=...            # never a flag: flags reach `ps` and shell history
$ export AXOND_ADMIN_ENDPOINT=https://axond.internal:8080
$ axond admin state
$ axond admin history --limit 20
$ axond admin audit --revision rev_01J...
$ axond admin convergence
$ axond admin resources                   # which documents `apply` accepts
```

`--endpoint` overrides `$AXOND_ADMIN_ENDPOINT`, which overrides
`http://127.0.0.1:8080`; `/admin/v1` is appended to it. `--operator` and
`--reason` carry breakglass attribution on any command.

`$AXOND_ADMIN_TOKEN` is a bearer credential for the whole control plane, so an
`http://` endpoint naming anything but this host is refused before the request
is built — a mistyped scheme would otherwise put the token on the wire in the
clear. Loopback is exempt, because there is no wire; a deployment that
terminates TLS in a sidecar on the same trusted path opts in explicitly with
`--insecure-plaintext`.

A mutation is a document, from a file or standard input:

```console
$ cat > tenant.json <<'JSON'
{
  "summary": "onboard the acme tenant",
  "mutation": "create",
  "resource": {
    "tenant": "01J000000000000000000TENANT",
    "slug": "acme",
    "display_name": "Acme"
  }
}
JSON
$ axond admin apply --resource tenants --file tenant.json \
    --idempotency-key onboard-acme-1 --expected-revision empty --dry-run
$ axond admin apply --resource tenants --file tenant.json \
    --idempotency-key onboard-acme-1 --expected-revision empty
```

The document is the route's own envelope — `summary`, `mutation`
(`create`/`update`/`delete`/`rotate`), and the typed `resource` — so the schema
has exactly one definition, and `--dry-run` against the real deployment is how
an operator checks a document before applying it. Rerunning the second command
after a lost response replays it rather than publishing twice.

Rollback takes flags rather than a document, because its whole input is which
revision and why:

```console
$ axond admin rollback --revision rev_01J... --summary "undo the bad alias" \
    --idempotency-key rollback-1 --expected-revision rev_01J...
```

A refusal is printed as the gateway sent it, typed `code` included, and exits
non-zero.

`axond admin secret` is the credential lifecycle, and reads material from
`--material-file` or standard input:

```console
$ axond admin secret stage     --tenant ten_01J... --material-file ./key.txt
$ axond admin secret rotate    --tenant ten_01J... --reference sct_01J...@v1 -f -
$ axond admin secret lifecycle --tenant ten_01J... --reference sct_01J...@v2 --state active
$ axond admin secret versions  --secret sct_01J... --tenant ten_01J...
```

One trailing newline is stripped and nothing else is, so `printf %s` and a file
written by an editor both store the key the operator holds.

## Related

- [ADR 0027 — stateless and stateful operating modes](../adr/0027-stateless-and-stateful-operating-modes.md)
- [Control-plane revision journal](./control-plane-journal.md)
- [Revision convergence](./revision-convergence.md)
- [Configuration reference](../configuration.md#stateful-bootstrap)
