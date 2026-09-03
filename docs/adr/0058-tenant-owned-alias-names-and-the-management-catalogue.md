# 58. Tenant-owned alias names and the management catalogue

Date: 2026-08-13

## Status

Superseded by
[ADR 0063](./0063-stateful-only-namespaced-gateway.md).

Tenant-owned alias names and `GET /admin/v1/catalogue` are withdrawn.
Namespaces are API resources; inference is `/ns/{ns}/v1/...`. This
record is retained for history. Do not implement from it.

Previously accepted, and previously partially superseded by
[ADR 0062](./0062-blob-backed-flat-namespace-control-plane.md).

Alias-name isolation and the distinction between serving and management views
remain in force. Namespace ownership replaces tenant ownership, and consumers
own every tenant/project mapping outside Axond.

Completes the tenant-facing half of
[ADR 0042](./0042-model-enablement-and-alias-contracts.md): 0042 types what a
tenant is enabled for and under which names, and this record decides what a
caller may *see* of it and where an administrator asks. It keeps the request-path
rule of [ADR 0002](./0002-stateless-by-default-stateful-by-opt-in.md) and
[ADR 0027](./0027-stateless-and-stateful-operating-modes.md) — inference reads a
published snapshot, administration reads the control plane — and inherits the
entitlement stance of item 2 of the
[security review](../security-review-2026-08-05.md): `/v1/models` is
authenticated and scoped.

## Context

An alias is the name an SDK sends. Two facts about it had no home.

- **Whose name is it?** A runtime alias was a deployment-wide row: any namespace
  with a credential for one of its targets could enumerate and invoke it. That is
  right for a single-tenant deployment and wrong for a multi-tenant one, where
  `fast` is one tenant's routing decision and another tenant's `fast` may point
  somewhere else entirely. Without ownership, two tenants cannot both hold the
  name, so the deployment's operator becomes the arbiter of a per-tenant choice —
  and one tenant can enumerate the other's naming.
- **Where does an administrator look before publishing?** `/v1/models` answers
  "what can this key invoke right now", and deliberately answers nothing else: it
  is an inference route, it reads only the immutable snapshot, and it must not
  grow reasons. But the questions that precede an enablement — what has this
  tenant enabled, which of it is disabled, which has no approved price, which has
  no alias pointing at it — are about *desired state*, and desired state is
  reachable only from `/admin/v1`. Every read there was deployment-wide, so a
  tenant administrator had no read at all: the only grant that answered was the
  one that answers for every tenant.

The tempting shortcut for the second question is a control-plane read behind an
inference route, which is exactly what makes a control-plane outage an inference
outage.

## Decision

**An alias name is scoped to a namespace.** A runtime alias carries an optional
owning namespace. An owned alias is invisible and unroutable outside it; an
unowned alias is deployment-wide and is what every prior release wrote, so
existing configuration keeps its meaning. Uniqueness is per owner, so two
namespaces may each hold `fast`, and a namespace's own row shadows the
deployment-wide one for that namespace alone — the same precedence a project
override has over a tenant default in 0042. Resolution and enumeration go through
one lookup that takes the caller's namespace, so listing and invoking cannot
disagree, and a name a caller does not own is `unknown_model` rather than a
forbidden one it can prove exists.

**The management catalogue is an administrative read, scoped to a tenant.**
`GET /admin/v1/catalogue` projects one tenant's or one project's enablements from
the desired revision: identities, the pinned catalogue coordinates, lifecycle, the
aliases that name them, and a closed vocabulary of reasons a model is not routable
(`disabled`, `shadowed`, `unpriced`, `unaliased`). The scope is a request
parameter, the grant must cover it, and there is no deployment-wide spelling — a
tenant administrator cannot enumerate another tenant's enablements or learn that
one exists. It reuses the `read_state` verb rather than adding one: a new resource
kind must not widen the action vocabulary, and what differs is the scope, not the
verb.

**Boundaries.** No inference route reads the control plane, and no administrative
read enables anything: the catalogue reports what a revision already says.
Enable, disable, alias, and retarget stay the existing `POST` documents, which
publish one complete candidate and converge through one revision. Offering
metadata (provider, capabilities, modalities, context limits) and observed
availability are *not* projected by this record's implementation: they belong to
catalogue import and the availability index, and the read names them as pending
rather than inferring them, so a caller can tell "not healthy" from "this build
cannot say".

### State tier

Tier 0 for alias ownership: it is a config key, so a single-file deployment gets
per-namespace alias names with no Redis and no Postgres, and a file that names no
namespace behaves exactly as before. Tier 2 for the management catalogue, because
it reads the desired revision the control plane owns — in Tier 0 and Tier 1 the
route is the same `stateful_mode_required` refusal the rest of `/admin/v1`
answers. No existing deployment's tier rises.

## Consequences

A tenant can hold its own alias names, and an operator can hand a tenant
administrator a read that is bounded by that tenant. Alias-scope patterns and
credential presence keep their meaning: ownership narrows what a caller may see,
never widens it, so a key still cannot reach a provider it holds no credential
for.

The unowned row is a compatibility surface that has to keep working: a
single-tenant file names no namespace, and every filter treats such a row as
reachable from everywhere. Removing that default later would break exactly the
configurations that never asked for multi-tenancy, so it stays.

Two operational costs. A reload summary now names an owned alias
`namespace/name`, because merging two tenants' `fast` would report one tenant's
withdrawal as no change — a log-format change for anyone parsing that line. And
the management catalogue is a control-plane read, so it stalls during a
control-plane outage while inference does not; an operator diagnosing "why can my
tenant not call this model" during such an outage has `/v1/models` and the
replica's own status, and must not read the catalogue's silence as a denial.
