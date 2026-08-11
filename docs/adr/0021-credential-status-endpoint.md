# 21. Credential status endpoint

Date: 2026-08-10

## Status

Accepted

## Context

Operators need to distinguish configured credentials from parked credentials
without exposing provider secrets. Tenants also need to see the credentials
their own requests can draw from, including deliberate platform fallback.

## Decision

`GET /v1/credentials` returns write-only credential attribution labels and
their circuit state. The default tenant view is the caller's own namespace,
plus platform pools used by explicitly configured platform fallback. An entry
identifies its owning namespace and whether its source is `byok` or
`platform`. `?namespaces=all` is an operator view and is permitted only when
the token explicitly contains `credentials:all`. Scope-less static keys and
scope-less tokens retain their own-namespace view; granting all namespaces to
them would let any tenant enumerate every namespace. A scoped token
additionally needs `credentials` for the route at all.
`credentials:all` is an operator-only capability: because it has no namespace
restriction, the all-namespaces view additionally requires that the caller's
namespace is the configured default/platform namespace. Operators must mint it
only into operator tokens, never tenant tokens; the verifier's `namespaces`
allowlist is defense in depth rather than the boundary.
Unknown query values are typed `400 bad_request`.

The response declares `observed: "replica"` and reports `healthy`, `parked`, or
`probe`. Presence is expressed by an entry existing: credentials resolve at
boot or boot fails. Credential ids are non-secret attribution labels already
used in tenant usage records; secret values remain write-only. For a tenant
using platform fallback, the platform credential's presence and circuit state
are visible, but its default env-derived label is omitted. An explicitly
configured `id` remains visible, so `credential_id` is optional in the
response.

### State tier

This is Tier 0: in-memory, per replica, with a replica-local view. It does not
raise the state tier of an existing deployment. Fleet-wide credential health
remains deferred per ADR 0017.

## Consequences

Tenants gain a consistent catalogue-style read surface without learning
secrets or other tenants' credentials. Operators must use explicitly scoped
tokens for the all-namespaces view. Replicas may disagree about transient
circuit state, which is intentional and visible in the response.
