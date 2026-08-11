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
`platform`. `?namespaces=all` is an operator view, reachable only by a caller
that holds the operator's own authority over the deployment: a scope-less
static `[[gateway_key]]` in the configured default namespace. Every other
caller gets `403 token_scope_insufficient` naming `credentials:all`, and a
scoped token additionally needs `credentials` for the route at all.

Authority here is direct, not delegated. Static keys are placed in the config
by an operator, so one in the default namespace already speaks for every
namespace; a static key in a tenant namespace does not, and remains limited to
its own view. Minted tokens only ever carry authority an operator delegated to
a subject, so no token reaches this view — including one presenting a
`credentials:all` claim, which a signer can emit even though `POST /v1/tokens`
refuses to mint it and an omitted minting scope cannot inherit it (#116).
Whether a principal is static or minted is therefore recorded as provenance
where it is resolved rather than inferred from its claims, and the direct-use
check is separate from the minting predicate: "may this caller delegate this
capability?" and "is this caller the operator?" are different questions, and
sharing one predicate between them would make either answer wrong.

Scope-less tokens retain their own-namespace view; granting all namespaces to
them would let any tenant enumerate every namespace.
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
secrets or other tenants' credentials. Operators use a default-namespace static
key — the breakglass key they already keep — for the all-namespaces view, and
cannot delegate that view to any token. The `credentials:all` capability
remains parseable and names the denial, but no principal is granted the view by
carrying it. The default namespace is consequently operator territory: a
deployment that hands its default-namespace static key to an application widens
that application's visibility to every namespace's labels and circuit state, so
applications belong in their own namespace or on minted tokens.
Replicas may disagree about transient circuit state, which is
intentional and visible in the response.
