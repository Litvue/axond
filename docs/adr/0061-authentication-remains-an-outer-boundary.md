# 61. Authentication remains an outer request boundary

Date: 2026-08-18

## Status

Accepted

Closes the authentication decision deliberately left open by
[ADR 0060](./0060-request-path-middleware.md). The fail-closed identity contract
remains the one established by
[ADR 0013](./0013-inbound-auth-fails-closed.md), and the Tier 0 serving gate
continues to exercise it under
[ADR 0018](./0018-tier-0-hermetic-boot-gate.md).

## Context

ADR 0060 introduced a middleware primitive for parsed provider content and left
authentication as the last possible inward migration, or as an intentional
exception. The rate-limit and budget migration now supplies the evidence that
decision needed: `MiddlewareExecution` is valuable when state acquired during a
request must follow buffered completion or a streamed response body's drop
boundary. A rate-limit permit, a budget hold, and content-middleware state all
have that lifetime.

Authentication does not. It consumes HTTP headers and the route's compiled
capability before a provider body is parsed, and it produces the `InboundKey`
request extension used by convergence, alias visibility, pricing ceilings,
budgets, provider credentials, and usage attribution. Its ordering is itself a
security contract:

- admission remains outermost, so shutdown and drain refusal can stop work
  before authentication;
- authentication is the first identity-sensitive refusal;
- convergence follows authentication, so an anonymous caller sees `401` and
  never learns that a stateful replica is unconverged through a `503`;
- only `/healthz` and `/readyz` are unauthenticated, as declared by the closed
  route table and checked by its sweep test.

The ADR 0060 primitive starts later. It receives a parsed `ProviderRequest`, not
headers, route capability, request extensions, a principal directory, or a
revocation backend. Moving authentication onto that interface would either run
it after body buffering and route work, which is too late, or expand a
synchronous, I/O-free content extension into a second HTTP routing framework.
Neither result buys response-lifetime ownership, because authentication has no
hold to carry to the response.

Authentication is also intentionally not operator-orderable. A policy document
may select content middleware; it may never insert, remove, fail open, or
reorder the stage that establishes which tenant and authority the rest of the
request is allowed to use. Uniform registration would therefore create no
supported operator capability.

## Decision

**Authentication does not migrate onto the ADR 0060 middleware primitive.** It
remains a compiled outer Axum layer selected by the closed route table. The
intentional request topology stays:

```text
admission -> authentication -> convergence -> parsed request path
```

Diagnostic routes retain their separately bounded variant of the same rule;
liveness probes retain their explicitly unauthenticated posture. The
`[core_middleware]` migration gate governs accounting ownership only and cannot
select, disable, or roll back authentication.

This is a security boundary, not a temporary implementation exception. The
content middleware trait stays limited to provider request, response, and stream
event data. It does not gain headers, route metadata, request extensions,
principal stores, revocation stores, or a way to declare an authentication
failure posture.

### Reconsideration gate

A future design may revisit the outer layer only if it proposes a distinct
pre-body HTTP primitive and proves all of the following directly:

1. Admission remains outermost, while authentication still precedes
   convergence, body buffering and parsing, route handling, configurable
   policy, permits, budgets, and dispatch.
2. The route table remains a closed declaration of authentication posture and
   capability, with no configuration capable of omitting or reordering the
   stage. Only the two liveness probes remain unauthenticated.
3. Static keys, minted tokens, stateful projected principals, token revocation,
   expiry, scope, and backend-unavailable behavior produce the same
   `InboundKey` and the same typed fail-closed refusals as the current layer.
4. Anonymous requests against unconverged stateful routes receive `401` rather
   than convergence `503`; authenticated requests receive the convergence
   refusal; draining requests are still refused before authentication.
5. Snapshot publication and reload cannot expose a request to a partially built
   principal table, and in-flight requests remain bound to one immutable
   authentication generation.
6. Tier 0 remains hermetic, every authenticated route passes the route-table
   sweep, and black-box tests cover both accepted header schemes, malformed and
   missing credentials, authority outages, diagnostics, reload, and shutdown.

Meeting those conditions would justify a new ADR. It would not silently turn
the parsed-content primitive into the outer security boundary.

## Consequences

- The request path intentionally has two internal middleware mechanisms: fixed
  outer HTTP security/lifecycle layers and the parsed-content primitive.
- Authentication keeps its earliest useful phase, its request-extension output,
  and its existing fail-closed ordering without adapter state or duplicated
  route metadata.
- The accounting migration's rollback gate stays narrow; operating it cannot
  change who is allowed to call the gateway.
- There is no remaining authentication migration task under ADR 0060. Any
  future work begins with the reconsideration evidence above and a new decision.
