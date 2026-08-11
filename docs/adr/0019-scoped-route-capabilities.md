# 19. Scoped route capabilities

Date: 2026-08-10

## Status

Accepted

## Context

ADR 0016 defines an optional `scope` claim for minted inbound identity, but
leaves the route capability vocabulary and its relationship to configured
provider authority unstated. A scope must narrow a caller without becoming a
second configuration authority, and static breakglass keys must retain their
existing behavior.

## Decision

The route capability set is fixed and maps directly to the provider routes:

| Capability | Route |
| --- | --- |
| `chat` | `POST /v1/chat/completions` |
| `messages` | `POST /v1/messages` |
| `embeddings` | `POST /v1/embeddings` |
| `responses` | `POST /v1/responses` |
| `models` | `GET /v1/models` |
| `credentials` | `GET /v1/credentials` |

`credentials:all` is an additional explicit operator scope for the
all-namespaces credential status view; it never grants the route by itself.
Scope-less principals retain the route's own-namespace view, while this
explicit capability is required for `?namespaces=all`.
The endpoint's replica-local state and operator-scope boundary are specified
in [ADR 0021](./0021-credential-status-endpoint.md).

Namespace authority is derived from the existing route and credential graph.
`models` is always available because the catalogue can be empty. Each provider
capability is available when at least one configured model alias has a target
whose provider kind speaks that route's wire and whose credentials are present
for the caller's namespace, including the existing platform-fallback rules.
This adds no configuration surface and mirrors catalogue scoping.

Only a principal carrying a `scope` is subject to this gate. Its effective
capabilities are the intersection of the token scope and the derived namespace
authority. A missing scope therefore preserves Phase 1 behavior, including for
static gateway keys, while an empty scope narrows authority to nothing. A
`null` claim is an absent claim, not an empty scope. Unknown scope values are
ignored and intersect away rather than failing token verification.
Scoped callers need the `responses` capability for `/v1/responses`.

The auth middleware enforces the capability before handler extractors run.
Scope denial is a typed `403` with code `token_scope_insufficient`; its message
names the missing static capability, for example
`token scope does not authorize \`messages\``. Logs may name the namespace,
subject, signer key id, and capability, but never token bytes or key material.

`axond mint --scope` emits repeatable capability claims and validates each
name against the fixed vocabulary before signing.

### State tier

This is Tier 0 / config-only: enforcement is derived from the config snapshot
and the token itself, with no Redis, Postgres, or request-path store. It does
not raise the state tier of an existing deployment.

## Consequences

- A minted caller can be narrowed per route without a config edit.
- A token cannot grant a route that its namespace cannot reach.
- Static gateway keys and scope-less tokens remain fully backward compatible.
- Adding a provider route requires declaring its capability in the route table.
