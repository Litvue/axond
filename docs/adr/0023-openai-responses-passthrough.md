# 23. OpenAI Responses native passthrough

Date: 2026-08-11

## Status

Accepted

## Context

The OpenAI Responses API carries signed reasoning items, opaque future fields,
and provider-side state through `store` and `previous_response_id`. Translating
it into Chat Completions would lose wire fidelity and make continuation
semantics ambiguous across failover targets.

## Decision

Serve `POST /v1/responses` on the shared route path as a native passthrough.
Only the resolved `model` is rewritten; buffered responses preserve values and
streaming responses relay upstream SSE bytes without re-emission or a
synthesized `[DONE]`. Gateway-generated terminal stream errors use the
Responses shape with `type`, `code`, and `message`.

Statefulness remains provider-side and the gateway remains stateless. *Every*
`/v1/responses` request — initial calls included — considers only the alias's
first configured target and its first configured credential; it does not fail
over or rotate credentials. Pinning the initial call is what makes affinity
recoverable without state: a response created on a failover target or a rotated
key has an id no continuation could reach, so the pin must hold on the request
that creates the id as well as on the one that uses it.

The pin is shared, but the error semantics are not. Only a request carrying a
non-empty, non-null `previous_response_id` has continuity to lose, so only it
yields the typed `continuation_affinity_unavailable` error when the pinned
target or credential is unusable. A pinned *initial* request in the same
situation yields the ordinary typed routing or credential error
(`all_provider_circuits_open`, `no_credential`, or the upstream error itself),
because nothing was continued.

The gateway stores no response-id-to-target map. With pinning applied
uniformly, an id created through the gateway is always recoverable under an
unchanged configuration; reordering an alias's targets or its credential pool
strands ids created under the previous order, and fixing that would require
affinity state the Tier 0 posture forbids.
A pinned request does not rotate off a rate-limited or failing credential, so it
surfaces that error rather than silently using a key that cannot see — or could
not later serve — the stored response. Credential health is intentionally
bypassed for the pinned first credential, including parked or cooling-down
state, because there is no valid alternate key. `GET`/`DELETE
/v1/responses/{id}` and cancellation routes are deliberately unserved.

Usage is read from the Responses `usage` block and priced through the model
catalogue. A cancelled or truncated stream is charged from measured prompt and
relayed output text when authoritative usage has not arrived.

### State tier

This is Tier 0 / config-only. The gateway stores no response state and adds no
Redis or Postgres requirement. Existing lower-tier deployments retain the same
implementation and this decision does not raise the state tier.

## Consequences

OpenAI Responses SDKs can use the gateway without translation loss, including
unknown fields and signed reasoning items. Any response created through the
gateway can be continued under an unchanged configuration, because the initial
call and the continuation resolve to the same target and credential.

The cost is availability: the Responses route has no failover and no credential
rotation at all, so an outage or an exhausted key on the alias's first target is
returned to the caller rather than routed around — including for initial calls,
which previously did fail over. Aliases whose first target is a low-availability
endpoint should not be used for Responses. Chat, Messages, and Embeddings are
unaffected: they keep full failover and rotation over the same aliases.

Retrieval and cancellation must be performed against the provider directly
because those routes are intentionally not served. An optional affinity store
mapping response ids to the serving target and credential remains the way to
restore failover later; it would be separately selectable and would not raise
the required tier of a stateless deployment, whose default `mode = "stateless"`
boot stays free of any backend
([ADR 0027](./0027-stateless-and-stateful-operating-modes.md)).
