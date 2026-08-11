# 22. OpenAI Responses native passthrough

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

Statefulness remains provider-side and the gateway remains stateless. A request
with a non-empty, non-null `previous_response_id` considers only the alias's
first configured target and its first configured credential; it does not fail
over or rotate credentials. Skipping that target or a credential failure yields
its typed `continuation_affinity_unavailable` error rather than sending the
continuation to another provider or key. The gateway stores no
response-id-to-target map, so a continuation whose previous turn was served by
a failover target may still 404 upstream; fixing that requires affinity state,
which the Tier 0 posture forbids.
Likewise, a pinned continuation does not rotate off a rate-limited or failing
credential, so it surfaces that error rather than silently using a key that
cannot see the stored response. Credential health is intentionally bypassed for
the pinned first credential, including parked or cooling-down state, because
there is no valid alternate key for the stored response. `GET`/`DELETE
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
unknown fields and signed reasoning items. Continuation requests remain
correct for provider-local response ids when they stay on the same provider and
credential, but a provider outage or credential failure cannot be hidden by
trying another target or key. A continuation after ordinary target failover may
still 404 because the gateway has no id-to-target affinity map, and adding that
state would violate the Tier 0 posture. Retrieval and cancellation must be
performed against the provider directly because those routes are intentionally
not served.
