# Alias wire-family validation

Date: 2026-08-11

## Status

Accepted

## Context

ADR 0012 makes provider routes passthrough-first: the gateway forwards a
request on its existing wire instead of translating it to another provider
wire. Failover therefore has to preserve the route's wire for every target,
not only for the first target selected.

The route is chosen per request, while an alias is shared across routes. A
configuration check must consequently avoid choosing a route prematurely while
still rejecting an alias that no route could ever serve.

## Decision

Provider kinds map to two wire families:

| Provider kind | Wire family | Routes |
| --- | --- | --- |
| `openai`, `openai-compatible` | OpenAI | `/v1/chat/completions`, `/v1/embeddings` |
| `anthropic` | Anthropic | `/v1/messages` |

Every alias's failover targets must belong to one wire family. OpenAI and
OpenAI-compatible targets may be mixed, as may multiple Anthropic targets, but
an alias mixing the two families is rejected as invalid configuration. The
all-targets rule used by request-time route checks means a cross-family alias
cannot be served by any route: each route would reject at least one target.
Boot and reload can therefore enforce this invariant without deciding which
route a future request will use.

The request-time `400 unsupported_wire` check remains. It handles the distinct
case of a valid single-family alias sent to a route for the other family, such
as an OpenAI-family alias on `/v1/messages`.

### State tier

This is Tier 0 / config-only. Validation is derived from the immutable
configuration snapshot and uses no Redis or Postgres state. It does not raise
the state tier of any existing deployment.

## Consequences

- Cross-family failover mistakes fail before boot or config publication.
- OpenAI and OpenAI-compatible failover remains legal.
- Route choice stays a request concern for valid aliases.
- Operators still receive a typed request-time error when a valid alias is sent
  to an incompatible route.
