# 12. Native provider routes: passthrough as the wire contract

Date: 2026-08-04

## Status

Accepted

## Context

The gateway serves `/v1/chat/completions` and, until now, answered `/v1/messages`,
`/v1/embeddings`, and `/v1/responses` with a typed `501`. That left the Anthropic
half of the market reachable only by sending an OpenAI-shaped request to an
Anthropic target and letting the adapter translate it — the arrangement the
passthrough-first turn (delta A1) was a reaction to.

Translation is not merely lossy in the abstract; the losses are the parts of the
wire that matter most to an agent:

- **Signed thinking blocks.** Anthropic returns extended-thinking content with a
  `signature` and expects it echoed back verbatim on the next turn. Round-tripping
  it through an OpenAI `reasoning_details` shape means re-encoding a value whose
  whole purpose is to be byte-identical.
- **Tool-use blocks.** `tool_use` / `tool_result` blocks, their ids, and their
  input JSON have to survive intact for a multi-turn tool loop to continue.
- **Everything new.** Any field Anthropic ships tomorrow is unknown to a
  translator and is therefore dropped, while a passthrough forwards it.

A caller that already speaks Anthropic's wire — the Anthropic SDK, Claude Code,
any agent framework built on them — needs none of that translation. It needs the
gateway's *other* value: aliasing, credential pools, failover, budgets, usage.

## Decision

**Native routes are passthrough: forward the body, rewrite only `model`.** A
request to `/v1/messages` is sent to the target's own `/messages` with only the
alias replaced by the resolved target model, and the provider's response body is
returned to the caller unchanged. Streaming is the same promise applied to bytes:
the upstream SSE bytes are relayed as they arrive, event names and payloads
untouched, and the stream ends on the provider's own `message_stop` rather than an
OpenAI `[DONE]` sentinel an Anthropic SDK would not expect. The gateway does not
have an opinion about the shape of a body it is not translating, so signed
thinking and tool-use blocks are preserved by construction rather than by a
translator that has to be kept in sync.

The promise is exact *values*, and on a stream exact bytes. A buffered body is
parsed and re-serialized on its way through, so object key order may differ from
what the provider emitted; every value, including a thinking `signature` and a
`tool_use` `input`, is carried through untouched, which is what the wire
semantics actually depend on. Passthrough also stops at the success body:
non-2xx upstreams are still mapped to the gateway's own typed error shape,
because that mapping is what classifies a failure as retryable and drives
failover (ADR 0008), and provider response headers are not forwarded. Forwarding
native error shapes and rate-limit headers is a follow-up that has to answer how
a passed-through error still participates in failover.

**One request path, parameterized by wire — not a second dispatch path.** All
routes share the `serve` body in `routes.rs`: one config snapshot per request
(ADR 0011), alias resolution, one budget hold (ADR 0010), the ordered target walk
with per-target circuits (ADR 0008) wrapped around credential-pool dispatch
(ADR 0006), then settlement and exactly one `UsageRecord`. A `Route` value
supplies the only genuine differences — the upstream path, which provider kinds
can serve the shape, the wire headers to carry, how usage is read back, and the
relay's framing. Native dispatch is a second *transport* call
(`HttpDispatcher::send` / `send_stream`, which POST an already-shaped body and
return the answer undecoded) inside the same walk, not a second walk. Adding
`/v1/messages` therefore cannot drift from `/v1/chat/completions` on failover,
budgets, or attribution: there is nothing to drift from.

**Native usage is mapped in `gateway-core`, into the same canonical record.**
Anthropic reports `usage.input_tokens` / `output_tokens` plus
`cache_creation_input_tokens` / `cache_read_input_tokens`, and in a stream splits
them across `message_start` (input and cache counters) and `message_delta`
(output). `native_message_usage` reads the buffered form and a
`NativeMessagesDecoder` folds the streamed halves together, both in the I/O-free
crate where the rest of the wire knowledge lives. The decoder forwards each event
untouched and exists *only* to observe usage — the relay does not need it to
produce output. Downstream, nothing knows a native route was involved: budgets
settle and sinks receive the same `UsageRecord` every other route produces.

**Embeddings bill input only.** An embeddings response has no completion, so
whatever a provider reports as output tokens is ignored rather than priced, and
the pre-dispatch estimate reserves nothing for output. The forwarded body is also
left exactly as sent — notably the gateway never adds a `stream` field, which the
endpoint does not accept. A caller that sends one anyway keeps it: passthrough
means the provider gets to reject the caller's own mistake, not that the gateway
starts editing bodies it does not translate.

**A wire the alias cannot speak is a typed 4xx, checked before dispatch.** A
native route has no translation to fall back on, so an alias whose target is an
OpenAI provider is rejected on `/v1/messages` with `400 unsupported_wire` naming
the alias and the provider. The check covers *every* target of the alias, not just
the first: an alias that would fail over into a target that cannot serve the shape
is a configuration mistake, and surfacing it as a 400 up front beats an upstream
`404` mid-failover. This is the fail-early posture the boot gate takes, applied to
the one thing boot cannot know — which route the caller will use.

**`/v1/responses` is deferred past beta.** It is the one route where passthrough
is not the whole job: the Responses API is *stateful* (server-side conversation
storage via `store`/`previous_response_id`), and an honest implementation has to
decide what a stateless gateway does with a response id it did not mint and cannot
resolve if the alias fails over to a different provider — a routing-affinity
question, not a wire question, and squarely against ADR 0002's stateless default.
It is also served today: the OpenAI SDK's chat surface goes through
`/v1/chat/completions`, so nothing is unreachable. The route keeps its typed `501`
with a message naming both the deferral and the alternative, because a missing
route is indistinguishable from a misconfigured `base_url`.

**No new dependencies**, so `deny.toml` is untouched and no licence needed
allowing.

## Consequences

- An Anthropic SDK can be pointed at the gateway with `base_url` alone: its
  `x-api-key` is accepted as the inbound gateway key (the same key table as
  `Authorization: Bearer`, just the scheme the client happens to send), and its
  `anthropic-version` / `anthropic-beta` headers are forwarded, with the caller's
  pinned version winning over the gateway's default.
- A native alias is provider-bound by nature: `/v1/messages` can only fail over
  between Anthropic targets. Cross-wire failover remains available through
  `/v1/chat/completions`, where translation is the point.
- The relay now has two framings. The native one relays upstream bytes verbatim,
  which means a mid-stream failure is reported as an Anthropic-shaped `error`
  event; the upstream has usually sent its own already, and an SDK stops at the
  first one.
- Usage on a native route is only as good as what the provider reports. A stream
  that dies before `message_delta` settles the partial charge from the prompt
  estimate (ADR 0010), exactly as the OpenAI-shaped path does.
- `/v1/responses` stays a `501` for beta. When it lands it will need its own ADR
  for the statefulness question, not just a route.
