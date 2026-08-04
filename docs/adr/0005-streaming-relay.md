# 5. Streaming relay

Date: 2026-08-04

## Status

Accepted

## Context

Streaming is the dominant LLM-gateway workload, and until now a request with
`"stream": true` short-circuited to a typed `501`. A relay has to answer four
questions that the buffered path never faced: what exactly is forwarded to the
client, who owns cancellation, where the usage numbers come from when the
response is a sequence of chunks, and what a caller is charged when the
connection dies halfway through.

Two constraints shape the answers. `gateway-core` already decodes SSE
(`SseDecoder`) and already turns provider chunks into normalized events with
terminal usage (`ProviderStreamDecoder`, implemented for both the
OpenAI-compatible and Anthropic wire shapes), so the gateway crate should feed
it bytes rather than grow a second parser. And ADR 0002 puts exactly one
canonical `UsageRecord` at the end of every request — including the ones that
end badly.

## Decision

**Decode and re-emit, rather than forward bytes verbatim.** Upstream bytes go
through `SseDecoder` into the target adapter's `ProviderStreamDecoder`, and the
resulting events are re-framed as `data:` (plus `event:` when the decoder names
one) followed by a single terminal `data: [DONE]`. Verbatim byte passthrough
would be cheaper but would leak the target's wire shape: an Anthropic target
would reach an OpenAI-compatible client as `message_delta` frames. Re-emitting
gives the caller the same shape the buffered path already produces, keeps the
`[DONE]` sentinel well-defined even when a provider drops the connection
without one, and costs one decode of a stream we must parse anyway to get
usage.

**No response-level buffering; backpressure is the socket's.** The response
body is a stream that decodes one upstream chunk at a time and yields the
events it produced. Axum polls it only as the client socket drains, so a slow
reader propagates back to the upstream connection. The only bounded buffer is
`SseDecoder`'s partial-event buffer, which fails the stream rather than growing
without limit.

**Cancellation is ownership, not a watchdog.** The upstream byte stream is
owned by the response body. A client hang-up drops the body, which drops the
`reqwest` response and aborts the in-flight upstream request — bounded by the
drop itself rather than by a timer, and with no task left running to leak.

**Usage comes out of the stream's terminal event.** Providers report usage in
the last frames (OpenAI under `stream_options.include_usage`, Anthropic in
`message_delta.usage`); the core decoders already accumulate this and surface it
as `ProviderStreamEvent::Done(usage)`. That value is what the record and the
cost are computed from, so a streamed request still emits exactly one
`UsageRecord` with real token figures.

**Settlement is attached to the body, and runs exactly once.** Completion,
mid-stream failure, and client cancellation all converge on one accounting
struct that commits the cost and writes the record; the cancellation arm is its
`Drop`. Because settlement outlives the request, it is spawned detached.

**A mid-stream upstream failure is an event, not a dropped connection.** Once
headers are sent the status code is spent, so an upstream error or an
undecodable frame is relayed as `event: error` with a typed payload, followed by
`[DONE]`, and recorded as `upstream_error`. A failure *before* any byte is
relayed still becomes an ordinary typed error response.

**Partial spend is charged, best-effort, through the existing
`BudgetStore::commit`.** A cancelled stream commits the usage accrued at the
moment of the drop. Where a provider only reports usage in its final frame,
that accrual is zero — the gateway does not invent token counts it was never
told. No new trait method was needed; a real reserve/release lifecycle (holding
the estimate, releasing the unused remainder, and doing so against a shared
durable backend) belongs to the durable-budget work and is deliberately left
there.

## Consequences

- Streaming works end-to-end on `/v1/chat/completions`, in the OpenAI chat
  chunk shape, for both OpenAI-compatible and Anthropic targets.
- The buffered path is untouched: the route only branches on `stream`.
- Re-emission normalizes framing, so provider-specific comment lines and
  heartbeats from upstream are not forwarded. Clients that depend on a
  provider's exact byte stream (rather than its events) are not served by this
  relay.
- Under-charging is possible on cancelled streams against providers that report
  usage only at the end. This is the conservative direction, and it narrows once
  durable budgets land.
- Failover and native routes are unaffected: the relay is a self-contained
  module the route calls after target resolution, so wiring a second attempt or
  a second surface into it later is additive.
