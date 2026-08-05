# Wire fixtures

Recorded provider responses, replayed through a fake upstream so the gateway's
wire fidelity is asserted offline and deterministically (ADR 0014). Nothing
here reaches the network: the harnesses in `crates/gateway/tests/` and
`tests/compat/` boot a real `axond` against a local server that serves these
bytes.

## Layout

```
tests/fixtures/<provider>/<case>.json   buffered response body
tests/fixtures/<provider>/<case>.sse    streamed response, verbatim SSE bytes
```

A `.sse` file is served byte-for-byte, split at arbitrary chunk boundaries. For
the native Anthropic route the relay is byte-faithful, so the file doubles as
the expected client-visible output — reformatting one is a behaviour change,
not a cosmetic edit.

## Adding a fixture

1. Capture the response against the real provider, e.g.

   ```bash
   curl -sN https://api.anthropic.com/v1/messages \
     -H "x-api-key: $ANTHROPIC_API_KEY" \
     -H 'anthropic-version: 2023-06-01' \
     -H 'content-type: application/json' \
     -d '{"model":"claude-3-7-sonnet-latest","max_tokens":1024,"stream":true, ...}' \
     > tests/fixtures/anthropic/<case>.sse
   ```

2. **Redact.** No API keys, account ids, organisation ids, request ids that
   identify a real account, or user content. Thinking-block `signature` values
   are opaque provider blobs — replace them with a `REDACTED_...` placeholder;
   the tests assert the placeholder survives the relay, which is the same
   property as a real signature surviving.
3. Keep it small but representative: a streamed fixture should carry the frames
   the accounting depends on (`message_start` / `message_delta` usage for
   Anthropic, the final `usage` chunk for OpenAI) plus whatever shape the case
   is about.
4. Wire it into the fake upstream (`crates/gateway/tests/support/upstream.rs`,
   `tests/compat/fake_upstream.py`) by adding a target model that serves it, and
   assert against it from a test.

## Current fixtures

| File | What it pins |
| --- | --- |
| `openai/chat_completion.json` | Buffered `/chat/completions`, usage block with token details |
| `openai/chat_completion.sse` | Streamed chat: role chunk, content deltas, `finish_reason`, usage-only chunk, `[DONE]` |
| `openai/embeddings.json` | Buffered `/embeddings`, prompt-only usage |
| `anthropic/message_thinking_tool_use.json` | Buffered `/messages` with a signed thinking block and a `tool_use` block |
| `anthropic/message_thinking_tool_use.sse` | Streamed `/messages`: `message_start` usage, thinking + `signature_delta`, `input_json_delta` tool call, `message_delta` output usage, `message_stop` |
