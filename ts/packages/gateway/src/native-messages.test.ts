import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import { isRateLimitPayload } from "./dispatch.ts";
import { createNativeMessagesSequence } from "./native-messages.ts";

function sequence() {
  return createNativeMessagesSequence(isRateLimitPayload);
}

function frame(type: string, body: unknown, event = type): string {
  return `event: ${event}\ndata: ${JSON.stringify(body)}\n\n`;
}

test("native messages sequence accepts a complete message and the thinking fixture", () => {
  const live = sequence();
  assert.equal(
    live.push(
      [
        frame("message_start", { type: "message_start", message: { usage: { input_tokens: 3 } } }),
        frame("message_delta", {
          type: "message_delta",
          delta: { stop_reason: "end_turn" },
          usage: { output_tokens: 1 },
        }),
        frame("message_stop", { type: "message_stop" }),
        "event: provider.extension\ndata: ",
      ].join(""),
    ),
    null,
  );
  assert.equal(live.finish(), null);

  const fixture = readFileSync(
    new URL("../../../../tests/fixtures/anthropic/message_thinking_tool_use.sse", import.meta.url),
    "utf8",
  );
  const replay = sequence();
  assert.equal(replay.push(fixture), null);
  assert.equal(replay.finish(), null);
});

test("native messages sequence names the broken event", () => {
  const premature = sequence();
  assert.equal(
    premature.push(frame("message_stop", { type: "message_stop" })),
    "provider stream was invalid: native Messages message_stop arrived before a complete message sequence",
  );

  const unsigned = sequence();
  const error = unsigned.push(
    [
      frame("message_start", { type: "message_start", message: {} }),
      frame("content_block_start", {
        type: "content_block_start",
        index: 0,
        content_block: { type: "thinking", thinking: "", signature: "" },
      }),
      frame("content_block_delta", {
        type: "content_block_delta",
        index: 0,
        delta: { type: "thinking_delta", thinking: "reason" },
      }),
      frame("content_block_stop", { type: "content_block_stop", index: 0 }),
    ].join(""),
  );
  assert.equal(
    error,
    "provider stream was invalid: native Messages content block stopped out of sequence",
  );

  const extension = sequence();
  assert.equal(
    extension.push(frame("provider.future", { type: "provider.future", opaque: true })),
    null,
  );
  assert.equal(extension.finish(), null);

  const open = sequence();
  assert.equal(open.push("event: message_start\ndata: {"), null);
  assert.equal(open.finish(), "stream ended with an incomplete SSE event");

  const providerError = sequence();
  assert.equal(
    providerError.push(frame("error", { type: "error", error: { type: "api_error", message: "overloaded" } })),
    "provider stream was invalid: overloaded",
  );
  const unnamed = sequence();
  assert.equal(
    unnamed.push(frame("error", { type: "error", error: { type: "api_error" } })),
    "provider stream was invalid: Anthropic stream error",
  );
  const limited = sequence();
  assert.equal(
    limited.push(frame("error", { type: "error", error: { type: "rate_limit_error", message: "slow down" } })),
    "provider stream was rate limited: slow down",
  );
  const huge = sequence();
  const bounded = huge.push(frame("error", {
    type: "error",
    error: { type: "api_error", message: "€".repeat(4096) },
  }));
  const prefix = "provider stream was invalid: ";
  const marker = "… [truncated]";
  assert.equal(typeof bounded, "string");
  assert.equal(bounded!.startsWith(prefix), true);
  assert.equal(bounded!.endsWith(marker), true);
  const kept = bounded!.slice(prefix.length, -marker.length);
  assert.equal(kept.length > 0 && [...kept].every((character) => character === "€"), true);
  assert.ok(
    new TextEncoder().encode(bounded!).length
      <= new TextEncoder().encode(prefix).length + 4096 + new TextEncoder().encode(marker).length,
  );
});
