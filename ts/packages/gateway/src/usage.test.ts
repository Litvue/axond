import assert from "node:assert/strict";
import test from "node:test";

import { emptyUsage, noteSseChunk, usageFromJson } from "./usage.ts";

test("openai cached prompt tokens are billed once", () => {
  const usage = usageFromJson("chat", {
    usage: {
      prompt_tokens: 19,
      completion_tokens: 7,
      prompt_tokens_details: { cached_tokens: 3 },
      completion_tokens_details: { reasoning_tokens: 2 },
    },
  });
  assert.equal(usage.inputTokens, 16n);
  assert.equal(usage.outputTokens, 7n);
  assert.equal(usage.reasoningTokens, 2n);
  assert.equal(usage.cacheReadTokens, 3n);
});

test("responses usage reads nested and detail blocks", () => {
  const buffered = usageFromJson("responses", {
    usage: {
      input_tokens: 19,
      output_tokens: 7,
      input_tokens_details: { cached_tokens: 3 },
      output_tokens_details: { reasoning_tokens: 2 },
    },
  });
  assert.equal(buffered.inputTokens, 16n);
  assert.equal(buffered.cacheReadTokens, 3n);
  assert.equal(buffered.reasoningTokens, 2n);

  const streamed = emptyUsage();
  noteSseChunk(
    "responses",
    streamed,
    'event: response.completed\ndata: {"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":19,"output_tokens":7,"input_tokens_details":{"cached_tokens":3},"output_tokens_details":{"reasoning_tokens":2}}}}\n\n',
  );
  assert.equal(streamed.inputTokens, 16n);
  assert.equal(streamed.outputTokens, 7n);
  assert.equal(streamed.cacheReadTokens, 3n);
  assert.equal(streamed.reasoningTokens, 2n);
});

test("embeddings ignore reported output tokens", () => {
  const usage = usageFromJson("embeddings", {
    usage: { prompt_tokens: 8, completion_tokens: 3, total_tokens: 11 },
  });
  assert.equal(usage.inputTokens, 8n);
  assert.equal(usage.outputTokens, 0n);
});

test("anthropic cache tokens stay disjoint from input", () => {
  const usage = usageFromJson("messages", {
    usage: { input_tokens: 19, output_tokens: 7, cache_read_input_tokens: 3, cache_creation_input_tokens: 1 },
  });
  assert.equal(usage.inputTokens, 19n);
  assert.equal(usage.cacheReadTokens, 3n);
  assert.equal(usage.cacheWriteTokens, 1n);
});
