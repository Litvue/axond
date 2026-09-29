import assert from "node:assert/strict";
import test from "node:test";

import { emptyUsage, noteSseChunk, sseTerminalSeen, usageEvent, usageFromJson } from "./usage.ts";

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

test("a complete terminal frame starts the post-terminal grace", () => {
  const partial = 'event: response.completed\ndata: {"type":"response.completed"';
  assert.equal(sseTerminalSeen("responses", partial), false);
  const completed =
    'event: response.completed\ndata: {"type":"response.completed","response":{"status":"completed"}}\n\n';
  assert.equal(sseTerminalSeen("responses", completed), true);
  assert.equal(sseTerminalSeen("chat", "data: [DONE]\n\n"), true);
  assert.equal(
    sseTerminalSeen("messages", 'event: message_stop\ndata: {"type":"message_stop"}\n\n'),
    true,
  );
});

test("anthropic cache tokens stay disjoint from input", () => {
  const usage = usageFromJson("messages", {
    usage: { input_tokens: 19, output_tokens: 7, cache_read_input_tokens: 3, cache_creation_input_tokens: 1 },
  });
  assert.equal(usage.inputTokens, 19n);
  assert.equal(usage.cacheReadTokens, 3n);
  assert.equal(usage.cacheWriteTokens, 1n);
});

test("usage event names the serving credential and omits an absent trace", () => {
  const base = {
    schemaVersion: 2 as const,
    requestId: "req-1",
    traceId: null,
    namespace: "platform",
    period: "compat",
    subject: "gateway-key",
    model: "fake-openai/gpt-test",
    targetProvider: "fake-openai",
    targetModel: "gpt-test",
    credentialSource: "byok" as const,
    credentialId: "tenant-own",
    status: "ok",
    inputTokens: 4n,
    outputTokens: 1n,
    reasoningTokens: 0n,
    cacheReadTokens: 2n,
    cacheWriteTokens: 0n,
    costMicrodollars: 5n,
    catalogVersion: 0 as const,
    priceBook: null,
    priceBookChecksum: null,
    priceCatalog: null,
    signerKid: null,
    latencyMs: 12,
    attempts: 1,
  };
  const plain = usageEvent(base);
  assert.equal(Object.hasOwn(plain, "trace_id"), false);
  assert.equal(plain.credential_source, "byok");
  assert.equal(plain.credential_id, "tenant-own");
  assert.equal(plain.attempts, 1);
  assert.equal(plain.latency_ms, 12);
  assert.equal(plain.period, "compat");
  assert.equal(plain.cache_read_tokens, "2");
  assert.equal(plain.price_catalog, null);
  const traced = usageEvent({ ...base, traceId: "0123456789abcdef0123456789abcdef" });
  assert.equal(traced.trace_id, "0123456789abcdef0123456789abcdef");
  const keys = Object.keys(traced);
  assert.equal(keys.indexOf("trace_id"), keys.indexOf("request_id") + 1);
});
