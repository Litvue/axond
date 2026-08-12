/**
 * The real Anthropic Node SDK, pointed at axond's native `/v1/messages`.
 *
 * The SDK sends its gateway key as `x-api-key`, which is why inbound auth
 * accepts both schemes (ADR 0012/0013), and it parses thinking and tool-use
 * blocks strictly — so it passing is the byte-fidelity claim, verified by a
 * client that was not written for this gateway.
 */

import assert from "node:assert/strict";
import { after, before, test } from "node:test";

import Anthropic, { AuthenticationError } from "@anthropic-ai/sdk";

import { MESSAGES, fixtureJson } from "./fakeUpstream.js";
import {
  GATEWAY_KEY,
  UPSTREAM_ANTHROPIC_KEY,
  start,
  type Harness,
} from "./harness.js";

interface MessageFixture {
  content: [
    { signature: string; thinking: string },
    { text: string },
    { name: string; input: Record<string, unknown> },
  ];
  usage: { input_tokens: number; output_tokens: number };
}

let harness: Harness;
let client: Anthropic;

before(async () => {
  harness = await start();
  client = new Anthropic({ baseURL: harness.baseUrl, apiKey: GATEWAY_KEY });
});

after(async () => {
  await harness.stop();
});

test("a buffered message preserves thinking and tool-use blocks", async () => {
  const message = await client.messages.create({
    model: "messages-golden",
    max_tokens: 1024,
    thinking: { type: "enabled", budget_tokens: 1024 },
    messages: [{ role: "user", content: "Weather in Paris?" }],
  });
  const expected = fixtureJson<MessageFixture>("anthropic/message_thinking_tool_use.json");

  const [thinking, text, toolUse] = message.content;
  assert.equal(thinking?.type, "thinking");
  assert.equal(thinking?.type === "thinking" ? thinking.signature : undefined, expected.content[0].signature);
  assert.equal(text?.type === "text" ? text.text : undefined, expected.content[1].text);
  assert.equal(toolUse?.type, "tool_use");
  assert.deepEqual(
    toolUse?.type === "tool_use" ? toolUse.input : undefined,
    expected.content[2].input,
  );
  assert.equal(message.usage.input_tokens, expected.usage.input_tokens);
  assert.equal(message.usage.output_tokens, expected.usage.output_tokens);

  // The Anthropic wire is native: the path, the rewritten model, and the
  // provider's own `x-api-key` — never the caller's gateway key.
  const sent = harness.upstream.lastRequest();
  assert.equal(sent.path, "/messages");
  assert.equal(sent.model, MESSAGES);
  assert.equal(sent.apiKey, UPSTREAM_ANTHROPIC_KEY);
  assert.notEqual(sent.apiKey, GATEWAY_KEY);
  assert.ok(sent.anthropicVersion, "the anthropic-version header did not survive");
});

test("a streamed message reassembles thinking and tool use", async () => {
  const final = await client.messages
    .stream({
      model: "messages-golden",
      max_tokens: 1024,
      thinking: { type: "enabled", budget_tokens: 1024 },
      messages: [{ role: "user", content: "Weather in Paris?" }],
    })
    .finalMessage();

  const [thinking, text, toolUse] = final.content;
  assert.ok(thinking?.type === "thinking");
  // The signature survives the relay byte-for-byte, which is what makes a
  // thinking block replayable back to the provider on the next turn.
  assert.equal(thinking.signature, "REDACTED_THINKING_SIGNATURE_0002");
  assert.equal(thinking.thinking, "The user wants the weather in Paris.");
  assert.ok(text?.type === "text");
  assert.equal(text.text, "Let me look that up.");
  assert.ok(toolUse?.type === "tool_use");
  assert.equal(toolUse.name, "get_weather");
  assert.deepEqual(toolUse.input, { location: "Paris, France" });
  assert.equal(final.stop_reason, "tool_use");
  assert.deepEqual([final.usage.input_tokens, final.usage.output_tokens], [41, 63]);
});

test("a declared tool round-trips to the upstream unchanged", async () => {
  const tools = [
    {
      name: "get_weather",
      description: "Look up the weather.",
      input_schema: {
        type: "object" as const,
        properties: { location: { type: "string" } },
        required: ["location"],
      },
    },
  ];
  await client.messages.create({
    model: "messages-golden",
    max_tokens: 1024,
    tools,
    messages: [{ role: "user", content: "Weather in Paris?" }],
  });
  assert.deepEqual(harness.upstream.lastRequest().body["tools"], tools);
});

test("an unknown gateway key is rejected", async () => {
  const stranger = new Anthropic({
    baseURL: harness.baseUrl,
    apiKey: "not-a-gateway-key",
    maxRetries: 0,
  });
  await assert.rejects(
    stranger.messages.create({
      model: "messages-golden",
      max_tokens: 16,
      messages: [{ role: "user", content: "hi" }],
    }),
    AuthenticationError,
  );
});
