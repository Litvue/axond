/** The real OpenAI Node SDK, pointed at axond instead of at OpenAI. */

import assert from "node:assert/strict";
import { after, before, test } from "node:test";

import OpenAI, { AuthenticationError } from "openai";

import { CHAT, RESPONSES, fixtureJson } from "./fakeUpstream.js";
import { ALIAS_NAMES, GATEWAY_KEY, UPSTREAM_OPENAI_KEY, start, type Harness } from "./harness.js";

interface ChatFixture {
  choices: [{ message: { content: string } }];
  usage: { prompt_tokens: number; completion_tokens: number };
}

interface EmbeddingsFixture {
  data: [{ embedding: number[] }];
  usage: { prompt_tokens: number };
}

interface ResponsesFixture {
  id: string;
  output: [{ content: [{ text: string }] }];
  usage: { input_tokens: number };
}

let harness: Harness;
let client: OpenAI;

before(async () => {
  harness = await start();
  client = new OpenAI({ baseURL: `${harness.baseUrl}/v1`, apiKey: GATEWAY_KEY });
});

after(async () => {
  await harness.stop();
});

test("a buffered chat completion round-trips and forwards the provider credential", async () => {
  const completion = await client.chat.completions.create({
    model: "chat-golden",
    messages: [{ role: "user", content: "What is the capital of France?" }],
  });
  const expected = fixtureJson<ChatFixture>("openai/chat_completion.json");
  assert.equal(completion.choices[0]?.message.content, expected.choices[0].message.content);
  assert.equal(completion.usage?.prompt_tokens, expected.usage.prompt_tokens);
  assert.equal(completion.usage?.completion_tokens, expected.usage.completion_tokens);

  // The alias is rewritten to the target model, and the caller's gateway key
  // never travels upstream.
  const sent = harness.upstream.lastRequest();
  assert.equal(sent.model, CHAT);
  assert.equal(sent.authorization, `Bearer ${UPSTREAM_OPENAI_KEY}`);
  assert.ok(!(sent.authorization ?? "").includes(GATEWAY_KEY));
  const messages = sent.body["messages"] as Array<{ content: string }>;
  assert.equal(messages[0]?.content, "What is the capital of France?");
});

test("a streamed chat completion reassembles from the relayed deltas", async () => {
  const stream = await client.chat.completions.create({
    model: "chat-golden",
    messages: [{ role: "user", content: "What is the capital of France?" }],
    stream: true,
  });
  let text = "";
  for await (const chunk of stream) {
    text += chunk.choices[0]?.delta.content ?? "";
  }
  assert.equal(text, "The capital of France is Paris.");
  assert.equal(harness.upstream.lastRequest().body["stream"], true);
});

test("embeddings round-trip", async () => {
  // The Node SDK asks for `base64` unless told otherwise and decodes what it
  // gets; the committed fixture is a float body, so the request states the
  // encoding it expects. Passthrough forwards that choice to the provider.
  const response = await client.embeddings.create({
    model: "embeddings-golden",
    input: "hello",
    encoding_format: "float",
  });
  const expected = fixtureJson<EmbeddingsFixture>("openai/embeddings.json");
  assert.deepEqual(response.data[0]?.embedding, expected.data[0].embedding);
  assert.equal(response.usage.prompt_tokens, expected.usage.prompt_tokens);
  const sent = harness.upstream.lastRequest();
  assert.equal(sent.path, "/embeddings");
  assert.equal(sent.body["encoding_format"], "float");
  assert.equal(sent.authorization, `Bearer ${UPSTREAM_OPENAI_KEY}`);
});

test("a buffered Responses call round-trips natively", async () => {
  const response = await client.responses.create({
    model: "responses-golden",
    input: "What is the capital of France?",
  });
  const expected = fixtureJson<ResponsesFixture>("openai/responses.json");
  assert.equal(response.id, expected.id);
  assert.equal(response.output_text, expected.output[0].content[0].text);
  assert.equal(response.usage?.input_tokens, expected.usage.input_tokens);

  const sent = harness.upstream.lastRequest();
  assert.equal(sent.path, "/responses");
  assert.equal(sent.model, RESPONSES);
  assert.equal(sent.authorization, `Bearer ${UPSTREAM_OPENAI_KEY}`);
});

test("a streamed Responses call reassembles from the relayed deltas", async () => {
  const stream = await client.responses.create({
    model: "responses-golden",
    input: "What is the capital of France?",
    stream: true,
  });
  let text = "";
  for await (const event of stream) {
    if (event.type === "response.output_text.delta") {
      text += event.delta;
    }
  }
  assert.equal(text, "The capital of France is Paris.");
});

test("the alias catalogue is served to the SDK's models.list", async () => {
  const listed = new Set<string>();
  for await (const model of client.models.list()) {
    listed.add(model.id);
  }
  for (const alias of ALIAS_NAMES) {
    assert.ok(listed.has(alias), `${alias} is missing from /v1/models`);
  }
});

test("an unknown gateway key is rejected", async () => {
  const stranger = new OpenAI({
    baseURL: `${harness.baseUrl}/v1`,
    apiKey: "not-a-gateway-key",
    maxRetries: 0,
  });
  await assert.rejects(
    stranger.chat.completions.create({
      model: "chat-golden",
      messages: [{ role: "user", content: "hi" }],
    }),
    AuthenticationError,
  );
});
