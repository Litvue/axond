/** The real OpenAI Node SDK, pointed at axond instead of at OpenAI. */

import assert from "node:assert/strict";
import { after, before, test } from "node:test";

import OpenAI, { APIError, AuthenticationError } from "openai";

import { CHAT, RESPONSES, fixtureJson } from "./fakeUpstream.js";
import {
  CHAT_MODEL,
  EMBEDDINGS_MODEL,
  GATEWAY_KEY,
  NAMESPACE,
  RESPONSES_MODEL,
  UNGRANTED_NAMESPACE,
  UPSTREAM_OPENAI_KEY,
  start,
  type Harness,
} from "./harness.js";

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

interface SdkMount {
  readonly name: string;
  readonly suffix: string;
}

const SDK_MOUNTS: readonly SdkMount[] = [
  { name: "canonical namespace", suffix: `/ns/${NAMESPACE}/v1` },
];

function sdkClient(mount: SdkMount): OpenAI {
  return new OpenAI({ baseURL: `${harness.baseUrl}${mount.suffix}`, apiKey: GATEWAY_KEY });
}

function sdkTest(name: string, body: (mount: SdkMount) => Promise<void>): void {
  for (const mount of SDK_MOUNTS) {
    test(`${mount.name}: ${name}`, () => body(mount));
  }
}

before(async () => {
  harness = await start();
});

after(async () => {
  // Optional so a failed boot reports its own error rather than this hook's.
  await harness?.stop();
});

sdkTest("a buffered chat completion round-trips and forwards the provider credential", async (mount) => {
  const client = sdkClient(mount);
  const completion = await client.chat.completions.create({
    model: CHAT_MODEL,
    messages: [{ role: "user", content: "What is the capital of France?" }],
  });
  const expected = fixtureJson<ChatFixture>("openai/chat_completion.json");
  assert.equal(completion.choices[0]?.message.content, expected.choices[0].message.content);
  assert.equal(completion.usage?.prompt_tokens, expected.usage.prompt_tokens);
  assert.equal(completion.usage?.completion_tokens, expected.usage.completion_tokens);

  // The alias is rewritten to the target model, and the caller's gateway key
  // never travels upstream.
  const sent = harness.upstream.lastRequest();
  assert.equal(sent.path, "/chat/completions");
  assert.equal(sent.model, CHAT);
  assert.equal(sent.authorization, `Bearer ${UPSTREAM_OPENAI_KEY}`);
  assert.ok(!(sent.authorization ?? "").includes(GATEWAY_KEY));
  const messages = sent.body["messages"] as Array<{ content: string }>;
  assert.equal(messages[0]?.content, "What is the capital of France?");
});

sdkTest("a streamed chat completion reassembles from the relayed deltas", async (mount) => {
  const client = sdkClient(mount);
  const stream = await client.chat.completions.create({
    model: CHAT_MODEL,
    messages: [{ role: "user", content: "What is the capital of France?" }],
    stream: true,
  });
  let text = "";
  for await (const chunk of stream) {
    text += chunk.choices[0]?.delta.content ?? "";
  }
  assert.equal(text, "The capital of France is Paris.");
  const sent = harness.upstream.lastRequest();
  assert.equal(sent.path, "/chat/completions");
  assert.equal(sent.model, CHAT);
  assert.equal(sent.body["stream"], true);
  assert.deepEqual(sent.body["messages"], [
    { role: "user", content: "What is the capital of France?" },
  ]);
});

sdkTest("embeddings round-trip", async (mount) => {
  const client = sdkClient(mount);
  // The Node SDK asks for `base64` unless told otherwise and decodes what it
  // gets; the committed fixture is a float body, so the request states the
  // encoding it expects. Passthrough forwards that choice to the provider.
  const response = await client.embeddings.create({
    model: EMBEDDINGS_MODEL,
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

sdkTest("a buffered Responses call round-trips natively", async (mount) => {
  const client = sdkClient(mount);
  const response = await client.responses.create({
    model: RESPONSES_MODEL,
    input: "What is the capital of France?",
  });
  const expected = fixtureJson<ResponsesFixture>("openai/responses.json");
  assert.equal(response.id, expected.id);
  assert.equal(response.output_text, expected.output[0].content[0].text);
  assert.equal(response.usage?.input_tokens, expected.usage.input_tokens);

  const sent = harness.upstream.lastRequest();
  assert.equal(sent.path, "/responses");
  assert.equal(sent.model, RESPONSES);
  assert.equal(sent.body["input"], "What is the capital of France?");
  assert.equal(sent.authorization, `Bearer ${UPSTREAM_OPENAI_KEY}`);
});

sdkTest("a streamed Responses call reassembles from the relayed deltas", async (mount) => {
  const client = sdkClient(mount);
  const stream = await client.responses.create({
    model: RESPONSES_MODEL,
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
  const sent = harness.upstream.lastRequest();
  assert.equal(sent.path, "/responses");
  assert.equal(sent.model, RESPONSES);
  assert.equal(sent.body["stream"], true);
  assert.equal(sent.body["input"], "What is the capital of France?");
});

sdkTest("the alias catalogue is served to the SDK's models.list", async (mount) => {
  const client = sdkClient(mount);
  const listed: string[] = [];
  for await (const model of client.models.list()) {
    listed.push(model.id);
  }
  assert.deepEqual(listed, []);
});

sdkTest("an unknown gateway key is rejected", async (mount) => {
  const stranger = new OpenAI({
    baseURL: `${harness.baseUrl}${mount.suffix}`,
    apiKey: "not-a-gateway-key",
    maxRetries: 0,
  });
  await assert.rejects(
    stranger.chat.completions.create({
      model: CHAT_MODEL,
      messages: [{ role: "user", content: "hi" }],
    }),
    AuthenticationError,
  );
});

interface Refusal {
  readonly status: number | undefined;
  readonly error: unknown;
}

async function modelsRefusal(namespace: string): Promise<Refusal> {
  const candidate = new OpenAI({
    baseURL: `${harness.baseUrl}/ns/${namespace}/v1`,
    apiKey: GATEWAY_KEY,
    maxRetries: 0,
  });
  try {
    await candidate.models.list();
  } catch (error) {
    assert.ok(error instanceof APIError);
    return { status: error.status, error: error.error };
  }
  assert.fail("the namespace request unexpectedly succeeded");
}

test("a store-backed namespace is addressable and an absent one is unknown", async () => {
  const before = harness.upstream.requests.length;
  const existing = new OpenAI({
    baseURL: `${harness.baseUrl}/ns/${UNGRANTED_NAMESPACE}/v1`,
    apiKey: GATEWAY_KEY,
    maxRetries: 0,
  });
  const listed = await existing.models.list();
  assert.equal(listed.object, "list");
  assert.equal(harness.upstream.requests.length, before);

  const absent = await modelsRefusal("ghost");
  assert.deepEqual(absent, {
    status: 404,
    error: {
      type: "unknown_namespace",
      message: "unknown namespace",
    },
  });
  assert.equal(harness.upstream.requests.length, before);
});

test("a noncanonical namespace path receives a generic non-disclosing refusal", async () => {
  const before = harness.upstream.requests.length;
  const refusal = await modelsRefusal("%70latform");

  assert.deepEqual(refusal, {
    status: 400,
    error: {
      type: "invalid_namespace",
      message: "namespace identifier is invalid",
    },
  });
  assert.ok(!JSON.stringify(refusal).includes("%70latform"));
  assert.equal(harness.upstream.requests.length, before);
});
