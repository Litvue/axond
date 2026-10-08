import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import { isRateLimitPayload } from "./dispatch.ts";
import { createResponsesSequence } from "./responses-sequence.ts";

function sequence() {
  return createResponsesSequence(isRateLimitPayload);
}

function frame(type: string, body: unknown, event: string | null = type): string {
  const name = event === null ? "" : `event: ${event}\n`;
  return `${name}data: ${typeof body === "string" ? body : JSON.stringify(body)}\n\n`;
}

test("responses sequence accepts the fixture and a data-only done", () => {
  const fixture = readFileSync(
    new URL("../../../../tests/fixtures/openai/responses.sse", import.meta.url),
    "utf8",
  );
  const replay = sequence();
  assert.equal(replay.push(fixture), null);
  assert.equal(replay.finish(), null);

  const done = sequence();
  assert.equal(done.push(`${frame("response.created", { type: "response.created" })}data: [DONE]\n\n`), null);
  assert.equal(done.push("event: provider.extension\ndata: "), null);
  assert.equal(done.finish(), null);

  const updated = sequence();
  assert.equal(
    updated.push(frame("rate_limits.updated", { type: "rate_limits.updated", rate_limits: { requests: 10 } }, null)),
    null,
  );
  assert.equal(updated.finish(), null);
});

test("responses sequence names the broken event", () => {
  const disagree = sequence();
  assert.equal(
    disagree.push(frame(
      "response.function_call_arguments.delta",
      { type: "response.function_call_arguments.delta", delta: "provider-controlled" },
      "response.output_text.delta",
    )),
    "provider stream was invalid: Responses SSE event name disagrees with data.type",
  );

  const missingStatus = sequence();
  assert.equal(
    missingStatus.push(frame("response.completed", {
      type: "response.completed",
      response: { id: "resp_1" },
    })),
    "provider stream was invalid: response.completed is missing status=completed",
  );

  const missingObject = sequence();
  assert.equal(
    missingObject.push(frame("response.completed", { type: "response.completed" })),
    "provider stream was invalid: response.completed is missing its completed response object",
  );

  const missingType = sequence();
  assert.equal(
    missingType.push(frame("response.completed", { response: { id: "resp_1" } })),
    "provider stream was invalid: Responses SSE event is missing data.type",
  );

  const namedDone = sequence();
  assert.equal(
    namedDone.push("event: provider.fake\ndata: [DONE]\n\n"),
    "provider stream was invalid: Responses [DONE] sentinel must be a data-only SSE event",
  );

  const limited = sequence();
  assert.equal(
    limited.push(frame("error", { type: "error", error: { type: "rate_limit_exceeded", message: "slow down" } })),
    "provider stream was rate limited: slow down",
  );

  const open = sequence();
  assert.equal(open.push("event: response.created\ndata: {"), null);
  assert.equal(open.finish(), "stream ended with an incomplete SSE event");
});
