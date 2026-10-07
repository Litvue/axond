import assert from "node:assert/strict";
import test from "node:test";

import { transformSseEvents } from "@axond/sdk";

import { rewriteTopLevelModel } from "./body.ts";
import { WITHDRAWN_SECTIONS, envSecretReader, loadConfig } from "./config.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const SECRET = "SECRET9f3c";
const SENTINEL = "SENTINEL9f3c";

function mulberry32(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let mixed = Math.imul(state ^ (state >>> 15), 1 | state);
    mixed = (mixed + Math.imul(mixed ^ (mixed >>> 7), 61 | mixed)) ^ mixed;
    return ((mixed ^ (mixed >>> 14)) >>> 0) / 4294967296;
  };
}

function pick(rng: () => number, limit: number): number {
  return Math.floor(rng() * limit);
}

async function pump(
  parts: Uint8Array[],
  onEvent: (event: { event: string | null; id: string | null; data: string }) => void,
): Promise<Uint8Array> {
  let index = 0;
  const stream = transformSseEvents(
    new ReadableStream({
      pull(controller) {
        if (index >= parts.length) {
          controller.close();
          return;
        }
        controller.enqueue(parts[index]!);
        index += 1;
      },
    }),
    (event) => {
      onEvent(event);
      return event;
    },
  );
  const reader = stream.getReader();
  const buffers: Uint8Array[] = [];
  for (;;) {
    const next = await reader.read();
    if (next.done) {
      break;
    }
    buffers.push(next.value);
  }
  const bytes = new Uint8Array(buffers.reduce((total, item) => total + item.length, 0));
  let offset = 0;
  for (const item of buffers) {
    bytes.set(item, offset);
    offset += item.length;
  }
  return bytes;
}

function splitBytes(bytes: Uint8Array, rng: () => number): Uint8Array[] {
  const cuts = new Set<number>();
  const count = pick(rng, 6);
  for (let index = 0; index < count; index += 1) {
    if (bytes.length > 1) {
      cuts.add(1 + pick(rng, bytes.length - 1));
    }
  }
  const ordered = [...cuts].sort((left, right) => left - right);
  const parts: Uint8Array[] = [];
  let start = 0;
  for (const cut of ordered) {
    parts.push(bytes.slice(start, cut));
    start = cut;
  }
  parts.push(bytes.slice(start));
  return parts;
}

function randomBody(rng: () => number): string {
  const tokens = ["a", ":", " ", "\n", "\r", "data", "event", "id", "é", "\n\n", "\r\n\r\n", ": ping", "data: hi"];
  const count = pick(rng, 24);
  let body = "";
  for (let index = 0; index < count; index += 1) {
    body += tokens[pick(rng, tokens.length)]!;
  }
  return body;
}

function eventKey(event: { event: string | null; id: string | null; data: string }): string {
  return `${event.event ?? ""}\u0000${event.id ?? ""}\u0000${event.data}`;
}

test("sse_config_and_body_rewrite_are_stable_under_arbitrary_splits", async () => {
  const rng = mulberry32(0x5eed);
  for (let round = 0; round < 48; round += 1) {
    const body = randomBody(rng);
    const bytes = encoder.encode(body);
    const wholeEvents: string[] = [];
    const splitEvents: string[] = [];
    const whole = await pump([bytes], (event) => wholeEvents.push(eventKey(event)));
    const split = await pump(splitBytes(bytes, rng), (event) => splitEvents.push(eventKey(event)));
    assert.deepEqual(whole, bytes);
    assert.deepEqual(split, bytes);
    assert.deepEqual(splitEvents, wholeEvents);
    const decoded = wholeEvents.reduce((total, key) => total + (key.split("\u0000")[2]?.length ?? 0), 0);
    assert.ok(decoded <= body.length);
  }

  for (let round = 0; round < 48; round += 1) {
    const alphabet = "ab\"\\é ";
    let current = "";
    let next = "";
    const currentLen = pick(rng, 8);
    const nextLen = pick(rng, 8);
    for (let index = 0; index < currentLen; index += 1) {
      current += alphabet[pick(rng, alphabet.length)]!;
    }
    for (let index = 0; index < nextLen; index += 1) {
      next += alphabet[pick(rng, alphabet.length)]!;
    }
    const raw = `{"z":1,"model":${JSON.stringify(current)},"n":9007199254740993,"a":1,"a":2,"child":{"model":"keep-nested"}}`;
    const rewritten = decoder.decode(rewriteTopLevelModel(encoder.encode(raw), next));
    const expected = `{"z":1,"model":${JSON.stringify(next)},"n":9007199254740993,"a":1,"a":2,"child":{"model":"keep-nested"}}`;
    assert.equal(rewritten, expected);
    assert.equal(rewritten.includes("9007199254740993"), true);
    assert.equal(rewritten.includes('"a":1,"a":2'), true);
    assert.equal(rewritten.includes('"model":"keep-nested"'), true);
    const again = decoder.decode(rewriteTopLevelModel(encoder.encode(rewritten), next));
    assert.equal(again, rewritten);
    const numeric = '{"model":12,"n":9007199254740993}';
    assert.equal(decoder.decode(rewriteTopLevelModel(encoder.encode(numeric), "chat")), numeric);
  }

  const secrets = envSecretReader({ GW_KEY: SECRET }, async () => "");
  for (let round = 0; round < 32; round += 1) {
    const section = WITHDRAWN_SECTIONS[pick(rng, WITHDRAWN_SECTIONS.length)]!;
    const toml = `
[storage]
backend = "sqlite"
path = "/tmp/axond-${round}.sqlite"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
[${section}]
token = "${SENTINEL}"
`;
    const first = await loadConfig(toml, secrets).then(
      () => "ok",
      (error: unknown) => (error instanceof Error ? error.message : "throw"),
    );
    const second = await loadConfig(toml, secrets).then(
      () => "ok",
      (error: unknown) => (error instanceof Error ? error.message : "throw"),
    );
    assert.equal(second, first);
    assert.equal(first.includes(SECRET), false);
    assert.equal(first.includes(SENTINEL), false);
    assert.equal(first.includes(section), true);
  }
});
