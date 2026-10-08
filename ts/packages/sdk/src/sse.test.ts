import assert from "node:assert/strict";
import test from "node:test";

import { transformSseEvents } from "./sse.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

function chunks(parts: string[]): ReadableStream<Uint8Array> {
  let index = 0;
  return new ReadableStream({
    pull(controller) {
      if (index >= parts.length) {
        controller.close();
        return;
      }
      controller.enqueue(encoder.encode(parts[index]!));
      index += 1;
    },
  });
}

async function readAll(stream: ReadableStream<Uint8Array>): Promise<{ text: string; pieces: string[] }> {
  const reader = stream.getReader();
  const pieces: string[] = [];
  const buffers: Uint8Array[] = [];
  for (;;) {
    const next = await reader.read();
    if (next.done) {
      break;
    }
    pieces.push(decoder.decode(next.value));
    buffers.push(next.value);
  }
  const bytes = new Uint8Array(buffers.reduce((total, item) => total + item.length, 0));
  let offset = 0;
  for (const item of buffers) {
    bytes.set(item, offset);
    offset += item.length;
  }
  return { text: decoder.decode(bytes), pieces };
}

test("a split frame is one event and an identity transform keeps the bytes", async () => {
  const seen: string[] = [];
  const { text, pieces } = await readAll(
    transformSseEvents(chunks(["data: hel", "lo\n\n"]), (event) => {
      seen.push(event.data);
      return event;
    }),
  );
  assert.deepEqual(seen, ["hello"]);
  assert.deepEqual(pieces, ["data: hello\n\n"]);
  assert.equal(text, "data: hello\n\n");
});

test("a delimiter split across chunks is not delivered early", async () => {
  const seen: string[] = [];
  const { text } = await readAll(
    transformSseEvents(chunks(["data: hi\n", "\n"]), (event) => {
      seen.push(event.data);
      return event;
    }),
  );
  assert.deepEqual(seen, ["hi"]);
  assert.equal(text, "data: hi\n\n");
});

test("an identity transform keeps a comment, field order, and a CRLF delimiter", async () => {
  const raw = "id: 7\r\ndata: hi\r\n: ping\r\n\r\n";
  const { text } = await readAll(
    transformSseEvents(chunks([raw.slice(0, 10), raw.slice(10)]), (event) => event),
  );
  assert.equal(text, raw);
});

test("a changed event is re-encoded and the next frame stays raw", async () => {
  const raw = "data: one\n\ndata: two\n: keep\n\n";
  const { pieces } = await readAll(
    transformSseEvents(chunks([raw]), (event) => {
      if (event.data === "one") {
        return { ...event, data: "ONE" };
      }
      return event;
    }),
  );
  assert.deepEqual(pieces, ["data: ONE\n\n", "data: two\n: keep\n\n"]);
});

test("a null transform drops that frame and still emits the next", async () => {
  const { text } = await readAll(
    transformSseEvents(chunks(["data: drop\n", "\ndata: keep\n\n"]), (event) =>
      event.data === "drop" ? null : event,
    ),
  );
  assert.equal(text, "data: keep\n\n");
});

test("an incomplete tail is forwarded and the transform is not called", async () => {
  let calls = 0;
  const { text } = await readAll(
    transformSseEvents(chunks(["data: partial"]), () => {
      calls += 1;
      return null;
    }),
  );
  assert.equal(calls, 0);
  assert.equal(text, "data: partial");
});

test("a multibyte character split across chunks stays one event", async () => {
  const cafe = encoder.encode("data: café\n\n");
  assert.equal(cafe[9], 0xc3);
  let step = 0;
  const seen: string[] = [];
  const { text } = await readAll(
    transformSseEvents(
      new ReadableStream({
        pull(controller) {
          if (step === 0) {
            controller.enqueue(cafe.slice(0, 10));
            step = 1;
            return;
          }
          if (step === 1) {
            controller.enqueue(cafe.slice(10));
            step = 2;
            return;
          }
          controller.close();
        },
      }),
      (event) => {
        seen.push(event.data);
        return event;
      },
    ),
  );
  assert.deepEqual(seen, ["café"]);
  assert.equal(text, "data: café\n\n");
});


test("bare CR and mixed newlines cannot bypass transforms, even across chunks", async () => {
  for (const delimiter of ["\r\r", "\n\r\n", "\r\n\n"]) {
    const bytes = new TextEncoder().encode(`data: secret${delimiter}`);
    const source = new ReadableStream({ start(c) { for (const byte of bytes) c.enqueue(new Uint8Array([byte])); c.close(); } });
    const transformed = transformSseEvents(source, (event) => ({ ...event, data: event.data.replace("secret", "hidden") }));
    assert.equal(await new Response(transformed).text(), "data: hidden\n\n");
  }
});

test("an oversized unterminated event cancels its source", async () => {
  let cancelled = false;
  const source = new ReadableStream({ pull(c) { c.enqueue(new Uint8Array(64 * 1024).fill(97)); }, cancel() { cancelled = true; } });
  await assert.rejects(new Response(transformSseEvents(source, (event) => event)).arrayBuffer(), /byte limit/);
  assert.equal(cancelled, true);
});


test("a live CR-terminated event is delivered without another upstream byte", async () => {
  for (const terminal of ["\r\r", "\r\n\r"]) {
    let source!: ReadableStreamDefaultController<Uint8Array>;
    const input = new ReadableStream<Uint8Array>({ start(c) { source = c; c.enqueue(encoder.encode("data: secret" + terminal)); } });
    const reader = transformSseEvents(input, event => ({ ...event, data: "hidden" })).getReader();
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      const value = await Promise.race([reader.read(), new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error("completed CR event stalled")), 250); })]);
      assert.equal(decoder.decode(value.value), "data: hidden\n\n");
      source.enqueue(encoder.encode("\ndata: next\n\n"));
      assert.equal(decoder.decode((await reader.read()).value), "data: hidden\n\n");
    } finally { clearTimeout(timer); await reader.cancel(); }
  }
});

test("an identity transform preserves a delayed optional delimiter LF", async () => {
  const seen: string[] = [];
  const wire = "data: one\r\n\r\ndata: two\r\r";
  const result = await readAll(transformSseEvents(chunks(["data: one\r\n\r", "\n", "data: two\r\r"]), event => { seen.push(event.data); return event; }));
  assert.equal(result.text, wire); assert.deepEqual(seen, ["one", "two"]);
});
