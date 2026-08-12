/**
 * A fake provider upstream serving the committed wire fixtures.
 *
 * The TypeScript twin of `tests/compat/fake_upstream.py` and
 * `crates/gateway/tests/support/upstream.rs`: same fixtures, same target-model
 * vocabulary, so every harness qualifies the gateway against identical bytes
 * (ADR 0014).
 */

import { createServer, type IncomingMessage, type Server, type ServerResponse } from "node:http";
import { readFileSync } from "node:fs";
import { join, resolve } from "node:path";
import type { AddressInfo } from "node:net";

const REPO_ROOT = resolve(import.meta.dirname, "../../..");
const FIXTURES = join(REPO_ROOT, "tests/fixtures");

/** Target models the fake upstream understands; an alias points at one. */
export const CHAT = "fixture-chat";
export const EMBEDDINGS = "fixture-embeddings";
export const RESPONSES = "fixture-responses";
export const MESSAGES = "fixture-messages";

const BUFFERED: Record<string, string> = {
  "/chat/completions": "openai/chat_completion.json",
  "/embeddings": "openai/embeddings.json",
  "/responses": "openai/responses.json",
  "/messages": "anthropic/message_thinking_tool_use.json",
};

const STREAMED: Record<string, string> = {
  "/chat/completions": "openai/chat_completion.sse",
  "/messages": "anthropic/message_thinking_tool_use.sse",
  "/responses": "openai/responses.sse",
};

export function fixture(name: string): Buffer {
  return readFileSync(join(FIXTURES, name));
}

export function fixtureJson<T>(name: string): T {
  return JSON.parse(fixture(name).toString("utf8")) as T;
}

/** What the gateway sent upstream, as the upstream saw it. */
export interface UpstreamRequest {
  readonly path: string;
  readonly model: unknown;
  readonly authorization: string | undefined;
  readonly apiKey: string | undefined;
  readonly anthropicVersion: string | undefined;
  readonly body: Record<string, unknown>;
}

/** A running fake upstream on loopback. */
export class FakeUpstream {
  readonly #server: Server;
  readonly #requests: UpstreamRequest[];

  private constructor(server: Server, requests: UpstreamRequest[]) {
    this.#server = server;
    this.#requests = requests;
  }

  static async start(): Promise<FakeUpstream> {
    const requests: UpstreamRequest[] = [];
    const server = createServer((request, response) => {
      handle(request, response, requests).catch(() => response.destroy());
    });
    await new Promise<void>((ready) => server.listen(0, "127.0.0.1", ready));
    return new FakeUpstream(server, requests);
  }

  get baseUrl(): string {
    const address = this.#server.address() as AddressInfo;
    return `http://127.0.0.1:${address.port}`;
  }

  get requests(): readonly UpstreamRequest[] {
    return this.#requests;
  }

  /** The most recent request, which is what a test asserts against. */
  lastRequest(): UpstreamRequest {
    const last = this.#requests.at(-1);
    if (last === undefined) {
      throw new Error("the fake upstream received no request");
    }
    return last;
  }

  async stop(): Promise<void> {
    this.#server.closeAllConnections();
    await new Promise<void>((closed, failed) =>
      this.#server.close((error) => (error ? failed(error) : closed())),
    );
  }
}

async function handle(
  request: IncomingMessage,
  response: ServerResponse,
  requests: UpstreamRequest[],
): Promise<void> {
  const raw = await readBody(request);
  const body = (raw.byteLength > 0 ? JSON.parse(raw.toString("utf8")) : {}) as Record<
    string,
    unknown
  >;
  const path = request.url ?? "";
  requests.push({
    path,
    model: body["model"],
    authorization: header(request, "authorization"),
    apiKey: header(request, "x-api-key"),
    anthropicVersion: header(request, "anthropic-version"),
    body,
  });

  const streamedFixture = STREAMED[path];
  if (body["stream"] === true && streamedFixture !== undefined) {
    stream(response, fixture(streamedFixture));
    return;
  }
  const bufferedFixture = BUFFERED[path];
  if (bufferedFixture === undefined) {
    response.writeHead(404, { "content-type": "application/json" });
    response.end(JSON.stringify({ error: { type: "not_found", message: path } }));
    return;
  }
  buffered(response, fixture(bufferedFixture));
}

function header(request: IncomingMessage, name: string): string | undefined {
  const value = request.headers[name];
  return Array.isArray(value) ? value[0] : value;
}

async function readBody(request: IncomingMessage): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of request) {
    chunks.push(chunk as Buffer);
  }
  return Buffer.concat(chunks);
}

function buffered(response: ServerResponse, payload: Buffer): void {
  response.writeHead(200, {
    "content-type": "application/json",
    "content-length": String(payload.byteLength),
  });
  response.end(payload);
}

/** Write the recorded SSE bytes one event at a time, close-delimited. */
function stream(response: ServerResponse, payload: Buffer): void {
  response.writeHead(200, { "content-type": "text/event-stream", connection: "close" });
  for (const event of payload.toString("utf8").split("\n\n")) {
    if (event.length === 0) {
      continue;
    }
    response.write(`${event}\n\n`);
  }
  response.end();
}
