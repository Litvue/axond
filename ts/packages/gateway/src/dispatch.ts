import type { CredentialConfig, ProviderConfig, TransportLimits } from "@axond/sdk";

import { GatewayFailure } from "./errors.ts";
import { emptyUsage, noteSseChunk, usageFromJson } from "./usage.ts";
import type { UsageTokens } from "@axond/sdk";

export interface CredentialPool {
  cursor: number;
  failures: Map<string, { count: number; openUntil: number }>;
}

const KEEPALIVE = new TextEncoder().encode(": keepalive\n\n");

export function selectCredential(
  credentials: readonly CredentialConfig[],
  pools: Map<string, CredentialPool>,
  namespace: string,
  provider: string,
  fallbackNamespace: string | null,
  pinned: boolean,
  now: number,
): CredentialConfig {
  const own = credentials.filter((credential) => credential.namespace === namespace && credential.provider === provider);
  const fallback =
    own.length === 0 && fallbackNamespace !== null
      ? credentials.filter((credential) => credential.namespace === fallbackNamespace && credential.provider === provider)
      : [];
  const pool = own.length > 0 ? own : fallback;
  if (pool.length === 0) {
    throw new GatewayFailure(
      "no_credential",
      502,
      `no credential for provider \`${provider}\` in namespace \`${namespace}\``,
    );
  }
  if (pinned) {
    return pool[0]!;
  }
  const key = `${namespace}\0${provider}`;
  const state = pools.get(key) ?? { cursor: 0, failures: new Map() };
  pools.set(key, state);
  for (let offset = 0; offset < pool.length; offset += 1) {
    const index = (state.cursor + offset) % pool.length;
    const candidate = pool[index]!;
    const health = state.failures.get(candidate.id);
    if (health && health.openUntil > now) {
      continue;
    }
    state.cursor = (index + 1) % pool.length;
    return candidate;
  }
  return pool[0]!;
}

export function noteCredentialFailure(
  pools: Map<string, CredentialPool>,
  namespace: string,
  provider: string,
  credentialId: string,
  now: number,
  threshold = 3,
  cooldownMs = 30_000,
): void {
  const key = `${namespace}\0${provider}`;
  const state = pools.get(key) ?? { cursor: 0, failures: new Map() };
  pools.set(key, state);
  const current = state.failures.get(credentialId) ?? { count: 0, openUntil: 0 };
  current.count += 1;
  if (current.count >= threshold) {
    current.openUntil = now + cooldownMs;
    current.count = 0;
  }
  state.failures.set(credentialId, current);
}

export function classifyUpstream(status: number, body: string): GatewayFailure {
  let message = "upstream request failed";
  try {
    const parsed = JSON.parse(body) as { error?: { message?: string } };
    if (typeof parsed.error?.message === "string" && parsed.error.message.length > 0) {
      message = parsed.error.message.slice(0, 512);
    }
  } catch {
    if (body.length > 0 && body.length < 512) {
      message = body;
    }
  }
  if (status === 401 || status === 403) {
    return new GatewayFailure("invalid_request", 502, message);
  }
  if (status === 404) {
    return new GatewayFailure("model_unavailable", 502, message);
  }
  if (status === 429 || status >= 500) {
    return new GatewayFailure("provider_dependency_failed", 502, message);
  }
  if (message.toLowerCase().includes("context window")) {
    return new GatewayFailure("context_window_exceeded", 400, message);
  }
  return new GatewayFailure("invalid_request", 400, message);
}

export async function callUpstream(input: {
  url: string;
  headers: Headers;
  body: Uint8Array;
  transport: TransportLimits;
  stream: boolean;
  route: string;
  onUsage: (usage: UsageTokens) => void;
  onStreamDone?: (reason: "end" | "cancel") => void;
}): Promise<{ response: Response; usage: UsageTokens }> {
  const usage = emptyUsage();
  const controller = new AbortController();
  const headerTimer = setTimeout(() => controller.abort(), input.transport.responseHeaderTimeoutMs);
  let response: Response;
  try {
    response = await fetch(input.url, {
      method: "POST",
      headers: input.headers,
      body: input.body,
      signal: controller.signal,
    });
  } catch (error) {
    if (controller.signal.aborted) {
      throw timeout("waiting for provider response headers", input.transport.responseHeaderTimeoutMs);
    }
    throw new GatewayFailure("upstream_transport", 502, "upstream transport failure");
  } finally {
    clearTimeout(headerTimer);
  }
  if (!response.ok) {
    const text = await readLimited(response, input.transport.maxResponseBytes);
    throw classifyUpstream(response.status, text);
  }
  if (!input.stream || response.body === null) {
    const bytes = await readLimitedBytes(response, input.transport.maxResponseBytes, input.transport.bufferedBodyTimeoutMs);
    try {
      input.onUsage(usageFromJson(input.route, JSON.parse(new TextDecoder().decode(bytes))));
    } catch {
      input.onUsage(usage);
    }
    const headers = passHeaders(response.headers);
    return {
      response: new Response(bytes, { status: response.status, headers }),
      usage,
    };
  }
  const stream = relayStream(response.body, input.transport, input.route, usage, (reason) => {
    input.onUsage(usage);
    input.onStreamDone?.(reason);
  });
  const headers = passHeaders(response.headers);
  if (!headers.has("content-type")) {
    headers.set("content-type", "text/event-stream");
  }
  return {
    response: new Response(stream, { status: response.status, headers }),
    usage,
  };
}

function relayStream(
  upstream: ReadableStream<Uint8Array>,
  transport: TransportLimits,
  route: string,
  usage: UsageTokens,
  onDone: (reason: "end" | "cancel") => void,
): ReadableStream<Uint8Array> {
  const reader = upstream.getReader();
  const decoder = new TextDecoder();
  let pending = "";
  let inflight: Promise<ReadableStreamReadResult<Uint8Array>> | null = null;
  let sawChunkAt = Date.now();
  let done = false;
  const finish = (reason: "end" | "cancel") => {
    if (done) {
      return;
    }
    done = true;
    if (pending.length > 0) {
      noteSseChunk(route, usage, pending);
      pending = "";
    }
    onDone(reason);
  };
  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      const value = await readWithIdle(reader, () => inflight, (next) => {
        inflight = next;
      }, transport.streamIdleTimeoutMs, sawChunkAt, controller);
      if (value === "keepalive") {
        return;
      }
      if (value === null) {
        finish("end");
        controller.close();
        return;
      }
      sawChunkAt = Date.now();
      pending += decoder.decode(value, { stream: true });
      noteSseChunk(route, usage, pending);
      pending = tail(pending);
      controller.enqueue(value);
    },
    cancel(reason) {
      finish("cancel");
      return reader.cancel(reason);
    },
  });
}

function tail(text: string): string {
  const lf = text.lastIndexOf("\n\n");
  const crlf = text.lastIndexOf("\r\n\r\n");
  const at = Math.max(lf, crlf);
  if (at === -1) {
    return text.length > 1_000_000 ? text.slice(-1024) : text;
  }
  return text.slice(at);
}

async function readWithIdle(
  reader: ReadableStreamDefaultReader<Uint8Array>,
  current: () => Promise<ReadableStreamReadResult<Uint8Array>> | null,
  setCurrent: (next: Promise<ReadableStreamReadResult<Uint8Array>> | null) => void,
  idleMs: number,
  sawChunkAt: number,
  controller: ReadableStreamDefaultController<Uint8Array>,
): Promise<Uint8Array | null | "keepalive"> {
  let read = current();
  if (read === null) {
    read = reader.read();
    setCurrent(read);
  }
  const started = Date.now();
  let lastKeepalive = sawChunkAt;
  for (;;) {
    const elapsed = Date.now() - started;
    if (elapsed >= idleMs) {
      await reader.cancel();
      throw timeout("waiting for the next provider stream chunk", idleMs);
    }
    const untilKeepalive = 15_000 - (Date.now() - lastKeepalive);
    const wait = Math.max(1, Math.min(idleMs - elapsed, untilKeepalive));
    const result = await Promise.race([
      read.then((chunk) => ({ kind: "chunk" as const, chunk })),
      delay(wait).then(() => ({ kind: "wait" as const })),
    ]);
    if (result.kind === "chunk") {
      setCurrent(null);
      return result.chunk.done ? null : result.chunk.value;
    }
    if (Date.now() - lastKeepalive >= 15_000) {
      lastKeepalive = Date.now();
      controller.enqueue(KEEPALIVE);
      return "keepalive";
    }
  }
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function timeout(phase: string, budgetMs: number): GatewayFailure {
  return new GatewayFailure("upstream_timeout", 504, `transport: ${phase} exceeded its ${budgetMs}ms bound`);
}

function passHeaders(headers: Headers): Headers {
  const next = new Headers();
  const contentType = headers.get("content-type");
  if (contentType) {
    next.set("content-type", contentType);
  }
  return next;
}

async function readLimited(response: Response, limit: number): Promise<string> {
  const bytes = await readLimitedBytes(response, limit, 30_000);
  return new TextDecoder().decode(bytes);
}

async function readLimitedBytes(response: Response, limit: number, timeoutMs: number): Promise<Uint8Array> {
  if (response.body === null) {
    return new Uint8Array();
  }
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  const started = Date.now();
  for (;;) {
    if (Date.now() - started > timeoutMs) {
      throw timeout("reading the provider response body", timeoutMs);
    }
    const chunk = await reader.read();
    if (chunk.done) {
      break;
    }
    total += chunk.value.length;
    if (total > limit) {
      throw new GatewayFailure(
        "upstream_body_too_large",
        502,
        `transport: provider response body exceeded its ${limit}-byte bound`,
      );
    }
    chunks.push(chunk.value);
  }
  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.length;
  }
  return out;
}
