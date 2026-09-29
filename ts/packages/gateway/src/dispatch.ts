import type { CredentialConfig, ProviderConfig, TransportLimits } from "@axond/sdk";

import { GatewayFailure } from "./errors.ts";
import { assignUsage, emptyUsage, noteSseChunk, usageFromJson } from "./usage.ts";
import type { UsageTokens } from "@axond/sdk";

export interface CredentialCircuit {
  failures: number;
  parkedAt: number | null;
}

export interface CredentialPool {
  tick: number;
  circuits: Map<string, CredentialCircuit>;
}

export interface CredentialPoolPolicy {
  strategy: "round-robin" | "weighted";
  failureThreshold: number;
  cooldownMs: number;
}

export const DEFAULT_CREDENTIAL_POLICY: CredentialPoolPolicy = {
  strategy: "round-robin",
  failureThreshold: 2,
  cooldownMs: 30_000,
};

const KEEPALIVE = new TextEncoder().encode(": keepalive\n\n");

export function credentialPolicy(input?: {
  strategy?: "round-robin" | "weighted";
  failureThreshold?: number;
  cooldownMs?: number;
}): CredentialPoolPolicy {
  return {
    strategy: input?.strategy ?? DEFAULT_CREDENTIAL_POLICY.strategy,
    failureThreshold: input?.failureThreshold ?? DEFAULT_CREDENTIAL_POLICY.failureThreshold,
    cooldownMs: input?.cooldownMs ?? DEFAULT_CREDENTIAL_POLICY.cooldownMs,
  };
}

/**
 * One request's credential walk. A pinned route returns the first credential
 * and does not read or advance health. Otherwise the rotation cursor moves
 * once, a cooldown-elapsed credential is taken as the single half-open probe,
 * and parked credentials are skipped unless every key is parked.
 */
export function planCredentials(
  credentials: readonly CredentialConfig[],
  pools: Map<string, CredentialPool>,
  namespace: string,
  provider: string,
  fallbackNamespace: string | null,
  pinned: boolean,
  now: number,
  policy: CredentialPoolPolicy,
): CredentialConfig[] {
  const pool = credentialPool(credentials, namespace, provider, fallbackNamespace);
  if (pinned) {
    return [pool[0]!];
  }
  const state = poolState(pools, namespace, provider);
  const start = rotationStart(pool, state.tick, policy.strategy);
  state.tick += 1;
  const order = pool.map((_, index) => pool[(start + index) % pool.length]!);
  let probe: CredentialConfig | null = null;
  const healthy: CredentialConfig[] = [];
  const parked: CredentialConfig[] = [];
  for (const candidate of order) {
    const circuit = state.circuits.get(candidate.id);
    if (!circuit || circuit.parkedAt === null) {
      healthy.push(candidate);
      continue;
    }
    if (now - circuit.parkedAt >= policy.cooldownMs) {
      circuit.parkedAt = now;
      probe = candidate;
      continue;
    }
    parked.push(candidate);
  }
  if (probe) {
    return [probe, ...healthy];
  }
  if (healthy.length > 0) {
    return healthy;
  }
  return [parked[0]!];
}

export function noteCredentialFailure(
  pools: Map<string, CredentialPool>,
  namespace: string,
  provider: string,
  credentialId: string,
  now: number,
  threshold: number,
): void {
  const state = poolState(pools, namespace, provider);
  const circuit = state.circuits.get(credentialId) ?? { failures: 0, parkedAt: null };
  circuit.failures += 1;
  if (circuit.failures >= threshold) {
    circuit.parkedAt = now;
  }
  state.circuits.set(credentialId, circuit);
}

export function noteCredentialSuccess(
  pools: Map<string, CredentialPool>,
  namespace: string,
  provider: string,
  credentialId: string,
): void {
  poolState(pools, namespace, provider).circuits.delete(credentialId);
}

export function credentialState(
  pools: Map<string, CredentialPool>,
  credential: CredentialConfig,
  now: number,
  cooldownMs: number,
): "healthy" | "parked" | "probe" {
  const circuit = pools.get(`${credential.namespace}\0${credential.provider}`)?.circuits.get(credential.id);
  if (!circuit || circuit.parkedAt === null) {
    return "healthy";
  }
  return now - circuit.parkedAt < cooldownMs ? "parked" : "probe";
}

function credentialPool(
  credentials: readonly CredentialConfig[],
  namespace: string,
  provider: string,
  fallbackNamespace: string | null,
): CredentialConfig[] {
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
  return pool;
}

function poolState(pools: Map<string, CredentialPool>, namespace: string, provider: string): CredentialPool {
  const key = `${namespace}\0${provider}`;
  const state = pools.get(key) ?? { tick: 0, circuits: new Map() };
  pools.set(key, state);
  return state;
}

function rotationStart(pool: readonly CredentialConfig[], tick: number, strategy: CredentialPoolPolicy["strategy"]): number {
  const count = pool.length;
  if (strategy === "round-robin") {
    return tick % count;
  }
  const total = pool.reduce((sum, credential) => sum + (credential.weight ?? 1), 0);
  let offset = tick % total;
  for (let index = 0; index < count; index += 1) {
    const weight = pool[index]!.weight ?? 1;
    if (offset < weight) {
      return index;
    }
    offset -= weight;
  }
  return count - 1;
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
  if (status === 429) {
    return new GatewayFailure("provider_dependency_failed", 502, message, true);
  }
  if (status >= 500) {
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
      assignUsage(usage, usageFromJson(input.route, JSON.parse(new TextDecoder().decode(bytes))));
    } catch {
      assignUsage(usage, emptyUsage());
    }
    input.onUsage(usage);
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
    const timer = setTimeout(resolve, ms);
    timer.unref?.();
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
