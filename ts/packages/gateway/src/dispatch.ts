import type { CredentialConfig, ProviderConfig, TransportLimits } from "@axond/sdk";

import { GatewayFailure } from "./errors.ts";
import { assignUsage, emptyUsage, noteSseChunk, sseTerminalSeen, usageFromJson } from "./usage.ts";
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
  /** Shared failover deadline. Absent starts a budget of `overallTimeoutMs` at this call. */
  deadlineAt?: number;
  now?: () => number;
  /** Node client that enforces the connect bound. Absent means the runtime owns connect. */
  dispatcher?: object;
  onTimeout?: (kind: string, bound: string) => void;
  /**
   * OpenAI chat only. A rate-limit event before any byte is released asks for
   * another upstream. Null keeps the held event and ends the stream.
   */
  onBeforeContentRateLimit?: () => Promise<Response | null>;
}): Promise<{ response: Response; usage: UsageTokens }> {
  const transport = withTransportDefaults(input.transport);
  const now = input.now ?? Date.now;
  const usage = emptyUsage();
  const current = now();
  const deadlineAt = input.deadlineAt ?? current + transport.overallTimeoutMs;
  if (current >= deadlineAt) {
    throw timeoutFailure("overall", "walk_budget", 0, input.onTimeout);
  }
  const header = phaseBudget(transport.responseHeaderTimeoutMs, deadlineAt, current);
  const controller = new AbortController();
  const headerTimer = setTimeout(() => controller.abort(), header.ms);
  let response: Response;
  try {
    const init: RequestInit & { dispatcher?: object } = {
      method: "POST",
      headers: input.headers,
      body: input.body,
      signal: controller.signal,
    };
    if (input.dispatcher) {
      init.dispatcher = input.dispatcher;
    }
    response = await fetch(input.url, init);
  } catch (error) {
    if (controller.signal.aborted) {
      throw timeoutFailure("response_headers", header.bound, header.ms, input.onTimeout);
    }
    if (isConnectTimeout(error)) {
      throw timeoutFailure("connect", "phase", transport.connectTimeoutMs, input.onTimeout);
    }
    throw new GatewayFailure("upstream_transport", 502, "upstream transport failure");
  } finally {
    clearTimeout(headerTimer);
  }
  if (!response.ok) {
    const errorBudget = phaseBudget(transport.bufferedBodyTimeoutMs, deadlineAt, now());
    const text = await readErrorBody(response, transport.maxErrorBytes, errorBudget.ms);
    throw classifyUpstream(response.status, text);
  }
  if (!input.stream || response.body === null) {
    const bodyBudget = phaseBudget(transport.bufferedBodyTimeoutMs, deadlineAt, now());
    if (bodyBudget.ms <= 0) {
      throw timeoutFailure("overall", "walk_budget", 0, input.onTimeout);
    }
    const bytes = await readLimitedBytes(
      response,
      transport.maxResponseBytes,
      bodyBudget,
      input.onTimeout,
    );
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
  const stream = relayStream(response.body, transport, input.route, usage, (reason) => {
    input.onUsage(usage);
    input.onStreamDone?.(reason);
  }, input.onTimeout, input.onBeforeContentRateLimit);
  const headers = passHeaders(response.headers);
  if (!headers.has("content-type")) {
    headers.set("content-type", "text/event-stream");
  }
  return {
    response: new Response(stream, { status: response.status, headers }),
    usage,
  };
}

function withTransportDefaults(transport: TransportLimits): Required<TransportLimits> {
  return {
    responseHeaderTimeoutMs: transport.responseHeaderTimeoutMs,
    bufferedBodyTimeoutMs: transport.bufferedBodyTimeoutMs,
    streamIdleTimeoutMs: transport.streamIdleTimeoutMs,
    maxResponseBytes: transport.maxResponseBytes,
    maxErrorBytes: transport.maxErrorBytes ?? 64 * 1024,
    connectTimeoutMs: transport.connectTimeoutMs ?? 5_000,
    streamTerminalGraceMs: transport.streamTerminalGraceMs ?? 1_000,
    overallTimeoutMs: transport.overallTimeoutMs ?? 30_000,
  };
}

/** The phase's own bound, or what is left of the failover budget when that is tighter. */
export function phaseBudget(
  ownMs: number,
  deadlineAt: number,
  now: number,
): { ms: number; bound: "phase" | "walk_budget" } {
  const remaining = deadlineAt - now;
  if (remaining < ownMs) {
    return { ms: Math.max(0, remaining), bound: "walk_budget" };
  }
  return { ms: ownMs, bound: "phase" };
}

function relayStream(
  upstream: ReadableStream<Uint8Array>,
  transport: Required<TransportLimits>,
  route: string,
  usage: UsageTokens,
  onDone: (reason: "end" | "cancel") => void,
  onTimeout?: (kind: string, bound: string) => void,
  onBeforeContentRateLimit?: () => Promise<Response | null>,
): ReadableStream<Uint8Array> {
  let reader = upstream.getReader();
  const decoder = new TextDecoder();
  let pending = "";
  let inflight: Promise<ReadableStreamReadResult<Uint8Array>> | null = null;
  let sawChunkAt = Date.now();
  let terminalAt: number | null = null;
  let done = false;
  let committed = onBeforeContentRateLimit === undefined;
  let delegated = false;
  const held: Uint8Array[] = [];
  let heldBytes = 0;
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
  const releaseHeld = (controller: ReadableStreamDefaultController<Uint8Array>) => {
    if (held.length === 0) {
      committed = true;
      return;
    }
    const merged = concatBytes(held, heldBytes);
    const text = new TextDecoder().decode(merged);
    if (terminalAt === null && sseTerminalSeen(route, pending + text)) {
      terminalAt = Date.now();
    }
    noteSseChunk(route, usage, pending + text);
    pending = tail(pending + text);
    controller.enqueue(merged);
    held.length = 0;
    heldBytes = 0;
    committed = true;
  };
  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      if (delegated) {
        const chunk = await reader.read();
        if (chunk.done) {
          controller.close();
          return;
        }
        controller.enqueue(chunk.value);
        return;
      }
      const value = await readWithIdle(reader, () => inflight, (next) => {
        inflight = next;
      }, transport.streamIdleTimeoutMs, sawChunkAt, controller, () => (
        terminalAt === null ? null : terminalAt + transport.streamTerminalGraceMs
      ), onTimeout);
      if (value === "keepalive") {
        return;
      }
      if (value === null) {
        if (!committed) {
          releaseHeld(controller);
        }
        finish("end");
        controller.close();
        return;
      }
      sawChunkAt = Date.now();
      if (!committed && onBeforeContentRateLimit) {
        held.push(value);
        heldBytes += value.length;
        const text = new TextDecoder().decode(concatBytes(held, heldBytes));
        const first = firstCompleteData(text);
        if (first === undefined && heldBytes < 64 * 1024) {
          return;
        }
        if (first && isRateLimitPayload(first)) {
          await reader.cancel().catch(() => undefined);
          inflight = null;
          const next = await onBeforeContentRateLimit();
          if (next?.body) {
            held.length = 0;
            heldBytes = 0;
            delegated = true;
            done = true;
            reader = next.body.getReader();
            const chunk = await reader.read();
            if (chunk.done) {
              controller.close();
              return;
            }
            controller.enqueue(chunk.value);
            return;
          }
          releaseHeld(controller);
          finish("end");
          controller.close();
          return;
        }
        releaseHeld(controller);
        return;
      }
      const buffered = pending + decoder.decode(value, { stream: true });
      if (terminalAt === null && sseTerminalSeen(route, buffered)) {
        terminalAt = Date.now();
      }
      noteSseChunk(route, usage, buffered);
      pending = tail(buffered);
      controller.enqueue(value);
    },
    cancel(reason) {
      if (!delegated) {
        finish("cancel");
      }
      return reader.cancel(reason);
    },
  });
}

/** The first complete SSE data payload, or undefined while the event is still open. */
function firstCompleteData(text: string): string | undefined {
  const parts = text.split(/\n\n|\r\n\r\n/);
  const ended = text.endsWith("\n\n") || text.endsWith("\r\n\r\n");
  const frames = (ended ? parts : parts.slice(0, -1)).filter((frame) => frame.length > 0);
  for (const frame of frames) {
    const data = frame
      .split(/\r?\n/)
      .filter((line) => line.startsWith("data:"))
      .map((line) => line.slice(5).replace(/^ /, ""))
      .join("\n");
    if (data.length > 0) {
      return data;
    }
  }
  return frames.length > 0 && ended ? "" : undefined;
}

/** Explicit provider rate-limit markers in one SSE JSON payload. */
export function isRateLimitPayload(data: string): boolean {
  let value: unknown;
  try {
    value = JSON.parse(data);
  } catch {
    return false;
  }
  if (!value || typeof value !== "object") {
    return false;
  }
  const record = value as Record<string, unknown>;
  const error = record["error"];
  const errorShaped = error !== undefined || record["type"] === "error";
  if (!errorShaped) {
    return false;
  }
  const statuses = [record["status"], error && typeof error === "object" ? (error as Record<string, unknown>)["status"] : undefined];
  if (statuses.some((status) => status === 429 || status === "429")) {
    return true;
  }
  const signals: unknown[] = [];
  if (error && typeof error === "object") {
    const body = error as Record<string, unknown>;
    signals.push(body["type"], body["code"]);
  }
  signals.push(record["type"], record["code"]);
  return signals.some((signal) => signal === 429 || signal === "429" || (typeof signal === "string" && signal.includes("rate_limit")));
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
  graceAt: () => number | null,
  onTimeout?: (kind: string, bound: string) => void,
): Promise<Uint8Array | null | "keepalive"> {
  let read = current();
  if (read === null) {
    read = reader.read();
    read.catch(() => undefined);
    setCurrent(read);
  }
  const started = Date.now();
  let lastKeepalive = sawChunkAt;
  for (;;) {
    const idleLeft = idleMs - (Date.now() - started);
    const terminalAt = graceAt();
    const graceLeft = terminalAt === null ? Number.POSITIVE_INFINITY : terminalAt - Date.now();
    const budget = Math.min(idleLeft, graceLeft);
    if (budget <= 0) {
      await reader.cancel().catch(() => undefined);
      if (terminalAt !== null) {
        return null;
      }
      throw timeoutFailure("stream_idle", "phase", idleMs, onTimeout);
    }
    const untilKeepalive = terminalAt === null ? 15_000 - (Date.now() - lastKeepalive) : Number.POSITIVE_INFINITY;
    const wait = Math.max(1, Math.min(budget, untilKeepalive));
    const result = await Promise.race([
      read.then((chunk) => ({ kind: "chunk" as const, chunk })),
      delay(wait).then(() => ({ kind: "wait" as const })),
    ]);
    if (result.kind === "chunk") {
      setCurrent(null);
      return result.chunk.done ? null : result.chunk.value;
    }
    if (terminalAt === null && Date.now() - lastKeepalive >= 15_000) {
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

function timeoutFailure(
  kind: "connect" | "response_headers" | "buffered_body" | "stream_idle" | "overall",
  bound: "phase" | "walk_budget",
  budgetMs: number,
  onTimeout?: (kind: string, bound: string) => void,
): GatewayFailure {
  onTimeout?.(kind, bound);
  const phase = {
    connect: "connecting to the provider",
    response_headers: "waiting for provider response headers",
    buffered_body: "reading the provider response body",
    stream_idle: "waiting for the next provider stream chunk",
    overall: "the request's failover budget",
  }[kind];
  const message = kind === "overall"
    ? "transport: the request's failover budget was spent before this attempt was dispatched"
    : bound === "walk_budget"
      ? `transport: ${phase} exceeded the ${budgetMs}ms left of the request's failover budget`
      : `transport: ${phase} exceeded its ${budgetMs}ms bound`;
  const error = new GatewayFailure("upstream_timeout", 504, message);
  error.timeoutKind = kind;
  error.timeoutBound = bound;
  return error;
}

function isConnectTimeout(error: unknown): boolean {
  if (!error || typeof error !== "object") {
    return false;
  }
  const cause = (error as { cause?: { code?: string } }).cause;
  return cause?.code === "UND_ERR_CONNECT_TIMEOUT";
}

function passHeaders(headers: Headers): Headers {
  const next = new Headers();
  const contentType = headers.get("content-type");
  if (contentType) {
    next.set("content-type", contentType);
  }
  return next;
}

/**
 * Best-effort provider error body. The status is already known, so a slow read
 * yields an empty message and an oversized body is cut at `limit`. Neither
 * case replaces the provider failure with a transport error.
 */
async function readErrorBody(response: Response, limit: number, timeoutMs: number): Promise<string> {
  if (response.body === null) {
    return "";
  }
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  const started = Date.now();
  const finish = () => new TextDecoder().decode(concatBytes(chunks, total));
  try {
    for (;;) {
      const remaining = timeoutMs - (Date.now() - started);
      if (remaining <= 0) {
        await reader.cancel().catch(() => undefined);
        return "";
      }
      const chunk = await Promise.race([
        reader.read(),
        delay(remaining).then(() => "timeout" as const),
      ]);
      if (chunk === "timeout") {
        await reader.cancel().catch(() => undefined);
        return "";
      }
      if (chunk.done) {
        return finish();
      }
      const room = limit - total;
      if (room <= 0) {
        await reader.cancel().catch(() => undefined);
        return finish();
      }
      const slice = chunk.value.length > room ? chunk.value.subarray(0, room) : chunk.value;
      chunks.push(slice);
      total += slice.length;
      if (total >= limit) {
        await reader.cancel().catch(() => undefined);
        return finish();
      }
    }
  } catch {
    return finish();
  }
}

function concatBytes(chunks: Uint8Array[], total: number): Uint8Array {
  const out = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.length;
  }
  return out;
}

async function readLimitedBytes(
  response: Response,
  limit: number,
  budget: { ms: number; bound: "phase" | "walk_budget" },
  onTimeout?: (kind: string, bound: string) => void,
): Promise<Uint8Array> {
  if (response.body === null) {
    return new Uint8Array();
  }
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  const started = Date.now();
  for (;;) {
    const remaining = budget.ms - (Date.now() - started);
    if (remaining <= 0) {
      await reader.cancel().catch(() => undefined);
      throw timeoutFailure("buffered_body", budget.bound, budget.ms, onTimeout);
    }
    const pending = reader.read();
    const chunk = await Promise.race([
      pending,
      delay(remaining).then(() => "timeout" as const),
    ]);
    if (chunk === "timeout") {
      pending.catch(() => undefined);
      await reader.cancel().catch(() => undefined);
      throw timeoutFailure("buffered_body", budget.bound, budget.ms, onTimeout);
    }
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
