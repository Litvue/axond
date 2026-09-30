import type { CredentialConfig, ProviderConfig, TransportLimits } from "@axond/sdk";

import { GatewayFailure } from "./errors.ts";
import { createNativeMessagesSequence } from "./native-messages.ts";
import { createResponsesSequence } from "./responses-sequence.ts";
import { applyObservedCharge, assignUsage, emptyUsage, noteSseChunk, relayedTextChars, sseTerminalSeen, usageFromJson } from "./usage.ts";
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
 * How many targets one request may open. Responses is always one target.
 * The number does not limit credentials inside a target.
 */
export function targetAttemptCap(pinned: boolean, configured: number | undefined): number {
  if (pinned) {
    return 1;
  }
  return configured ?? 3;
}

/** Credentials this request will call, and the parked ones it will skip. */
export interface CredentialWalk {
  attempts: CredentialConfig[];
  parked: CredentialConfig[];
}

/**
 * One request's credential walk. A pinned route returns the first credential
 * and does not read or advance health. Otherwise the rotation cursor moves
 * once, a cooldown-elapsed credential is taken as the single half-open probe,
 * and parked credentials are skipped unless every key is parked. A key forced
 * through because the whole pool is parked is an attempt, not a skip.
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
  return planCredentialWalk(credentials, pools, namespace, provider, fallbackNamespace, pinned, now, policy).attempts;
}

export function planCredentialWalk(
  credentials: readonly CredentialConfig[],
  pools: Map<string, CredentialPool>,
  namespace: string,
  provider: string,
  fallbackNamespace: string | null,
  pinned: boolean,
  now: number,
  policy: CredentialPoolPolicy,
): CredentialWalk {
  const pool = credentialPool(credentials, namespace, provider, fallbackNamespace);
  if (pinned) {
    return { attempts: [pool[0]!], parked: [] };
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
    return { attempts: [probe, ...healthy], parked };
  }
  if (healthy.length > 0) {
    return { attempts: healthy, parked };
  }
  return { attempts: [parked[0]!], parked: parked.slice(1) };
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

/** A provider may echo the credential it rejected. Replace it before classification. */
function redactUpstreamCredential(body: string, headers: Headers): string {
  const secrets: string[] = [];
  const authorization = headers.get("authorization");
  if (authorization) {
    const bearer = /^Bearer\s+(\S+)/i.exec(authorization);
    if (bearer?.[1]) {
      secrets.push(bearer[1]);
    }
  }
  const apiKey = headers.get("x-api-key");
  if (apiKey && apiKey.length > 0) {
    secrets.push(apiKey);
  }
  let text = body;
  for (const secret of secrets) {
    text = text.split(secret).join("[REDACTED]");
  }
  return text;
}

/** Longest provider diagnostic kept, before the truncation marker. */
const MAX_DIAGNOSTIC_BYTES = 4096;

/** Appended when a diagnostic hit the byte bound, so a short message stays distinct. */
const DIAGNOSTIC_TRUNCATION_MARKER = "… [truncated]";

const CONTEXT_LIMIT_SIGNALS = [
  "context_length_exceeded",
  "context length",
  "context window",
  "prompt is too long",
  "prompt too long",
  "maximum number of tokens",
  "too many tokens",
  "maximum prompt length",
];

/** `/error/message`, then a top-level `message`, then the body itself. */
function extractUpstreamMessage(body: string): string {
  try {
    const parsed = JSON.parse(body) as unknown;
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      const record = parsed as { error?: unknown; message?: unknown };
      const error = record.error;
      if (error && typeof error === "object" && !Array.isArray(error)) {
        const nested = (error as { message?: unknown }).message;
        if (typeof nested === "string") {
          return nested;
        }
      }
      if (typeof record.message === "string") {
        return record.message;
      }
    }
  } catch {
    // The body itself is the diagnostic.
  }
  return body;
}

function isContextLengthError(text: string): boolean {
  const lower = text.toLowerCase();
  return CONTEXT_LIMIT_SIGNALS.some((signal) => lower.includes(signal));
}

/** Cut on a UTF-8 boundary and mark the cut. A short diagnostic is unchanged. */
function boundDiagnostic(message: string): string {
  const bytes = new TextEncoder().encode(message);
  if (bytes.length <= MAX_DIAGNOSTIC_BYTES) {
    return message;
  }
  let cut = MAX_DIAGNOSTIC_BYTES;
  while (cut > 0 && (bytes[cut]! & 0xc0) === 0x80) {
    cut -= 1;
  }
  return new TextDecoder().decode(bytes.subarray(0, cut)) + DIAGNOSTIC_TRUNCATION_MARKER;
}

export function classifyUpstream(status: number, body: string): GatewayFailure {
  const extracted = extractUpstreamMessage(body);
  const message = extracted.length === 0 ? "upstream request failed" : boundDiagnostic(extracted);
  if (isContextLengthError(body) || isContextLengthError(extracted)) {
    const httpStatus = status === 401 || status === 403 ? 502 : 400;
    return upstreamFailure("context_window_exceeded", httpStatus, message, false, status);
  }
  if (status === 401 || status === 403) {
    return upstreamFailure("invalid_request", 502, message, false, status);
  }
  if (status === 404) {
    return upstreamFailure("model_unavailable", 502, message, false, status);
  }
  if (status === 429) {
    return upstreamFailure("provider_dependency_failed", 502, message, true, status);
  }
  if (status >= 500) {
    return upstreamFailure("provider_dependency_failed", 502, message, false, status);
  }
  return upstreamFailure("invalid_request", 400, message, false, status);
}

function upstreamFailure(
  type: string,
  status: number,
  message: string,
  rateLimited: boolean,
  upstreamStatus: number,
): GatewayFailure {
  const error = new GatewayFailure(type, status, message, rateLimited);
  error.upstreamStatus = upstreamStatus;
  return error;
}

export async function callUpstream(input: {
  url: string;
  headers: Headers;
  body: Uint8Array;
  transport: TransportLimits;
  stream: boolean;
  route: string;
  onUsage: (usage: UsageTokens) => void;
  onStreamDone?: (reason: "end" | "cancel" | "fail") => void;
  /** Shared failover deadline. Absent starts a budget of `overallTimeoutMs` at this call. */
  deadlineAt?: number;
  now?: () => number;
  /** Node client that enforces the connect bound. Absent means the runtime owns connect. */
  dispatcher?: object;
  onTimeout?: (kind: string, bound: string) => void;
  /** An open stream hit its duration or byte cap before a terminal event. */
  onStreamLimit?: (limit: "duration" | "bytes") => void;
  /**
   * A byte-faithful body stayed open after its terminal event until this bound.
   * The charge stays `ok`.
   */
  onTerminalRemain?: (bound: "grace" | "duration") => void;
  /**
   * The socket failed. `phase` is `request` before headers, `stream` while
   * relaying, or `closing` after a terminal event. The reason is a class, not
   * the runtime message.
   */
  onTransport?: (
    phase: "request" | "stream" | "closing",
    reason: TransportFailureReason,
    committed?: boolean,
  ) => void;
  /**
   * OpenAI chat only. A rate-limit event before any byte is released asks for
   * another upstream. Null keeps the held event and ends the stream.
   */
  onBeforeContentRateLimit?: () => Promise<Response | null>;
  /**
   * A rate-limit SSE event on this stream, before its terminal frame.
   * The bytes stay on the wire. The caller records the credential failure.
   */
  onCredentialRateLimit?: () => void;
  /** Prompt-token estimate used when a stream ends before the provider reports usage. */
  estimatedInputTokens?: number;
  /** Total stream lifetime. `null` disables. Absent uses one hour. */
  maxStreamDurationMs?: number | null;
  /** Upstream bytes one stream may relay. `null` disables. Absent uses 64 MiB. */
  maxStreamBytes?: number | null;
  /**
   * First provider byte released toward the client, measured from `clockStartedMs`.
   * A held rate-limit frame does not count: those bytes are not released.
   */
  onDownstreamFirstToken?: (elapsedMs: number) => void;
  /** First decoded SSE data event, including one that is later rotated away. */
  onUpstreamFirstToken?: (elapsedMs: number) => void;
  /** Start of the attempt that opened this stream. Absent starts at this call. */
  clockStartedMs?: number;
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
      redirect: "manual",
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
    input.onTransport?.("request", transportFailureReason(error));
    throw new GatewayFailure("upstream_transport", 502, "upstream transport failure");
  } finally {
    clearTimeout(headerTimer);
  }
  if (!response.ok) {
    const errorBudget = phaseBudget(transport.bufferedBodyTimeoutMs, deadlineAt, now());
    const text = redactUpstreamCredential(
      await readErrorBody(response, transport.maxErrorBytes, errorBudget.ms),
      input.headers,
    );
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
  const stream = relayStream(
    response.body,
    transport,
    input.route,
    usage,
    (reason) => {
      input.onUsage(usage);
      input.onStreamDone?.(reason);
    },
    input.onTimeout,
    input.onStreamLimit,
    input.onTransport,
    input.onBeforeContentRateLimit,
    input.onCredentialRateLimit,
    input.estimatedInputTokens ?? 0,
    input.maxStreamDurationMs === undefined ? 3_600_000 : input.maxStreamDurationMs,
    input.maxStreamBytes === undefined ? 64 * 1024 * 1024 : input.maxStreamBytes,
    input.onDownstreamFirstToken,
    input.onUpstreamFirstToken,
    input.clockStartedMs ?? now(),
    input.onTerminalRemain,
  );
  const headers = passHeaders(response.headers);
  headers.set("content-type", "text/event-stream");
  headers.set("cache-control", "no-cache");
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
    maxAttempts: transport.maxAttempts ?? 3,
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
  onDone: (reason: "end" | "cancel" | "fail") => void,
  onTimeout?: (kind: string, bound: string) => void,
  onStreamLimit?: (limit: "duration" | "bytes") => void,
  onTransport?: (
    phase: "request" | "stream" | "closing",
    reason: TransportFailureReason,
    committed?: boolean,
  ) => void,
  onBeforeContentRateLimit?: () => Promise<Response | null>,
  onCredentialRateLimit?: () => void,
  estimatedInputTokens = 0,
  maxStreamDurationMs: number | null = 3_600_000,
  maxStreamBytes: number | null = 64 * 1024 * 1024,
  onDownstreamFirstToken?: (elapsedMs: number) => void,
  onUpstreamFirstToken?: (elapsedMs: number) => void,
  clockStartedMs: number = Date.now(),
  onTerminalRemain?: (bound: "grace" | "duration") => void,
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
  let relayedBytes = 0;
  const startedAt = Date.now();
  const durationAt = maxStreamDurationMs !== null && maxStreamDurationMs > 0
    ? startedAt + maxStreamDurationMs
    : null;
  const byteLimit = maxStreamBytes !== null && maxStreamBytes > 0 ? maxStreamBytes : null;
  let unscanned = "";
  const sequence = route === "messages"
    ? createNativeMessagesSequence(isRateLimitPayload)
    : route === "responses"
      ? createResponsesSequence(isRateLimitPayload)
      : null;
  let rateLimitNoted = false;
  let stopRateLimitScan = false;
  let observedChars = 0;
  let sawDownstream = false;
  let sawUpstream = false;
  const markDownstream = () => {
    if (sawDownstream) {
      return;
    }
    sawDownstream = true;
    onDownstreamFirstToken?.(Math.max(0, Date.now() - clockStartedMs));
  };
  const markUpstream = (text: string) => {
    if (sawUpstream) {
      return;
    }
    const data = firstCompleteData(text);
    if (data === undefined || data.length === 0 || data === "[DONE]") {
      return;
    }
    sawUpstream = true;
    onUpstreamFirstToken?.(Math.max(0, Date.now() - clockStartedMs));
  };
  const note = (text: string) => {
    noteSseChunk(route, usage, text);
    observedChars += relayedTextChars(text);
  };
  const consider = (text: string) => {
    if (!onCredentialRateLimit || rateLimitNoted || stopRateLimitScan || text.length === 0) {
      return;
    }
    unscanned += text;
    const parts = unscanned.split(/\n\n|\r\n\r\n/);
    const ended = unscanned.endsWith("\n\n") || unscanned.endsWith("\r\n\r\n");
    const complete = (ended ? parts : parts.slice(0, -1)).filter((frame) => frame.length > 0);
    unscanned = ended ? "" : parts.at(-1) ?? "";
    for (const frame of complete) {
      if (sseTerminalSeen(route, `${frame}\n\n`)) {
        stopRateLimitScan = true;
        return;
      }
      const data = firstCompleteData(`${frame}\n\n`);
      if (data && isRateLimitPayload(data)) {
        rateLimitNoted = true;
        onCredentialRateLimit();
        return;
      }
    }
  };
  const finish = (reason: "end" | "cancel" | "fail") => {
    if (done) {
      return;
    }
    done = true;
    if (pending.length > 0) {
      note(pending);
      pending = "";
    }
    applyObservedCharge(usage, observedChars, estimatedInputTokens);
    onDone(reason);
  };
  const releaseHeld = (controller: ReadableStreamDefaultController<Uint8Array>) => {
    if (held.length === 0) {
      committed = true;
      return "";
    }
    const merged = concatBytes(held, heldBytes);
    const text = new TextDecoder().decode(merged);
    if (terminalAt === null && sseTerminalSeen(route, pending + text)) {
      terminalAt = Date.now();
    }
    note(pending + text);
    pending = tail(pending + text);
    markDownstream();
    controller.enqueue(merged);
    held.length = 0;
    heldBytes = 0;
    committed = true;
    return text;
  };
  const fits = (chunk: Uint8Array): boolean => {
    if (byteLimit !== null && relayedBytes + chunk.length > byteLimit) {
      return false;
    }
    relayedBytes += chunk.length;
    return true;
  };
  const failBound = (controller: ReadableStreamDefaultController<Uint8Array>, message: string) => {
    if (!committed) {
      releaseHeld(controller);
    }
    controller.enqueue(streamFailureFrame(route, message));
    finish("fail");
    controller.close();
  };
  return new ReadableStream<Uint8Array>({
    async pull(controller) {
      if (delegated) {
        const chunk = await reader.read();
        if (chunk.done) {
          controller.close();
          return;
        }
        markDownstream();
        controller.enqueue(chunk.value);
        return;
      }
      let value: Uint8Array | null | "keepalive" | "duration";
      try {
        value = await readWithIdle(reader, () => inflight, (next) => {
          inflight = next;
        }, transport.streamIdleTimeoutMs, sawChunkAt, controller, () => (
          terminalAt === null ? null : terminalAt + transport.streamTerminalGraceMs
        ), durationAt, onTimeout, onTerminalRemain);
      } catch (error) {
        if (terminalAt !== null) {
          if (!(error instanceof GatewayFailure)) {
            onTransport?.("closing", transportFailureReason(error));
          }
          await reader.cancel().catch(() => undefined);
          finish("end");
          controller.close();
          return;
        }
        if (!(error instanceof GatewayFailure)) {
          onTransport?.("stream", transportFailureReason(error), committed);
        }
        const message = error instanceof GatewayFailure ? error.message : "upstream stream failed";
        failBound(controller, message);
        return;
      }
      if (value === "keepalive") {
        return;
      }
      if (value === "duration") {
        onStreamLimit?.("duration");
        failBound(controller, "stream exceeded the gateway's maximum stream duration");
        return;
      }
      if (value === null) {
        if (!committed) {
          releaseHeld(controller);
        }
        const incomplete = sequence?.finish();
        if (incomplete) {
          failBound(controller, incomplete);
          return;
        }
        finish("end");
        controller.close();
        return;
      }
      if (!fits(value)) {
        onStreamLimit?.("bytes");
        failBound(controller, "stream exceeded the gateway's maximum stream size");
        return;
      }
      sawChunkAt = Date.now();
      if (!committed && onBeforeContentRateLimit) {
        held.push(value);
        heldBytes += value.length;
        const text = new TextDecoder().decode(concatBytes(held, heldBytes));
        markUpstream(text);
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
            markDownstream();
            controller.enqueue(chunk.value);
            return;
          }
          releaseHeld(controller);
          finish("end");
          controller.close();
          return;
        }
        consider(releaseHeld(controller));
        return;
      }
      const piece = decoder.decode(value, { stream: true });
      const buffered = pending + piece;
      consider(piece);
      if (terminalAt === null && sseTerminalSeen(route, buffered)) {
        terminalAt = Date.now();
      }
      markUpstream(buffered);
      note(buffered);
      pending = tail(buffered);
      markDownstream();
      controller.enqueue(value);
      const sequenceError = sequence?.push(piece);
      if (sequenceError) {
        controller.enqueue(streamFailureFrame(route, sequenceError));
        finish("fail");
        controller.close();
        await reader.cancel().catch(() => undefined);
      }
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

/** A terminal SSE error on a response whose status is already 200. Chat also ends with [DONE]. */
function streamFailureFrame(route: string, message: string): Uint8Array {
  const payload = route === "messages"
    ? { type: "error", error: { type: "upstream_stream_error", message } }
    : route === "responses"
      ? { type: "error", code: "upstream_stream_error", message }
      : { error: { type: "upstream_stream_error", message } };
  let text = `event: error\ndata: ${JSON.stringify(payload)}\n\n`;
  if (route === "chat") {
    text += "data: [DONE]\n\n";
  }
  return new TextEncoder().encode(text);
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
  durationAt: number | null,
  onTimeout?: (kind: string, bound: string) => void,
  onTerminalRemain?: (bound: "grace" | "duration") => void,
): Promise<Uint8Array | null | "keepalive" | "duration"> {
  let read = current();
  if (read === null) {
    read = reader.read();
    read.catch(() => undefined);
    setCurrent(read);
  }
  const started = Date.now();
  let lastKeepalive = sawChunkAt;
  for (;;) {
    const now = Date.now();
    const idleLeft = idleMs - (now - started);
    const terminalDeadline = graceAt();
    const graceLeft = terminalDeadline === null ? Number.POSITIVE_INFINITY : terminalDeadline - now;
    const durationLeft = durationAt === null ? Number.POSITIVE_INFINITY : durationAt - now;
    if (terminalDeadline !== null && graceLeft <= 0 && graceLeft <= durationLeft) {
      await reader.cancel().catch(() => undefined);
      onTerminalRemain?.("grace");
      return null;
    }
    if (durationLeft <= 0 && terminalDeadline !== null) {
      await reader.cancel().catch(() => undefined);
      onTerminalRemain?.("duration");
      return null;
    }
    if (durationLeft <= 0 && durationLeft <= idleLeft && durationLeft <= graceLeft) {
      await reader.cancel().catch(() => undefined);
      return "duration";
    }
    const budget = Math.min(idleLeft, graceLeft, durationLeft);
    if (budget <= 0) {
      await reader.cancel().catch(() => undefined);
      if (terminalDeadline !== null) {
        return null;
      }
      throw timeoutFailure("stream_idle", "phase", idleMs, onTimeout);
    }
    const untilKeepalive = terminalDeadline === null ? 15_000 - (Date.now() - lastKeepalive) : Number.POSITIVE_INFINITY;
    const wait = Math.max(1, Math.min(budget, untilKeepalive));
    const result = await Promise.race([
      read.then((chunk) => ({ kind: "chunk" as const, chunk })),
      delay(wait).then(() => ({ kind: "wait" as const })),
    ]);
    if (result.kind === "chunk") {
      setCurrent(null);
      return result.chunk.done ? null : result.chunk.value;
    }
    if (terminalDeadline === null && Date.now() - lastKeepalive >= 15_000) {
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

export type TransportFailureReason = "dns" | "refused" | "reset" | "tls" | "other";

const TRANSPORT_REASON_BY_CODE: Record<string, TransportFailureReason> = {
  ENOTFOUND: "dns",
  EAI_AGAIN: "dns",
  EAI_NODATA: "dns",
  ECONNREFUSED: "refused",
  ConnectionRefused: "refused",
  ECONNRESET: "reset",
  EPIPE: "reset",
  ECONNABORTED: "reset",
  UND_ERR_SOCKET: "reset",
  CERT_HAS_EXPIRED: "tls",
  UNABLE_TO_VERIFY_LEAF_SIGNATURE: "tls",
  DEPTH_ZERO_SELF_SIGNED_CERT: "tls",
  ERR_TLS_CERT_ALTNAME_INVALID: "tls",
  UNABLE_TO_GET_ISSUER_CERT: "tls",
  UNABLE_TO_GET_ISSUER_CERT_LOCALLY: "tls",
};

/** Classify a socket failure without returning the runtime's message or address. */
export function transportFailureReason(error: unknown): TransportFailureReason {
  const seen = new Set<unknown>();
  let current: unknown = error;
  while (current && typeof current === "object" && !seen.has(current)) {
    seen.add(current);
    const record = current as { code?: unknown; cause?: unknown; message?: unknown };
    if (typeof record.code === "string") {
      const mapped = TRANSPORT_REASON_BY_CODE[record.code];
      if (mapped) {
        return mapped;
      }
    }
    if (typeof record.message === "string") {
      const text = record.message.toLowerCase();
      if (text.includes("enotfound") || text.includes("getaddrinfo")) {
        return "dns";
      }
      if (text.includes("econnrefused")) {
        return "refused";
      }
      if (text.includes("econnreset") || text.includes("epipe") || text.includes("socket hang up")) {
        return "reset";
      }
      if (text.includes("certificate") || text.includes("ssl routines") || text.includes(" tls")) {
        return "tls";
      }
    }
    current = record.cause;
  }
  return "other";
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
