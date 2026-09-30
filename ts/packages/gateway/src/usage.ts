import type { UsageRecord, UsageTokens } from "@axond/sdk";

export function emptyUsage(): UsageTokens {
  return {
    inputTokens: 0n,
    outputTokens: 0n,
    reasoningTokens: 0n,
    cacheReadTokens: 0n,
    cacheWriteTokens: 0n,
  };
}

export function usageFromJson(route: string, value: unknown): UsageTokens {
  if (!value || typeof value !== "object") {
    return emptyUsage();
  }
  const record = value as Record<string, unknown>;
  const block = usageBlock(route, record);
  if (!block) {
    return emptyUsage();
  }
  if (route === "messages") {
    return {
      inputTokens: asBig(block["input_tokens"]),
      outputTokens: asBig(block["output_tokens"]),
      reasoningTokens: asBig(block["reasoning_tokens"]),
      cacheReadTokens: asBig(block["cache_read_input_tokens"]),
      cacheWriteTokens: asBig(block["cache_creation_input_tokens"]),
    };
  }
  const parsed = openaiUsage(block);
  if (route === "embeddings") {
    parsed.outputTokens = 0n;
    parsed.reasoningTokens = 0n;
  }
  return parsed;
}

/** OpenAI reports cached input inside the prompt total. Split it out so it is billed once. */
function openaiUsage(tokens: Record<string, unknown>): UsageTokens {
  const cached = pointer(tokens, ["prompt_tokens_details", "cached_tokens"])
    ?? pointer(tokens, ["input_tokens_details", "cached_tokens"]);
  const prompt = asBig(tokens["prompt_tokens"] ?? tokens["input_tokens"]);
  const cacheRead = cached ?? asBig(tokens["cache_read_input_tokens"]);
  const inputTokens = cached === null ? prompt : prompt > cached ? prompt - cached : 0n;
  return {
    inputTokens,
    outputTokens: asBig(tokens["completion_tokens"] ?? tokens["output_tokens"]),
    reasoningTokens:
      pointer(tokens, ["completion_tokens_details", "reasoning_tokens"])
      ?? pointer(tokens, ["output_tokens_details", "reasoning_tokens"])
      ?? 0n,
    cacheReadTokens: cacheRead,
    cacheWriteTokens: asBig(tokens["cache_creation_input_tokens"]),
  };
}

function usageBlock(route: string, record: Record<string, unknown>): Record<string, unknown> | null {
  if (route === "responses") {
    const response = record["response"];
    if (response && typeof response === "object") {
      const nested = (response as Record<string, unknown>)["usage"];
      if (nested && typeof nested === "object") {
        return nested as Record<string, unknown>;
      }
    }
  }
  const block = record["usage"];
  if (!block || typeof block !== "object") {
    return null;
  }
  return block as Record<string, unknown>;
}

function pointer(record: Record<string, unknown>, path: readonly string[]): bigint | null {
  let current: unknown = record;
  for (const key of path) {
    if (!current || typeof current !== "object") {
      return null;
    }
    current = (current as Record<string, unknown>)[key];
  }
  if (current === undefined || current === null) {
    return null;
  }
  return asBig(current);
}

/**
 * A complete SSE frame is the provider's semantic end of the answer.
 * Chat ends at a data-only `[DONE]`. Responses ends at `response.completed`
 * with `status=completed`. Messages ends at `message_stop`.
 */
export function sseTerminalSeen(route: string, text: string): boolean {
  const parts = text.split(/\n\n|\r\n\r\n/);
  const ended = text.endsWith("\n\n") || text.endsWith("\r\n\r\n");
  const frames = (ended ? parts : parts.slice(0, -1)).filter((frame) => frame.length > 0);
  return frames.some((frame) => frameIsTerminal(route, frame));
}

function frameIsTerminal(route: string, frame: string): boolean {
  let event: string | null = null;
  const dataLines: string[] = [];
  for (const line of frame.split(/\r?\n/)) {
    if (line.startsWith("event:")) {
      event = line.slice(6).replace(/^ /, "");
    } else if (line.startsWith("data:")) {
      dataLines.push(line.slice(5).replace(/^ /, ""));
    }
  }
  const data = dataLines.join("\n");
  if (data.trim() === "[DONE]") {
    return route !== "responses" || event === null;
  }
  if (route === "responses") {
    if (event !== null && event !== "response.completed") {
      return false;
    }
    try {
      const parsed = JSON.parse(data) as { type?: string; response?: { status?: string } };
      return parsed.type === "response.completed" && parsed.response?.status === "completed";
    } catch {
      return false;
    }
  }
  if (route === "messages") {
    if (event !== "message_stop") {
      return false;
    }
    try {
      return (JSON.parse(data) as { type?: string }).type === "message_stop";
    } catch {
      return false;
    }
  }
  return false;
}

/** Fold usage out of an SSE stream without modifying the bytes the caller sees. */
export function noteSseChunk(route: string, usage: UsageTokens, text: string): void {
  for (const frame of text.split(/\n\n|\r\n\r\n/)) {
    const data = frame
      .split(/\r?\n/)
      .filter((line) => line.startsWith("data:"))
      .map((line) => line.slice(5).replace(/^ /, ""))
      .join("\n");
    if (data.length === 0 || data === "[DONE]") {
      continue;
    }
    let parsed: unknown;
    try {
      parsed = JSON.parse(data);
    } catch {
      continue;
    }
    if (!parsed || typeof parsed !== "object") {
      continue;
    }
    const record = parsed as Record<string, unknown>;
    if (route === "messages") {
      const type = record["type"];
      if (type === "message_start" && record["message"] && typeof record["message"] === "object") {
        const message = record["message"] as Record<string, unknown>;
        const block = message["usage"];
        if (block && typeof block === "object" && !Array.isArray(block)) {
          mergeAnthropicUsage(usage, block as Record<string, unknown>);
        }
      } else if (type === "message_delta") {
        const block = record["usage"];
        if (block && typeof block === "object" && !Array.isArray(block)) {
          mergeAnthropicUsage(usage, block as Record<string, unknown>);
        }
      }
      continue;
    }
    if (usageBlock(route, record)) {
      assignUsage(usage, usageFromJson(route, record));
    }
  }
}

/**
 * Characters of generated text in one SSE buffer. Only complete frames count.
 * OpenAI chat deltas, Anthropic text deltas, and Responses string deltas contribute.
 * Anything else contributes nothing.
 */
export function relayedTextChars(text: string): number {
  const parts = text.split(/\n\n|\r\n\r\n/);
  const ended = text.endsWith("\n\n") || text.endsWith("\r\n\r\n");
  const frames = (ended ? parts : parts.slice(0, -1)).filter((frame) => frame.length > 0);
  let chars = 0;
  for (const frame of frames) {
    const data = frame
      .split(/\r?\n/)
      .filter((line) => line.startsWith("data:"))
      .map((line) => line.slice(5).replace(/^ /, ""))
      .join("\n");
    if (data.length === 0 || data === "[DONE]") {
      continue;
    }
    try {
      chars += relayedTextLen(JSON.parse(data));
    } catch {
      continue;
    }
  }
  return chars;
}

/**
 * When the provider never reported usage, a stream that relayed text is charged
 * for the admission estimate of the prompt and one token per four observed characters.
 * Provider token counts win when any of them is non-zero.
 */
export function applyObservedCharge(usage: UsageTokens, chars: number, estimatedInput: number): void {
  if (chars <= 0) {
    return;
  }
  if (
    usage.inputTokens > 0n
    || usage.outputTokens > 0n
    || usage.cacheReadTokens > 0n
    || usage.cacheWriteTokens > 0n
  ) {
    return;
  }
  usage.inputTokens = BigInt(Math.max(0, Math.floor(estimatedInput)));
  usage.outputTokens = BigInt(Math.ceil(chars / 4));
}

function relayedTextLen(data: unknown): number {
  if (!data || typeof data !== "object") {
    return 0;
  }
  const record = data as Record<string, unknown>;
  let chars = 0;
  const choices = record["choices"];
  if (Array.isArray(choices)) {
    for (const choice of choices) {
      chars += textChars(nestedString(choice, ["delta", "content"]));
      chars += textChars(nestedString(choice, ["delta", "reasoning_content"]));
    }
  }
  chars += textChars(nestedString(record, ["delta", "text"]));
  chars += textChars(nestedString(record, ["content_block", "text"]));
  chars += textChars(nestedString(record, ["delta", "partial_json"]));
  chars += textChars(typeof record["delta"] === "string" ? record["delta"] : "");
  return chars;
}

function nestedString(value: unknown, path: readonly string[]): string {
  let current = value;
  for (const key of path) {
    if (!current || typeof current !== "object") {
      return "";
    }
    current = (current as Record<string, unknown>)[key];
  }
  return typeof current === "string" ? current : "";
}

function textChars(text: string): number {
  return [...text].length;
}

/** The stdout usage event. `trace_id` is omitted when the request had no inbound trace. */
export function usageEvent(record: UsageRecord): Record<string, unknown> {
  const line: Record<string, unknown> = {
    schema_version: record.schemaVersion,
    request_id: record.requestId,
    ...(record.traceId ? { trace_id: record.traceId } : {}),
    namespace: record.namespace,
    period: record.period,
    subject: record.subject,
    signer_kid: record.signerKid,
    model: record.model,
    target_provider: record.targetProvider,
    target_model: record.targetModel,
    credential_source: record.credentialSource,
    credential_id: record.credentialId,
    status: record.status,
    input_tokens: record.inputTokens.toString(),
    cache_read_tokens: record.cacheReadTokens.toString(),
    cache_write_tokens: record.cacheWriteTokens.toString(),
    output_tokens: record.outputTokens.toString(),
    cost_microdollars: record.costMicrodollars?.toString() ?? null,
    catalog_version: record.catalogVersion,
    price_book: record.priceBook,
    price_book_checksum: record.priceBookChecksum,
    price_catalog: record.priceCatalog,
    latency_ms: record.latencyMs,
    attempts: record.attempts,
  };
  return line;
}

export function assignUsage(target: UsageTokens, next: UsageTokens): void {
  target.inputTokens = next.inputTokens;
  target.outputTokens = next.outputTokens;
  target.reasoningTokens = next.reasoningTokens;
  target.cacheReadTokens = next.cacheReadTokens;
  target.cacheWriteTokens = next.cacheWriteTokens;
}

/**
 * Anthropic reports split usage: input and cache on `message_start`, output on
 * `message_delta`. A key that is present replaces that counter, including zero.
 * A key that is absent leaves the earlier value in place.
 */
function mergeAnthropicUsage(target: UsageTokens, block: Record<string, unknown>): void {
  if ("input_tokens" in block) {
    target.inputTokens = asBig(block["input_tokens"]);
  }
  if ("output_tokens" in block) {
    target.outputTokens = asBig(block["output_tokens"]);
  }
  if ("reasoning_tokens" in block) {
    target.reasoningTokens = asBig(block["reasoning_tokens"]);
  }
  if ("cache_read_input_tokens" in block) {
    target.cacheReadTokens = asBig(block["cache_read_input_tokens"]);
  }
  if ("cache_creation_input_tokens" in block) {
    target.cacheWriteTokens = asBig(block["cache_creation_input_tokens"]);
  }
}

function asBig(value: unknown): bigint {
  if (typeof value === "bigint") {
    return value;
  }
  if (typeof value === "number" && Number.isFinite(value) && value >= 0) {
    return BigInt(Math.trunc(value));
  }
  if (typeof value === "string" && /^[0-9]+$/.test(value)) {
    return BigInt(value);
  }
  return 0n;
}
