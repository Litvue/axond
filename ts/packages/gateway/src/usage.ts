import type { UsageTokens } from "@axond/sdk";

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
        const next = usageFromJson(route, { usage: message["usage"] });
        copyPresent(usage, next);
      } else if (type === "message_delta") {
        const next = usageFromJson(route, record);
        if (next.outputTokens > 0n) {
          usage.outputTokens = next.outputTokens;
        }
      }
      continue;
    }
    if (usageBlock(route, record)) {
      assignUsage(usage, usageFromJson(route, record));
    }
  }
}

export function assignUsage(target: UsageTokens, next: UsageTokens): void {
  target.inputTokens = next.inputTokens;
  target.outputTokens = next.outputTokens;
  target.reasoningTokens = next.reasoningTokens;
  target.cacheReadTokens = next.cacheReadTokens;
  target.cacheWriteTokens = next.cacheWriteTokens;
}

function copyPresent(target: UsageTokens, next: UsageTokens): void {
  if (next.inputTokens > 0n) {
    target.inputTokens = next.inputTokens;
  }
  if (next.outputTokens > 0n) {
    target.outputTokens = next.outputTokens;
  }
  if (next.reasoningTokens > 0n) {
    target.reasoningTokens = next.reasoningTokens;
  }
  if (next.cacheReadTokens > 0n) {
    target.cacheReadTokens = next.cacheReadTokens;
  }
  if (next.cacheWriteTokens > 0n) {
    target.cacheWriteTokens = next.cacheWriteTokens;
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
