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
  const usage = emptyUsage();
  if (!value || typeof value !== "object") {
    return usage;
  }
  const record = value as Record<string, unknown>;
  const block = record["usage"];
  if (!block || typeof block !== "object") {
    return usage;
  }
  const tokens = block as Record<string, unknown>;
  if (route === "messages") {
    usage.inputTokens = asBig(tokens["input_tokens"]);
    usage.outputTokens = asBig(tokens["output_tokens"]);
    usage.cacheReadTokens = asBig(tokens["cache_read_input_tokens"]);
    usage.cacheWriteTokens = asBig(tokens["cache_creation_input_tokens"]);
    return usage;
  }
  usage.inputTokens = asBig(tokens["prompt_tokens"] ?? tokens["input_tokens"]);
  usage.outputTokens = asBig(tokens["completion_tokens"] ?? tokens["output_tokens"]);
  const completion = tokens["completion_tokens_details"];
  if (completion && typeof completion === "object") {
    usage.reasoningTokens = asBig((completion as Record<string, unknown>)["reasoning_tokens"]);
  }
  const prompt = tokens["prompt_tokens_details"];
  if (prompt && typeof prompt === "object") {
    usage.cacheReadTokens = asBig((prompt as Record<string, unknown>)["cached_tokens"]);
  }
  return usage;
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
    const next = usageFromJson(route, record);
    if (next.inputTokens > 0n || next.outputTokens > 0n) {
      copyPresent(usage, next);
    }
  }
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
