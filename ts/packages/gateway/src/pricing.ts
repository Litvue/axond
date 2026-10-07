import type { PriceRule, UsageTokens } from "@axond/sdk";

import { globMatch } from "./glob.ts";

const MILLION = 1_000_000n;

export function lookupPrice(rules: readonly PriceRule[], provider: string, model: string): PriceRule | null {
  for (const rule of rules) {
    if (rule.provider === provider && globMatch(rule.model, model)) {
      return rule;
    }
  }
  return null;
}

/** Integer microdollars. A partial microdollar is truncated. Reasoning is billed separately from output. */
export function costMicrodollars(rule: PriceRule, usage: UsageTokens): bigint {
  const billedOutput = usage.outputTokens > usage.reasoningTokens ? usage.outputTokens - usage.reasoningTokens : 0n;
  return (
    component(usage.inputTokens, rule.inputMicrodollarsPerMillion) +
    component(billedOutput, rule.outputMicrodollarsPerMillion) +
    component(usage.reasoningTokens, rule.reasoningMicrodollarsPerMillion ?? rule.outputMicrodollarsPerMillion) +
    component(usage.cacheReadTokens, rule.cacheReadMicrodollarsPerMillion ?? rule.inputMicrodollarsPerMillion) +
    component(usage.cacheWriteTokens, rule.cacheWriteMicrodollarsPerMillion ?? rule.inputMicrodollarsPerMillion)
  );
}

function component(tokens: bigint, rate: bigint): bigint {
  return (tokens * rate) / MILLION;
}
