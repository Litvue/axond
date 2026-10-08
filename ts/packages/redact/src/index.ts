import { Hono } from "hono";
import { transformSseEvents, type AxondEnv, type AxondExtension } from "@axond/sdk";

import { compilePattern } from "./pattern.ts";

export interface RedactRule {
  pattern: string;
  replacement: string;
}

/**
 * Request rewrite plus a response SSE transform. Patterns are compiled with
 * the linear-time engine in `pattern.ts`. The SSE helper is the SDK's; this
 * package does not reimplement the decoder.
 */
export function redactExtension(rules: readonly RedactRule[]): AxondExtension {
  const compiled = rules.map((rule) => ({
    replace: compilePattern(rule.pattern),
    replacement: rule.replacement,
  }));
  const routes = new Hono<AxondEnv>();
  return {
    name: "redact",
    apiVersion: 1,
    stage: "pre-dispatch",
    trusted: true,
    routes,
    async middleware(c, next) {
      const axond = c.get("axond");
      if (axond.route === "chat" || axond.route === "messages" || axond.route === "embeddings" || axond.route === "responses") {
        const current = await axond.body.json<Record<string, unknown>>();
        const redacted = redactValue(current, compiled);
        if (JSON.stringify(redacted) !== JSON.stringify(current)) axond.body.setJson(redacted);
      }
      await next();
      if (!c.res.body) {
        return;
      }
      const contentType = c.res.headers.get("content-type") ?? "";
      if (!contentType.includes("text/event-stream")) {
        return;
      }
      const stream = transformSseEvents(c.res.body, (event) => {
        let data: string;
        try {
          data = JSON.stringify(redactValue(JSON.parse(event.data), compiled));
        } catch (error) {
          if (!(error instanceof SyntaxError)) throw error;
          data = apply(event.data, compiled);
        }
        if (data === event.data) {
          return event;
        }
        return { ...event, data };
      });
      c.res = new Response(stream, { status: c.res.status, headers: c.res.headers });
    },
  };
}

function apply(text: string, rules: { replace: (input: string) => Array<{ start: number; end: number }>; replacement: string }[]): string {
  let out = text;
  for (const rule of rules) {
    const hits = rule.replace(out);
    if (hits.length === 0) {
      continue;
    }
    let next = "";
    let cursor = 0;
    for (const hit of hits) {
      next += out.slice(cursor, hit.start) + rule.replacement;
      cursor = hit.end;
    }
    next += out.slice(cursor);
    out = next;
  }
  return out;
}

function redactValue(value: unknown, rules: Parameters<typeof apply>[1]): unknown {
  if (typeof value === "string") return apply(value, rules);
  if (Array.isArray(value)) return value.map((item) => redactValue(item, rules));
  if (value !== null && typeof value === "object") {
    return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, redactValue(item, rules)]));
  }
  return value;
}
