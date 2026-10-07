/** Compare JSON key order and SSE JSON spacing without rounding large numbers.
 * Event order, framing, error messages, and all other bytes remain significant.
 */
export function samePayload(left: Buffer, right: Buffer): boolean {
  if (left.equals(right)) return true;
  const a = left.toString("utf8");
  const b = right.toString("utf8");
  if (/^\s*[\[{]/.test(a)) {
    try {
      return canonicalJson(a) === canonicalJson(b);
    } catch {
      return false;
    }
  }
  return canonicalSse(a) === canonicalSse(b);
}

class JsonNumber {
  readonly source: string;
  constructor(source: string) { this.source = source; }
}

function canonicalJson(text: string): string {
  // Node 22's reviver source preserves digits that JSON.parse would round.
  const value = JSON.parse(text, (_key, value, context) =>
    typeof value === "number" ? new JsonNumber(context.source) : value);
  return canonical(value);
}

function canonical(value: unknown): string {
  if (value instanceof JsonNumber) return `number:${value.source}`;
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value !== null && typeof value === "object") {
    const record = value as Record<string, unknown>;
    return `{${Object.keys(record).sort().map((key) => `${JSON.stringify(key)}:${canonical(record[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

function canonicalSse(text: string): string {
  return text.split("\n\n").map((frame) => frame.split("\n").map((line) => {
    if (!line.startsWith("data:")) return line;
    const data = line.slice(5).replace(/^ /, "");
    if (data === "[DONE]" || data.length === 0) return line;
    try {
      return `data: ${canonicalJson(data)}`;
    } catch {
      return line;
    }
  }).join("\n")).join("\n\n");
}
