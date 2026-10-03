import type { RequestBody } from "@axond/sdk";

import { GatewayFailure, badRequest } from "./errors.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });

/**
 * Holds the original request bytes. Parsing does not replace them. `setModel`
 * splices the top-level model string. `forceIncludeUsage` splices
 * `stream_options.include_usage` on an OpenAI chat stream. `setJson` replaces
 * the body.
 */
export class ByteRequestBody implements RequestBody {
  private loaded: Uint8Array | null = null;
  private model: string | null = null;
  private forceUsage = false;
  private replacement: unknown | undefined;
  private replaced = false;
  private readonly source: Request;

  constructor(source: Request) {
    this.source = source;
  }

  async raw(): Promise<Uint8Array> {
    return this.bytes();
  }

  async json<T = unknown>(): Promise<T> {
    if (this.replaced && !this.forceUsage) {
      return this.replacement as T;
    }
    if (this.replaced || this.model !== null || this.forceUsage) {
      if (!this.replaced) {
        await this.bytes();
      }
      try {
        return JSON.parse(decoder.decode(this.outgoing())) as T;
      } catch {
        throw badRequest("request body is not valid JSON");
      }
    }
    const bytes = await this.bytes();
    try {
      return JSON.parse(decoder.decode(bytes)) as T;
    } catch {
      throw badRequest("request body is not valid JSON");
    }
  }

  setModel(model: string): void {
    this.model = model;
  }

  /** OpenAI chat streams ask the provider for a usage frame. */
  forceIncludeUsage(): void {
    this.forceUsage = true;
  }

  setJson(value: unknown): void {
    this.replaced = true;
    this.replacement = value;
  }

  /** Bytes to send upstream. */
  outgoing(): Uint8Array {
    if (this.replaced) {
      const value = this.forceUsage ? withIncludeUsage(this.replacement) : this.replacement;
      return encoder.encode(JSON.stringify(value));
    }
    if (this.loaded === null) {
      throw badRequest("request body was not read");
    }
    let bytes = this.model === null ? this.loaded : rewriteTopLevelModel(this.loaded, this.model);
    if (this.forceUsage) {
      bytes = forceChatIncludeUsage(bytes);
    }
    return bytes;
  }

  private async bytes(): Promise<Uint8Array> {
    if (this.loaded === null) {
      this.loaded = new Uint8Array(await this.source.arrayBuffer());
    }
    return this.loaded;
  }
}

/**
 * Replace every top-level `"model"` string. Duplicate keys and unrelated
 * numbers, including integers above 2^53, stay byte-for-byte.
 */
export function rewriteTopLevelModel(bytes: Uint8Array, model: string): Uint8Array {
  let text: string;
  try {
    text = decoder.decode(bytes);
  } catch {
    throw badRequest("request body is not utf-8");
  }
  const replacement = JSON.stringify(model);
  let out = "";
  let index = 0;
  let depth = 0;
  let inString = false;
  let escape = false;
  while (index < text.length) {
    const char = text[index]!;
    if (inString) {
      out += char;
      if (escape) {
        escape = false;
      } else if (char === "\\") {
        escape = true;
      } else if (char === '"') {
        inString = false;
      }
      index += 1;
      continue;
    }
    if (char === '"') {
      const key = readString(text, index);
      if (depth === 1 && key.value === "model") {
        const after = skipSpace(text, key.end);
        if (text[after] === ":") {
          const valueAt = skipSpace(text, after + 1);
          if (text[valueAt] === '"') {
            const current = readString(text, valueAt);
            out += text.slice(index, valueAt);
            out += replacement;
            index = current.end;
            continue;
          }
        }
      }
      out += text.slice(index, key.end);
      index = key.end;
      continue;
    }
    if (char === "{" || char === "[") {
      depth += 1;
    } else if (char === "}" || char === "]") {
      depth -= 1;
    }
    out += char;
    index += 1;
  }
  return encoder.encode(out);
}

function readString(text: string, start: number): { value: string; end: number } {
  if (text[start] !== '"') {
    throw badRequest("malformed json");
  }
  let index = start + 1;
  let value = "";
  while (index < text.length) {
    const char = text[index]!;
    if (char === "\\") {
      const next = text[index + 1];
      if (next === undefined) {
        throw badRequest("malformed json");
      }
      value += char + next;
      index += 2;
      continue;
    }
    if (char === '"') {
      return { value: JSON.parse(`"${value}"`) as string, end: index + 1 };
    }
    value += char;
    index += 1;
  }
  throw new GatewayFailure("bad_request", 400, "malformed json");
}

function skipSpace(text: string, index: number): number {
  while (index < text.length && " \n\r\t".includes(text[index]!)) {
    index += 1;
  }
  return index;
}

type ValueSpan = {
  kind: "string" | "object" | "array" | "literal" | "number";
  start: number;
  end: number;
};

type KeyHit = { key: string; value: ValueSpan };

/**
 * OpenAI chat streams need a usage frame. Set the last top-level
 * `stream_options.include_usage` to true when the last top-level `stream` is
 * true. Other bytes, including duplicate keys and integers above 2^53, stay.
 * A non-object `stream_options` is replaced. Responses and Messages do not
 * call this.
 */
export function forceChatIncludeUsage(bytes: Uint8Array): Uint8Array {
  let text: string;
  try {
    text = decoder.decode(bytes);
  } catch {
    throw badRequest("request body is not utf-8");
  }
  const keys = topLevelKeys(text);
  let stream: KeyHit | undefined;
  let options: KeyHit | undefined;
  for (const hit of keys) {
    if (hit.key === "stream") {
      stream = hit;
    } else if (hit.key === "stream_options") {
      options = hit;
    }
  }
  if (!stream || text.slice(stream.value.start, stream.value.end) !== "true") {
    return bytes;
  }
  if (!options) {
    const root = scanContainer(text, text.indexOf("{"));
    return encoder.encode(
      insertMember(text, root.start, root.end - 1, '"stream_options":{"include_usage":true}'),
    );
  }
  if (options.value.kind === "object") {
    return encoder.encode(setIncludeUsage(text, options.value.start, options.value.end));
  }
  const next =
    text.slice(0, options.value.start) + '{"include_usage":true}' + text.slice(options.value.end);
  return encoder.encode(next);
}

function withIncludeUsage(value: unknown): unknown {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    return value;
  }
  const record = value as Record<string, unknown>;
  if (record["stream"] !== true) {
    return value;
  }
  const options = record["stream_options"];
  if (options !== null && typeof options === "object" && !Array.isArray(options)) {
    return { ...record, stream_options: { ...(options as Record<string, unknown>), include_usage: true } };
  }
  return { ...record, stream_options: { include_usage: true } };
}

function topLevelKeys(text: string): KeyHit[] {
  const hits: KeyHit[] = [];
  let index = 0;
  let depth = 0;
  let inString = false;
  let escape = false;
  while (index < text.length) {
    const char = text[index]!;
    if (inString) {
      if (escape) {
        escape = false;
      } else if (char === "\\") {
        escape = true;
      } else if (char === '"') {
        inString = false;
      }
      index += 1;
      continue;
    }
    if (char === '"') {
      const key = readString(text, index);
      if (depth === 1) {
        const after = skipSpace(text, key.end);
        if (text[after] === ":") {
          const valueAt = skipSpace(text, after + 1);
          const value = readValue(text, valueAt);
          hits.push({ key: key.value, value });
          index = value.end;
          continue;
        }
      }
      index = key.end;
      continue;
    }
    if (char === "{" || char === "[") {
      depth += 1;
    } else if (char === "}" || char === "]") {
      depth -= 1;
    }
    index += 1;
  }
  return hits;
}

function readValue(text: string, start: number): ValueSpan {
  const char = text[start];
  if (char === '"') {
    return { kind: "string", start, end: readString(text, start).end };
  }
  if (char === "{" || char === "[") {
    return scanContainer(text, start);
  }
  if (text.startsWith("true", start)) {
    return { kind: "literal", start, end: start + 4 };
  }
  if (text.startsWith("false", start)) {
    return { kind: "literal", start, end: start + 5 };
  }
  if (text.startsWith("null", start)) {
    return { kind: "literal", start, end: start + 4 };
  }
  let end = start;
  while (end < text.length && !",}] \n\r\t".includes(text[end]!)) {
    end += 1;
  }
  if (end === start) {
    throw badRequest("malformed json");
  }
  return { kind: "number", start, end };
}

function scanContainer(text: string, start: number): ValueSpan {
  const kind = text[start] === "{" ? "object" : "array";
  let depth = 0;
  let inString = false;
  let escape = false;
  let index = start;
  while (index < text.length) {
    const char = text[index]!;
    if (inString) {
      if (escape) {
        escape = false;
      } else if (char === "\\") {
        escape = true;
      } else if (char === '"') {
        inString = false;
      }
      index += 1;
      continue;
    }
    if (char === '"') {
      inString = true;
      index += 1;
      continue;
    }
    if (char === "{" || char === "[") {
      depth += 1;
    } else if (char === "}" || char === "]") {
      depth -= 1;
      if (depth === 0) {
        return { kind, start, end: index + 1 };
      }
    }
    index += 1;
  }
  throw badRequest("malformed json");
}

function insertMember(text: string, openIndex: number, closeIndex: number, member: string): string {
  const interior = text.slice(openIndex + 1, closeIndex);
  const comma = interior.trim() === "" ? "" : ",";
  return text.slice(0, closeIndex) + comma + member + text.slice(closeIndex);
}

function setIncludeUsage(text: string, start: number, end: number): string {
  const inner = text.slice(start, end);
  let usage: KeyHit | undefined;
  for (const hit of topLevelKeys(inner)) {
    if (hit.key === "include_usage") {
      usage = hit;
    }
  }
  if (!usage) {
    const inserted = insertMember(inner, 0, inner.length - 1, '"include_usage":true');
    return text.slice(0, start) + inserted + text.slice(end);
  }
  if (inner.slice(usage.value.start, usage.value.end) === "true") {
    return text;
  }
  const next = inner.slice(0, usage.value.start) + "true" + inner.slice(usage.value.end);
  return text.slice(0, start) + next + text.slice(end);
}
