import type { RequestBody } from "@axond/sdk";

import { GatewayFailure, badRequest } from "./errors.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });

/**
 * Holds the original request bytes. Parsing does not replace them. `setModel`
 * splices the top-level model string. `setJson` replaces the body.
 */
export class ByteRequestBody implements RequestBody {
  private loaded: Uint8Array | null = null;
  private model: string | null = null;
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
    if (this.replaced) {
      return this.replacement as T;
    }
    const bytes = this.model === null ? await this.bytes() : this.outgoing();
    try {
      return JSON.parse(decoder.decode(bytes)) as T;
    } catch {
      throw badRequest("malformed json");
    }
  }

  setModel(model: string): void {
    this.model = model;
  }

  setJson(value: unknown): void {
    this.replaced = true;
    this.replacement = value;
  }

  /** Bytes to send upstream. */
  outgoing(): Uint8Array {
    if (this.replaced) {
      return encoder.encode(JSON.stringify(this.replacement));
    }
    if (this.loaded === null) {
      throw badRequest("request body was not read");
    }
    if (this.model === null) {
      return this.loaded;
    }
    return rewriteTopLevelModel(this.loaded, this.model);
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
