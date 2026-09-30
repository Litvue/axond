import { badRequest } from "./errors.ts";

/**
 * The `namespaces` value from a credential-status query.
 *
 * Walks the raw query the way the Rust gateway does: `+` is a space, `%HH`
 * is one byte, and the result must be UTF-8. A second `namespaces` key is
 * refused. Other keys are ignored after they decode. An absent key is `null`;
 * a present empty value is `""`.
 */
export function parseCredentialQuery(rawQuery: string | null): string | null {
  let namespaces: string | null = null;
  for (const pair of (rawQuery ?? "").split("&")) {
    if (pair.length === 0) {
      continue;
    }
    const split = pair.indexOf("=");
    const rawKey = split === -1 ? pair : pair.slice(0, split);
    const rawValue = split === -1 ? "" : pair.slice(split + 1);
    const key = decodeQueryComponent(rawKey);
    const value = decodeQueryComponent(rawValue);
    if (key === "namespaces") {
      if (namespaces !== null) {
        throw badRequest("duplicate query parameter `namespaces`");
      }
      namespaces = value;
    }
  }
  return namespaces;
}

const QUERY_DESERIALIZE = "Failed to deserialize query string";
const U32_MAX = 4294967295n;

export type ManagementListQuery = { cursor: string | null; limit: number };

/**
 * `cursor` and `limit` from a namespace list query.
 *
 * Unknown keys are ignored. A repeated `cursor` or `limit` is refused before
 * the second value is read. `limit` is a Rust `u32`: digits only, and a value
 * above `4294967295` names the target type. The page size is then 1–1000.
 */
export function readListQuery(params: Iterable<[string, string]>): ManagementListQuery {
  let cursor: string | null = null;
  let sawCursor = false;
  let sawLimit = false;
  let limit: number | null = null;
  for (const [key, value] of params) {
    if (key === "cursor") {
      if (sawCursor) {
        throw badRequest(`${QUERY_DESERIALIZE}: duplicate field \`cursor\``);
      }
      sawCursor = true;
      cursor = value;
      continue;
    }
    if (key !== "limit") {
      continue;
    }
    if (sawLimit) {
      throw badRequest(`${QUERY_DESERIALIZE}: duplicate field \`limit\``);
    }
    sawLimit = true;
    limit = parseQueryU32(value);
  }
  const page = limit ?? 100;
  if (page < 1 || page > 1000) {
    throw badRequest("`limit` must be between 1 and 1000");
  }
  return { cursor, limit: page };
}

/**
 * The usage `period` query. A repeated key is a serde duplicate field.
 * An absent or empty value is still missing, for the caller to validate.
 */
export function readUsagePeriod(params: Iterable<[string, string]>): string | null {
  let period: string | null = null;
  let saw = false;
  for (const [key, value] of params) {
    if (key !== "period") {
      continue;
    }
    if (saw) {
      throw badRequest(`${QUERY_DESERIALIZE}: duplicate field \`period\``);
    }
    saw = true;
    period = value;
  }
  return period;
}

function parseQueryU32(text: string): number {
  if (text.length === 0) {
    throw badRequest(`${QUERY_DESERIALIZE}: limit: cannot parse integer from empty string`);
  }
  if (!/^[0-9]+$/.test(text)) {
    throw badRequest(`${QUERY_DESERIALIZE}: limit: invalid digit found in string`);
  }
  const value = BigInt(text);
  if (value > U32_MAX) {
    throw badRequest(`${QUERY_DESERIALIZE}: limit: number too large to fit in target type`);
  }
  return Number(value);
}

/** The query string of `requestUrl`, without the leading `?`. */
export function rawSearch(requestUrl: string): string | null {
  const search = new URL(requestUrl).search;
  if (!search.startsWith("?")) {
    return null;
  }
  return search.slice(1);
}

function decodeQueryComponent(value: string): string {
  const bytes: number[] = [];
  for (let index = 0; index < value.length; ) {
    const code = value.charCodeAt(index);
    if (code === 0x2b) {
      bytes.push(0x20);
      index += 1;
      continue;
    }
    if (code === 0x25) {
      if (index + 2 >= value.length) {
        throw badRequest("invalid query string encoding");
      }
      const high = hexDigit(value.charCodeAt(index + 1));
      const low = hexDigit(value.charCodeAt(index + 2));
      if (high === null || low === null) {
        throw badRequest("invalid query string encoding");
      }
      bytes.push((high << 4) | low);
      index += 3;
      continue;
    }
    const point = value.codePointAt(index);
    if (point === undefined) {
      throw badRequest("invalid query string encoding");
    }
    pushUtf8(bytes, point);
    index += point > 0xffff ? 2 : 1;
  }
  try {
    return new TextDecoder("utf-8", { fatal: true }).decode(Uint8Array.from(bytes));
  } catch {
    throw badRequest("invalid query string encoding");
  }
}

function hexDigit(byte: number): number | null {
  if (byte >= 0x30 && byte <= 0x39) {
    return byte - 0x30;
  }
  if (byte >= 0x61 && byte <= 0x66) {
    return byte - 0x61 + 10;
  }
  if (byte >= 0x41 && byte <= 0x46) {
    return byte - 0x41 + 10;
  }
  return null;
}

function pushUtf8(bytes: number[], point: number): void {
  if (point <= 0x7f) {
    bytes.push(point);
    return;
  }
  if (point <= 0x7ff) {
    bytes.push(0xc0 | (point >> 6), 0x80 | (point & 0x3f));
    return;
  }
  if (point <= 0xffff) {
    bytes.push(0xe0 | (point >> 12), 0x80 | ((point >> 6) & 0x3f), 0x80 | (point & 0x3f));
    return;
  }
  bytes.push(
    0xf0 | (point >> 18),
    0x80 | ((point >> 12) & 0x3f),
    0x80 | ((point >> 6) & 0x3f),
    0x80 | (point & 0x3f),
  );
}
