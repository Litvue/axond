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
