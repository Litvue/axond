/**
 * Axum accepts a JSON body when the `content-type` parses as mime 0.3 and the
 * type is `application` with subtype `json` or a `+json` suffix. The header
 * text stays out of the error.
 */
export function isJsonContentType(header: string): boolean {
  const bytes = latin1(header);
  if (!bytes) {
    return false;
  }
  const parsed = parseMime(bytes);
  if (!parsed) {
    return false;
  }
  return parsed.type === "application" && (parsed.subtype === "json" || parsed.suffix === "json");
}

function latin1(header: string): Uint8Array | null {
  const bytes = new Uint8Array(header.length);
  for (let index = 0; index < header.length; index += 1) {
    const code = header.charCodeAt(index);
    if (code > 255) {
      return null;
    }
    bytes[index] = code;
  }
  return bytes;
}

function isTokenByte(byte: number): boolean {
  return (
    (byte >= 0x30 && byte <= 0x39) ||
    (byte >= 0x41 && byte <= 0x5a) ||
    (byte >= 0x61 && byte <= 0x7a) ||
    byte === 0x21 ||
    byte === 0x23 ||
    byte === 0x24 ||
    byte === 0x25 ||
    byte === 0x26 ||
    byte === 0x27 ||
    byte === 0x2a ||
    byte === 0x2b ||
    byte === 0x2d ||
    byte === 0x2e ||
    byte === 0x5e ||
    byte === 0x5f ||
    byte === 0x60 ||
    byte === 0x7c ||
    byte === 0x7e
  );
}

function asciiLower(bytes: Uint8Array, start: number, end: number): string {
  let text = "";
  for (let index = start; index < end; index += 1) {
    const byte = bytes[index]!;
    text += String.fromCharCode(byte >= 0x41 && byte <= 0x5a ? byte + 0x20 : byte);
  }
  return text;
}

function parseMime(bytes: Uint8Array): { type: string; subtype: string; suffix: string | null } | null {
  const length = bytes.length;
  let index = 0;
  while (index < length && isTokenByte(bytes[index]!)) {
    index += 1;
  }
  if (index === 0 || index >= length || bytes[index] !== 0x2f) {
    return null;
  }
  const type = asciiLower(bytes, 0, index);
  index += 1;
  const subtypeStart = index;
  let plus = -1;
  while (index < length) {
    const byte = bytes[index]!;
    if (byte === 0x2b && index > subtypeStart) {
      plus = index;
      index += 1;
      continue;
    }
    if (byte === 0x3b && index > subtypeStart) {
      break;
    }
    if (!isTokenByte(byte)) {
      return null;
    }
    index += 1;
  }
  if (index === subtypeStart) {
    return null;
  }
  const subtypeEnd = plus === -1 ? index : plus;
  const subtype = asciiLower(bytes, subtypeStart, subtypeEnd);
  const suffix = plus === -1 ? null : asciiLower(bytes, plus + 1, index);
  if (index < length) {
    if (!parseParams(bytes, index)) {
      return null;
    }
  }
  return { type, subtype, suffix };
}

function parseParams(bytes: Uint8Array, semicolon: number): boolean {
  let start = semicolon + 1;
  const length = bytes.length;
  while (start < length) {
    let index = start;
    let restarted = false;
    while (true) {
      if (index >= length) {
        return false;
      }
      const byte = bytes[index]!;
      if (byte === 0x20 && index === start) {
        start = index + 1;
        restarted = true;
        break;
      }
      if (isTokenByte(byte)) {
        index += 1;
        continue;
      }
      if (byte === 0x3d && index > start) {
        index += 1;
        break;
      }
      return false;
    }
    if (restarted) {
      continue;
    }
    const valueStart = index;
    if (index < length && bytes[index] === 0x22) {
      index += 1;
      const quotedStart = index;
      while (true) {
        if (index >= length) {
          return false;
        }
        const byte = bytes[index]!;
        if (byte === 0x22 && index > quotedStart) {
          index += 1;
          break;
        }
        if (byte > 31 && byte !== 127) {
          index += 1;
          continue;
        }
        return false;
      }
      while (true) {
        if (index >= length) {
          start = length;
          break;
        }
        const byte = bytes[index]!;
        if (byte === 0x3b) {
          start = index + 1;
          break;
        }
        if (byte === 0x20) {
          index += 1;
          continue;
        }
        return false;
      }
      continue;
    }
    while (true) {
      if (index >= length) {
        start = length;
        break;
      }
      const byte = bytes[index]!;
      if (isTokenByte(byte)) {
        index += 1;
        continue;
      }
      if (byte === 0x3b && index > valueStart) {
        start = index + 1;
        break;
      }
      return false;
    }
  }
  return true;
}
