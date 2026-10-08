import { badRequest } from "./errors.ts";

const DESERIALIZE = "Failed to deserialize the JSON body into the target type";
const PARSE = "Failed to parse the request body as JSON";
const I64_MIN = -9223372036854775808n;
const I64_MAX = 9223372036854775807n;
const U64_MAX = 18446744073709551615n;
const CANONICAL = Symbol.for("axond.serdeCanonical");

export type StrictField = {
  name: string;
  kind: "string" | "u64" | "cadence" | "any" | "strings";
  required?: boolean;
  /** JSON null is the absent optional value. */
  nullOk?: boolean;
};

type Loc = { line: number; column: number };

class Cursor {
  i = 0;
  line = 1;
  /** 1-based index of the last consumed byte. 0 before the first byte. */
  col = 0;
  bytes: Uint8Array;
  /** The last `bump` consumed a byte that is not a UTF-8 scalar. */
  lastInvalid = false;

  constructor(bytes: Uint8Array) {
    this.bytes = bytes;
  }

  peek(): string {
    const decoded = decodeScalar(this.bytes, this.i);
    return decoded === null ? (this.i >= this.bytes.length ? "" : "\uFFFD") : decoded.char;
  }

  bump(): string {
    if (this.i >= this.bytes.length) {
      this.lastInvalid = false;
      return "";
    }
    const decoded = decodeScalar(this.bytes, this.i);
    if (decoded === null) {
      this.i += 1;
      this.col += 1;
      this.lastInvalid = true;
      return "\uFFFD";
    }
    this.lastInvalid = false;
    this.i += decoded.size;
    if (decoded.char === "\n") {
      this.line += 1;
      this.col = 0;
      return decoded.char;
    }
    this.col += decoded.size;
    return decoded.char;
  }

  loc(): Loc {
    return { line: this.line, column: this.col };
  }

  skipWs(): void {
    while (this.peek() === " " || this.peek() === "\n" || this.peek() === "\t" || this.peek() === "\r") {
      this.bump();
    }
  }
}

/** One Unicode scalar, or null when the next byte is not well-formed UTF-8. */
function decodeScalar(bytes: Uint8Array, i: number): { char: string; size: number } | null {
  const b0 = bytes[i];
  if (b0 === undefined) {
    return null;
  }
  if (b0 < 0x80) {
    return { char: String.fromCharCode(b0), size: 1 };
  }
  if (b0 < 0xc2 || b0 > 0xf4) {
    return null;
  }
  const width = b0 < 0xe0 ? 2 : b0 < 0xf0 ? 3 : 4;
  if (i + width > bytes.length) {
    return null;
  }
  const b1 = bytes[i + 1]!;
  if ((b1 & 0xc0) !== 0x80) {
    return null;
  }
  if (width === 2) {
    return { char: String.fromCharCode(((b0 & 0x1f) << 6) | (b1 & 0x3f)), size: 2 };
  }
  const b2 = bytes[i + 2]!;
  if ((b2 & 0xc0) !== 0x80) {
    return null;
  }
  if (width === 3) {
    if ((b0 === 0xe0 && b1 < 0xa0) || (b0 === 0xed && b1 >= 0xa0)) {
      return null;
    }
    return {
      char: String.fromCharCode(((b0 & 0x0f) << 12) | ((b1 & 0x3f) << 6) | (b2 & 0x3f)),
      size: 3,
    };
  }
  const b3 = bytes[i + 3]!;
  if ((b3 & 0xc0) !== 0x80) {
    return null;
  }
  if ((b0 === 0xf0 && b1 < 0x90) || (b0 === 0xf4 && b1 >= 0x90)) {
    return null;
  }
  const cp = ((b0 & 0x07) << 18) | ((b1 & 0x3f) << 12) | ((b2 & 0x3f) << 6) | (b3 & 0x3f);
  return { char: String.fromCodePoint(cp), size: 4 };
}

function at(loc: Loc): string {
  return `at line ${loc.line} column ${loc.column}`;
}

function expectedFields(names: readonly string[]): string {
  if (names.length === 1) {
    return `expected \`${names[0]}\``;
  }
  if (names.length === 2) {
    return `expected \`${names[0]}\` or \`${names[1]}\``;
  }
  return `expected one of ${names.map((name) => `\`${name}\``).join(", ")}`;
}

function fail(message: string): never {
  throw badRequest(message);
}

function peekLoc(cur: Cursor): Loc {
  if (cur.i >= cur.bytes.length) {
    return cur.loc();
  }
  if (cur.bytes[cur.i] === 0x0a) {
    return { line: cur.line + 1, column: 0 };
  }
  return { line: cur.line, column: cur.col + 1 };
}

function parseFail(message: string, loc: Loc): never {
  fail(`${PARSE}: ${message} ${at(loc)}`);
}

function syntax(cur: Cursor, path = ""): never {
  const prefix = path.length === 0 ? "" : `${path}: `;
  if (cur.peek() === "") {
    parseFail(`${prefix}EOF while parsing a value`, cur.loc());
  }
  parseFail(`${prefix}expected value`, peekLoc(cur));
}

/**
 * Read one management JSON body the way serde reads it with
 * `deny_unknown_fields`. A JSON array is the positional struct form.
 * The column is the last byte serde consumed, or the byte it refused.
 */
export function readStrictObject(
  raw: string | Uint8Array,
  fields: readonly StrictField[],
  structName: string,
): Record<string, unknown> {
  const bytes = typeof raw === "string" ? new TextEncoder().encode(raw) : raw;
  const cur = new Cursor(bytes);
  cur.skipWs();
  if (cur.peek() === "") {
    parseFail("EOF while parsing a value", cur.loc());
  }
  if (cur.peek() === "{") {
    return readObjectFields(cur, fields);
  }
  if (cur.peek() === "[") {
    return readPositional(cur, fields, structName);
  }
  if (cur.peek() === '"') {
    const text = readString(cur);
    fail(`${DESERIALIZE}: invalid type: string ${JSON.stringify(text)}, expected struct ${structName} ${at(cur.loc())}`);
  }
  const described = describeOther(cur);
  fail(`${DESERIALIZE}: invalid type: ${described}, expected struct ${structName} ${at(cur.loc())}`);
}

/**
 * serde stamps a duplicate or unknown field after `end_map` runs.
 * Whitespace is consumed. A closing `}` is consumed. Any other byte stays.
 */
function fieldErrorLoc(cur: Cursor): Loc {
  cur.skipWs();
  if (cur.peek() === "}") {
    cur.bump();
  }
  return cur.loc();
}

function readObjectFields(cur: Cursor, fields: readonly StrictField[]): Record<string, unknown> {
  cur.bump();
  const names = fields.map((field) => field.name);
  const byName = new Map(fields.map((field) => [field.name, field]));
  const out: Record<string, unknown> = {};
  const seen = new Set<string>();
  let afterComma = false;
  while (true) {
    cur.skipWs();
    if (cur.peek() === "") {
      parseFail("EOF while parsing an object", cur.loc());
    }
    if (cur.peek() === "}") {
      if (afterComma) {
        parseFail("trailing comma", peekLoc(cur));
      }
      cur.bump();
      finish(cur, fields, seen, out);
      return out;
    }
    if (cur.peek() !== '"') {
      parseFail("key must be a string", peekLoc(cur));
    }
    const key = readString(cur);
    const field = byName.get(key);
    if (!field || seen.has(key)) {
      const loc = fieldErrorLoc(cur);
      if (!field) {
        fail(`${DESERIALIZE}: ${key}: unknown field \`${key}\`, ${expectedFields(names)} ${at(loc)}`);
      }
      fail(`${DESERIALIZE}: duplicate field \`${key}\` ${at(loc)}`);
    }
    cur.skipWs();
    if (cur.peek() !== ":") {
      parseFail("expected `:`", peekLoc(cur));
    }
    cur.bump();
    cur.skipWs();
    out[key] = readField(cur, field, key);
    seen.add(key);
    cur.skipWs();
    if (cur.peek() === "") {
      parseFail("EOF while parsing an object", cur.loc());
    }
    if (cur.peek() === ",") {
      cur.bump();
      afterComma = true;
      continue;
    }
    afterComma = false;
    if (cur.peek() !== "}") {
      parseFail("expected `,` or `}`", peekLoc(cur));
    }
  }
}

function readPositional(cur: Cursor, fields: readonly StrictField[], structName: string): Record<string, unknown> {
  cur.bump();
  const values: unknown[] = [];
  cur.skipWs();
  while (cur.peek() !== "]") {
    if (cur.peek() === "") {
      parseFail("EOF while parsing a list", cur.loc());
    }
    if (values.length >= fields.length) {
      parseFail("trailing characters", peekLoc(cur));
    }
    const field = fields[values.length]!;
    values.push(readField(cur, field, `[${values.length}]`));
    cur.skipWs();
    if (cur.peek() === ",") {
      cur.bump();
      cur.skipWs();
      if (cur.peek() === "]") parseFail("trailing comma", peekLoc(cur));
      continue;
    }
    if (cur.peek() !== "]" && cur.peek() !== "") {
      syntax(cur);
    }
  }
  const end = peekLoc(cur);
  cur.bump();
  const missing = fields.slice(values.length).some((field) => field.required);
  if (missing) {
    const noun = fields.length === 1 ? "element" : "elements";
    fail(
      `${DESERIALIZE}: invalid length ${values.length}, expected struct ${structName} with ${fields.length} ${noun} ${at(end)}`,
    );
  }
  const out: Record<string, unknown> = {};
  const seen = new Set<string>();
  for (let index = 0; index < values.length; index += 1) {
    const field = fields[index]!;
    out[field.name] = values[index];
    seen.add(field.name);
  }
  finish(cur, fields, seen, out);
  return out;
}

function finish(cur: Cursor, fields: readonly StrictField[], seen: Set<string>, out: Record<string, unknown>): void {
  const endLoc = cur.loc();
  for (const field of fields) {
    if (field.required && !seen.has(field.name)) {
      fail(`${DESERIALIZE}: missing field \`${field.name}\` ${at(endLoc)}`);
    }
  }
  cur.skipWs();
  if (cur.peek() !== "") {
    fail(`Failed to parse the request body as JSON: trailing characters ${at(peekLoc(cur))}`);
  }
  for (const field of fields) {
    if (field.kind === "u64" && seen.has(field.name)) {
      const amount = out[field.name];
      if (typeof amount === "bigint" && amount > I64_MAX) {
        fail("microdollar amount exceeds the store integer range");
      }
    }
  }
}

function readField(cur: Cursor, field: StrictField, key: string): unknown {
  if (cur.peek() === "") {
    parseFail(`${key}: EOF while parsing a value`, cur.loc());
  }
  if (cur.peek() === "}" || cur.peek() === "]" || cur.peek() === ",") {
    parseFail(`${key}: expected value`, peekLoc(cur));
  }
  const peek = cur.peek();
  if (field.nullOk && peek === "n") {
    const loc = readLiteral(cur, "null");
    void loc;
    return null;
  }
  if (field.kind === "string") {
    return expectString(cur, key, "a string");
  }
  if (field.kind === "cadence") {
    return readCadence(cur, key);
  }
  if (field.kind === "u64") {
    return readU64(cur, key);
  }
  if (field.kind === "strings") {
    return readStringList(cur, key);
  }
  if (cur.peek() === "n") {
    readLiteral(cur, "null");
    return null;
  }
  return jsonValue(cur, key);
}

function readCadence(cur: Cursor, key: string): "monthly" | "fixed" {
  const peek = cur.peek();
  if (peek === '"') {
    return knownCadence(readString(cur, key), key, cur.loc());
  }
  if (peek !== "{") {
    parseFail(`${key}: expected value`, peekLoc(cur));
  }
  cur.bump();
  cur.skipWs();
  if (cur.peek() === "") {
    parseFail(`${key}: EOF while parsing an object`, cur.loc());
  }
  if (cur.peek() === "}") {
    parseFail(`${key}: expected value`, peekLoc(cur));
  }
  if (cur.peek() !== '"') {
    parseFail(`${key}: key must be a string`, peekLoc(cur));
  }
  const variant = readString(cur, key);
  const name = knownCadence(variant, key, cur.loc());
  cur.skipWs();
  if (cur.peek() === "") {
    parseFail(`${key}: EOF while parsing an object`, cur.loc());
  }
  if (cur.peek() !== ":") {
    parseFail(`${key}: expected \`:\``, peekLoc(cur));
  }
  cur.bump();
  cur.skipWs();
  readUnit(cur, `${key}.${name}`);
  cur.skipWs();
  if (cur.peek() === "") {
    parseFail(`${key}: EOF while parsing an object`, cur.loc());
  }
  if (cur.peek() !== "}") {
    parseFail(`${key}: expected value`, cur.loc());
  }
  cur.bump();
  return name;
}

function knownCadence(text: string, key: string, loc: Loc): "monthly" | "fixed" {
  if (text === "monthly" || text === "fixed") {
    return text;
  }
  fail(`${DESERIALIZE}: ${key}: unknown variant \`${text}\`, expected \`monthly\` or \`fixed\` ${at(loc)}`);
}

function readUnit(cur: Cursor, path: string): void {
  if (cur.peek() === "") {
    parseFail(`${path}: EOF while parsing a value`, cur.loc());
  }
  const peek = cur.peek();
  if (peek === "n") {
    cur.bump();
    readIdent(cur, path, "ull");
    return;
  }
  if (peek === "t" || peek === "f") {
    const word = peek === "t" ? "true" : "false";
    cur.bump();
    readIdent(cur, path, word.slice(1));
    fail(`${DESERIALIZE}: ${path}: invalid type: boolean \`${word}\`, expected unit ${at(cur.loc())}`);
  }
  if (peek === '"') {
    const text = readString(cur, path);
    fail(`${DESERIALIZE}: ${path}: invalid type: string ${JSON.stringify(text)}, expected unit ${at(cur.loc())}`);
  }
  if (peek === "[" || peek === "{") {
    const kind = peek === "[" ? "sequence" : "map";
    fail(`${DESERIALIZE}: ${path}: invalid type: ${kind}, expected unit ${at(cur.loc())}`);
  }
  if (peek === "-" || (peek >= "0" && peek <= "9")) {
    const token = readUnitNumber(cur, path);
    rejectOutOfRange(token, path, cur.loc());
    const kind = token.includes(".") || token.includes("e") || token.includes("E")
      ? `floating point \`${formatRustFloat(token)}\``
      : `integer \`${token}\``;
    fail(`${DESERIALIZE}: ${path}: invalid type: ${kind}, expected unit ${at(cur.loc())}`);
  }
  parseFail(`${path}: expected value`, peekLoc(cur));
}

function readIdent(cur: Cursor, path: string, word: string): void {
  for (const ch of word) {
    const got = cur.bump();
    if (got === "") {
      parseFail(`${path}: EOF while parsing a value`, cur.loc());
    }
    if (got !== ch) {
      parseFail(`${path}: expected ident`, cur.loc());
    }
  }
}

function readUnitNumber(cur: Cursor, path: string): string {
  const start = cur.i;
  if (cur.peek() === "-") {
    cur.bump();
  }
  const first = cur.bump();
  if (first === "") {
    parseFail(`${path}: EOF while parsing a value`, cur.loc());
  }
  if (first === "0") {
    if (cur.peek() >= "0" && cur.peek() <= "9") {
      parseFail(`${path}: invalid number`, peekLoc(cur));
    }
  } else if (first >= "1" && first <= "9") {
    while (cur.peek() >= "0" && cur.peek() <= "9") {
      cur.bump();
    }
  } else {
    parseFail(`${path}: invalid number`, cur.loc());
  }
  if (cur.peek() === ".") {
    cur.bump();
    if (cur.peek() < "0" || cur.peek() > "9") {
      if (cur.peek() === "") {
        parseFail(`${path}: EOF while parsing a value`, cur.loc());
      }
      parseFail(`${path}: invalid number`, peekLoc(cur));
    }
    while (cur.peek() >= "0" && cur.peek() <= "9") {
      cur.bump();
    }
  }
  if (cur.peek() === "e" || cur.peek() === "E") {
    cur.bump();
    if (cur.peek() === "+" || cur.peek() === "-") {
      cur.bump();
    }
    const exp = cur.bump();
    if (exp === "") {
      parseFail(`${path}: EOF while parsing a value`, cur.loc());
    }
    if (exp < "0" || exp > "9") {
      parseFail(`${path}: invalid number`, cur.loc());
    }
    while (cur.peek() >= "0" && cur.peek() <= "9") {
      cur.bump();
    }
  }
  return asciiSlice(cur, start, cur.i);
}

function readU64(cur: Cursor, key: string): bigint {
  const peek = cur.peek();
  if (peek === '"') {
    const text = readString(cur, key);
    fail(`${DESERIALIZE}: ${key}: invalid type: string ${JSON.stringify(text)}, expected u64 ${at(cur.loc())}`);
  }
  if (peek === "t" || peek === "f") {
    const word = peek === "t" ? "true" : "false";
    readLiteral(cur, word);
    fail(`${DESERIALIZE}: ${key}: invalid type: boolean \`${word}\`, expected u64 ${at(cur.loc())}`);
  }
  if (peek === "n") {
    readLiteral(cur, "null");
    fail(`${DESERIALIZE}: ${key}: invalid type: null, expected u64 ${at(cur.loc())}`);
  }
  if (peek === "[" || peek === "{") {
    const where = cur.loc();
    const kind = peek === "[" ? "sequence" : "map";
    fail(`${DESERIALIZE}: ${key}: invalid type: ${kind}, expected u64 ${at(where)}`);
  }
  if (peek !== "-" && (peek < "0" || peek > "9")) {
    syntax(cur, key);
  }
  const token = readNumber(cur);
  const loc = cur.loc();
  rejectOutOfRange(token, key, loc);
  if (token.includes(".") || token.includes("e") || token.includes("E")) {
    fail(`${DESERIALIZE}: ${key}: invalid type: floating point \`${formatRustFloat(token)}\`, expected u64 ${at(loc)}`);
  }
  if (token.startsWith("-")) {
    fail(`${DESERIALIZE}: ${key}: invalid value: integer \`${token}\`, expected u64 ${at(loc)}`);
  }
  let amount: bigint;
  try {
    amount = BigInt(token);
  } catch {
    syntax(cur);
  }
  if (amount > U64_MAX) {
    fail(`${DESERIALIZE}: ${key}: invalid type: floating point \`${formatRustFloat(token)}\`, expected u64 ${at(loc)}`);
  }
  return amount;
}

function readStringList(cur: Cursor, key: string): string[] | null {
  if (cur.peek() === "n") {
    readLiteral(cur, "null");
    return null;
  }
  if (cur.peek() !== "[") {
    if (cur.peek() === '"') {
      const text = readString(cur, key);
      fail(`${DESERIALIZE}: ${key}: invalid type: string ${JSON.stringify(text)}, expected a sequence ${at(cur.loc())}`);
    }
    if (cur.peek() === "[" || cur.peek() === "{") {
      const where = cur.loc();
      const kind = cur.peek() === "[" ? "sequence" : "map";
      fail(`${DESERIALIZE}: ${key}: invalid type: ${kind}, expected a sequence ${at(where)}`);
    }
    const kind = describeOther(cur, key);
    fail(`${DESERIALIZE}: ${key}: invalid type: ${kind}, expected a sequence ${at(cur.loc())}`);
  }
  cur.bump();
  const items: string[] = [];
  cur.skipWs();
  if (cur.peek() === "]") {
    cur.bump();
    return items;
  }
  let index = 0;
  while (cur.peek() !== "") {
    if (cur.peek() !== '"') {
      if (cur.peek() === "[" || cur.peek() === "{") {
        const where = cur.loc();
        const kind = cur.peek() === "[" ? "sequence" : "map";
        fail(`${DESERIALIZE}: ${key}[${index}]: invalid type: ${kind}, expected a string ${at(where)}`);
      }
      const described = describeOther(cur, `${key}[${index}]`);
      fail(`${DESERIALIZE}: ${key}[${index}]: invalid type: ${described}, expected a string ${at(cur.loc())}`);
    }
    items.push(readString(cur, `${key}[${index}]`));
    index += 1;
    cur.skipWs();
    if (cur.peek() === ",") {
      cur.bump();
      cur.skipWs();
      continue;
    }
    if (cur.peek() === "]") {
      cur.bump();
      return items;
    }
    parseFail(`${key}: expected \`,\` or \`]\``, peekLoc(cur));
  }
  syntax(cur);
}

function expectString(cur: Cursor, key: string, expected: string): string {
  if (cur.peek() !== '"') {
    if (cur.peek() === "[" || cur.peek() === "{") {
      const where = cur.loc();
      const kind = cur.peek() === "[" ? "sequence" : "map";
      fail(`${DESERIALIZE}: ${key}: invalid type: ${kind}, expected ${expected} ${at(where)}`);
    }
    const kind = describeOther(cur, key);
    fail(`${DESERIALIZE}: ${key}: invalid type: ${kind}, expected ${expected} ${at(cur.loc())}`);
  }
  return readString(cur, key);
}

function describeOther(cur: Cursor, path = ""): string {
  const peek = cur.peek();
  if (peek === "t" || peek === "f") {
    const word = peek === "t" ? "true" : "false";
    readLiteral(cur, word);
    return `boolean \`${word}\``;
  }
  if (peek === "n") {
    readLiteral(cur, "null");
    return "null";
  }
  if (peek === "-" || (peek >= "0" && peek <= "9")) {
    const token = readNumber(cur);
    rejectOutOfRange(token, path, cur.loc());
    if (token.includes(".") || token.includes("e") || token.includes("E")) {
      return `floating point \`${formatRustFloat(token)}\``;
    }
    return `integer \`${token}\``;
  }
  syntax(cur, path);
}

function asciiSlice(cur: Cursor, start: number, end: number): string {
  let out = "";
  for (let i = start; i < end; i += 1) {
    out += String.fromCharCode(cur.bytes[i]!);
  }
  return out;
}

function jsonValue(cur: Cursor, path: string): unknown {
  const start = cur.i;
  skipValue(cur, path);
  return serdeValue(canonicalJson(cur.bytes.subarray(start, cur.i)));
}

const CONTROL_IN_STRING = "control character (\\u0000-\\u001F) found while parsing a string";

function stringError(cur: Cursor, path: string | undefined, message: string): never {
  const prefix = path ? `${path}: ` : "";
  parseFail(`${prefix}${message}`, cur.loc());
}

function readHex4(cur: Cursor, path: string | undefined): number {
  let hex = "";
  for (let i = 0; i < 4; i += 1) {
    const digit = cur.bump();
    if (digit === "") {
      stringError(cur, path, "EOF while parsing a string");
    }
    hex += digit;
  }
  if (!/^[0-9a-fA-F]{4}$/.test(hex)) {
    stringError(cur, path, "invalid escape");
  }
  return Number.parseInt(hex, 16);
}

function appendUnicode(out: { text: string }, cur: Cursor, path: string | undefined, code: number): void {
  if (code >= 0xdc00 && code <= 0xdfff) {
    stringError(cur, path, "lone leading surrogate in hex escape");
  }
  if (code < 0xd800 || code > 0xdbff) {
    out.text += String.fromCharCode(code);
    return;
  }
  if (cur.peek() === "") {
    stringError(cur, path, "EOF while parsing a string");
  }
  if (cur.peek() !== "\\") {
    cur.bump();
    stringError(cur, path, "unexpected end of hex escape");
  }
  cur.bump();
  if (cur.peek() === "") {
    stringError(cur, path, "EOF while parsing a string");
  }
  if (cur.peek() !== "u") {
    cur.bump();
    stringError(cur, path, "unexpected end of hex escape");
  }
  cur.bump();
  const low = readHex4(cur, path);
  if (low < 0xdc00 || low > 0xdfff) {
    stringError(cur, path, "lone leading surrogate in hex escape");
  }
  const point = ((code - 0xd800) << 10) + (low - 0xdc00) + 0x10000;
  out.text += String.fromCodePoint(point);
}

function readString(cur: Cursor, path?: string): string {
  if (cur.bump() !== '"') {
    syntax(cur);
  }
  const out = { text: "" };
  let sawInvalid = false;
  while (cur.peek() !== "") {
    const ch = cur.bump();
    if (cur.lastInvalid) {
      sawInvalid = true;
      continue;
    }
    if (ch === '"') {
      if (sawInvalid) {
        stringError(cur, path, "invalid unicode code point");
      }
      return out.text;
    }
    if (ch === "\\") {
      const esc = cur.bump();
      if (esc === "") {
        stringError(cur, path, "EOF while parsing a string");
      }
      if (cur.lastInvalid) {
        stringError(cur, path, "invalid escape");
      }
      if (esc === '"' || esc === "\\" || esc === "/") {
        out.text += esc;
      } else if (esc === "b") {
        out.text += "\b";
      } else if (esc === "f") {
        out.text += "\f";
      } else if (esc === "n") {
        out.text += "\n";
      } else if (esc === "r") {
        out.text += "\r";
      } else if (esc === "t") {
        out.text += "\t";
      } else if (esc === "u") {
        appendUnicode(out, cur, path, readHex4(cur, path));
      } else {
        stringError(cur, path, "invalid escape");
      }
      continue;
    }
    if (ch < " ") {
      stringError(cur, path, CONTROL_IN_STRING);
    }
    out.text += ch;
  }
  stringError(cur, path, "EOF while parsing a string");
}

function readLiteral(cur: Cursor, word: string): Loc {
  for (const ch of word) {
    if (cur.bump() !== ch) {
      syntax(cur);
    }
  }
  return cur.loc();
}

function readNumber(cur: Cursor): string {
  const start = cur.i;
  if (cur.peek() === "-") {
    cur.bump();
  }
  if (cur.peek() === "0") {
    cur.bump();
  } else if (cur.peek() >= "1" && cur.peek() <= "9") {
    while (cur.peek() >= "0" && cur.peek() <= "9") {
      cur.bump();
    }
  } else {
    syntax(cur);
  }
  if (cur.peek() === ".") {
    cur.bump();
    if (cur.peek() < "0" || cur.peek() > "9") {
      syntax(cur);
    }
    while (cur.peek() >= "0" && cur.peek() <= "9") {
      cur.bump();
    }
  }
  if (cur.peek() === "e" || cur.peek() === "E") {
    cur.bump();
    if (cur.peek() === "+" || cur.peek() === "-") {
      cur.bump();
    }
    if (cur.peek() < "0" || cur.peek() > "9") {
      syntax(cur);
    }
    while (cur.peek() >= "0" && cur.peek() <= "9") {
      cur.bump();
    }
  }
  return asciiSlice(cur, start, cur.i);
}

function skipValue(cur: Cursor, path = "", depth = 0): Loc {
  if (depth >= 128) parseFail("recursion limit exceeded", peekLoc(cur));
  const peek = cur.peek();
  if (peek === '"') {
    readString(cur, path.length === 0 ? undefined : path);
    return cur.loc();
  }
  if (peek === "t") {
    return readLiteral(cur, "true");
  }
  if (peek === "f") {
    return readLiteral(cur, "false");
  }
  if (peek === "n") {
    return readLiteral(cur, "null");
  }
  if (peek === "-" || (peek >= "0" && peek <= "9")) {
    const token = readNumber(cur);
    const loc = cur.loc();
    rejectOutOfRange(token, path, loc);
    return loc;
  }
  if (peek === "{") {
    cur.bump();
    cur.skipWs();
    if (cur.peek() === "}") {
      cur.bump();
      return cur.loc();
    }
    while (cur.peek() !== "") {
      if (cur.peek() !== '"') {
        const prefix = path.length === 0 ? "" : `${path}.?: `;
        parseFail(`${prefix}key must be a string`, peekLoc(cur));
      }
      const key = readString(cur, path.length === 0 ? undefined : `${path}.?`);
      cur.skipWs();
      if (cur.bump() !== ":") {
        syntax(cur);
      }
      cur.skipWs();
      skipValue(cur, path.length === 0 ? "" : `${path}.${key}`, depth + 1);
      cur.skipWs();
      if (cur.peek() === ",") {
        cur.bump();
        cur.skipWs();
        continue;
      }
      if (cur.peek() === "}") {
        cur.bump();
        return cur.loc();
      }
      const prefix = path.length === 0 ? "" : `${path}.?: `;
      parseFail(`${prefix}expected \`,\` or \`}\``, peekLoc(cur));
    }
    syntax(cur);
  }
  if (peek === "[") {
    cur.bump();
    cur.skipWs();
    if (cur.peek() === "]") {
      cur.bump();
      return cur.loc();
    }
    let index = 0;
    while (cur.peek() !== "") {
      skipValue(cur, path.length === 0 ? "" : `${path}[${index}]`, depth + 1);
      index += 1;
      cur.skipWs();
      if (cur.peek() === ",") {
        cur.bump();
        cur.skipWs();
        continue;
      }
      if (cur.peek() === "]") {
        cur.bump();
        return cur.loc();
      }
      const prefix = path.length === 0 ? "" : `${path}: `;
      parseFail(`${prefix}expected \`,\` or \`]\``, peekLoc(cur));
    }
    syntax(cur);
  }
  syntax(cur, path);
}

function rejectOutOfRange(token: string, path: string, loc: Loc): void {
  if (Number.isFinite(Number(token))) {
    return;
  }
  const prefix = path.length === 0 ? "" : `${path}: `;
  parseFail(`${prefix}number out of range`, loc);
}

/**
 * JSON value carried with the bytes serde_json would write.
 * A class instance stringifies as an object, so callers use `encodeAttrs`.
 */
export class SerdeDoc {
  canonical: string;
  constructor(canonical: string) {
    this.canonical = canonical;
  }
}

export function serdeCanonical(value: unknown): string | null {
  if (value instanceof SerdeDoc) {
    return value.canonical;
  }
  if (value !== null && typeof value === "object" && CANONICAL in value) {
    return (value as { [CANONICAL]: string })[CANONICAL];
  }
  return null;
}

/** Parse `canonical` for field access and keep those bytes for the wire. */
export function serdeValue(canonical: string): unknown {
  const parsed = JSON.parse(canonical) as unknown;
  if (parsed === null) return null;
  if (parsed !== null && typeof parsed === "object") {
    Object.defineProperty(parsed, CANONICAL, { value: canonical, enumerable: false });
    return parsed;
  }
  return new SerdeDoc(canonical);
}

export function encodeAttrs(value: unknown): string {
  const canonical = serdeCanonical(value);
  if (canonical !== null) {
    return canonical;
  }
  return JSON.stringify(value);
}

export function canonicalJson(bytes: Uint8Array): string {
  const cur = new Cursor(bytes);
  cur.skipWs();
  return canonicalValue(cur);
}

function canonicalValue(cur: Cursor, depth = 0): string {
  if (depth >= 128) parseFail("recursion limit exceeded", peekLoc(cur));
  const peek = cur.peek();
  if (peek === '"') {
    return JSON.stringify(readString(cur));
  }
  if (peek === "t") {
    readLiteral(cur, "true");
    return "true";
  }
  if (peek === "f") {
    readLiteral(cur, "false");
    return "false";
  }
  if (peek === "n") {
    readLiteral(cur, "null");
    return "null";
  }
  if (peek === "-" || (peek >= "0" && peek <= "9")) {
    return canonicalNumber(readNumber(cur));
  }
  if (peek === "{") {
    cur.bump();
    cur.skipWs();
    const entries = new Map<string, string>();
    if (cur.peek() !== "}") {
      while (cur.peek() !== "") {
        const key = readString(cur);
        cur.skipWs();
        if (cur.bump() !== ":") {
          syntax(cur);
        }
        cur.skipWs();
        entries.set(key, canonicalValue(cur, depth + 1));
        cur.skipWs();
        if (cur.peek() === ",") {
          cur.bump();
          cur.skipWs();
          continue;
        }
        break;
      }
    }
    if (cur.bump() !== "}") {
      syntax(cur);
    }
    const keys = [...entries.keys()].sort(compareUtf8);
    return `{${keys.map((key) => `${JSON.stringify(key)}:${entries.get(key)}`).join(",")}}`;
  }
  if (peek === "[") {
    cur.bump();
    cur.skipWs();
    const items: string[] = [];
    if (cur.peek() !== "]") {
      while (cur.peek() !== "") {
        items.push(canonicalValue(cur, depth + 1));
        cur.skipWs();
        if (cur.peek() === ",") {
          cur.bump();
          cur.skipWs();
          continue;
        }
        break;
      }
    }
    if (cur.bump() !== "]") {
      syntax(cur);
    }
    return `[${items.join(",")}]`;
  }
  syntax(cur);
}

function compareUtf8(left: string, right: string): number {
  const encoded = new TextEncoder();
  const a = encoded.encode(left);
  const b = encoded.encode(right);
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i += 1) {
    if (a[i] !== b[i]) {
      return a[i]! - b[i]!;
    }
  }
  return a.length - b.length;
}

function canonicalNumber(token: string): string {
  if (/^-?(0|[1-9][0-9]*)$/.test(token)) {
    if (token === "-0") {
      return "-0.0";
    }
    const n = BigInt(token);
    if (n >= 0n && n <= U64_MAX) {
      return n.toString();
    }
    if (n < 0n && n >= I64_MIN) {
      return n.toString();
    }
    return formatSerdeF64(Number(token));
  }
  return formatSerdeF64(Number(token));
}

function scientific(abs: number): string {
  for (let p = 0; p <= 17; p += 1) {
    const raw = abs.toExponential(p);
    if (Number(raw) !== abs) {
      continue;
    }
    let body = raw.replace(/(\.\d*?)0+e/, "$1e").replace(/\.e/, "e");
    if (/e\d/.test(body)) {
      body = body.replace("e", "e+");
    }
    body = body.replace("e+-", "e-");
    if (Number(body) === abs) {
      return body;
    }
  }
  return abs.toExponential();
}

/** serde_json 1.0 finite f64 text (zmij), including `1.0` and `-0.0`. */
function formatSerdeF64(n: number): string {
  if (Object.is(n, -0)) {
    return "-0.0";
  }
  if (n === 0) {
    return "0.0";
  }
  const neg = n < 0;
  const abs = Math.abs(n);
  const sign = neg ? "-" : "";
  if (Number.isInteger(abs) && abs < 1e16) {
    return `${sign}${abs.toFixed(0)}.0`;
  }
  if (abs >= 1e16 || abs < 1e-5) {
    return sign + scientific(abs);
  }
  return sign + JSON.stringify(abs);
}

function formatRustFloat(token: string): string {
  const value = Number(token);
  if (!Number.isFinite(value)) {
    return token;
  }
  const abs = Math.abs(value);
  if (value !== 0 && (abs >= 1e16 || abs < 1e-6)) {
    return value.toExponential(16).replace(/e\+/, "e+");
  }
  if (Number.isInteger(value)) {
    return `${value}.0`;
  }
  return String(value);
}
