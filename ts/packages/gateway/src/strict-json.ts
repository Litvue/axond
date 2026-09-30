import { badRequest } from "./errors.ts";

const DESERIALIZE = "Failed to deserialize the JSON body into the target type";
const PARSE = "Failed to parse the request body as JSON";
const I64_MAX = 9223372036854775807n;
const U64_MAX = 18446744073709551615n;

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
  raw: string;

  constructor(raw: string) {
    this.raw = raw;
  }

  peek(): string {
    const point = this.raw.codePointAt(this.i);
    return point === undefined ? "" : String.fromCodePoint(point);
  }

  bump(): string {
    const ch = this.peek();
    if (ch.length === 0) {
      return "";
    }
    this.i += ch.length;
    if (ch === "\n") {
      this.line += 1;
      this.col = 0;
      return ch;
    }
    this.col += new TextEncoder().encode(ch).length;
    return ch;
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
  const ch = cur.peek();
  if (ch.length === 0) {
    return cur.loc();
  }
  if (ch === "\n") {
    return { line: cur.line + 1, column: 0 };
  }
  return { line: cur.line, column: cur.col + new TextEncoder().encode(ch).length };
}

function parseFail(message: string, loc: Loc): never {
  fail(`${PARSE}: ${message} ${at(loc)}`);
}

function syntax(cur: Cursor): never {
  if (cur.peek() === "") {
    parseFail("EOF while parsing a value", cur.loc());
  }
  parseFail("expected value", peekLoc(cur));
}

/**
 * Read one management JSON body the way serde reads it with
 * `deny_unknown_fields`. A JSON array is the positional struct form.
 * The column is the last byte serde consumed, or the byte it refused.
 */
export function readStrictObject(
  raw: string,
  fields: readonly StrictField[],
  structName: string,
): Record<string, unknown> {
  const cur = new Cursor(raw);
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
      syntax(cur);
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
    cur.bump();
    fail(`Failed to parse the request body as JSON: trailing characters ${at(cur.loc())}`);
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
    if (peek !== '"') {
      const loc = skipValue(cur);
      fail(`Failed to parse the request body as JSON: ${key}: expected value ${at(loc)}`);
    }
    const text = readString(cur);
    const loc = cur.loc();
    if (text !== "monthly" && text !== "fixed") {
      fail(
        `${DESERIALIZE}: ${key}: unknown variant \`${text}\`, expected \`monthly\` or \`fixed\` ${at(loc)}`,
      );
    }
    return text;
  }
  if (field.kind === "u64") {
    return readU64(cur, key);
  }
  if (field.kind === "strings") {
    return readStringList(cur, key);
  }
  return jsonValue(cur);
}

function readU64(cur: Cursor, key: string): bigint {
  const peek = cur.peek();
  if (peek === '"') {
    const text = readString(cur);
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
    skipValue(cur);
    const kind = peek === "[" ? "sequence" : "map";
    fail(`${DESERIALIZE}: ${key}: invalid type: ${kind}, expected u64 ${at(where)}`);
  }
  if (peek !== "-" && (peek < "0" || peek > "9")) {
    syntax(cur);
  }
  const token = readNumber(cur);
  const loc = cur.loc();
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
      const text = readString(cur);
      fail(`${DESERIALIZE}: ${key}: invalid type: string ${JSON.stringify(text)}, expected a sequence ${at(cur.loc())}`);
    }
    if (cur.peek() === "[" || cur.peek() === "{") {
      const where = cur.loc();
      const kind = cur.peek() === "[" ? "sequence" : "map";
      skipValue(cur);
      fail(`${DESERIALIZE}: ${key}: invalid type: ${kind}, expected a sequence ${at(where)}`);
    }
    const kind = describeOther(cur);
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
        skipValue(cur);
        fail(`${DESERIALIZE}: ${key}[${index}]: invalid type: ${kind}, expected a string ${at(where)}`);
      }
      const described = describeOther(cur);
      fail(`${DESERIALIZE}: ${key}[${index}]: invalid type: ${described}, expected a string ${at(cur.loc())}`);
    }
    items.push(readString(cur));
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
    syntax(cur);
  }
  syntax(cur);
}

function expectString(cur: Cursor, key: string, expected: string): string {
  if (cur.peek() !== '"') {
    if (cur.peek() === "[" || cur.peek() === "{") {
      const where = cur.loc();
      const kind = cur.peek() === "[" ? "sequence" : "map";
      skipValue(cur);
      fail(`${DESERIALIZE}: ${key}: invalid type: ${kind}, expected ${expected} ${at(where)}`);
    }
    const kind = describeOther(cur);
    fail(`${DESERIALIZE}: ${key}: invalid type: ${kind}, expected ${expected} ${at(cur.loc())}`);
  }
  return readString(cur, key);
}

function describeOther(cur: Cursor): string {
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
    if (token.includes(".") || token.includes("e") || token.includes("E")) {
      return `floating point \`${formatRustFloat(token)}\``;
    }
    return `integer \`${token}\``;
  }
  syntax(cur);
}

function jsonValue(cur: Cursor): unknown {
  const start = cur.i;
  skipValue(cur);
  try {
    return JSON.parse(cur.raw.slice(start, cur.i)) as unknown;
  } catch {
    syntax(cur);
  }
}

function readString(cur: Cursor, path?: string): string {
  if (cur.bump() !== '"') {
    syntax(cur);
  }
  let out = "";
  while (cur.peek() !== "") {
    const ch = cur.bump();
    if (ch === '"') {
      return out;
    }
    if (ch === "\\") {
      const esc = cur.bump();
      if (esc === '"' || esc === "\\" || esc === "/") {
        out += esc;
      } else if (esc === "b") {
        out += "\b";
      } else if (esc === "f") {
        out += "\f";
      } else if (esc === "n") {
        out += "\n";
      } else if (esc === "r") {
        out += "\r";
      } else if (esc === "t") {
        out += "\t";
      } else if (esc === "u") {
        let hex = "";
        for (let i = 0; i < 4; i += 1) {
          hex += cur.bump();
        }
        if (!/^[0-9a-fA-F]{4}$/.test(hex)) {
          syntax(cur);
        }
        out += String.fromCharCode(Number.parseInt(hex, 16));
      } else {
        syntax(cur);
      }
      continue;
    }
    if (ch < " ") {
      syntax(cur);
    }
    out += ch;
  }
  const prefix = path ? `${path}: ` : "";
  parseFail(`${prefix}EOF while parsing a string`, cur.loc());
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
  return cur.raw.slice(start, cur.i);
}

function skipValue(cur: Cursor): Loc {
  const peek = cur.peek();
  if (peek === '"') {
    readString(cur);
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
    readNumber(cur);
    return cur.loc();
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
        syntax(cur);
      }
      readString(cur);
      cur.skipWs();
      if (cur.bump() !== ":") {
        syntax(cur);
      }
      cur.skipWs();
      skipValue(cur);
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
      syntax(cur);
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
    while (cur.peek() !== "") {
      skipValue(cur);
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
      syntax(cur);
    }
    syntax(cur);
  }
  syntax(cur);
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
