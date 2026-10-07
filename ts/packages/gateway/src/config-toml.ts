export const I64_MAX = 9223372036854775807n;

export const I64_MIN = -9223372036854775808n;

export const U64_MAX = 18446744073709551615n;

/**
 * Figment fails the document in the parser, before extract. A file integer
 * outside `i64` is `number too large` or `number too small`. An inline table
 * stays on one line: a trailing comma, a newline, or a comment is
 * `invalid inline table`. A basic string `\x` with two hex digits, or `\e`,
 * is `invalid escape sequence` at the character after the escape letter.
 * `\a`, `\q`, `\x` without two hex digits, and a backslash before a newline
 * use that diagram. A short `\u` or `\U`, or a code point that is a surrogate
 * or above U+10FFFF, is
 * `invalid unicode 4-digit hex code` or `invalid unicode 8-digit hex code`
 * at the character after `u` or `U`. A complete hex sequence that is out of
 * range also says `value is out of range`. A decimal integer that is only
 * `0` (optional sign) stops there: a following digit, `_`, or radix letter
 * is `expected newline, `#`` at that character. A local time `07:32:00`
 * and a four-digit year are not that diagram. Inside an array that is
 * `invalid array` / `expected `]``. Inside an inline table it is the inline
 * closer. `0.5`, `0e1`, and `0x10` still parse. A decimal float that
 * parses as positive infinity is `invalid floating-point number` at the
 * start of that number. `-1e309`, `1e308`, `1e-400`, and `inf` still parse.
 * A calendar day that month does not have is `invalid date-time` and
 * `value is out of range` on the day. `2024-02-29` and `1900-02-28` still parse.
 * A time whose seconds are `60` is a leap second and still parses. An hour
 * past 23 is the container's newline diagram on `:` or `T`. A minute past
 * 59 or a second past 60 is `invalid time` or `invalid date-time`, and
 * `value is out of range`. An offset hour past 23 is `invalid time offset`.
 * An underscore in a number must sit between digits. `1_`, `1__2`, and
 * `1_e2` are `invalid integer` and `expected digit` at the character after
 * the underscore. `0x_1` is `invalid hexadecimal integer` on that
 * underscore, and `0x1_` adds `expected digit`. Octal and binary use their
 * own labels. A fraction `1.0_` is `invalid floating-point number` and
 * `expected digit, digit`. An exponent `1e1_` is `expected digit`. `1e_`
 * is the float label only. `1_000`, `0x1_0`, `1.0_1`, and `1e1_0` still parse.
 */
/** UTF-8 length of one code point. ASCII is one byte and one caret. */
function tomlUtf8Bytes(codePoint: number): number {
  if (codePoint <= 0x7f) {
    return 1;
  }
  if (codePoint <= 0x7ff) {
    return 2;
  }
  if (codePoint <= 0xffff) {
    return 3;
  }
  return 4;
}

function tomlUtf8Length(text: string): number {
  let bytes = 0;
  for (const char of text) {
    bytes += tomlUtf8Bytes(char.codePointAt(0) ?? 0);
  }
  return bytes;
}

export function formatTomlIntegerRange(source: string, index: number, message: string): string {
  // toml_edit clamps an index past the last byte onto that byte and keeps the
  // extra columns. A trailing newline stays on that line, one column past the end.
  let at = index;
  let columnOffset = 0;
  if (source.length > 0 && at >= source.length) {
    columnOffset = at - (source.length - 1);
    at = source.length - 1;
  }
  const line = source.slice(0, at).split("\n").length - 1;
  const lineStart = source.lastIndexOf("\n", Math.max(0, at - 1)) + 1;
  const lineEnd = source.indexOf("\n", at);
  const content = source.slice(lineStart, lineEnd === -1 ? source.length : lineEnd);
  const prefix = source.slice(lineStart, at);
  // A multi-byte character fails as a byte span. The column is that byte
  // offset, and the caret is as wide as the character.
  const onChar = index < source.length;
  const codePoint = onChar ? (source.codePointAt(at) ?? 0) : 0;
  const charBytes = tomlUtf8Bytes(codePoint);
  const multibyte = onChar && charBytes > 1;
  const column = (multibyte ? tomlUtf8Length(prefix) : [...prefix].length) + columnOffset;
  let highlight = 1;
  if (multibyte) {
    const room = tomlUtf8Length(content) - column;
    highlight = Math.min(charBytes, room);
    if (highlight < 1) {
      highlight = 1;
    }
  }
  const lineNum = line + 1;
  const gutter = String(lineNum).length;
  const pad = " ".repeat(gutter + 1);
  return (
    `TOML parse error at line ${lineNum}, column ${column + 1}\n` +
    `${pad}|\n` +
    `${lineNum} | ${content}\n` +
    `${pad}|${" ".repeat(column + 1)}${"^".repeat(highlight)}\n` +
    `${message}\n`
  );
}

type TomlScan = { end: number; hit: { index: number; message: string } | null; bail: boolean; token?: number };

type TomlScope = "document" | "inline";

interface TomlTable {
  kind: "table";
  implicit: boolean;
  dotted: boolean;
  items: Map<string, TomlNode>;
}

type TomlNode = { kind: "value"; typeName: string } | { kind: "inline" } | { kind: "aot"; last: TomlTable } | TomlTable;

function newTomlTable(implicit: boolean, dotted: boolean): TomlTable {
  return { kind: "table", implicit, dotted, items: new Map() };
}

function duplicateTomlMessage(key: string, table: readonly string[] | null): string {
  if (table === null) {
    return `duplicate key \`${key}\``;
  }
  if (table.length === 0) {
    return `duplicate key \`${key}\` in document root`;
  }
  return `duplicate key \`${key}\` in table \`${table.join(".")}\``;
}

function extendTomlMessage(path: readonly string[], actual: string): string {
  return `dotted key \`${path.join(".")}\` attempted to extend non-table type (${actual})`;
}

/** Walk a dotted prefix. An error string is Figment's message, without a header label. */
function descendToml(
  table: TomlTable,
  path: readonly string[],
  dotted: boolean,
  scope: TomlScope,
): TomlTable | string {
  let current = table;
  for (let i = 0; i < path.length; i += 1) {
    const key = path[i] ?? "";
    const existing = current.items.get(key);
    if (!existing) {
      const created = newTomlTable(true, dotted);
      current.items.set(key, created);
      current = created;
      continue;
    }
    if (existing.kind === "aot") {
      current = existing.last;
      continue;
    }
    if (existing.kind === "value" || (existing.kind === "inline" && scope === "document")) {
      const typeName = existing.kind === "value" ? existing.typeName : "inline table";
      return extendTomlMessage(path.slice(0, i + 1), typeName);
    }
    const implicit = existing.kind === "table" ? existing.implicit : false;
    if (dotted && !implicit) {
      return duplicateTomlMessage(key, null);
    }
    if (existing.kind !== "table") {
      return duplicateTomlMessage(key, null);
    }
    current = existing;
  }
  return current;
}

function defineTomlKey(
  table: TomlTable,
  tablePath: readonly string[],
  segments: readonly string[],
  value: TomlNode,
  scope: TomlScope,
): string | null {
  if (segments.length === 0) {
    return null;
  }
  const leaf = segments[segments.length - 1] ?? "";
  const prefix = segments.slice(0, -1);
  const landed = descendToml(table, prefix, true, scope);
  if (typeof landed === "string") {
    return landed;
  }
  if (landed.dotted === (prefix.length === 0)) {
    return duplicateTomlMessage(leaf, null);
  }
  if (landed.items.has(leaf)) {
    return duplicateTomlMessage(leaf, scope === "inline" ? null : tablePath);
  }
  landed.items.set(leaf, value);
  return null;
}

function defineTomlHeader(
  root: TomlTable,
  segments: readonly string[],
  array: boolean,
): { table: TomlTable } | { message: string } {
  const leaf = segments[segments.length - 1] ?? "";
  const prefix = segments.slice(0, -1);
  const parent = descendToml(root, prefix, false, "document");
  if (typeof parent === "string") {
    return { message: parent };
  }
  const existing = parent.items.get(leaf);
  if (array) {
    if (!existing) {
      const last = newTomlTable(false, false);
      parent.items.set(leaf, { kind: "aot", last });
      return { table: last };
    }
    if (existing.kind === "aot") {
      const last = newTomlTable(false, false);
      existing.last = last;
      return { table: last };
    }
    return { message: duplicateTomlMessage(leaf, prefix) };
  }
  if (existing?.kind === "table" && existing.implicit && !existing.dotted) {
    existing.implicit = false;
    existing.dotted = false;
    return { table: existing };
  }
  if (existing) {
    return { message: duplicateTomlMessage(leaf, prefix) };
  }
  const created = newTomlTable(false, false);
  parent.items.set(leaf, created);
  return { table: created };
}

function tomlDefinedValue(source: string, start: number, end: number): TomlNode {
  const char = source[start] ?? "";
  if (char === '"' || char === "'") {
    return { kind: "value", typeName: "string" };
  }
  if (char === "{") {
    return { kind: "inline" };
  }
  if (char === "[") {
    return { kind: "value", typeName: "array" };
  }
  if (char === "t" || char === "f") {
    return { kind: "value", typeName: "boolean" };
  }
  const slice = source.slice(start, end);
  if (/^[+-]?(?:inf|nan)$/.test(slice)) {
    return { kind: "value", typeName: "float" };
  }
  if (isTomlDateOrTime(source, start)) {
    return { kind: "value", typeName: "datetime" };
  }
  const body = char === "+" || char === "-" ? start + 1 : start;
  if (source.startsWith("0x", body) || source.startsWith("0o", body) || source.startsWith("0b", body)) {
    return { kind: "value", typeName: "integer" };
  }
  if (/[.eE]/.test(slice)) {
    return { kind: "value", typeName: "float" };
  }
  return { kind: "value", typeName: "integer" };
}

export function scanTomlDocument(source: string, index: number, leaps: number[] = []): TomlScan {
  const root = newTomlTable(false, false);
  let current = root;
  let currentPath: string[] = [];
  let cursor = index;
  while (cursor < source.length) {
    const leading = scanTomlTrivia(source, cursor);
    if (leading.hit) {
      return leading;
    }
    cursor = leading.end;
    if (cursor >= source.length) {
      break;
    }
    if (source[cursor] === "[") {
      const at = cursor;
      const segments: string[] = [];
      const header = skipTomlHeader(source, cursor, segments);
      if (header.hit || header.bail) {
        return header;
      }
      const defined = defineTomlHeader(root, segments, source.startsWith("[[", at));
      if ("message" in defined) {
        return {
          end: at,
          hit: { index: at, message: `invalid table header\n${defined.message}` },
          bail: false,
        };
      }
      current = defined.table;
      currentPath = segments.slice();
      cursor = header.end;
      continue;
    }
    const at = cursor;
    const segments: string[] = [];
    const key = skipTomlKey(source, cursor, segments);
    if (key.hit || key.bail) {
      return key;
    }
    const between = scanTomlTrivia(source, key.end);
    if (between.hit) {
      return between;
    }
    cursor = between.end;
    if (source[cursor] !== "=") {
      return { end: cursor, hit: null, bail: true };
    }
    const value = scanTomlValue(source, cursor + 1, "document", leaps);
    if (value.hit || value.bail) {
      return value;
    }
    const failure = defineTomlKey(
      current,
      currentPath,
      segments,
      tomlDefinedValue(source, value.token ?? cursor + 1, value.end),
      "document",
    );
    if (failure) {
      return { end: at, hit: { index: at, message: failure }, bail: false };
    }
    const tail = documentLineTail(source, value.end);
    if (tail) {
      return tail;
    }
    cursor = value.end;
  }
  return { end: cursor, hit: null, bail: false };
}

type TomlContainer = "document" | "array" | "inline";

const DOCUMENT_AFTER_VALUE = "expected newline, `#`";

const ARRAY_AFTER_VALUE = "invalid array\nexpected `]`";

const STRING_VALUE = "invalid string\nexpected `\"`, `'`";

const LEADING_FLOAT = "invalid floating-point number\nexpected leading digit";

const LEADING_INTEGER = "invalid integer\nexpected leading digit";

/** After a value, Figment allows spaces, a comment, and the end of the line. */
function documentLineTail(source: string, index: number): TomlScan | null {
  const cursor = skipInlineWs(source, index);
  const char = source[cursor] ?? "";
  if (char === "" || char === "\n" || char === "#") {
    return null;
  }
  if (char === "\r" && source[cursor + 1] === "\n") {
    return null;
  }
  return { end: cursor, hit: { index: cursor, message: DOCUMENT_AFTER_VALUE }, bail: false };
}

function afterZeroMessage(container: TomlContainer): string {
  if (container === "array") {
    return ARRAY_AFTER_VALUE;
  }
  if (container === "inline") {
    return INLINE_TABLE_MESSAGE;
  }
  return DOCUMENT_AFTER_VALUE;
}

/** A finished `0` ends before trivia, a comment, or this container's closer. */
function isCompletedZeroBoundary(char: string, container: TomlContainer): boolean {
  if (char === "" || char === " " || char === "\t" || char === "\n" || char === "\r" || char === "#") {
    return true;
  }
  if (container === "array") {
    return char === "," || char === "]";
  }
  if (container === "inline") {
    return char === "," || char === "}";
  }
  return false;
}

function tagTomlValue(scan: TomlScan, token: number): TomlScan {
  if (scan.hit || scan.bail) {
    return scan;
  }
  return { ...scan, token };
}

function scanTomlValue(source: string, index: number, container: TomlContainer, leaps: number[]): TomlScan {
  // A value may follow spaces and tabs. A newline or comment is the value in
  // a document or inline table. An array already skipped that trivia.
  const cursor = skipInlineWs(source, index);
  if (cursor >= source.length) {
    if (container === "array") {
      return { end: cursor, hit: { index: cursor, message: ARRAY_AFTER_VALUE }, bail: false };
    }
    return { end: cursor, hit: { index: cursor, message: "" }, bail: false };
  }
  const char = source[cursor];
  if (container !== "array" && (char === "\n" || char === "\r" || char === "#")) {
    return { end: cursor, hit: { index: cursor, message: STRING_VALUE }, bail: false };
  }
  if (char === '"' || char === "'") {
    return tagTomlValue(scanTomlString(source, cursor), cursor);
  }
  if (char === "{") {
    return tagTomlValue(scanTomlInline(source, cursor, leaps), cursor);
  }
  if (char === "[") {
    return tagTomlValue(scanTomlArray(source, cursor, leaps), cursor);
  }
  const word = tomlWord(source, cursor);
  const keywordHead = char === "t" ? "true" : char === "f" ? "false" : char === "i" ? "inf" : char === "n" ? "nan" : "";
  if (keywordHead) {
    if (word.startsWith(keywordHead)) {
      return { end: cursor + keywordHead.length, hit: null, bail: false, token: cursor };
    }
    // A lowercase t, f, i, or n commits. Anything short of the keyword is a string.
    return { end: cursor, hit: { index: cursor, message: STRING_VALUE }, bail: false };
  }
  if ((char === "+" || char === "-") && (source.startsWith("inf", cursor + 1) || source.startsWith("nan", cursor + 1))) {
    const end = cursor + 1 + (source.startsWith("inf", cursor + 1) ? 3 : 3);
    return { end, hit: null, bail: false, token: cursor };
  }
  if (char === "+" || char === "-" || isTomlDigit(char)) {
    return tagTomlValue(scanTomlNumber(source, cursor, container, leaps), cursor);
  }
  if (char === "." || char === "_") {
    if (container === "array") {
      return { end: cursor, hit: { index: cursor, message: ARRAY_AFTER_VALUE }, bail: false };
    }
    return {
      end: cursor,
      hit: { index: cursor, message: char === "." ? LEADING_FLOAT : LEADING_INTEGER },
      bail: false,
    };
  }
  if (container === "array") {
    return { end: cursor, hit: { index: cursor, message: ARRAY_AFTER_VALUE }, bail: false };
  }
  return { end: cursor, hit: { index: cursor, message: STRING_VALUE }, bail: false };
}

function scanTomlArray(source: string, index: number, leaps: number[]): TomlScan {
  let cursor = index + 1;
  for (;;) {
    const leading = scanTomlTrivia(source, cursor);
    if (leading.hit) {
      return leading;
    }
    cursor = leading.end;
    if (source[cursor] === "]") {
      return { end: cursor + 1, hit: null, bail: false };
    }
    const value = scanTomlValue(source, cursor, "array", leaps);
    if (value.hit || value.bail) {
      return value;
    }
    const between = scanTomlTrivia(source, value.end);
    if (between.hit) {
      return between;
    }
    cursor = between.end;
    if (source[cursor] === ",") {
      cursor += 1;
      continue;
    }
    if (source[cursor] === "]") {
      return { end: cursor + 1, hit: null, bail: false };
    }
    return { end: cursor, hit: { index: cursor, message: ARRAY_AFTER_VALUE }, bail: false };
  }
}

const INLINE_TABLE_MESSAGE = "invalid inline table\nexpected `}`";

/** Inline tables take spaces and tabs. A newline or comment ends the table. */
function skipInlineWs(source: string, index: number): number {
  let cursor = index;
  while (source[cursor] === " " || source[cursor] === "\t") {
    cursor += 1;
  }
  return cursor;
}

function canStartInlineKey(source: string, index: number): boolean {
  const char = source[index] ?? "";
  return char === '"' || char === "'" || /[A-Za-z0-9_-]/.test(char);
}

function inlineTableHit(index: number): TomlScan {
  return { end: index, hit: { index, message: INLINE_TABLE_MESSAGE }, bail: false };
}

function isInlineBreak(source: string, index: number): boolean {
  const char = source[index] ?? "";
  return char === "\n" || char === "\r" || char === "#";
}

/**
 * TOML 1.0 inline tables have no trailing comma and no newline. Figment
 * reports `invalid inline table` at the comma, or at the newline or `#`
 * when that token is where `}` was required. A comma followed by a key
 * continues. Arrays keep their own trailing commas.
 */
function scanTomlInline(source: string, index: number, leaps: number[]): TomlScan {
  const root = newTomlTable(false, false);
  const mark = index + 1;
  let pending: string | null = null;
  let cursor = index + 1;
  for (;;) {
    cursor = skipInlineWs(source, cursor);
    if (cursor >= source.length) {
      if (pending) {
        return { end: mark, hit: { index: mark, message: pending }, bail: false };
      }
      return inlineTableHit(cursor);
    }
    if (source[cursor] === "}") {
      if (pending) {
        return { end: mark, hit: { index: mark, message: pending }, bail: false };
      }
      return { end: cursor + 1, hit: null, bail: false };
    }
    if (source[cursor] === "," || isInlineBreak(source, cursor) || !canStartInlineKey(source, cursor)) {
      if (pending) {
        return { end: mark, hit: { index: mark, message: pending }, bail: false };
      }
      return inlineTableHit(cursor);
    }
    const segments: string[] = [];
    const keyEnd = scanDottedKey(source, cursor, "equals", segments);
    if (keyEnd.hit || keyEnd.bail) {
      return keyEnd;
    }
    cursor = skipInlineWs(source, keyEnd.end);
    if (source[cursor] !== "=") {
      return { end: cursor, hit: null, bail: true };
    }
    const value = scanInlineTableValue(source, cursor + 1, leaps);
    if (value.hit || value.bail) {
      return value;
    }
    const failure = defineTomlKey(
      root,
      [],
      segments,
      tomlDefinedValue(source, value.token ?? keyEnd.end, value.end),
      "inline",
    );
    if (failure && pending === null) {
      pending = failure;
    }
    cursor = skipInlineWs(source, value.end);
    if (source[cursor] === ",") {
      const after = skipInlineWs(source, cursor + 1);
      if (canStartInlineKey(source, after)) {
        cursor = after;
        continue;
      }
      if (pending) {
        return { end: mark, hit: { index: mark, message: pending }, bail: false };
      }
      return inlineTableHit(cursor);
    }
    if (source[cursor] === "}") {
      if (pending) {
        return { end: mark, hit: { index: mark, message: pending }, bail: false };
      }
      return { end: cursor + 1, hit: null, bail: false };
    }
    if (isInlineBreak(source, cursor) || cursor >= source.length) {
      if (pending) {
        return { end: mark, hit: { index: mark, message: pending }, bail: false };
      }
      return inlineTableHit(cursor);
    }
    if (pending) {
      return { end: mark, hit: { index: mark, message: pending }, bail: false };
    }
    return inlineTableHit(cursor);
  }
}

function scanInlineTableValue(source: string, index: number, leaps: number[]): TomlScan {
  const cursor = skipInlineWs(source, index);
  if (cursor >= source.length) {
    return { end: cursor, hit: { index: cursor, message: "" }, bail: false };
  }
  if (isInlineBreak(source, cursor) || source[cursor] === "}" || source[cursor] === ",") {
    return { end: cursor, hit: { index: cursor, message: STRING_VALUE }, bail: false };
  }
  return scanTomlValue(source, cursor, "inline", leaps);
}

const INTEGER_LABEL = "invalid integer";

const INTEGER_DIGIT = "invalid integer\nexpected digit";

const FLOAT_LABEL = "invalid floating-point number";

const FLOAT_DIGIT = "invalid floating-point number\nexpected digit";

const FLOAT_FRAC_DIGIT = "invalid floating-point number\nexpected digit, digit";

function radixIntegerLabel(base: number): string {
  if (base === 16) {
    return "invalid hexadecimal integer";
  }
  if (base === 8) {
    return "invalid octal integer";
  }
  return "invalid binary integer";
}

/**
 * `index` is a digit. A later `_` must be followed by another digit.
 * The caret sits on the character that was supposed to be that digit.
 */
function walkGroupedDigits(
  source: string,
  index: number,
  digit: (char: string) => boolean,
  underscoreMessage: string,
): { end: number; hit: { index: number; message: string } | null } {
  let cursor = index + 1;
  while (cursor < source.length) {
    const char = source[cursor] ?? "";
    if (digit(char)) {
      cursor += 1;
      continue;
    }
    if (char === "_") {
      const after = cursor + 1;
      if (digit(source[after] ?? "")) {
        cursor = after + 1;
        continue;
      }
      return { end: after, hit: { index: after, message: underscoreMessage } };
    }
    break;
  }
  return { end: cursor, hit: null };
}

function scanTomlNumber(source: string, index: number, container: TomlContainer, leaps: number[]): TomlScan {
  const head = source[index] ?? "";
  if (source.startsWith("0x", index) || source.startsWith("0o", index) || source.startsWith("0b", index)) {
    const base = source[index + 1] === "x" ? 16 : source[index + 1] === "o" ? 8 : 2;
    const label = radixIntegerLabel(base);
    const digit = (char: string) => isRadixDigit(char, base);
    const prefix = index + 2;
    if (!digit(source[prefix] ?? "")) {
      return { end: prefix, hit: { index: prefix, message: label }, bail: false };
    }
    const walked = walkGroupedDigits(source, prefix, digit, `${label}\nexpected digit`);
    if (walked.hit) {
      return { end: walked.end, hit: walked.hit, bail: false };
    }
    const raw = source.slice(prefix, walked.end).replaceAll("_", "");
    const integer = BigInt(base === 16 ? `0x${raw}` : base === 8 ? `0o${raw}` : `0b${raw}`);
    if (integer > I64_MAX) {
      return { end: walked.end, hit: { index, message: "number too large to fit in target type" }, bail: false };
    }
    return { end: walked.end, hit: null, bail: false };
  }
  let cursor = index;
  let signed = false;
  // Figment's integer label keeps the missing digit. An array backtracks to `]`.
  if (head === "+" || head === "-") {
    signed = true;
    cursor += 1;
    if (!isTomlDigit(source[cursor] ?? "")) {
      if (container === "array") {
        return { end: index, hit: { index, message: ARRAY_AFTER_VALUE }, bail: false };
      }
      return { end: cursor, hit: { index: cursor, message: INTEGER_LABEL }, bail: false };
    }
  }
  const digitsAt = cursor;
  if (source[cursor] === "0") {
    const next = source[cursor + 1] ?? "";
    // A sign cannot start a date. `+07:32:00` is the integer `+0`.
    const dateLike = !signed && isTomlDateOrTime(source, cursor);
    if (next !== "." && next !== "e" && next !== "E" && !dateLike) {
      if (isCompletedZeroBoundary(next, container)) {
        return { end: cursor + 1, hit: null, bail: false };
      }
      return { end: cursor + 1, hit: { index: cursor + 1, message: afterZeroMessage(container) }, bail: false };
    }
  }
  const grouped = walkGroupedDigits(source, cursor, isTomlDigit, INTEGER_DIGIT);
  if (grouped.hit) {
    return { end: grouped.end, hit: grouped.hit, bail: false };
  }
  cursor = grouped.end;
  const next = source[cursor] ?? "";
  if (next === "." || next === "e" || next === "E") {
    return scanDecimalFloat(source, index, cursor);
  }
  // A date is four digits then `-`. A time is two digits then `:`. A sign is an integer.
  if (!signed && continuesTomlDateOrTime(source, digitsAt, cursor, next)) {
    if (next === "-") {
      return scanTomlCalendar(source, digitsAt, leaps);
    }
    const hour = Number(source.slice(digitsAt, cursor));
    if (hour <= 23) {
      return scanTomlLocalTime(source, digitsAt, leaps);
    }
  }
  const raw = source.slice(index, cursor).replaceAll("_", "");
  let integer: bigint;
  try {
    integer = BigInt(raw);
  } catch {
    return { end: index, hit: null, bail: true };
  }
  if (integer > I64_MAX || integer < I64_MIN) {
    return {
      end: cursor,
      hit: {
        index,
        message: integer < I64_MIN ? "number too small to fit in target type" : "number too large to fit in target type",
      },
      bail: false,
    };
  }
  return { end: cursor, hit: null, bail: false };
}

function scanDecimalFloat(source: string, numberStart: number, cursor: number): TomlScan {
  let at = cursor;
  if (source[at] === ".") {
    const first = at + 1;
    if (!isTomlDigit(source[first] ?? "")) {
      return { end: first, hit: { index: first, message: FLOAT_DIGIT }, bail: false };
    }
    const walked = walkGroupedDigits(source, first, isTomlDigit, FLOAT_FRAC_DIGIT);
    if (walked.hit) {
      return { end: walked.end, hit: walked.hit, bail: false };
    }
    at = walked.end;
  }
  if (source[at] === "e" || source[at] === "E") {
    at += 1;
    if (source[at] === "+" || source[at] === "-") {
      at += 1;
    }
    if (!isTomlDigit(source[at] ?? "")) {
      return { end: at, hit: { index: at, message: FLOAT_LABEL }, bail: false };
    }
    const walked = walkGroupedDigits(source, at, isTomlDigit, FLOAT_DIGIT);
    if (walked.hit) {
      return { end: walked.end, hit: walked.hit, bail: false };
    }
    at = walked.end;
  }
  const value = Number(source.slice(numberStart, at).replaceAll("_", ""));
  if (value === Number.POSITIVE_INFINITY) {
    return { end: at, hit: { index: numberStart, message: FLOAT_LABEL }, bail: false };
  }
  return { end: at, hit: null, bail: false };
}

function isRadixDigit(char: string, base: number): boolean {
  if (base === 16) {
    return /[0-9a-fA-F]/.test(char);
  }
  if (base === 8) {
    return /[0-7]/.test(char);
  }
  return char === "0" || char === "1";
}

/**
 * smol-toml reads a time through `Date`, which rejects second 60. Figment
 * accepts that leap second. The two digits are read as 59 so the document
 * parses. No config field is a date-time, so the loaded value is unused.
 */
export function tomlLeapSecondsAs59(source: string, leaps: readonly number[]): string {
  if (leaps.length === 0) {
    return source;
  }
  let out = source;
  for (const index of [...leaps].reverse()) {
    if (out.slice(index, index + 2) !== "60") {
      continue;
    }
    out = `${out.slice(0, index)}59${out.slice(index + 2)}`;
  }
  return out;
}

function leapSecondAt(source: string, index: number, end: number): number | null {
  const token = source.slice(index, end);
  const match = /(?:^|[Tt ])(\d{2}):(\d{2}):60/.exec(token);
  if (match === null) {
    return null;
  }
  const hour = Number(match[1]);
  const minute = Number(match[2]);
  if (hour > 23 || minute > 59) {
    return null;
  }
  return index + match.index + match[0].length - 2;
}

const TIME_RANGE = "invalid time\nvalue is out of range";

const OFFSET_RANGE = "invalid time offset\nvalue is out of range";

function isTomlDateOrTime(source: string, index: number): boolean {
  const head = source.slice(index, index + 2);
  if (/^\d\d$/.test(head) && source[index + 2] === ":") {
    return true;
  }
  return /^\d{4}-\d{2}-\d{2}/.test(source.slice(index, index + 10));
}

/** Figment tries a date only for four digits before `-`, or two digits before `:`. */
function continuesTomlDateOrTime(source: string, digitsAt: number, cursor: number, next: string): boolean {
  const digits = source.slice(digitsAt, cursor);
  if (next === "-" && /^\d{4}$/.test(digits)) {
    return true;
  }
  return next === ":" && /^\d{2}$/.test(digits);
}

const DATE_LABEL = "invalid date-time";

const DATE_OUT_OF_RANGE = "invalid date-time\nvalue is out of range";

const OFFSET_LABEL = "invalid time offset";

function tomlTwoDigits(source: string, index: number): number | null {
  const head = source.slice(index, index + 2);
  if (!/^\d{2}$/.test(head)) {
    return null;
  }
  return Number(head);
}

/**
 * Figment cuts a date after `YYYY-`. A missing month, dash, or day is
 * `invalid date-time` on that character. A `T` that is not a finished time
 * stays outside the date, so the container reports it.
 */
function scanTomlCalendar(source: string, yearAt: number, leaps: number[]): TomlScan {
  const year = Number(source.slice(yearAt, yearAt + 4));
  let cursor = yearAt + 5;
  const monthAt = cursor;
  const month = tomlTwoDigits(source, cursor);
  if (month === null) {
    return { end: cursor, hit: { index: cursor, message: DATE_LABEL }, bail: false };
  }
  if (month < 1 || month > 12) {
    return { end: monthAt, hit: { index: monthAt, message: DATE_OUT_OF_RANGE }, bail: false };
  }
  cursor += 2;
  if (source[cursor] !== "-") {
    return { end: cursor, hit: { index: cursor, message: DATE_LABEL }, bail: false };
  }
  cursor += 1;
  const dayAt = cursor;
  const day = tomlTwoDigits(source, cursor);
  if (day === null) {
    return { end: cursor, hit: { index: cursor, message: DATE_LABEL }, bail: false };
  }
  const leapYear = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const shortMonth = month === 4 || month === 6 || month === 9 || month === 11;
  const maxDay = month === 2 ? (leapYear ? 29 : 28) : shortMonth ? 30 : 31;
  if (day < 1 || day > maxDay) {
    return { end: dayAt, hit: { index: dayAt, message: DATE_OUT_OF_RANGE }, bail: false };
  }
  cursor += 2;
  const suffix = scanTomlDateSuffix(source, cursor);
  if (suffix.hit) {
    return suffix;
  }
  const leap = leapSecondAt(source, yearAt, suffix.end);
  if (leap !== null) {
    leaps.push(leap);
  }
  return suffix;
}

/** A time attaches only after its hour and colon. Otherwise the date already ended. */
function scanTomlDateSuffix(source: string, dateEnd: number): TomlScan {
  const delim = source[dateEnd] ?? "";
  if (delim !== "T" && delim !== "t" && delim !== " ") {
    return { end: dateEnd, hit: null, bail: false };
  }
  const timeAt = dateEnd + 1;
  const hour = tomlTwoDigits(source, timeAt);
  if (hour === null || hour > 23 || source[timeAt + 2] !== ":") {
    return { end: dateEnd, hit: null, bail: false };
  }
  let cursor = timeAt + 3;
  const minuteAt = cursor;
  const minute = tomlTwoDigits(source, cursor);
  if (minute === null) {
    return { end: cursor, hit: { index: cursor, message: DATE_LABEL }, bail: false };
  }
  if (minute > 59) {
    return { end: minuteAt, hit: { index: minuteAt, message: DATE_OUT_OF_RANGE }, bail: false };
  }
  cursor += 2;
  if (source[cursor] !== ":") {
    return { end: cursor, hit: { index: cursor, message: DATE_LABEL }, bail: false };
  }
  cursor += 1;
  const secondAt = cursor;
  const second = tomlTwoDigits(source, cursor);
  if (second === null) {
    return { end: cursor, hit: { index: cursor, message: DATE_LABEL }, bail: false };
  }
  if (second > 60) {
    return { end: secondAt, hit: { index: secondAt, message: DATE_OUT_OF_RANGE }, bail: false };
  }
  cursor += 2;
  if (source[cursor] === "." && isTomlDigit(source[cursor + 1] ?? "")) {
    cursor += 2;
    while (isTomlDigit(source[cursor] ?? "")) {
      cursor += 1;
    }
  }
  const offset = source[cursor] ?? "";
  if (offset === "Z" || offset === "z") {
    return { end: cursor + 1, hit: null, bail: false };
  }
  if (offset === "+" || offset === "-") {
    return scanTomlOffset(source, cursor);
  }
  return { end: cursor, hit: null, bail: false };
}

function scanTomlOffset(source: string, index: number): TomlScan {
  const hourAt = index + 1;
  const hour = tomlTwoDigits(source, hourAt);
  if (hour === null) {
    return { end: hourAt, hit: { index: hourAt, message: OFFSET_LABEL }, bail: false };
  }
  if (hour > 23) {
    return { end: hourAt, hit: { index: hourAt, message: OFFSET_RANGE }, bail: false };
  }
  let cursor = hourAt + 2;
  if (source[cursor] !== ":") {
    return { end: cursor, hit: { index: cursor, message: OFFSET_LABEL }, bail: false };
  }
  cursor += 1;
  const minuteAt = cursor;
  const minute = tomlTwoDigits(source, cursor);
  if (minute === null) {
    return { end: cursor, hit: { index: cursor, message: OFFSET_LABEL }, bail: false };
  }
  if (minute > 59) {
    return { end: minuteAt, hit: { index: minuteAt, message: OFFSET_RANGE }, bail: false };
  }
  return { end: cursor + 2, hit: null, bail: false };
}

const TIME_LABEL = "invalid time";

/** The colon after a legal hour commits the rest of a local time. */
function scanTomlLocalTime(source: string, hourAt: number, leaps: number[]): TomlScan {
  let cursor = hourAt + 3;
  const minuteAt = cursor;
  const minute = tomlTwoDigits(source, cursor);
  if (minute === null) {
    return { end: cursor, hit: { index: cursor, message: TIME_LABEL }, bail: false };
  }
  if (minute > 59) {
    return { end: minuteAt, hit: { index: minuteAt, message: TIME_RANGE }, bail: false };
  }
  cursor += 2;
  if (source[cursor] !== ":") {
    return { end: cursor, hit: { index: cursor, message: TIME_LABEL }, bail: false };
  }
  cursor += 1;
  const secondAt = cursor;
  const second = tomlTwoDigits(source, cursor);
  if (second === null) {
    return { end: cursor, hit: { index: cursor, message: TIME_LABEL }, bail: false };
  }
  if (second > 60) {
    return { end: secondAt, hit: { index: secondAt, message: TIME_RANGE }, bail: false };
  }
  cursor += 2;
  if (source[cursor] === "." && isTomlDigit(source[cursor + 1] ?? "")) {
    cursor += 2;
    while (isTomlDigit(source[cursor] ?? "")) {
      cursor += 1;
    }
  }
  const leap = leapSecondAt(source, hourAt, cursor);
  if (leap !== null) {
    leaps.push(leap);
  }
  return { end: cursor, hit: null, bail: false };
}

function isTomlCommentChar(char: string): boolean {
  const code = char.codePointAt(0) ?? 0;
  return code === 0x09 || (code >= 0x20 && code <= 0x7e) || code >= 0x80;
}

/** Spaces, tabs, newlines, and comments. A control character in a comment is an error with an empty message. */
function scanTomlTrivia(source: string, index: number): TomlScan {
  let cursor = index;
  for (;;) {
    while (cursor < source.length && " \t\r\n".includes(source[cursor] ?? "")) {
      cursor += 1;
    }
    if (source[cursor] !== "#") {
      return { end: cursor, hit: null, bail: false };
    }
    cursor += 1;
    while (cursor < source.length && source[cursor] !== "\n") {
      const char = source[cursor] ?? "";
      if (char === "\r" && source[cursor + 1] === "\n") {
        break;
      }
      if (!isTomlCommentChar(char)) {
        return { end: cursor, hit: { index: cursor, message: "" }, bail: false };
      }
      cursor += 1;
    }
  }
}

const KEY_EQUALS = "expected `.`, `=`";

const INVALID_KEY = "invalid key";

const HEADER_STD = "invalid table header\nexpected `.`, `]`";

const HEADER_ARRAY = "invalid table header\nexpected `.`, `]]`";

const HEADER_TRAIL = "invalid table header\nexpected newline, `#`";

type KeyStop = "equals" | "header-std" | "header-array";

/**
 * A dotted key allows spaces and tabs around `.`. A `.` that is not followed
 * by another segment is reported on that dot. A key that never starts is
 * `invalid key`, except an inline table, which still wants `}`.
 */
function scanDottedKey(source: string, index: number, stop: KeyStop, segments: string[]): TomlScan {
  let cursor = index;
  for (;;) {
    let segmentEnd: number;
    if (source[cursor] === '"' || source[cursor] === "'") {
      const scanned = scanTomlString(source, cursor, false);
      if (scanned.hit || scanned.bail) {
        return scanned;
      }
      segmentEnd = scanned.end;
      segments.push(decodeTomlKey(source, cursor, segmentEnd));
    } else {
      segmentEnd = cursor;
      while (/[A-Za-z0-9_-]/.test(source[segmentEnd] ?? "")) {
        segmentEnd += 1;
      }
      segments.push(source.slice(cursor, segmentEnd));
    }
    const after = skipInlineWs(source, segmentEnd);
    if (source[after] === ".") {
      const next = skipInlineWs(source, after + 1);
      if (canStartInlineKey(source, next)) {
        cursor = next;
        continue;
      }
      return { end: after, hit: { index: after, message: keyStopMessage(stop) }, bail: false };
    }
    return finishKeyStop(source, after, stop);
  }
}

function decodeTomlKey(source: string, start: number, end: number): string {
  const quote = source[start] ?? "";
  if (quote !== '"' && quote !== "'") {
    return source.slice(start, end);
  }
  if (quote === "'") {
    return source.slice(start + 1, end - 1);
  }
  let cursor = start + 1;
  const stop = end - 1;
  let out = "";
  while (cursor < stop) {
    if (source[cursor] !== "\\") {
      out += source[cursor] ?? "";
      cursor += 1;
      continue;
    }
    const esc = source[cursor + 1] ?? "";
    const simple: Record<string, string> = {
      b: "\b",
      f: "\f",
      n: "\n",
      r: "\r",
      t: "\t",
      '"': '"',
      "\\": "\\",
    };
    const decoded = simple[esc];
    if (decoded !== undefined) {
      out += decoded;
      cursor += 2;
      continue;
    }
    if (esc === "u" || esc === "U") {
      const width = esc === "u" ? 4 : 8;
      out += String.fromCodePoint(Number.parseInt(source.slice(cursor + 2, cursor + 2 + width), 16));
      cursor += 2 + width;
      continue;
    }
    out += source[cursor] ?? "";
    cursor += 1;
  }
  return out;
}

function keyStopMessage(stop: KeyStop): string {
  if (stop === "header-std") {
    return HEADER_STD;
  }
  if (stop === "header-array") {
    return HEADER_ARRAY;
  }
  return KEY_EQUALS;
}

function finishKeyStop(source: string, cursor: number, stop: KeyStop): TomlScan {
  if (stop === "equals") {
    if (source[cursor] === "=") {
      return { end: cursor, hit: null, bail: false };
    }
    return { end: cursor, hit: { index: cursor, message: KEY_EQUALS }, bail: false };
  }
  if (stop === "header-array") {
    if (source.startsWith("]]", cursor)) {
      return finishHeaderLine(source, cursor + 2);
    }
    return { end: cursor, hit: { index: cursor, message: HEADER_ARRAY }, bail: false };
  }
  if (source[cursor] === "]") {
    return finishHeaderLine(source, cursor + 1);
  }
  return { end: cursor, hit: { index: cursor, message: HEADER_STD }, bail: false };
}

function finishHeaderLine(source: string, cursor: number): TomlScan {
  const after = skipInlineWs(source, cursor);
  const char = source[after] ?? "";
  if (char === "" || char === "\n" || char === "\r" || char === "#") {
    return { end: cursor, hit: null, bail: false };
  }
  return { end: after, hit: { index: after, message: HEADER_TRAIL }, bail: false };
}

function skipTomlHeader(source: string, index: number, segments: string[]): TomlScan {
  const array = source.startsWith("[[", index);
  let cursor = skipInlineWs(source, index + (array ? 2 : 1));
  if (!canStartInlineKey(source, cursor)) {
    return { end: cursor, hit: { index: cursor, message: INVALID_KEY }, bail: false };
  }
  return scanDottedKey(source, cursor, array ? "header-array" : "header-std", segments);
}

function skipTomlKey(source: string, index: number, segments: string[]): TomlScan {
  const cursor = skipInlineWs(source, index);
  if (!canStartInlineKey(source, cursor)) {
    return { end: cursor, hit: { index: cursor, message: INVALID_KEY }, bail: false };
  }
  return scanDottedKey(source, cursor, "equals", segments);
}

const ESCAPE_SEQUENCE_MESSAGE = "invalid escape sequence\nexpected `b`, `f`, `n`, `r`, `t`, `u`, `U`, `\\`, `\"`";

function isTomlHex(char: string): boolean {
  return (char >= "0" && char <= "9") || (char >= "A" && char <= "F") || (char >= "a" && char <= "f");
}

function stringLabel(multiline: boolean, literal: boolean): string {
  if (multiline && literal) {
    return "invalid multiline literal string";
  }
  if (multiline) {
    return "invalid multiline basic string";
  }
  if (literal) {
    return "invalid literal string";
  }
  return "invalid basic string";
}

/** Tab, printable ASCII other than the delimiter and `\`, and non-ASCII. */
function isTomlStringChar(char: string, literal: boolean): boolean {
  const code = char.codePointAt(0) ?? 0;
  if (code === 0x09 || code >= 0x80) {
    return true;
  }
  if (literal) {
    return (code >= 0x20 && code <= 0x26) || (code >= 0x28 && code <= 0x7e);
  }
  return code === 0x20 || code === 0x21 || (code >= 0x23 && code <= 0x5b) || (code >= 0x5d && code <= 0x7e);
}

/**
 * Figment rejects `\xHH` and `\e` in a basic string, and a `\u` or `\U`
 * that is short or names a surrogate or a code point above U+10FFFF. The
 * caret is the character after the escape letter. Literal strings keep the
 * backslash. A raw control character or newline is `invalid basic string`
 * or `invalid literal string` on that character. Multiline strings still
 * contain newlines.
 */
function scanTomlString(source: string, index: number, allowMultiline = true): TomlScan {
  const multiline = allowMultiline && (source.startsWith('"""', index) || source.startsWith("'''", index));
  const quote = multiline ? source.slice(index, index + 3) : (source[index] ?? "");
  if (quote !== '"' && quote !== "'" && quote !== '"""' && quote !== "'''") {
    return { end: index, hit: null, bail: true };
  }
  const literal = quote.startsWith("'");
  const label = stringLabel(multiline, literal);
  let cursor = index + quote.length;
  if (multiline && source[cursor] === "\r" && source[cursor + 1] === "\n") {
    cursor += 2;
  } else if (multiline && source[cursor] === "\n") {
    cursor += 1;
  }
  while (cursor < source.length) {
    const char = source[cursor] ?? "";
    if (!literal && char === "\\") {
      // A backslash with nothing after it never starts an escape. The string ends there.
      if (cursor + 1 >= source.length) {
        return { end: cursor, hit: { index: cursor, message: label }, bail: false };
      }
      const escaped = scanBasicEscape(source, cursor + 1, multiline);
      if (escaped.hit || escaped.bail) {
        return escaped;
      }
      cursor = escaped.end;
      continue;
    }
    if (source.startsWith(quote, cursor)) {
      return { end: cursor + quote.length, hit: null, bail: false };
    }
    if (multiline && (char === "\n" || (char === "\r" && source[cursor + 1] === "\n"))) {
      cursor += char === "\r" ? 2 : 1;
      continue;
    }
    if (multiline && char === quote[0]) {
      cursor += 1;
      continue;
    }
    if (!isTomlStringChar(char, literal)) {
      return { end: cursor, hit: { index: cursor, message: label }, bail: false };
    }
    cursor += 1;
  }
  return { end: cursor, hit: { index: cursor, message: label }, bail: false };
}

function scanBasicEscape(source: string, index: number, multiline: boolean): TomlScan {
  const char = source[index] ?? "";
  if (char === "b" || char === "f" || char === "n" || char === "r" || char === "t" || char === '"' || char === "\\") {
    return { end: index + 1, hit: null, bail: false };
  }
  if (char === "u" || char === "U") {
    const width = char === "u" ? 4 : 8;
    const label = `invalid unicode ${width}-digit hex code`;
    let hex = "";
    for (let offset = 1; offset <= width; offset += 1) {
      const digit = source[index + offset] ?? "";
      if (!isTomlHex(digit)) {
        return { end: index, hit: { index: index + 1, message: label }, bail: false };
      }
      hex += digit;
    }
    const code = Number.parseInt(hex, 16);
    if (code > 0x10ffff || (code >= 0xd800 && code <= 0xdfff)) {
      return { end: index, hit: { index: index + 1, message: `${label}\nvalue is out of range` }, bail: false };
    }
    return { end: index + 1 + width, hit: null, bail: false };
  }
  if (multiline && (char === " " || char === "\t" || char === "\n" || char === "\r")) {
    let cursor = index;
    while (source[cursor] === " " || source[cursor] === "\t") {
      cursor += 1;
    }
    if (source[cursor] !== "\n" && source[cursor] !== "\r") {
      return escapeSequenceHit(index);
    }
    if (source[cursor] === "\r") {
      cursor += 1;
    }
    if (source[cursor] === "\n") {
      cursor += 1;
    }
    while (source[cursor] === " " || source[cursor] === "\t" || source[cursor] === "\n" || source[cursor] === "\r") {
      cursor += 1;
    }
    return { end: cursor, hit: null, bail: false };
  }
  // `dispatch` consumes the bad letter, so the caret sits on the next character.
  return escapeSequenceHit(index);
}

function escapeSequenceHit(index: number): TomlScan {
  return { end: index, hit: { index: index + 1, message: ESCAPE_SEQUENCE_MESSAGE }, bail: false };
}

function tomlWord(source: string, index: number): string {
  let cursor = index;
  while (/[A-Za-z]/.test(source[cursor] ?? "")) {
    cursor += 1;
  }
  return source.slice(index, cursor);
}

function isTomlDigit(char: string): boolean {
  return char >= "0" && char <= "9";
}
