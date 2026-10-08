import { TZ_NAMES, TZ_RANGES, TZDB_B64 } from "./tzdb-data.ts";

/**
 * Offset, in seconds east of UTC, for an IANA name in the tzdb the Rust
 * gateway bundles. `null` means `jiff::tz::TimeZone::get` would refuse the name.
 * `Etc/Unknown` is the one special zone outside that table, and it stays at UTC.
 */
export function ianaOffsetSeconds(name: string, unixSeconds: number): number | null {
  if (asciiEq(name, "Etc/Unknown")) {
    return 0;
  }
  const range = findRange(name);
  if (range === null) {
    return null;
  }
  return zone(range.start, range.end).offsetAt(unixSeconds);
}

type Range = { start: number; end: number };

type Civil = { y: number; m: number; d: number; secs: number };

type MonthRule = { month: number; week: number; weekday: number; seconds: number };

type Posix = {
  std: number;
  dst: number | null;
  start: MonthRule | null;
  end: MonthRule | null;
};

type Zone = {
  times: number[];
  types: number[];
  offsets: number[];
  posix: Posix;
  offsetAt(unixSeconds: number): number;
};

const parsed = new Map<number, Zone>();
let bytes: Uint8Array | null = null;

function database(): Uint8Array {
  if (bytes === null) {
    const binary = atob(TZDB_B64);
    const out = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) {
      out[i] = binary.charCodeAt(i);
    }
    bytes = out;
  }
  return bytes;
}

function findRange(name: string): Range | null {
  let lo = 0;
  let hi = TZ_NAMES.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    const order = cmpIgnoreAsciiCase(TZ_NAMES[mid]!, name);
    if (order === 0) {
      return { start: TZ_RANGES[mid * 2]!, end: TZ_RANGES[mid * 2 + 1]! };
    }
    if (order < 0) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }
  return null;
}

function cmpIgnoreAsciiCase(left: string, right: string): number {
  const limit = Math.min(left.length, right.length);
  for (let i = 0; i < limit; i++) {
    const a = asciiLower(left.charCodeAt(i));
    const b = asciiLower(right.charCodeAt(i));
    if (a !== b) {
      return a < b ? -1 : 1;
    }
  }
  if (left.length === right.length) {
    return 0;
  }
  return left.length < right.length ? -1 : 1;
}

function asciiLower(code: number): number {
  return code >= 0x41 && code <= 0x5a ? code + 0x20 : code;
}

function asciiEq(left: string, right: string): boolean {
  return cmpIgnoreAsciiCase(left, right) === 0;
}

function zone(start: number, end: number): Zone {
  const cached = parsed.get(start);
  if (cached) {
    return cached;
  }
  const built = parseZone(database().subarray(start, end));
  parsed.set(start, built);
  return built;
}

function parseZone(blob: Uint8Array): Zone {
  const view = new DataView(blob.buffer, blob.byteOffset, blob.byteLength);
  let off = skipVersion(view, 0);
  const isutcnt = view.getUint32(off + 20, false);
  const isstdcnt = view.getUint32(off + 24, false);
  const leapcnt = view.getUint32(off + 28, false);
  const timecnt = view.getUint32(off + 32, false);
  const typecnt = view.getUint32(off + 36, false);
  const charcnt = view.getUint32(off + 40, false);
  off += 44;
  const times: number[] = [];
  for (let i = 0; i < timecnt; i++) {
    times.push(readI64(view, off));
    off += 8;
  }
  const types: number[] = [];
  for (let i = 0; i < timecnt; i++) {
    types.push(blob[off + i]!);
  }
  off += timecnt;
  const offsets: number[] = [];
  for (let i = 0; i < typecnt; i++) {
    offsets.push(view.getInt32(off, false));
    off += 6;
  }
  off += charcnt + leapcnt * 12 + isstdcnt + isutcnt;
  if (blob[off] !== 0x0a) {
    throw new Error("tzdb footer is missing its leading newline");
  }
  let end = off + 1;
  while (end < blob.length && blob[end] !== 0x0a) {
    end += 1;
  }
  const posix = parsePosix(new TextDecoder().decode(blob.subarray(off + 1, end)));
  const loaded: Zone = {
    times,
    types,
    offsets,
    posix,
    offsetAt(unixSeconds: number): number {
      if (times.length === 0 || unixSeconds >= times[times.length - 1]!) {
        return posixOffset(posix, unixSeconds);
      }
      let lo = 0;
      let hi = times.length;
      while (lo < hi) {
        const mid = (lo + hi) >> 1;
        if (times[mid]! <= unixSeconds) {
          lo = mid + 1;
        } else {
          hi = mid;
        }
      }
      if (lo === 0) {
        return offsets[0]!;
      }
      return offsets[types[lo - 1]!]!;
    },
  };
  return loaded;
}

function skipVersion(view: DataView, off: number): number {
  const timecnt = view.getUint32(off + 32, false);
  const typecnt = view.getUint32(off + 36, false);
  const charcnt = view.getUint32(off + 40, false);
  const leapcnt = view.getUint32(off + 28, false);
  const isstdcnt = view.getUint32(off + 24, false);
  const isutcnt = view.getUint32(off + 20, false);
  return off + 44 + timecnt * 5 + typecnt * 6 + charcnt + leapcnt * 8 + isstdcnt + isutcnt;
}

function readI64(view: DataView, off: number): number {
  const hi = view.getInt32(off, false);
  const lo = view.getUint32(off + 4, false);
  return hi * 0x100000000 + lo;
}

function posixOffset(rule: Posix, unixSeconds: number): number {
  if (rule.dst === null || rule.start === null || rule.end === null) {
    return rule.std;
  }
  const utc = unixToCivil(unixSeconds);
  const start = ruleInstant(rule.start, utc.y, rule.std);
  let end = ruleInstant(rule.end, utc.y, rule.dst);
  if (
    start.m === 1 &&
    start.d === 1 &&
    start.secs === 0 &&
    addSeconds(end, rule.std).y !== utc.y
  ) {
    end = { y: utc.y, m: 12, d: 31, secs: 86400 };
  }
  return inDst(utc, start, end) ? rule.dst : rule.std;
}

function inDst(utc: Civil, start: Civil, end: Civil): boolean {
  if (cmpCivil(start, end) <= 0) {
    return cmpCivil(start, utc) <= 0 && cmpCivil(utc, end) < 0;
  }
  return !(cmpCivil(end, utc) <= 0 && cmpCivil(utc, start) < 0);
}

function ruleInstant(rule: MonthRule, year: number, offset: number): Civil {
  const date = weekdayOfMonth(year, rule.month, rule.week, rule.weekday);
  const delta = rule.seconds - offset;
  const days = Math.floor(delta / 86400);
  const secs = remEuclid(delta, 86400);
  const shifted = addDays(date.y, date.m, date.d, days);
  if (shifted.y < year) {
    return { y: year, m: 1, d: 1, secs: 0 };
  }
  if (shifted.y > year) {
    return { y: year, m: 12, d: 31, secs: 86400 };
  }
  return { y: shifted.y, m: shifted.m, d: shifted.d, secs };
}

function weekdayOfMonth(year: number, month: number, week: number, weekday: number): Civil {
  const firstDow = new Date(Date.UTC(year, month - 1, 1)).getUTCDay();
  let day = 1 + ((weekday - firstDow + 7) % 7);
  if (week === 5) {
    const dim = new Date(Date.UTC(year, month, 0)).getUTCDate();
    while (day + 7 <= dim) {
      day += 7;
    }
  } else {
    day += (week - 1) * 7;
  }
  return { y: year, m: month, d: day, secs: 0 };
}

function unixToCivil(unixSeconds: number): Civil {
  const date = new Date(unixSeconds * 1000);
  return {
    y: date.getUTCFullYear(),
    m: date.getUTCMonth() + 1,
    d: date.getUTCDate(),
    secs: date.getUTCHours() * 3600 + date.getUTCMinutes() * 60 + date.getUTCSeconds(),
  };
}

function addDays(year: number, month: number, day: number, days: number): Civil {
  const date = new Date(Date.UTC(year, month - 1, day + days));
  return { y: date.getUTCFullYear(), m: date.getUTCMonth() + 1, d: date.getUTCDate(), secs: 0 };
}

function addSeconds(civil: Civil, seconds: number): Civil {
  const delta = civil.secs + seconds;
  const days = Math.floor(delta / 86400);
  const secs = remEuclid(delta, 86400);
  const date = addDays(civil.y, civil.m, civil.d, days);
  return { y: date.y, m: date.m, d: date.d, secs };
}

function remEuclid(value: number, divisor: number): number {
  return ((value % divisor) + divisor) % divisor;
}

function cmpCivil(left: Civil, right: Civil): number {
  if (left.y !== right.y) {
    return left.y < right.y ? -1 : 1;
  }
  if (left.m !== right.m) {
    return left.m < right.m ? -1 : 1;
  }
  if (left.d !== right.d) {
    return left.d < right.d ? -1 : 1;
  }
  if (left.secs !== right.secs) {
    return left.secs < right.secs ? -1 : 1;
  }
  return 0;
}

type Cursor = { s: string; i: number };

function parsePosix(spec: string): Posix {
  const cursor: Cursor = { s: spec, i: 0 };
  skipAbbrev(cursor);
  const std = parseOffset(cursor);
  let dst: number | null = null;
  let start: MonthRule | null = null;
  let end: MonthRule | null = null;
  if (cursor.i < cursor.s.length && (isLetter(cursor.s.charCodeAt(cursor.i)) || cursor.s[cursor.i] === "<")) {
    skipAbbrev(cursor);
    dst = std + 3600;
    if (cursor.s[cursor.i] !== ",") {
      dst = parseOffset(cursor);
    }
    expect(cursor, ",");
    start = parseRule(cursor);
    expect(cursor, ",");
    end = parseRule(cursor);
  }
  if (cursor.i !== cursor.s.length) {
    throw new Error(`tzdb posix rule has a tail: ${spec}`);
  }
  return { std, dst, start, end };
}

function parseRule(cursor: Cursor): MonthRule {
  expect(cursor, "M");
  const month = readDigits(cursor, 2);
  expect(cursor, ".");
  const week = readDigits(cursor, 1);
  expect(cursor, ".");
  const weekday = readDigits(cursor, 1);
  let seconds = 2 * 3600;
  if (cursor.s[cursor.i] === "/") {
    cursor.i += 1;
    seconds = parseTransitionTime(cursor);
  }
  return { month, week, weekday, seconds };
}

function parseTransitionTime(cursor: Cursor): number {
  let sign = 1;
  if (cursor.s[cursor.i] === "+") {
    cursor.i += 1;
  } else if (cursor.s[cursor.i] === "-") {
    sign = -1;
    cursor.i += 1;
  }
  const hour = readDigits(cursor, 3);
  let minute = 0;
  let second = 0;
  if (cursor.s[cursor.i] === ":") {
    cursor.i += 1;
    minute = readExactDigits(cursor, 2);
    if (cursor.s[cursor.i] === ":") {
      cursor.i += 1;
      second = readExactDigits(cursor, 2);
    }
  }
  return sign * (hour * 3600 + minute * 60 + second);
}

function parseOffset(cursor: Cursor): number {
  let sign = 1;
  if (cursor.s[cursor.i] === "+") {
    cursor.i += 1;
  } else if (cursor.s[cursor.i] === "-") {
    sign = -1;
    cursor.i += 1;
  }
  const hour = readDigits(cursor, 2);
  let minute = 0;
  let second = 0;
  if (cursor.s[cursor.i] === ":") {
    cursor.i += 1;
    minute = readExactDigits(cursor, 2);
    if (cursor.s[cursor.i] === ":") {
      cursor.i += 1;
      second = readExactDigits(cursor, 2);
    }
  }
  const magnitude = hour * 3600 + minute * 60 + second;
  return sign > 0 ? -magnitude : magnitude;
}

function skipAbbrev(cursor: Cursor): void {
  if (cursor.s[cursor.i] === "<") {
    const close = cursor.s.indexOf(">", cursor.i + 1);
    if (close < 0) {
      throw new Error("tzdb posix abbreviation is unclosed");
    }
    cursor.i = close + 1;
    return;
  }
  const start = cursor.i;
  while (cursor.i < cursor.s.length && isLetter(cursor.s.charCodeAt(cursor.i))) {
    cursor.i += 1;
  }
  if (cursor.i - start < 3) {
    throw new Error(`tzdb posix abbreviation is short: ${cursor.s}`);
  }
}

function readDigits(cursor: Cursor, max: number): number {
  let value = 0;
  let count = 0;
  while (count < max && cursor.i < cursor.s.length && isDigit(cursor.s.charCodeAt(cursor.i))) {
    value = value * 10 + (cursor.s.charCodeAt(cursor.i) - 0x30);
    cursor.i += 1;
    count += 1;
  }
  if (count === 0) {
    throw new Error(`tzdb posix number is missing: ${cursor.s}`);
  }
  return value;
}

function readExactDigits(cursor: Cursor, count: number): number {
  const start = cursor.i;
  const value = readDigits(cursor, count);
  if (cursor.i - start !== count) {
    throw new Error(`tzdb posix number has the wrong width: ${cursor.s}`);
  }
  return value;
}

function expect(cursor: Cursor, token: string): void {
  if (!cursor.s.startsWith(token, cursor.i)) {
    throw new Error(`tzdb posix expected ${token} in ${cursor.s}`);
  }
  cursor.i += token.length;
}

function isLetter(code: number): boolean {
  return (code >= 0x41 && code <= 0x5a) || (code >= 0x61 && code <= 0x7a);
}

function isDigit(code: number): boolean {
  return code >= 0x30 && code <= 0x39;
}
