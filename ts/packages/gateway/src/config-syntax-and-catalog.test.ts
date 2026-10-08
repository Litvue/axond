import assert from "node:assert/strict";

import test from "node:test";


import { envSecretReader, loadConfig, usageBatchSize } from "./config.ts";


const BASE = `
[server]
bind = "127.0.0.1:9"
[storage]
backend = "sqlite"
path = "/tmp/axond.sqlite"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
`;


const secrets = envSecretReader({ GW_KEY: "k" }, async () => "");


const GRAPH = `
[storage]
backend = "sqlite"
path = "/tmp/axond.sqlite"
[[namespace]]
id = "platform"
default = true
[[provider]]
id = "openai"
kind = "openai"
base_url = "http://127.0.0.1:9"
[[credential]]
namespace = "platform"
provider = "openai"
env = "OPENAI_KEY"
id = "primary"
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
`;


test("an unfinished date is a parse error before extract", async () => {
  const label = "invalid date-time";
  const newline = "expected newline, `#`";
  const array = "invalid array\nexpected `]`";
  const diagram = (source: string, index: number, message: string) => {
    const line = source.slice(0, index).split("\n").length;
    const lineStart = source.lastIndexOf("\n", Math.max(0, index - 1)) + 1;
    const lineEnd = source.indexOf("\n", index);
    const content = source.slice(lineStart, lineEnd === -1 ? source.length : lineEnd);
    const column = [...source.slice(lineStart, index)].length;
    const pad = " ".repeat(String(line).length + 1);
    return (
      `config: TOML parse error at line ${line}, column ${column + 1}\n` +
      `${pad}|\n` +
      `${line} | ${content}\n` +
      `${pad}|${" ".repeat(column + 1)}^\n` +
      `${message}\n`
    );
  };
  const reject = async (toml: string, index: number, message: string) => {
    await assert.rejects(
      () => loadConfig(toml, secrets),
      (error: unknown) => {
        assert.equal(error instanceof Error ? error.message : "", diagram(toml, index, message));
        return true;
      },
    );
  };
  const beforeShutdown = `n = 1979-\n[shutdown]\nnope = 1\n${BASE}`;
  const mark = beforeShutdown.indexOf("-") + 1;
  const markMessage = diagram(beforeShutdown, mark, label);
  await reject(beforeShutdown, mark, label);
  assert.equal(markMessage.includes("unknown field"), false);
  assert.equal(markMessage.includes("number too large"), false);
  const later = `n = 1979-\nn = 9223372036854775808\n${BASE}`;
  await reject(later, later.indexOf("-") + 1, label);
  const earlier = `n = 9223372036854775808\nn = 1979-\n${BASE}`;
  await reject(earlier, earlier.indexOf("9223372036854775808"), "number too large to fit in target type");
  const month = `n = 1979-05\n${BASE}`;
  await reject(month, month.indexOf("\n"), label);
  const day = `n = 1979-05-\n${BASE}`;
  await reject(day, day.indexOf("05-") + 3, label);
  const comment = `n = 1979-# c\n${BASE}`;
  await reject(comment, comment.indexOf("#"), label);
  const arr = `n = [1979-]\n${BASE}`;
  await reject(arr, arr.indexOf("]"), label);
  const inline = `n = { a = 1979- }\n${BASE}`;
  await reject(inline, inline.indexOf("-") + 1, label);
  const tee = `n = 1979-05-27T\n${BASE}`;
  await reject(tee, tee.indexOf("T"), newline);
  const junk = `n = 1979-05-27x\n${BASE}`;
  await reject(junk, junk.indexOf("x"), newline);
  const arrTee = `n = [1979-05-27T]\n${BASE}`;
  await reject(arrTee, arrTee.indexOf("T"), array);
  const dated = await loadConfig(`n = 1979-05-27T00:32:00Z\n${BASE}`, secrets);
  assert.equal(dated.storage.path, "/tmp/axond.sqlite");
  const spaced = await loadConfig(`n = 1979-05-27 00:32:00\n${BASE}`, secrets);
  assert.equal(spaced.storage.path, "/tmp/axond.sqlite");
});


test("an unfinished local time is a parse error before extract", async () => {
  const label = "invalid time";
  const diagram = (source: string, index: number, message: string) => {
    const line = source.slice(0, index).split("\n").length;
    const lineStart = source.lastIndexOf("\n", Math.max(0, index - 1)) + 1;
    const lineEnd = source.indexOf("\n", index);
    const content = source.slice(lineStart, lineEnd === -1 ? source.length : lineEnd);
    const column = [...source.slice(lineStart, index)].length;
    const pad = " ".repeat(String(line).length + 1);
    return (
      `config: TOML parse error at line ${line}, column ${column + 1}\n` +
      `${pad}|\n` +
      `${line} | ${content}\n` +
      `${pad}|${" ".repeat(column + 1)}^\n` +
      `${message}\n`
    );
  };
  const reject = async (toml: string, index: number, message: string) => {
    await assert.rejects(
      () => loadConfig(toml, secrets),
      (error: unknown) => {
        assert.equal(error instanceof Error ? error.message : "", diagram(toml, index, message));
        return true;
      },
    );
  };
  const beforeShutdown = `n = 07:\n[shutdown]\nnope = 1\n${BASE}`;
  const mark = beforeShutdown.indexOf(":") + 1;
  const markMessage = diagram(beforeShutdown, mark, label);
  await reject(beforeShutdown, mark, label);
  assert.equal(markMessage.includes("unknown field"), false);
  assert.equal(markMessage.includes("number too large"), false);
  const later = `n = 07:\nn = 9223372036854775808\n${BASE}`;
  await reject(later, later.indexOf(":") + 1, label);
  const earlier = `n = 9223372036854775808\nn = 07:\n${BASE}`;
  await reject(earlier, earlier.indexOf("9223372036854775808"), "number too large to fit in target type");
  const seconds = `n = 07:32:\n${BASE}`;
  await reject(seconds, seconds.indexOf("32:") + 3, label);
  const minute = `n = 07:3\n${BASE}`;
  await reject(minute, minute.indexOf("3"), label);
  const comment = `n = 07:# c\n${BASE}`;
  await reject(comment, comment.indexOf("#"), label);
  const arr = `n = [07:]\n${BASE}`;
  await reject(arr, arr.indexOf("]"), label);
  const inline = `n = { a = 07: }\n${BASE}`;
  await reject(inline, inline.indexOf(":") + 1, label);
  const clock = await loadConfig(`n = 07:32:00\n${BASE}`, secrets);
  assert.equal(clock.storage.path, "/tmp/axond.sqlite");
  const frac = await loadConfig(`n = 07:32:00.5\n${BASE}`, secrets);
  assert.equal(frac.storage.path, "/tmp/axond.sqlite");
});


test("a non-ascii key is a parse error before extract", async () => {
  const equals = "expected `.`, `=`";
  const header = "invalid table header\nexpected `.`, `]`";
  const string = "invalid string\nexpected `\"`, `'`";
  const diagram = (source: string, index: number, message: string) => {
    let at = index;
    let columnOffset = 0;
    if (source.length > 0 && at >= source.length) {
      columnOffset = at - (source.length - 1);
      at = source.length - 1;
    }
    const line = source.slice(0, at).split("\n").length;
    const lineStart = source.lastIndexOf("\n", Math.max(0, at - 1)) + 1;
    const lineEnd = source.indexOf("\n", at);
    const content = source.slice(lineStart, lineEnd === -1 ? source.length : lineEnd);
    const prefix = source.slice(lineStart, at);
    const onChar = index < source.length;
    const codePoint = onChar ? (source.codePointAt(at) ?? 0) : 0;
    const charBytes = codePoint <= 0x7f ? 1 : codePoint <= 0x7ff ? 2 : codePoint <= 0xffff ? 3 : 4;
    const multibyte = onChar && charBytes > 1;
    const utf8 = (value: string) => {
      let bytes = 0;
      for (const char of value) {
        const point = char.codePointAt(0) ?? 0;
        bytes += point <= 0x7f ? 1 : point <= 0x7ff ? 2 : point <= 0xffff ? 3 : 4;
      }
      return bytes;
    };
    const column = (multibyte ? utf8(prefix) : [...prefix].length) + columnOffset;
    let highlight = 1;
    if (multibyte) {
      highlight = Math.min(charBytes, utf8(content) - column);
      if (highlight < 1) {
        highlight = 1;
      }
    }
    const pad = " ".repeat(String(line).length + 1);
    return (
      `config: TOML parse error at line ${line}, column ${column + 1}\n` +
      `${pad}|\n` +
      `${line} | ${content}\n` +
      `${pad}|${" ".repeat(column + 1)}${"^".repeat(highlight)}\n` +
      `${message}\n`
    );
  };
  const reject = async (toml: string, index: number, message: string) => {
    await assert.rejects(
      () => loadConfig(toml, secrets),
      (error: unknown) => {
        assert.equal(error instanceof Error ? error.message : "", diagram(toml, index, message));
        return true;
      },
    );
  };
  const beforeShutdown = `café = 1\n[shutdown]\nnope = 1\n${BASE}`;
  const mark = beforeShutdown.indexOf("é");
  const markMessage = diagram(beforeShutdown, mark, equals);
  await reject(beforeShutdown, mark, equals);
  assert.equal(markMessage.includes("unknown field"), false);
  assert.equal(markMessage.includes("number too large"), false);
  assert.equal(markMessage.includes("^^"), true);
  const later = `café = 1\nn = 9223372036854775808\n${BASE}`;
  await reject(later, later.indexOf("é"), equals);
  const earlier = `n = 9223372036854775808\ncafé = 1\n${BASE}`;
  await reject(earlier, earlier.indexOf("9223372036854775808"), "number too large to fit in target type");
  const euro = `a€ = 1\n${BASE}`;
  await reject(euro, euro.indexOf("€"), equals);
  const emoji = `a😀 = 1\n${BASE}`;
  await reject(emoji, emoji.indexOf("😀"), equals);
  const lead = `éé = 1\n${BASE}`;
  await reject(lead, lead.indexOf("é"), "invalid key");
  const table = `[café]\n${BASE}`;
  await reject(table, table.indexOf("é"), header);
  const value = `n = café\n${BASE}`;
  await reject(value, value.indexOf("c"), string);
  const ascii = await loadConfig(`cafe = 1\n${BASE}`, secrets);
  assert.equal(ascii.storage.path, "/tmp/axond.sqlite");
});


test("a file that ends inside a value is a parse error before extract", async () => {
  const inline = "invalid inline table\nexpected `}`";
  const array = "invalid array\nexpected `]`";
  const string = "invalid string\nexpected `\"`, `'`";
  const basic = "invalid basic string";
  const escape = "invalid escape sequence\nexpected `b`, `f`, `n`, `r`, `t`, `u`, `U`, `\\`, `\"`";
  const diagram = (source: string, index: number, message: string) => {
    let at = index;
    let columnOffset = 0;
    if (source.length > 0 && at >= source.length) {
      columnOffset = at - (source.length - 1);
      at = source.length - 1;
    }
    const line = source.slice(0, at).split("\n").length;
    const lineStart = source.lastIndexOf("\n", Math.max(0, at - 1)) + 1;
    const lineEnd = source.indexOf("\n", at);
    const content = source.slice(lineStart, lineEnd === -1 ? source.length : lineEnd);
    const column = [...source.slice(lineStart, at + 1)].length - 1 + columnOffset;
    const pad = " ".repeat(String(line).length + 1);
    return (
      `config: TOML parse error at line ${line}, column ${column + 1}\n` +
      `${pad}|\n` +
      `${line} | ${content}\n` +
      `${pad}|${" ".repeat(column + 1)}^\n` +
      `${message}\n`
    );
  };
  const reject = async (toml: string, index: number, message: string) => {
    await assert.rejects(
      () => loadConfig(toml, secrets),
      (error: unknown) => {
        assert.equal(error instanceof Error ? error.message : "", diagram(toml, index, message));
        return true;
      },
    );
  };
  const open = "n = { a = 1, b = 2";
  await reject(open, open.length, inline);
  const brace = "n = {";
  await reject(brace, brace.length, inline);
  const missing = "n =";
  await reject(missing, missing.length, "");
  const emptyArray = "n = [";
  await reject(emptyArray, emptyArray.length, array);
  const slash = 'n = "foo\\';
  await reject(slash, slash.length - 1, basic);
  const slashNl = 'n = "foo\\\n';
  await reject(slashNl, slashNl.length, escape);
  const beforeShutdown = `n =\n[shutdown]\nnope = 1\n${BASE}`;
  const newline = beforeShutdown.indexOf("\n");
  const newlineMessage = diagram(beforeShutdown, newline, string);
  await reject(beforeShutdown, newline, string);
  assert.equal(newlineMessage.includes("unknown field"), false);
  assert.equal(newlineMessage.includes("number too large"), false);
  const later = `n =\nn = 9223372036854775808\n${BASE}`;
  await reject(later, later.indexOf("\n"), string);
  const earlier = `n = 9223372036854775808\nn =\n${BASE}`;
  await reject(earlier, earlier.indexOf("9223372036854775808"), "number too large to fit in target type");
  const dup = "n = { a = 1, a = 2";
  await reject(dup, dup.indexOf("{") + 1, "duplicate key `a`");
  const closed = await loadConfig(`n = { a = 1 }\n${BASE}`, secrets);
  assert.equal(closed.storage.path, "/tmp/axond.sqlite");
  const text = await loadConfig(`n = "foo"\n${BASE}`, secrets);
  assert.equal(text.storage.path, "/tmp/axond.sqlite");
});


test("a capital true is a string error before extract", async () => {
  const label = "invalid string\nexpected `\"`, `'`";
  const array = "invalid array\nexpected `]`";
  const newline = "expected newline, `#`";
  const diagram = (source: string, index: number, message: string) => {
    const line = source.slice(0, index).split("\n").length;
    const lineStart = source.lastIndexOf("\n", Math.max(0, index - 1)) + 1;
    const lineEnd = source.indexOf("\n", index);
    const content = source.slice(lineStart, lineEnd === -1 ? source.length : lineEnd);
    const column = [...source.slice(lineStart, index)].length;
    const pad = " ".repeat(String(line).length + 1);
    return (
      `config: TOML parse error at line ${line}, column ${column + 1}\n` +
      `${pad}|\n` +
      `${line} | ${content}\n` +
      `${pad}|${" ".repeat(column + 1)}^\n` +
      `${message}\n`
    );
  };
  const reject = async (toml: string, index: number, message: string) => {
    await assert.rejects(
      () => loadConfig(toml, secrets),
      (error: unknown) => {
        assert.equal(error instanceof Error ? error.message : "", diagram(toml, index, message));
        return true;
      },
    );
  };
  const beforeShutdown = `n = TRUE\n[shutdown]\nnope = 1\n${BASE}`;
  const mark = beforeShutdown.indexOf("T");
  const markMessage = diagram(beforeShutdown, mark, label);
  await reject(beforeShutdown, mark, label);
  assert.equal(markMessage.includes("unknown field"), false);
  assert.equal(markMessage.includes("number too large"), false);
  const later = `n = TRUE\nn = 9223372036854775808\n${BASE}`;
  await reject(later, later.indexOf("T"), label);
  const earlier = `n = 9223372036854775808\nn = TRUE\n${BASE}`;
  await reject(earlier, earlier.indexOf("9223372036854775808"), "number too large to fit in target type");
  const short = `n = t\n${BASE}`;
  await reject(short, short.indexOf("t"), label);
  const partial = `n = tru\n${BASE}`;
  await reject(partial, partial.indexOf("t"), label);
  const arrCap = `n = [T]\n${BASE}`;
  await reject(arrCap, arrCap.indexOf("T"), array);
  const arrLow = `n = [t]\n${BASE}`;
  await reject(arrLow, arrLow.indexOf("t"), label);
  const arrJunk = `n = [truex]\n${BASE}`;
  await reject(arrJunk, arrJunk.indexOf("x"), array);
  const inline = `n = { a = T }\n${BASE}`;
  await reject(inline, inline.indexOf("T"), label);
  const word = `n = truex\n${BASE}`;
  await reject(word, word.indexOf("x"), newline);
  const ok = await loadConfig(`n = true\n${BASE}`, secrets);
  assert.equal(ok.storage.path, "/tmp/axond.sqlite");
});


test("a float past the finite range is a parse error before extract", async () => {
  const message = "invalid floating-point number";
  const diagram = (source: string, index: number) => {
    const line = source.slice(0, index).split("\n").length;
    const lineStart = source.lastIndexOf("\n", Math.max(0, index - 1)) + 1;
    const lineEnd = source.indexOf("\n", index);
    const content = source.slice(lineStart, lineEnd === -1 ? source.length : lineEnd);
    const column = [...source.slice(lineStart, index)].length;
    const pad = " ".repeat(String(line).length + 1);
    return (
      `config: TOML parse error at line ${line}, column ${column + 1}\n` +
      `${pad}|\n` +
      `${line} | ${content}\n` +
      `${pad}|${" ".repeat(column + 1)}^\n` +
      `${message}\n`
    );
  };
  const reject = async (toml: string, index: number) => {
    await assert.rejects(
      () => loadConfig(toml, secrets),
      (error: unknown) => {
        assert.equal(error instanceof Error ? error.message : "", diagram(toml, index));
        return true;
      },
    );
  };
  const beforeShutdown = `n = 1e309\n[shutdown]\nnope = 1\n${BASE}`;
  const floatMessage = diagram(beforeShutdown, beforeShutdown.indexOf("1e309"));
  await reject(beforeShutdown, beforeShutdown.indexOf("1e309"));
  assert.equal(floatMessage.includes("unknown field"), false);
  assert.equal(floatMessage.includes("number too large"), false);
  const later = `n = 1e309\nn = 9223372036854775808\n${BASE}`;
  await reject(later, later.indexOf("1e309"));
  const earlier = `n = 9223372036854775808\nn = 1e309\n${BASE}`;
  await assert.rejects(
    () => loadConfig(earlier, secrets),
    /number too large to fit in target type/,
  );
  const plus = `n = +1e309\n${BASE}`;
  await reject(plus, plus.indexOf("+1e309"));
  const dotted = `n = 1.5e309\n${BASE}`;
  await reject(dotted, dotted.indexOf("1.5e309"));
  const underscored = `n = 1_0e309\n${BASE}`;
  await reject(underscored, underscored.indexOf("1_0e309"));
  const models = `blocklist = { models = [1e309] }\n${BASE}`;
  await reject(models, models.indexOf("1e309"));
  const table = `n = { a = 1e309 }\n${BASE}`;
  await reject(table, table.indexOf("1e309"));
  for (const line of ["n = -1e309", "n = 1e308", "n = 1e-400", "n = inf", "n = 1e20", "n = 0e309"]) {
    const loaded = await loadConfig(`${line}\n${BASE}`, secrets);
    assert.equal(loaded.storage.path, "/tmp/axond.sqlite");
  }
});


test("an impossible calendar day is a parse error before extract", async () => {
  const message = "invalid date-time\nvalue is out of range";
  const diagram = (source: string, index: number) => {
    const line = source.slice(0, index).split("\n").length;
    const lineStart = source.lastIndexOf("\n", Math.max(0, index - 1)) + 1;
    const lineEnd = source.indexOf("\n", index);
    const content = source.slice(lineStart, lineEnd === -1 ? source.length : lineEnd);
    const column = [...source.slice(lineStart, index)].length;
    const pad = " ".repeat(String(line).length + 1);
    return (
      `config: TOML parse error at line ${line}, column ${column + 1}\n` +
      `${pad}|\n` +
      `${line} | ${content}\n` +
      `${pad}|${" ".repeat(column + 1)}^\n` +
      `${message}\n`
    );
  };
  const reject = async (toml: string, index: number) => {
    await assert.rejects(
      () => loadConfig(toml, secrets),
      (error: unknown) => {
        assert.equal(error instanceof Error ? error.message : "", diagram(toml, index));
        return true;
      },
    );
  };
  const beforeShutdown = `n = 2024-02-30\n[shutdown]\nnope = 1\n${BASE}`;
  const day = beforeShutdown.indexOf("2024-02-30") + 8;
  const dateMessage = diagram(beforeShutdown, day);
  await reject(beforeShutdown, day);
  assert.equal(dateMessage.includes("unknown field"), false);
  assert.equal(dateMessage.includes("number too large"), false);
  const later = `n = 2024-02-30T12:00:00Z\nn = 9223372036854775808\n${BASE}`;
  await reject(later, later.indexOf("2024-02-30") + 8);
  const earlier = `n = 9223372036854775808\nn = 2024-02-30\n${BASE}`;
  await assert.rejects(() => loadConfig(earlier, secrets), /number too large to fit in target type/);
  const april = `n = [2024-04-31]\n${BASE}`;
  await reject(april, april.indexOf("2024-04-31") + 8);
  const leap = `n = { a = 2023-02-29 }\n${BASE}`;
  await reject(leap, leap.indexOf("2023-02-29") + 8);
  const century = `n = 2100-02-29\n${BASE}`;
  await reject(century, century.indexOf("2100-02-29") + 8);
  const month = `n = 2024-13-01\n${BASE}`;
  await reject(month, month.indexOf("2024-13-01") + 5);
  for (const line of [
    "n = 2024-02-29",
    "n = 2000-02-29",
    "n = 1900-02-28",
    "n = 2024-04-30",
    'n = "2024-02-30"',
    "n = 07:32:00",
    "n = 00:00:00",
    "n = 0123-01-01",
    "n = 1979-05-27T07:32:00Z",
  ]) {
    const loaded = await loadConfig(`${line}\n${BASE}`, secrets);
    assert.equal(loaded.storage.path, "/tmp/axond.sqlite");
  }
});


test("a leap second is a valid time", async () => {
  for (const line of [
    "n = 2024-01-01T23:59:60Z",
    "n = 09:59:60",
    "n = 07:32:60",
    "n = 23:59:60.5",
    "n = 2024-01-01 23:59:60",
    "n = 2024-01-01t23:59:60z",
    "n = 2024-01-01T23:59:60+00:00",
    "n = [23:59:60]",
    "n = { a = 07:32:60 }",
    'n = "23:59:60"',
  ]) {
    const loaded = await loadConfig(`${line}\n${BASE}`, secrets);
    assert.equal(loaded.storage.path, "/tmp/axond.sqlite");
  }
  const shutdown = `n = 23:59:60\n[shutdown]\nnope = 1\n${BASE}`;
  await assert.rejects(() => loadConfig(shutdown, secrets), /unknown field/);
  const day = `n = 2024-02-30\nn = 23:59:60\n${BASE}`;
  await assert.rejects(() => loadConfig(day, secrets), /invalid date-time/);
  const later = `n = 23:59:60\nn = 9223372036854775808\n${BASE}`;
  await assert.rejects(() => loadConfig(later, secrets), /number too large to fit in target type/);
});


test("an impossible clock is a parse error before extract", async () => {
  const diagram = (source: string, index: number, message: string) => {
    const line = source.slice(0, index).split("\n").length;
    const lineStart = source.lastIndexOf("\n", Math.max(0, index - 1)) + 1;
    const lineEnd = source.indexOf("\n", index);
    const content = source.slice(lineStart, lineEnd === -1 ? source.length : lineEnd);
    const column = [...source.slice(lineStart, index)].length;
    const pad = " ".repeat(String(line).length + 1);
    return (
      `config: TOML parse error at line ${line}, column ${column + 1}\n` +
      `${pad}|\n` +
      `${line} | ${content}\n` +
      `${pad}|${" ".repeat(column + 1)}^\n` +
      `${message}\n`
    );
  };
  const reject = async (toml: string, index: number, message: string) => {
    await assert.rejects(
      () => loadConfig(toml, secrets),
      (error: unknown) => {
        assert.equal(error instanceof Error ? error.message : "", diagram(toml, index, message));
        return true;
      },
    );
  };
  const newline = "expected newline, `#`";
  const time = "invalid time\nvalue is out of range";
  const date = "invalid date-time\nvalue is out of range";
  const offset = "invalid time offset\nvalue is out of range";
  const hour = `n = 24:00:00\n[shutdown]\nnope = 1\n${BASE}`;
  await reject(hour, hour.indexOf(":"), newline);
  assert.equal(diagram(hour, hour.indexOf(":"), newline).includes("unknown field"), false);
  const dated = `n = 2024-01-01T24:00:00\n${BASE}`;
  await reject(dated, dated.indexOf("T"), newline);
  const spaced = `n = 2024-01-01 24:00:00\n${BASE}`;
  await reject(spaced, spaced.indexOf(" 24") + 1, newline);
  const array = `n = [24:00:00]\n${BASE}`;
  await reject(array, array.indexOf(":"), "invalid array\nexpected `]`");
  const minute = `n = 23:60:00\n${BASE}`;
  await reject(minute, minute.indexOf("60"), time);
  const second = `n = 23:59:61\n${BASE}`;
  await reject(second, second.indexOf("61"), time);
  const dateMinute = `n = 2024-01-01T23:60:00\n${BASE}`;
  await reject(dateMinute, dateMinute.indexOf("60"), date);
  const dateSecond = `n = 2024-01-01T23:59:61\n${BASE}`;
  await reject(dateSecond, dateSecond.indexOf("61"), date);
  const off = `n = 2024-01-01T00:00:00+24:00\n${BASE}`;
  await reject(off, off.indexOf("24:00"), offset);
  const offMinute = `n = 2024-01-01T00:00:00+23:60\n${BASE}`;
  await reject(offMinute, offMinute.indexOf("60"), offset);
  const short = `n = 07:32\n${BASE}`;
  await reject(short, short.indexOf("07:32") + "07:32".length, "invalid time");
  const later = `n = 23:60:00\nn = 9223372036854775808\n${BASE}`;
  await reject(later, later.indexOf("60"), time);
  const earlier = `n = 9223372036854775808\nn = 23:60:00\n${BASE}`;
  await assert.rejects(() => loadConfig(earlier, secrets), /number too large to fit in target type/);
  for (const line of ["n = 23:00:00", "n = 23:59:60", "n = 2024-01-01T00:00:00+23:59", "n = 07:32:00"]) {
    const loaded = await loadConfig(`${line}\n${BASE}`, secrets);
    assert.equal(loaded.storage.path, "/tmp/axond.sqlite");
  }
});


test("storage_fields_match_the_rust_boot_refusals", async () => {
  const instead = (pathBlock: string) => BASE.replace('backend = "sqlite"\npath = "/tmp/axond.sqlite"\n', pathBlock);
  const refuse = async (toml: string, pattern: RegExp) => {
    await assert.rejects(() => loadConfig(toml, secrets), pattern);
  };
  await refuse(instead('backend = "sqlite"\npath = ":memory:"\n'), /`\[storage\]` sqlite `:memory:` is not durable; use a file path/);
  await refuse(
    instead('backend = "sqlite"\npath = "  :memory:  "\n'),
    /`\[storage\]` sqlite `:memory:` is not durable; use a file path/,
  );
  await refuse(instead('backend = "sqlite"\npath = "   "\n'), /`\[storage\]` sqlite requires a non-empty `path`/);
  await refuse(instead('backend = "sqlite"\npath = ""\n'), /`\[storage\]` sqlite requires a non-empty `path`/);
  await refuse(
    instead('backend = "sqlite"\npath = "/tmp/axond.sqlite"\ndsn_env = "DSN"\n'),
    /`\[storage\]` sqlite ignores `dsn_env`; omit it or use backend = "postgres"/,
  );
  await refuse(
    instead('backend = "postgres"\ndsn_env = "DSN"\npath = "/tmp/axond.sqlite"\n'),
    /`\[storage\]` postgres ignores `path`; omit it or use backend = "sqlite"/,
  );
  await refuse(
    instead('backend = "postgres"\ndsn_env = "  "\n'),
    /`\[storage\]` postgres requires a non-empty `dsn_env`/,
  );
  await refuse(
    `${BASE}\n[storage.usage_index]\nbuffer_capacity = 0\n`,
    /`\[storage.usage_index\]` buffer_capacity must be at least 1/,
  );
  await refuse(
    `${BASE}\n[storage.usage_index]\nmax_batch = 0\n`,
    /`\[storage.usage_index\]` max_batch must be at least 1/,
  );
  await refuse(
    `${BASE}\n[storage.usage_index]\nbuffer_capacity = 8\nmax_batch = 9\n`,
    /`\[storage.usage_index\]` max_batch \(9\) must not exceed buffer_capacity \(8\)/,
  );
  await refuse(
    `${BASE}\n[storage.usage_index]\nbuffer_capacity = 100000\nmax_batch = 5000\n`,
    /`\[storage.usage_index\]` max_batch \(5000\) must not exceed 4096/,
  );
  await refuse(
    `${BASE}\n[storage.usage_index]\nbuffer_capacity = 2305843009213693952\n`,
    /`\[storage.usage_index\]` buffer_capacity \(2305843009213693952\) must not exceed 2305843009213693951/,
  );
  await refuse(
    `${BASE}\n[storage.usage_index]\nflush_interval_ms = 86400001\n`,
    /`\[storage.usage_index\]` flush_interval_ms \(86400001\) must not exceed 86400000 \(24h\)/,
  );
  await refuse(
    `${instead('backend = "sqlite"\npath = ":memory:"\n')}\n[storage.usage_index]\nbuffer_capacity = 0\n`,
    /buffer_capacity must be at least 1/,
  );
  await refuse(
    `${BASE}\n[storage.usage_index]\nbuffer_capacity = 1.5\n`,
    /config: invalid type: found float `1\.5`, expected usize for key "default\.storage\.usage_index\.buffer_capacity"/,
  );
  await refuse(
    `${BASE}\n[storage.usage_index]\nbuffer_capacity = 1.0\n`,
    /config: invalid type: found float `1`, expected usize for key "default\.storage\.usage_index\.buffer_capacity"/,
  );
  await refuse(
    `${BASE}\n[storage.usage_index]\nbuffer_capacity = -1\n`,
    /config: invalid value signed int `-1`, expected usize for key "default\.storage\.usage_index\.buffer_capacity"/,
  );
  await refuse(
    `${BASE}\n[storage.usage_index]\nmax_batch = "9"\n`,
    /config: invalid type: found string "9", expected usize for key "default\.storage\.usage_index\.max_batch"/,
  );
  await refuse(
    `${BASE}\n[storage.usage_index]\nflush_interval_ms = true\n`,
    /config: invalid type: found bool true, expected u64 for key "default\.storage\.usage_index\.flush_interval_ms"/,
  );
  await refuse(
    `${BASE}\n[storage.usage_index]\nbuffer_capacity = [1]\n`,
    /config: invalid type: found sequence, expected usize/,
  );
  await refuse(
    `${BASE}\n[storage.usage_index]\nbuffer_capacity = 18446744073709551616\n`,
    /TOML parse error at line \d+, column \d+\n[\s\S]*number too large to fit in target type/,
  );

  const tuned = await loadConfig(
    `${BASE}\n[storage.usage_index]\nbuffer_capacity = 64\nmax_batch = 8\nflush_interval_ms = 0\n`,
    secrets,
  );
  assert.equal(tuned.storage.path, "/tmp/axond.sqlite");

  const pending = await loadConfig(
    `
[server]
bind = "127.0.0.1:9"
[storage]
backend = "postgres"
dsn_env = "AXOND_STORAGE_BOOT_DSN"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
`,
    secrets,
  );
  assert.equal(pending.storage.dsnEnv, "AXOND_STORAGE_BOOT_DSN");
  assert.equal(pending.storage.dsn, undefined);

  const whitespaceEnv = await loadConfig(instead('backend = "sqlite"\npath = "/tmp/axond.sqlite"\ndsn_env = "   "\n'), secrets);
  assert.equal(whitespaceEnv.storage.backend, "sqlite");
});


test("credential and gateway key graph matches the rust refusals", async () => {
  const reader = (
    env: Record<string, string | undefined>,
    file: (path: string) => Promise<string> = async () => "file-secret",
  ) => envSecretReader(env, file);
  const ready = { GW_KEY: "k", OPENAI_KEY: "sk" };
  const loaded = await loadConfig(GRAPH, reader(ready));
  assert.equal(loaded.gatewayKey, "k");
  assert.equal(loaded.gatewayKeySource, "env");
  assert.equal(loaded.credentials[0]?.secret, "sk");
  assert.equal(loaded.credentials[0]?.env, "OPENAI_KEY");
  const held = await loadConfig(GRAPH, reader({}), { resolveSecrets: false });
  assert.equal(held.gatewayKey, "");
  assert.equal(held.credentials[0]?.secret, "");

  const reject = async (
    toml: string,
    message: string,
    env: Record<string, string | undefined> = ready,
    file?: (path: string) => Promise<string>,
  ) => {
    await assert.rejects(
      () => loadConfig(toml, reader(env, file)),
      (error: unknown) => {
        assert.equal(error instanceof Error ? error.message : "", message);
        return true;
      },
    );
  };
  const coded =
    (code: string) =>
    async (): Promise<string> => {
      const error = new Error("node");
      (error as { code: string }).code = code;
      throw error;
    };

  await reject(
    GRAPH.replace(
      'namespace = "platform"\nprovider = "openai"\nenv = "OPENAI_KEY"',
      'namespace = "ghost"\nprovider = "openai"\nenv = "OPENAI_KEY"',
    ),
    "credential references undefined namespace `ghost`",
  );
  await reject(
    GRAPH.replace('provider = "openai"\nenv = "OPENAI_KEY"', 'provider = "missing"\nenv = "OPENAI_KEY"'),
    "credential references undefined provider `missing`",
  );
  await reject(
    `${GRAPH}[[credential]]\nnamespace = "platform"\nprovider = "openai"\nenv = "OTHER"\nid = "primary"\n`,
    "duplicate credential id `primary` for namespace `platform` provider `openai`",
  );
  await reject(
    GRAPH.replace('id = "primary"', 'id = "primary"\nweight = 0'),
    "credential `primary` has weight 0; remove it instead",
  );
  await reject(
    GRAPH.replace('env = "OPENAI_KEY"', 'env = "   "'),
    "credential for namespace `platform` provider `openai` has an empty `env`",
  );
  await reject(
    GRAPH.replace('id = "primary"', 'id = ""\nweight = 0'),
    "credential `` has weight 0; remove it instead",
  );
  await reject(
    `${GRAPH.replace(
      'namespace = "platform"\nprovider = "openai"\nenv = "OPENAI_KEY"',
      'namespace = "ghost"\nprovider = "openai"\nenv = "OPENAI_KEY"',
    )}[failover]\nfailure_threshold = 0\n`,
    "failover.failure_threshold must be at least 1",
  );
  await reject(
    `${GRAPH}[[price]]\nprovider = "nope"\nmodel = "a*b"\ninput_microdollars_per_million = 1\noutput_microdollars_per_million = 1\n`,
    "`[[price]]` references undefined provider `nope`",
  );
  await reject(
    `${GRAPH}[[price]]\nmodel = "gpt"\n`,
    'config: missing field `provider` for key "default.price.0"',
  );
  await reject(
    `${GRAPH}[[price]]\nprovider = "openai"\nmodel = "a*b"\ninput_microdollars_per_million = 1\noutput_microdollars_per_million = 1\n`,
    "`[[price]]` model glob `a*b` is invalid: use an exact id, `prefix*`, `*suffix`, or `*`",
  );
  await reject(
    `${GRAPH.replace(
      'namespace = "platform"\nprovider = "openai"\nenv = "OPENAI_KEY"',
      'namespace = "ghost"\nprovider = "openai"\nenv = "OPENAI_KEY"',
    )}[blocklist]\nmodels = ["a*b"]\n`,
    "blocklist glob `a*b` is invalid: use an exact id, `prefix*`, `*suffix`, or `*`",
  );
  await reject(
    GRAPH.replace(
      '[[gateway_key]]\nenv = "GW_KEY"\nnamespace = "platform"',
      '[[gateway_key]]\nenv = "GW_KEY"\nfile = "/tmp/k"\nnamespace = "ghost"',
    ),
    "gateway_key for namespace `ghost` declares both `env` and `file`; exactly one source is permitted",
  );
  await reject(
    GRAPH.replace("env = \"GW_KEY\"\n", ""),
    "gateway_key for namespace `platform` must declare exactly one non-empty source (`env` or `file`)",
  );
  await reject(
    GRAPH.replace(
      '[[gateway_key]]\nenv = "GW_KEY"\nnamespace = "platform"',
      '[[gateway_key]]\nenv = " GW_KEY "\nnamespace = "ghost"',
    ),
    "gateway_key ` GW_KEY ` references undefined namespace `ghost`",
  );
  await reject(
    GRAPH,
    "config resolution failed: credential `primary` for namespace `platform` provider `openai` references env var `OPENAI_KEY`, which is unset or empty",
    { GW_KEY: "k", OPENAI_KEY: "" },
  );
  await reject(
    GRAPH,
    "config resolution failed: gateway_key for namespace `platform` references env var `GW_KEY`, which is unset or empty",
    { OPENAI_KEY: "sk" },
  );
  const fileToml = GRAPH.replace(
    '[[gateway_key]]\nenv = "GW_KEY"\nnamespace = "platform"',
    '[[gateway_key]]\nfile = "/tmp/gateway-key"\nnamespace = "platform"',
  );
  const fromFile = await loadConfig(fileToml, reader({ OPENAI_KEY: "sk" }, async () => "secret\n"));
  assert.equal(fromFile.gatewayKey, "secret\n");
  assert.equal(fromFile.gatewayKeySource, "file");
  await reject(
    fileToml,
    "config resolution failed: gateway_key for namespace `platform` file `/tmp/gateway-key` is empty",
    { OPENAI_KEY: "sk" },
    async () => "",
  );
  await reject(
    fileToml,
    "config resolution failed: gateway_key for namespace `platform` file `/tmp/gateway-key` failed (entity not found): No such file or directory (os error 2)",
    { OPENAI_KEY: "sk" },
    coded("ENOENT"),
  );
  await reject(
    fileToml,
    "config resolution failed: gateway_key for namespace `platform` file `/tmp/gateway-key` failed (is a directory): Is a directory (os error 21)",
    { OPENAI_KEY: "sk" },
    coded("EISDIR"),
  );
  await reject(
    fileToml,
    "config resolution failed: gateway_key for namespace `platform` file `/tmp/gateway-key` failed (permission denied): Permission denied (os error 13)",
    { OPENAI_KEY: "sk" },
    coded("EACCES"),
  );
  await reject(
    fileToml,
    "config resolution failed: gateway_key for namespace `platform` file `/tmp/gateway-key` is not valid UTF-8",
    { OPENAI_KEY: "sk" },
    coded("INVALID_UTF8"),
  );
  await reject(
    fileToml,
    "config resolution failed: gateway_key for namespace `platform` file `/tmp/gateway-key` failed (other error): unknown error",
    { OPENAI_KEY: "sk" },
    coded("EIO"),
  );
});


test("catalogue boot matches the rust refusals", async () => {
  const reader = envSecretReader({ GW_KEY: "k", OPENAI_KEY: "sk" }, async () => "");
  const https =
    "catalog.source_url `http://models.dev/catalog.json` must be `https://`: imported metadata is read for pricing and enablement decisions, so a source that can be substituted in transit is refused rather than trusted";
  const reject = async (extra: string, message: string) => {
    await assert.rejects(
      () => loadConfig(`${GRAPH}${extra}`, reader),
      (error: unknown) => {
        assert.equal(error instanceof Error ? error.message : "", message);
        return true;
      },
    );
  };
  const loaded = await loadConfig(`${GRAPH}[catalog]\nsource = "models-dev"\n`, reader);
  assert.deepEqual(loaded.catalog, { source: "models-dev", sourceUrl: "https://models.dev/catalog.json" });
  const mirror = await loadConfig(
    `${GRAPH}[catalog]\nsource = "models-dev"\nsource_url = "https://mirror.example/models.dev/catalog.json"\n`,
    reader,
  );
  assert.equal(mirror.catalog.sourceUrl, "https://mirror.example/models.dev/catalog.json");
  const queried = await loadConfig(
    `${GRAPH}[catalog]\nsource = "models-dev"\nsource_url = "https://models.dev/catalog.json?x=1"\n`,
    reader,
  );
  assert.equal(queried.catalog.sourceUrl, "https://models.dev/catalog.json?x=1");
  const seeded = await loadConfig(`${GRAPH}[catalog]\nsource = "seed"\n`, reader);
  assert.deepEqual(seeded.catalog, { source: "seed", sourceUrl: null });
  const ignored = await loadConfig(
    `${GRAPH}[catalog]\nsource = "none"\nsource_url = "http://models.dev/catalog.json"\n`,
    reader,
  );
  assert.deepEqual(ignored.catalog, { source: "none" });

  await reject('[catalog]\nsource = "models-dev"\nrefresh_interval_seconds = 0\n', "catalog.refresh_interval_seconds must be at least 1");
  await reject('[catalog]\nsource = "models-dev"\nmax_payload_bytes = 0\n', "catalog.max_payload_bytes must be at least 1");
  await reject(
    '[catalog]\nsource = "models-dev"\nrefresh_interval_seconds = 30\n',
    "catalog: catalogue refresh timeout (60s) must not exceed the interval (30s)",
  );
  await reject(
    '[catalog]\nsource = "models-dev"\nretry_initial_seconds = 600\nretry_max_seconds = 60\n',
    "catalog: backoff.max (60s) must be at least backoff.initial (600s)",
  );
  await reject(
    '[catalog]\nsource = "models-dev"\nrefresh_interval_seconds = 120\n',
    "catalog: catalogue retry ceiling (3600s) must not exceed the refresh interval (120s): a refusing deployment would refresh less often than a healthy one",
  );
  await reject('[catalog]\nsource = "models-dev"\nsource_url = "http://models.dev/catalog.json"\n', https);
  await reject(
    '[catalog]\nsource = "models-dev"\nsource_url = "https://"\n',
    "catalog.source_url is not a valid URL: empty host",
  );
  await reject(
    '[catalog]\nsource = "models-dev"\nsource_url = "not a url"\n',
    "catalog.source_url is not a valid URL: relative URL without a base",
  );
  await reject(
    '[catalog]\nsource = "models-dev"\nsource_url = "https:///catalog.json"\n',
    "catalog.source_url must name an HTTPS host",
  );
  await reject(
    '[catalog]\nsource = "models-dev"\nsource_url = "https://user:secret@mirror.example/catalog.json"\n',
    "catalog.source_url must not contain embedded credentials",
  );
  await reject(
    '[catalog]\nsource = "models-dev"\nsource_url = "https://models.dev/nope"\n',
    "catalog.source_url: `https://models.dev/nope` is not a supported models.dev document; only `/catalog.json` is (`api.json` and `models.json` have different shapes)",
  );
  await reject(
    '[catalog]\nsource = "seed"\nsource_url = "https://models.dev/catalog.json"\n',
    "catalog `seed`: `source_url` applies only to `models-dev`",
  );
  await reject(
    '[catalog]\nsource = "models-dev"\nsource_url = "https://models.dev/nope"\n[[credential]]\nnamespace = "ghost"\nprovider = "openai"\nenv = "OPENAI_KEY"\n',
    "catalog.source_url: `https://models.dev/nope` is not a supported models.dev document; only `/catalog.json` is (`api.json` and `models.json` have different shapes)",
  );
  await reject(
    '[discovery]\nrefresh_interval_seconds = 0\n[catalog]\nsource = "models-dev"\nsource_url = "http://models.dev/catalog.json"\n',
    "discovery.refresh_interval_seconds must be at least 1",
  );
  const dsn =
    "catalog `postgres`: `dsn_env` must name the env var holding the connection string";
  await reject(
    '[catalog]\nsource = "models-dev"\nstore = "postgres"\n',
    dsn,
  );
  await reject(
    '[catalog]\nsource = "seed"\nstore = "postgres"\ndsn_env = "  "\n',
    dsn,
  );
  await reject(
    '[catalog]\nsource = "models-dev"\nsource_url = "http://models.dev/catalog.json"\nstore = "postgres"\n',
    https,
  );
  await reject(
    '[catalog]\nsource = "models-dev"\nrefresh_interval_seconds = 0\nstore = "postgres"\n',
    "catalog.refresh_interval_seconds must be at least 1",
  );
  await reject(
    '[catalog]\nsource = "seed"\nsource_url = "https://models.dev/catalog.json"\nstore = "postgres"\n',
    "catalog `seed`: `source_url` applies only to `models-dev`",
  );
  await reject(
    '[catalog]\nsource = "seed"\nstore = "postgres"\ndsn_env = "CATALOG_DSN"\nschema = "Bad"\n',
    "`catalog.schema`: `Bad` is not a valid table name: use lowercase letters, digits, and underscores",
  );
  await reject(
    '[catalog]\nsource = "seed"\nstore = "postgres"\ndsn_env = "CATALOG_DSN"\nschema = "public.axond"\n',
    "`catalog.schema` must be a single unqualified schema name: it names the search path, not a table",
  );
  await reject(
    '[catalog]\nsource = "seed"\nstore = "postgres"\nschema = "Bad"\n',
    dsn,
  );
  const retained = await loadConfig(
    `${GRAPH}[catalog]\nsource = "models-dev"\nstore = "postgres"\ndsn_env = "CATALOG_DSN"\nschema = "public"\n`,
    reader,
  );
  assert.equal(retained.catalog.source, "models-dev");
  const disabledStore = await loadConfig(
    `${GRAPH}[catalog]\nsource = "none"\nstore = "postgres"\n`,
    reader,
  );
  assert.deepEqual(disabledStore.catalog, { source: "none" });
  const long = `https://mirror.example/${"snapshot/".repeat(20)}not-the-catalogue.json`;
  await assert.rejects(
    () => loadConfig(`${GRAPH}[catalog]\nsource = "models-dev"\nsource_url = "${long}"\n`, reader),
    (error: unknown) => {
      const message = error instanceof Error ? error.message : "";
      assert.equal(message.includes("not-the-catalogue.json"), true);
      assert.equal(message.includes("secret"), false);
      assert.equal(message.startsWith("catalog.source_url: `"), true);
      return true;
    },
  );
});
