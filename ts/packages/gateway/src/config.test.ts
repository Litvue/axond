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

test("no usage sink is the stdout default and an inert journal is ignored", async () => {
  const loaded = await loadConfig(BASE, secrets);
  assert.deepEqual(loaded.usageSinks, []);
  const inert = await loadConfig(`${BASE}\n[usage_journal]\nbackend = "none"\nmax_events = 0\n`, secrets);
  assert.deepEqual(inert.usageSinks, []);
});

test("declared usage sinks keep their kinds and postgres defaults", async () => {
  const loaded = await loadConfig(
    `
${BASE}
[[usage_sink]]
kind = "postgres"
dsn_env = "AXOND_USAGE_POSTGRES_DSN"
table = "billing.axond_usage"
create_table = true
max_batch = 250

[[usage_sink]]
kind = "otlp"

[[usage_sink]]
kind = "stdout"
buffer_capacity = 0
max_batch = 0
flush_interval_ms = 0
`,
    secrets,
  );
  assert.equal(loaded.usageSinks.length, 3);
  assert.equal(loaded.usageSinks[0]!.kind, "postgres");
  assert.equal(loaded.usageSinks[0]!.dsnEnv, "AXOND_USAGE_POSTGRES_DSN");
  assert.equal(loaded.usageSinks[0]!.table, "billing.axond_usage");
  assert.equal(loaded.usageSinks[0]!.createTable, true);
  assert.equal(loaded.usageSinks[0]!.maxBatch, 250);
  assert.equal(loaded.usageSinks[0]!.bufferCapacity, 10_000);
  assert.equal(loaded.usageSinks[0]!.flushIntervalMs, 1_000);
  assert.equal(loaded.usageSinks[1]!.kind, "otlp");
  assert.equal(loaded.usageSinks[1]!.table, "axond_usage");
  assert.equal(loaded.usageSinks[2]!.kind, "stdout");
});

test("a postgres sink rejects a missing dsn, a bad table, and a batch that does not fit", async () => {
  await assert.rejects(
    () => loadConfig(`${BASE}\n[[usage_sink]]\nkind = "postgres"\n`, secrets),
    /usage_sink `postgres`: `dsn_env` must name the env var holding the connection string/,
  );
  await assert.rejects(
    () =>
      loadConfig(
        `${BASE}\n[[usage_sink]]\nkind = "postgres"\ndsn_env = "DSN"\ntable = "Usage"\n`,
        secrets,
      ),
    /not a valid table name/,
  );
  await assert.rejects(
    () =>
      loadConfig(
        `${BASE}\n[[usage_sink]]\nkind = "postgres"\ndsn_env = "DSN"\nbuffer_capacity = 99\nmax_batch = 100\n`,
        secrets,
      ),
    /max_batch \(100\) must not exceed buffer_capacity \(99\)/,
  );
  for (const bad of ["max_batch = 0", "buffer_capacity = 0", "flush_interval_ms = 0"]) {
    await assert.rejects(
      () => loadConfig(`${BASE}\n[[usage_sink]]\nkind = "postgres"\ndsn_env = "DSN"\n${bad}\n`, secrets),
      /must be at least 1/,
    );
  }
  const clamped = await loadConfig(
    `${BASE}\n[[usage_sink]]\nkind = "postgres"\ndsn_env = "DSN"\nbuffer_capacity = 100\n`,
    secrets,
  );
  assert.equal(clamped.usageSinks[0]!.maxBatch, 500);
  assert.equal(clamped.usageSinks[0]!.maxBatchExplicit, false);
  assert.equal(usageBatchSize(clamped.usageSinks[0]!), 100);
});

test("a postgres usage sink keeps every digit of a batch above 2^53", async () => {
  const huge = "18446744073709551615";
  const i64Max = "9223372036854775807";
  await assert.rejects(
    () =>
      loadConfig(
        `${BASE}\n[[usage_sink]]\nkind = "postgres"\ndsn_env = "DSN"\nbuffer_capacity = 1\nmax_batch = ${huge}\n`,
        secrets,
      ),
    /TOML parse error at line \d+, column \d+\n[\s\S]*number too large to fit in target type/,
  );
  await assert.rejects(
    () =>
      loadConfig(
        `${BASE}\n[[usage_sink]]\nkind = "postgres"\nbuffer_capacity = ${huge}\n`,
        secrets,
      ),
    /number too large to fit in target type/,
  );
  const loaded = await loadConfig(
    `${BASE}\n[[usage_sink]]\nkind = "postgres"\ndsn_env = "DSN"\nbuffer_capacity = ${i64Max}\nflush_interval_ms = ${i64Max}\n`,
    secrets,
  );
  assert.equal(loaded.usageSinks[0]!.bufferCapacity, Number.MAX_SAFE_INTEGER);
  assert.equal(loaded.usageSinks[0]!.flushIntervalMs, 2_147_483_647);
  assert.equal(loaded.usageSinks[0]!.maxBatch, 500);
  await assert.rejects(
    () =>
      loadConfig(
        BASE,
        envSecretReader(
          {
            GW_KEY: "k",
            AXOND_USAGE_SINK:
              '[{kind="postgres",dsn_env="DSN",buffer_capacity=1,max_batch=' + huge + "}]",
          },
          async () => "",
        ),
      ),
    /max_batch \(18446744073709551615\) must not exceed buffer_capacity \(1\)/,
  );
  await assert.rejects(
    () =>
      loadConfig(
        `${BASE}\n[[usage_sink]]\nkind = "postgres"\ndsn_env = "DSN"\nbuffer_capacity = 1\nmax_batch = ${i64Max}\n`,
        secrets,
      ),
    /max_batch \(9223372036854775807\) must not exceed buffer_capacity \(1\)/,
  );
});

test("a toml integer outside i64 is a parse error before extract", async () => {
  const diagram = (source: string, token: string, message: string) => {
    const index = source.indexOf(token);
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
  const reject = async (toml: string, message: string) => {
    await assert.rejects(
      () => loadConfig(toml, secrets),
      (error: unknown) => {
        assert.equal(error instanceof Error ? error.message : "", message);
        return true;
      },
    );
  };
  const tooLarge = "9223372036854775808";
  const tooSmall = "-9223372036854775809";
  const large = "number too large to fit in target type";
  const small = "number too small to fit in target type";
  const failover = `failover = { overall_timeout_ms = ${tooLarge} }`;
  const beforeShutdown = `${failover}\n[shutdown]\nnope = 1\n${BASE}`;
  await reject(beforeShutdown, diagram(beforeShutdown, tooLarge, large));
  const afterShutdown = `[shutdown]\nnope = 1\n${failover}\n${BASE}`;
  const afterMessage = diagram(afterShutdown, tooLarge, large);
  await reject(afterShutdown, afterMessage);
  assert.equal(afterMessage.includes("unknown field"), false);
  for (const [line, token, message] of [
    [`n = ${tooSmall}`, tooSmall, small],
    [`n = +${tooLarge}`, `+${tooLarge}`, large],
    [`n = 9_223_372_036_854_775_808`, "9_223_372_036_854_775_808", large],
    [`n = 0x8000000000000000`, "0x8000000000000000", large],
    [`n = 0x8000_0000_0000_0000`, "0x8000_0000_0000_0000", large],
    [`n = 0o1000000000000000000000`, "0o1000000000000000000000", large],
    [`n = 0b${"1"}${"0".repeat(63)}`, `0b${"1"}${"0".repeat(63)}`, large],
  ] as const) {
    const source = `${line}\n${BASE}`;
    await reject(source, diagram(source, token, message));
  }
  const kept = await loadConfig(
    `# ${tooLarge}\nlabel = "${tooLarge}"\n${tooLarge} = 1\n[failover]\noverall_timeout_ms = 9223372036854775807\n${BASE}`,
    secrets,
  );
  assert.equal(kept.transport.overallTimeoutMs, 9223372036854775807n);
  const hex = await loadConfig(`${BASE}\n[failover]\noverall_timeout_ms = 0x7fffffffffffffff\n`, secrets);
  assert.equal(hex.transport.overallTimeoutMs, 9223372036854775807n);
  await assert.rejects(
    () => loadConfig(`${BASE}\n[failover]\noverall_timeout_ms = -9223372036854775808\n`, secrets),
    /invalid value signed int `-9223372036854775808`, expected u64/,
  );
  await assert.rejects(
    () => loadConfig(`${BASE}\n[failover]\noverall_timeout_ms = 1e20\n`, secrets),
    (error: unknown) => {
      const message = error instanceof Error ? error.message : "";
      assert.equal(message.includes("TOML parse error"), false);
      assert.match(message, /expected u64/);
      return true;
    },
  );
  const lead = `${BASE}\n[failover]\noverall_timeout_ms = 0922\n`;
  const leadMessage = diagram(lead, "922", "expected newline, `#`");
  await reject(lead, leadMessage);
  assert.equal(leadMessage.includes("number too large"), false);
  assert.equal(leadMessage.includes("leading zero"), false);
  const dated = `[failover]\noverall_timeout_ms = 1979-05-27 07:32:00Z\n${failover}\n${BASE}`;
  await reject(dated, diagram(dated, tooLarge, large));
});

test("a trailing comma in an inline table is a parse error before extract", async () => {
  const inline = "invalid inline table\nexpected `}`";
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
  const reject = async (toml: string, message: string) => {
    await assert.rejects(
      () => loadConfig(toml, secrets),
      (error: unknown) => {
        assert.equal(error instanceof Error ? error.message : "", message);
        return true;
      },
    );
  };
  const comma = `failover = { overall_timeout_ms = 8, }\n[shutdown]\nnope = 1\n${BASE}`;
  const commaMessage = diagram(comma, comma.indexOf(","), inline);
  await reject(comma, commaMessage);
  assert.equal(commaMessage.includes("unknown field"), false);
  assert.equal(commaMessage.includes("number too large"), false);
  const later = `failover = { overall_timeout_ms = 8, }\nn = 9223372036854775808\n${BASE}`;
  await reject(later, diagram(later, later.indexOf(","), inline));
  const huge = `failover = { overall_timeout_ms = 9223372036854775808, }\n${BASE}`;
  await reject(huge, diagram(huge, huge.indexOf("9223372036854775808"), "number too large to fit in target type"));
  const pair = `n = { a = 1, b = 2, }\n${BASE}`;
  await reject(pair, diagram(pair, pair.lastIndexOf(","), inline));
  const array = `n = { a = [1, 2,], }\n${BASE}`;
  await reject(array, diagram(array, array.lastIndexOf(","), inline));
  for (const source of [
    `n = { a = { b = 1, } }\n${BASE}`,
    `n = { a = 1, # c\n}\n${BASE}`,
    `n = {,}\n${BASE}`,
    `n = { a = 1,, }\n${BASE}`,
    `n = { a = 1 , }\n${BASE}`,
    `n = { a = 1,}\n${BASE}`,
    `m = [{ a = 1, }]\n${BASE}`,
    `n = { "a" = 1, }\n${BASE}`,
    `n = { a.b = 1, }\n${BASE}`,
    `n = { a = 1,\n}\n${BASE}`,
  ]) {
    await reject(source, diagram(source, source.indexOf(","), inline));
  }
  const opened = `failover = {\n  overall_timeout_ms = 8\n}\n${BASE}`;
  await reject(opened, diagram(opened, opened.indexOf("\n"), inline));
  const broken = `n = { a = 1\n}\n${BASE}`;
  await reject(broken, diagram(broken, broken.indexOf("\n"), inline));
  const cr = `n = {\r\n}\n${BASE}`;
  await reject(cr, diagram(cr, cr.indexOf("\r"), inline));
  const comment = `n = { a = 8 # c\n}\n${BASE}`;
  await reject(comment, diagram(comment, comment.indexOf("#"), inline));
  const kept = await loadConfig(
    `failover = { overall_timeout_ms = 8, max_attempts = 2 }\nblocklist = { models = ["a",] }\n${BASE}`,
    secrets,
  );
  assert.equal(kept.transport.overallTimeoutMs, 8);
  assert.equal(kept.transport.maxAttempts, 2);
});

test("a hex escape in a string is a parse error before extract", async () => {
  const escape = "invalid escape sequence\nexpected `b`, `f`, `n`, `r`, `t`, `u`, `U`, `\\`, `\"`";
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
  const reject = async (toml: string, message: string) => {
    await assert.rejects(
      () => loadConfig(toml, secrets),
      (error: unknown) => {
        assert.equal(error instanceof Error ? error.message : "", message);
        return true;
      },
    );
  };
  const hexPath = BASE.replace('path = "/tmp/axond.sqlite"', 'path = "data\\x41.db"');
  const beforeShutdown = `${hexPath}\n[shutdown]\nnope = 1\n`;
  const hexMessage = diagram(beforeShutdown, beforeShutdown.indexOf("\\x") + 2, escape);
  await reject(beforeShutdown, hexMessage);
  assert.equal(hexMessage.includes("unknown field"), false);
  assert.equal(hexMessage.includes("number too large"), false);
  const later = `${hexPath}\nn = 9223372036854775808\n`;
  await reject(later, diagram(later, later.indexOf("\\x") + 2, escape));
  const earlier = `n = 9223372036854775808\n${hexPath}`;
  await reject(earlier, diagram(earlier, earlier.indexOf("9223372036854775808"), "number too large to fit in target type"));
  const esc = BASE.replace('id = "platform"', 'id = "plat\\eform"');
  await reject(esc, diagram(esc, esc.indexOf("\\e") + 2, escape));
  const multiline = `n = """hi\\\n\\x41"""\n${BASE}`;
  await reject(multiline, diagram(multiline, multiline.indexOf("\\x") + 2, escape));
  const header = `["hi\\x41"]\n${BASE}`;
  await reject(header, diagram(header, header.indexOf("\\x") + 2, escape));
  const models = `blocklist = { models = ["a\\x41"] }\n${BASE}`;
  await reject(models, diagram(models, models.indexOf("\\x") + 2, escape));
  const key = `"hi\\x41" = 1\n${BASE}`;
  await reject(key, diagram(key, key.indexOf("\\x") + 2, escape));
  const upper = BASE.replace('path = "/tmp/axond.sqlite"', 'path = "\\x4A"');
  await reject(upper, diagram(upper, upper.indexOf("\\x") + 2, escape));
  const nul = BASE.replace('path = "/tmp/axond.sqlite"', 'path = "\\x00"');
  await reject(nul, diagram(nul, nul.indexOf("\\x") + 2, escape));
  const decoded = await loadConfig(BASE.replace('path = "/tmp/axond.sqlite"', 'path = "hi\\u0041.db"'), secrets);
  assert.equal(decoded.storage.path, "hiA.db");
  const wide = await loadConfig(BASE.replace('path = "/tmp/axond.sqlite"', 'path = "hi\\U00000041.db"'), secrets);
  assert.equal(wide.storage.path, "hiA.db");
  const literal = await loadConfig(BASE.replace('path = "/tmp/axond.sqlite"', "path = 'hi\\x41.db'"), secrets);
  assert.equal(literal.storage.path, "hi\\x41.db");
  const kept = await loadConfig(BASE.replace('path = "/tmp/axond.sqlite"', 'path = "hi\\\\x41.db"'), secrets);
  assert.equal(kept.storage.path, "hi\\x41.db");
  const commented = await loadConfig(`# \\x41\n${BASE}`, secrets);
  assert.equal(commented.storage.path, "/tmp/axond.sqlite");
  const letter = BASE.replace('path = "/tmp/axond.sqlite"', 'path = "data\\a.db"');
  await reject(letter, diagram(letter, letter.indexOf("\\a") + 2, escape));
  const other = BASE.replace('id = "platform"', 'id = "plat\\qform"');
  await reject(other, diagram(other, other.indexOf("\\q") + 2, escape));
  const shortHex = BASE.replace('path = "/tmp/axond.sqlite"', 'path = "\\x4"');
  await reject(shortHex, diagram(shortHex, shortHex.indexOf("\\x") + 2, escape));
  const badHex = BASE.replace('path = "/tmp/axond.sqlite"', 'path = "\\xz1"');
  await reject(badHex, diagram(badHex, badHex.indexOf("\\x") + 2, escape));
  const broken = `n = "foo\\\nbar"\n${BASE}`;
  await reject(broken, diagram(broken, broken.indexOf("bar"), escape));
  const spaced = `n = """a\\ b"""\n${BASE}`;
  await reject(spaced, diagram(spaced, spaced.indexOf("\\ ") + 2, escape));
  const literalLetter = await loadConfig(BASE.replace('path = "/tmp/axond.sqlite"', "path = 'hi\\a.db'"), secrets);
  assert.equal(literalLetter.storage.path, "hi\\a.db");
});

test("a short unicode escape is a parse error before extract", async () => {
  const short = "invalid unicode 4-digit hex code";
  const shortWide = "invalid unicode 8-digit hex code";
  const ranged = (label: string) => `${label}\nvalue is out of range`;
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
  const reject = async (toml: string, message: string) => {
    await assert.rejects(
      () => loadConfig(toml, secrets),
      (error: unknown) => {
        assert.equal(error instanceof Error ? error.message : "", message);
        return true;
      },
    );
  };
  const shortPath = BASE.replace('path = "/tmp/axond.sqlite"', 'path = "data\\u41.db"');
  const beforeShutdown = `${shortPath}\n[shutdown]\nnope = 1\n`;
  const shortMessage = diagram(beforeShutdown, beforeShutdown.indexOf("\\u") + 2, short);
  await reject(beforeShutdown, shortMessage);
  assert.equal(shortMessage.includes("unknown field"), false);
  assert.equal(shortMessage.includes("number too large"), false);
  const later = `${shortPath}\nn = 9223372036854775808\n`;
  await reject(later, diagram(later, later.indexOf("\\u") + 2, short));
  const earlier = `n = 9223372036854775808\n${shortPath}`;
  await reject(earlier, diagram(earlier, earlier.indexOf("9223372036854775808"), "number too large to fit in target type"));
  const surrogate = BASE.replace('path = "/tmp/axond.sqlite"', 'path = "data\\uD800.db"');
  await reject(surrogate, diagram(surrogate, surrogate.indexOf("\\u") + 2, ranged(short)));
  const lower = BASE.replace('path = "/tmp/axond.sqlite"', 'path = "data\\ud800.db"');
  await reject(lower, diagram(lower, lower.indexOf("\\u") + 2, ranged(short)));
  const wide = BASE.replace('path = "/tmp/axond.sqlite"', 'path = "data\\U00110000.db"');
  await reject(wide, diagram(wide, wide.indexOf("\\U") + 2, ranged(shortWide)));
  const wideSurrogate = BASE.replace('path = "/tmp/axond.sqlite"', 'path = "data\\U0000D800.db"');
  await reject(wideSurrogate, diagram(wideSurrogate, wideSurrogate.indexOf("\\U") + 2, ranged(shortWide)));
  const seven = BASE.replace('path = "/tmp/axond.sqlite"', 'path = "data\\U0000004.db"');
  await reject(seven, diagram(seven, seven.indexOf("\\U") + 2, shortWide));
  const multiline = `n = """hi\\\n\\u41"""\n${BASE}`;
  await reject(multiline, diagram(multiline, multiline.indexOf("\\u") + 2, short));
  const header = `["hi\\u41"]\n${BASE}`;
  await reject(header, diagram(header, header.indexOf("\\u") + 2, short));
  const models = `blocklist = { models = ["a\\u41"] }\n${BASE}`;
  await reject(models, diagram(models, models.indexOf("\\u") + 2, short));
  const key = `"hi\\u41" = 1\n${BASE}`;
  await reject(key, diagram(key, key.indexOf("\\u") + 2, short));
  const brace = BASE.replace('path = "/tmp/axond.sqlite"', 'path = "\\u{41}"');
  await reject(brace, diagram(brace, brace.indexOf("\\u") + 2, short));
  const spaced = BASE.replace('path = "/tmp/axond.sqlite"', 'path = "\\u 041"');
  await reject(spaced, diagram(spaced, spaced.indexOf("\\u") + 2, short));
  const decoded = await loadConfig(BASE.replace('path = "/tmp/axond.sqlite"', 'path = "hi\\u0041.db"'), secrets);
  assert.equal(decoded.storage.path, "hiA.db");
  const plane = await loadConfig(BASE.replace('path = "/tmp/axond.sqlite"', 'path = "hi\\U0010FFFF.db"'), secrets);
  assert.equal(plane.storage.path, "hi\u{10FFFF}.db");
  const before = await loadConfig(BASE.replace('path = "/tmp/axond.sqlite"', 'path = "hi\\uD7FF.db"'), secrets);
  assert.equal(before.storage.path, "hi\u{D7FF}.db");
  const after = await loadConfig(BASE.replace('path = "/tmp/axond.sqlite"', 'path = "hi\\uE000.db"'), secrets);
  assert.equal(after.storage.path, "hi\u{E000}.db");
  const nul = await loadConfig(BASE.replace('path = "/tmp/axond.sqlite"', 'path = "hi\\u0000.db"'), secrets);
  assert.equal(nul.storage.path, "hi\u0000.db");
  const literal = await loadConfig(BASE.replace('path = "/tmp/axond.sqlite"', "path = 'hi\\u41.db'"), secrets);
  assert.equal(literal.storage.path, "hi\\u41.db");
  const kept = await loadConfig(BASE.replace('path = "/tmp/axond.sqlite"', 'path = "hi\\\\u41.db"'), secrets);
  assert.equal(kept.storage.path, "hi\\u41.db");
  const commented = await loadConfig(`# \\u41\n${BASE}`, secrets);
  assert.equal(commented.storage.path, "/tmp/axond.sqlite");
});

test("a leading zero is a parse error before extract", async () => {
  const document = "expected newline, `#`";
  const array = "invalid array\nexpected `]`";
  const inline = "invalid inline table\nexpected `}`";
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
  const reject = async (toml: string, message: string) => {
    await assert.rejects(
      () => loadConfig(toml, secrets),
      (error: unknown) => {
        assert.equal(error instanceof Error ? error.message : "", message);
        return true;
      },
    );
  };
  const beforeShutdown = `n = 0922\n[shutdown]\nnope = 1\n${BASE}`;
  const zeroMessage = diagram(beforeShutdown, beforeShutdown.indexOf("0922") + 1, document);
  await reject(beforeShutdown, zeroMessage);
  assert.equal(zeroMessage.includes("unknown field"), false);
  assert.equal(zeroMessage.includes("number too large"), false);
  const later = `n = 0922\nn = 9223372036854775808\n${BASE}`;
  await reject(later, diagram(later, later.indexOf("0922") + 1, document));
  const earlier = `n = 9223372036854775808\nn = 0922\n${BASE}`;
  await reject(earlier, diagram(earlier, earlier.indexOf("9223372036854775808"), "number too large to fit in target type"));
  const plus = `n = +08\n${BASE}`;
  await reject(plus, diagram(plus, plus.indexOf("+08") + 2, document));
  const radix = `n = +0x10\n${BASE}`;
  await reject(radix, diagram(radix, radix.indexOf("+0x") + 2, document));
  const under = `n = 0_1\n${BASE}`;
  await reject(under, diagram(under, under.indexOf("0_1") + 1, document));
  const models = `blocklist = { models = [0922] }\n${BASE}`;
  await reject(models, diagram(models, models.indexOf("0922") + 1, array));
  const table = `n = { a = 0922 }\n${BASE}`;
  await reject(table, diagram(table, table.indexOf("0922") + 1, inline));
  const signedInline = `n = { a = +08 }\n${BASE}`;
  await reject(signedInline, diagram(signedInline, signedInline.indexOf("+08") + 2, inline));
  const zero = await loadConfig(`n = 0\n${BASE}`, secrets);
  assert.equal(zero.storage.path, "/tmp/axond.sqlite");
  const fraction = await loadConfig(`n = 0.5\n${BASE}`, secrets);
  assert.equal(fraction.storage.path, "/tmp/axond.sqlite");
  const exponent = await loadConfig(`n = 0e1\n${BASE}`, secrets);
  assert.equal(exponent.storage.path, "/tmp/axond.sqlite");
  const hex = await loadConfig(`n = 0x10\n${BASE}`, secrets);
  assert.equal(hex.storage.path, "/tmp/axond.sqlite");
  const plusZero = await loadConfig(`n = +0\n${BASE}`, secrets);
  assert.equal(plusZero.storage.path, "/tmp/axond.sqlite");
});

test("an underscore outside a digit pair is a parse error before extract", async () => {
  const integer = "invalid integer\nexpected digit";
  const hex = "invalid hexadecimal integer";
  const hexDigit = "invalid hexadecimal integer\nexpected digit";
  const octal = "invalid octal integer";
  const binary = "invalid binary integer\nexpected digit";
  const fraction = "invalid floating-point number\nexpected digit, digit";
  const exponent = "invalid floating-point number\nexpected digit";
  const floatLabel = "invalid floating-point number";
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
  const beforeShutdown = `n = 1__2\n[shutdown]\nnope = 1\n${BASE}`;
  const pair = beforeShutdown.indexOf("__") + 1;
  const pairMessage = diagram(beforeShutdown, pair, integer);
  await reject(beforeShutdown, pair, integer);
  assert.equal(pairMessage.includes("unknown field"), false);
  assert.equal(pairMessage.includes("number too large"), false);
  const later = `n = 1_\nn = 9223372036854775808\n${BASE}`;
  await reject(later, later.indexOf("1_") + 2, integer);
  const earlier = `n = 9223372036854775808\nn = 1__2\n${BASE}`;
  await reject(earlier, earlier.indexOf("9223372036854775808"), "number too large to fit in target type");
  const exponentDigit = `n = 1_e2\n${BASE}`;
  await reject(exponentDigit, exponentDigit.indexOf("1_e") + 2, integer);
  const array = `n = [1__2]\n${BASE}`;
  await reject(array, array.indexOf("__") + 1, integer);
  const table = `n = { a = 1_ }\n${BASE}`;
  await reject(table, table.indexOf("1_ ") + 2, integer);
  const hexPrefix = `n = 0x_1\n${BASE}`;
  await reject(hexPrefix, hexPrefix.indexOf("0x_") + 2, hex);
  const hexTail = `n = 0x1_\n${BASE}`;
  await reject(hexTail, hexTail.indexOf("0x1_") + 4, hexDigit);
  const octalPrefix = `n = 0o_1\n${BASE}`;
  await reject(octalPrefix, octalPrefix.indexOf("0o_") + 2, octal);
  const binaryTail = `n = 0b1_\n${BASE}`;
  await reject(binaryTail, binaryTail.indexOf("0b1_") + 4, binary);
  const badDigit = `n = 0xg\n${BASE}`;
  await reject(badDigit, badDigit.indexOf("0xg") + 2, hex);
  const frac = `n = 1.0_\n${BASE}`;
  await reject(frac, frac.indexOf("1.0_") + 4, fraction);
  const exp = `n = 1e1_\n${BASE}`;
  await reject(exp, exp.indexOf("1e1_") + 4, exponent);
  const expStart = `n = 1e_\n${BASE}`;
  await reject(expStart, expStart.indexOf("1e_") + 2, floatLabel);
  const fracStart = `n = 1._0\n${BASE}`;
  await reject(fracStart, fracStart.indexOf("1._") + 2, exponent);
  for (const line of ["n = 1_000", "n = 0x1_0", "n = 1.0_1", "n = 1e1_0", "n = +1_000", "n = 0o0_755", "n = 0b1_0_1"]) {
    const loaded = await loadConfig(`${line}\n${BASE}`, secrets);
    assert.equal(loaded.storage.path, "/tmp/axond.sqlite");
  }
  await assert.rejects(() => loadConfig(`n = 1_000\n[shutdown]\nnope = 1\n${BASE}`, secrets), /unknown field/);
});

test("an empty dotted key is a parse error before extract", async () => {
  const equals = "expected `.`, `=`";
  const header = "invalid table header\nexpected `.`, `]`";
  const arrayHeader = "invalid table header\nexpected `.`, `]]`";
  const trail = "invalid table header\nexpected newline, `#`";
  const key = "invalid key";
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
  const beforeShutdown = `a..b = 1\n[shutdown]\nnope = 1\n${BASE}`;
  const dot = beforeShutdown.indexOf("..");
  const dotMessage = diagram(beforeShutdown, dot, equals);
  await reject(beforeShutdown, dot, equals);
  assert.equal(dotMessage.includes("unknown field"), false);
  assert.equal(dotMessage.includes("number too large"), false);
  const later = `a. = 1\nn = 9223372036854775808\n${BASE}`;
  await reject(later, later.indexOf("a.") + 1, equals);
  const earlier = `n = 9223372036854775808\na..b = 1\n${BASE}`;
  await reject(earlier, earlier.indexOf("9223372036854775808"), "number too large to fit in target type");
  const mid = `a.b. = 1\n${BASE}`;
  await reject(mid, mid.indexOf("b.") + 1, equals);
  const spaced = `a . . b = 1\n${BASE}`;
  await reject(spaced, spaced.indexOf("."), equals);
  const table = `[a..b]\n${BASE}`;
  await reject(table, table.indexOf(".."), header);
  const array = `[[a.]]\n${BASE}`;
  await reject(array, array.indexOf("a.") + 1, arrayHeader);
  const inline = `n = { a..b = 1 }\n${BASE}`;
  await reject(inline, inline.indexOf(".."), equals);
  const lead = `.a = 1\n${BASE}`;
  await reject(lead, lead.indexOf("."), key);
  const empty = `[]\n${BASE}`;
  await reject(empty, empty.indexOf("]"), key);
  const extra = `[a.b]]\n${BASE}`;
  await reject(extra, extra.indexOf("]]") + 1, trail);
  const broken = `a\n= 1\n${BASE}`;
  await reject(broken, broken.indexOf("\n"), equals);
  for (const line of ["a.b = 1", "a . b = 1", "a.\tb = 1"]) {
    const loaded = await loadConfig(`${line}\n${BASE}`, secrets);
    assert.equal(loaded.storage.path, "/tmp/axond.sqlite");
  }
  const headerOk = await loadConfig(`[ a.b ]\nn = 1\n${BASE}`, secrets);
  assert.equal(headerOk.storage.path, "/tmp/axond.sqlite");
  await assert.rejects(() => loadConfig(`a.b = 1\n[shutdown]\nnope = 1\n${BASE}`, secrets), /unknown field/);
});

test("a control character in a string is a parse error before extract", async () => {
  const basic = "invalid basic string";
  const literal = "invalid literal string";
  const multi = "invalid multiline basic string";
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
  const beforeShutdown = `n = "a\u0001b"\n[shutdown]\nnope = 1\n${BASE}`;
  const mark = beforeShutdown.indexOf("\u0001");
  const markMessage = diagram(beforeShutdown, mark, basic);
  await reject(beforeShutdown, mark, basic);
  assert.equal(markMessage.includes("unknown field"), false);
  assert.equal(markMessage.includes("number too large"), false);
  const newline = `n = "foo\nbar"\n${BASE}`;
  await reject(newline, newline.indexOf("\n"), basic);
  const later = `n = "a\u0001b"\nn = 9223372036854775808\n${BASE}`;
  await reject(later, later.indexOf("\u0001"), basic);
  const earlier = `n = 9223372036854775808\nn = "a\u0001b"\n${BASE}`;
  await reject(earlier, earlier.indexOf("9223372036854775808"), "number too large to fit in target type");
  const quoted = `n = 'a\u0001b'\n${BASE}`;
  await reject(quoted, quoted.indexOf("\u0001"), literal);
  const wide = `n = """a\u0001b"""\n${BASE}`;
  await reject(wide, wide.indexOf("\u0001"), multi);
  const header = `["a\u0001"]\n${BASE}`;
  await reject(header, header.indexOf("\u0001"), basic);
  const comment = `# a\u0001b\n${BASE}`;
  await reject(comment, comment.indexOf("\u0001"), "");
  const deleted = `n = "a\u007fb"\n${BASE}`;
  await reject(deleted, deleted.indexOf("\u007f"), basic);
  const tab = await loadConfig(`n = "a\tb"\n${BASE}`, secrets);
  assert.equal(tab.storage.path, "/tmp/axond.sqlite");
  const lines = await loadConfig(`n = """a\nb"""\n${BASE}`, secrets);
  assert.equal(lines.storage.path, "/tmp/axond.sqlite");
  const noted = await loadConfig(`# a tab\there\n${BASE}`, secrets);
  assert.equal(noted.storage.path, "/tmp/axond.sqlite");
});

test("a duplicate key is a parse error before extract", async () => {
  const rootDup = "duplicate key `n` in document root";
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
  const beforeShutdown = `n = 1\nn = 2\n[shutdown]\nnope = 1\n${BASE}`;
  const mark = beforeShutdown.indexOf("\nn = 2") + 1;
  const markMessage = diagram(beforeShutdown, mark, rootDup);
  await reject(beforeShutdown, mark, rootDup);
  assert.equal(markMessage.includes("unknown field"), false);
  assert.equal(markMessage.includes("number too large"), false);
  const later = `n = 1\nn = 2\nn = 9223372036854775808\n${BASE}`;
  await reject(later, later.indexOf("\nn = 2") + 1, rootDup);
  const earlier = `n = 9223372036854775808\nn = 2\n${BASE}`;
  await reject(earlier, earlier.indexOf("9223372036854775808"), "number too large to fit in target type");
  const header = `[a]\n[a]\n${BASE}`;
  await reject(header, header.indexOf("\n[a]") + 1, "invalid table header\nduplicate key `a` in document root");
  const inline = `n = { a = 1, a = 2 }\n${BASE}`;
  await reject(inline, inline.indexOf("{") + 1, "duplicate key `a`");
  const trailed = `n = { a = 1, a = 2, }\n${BASE}`;
  await reject(trailed, trailed.indexOf("{") + 1, "duplicate key `a`");
  const nested = `[a]\nn = 1\nn = 2\n${BASE}`;
  await reject(nested, nested.lastIndexOf("n = 2"), "duplicate key `n` in table `a`");
  const extend = `a = 1\na.b = 2\n${BASE}`;
  await reject(extend, extend.indexOf("a.b"), "dotted key `a` attempted to extend non-table type (integer)");
  const sub = `[fruit.apple]\n[fruit]\napple.taste = "sweet"\n${BASE}`;
  await reject(sub, sub.indexOf("apple.taste"), "duplicate key `apple`");
  const headerExtend = `a = 1\n[a.b]\n${BASE}`;
  await reject(
    headerExtend,
    headerExtend.indexOf("[a.b]"),
    "invalid table header\ndotted key `a` attempted to extend non-table type (integer)",
  );
  const dotted = await loadConfig(`a.b = 1\na.c = 2\n${BASE}`, secrets);
  assert.equal(dotted.storage.path, "/tmp/axond.sqlite");
});

test("a leading dot or junk after a value is a parse error before extract", async () => {
  const floatLead = "invalid floating-point number\nexpected leading digit";
  const intLead = "invalid integer\nexpected leading digit";
  const newline = "expected newline, `#`";
  const array = "invalid array\nexpected `]`";
  const inline = "invalid inline table\nexpected `}`";
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
  const beforeShutdown = `n = .5\n[shutdown]\nnope = 1\n${BASE}`;
  const mark = beforeShutdown.indexOf(".");
  const markMessage = diagram(beforeShutdown, mark, floatLead);
  await reject(beforeShutdown, mark, floatLead);
  assert.equal(markMessage.includes("unknown field"), false);
  assert.equal(markMessage.includes("number too large"), false);
  const later = `n = .5\nn = 9223372036854775808\n${BASE}`;
  await reject(later, later.indexOf("."), floatLead);
  const earlier = `n = 9223372036854775808\nn = .5\n${BASE}`;
  await reject(earlier, earlier.indexOf("9223372036854775808"), "number too large to fit in target type");
  const under = `n = _1\n${BASE}`;
  await reject(under, under.indexOf("_"), intLead);
  const spaced = `n = 1 _2\n${BASE}`;
  await reject(spaced, spaced.indexOf("_"), newline);
  const hex = `n = 0x1g\n${BASE}`;
  await reject(hex, hex.indexOf("g"), newline);
  const word = `n = truex\n${BASE}`;
  await reject(word, word.indexOf("x"), newline);
  const arrDot = `n = [.5]\n${BASE}`;
  await reject(arrDot, arrDot.indexOf("."), array);
  const arrUnder = `n = [_1]\n${BASE}`;
  await reject(arrUnder, arrUnder.indexOf("_"), array);
  const arrSpace = `n = [1 _2]\n${BASE}`;
  await reject(arrSpace, arrSpace.indexOf("_"), array);
  const inlineDot = `n = { a = .5 }\n${BASE}`;
  await reject(inlineDot, inlineDot.indexOf("."), floatLead);
  const inlineUnder = `n = { a = _1 }\n${BASE}`;
  await reject(inlineUnder, inlineUnder.indexOf("_"), intLead);
  const inlineSpace = `n = { a = 1 _2 }\n${BASE}`;
  await reject(inlineSpace, inlineSpace.indexOf("_"), inline);
  const commented = await loadConfig(`n = 1 # still a value\n${BASE}`, secrets);
  assert.equal(commented.storage.path, "/tmp/axond.sqlite");
  const hexOk = await loadConfig(`n = 0x10\n${BASE}`, secrets);
  assert.equal(hexOk.storage.path, "/tmp/axond.sqlite");
});

test("a sign without a digit is a parse error before extract", async () => {
  const label = "invalid integer";
  const array = "invalid array\nexpected `]`";
  const inline = "invalid inline table\nexpected `}`";
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
  const beforeShutdown = `n = +\n[shutdown]\nnope = 1\n${BASE}`;
  const mark = beforeShutdown.indexOf("+") + 1;
  const markMessage = diagram(beforeShutdown, mark, label);
  await reject(beforeShutdown, mark, label);
  assert.equal(markMessage.includes("unknown field"), false);
  assert.equal(markMessage.includes("number too large"), false);
  const later = `n = +\nn = 9223372036854775808\n${BASE}`;
  await reject(later, later.indexOf("+") + 1, label);
  const earlier = `n = 9223372036854775808\nn = +\n${BASE}`;
  await reject(earlier, earlier.indexOf("9223372036854775808"), "number too large to fit in target type");
  const minus = `n = -\n${BASE}`;
  await reject(minus, minus.indexOf("-") + 1, label);
  const plusDot = `n = +.5\n${BASE}`;
  await reject(plusDot, plusDot.indexOf("."), label);
  const under = `n = +_1\n${BASE}`;
  await reject(under, under.indexOf("_"), label);
  const exponent = `n = +e1\n${BASE}`;
  await reject(exponent, exponent.indexOf("e"), label);
  const spaced = `n = + 1\n${BASE}`;
  await reject(spaced, spaced.indexOf("+") + 1, label);
  const arr = `n = [+]\n${BASE}`;
  await reject(arr, arr.indexOf("+"), array);
  const arrDot = `n = [+.5]\n${BASE}`;
  await reject(arrDot, arrDot.indexOf("+"), array);
  const arrComma = `n = [1, +]\n${BASE}`;
  await reject(arrComma, arrComma.indexOf("+"), array);
  const inlinePlus = `n = { a = + }\n${BASE}`;
  await reject(inlinePlus, inlinePlus.indexOf("+") + 1, label);
  const inlineDot = `n = { a = +.5 }\n${BASE}`;
  await reject(inlineDot, inlineDot.indexOf("."), label);
  const inlineKey = `n = { + }\n${BASE}`;
  await reject(inlineKey, inlineKey.indexOf("+"), inline);
  const hex = `n = +0x1\n${BASE}`;
  await reject(hex, hex.indexOf("x"), newline);
  const word = `n = +infinity\n${BASE}`;
  await reject(word, word.indexOf("infinity") + 3, newline);
  const plus = await loadConfig(`n = +1\n${BASE}`, secrets);
  assert.equal(plus.storage.path, "/tmp/axond.sqlite");
  const negative = await loadConfig(`n = -1\n${BASE}`, secrets);
  assert.equal(negative.storage.path, "/tmp/axond.sqlite");
  const inf = await loadConfig(`n = +inf\n${BASE}`, secrets);
  assert.equal(inf.storage.path, "/tmp/axond.sqlite");
});

test("a signed date is an integer before extract", async () => {
  const newline = "expected newline, `#`";
  const array = "invalid array\nexpected `]`";
  const inline = "invalid inline table\nexpected `}`";
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
  const beforeShutdown = `n = +1979-05-27\n[shutdown]\nnope = 1\n${BASE}`;
  const mark = beforeShutdown.indexOf("-");
  const markMessage = diagram(beforeShutdown, mark, newline);
  await reject(beforeShutdown, mark, newline);
  assert.equal(markMessage.includes("unknown field"), false);
  assert.equal(markMessage.includes("number too large"), false);
  const later = `n = +1979-05-27\nn = 9223372036854775808\n${BASE}`;
  await reject(later, later.indexOf("-"), newline);
  const earlier = `n = 9223372036854775808\nn = +1979-05-27\n${BASE}`;
  await reject(earlier, earlier.indexOf("9223372036854775808"), "number too large to fit in target type");
  const short = `n = 1-2\n${BASE}`;
  await reject(short, short.indexOf("-"), newline);
  const hour = `n = +07:32:00\n${BASE}`;
  await reject(hour, hour.indexOf("7"), newline);
  const huge = `n = +9223372036854775808-01-01\n${BASE}`;
  await reject(huge, huge.indexOf("+"), "number too large to fit in target type");
  const arr = `n = [+1979-05-27]\n${BASE}`;
  await reject(arr, arr.indexOf("-"), array);
  const arrShort = `n = [1-2]\n${BASE}`;
  await reject(arrShort, arrShort.indexOf("-"), array);
  const inlineDate = `n = { a = +1979-05-27 }\n${BASE}`;
  await reject(inlineDate, inlineDate.indexOf("-"), inline);
  const dated = await loadConfig(`n = 1979-05-27\n${BASE}`, secrets);
  assert.equal(dated.storage.path, "/tmp/axond.sqlite");
  const clock = await loadConfig(`n = 07:32:00\n${BASE}`, secrets);
  assert.equal(clock.storage.path, "/tmp/axond.sqlite");
  const year = await loadConfig(`n = 0123-01-01\n${BASE}`, secrets);
  assert.equal(year.storage.path, "/tmp/axond.sqlite");
  const plusYear = await loadConfig(`n = +1979\n${BASE}`, secrets);
  assert.equal(plusYear.storage.path, "/tmp/axond.sqlite");
});

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

test("server bind matches the rust socket address refusal", async () => {
  const withBind = (line: string) => BASE.replace('bind = "127.0.0.1:9"', line);
  const reject = async (toml: string, message: string, env: Record<string, string | undefined> = {}) => {
    await assert.rejects(
      () => loadConfig(toml, envSecretReader({ GW_KEY: "k", ...env }, async () => "")),
      (error: unknown) => {
        assert.equal(error instanceof Error ? error.message : "", message);
        return true;
      },
    );
  };
  const syntax = 'config: invalid socket address syntax for key "default.server.bind"';
  const typed = (found: string) =>
    `config: invalid type: found ${found}, expected socket address for key "default.server.bind"`;
  await reject(withBind('bind = "localhost:8080"'), syntax);
  await reject(withBind('bind = "not-a-socket"'), syntax);
  await reject(withBind('bind = ""'), syntax);
  await reject(withBind('bind = "127.0.0.1"'), syntax);
  await reject(withBind('bind = "127.0.0.1:99999"'), syntax);
  await reject(withBind('bind = "127.0.0.01:80"'), syntax);
  await reject(withBind('bind = "[::1]"'), syntax);
  await reject('[server]\nbind = "localhost:8080"\n', syntax);
  await reject(withBind("bind = 8080"), typed("signed int `8080`"));
  await reject(withBind("bind = -1"), typed("signed int `-1`"));
  await reject(withBind("bind = 1.0"), typed("float `1`"));
  await reject(withBind("bind = 1.5"), typed("float `1.5`"));
  await reject(withBind("bind = inf"), typed("float `inf`"));
  await reject(withBind("bind = nan"), typed("float `NaN`"));
  await reject(withBind("bind = true"), typed("bool true"));
  await reject(withBind('bind = ["127.0.0.1", 8080]'), typed("sequence"));
  await reject(withBind('bind = { host = "127.0.0.1", port = 8080 }'), typed("map"));
  await reject(
    withBind("bind = 9223372036854775808"),
    "config: TOML parse error at line 3, column 8\n" +
      "  |\n" +
      "3 | bind = 9223372036854775808\n" +
      "  |        ^\n" +
      "number too large to fit in target type\n",
  );
  await reject(
    `${withBind('bind = "localhost:8080"')}\n[budget]\nenabled = true\n`,
    syntax,
  );
  await reject('server.bind = "localhost:8080"\n[storage]\npath = "/tmp/axond.sqlite"\n', syntax);
  await reject("server = { bind = 1.0 }\n[storage]\npath = \"/tmp/axond.sqlite\"\n", typed("float `1`"));

  const envSyntax =
    "config: invalid socket address syntax for key \"SERVER.BIND\" in `AXOND_` environment variable(s)";
  const envTyped = (found: string) =>
    `config: invalid type: found ${found}, expected socket address for key "SERVER.BIND" in \`AXOND_\` environment variable(s)`;
  await reject(BASE, envSyntax, { AXOND_SERVER__BIND: "localhost:8080" });
  await reject(BASE, envSyntax, { AXOND_SERVER__BIND: "" });
  await reject(BASE, envSyntax, { axond_server__bind: "not-a-socket" });
  await reject(BASE, envTyped("unsigned int `8080`"), { AXOND_SERVER__BIND: "8080" });
  await reject(BASE, envTyped("unsigned int `8`"), { AXOND_SERVER__BIND: "+8" });
  await reject(BASE, envTyped("signed int `-1`"), { AXOND_SERVER__BIND: "-1" });
  await reject(BASE, envTyped("float `1`"), { AXOND_SERVER__BIND: "1.0" });
  await reject(BASE, envTyped("float `1.5`"), { AXOND_SERVER__BIND: "1.5" });
  await reject(BASE, envTyped("bool true"), { AXOND_SERVER__BIND: "true" });
  await reject(BASE, envTyped("sequence"), { AXOND_SERVER__BIND: "[1]" });
  await reject(BASE, envTyped("map"), { AXOND_SERVER__BIND: "{a=1}" });
  await reject(withBind('bind = "localhost:8080"'), envSyntax, { AXOND_SERVER__BIND: "localhost:8080" });

  const ipv6 = await loadConfig(withBind('bind = "[::1]:080"'), secrets);
  assert.equal(ipv6.bind, "[::1]:80");
  const mapped = await loadConfig(withBind('bind = "[::ffff:127.0.0.1]:80"'), secrets);
  assert.equal(mapped.bind, "[::ffff:127.0.0.1]:80");
  const zero = await loadConfig(withBind('bind = "0.0.0.0:0"'), secrets);
  assert.equal(zero.bind, "0.0.0.0:0");
  const padded = await loadConfig(withBind('bind = "127.0.0.1:08080"'), secrets);
  assert.equal(padded.bind, "127.0.0.1:8080");
  const omitted = await loadConfig(BASE.replace("[server]\nbind = \"127.0.0.1:9\"\n", ""), secrets);
  assert.equal(omitted.bind, "0.0.0.0:8080");
  const fromEnv = await loadConfig(withBind('bind = "localhost:8080"'), envSecretReader(
    { GW_KEY: "k", AXOND_SERVER__BIND: "  127.0.0.1:9  " },
    async () => "",
  ));
  assert.equal(fromEnv.bind, "127.0.0.1:9");
  const quoted = await loadConfig(BASE, envSecretReader(
    { GW_KEY: "k", AXOND_SERVER__BIND: '"[::]:8080"' },
    async () => "",
  ));
  assert.equal(quoted.bind, "[::]:8080");
});

test("process local bounds are refused before credentials and the catalogue", async () => {
  const reader = envSecretReader({ GW_KEY: "k" }, async () => "");
  const base = `
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
namespace = "ghost"
provider = "openai"
env = "OPENAI_KEY"
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
`;
  const reject = async (extra: string, message: string) => {
    await assert.rejects(
      () => loadConfig(`${base}${extra}`, reader),
      (error: unknown) => {
        assert.equal(error instanceof Error ? error.message : "", message);
        return true;
      },
    );
  };
  await reject("[failover]\nmax_attempts = 0\n", "failover.max_attempts must be at least 1");
  await reject(
    "[failover]\nmax_attempts = 1.0\n",
    'config: invalid type: found float `1`, expected u32 for key "default.failover.max_attempts"',
  );
  await reject(
    "[failover]\nmax_attempts = -1\n",
    'config: invalid value signed int `-1`, expected u32 for key "default.failover.max_attempts"',
  );
  await reject(
    "[failover]\noverall_timeout_ms = 1.5\n",
    'config: invalid type: found float `1.5`, expected u64 for key "default.failover.overall_timeout_ms"',
  );
  await reject("[admission]\nmax_request_bytes = 0\n", "admission.max_request_bytes must be at least 1");
  await reject(
    '[failover]\nmax_attempts = 0\n[catalog]\nsource = "models-dev"\nsource_url = "http://models.dev/catalog.json"\n',
    "failover.max_attempts must be at least 1",
  );
  await reject(
    '[admission]\nmax_request_bytes = 0\n[catalog]\nsource = "models-dev"\nsource_url = "http://models.dev/catalog.json"\n',
    "admission.max_request_bytes must be at least 1",
  );
  await reject(
    "[transport]\nconnect_timeout_ms = 0\n[[credential]]\nnamespace = \"ghost\"\nprovider = \"openai\"\nenv = \"OPENAI_KEY\"\n",
    "transport.connect_timeout_ms must be at least 1",
  );
  await reject(
    "[admission]\nmax_request_bytes = 0\n[transport]\nconnect_timeout_ms = 0\nmax_response_bytes = 0\n",
    "admission.max_request_bytes must be at least 1",
  );
  await reject(
    "[transport]\nconnect_timeout_ms = 0\nmax_response_bytes = 0\n",
    "transport.connect_timeout_ms must be at least 1",
  );
  await reject(
    "[transport]\nconnect_timeout_ms = 0\nmax_response_bytes = 100\nmax_error_bytes = 200\n",
    "transport.connect_timeout_ms must be at least 1",
  );
  await reject(
    "[transport]\nconnect_timeout_ms = 0\n[shutdown]\ndeadline_ms = 0\n",
    "transport.connect_timeout_ms must be at least 1",
  );
});

test("extract type errors are figment sentences before later bounds", async () => {
  const reader = envSecretReader({ GW_KEY: "k" }, async () => "");
  const reject = async (toml: string, message: string) => {
    await assert.rejects(
      () => loadConfig(toml, reader),
      (error: unknown) => {
        assert.equal(error instanceof Error ? error.message : "", message);
        return true;
      },
    );
  };
  const typed = (found: string, expected: string, key: string) =>
    `config: invalid type: found ${found}, expected ${expected} for key "${key}"`;
  await reject(
    `${BASE}[admission]\nmax_request_bytes = 1.5\n[failover]\nmax_attempts = 0\n[catalog]\nsource = "models-dev"\nsource_url = "http://models.dev/catalog.json"\n[[credential]]\nnamespace = "ghost"\nprovider = "openai"\nenv = "OPENAI_KEY"\n`,
    typed("float `1.5`", "usize", "default.admission.max_request_bytes"),
  );
  await reject(
    `${BASE.replace("[server]\nbind = \"127.0.0.1:9\"\n", 'server = "x"\n')}[admission]\nmax_request_bytes = 1.0\n`,
    typed("float `1`", "usize", "default.admission.max_request_bytes"),
  );
  await reject(
    BASE.replace("[server]\nbind = \"127.0.0.1:9\"\n", 'server = "x"\n'),
    typed('string "x"', "struct Server", "default.server"),
  );
  await reject(
    BASE.replace("[server]\nbind = \"127.0.0.1:9\"\n", "server = 1\n"),
    typed("signed int `1`", "struct Server", "default.server"),
  );
  await reject(
    BASE.replace("[server]\nbind = \"127.0.0.1:9\"\n", "server = [1]\n"),
    'config: invalid type: found signed int `1`, expected socket address for key "default.server.0"',
  );
  await reject(
    BASE.replace("[server]\nbind = \"127.0.0.1:9\"\n", "server = [1.0]\n"),
    'config: invalid type: found float `1`, expected socket address for key "default.server.0"',
  );
  await reject(
    BASE.replace("[server]\nbind = \"127.0.0.1:9\"\n", 'server = ["localhost:8080"]\n'),
    'config: invalid socket address syntax for key "default.server.0"',
  );
  const fromArray = await loadConfig(
    BASE.replace("[server]\nbind = \"127.0.0.1:9\"\n", 'server = ["0.0.0.0:9", 1]\n'),
    reader,
  );
  assert.equal(fromArray.bind, "0.0.0.0:9");
  const emptyArray = await loadConfig(BASE.replace("[server]\nbind = \"127.0.0.1:9\"\n", "server = []\n"), reader);
  assert.equal(emptyArray.bind, "0.0.0.0:8080");
  await reject(
    `${BASE}[catalog]\nsource = "none"\nrefresh_interval_seconds = 1.0\n`,
    typed("float `1`", "u64", "default.catalog.refresh_interval_seconds"),
  );
  await reject(
    `${BASE}[catalog]\nsource = "nope"\n`,
    "config: unknown variant: found `nope`, expected `one of `none`, `models-dev`, `seed`` for key \"default.catalog.source\"",
  );
  await reject(
    `${BASE}[discovery]\nrefresh_interval_seconds = 1.5\n`,
    typed("float `1.5`", "u64", "default.discovery.refresh_interval_seconds"),
  );
  await reject(
    `${BASE}[[credential]]\nnamespace = "platform"\nprovider = "openai"\nenv = "OPENAI_KEY"\nweight = 1\n[[credential]]\nnamespace = "platform"\nprovider = "openai"\nenv = "OPENAI_KEY"\nweight = 1.0\n`,
    typed("float `1`", "u32", "default.credential.1.weight"),
  );
  await reject(
    `${BASE}[credential_pool]\nstrategy = "nope"\n`,
    "config: unknown variant: found `nope`, expected ``round-robin` or `weighted`` for key \"default.credential_pool.strategy\"",
  );
  await reject(
    `${BASE}[credential_pool]\nstrategy = 1\n`,
    typed("signed int `1`", "enum SelectionStrategy", "default.credential_pool.strategy"),
  );
});

test("a non-table section is refused while figment extracts", async () => {
  const reader = envSecretReader({ GW_KEY: "k" }, async () => "");
  const reject = async (toml: string, message: string) => {
    await assert.rejects(
      () => loadConfig(toml, reader),
      (error: unknown) => {
        assert.equal(error instanceof Error ? error.message : "", message);
        return true;
      },
    );
  };
  const typed = (found: string, expected: string, key: string) =>
    `config: invalid type: found ${found}, expected ${expected} for key "${key}"`;
  await reject(
    `admission = "x"\n${BASE}[failover]\nmax_attempts = 0\n[[credential]]\nnamespace = "ghost"\nprovider = "openai"\nenv = "OPENAI_KEY"\n[catalog]\nsource = "models-dev"\nsource_url = "http://models.dev/catalog.json"\n`,
    typed('string "x"', "struct AdmissionConfigWire", "default.admission"),
  );
  await reject(
    `admission = "y"\n${BASE.replace("[server]\nbind = \"127.0.0.1:9\"\n", 'server = "x"\n')}`,
    typed('string "y"', "struct AdmissionConfigWire", "default.admission"),
  );
  await reject(
    `failover = 1.0\n${BASE}`,
    typed("float `1`", "struct Failover", "default.failover"),
  );
  await reject(
    `shutdown = "y"\n${BASE.replace("[server]\nbind = \"127.0.0.1:9\"\n", 'server = "x"\n')}`,
    typed('string "x"', "struct Server", "default.server"),
  );
  await reject(
    `shutdown = "y"\n${BASE}`,
    typed('string "y"', "struct Shutdown", "default.shutdown"),
  );
  await reject(
    `storage = "x"\n${BASE.replace("[storage]\nbackend = \"sqlite\"\npath = \"/tmp/axond.sqlite\"\n", "")}`,
    typed('string "x"', "struct StorageConfig", "default.storage"),
  );
  await reject(
    BASE.replace("[[namespace]]\nid = \"platform\"\ndefault = true\n", '[namespace]\nid = "platform"\ndefault = true\n'),
    typed("map", "a sequence", "default.namespace"),
  );
  const withoutNamespace = BASE.replace('[[namespace]]\nid = "platform"\ndefault = true\n', "");
  await reject(
    `namespace = ["x"]\n${withoutNamespace}`,
    typed('string "x"', "struct Namespace", "default.namespace.0"),
  );
  await reject(
    `namespace = [1.0]\n${withoutNamespace}`,
    typed("float `1`", "struct Namespace", "default.namespace.0"),
  );
});

test("a struct written as a sequence fills its fields", async () => {
  const reject = async (toml: string, message: string) => {
    await assert.rejects(
      () => loadConfig(toml, secrets),
      (error: unknown) => {
        assert.equal(error instanceof Error ? error.message : "", message);
        return true;
      },
    );
  };
  const typed = (found: string, expected: string, key: string) =>
    `config: invalid type: found ${found}, expected ${expected} for key "${key}"`;
  const withoutStorage = BASE.replace('[storage]\nbackend = "sqlite"\npath = "/tmp/axond.sqlite"\n', "");
  await reject(
    `failover = [0]\n${BASE}[[credential]]\nnamespace = "ghost"\nprovider = "openai"\nenv = "OPENAI_KEY"\n[catalog]\nsource = "models-dev"\nsource_url = "http://models.dev/catalog.json"\n`,
    "failover.max_attempts must be at least 1",
  );
  await reject(`failover = [1.5]\n${BASE}`, typed("float `1.5`", "u32", "default.failover.0"));
  await reject(`failover = [3, 1.0]\n${BASE}`, typed("float `1`", "u64", "default.failover.1"));
  await reject(`admission = [0]\n${BASE}`, "admission.max_request_bytes must be at least 1");
  await reject(
    `discovery = [0]\nfailover = [0]\n${BASE}`,
    "discovery.refresh_interval_seconds must be at least 1",
  );
  await reject(
    `catalog = ["nope"]\ndiscovery = [0]\n${BASE}`,
    "config: unknown variant: found `nope`, expected `one of `none`, `models-dev`, `seed`` for key \"default.catalog.0\"",
  );
  await reject(
    `storage = ["nope"]\n${withoutStorage}`,
    "config: unknown variant: found `nope`, expected ``sqlite` or `postgres`` for key \"default.storage.0\"",
  );
  await reject(
    `storage = ["postgres"]\n${withoutStorage}`,
    "`[storage]` postgres requires a non-empty `dsn_env`",
  );
  await reject(`storage = []\n${withoutStorage}`, "`[storage]` sqlite requires a non-empty `path`");
  await reject(
    `storage = ["sqlite", 1]\n${withoutStorage}`,
    typed("signed int `1`", "a string", "default.storage.1"),
  );
  await reject(
    BASE.replace('path = "/tmp/axond.sqlite"\n', 'path = "/tmp/axond.sqlite"\nusage_index = [0]\n'),
    "`[storage.usage_index]` buffer_capacity must be at least 1",
  );
  await reject(
    BASE.replace('path = "/tmp/axond.sqlite"\n', 'path = "/tmp/axond.sqlite"\nusage_index = [1.5]\n'),
    typed("float `1.5`", "usize", "default.storage.usage_index.0"),
  );
  await reject(
    `shutdown = [0, 0]\n${BASE}`,
    "shutdown.deadline_ms must be at least 1: shutdown waits are bounded",
  );
  await reject(`transport = [0]\n${BASE}`, "transport.connect_timeout_ms must be at least 1");
  await reject(
    `usage_journal = ["postgres"]\n${BASE}`,
    '`[usage_journal] backend = "postgres"` is not built (ADR 0049)',
  );
  await reject(`blocklist = ["gpt"]\n${BASE}`, typed('string "gpt"', "a sequence", "default.blocklist.0"));
  await reject(
    `credential_pool = ["weighted", 0]\n${BASE}`,
    "credential_pool.failure_threshold must be at least 1",
  );

  const attempts = await loadConfig(`failover = [1]\n${BASE}`, secrets);
  assert.equal(attempts.transport.maxAttempts, 1);
  const bytes = await loadConfig(`admission = [1]\n${BASE}`, secrets);
  assert.equal(bytes.maxRequestBytes, 1);
  const drain = await loadConfig(`shutdown = [0]\n${BASE}`, secrets);
  assert.equal(drain.shutdown.drainGraceMs, 0);
  assert.equal(drain.shutdown.deadlineMs, 15_000);
  const catalog = await loadConfig(`catalog = ["models-dev"]\n${BASE}`, secrets);
  assert.deepEqual(catalog.catalog, {
    source: "models-dev",
    sourceUrl: "https://models.dev/catalog.json",
  });
  const bind = await loadConfig(
    BASE.replace('[server]\nbind = "127.0.0.1:9"\n', 'server = ["127.0.0.1:9", 1]\n'),
    secrets,
  );
  assert.equal(bind.bind, "127.0.0.1:9");
  const empty = await loadConfig(`failover = []\n${BASE}`, secrets);
  assert.equal(empty.transport.maxAttempts, 3);
  const withoutNamespace = BASE.replace('[[namespace]]\nid = "platform"\ndefault = true\n', "");
  await reject(
    `namespace = [[1]]\n${withoutNamespace}`,
    typed("signed int `1`", "a string", "default.namespace.0.0"),
  );
  await reject(
    `namespace = [[]]\n${withoutNamespace}`,
    'config: invalid length 0, expected struct Namespace with 3 elements for key "default.namespace.0"',
  );
  await reject(
    `namespace = [["ok", 1]]\n${withoutNamespace}`,
    typed("signed int `1`", "a boolean", "default.namespace.0.1"),
  );
  await reject(
    `failover = [1.5]\nnamespace = [[1]]\n${withoutNamespace}`,
    typed("float `1.5`", "u32", "default.failover.0"),
  );
  await reject(
    `credential = [[1]]\n${BASE}`,
    typed("signed int `1`", "a string", "default.credential.0.0"),
  );
  await reject(
    `credential = [["only"]]\n${BASE}`,
    'config: invalid length 1, expected struct Credential with 5 elements for key "default.credential.0"',
  );
  await reject(
    `credential = [["ns", "p", "ENV", "id", 1.5]]\n${BASE}`,
    typed("float `1.5`", "u32", "default.credential.0.4"),
  );
  await reject(
    `provider = [["openai", "nope"]]\n${BASE}`,
    'config: unknown variant: found `nope`, expected `one of `openai`, `anthropic`, `openai-compatible`` for key "default.provider.0.1"',
  );
  await reject(
    `provider = [["openai"]]\n${BASE}`,
    'config: invalid length 1, expected struct Provider with 4 elements for key "default.provider.0"',
  );
  await reject(
    `gateway_key = [[]]\n${BASE.replace("[[gateway_key]]\nenv = \"GW_KEY\"\nnamespace = \"platform\"\n", "")}`,
    'config: invalid length 2, expected struct GatewayKey with 3 elements for key "default.gateway_key.0"',
  );
  await reject(
    `price = [[1]]\n${BASE}`,
    typed("sequence", "struct PriceRule", "default.price.0"),
  );
  await reject(
    `usage_sink = [[1]]\n${BASE}`,
    typed("signed int `1`", "enum UsageSinkKind", "default.usage_sink.0.0"),
  );
  await reject(
    `usage_sink = [["nope"]]\n${BASE}`,
    'config: unknown variant: found `nope`, expected `one of `stdout`, `postgres`, `otlp`` for key "default.usage_sink.0.0"',
  );
  await reject(
    `usage_sink = [[]]\n${BASE}`,
    'config: invalid length 0, expected struct UsageSinkConfigWire with 7 elements for key "default.usage_sink.0"',
  );
  const named = await loadConfig(`namespace = [["platform", true, false, "extra"]]\n${withoutNamespace}`, secrets);
  assert.equal(named.namespaces[0]?.id, "platform");
  assert.equal(named.namespaces[0]?.default, true);
  assert.equal(named.namespaces[0]?.allowPlatformFallback, false);
  const sink = await loadConfig(`usage_sink = [["stdout"]]\n${BASE}`, secrets);
  assert.equal(sink.usageSinks[0]?.kind, "stdout");
});

test("storage enums and unknown shutdown fields are figment extract errors", async () => {
  const reject = async (toml: string, message: string) => {
    await assert.rejects(
      () => loadConfig(toml, secrets),
      (error: unknown) => {
        assert.equal(error instanceof Error ? error.message : "", message);
        return true;
      },
    );
  };
  const variant = (found: string, expected: string, key: string) =>
    `config: unknown variant: found \`${found}\`, expected \`${expected}\` for key "${key}"`;
  const unknown = (field: string) =>
    "config: unknown field: found `" +
    field +
    "`, expected `one of `drain_grace_ms`, `deadline_ms`, `flush_timeout_ms`` for key \"default.shutdown." +
    field +
    "\"";
  await reject(
    BASE.replace('backend = "sqlite"', 'backend = "nope"') + "[failover]\nmax_attempts = 0\n",
    variant("nope", "`sqlite` or `postgres`", "default.storage.backend"),
  );
  await reject(
    BASE.replace('[storage]\nbackend = "sqlite"\npath = "/tmp/axond.sqlite"\n', '[storage]\nbackend = "nope"\n'),
    variant("nope", "`sqlite` or `postgres`", "default.storage.backend"),
  );
  await reject(
    BASE.replace(
      'backend = "sqlite"\npath = "/tmp/axond.sqlite"\n',
      'on_unavailable = "nope"\nbackend = "nope"\npath = "/tmp/axond.sqlite"\n',
    ),
    variant("nope", "`sqlite` or `postgres`", "default.storage.backend"),
  );
  await reject(
    BASE.replace('[storage]\nbackend = "sqlite"\npath = "/tmp/axond.sqlite"\n', '[storage]\nbackend = "sqlite"\non_unavailable = "nope"\n'),
    variant("nope", "`deny` or `allow`", "default.storage.on_unavailable"),
  );
  await reject(
    BASE.replace(
      'backend = "sqlite"\npath = "/tmp/axond.sqlite"\n',
      'create_table = 1.5\nbackend = "nope"\npath = "/tmp/axond.sqlite"\n',
    ),
    variant("nope", "`sqlite` or `postgres`", "default.storage.backend"),
  );
  await reject(
    BASE.replace('path = "/tmp/axond.sqlite"\n', 'path = "/tmp/axond.sqlite"\ncreate_table = 1.5\n'),
    'config: invalid type: found float `1.5`, expected a boolean for key "default.storage.create_table"',
  );
  await reject(
    `${BASE}[shutdown]\nnope = 1\n[admission]\nmax_request_bytes = 1.5\n[failover]\nmax_attempts = 0\n`,
    'config: invalid type: found float `1.5`, expected usize for key "default.admission.max_request_bytes"',
  );
  await reject(`${BASE}[shutdown]\nnope = 1\naaa = 1\n`, unknown("aaa"));
  await reject(
    `${BASE}[shutdown]\nnope = 1\ndrain_grace_ms = 1.5\n`,
    'config: invalid type: found float `1.5`, expected u64 for key "default.shutdown.drain_grace_ms"',
  );
  await reject(
    `${BASE}[shutdown]\naaa = 1\ndrain_grace_ms = 1.5\n`,
    unknown("aaa"),
  );
  await reject(
    `${BASE}[shutdown]\ndrain_grace_ms = 1.5\ndeadline_ms = 1.5\n`,
    'config: invalid type: found float `1.5`, expected u64 for key "default.shutdown.deadline_ms"',
  );
  await reject(
    `${BASE}[failover]\nmax_attempts = 1.5\n[shutdown]\nnope = 1\n`,
    'config: invalid type: found float `1.5`, expected u32 for key "default.failover.max_attempts"',
  );
  await reject(
    `${BASE}[transport]\nconnect_timeout_ms = 1.5\n[shutdown]\nnope = 1\n[admission]\nmax_request_bytes = 0\n`,
    unknown("nope"),
  );
  await reject(
    `${BASE}[transport]\nbuffered_body_timeout_ms = 1.5\nconnect_timeout_ms = 1.5\n`,
    'config: invalid type: found float `1.5`, expected u64 for key "default.transport.buffered_body_timeout_ms"',
  );
  await reject(
    `${BASE}[transport]\nmax_response_bytes = 1.5\nconnect_timeout_ms = 1.0\n`,
    'config: invalid type: found float `1`, expected u64 for key "default.transport.connect_timeout_ms"',
  );
  await reject(
    BASE.replace("[server]\nbind = \"127.0.0.1:9\"\n", "[server]\nbind = 1\n").replace(
      'path = "/tmp/axond.sqlite"\n',
      'path = "/tmp/axond.sqlite"\ncreate_table = 1.5\n',
    ) + "[shutdown]\nnope = 1\n",
    'config: invalid type: found signed int `1`, expected socket address for key "default.server.bind"',
  );
  await reject(
    BASE.replace('path = "/tmp/axond.sqlite"\n', "path = 1\ncreate_table = 1.5\n"),
    'config: invalid type: found float `1.5`, expected a boolean for key "default.storage.create_table"',
  );
  await reject(
    BASE.replace(
      'path = "/tmp/axond.sqlite"\n',
      'path = "/tmp/axond.sqlite"\n[storage.usage_index]\nbuffer_capacity = 8\nflush_interval_ms = 1.5\nmax_batch = 1.5\n',
    ),
    'config: invalid type: found float `1.5`, expected u64 for key "default.storage.usage_index.flush_interval_ms"',
  );
  await reject(
    `${BASE}[failover]\nmax_attempts = 1.5\n[transport]\nconnect_timeout_ms = 1.5\n`,
    'config: invalid type: found float `1.5`, expected u32 for key "default.failover.max_attempts"',
  );
  await reject(
    BASE.replace('path = "/tmp/axond.sqlite"', "path = 1"),
    'config: invalid type: found signed int `1`, expected a string for key "default.storage.path"',
  );
  await reject(
    BASE.replace('backend = "sqlite"\npath = "/tmp/axond.sqlite"', 'path = 1\nbackend = "nope"'),
    variant("nope", "`sqlite` or `postgres`", "default.storage.backend"),
  );
});

test("catalog credential and namespace keys follow figment order", async () => {
  const reject = async (toml: string, message: string) => {
    await assert.rejects(
      () => loadConfig(toml, secrets),
      (error: unknown) => {
        assert.equal(error instanceof Error ? error.message : "", message);
        return true;
      },
    );
  };
  const typed = (found: string, expected: string, key: string) =>
    `config: invalid type: found ${found}, expected ${expected} for key "${key}"`;
  const variant = (found: string, expected: string, key: string) =>
    `config: unknown variant: found \`${found}\`, expected \`${expected}\` for key "${key}"`;
  await reject(
    `${BASE}[catalog]\ncreate_table = 1.5\nrefresh_interval_seconds = 1.5\n`,
    typed("float `1.5`", "a boolean", "default.catalog.create_table"),
  );
  await reject(
    `${BASE}[catalog]\nbootstrap = "nope"\nsource = "nope"\nrefresh_interval_seconds = 1.5\n`,
    variant("nope", "`empty` or `seed`", "default.catalog.bootstrap"),
  );
  await reject(
    `${BASE}[catalog]\nstore = "nope"\n`,
    variant("nope", "`in-memory` or `postgres`", "default.catalog.store"),
  );
  await reject(
    `${BASE}[catalog]\nsource = "none"\nsource_url = 1\n`,
    typed("signed int `1`", "a string", "default.catalog.source_url"),
  );
  await reject(
    `${BASE}[catalog]\nconnect_timeout_ms = 1.5\nsource = "nope"\n`,
    typed("float `1.5`", "u64", "default.catalog.connect_timeout_ms"),
  );
  const kept = await loadConfig(
    `${BASE}[catalog]\nbootstrap = "empty"\nstore = "in-memory"\ncreate_table = false\nsource = "none"\n`,
    secrets,
  );
  assert.deepEqual(kept.catalog, { source: "none" });
  await reject(
    `${BASE}[[credential]]\nnamespace = 1\nweight = 1.5\n`,
    typed("signed int `1`", "a string", "default.credential.0.namespace"),
  );
  await reject(
    `${BASE}[[credential]]\nenv = 1\nweight = 1.5\n`,
    typed("signed int `1`", "a string", "default.credential.0.env"),
  );
  await reject(
    `${BASE}[admission]\nmax_request_bytes = 1.5\n[[credential]]\nnamespace = "platform"\nprovider = "openai"\nenv = "OPENAI_KEY"\nweight = 1.5\n`,
    typed("float `1.5`", "usize", "default.admission.max_request_bytes"),
  );
  await reject(
    BASE.replace('id = "platform"\ndefault = true\n', "allow_platform_fallback = 1.5\nid = 1\ndefault = 1.5\n"),
    typed("float `1.5`", "a boolean", "default.namespace.0.allow_platform_fallback"),
  );
  await reject(
    BASE.replace('id = "platform"', "id = 1").replace('bind = "127.0.0.1:9"', "bind = 1"),
    typed("signed int `1`", "a string", "default.namespace.0.id"),
  );
  await reject(
    BASE.replace('env = "GW_KEY"\n', 'file = 1\nenv = "GW_KEY"\n'),
    typed("signed int `1`", "a string", "default.gateway_key.0.file"),
  );
});

test("blocklist price provider and usage keys follow figment order", async () => {
  const reject = async (toml: string, message: string) => {
    await assert.rejects(
      () => loadConfig(toml, secrets),
      (error: unknown) => {
        assert.equal(error instanceof Error ? error.message : "", message);
        return true;
      },
    );
  };
  const typed = (found: string, expected: string, key: string) =>
    `config: invalid type: found ${found}, expected ${expected} for key "${key}"`;
  const variant = (found: string, expected: string, key: string) =>
    `config: unknown variant: found \`${found}\`, expected \`${expected}\` for key "${key}"`;
  await reject(
    `${BASE}[blocklist]\nmodels = [1]\n[catalog]\ncreate_table = 1.5\n`,
    typed("signed int `1`", "a string", "default.blocklist.models.0"),
  );
  await reject(
    `${BASE}[admission]\nmax_request_bytes = 1.5\n[blocklist]\nmodels = [1]\n`,
    typed("float `1.5`", "usize", "default.admission.max_request_bytes"),
  );
  await reject(`${BASE}[blocklist]\nmodels = 1\n`, typed("signed int `1`", "a sequence", "default.blocklist.models"));
  const listed = await loadConfig(`${BASE}[blocklist]\nmodels = ["gpt-4"]\n`, secrets);
  assert.deepEqual(listed.blocklist, ["gpt-4"]);
  await reject(
    `${BASE}[[price]]\nmodel = 1\nprovider = "openai"\ninput_microdollars_per_million = 1.5\noutput_microdollars_per_million = 1\n`,
    typed("signed int `1`", "a string", "default.price.0.model"),
  );
  await reject(
    `${BASE}[[provider]]\nid = "openai"\nkind = "openai"\nbase_url = "http://127.0.0.1:9"\n[[price]]\nprovider = "openai"\nmodel = "*"\ninput_microdollars_per_million = 1.5\noutput_microdollars_per_million = 1\n`,
    typed("float `1.5`", "u64", "default.price.0"),
  );
  await reject(
    `${BASE}[[price]]\nprovider = "openai"\ninput_microdollars_per_million = 1.5\noutput_microdollars_per_million = 1\n`,
    'config: missing field `model` for key "default.price.0"',
  );
  const priced = await loadConfig(
    `${BASE}[[provider]]\nid = "openai"\nkind = "openai"\nbase_url = "http://127.0.0.1:9"\n[[price]]\nprovider = "openai"\nmodel = "*"\ninput_microdollars_per_million = 1\noutput_microdollars_per_million = 2\n`,
    secrets,
  );
  assert.equal(priced.prices[0]?.inputMicrodollarsPerMillion, 1n);
  await reject(
    BASE.replace('bind = "127.0.0.1:9"', "bind = 1") +
      '[[provider]]\nbase_url = 1\nid = "openai"\nkind = "openai"\n',
    typed("signed int `1`", "a string", "default.provider.0.base_url"),
  );
  await reject(
    `${BASE}[[provider]]\nid = "openai"\nkind = "nope"\nbase_url = "http://127.0.0.1:9"\n`,
    variant("nope", "one of `openai`, `anthropic`, `openai-compatible`", "default.provider.0.kind"),
  );
  await reject(
    `${BASE}[[provider]]\nid = "openai"\nkind = "openai"\nbase_url = "http://127.0.0.1:9"\nunpriced_models = "nope"\n`,
    variant("nope", "`deny` or `allow`", "default.provider.0.unpriced_models"),
  );
  const allowed = await loadConfig(
    `${BASE}[[provider]]\nid = "openai"\nkind = "openai"\nbase_url = "http://127.0.0.1:9"\nunpriced_models = "allow"\n`,
    secrets,
  );
  assert.equal(allowed.providers[0]?.unpricedModels, "allow");
  await reject(
    `${BASE}[usage_journal]\nbackend = "none"\ncreate_schema = 1.5\nconnect_timeout_ms = 1.5\n`,
    typed("float `1.5`", "u64", "default.usage_journal.connect_timeout_ms"),
  );
  await reject(
    `${BASE}[usage_journal]\nbackend = "none"\ncreate_schema = 1.5\n`,
    typed("float `1.5`", "a boolean", "default.usage_journal.create_schema"),
  );
  await reject(
    `${BASE}[usage_journal]\ncapacity_policy = "nope"\nconnect_timeout_ms = 1.5\nbackend = "none"\n`,
    variant("nope", "`refuse` or `drop-oldest`", "default.usage_journal.capacity_policy"),
  );
  const inert = await loadConfig(`${BASE}[usage_journal]\nbackend = "none"\nmax_events = 0\n`, secrets);
  assert.deepEqual(inert.usageSinks, []);
  await reject(
    `${BASE}[[usage_sink]]\nkind = "stdout"\ncreate_table = 1.5\nbuffer_capacity = 1.5\n`,
    typed("float `1.5`", "usize", "default.usage_sink.0.buffer_capacity"),
  );
  await reject(
    `${BASE}[[usage_sink]]\nkind = "stdout"\nbuffer_capacity = 8\ncreate_table = 1.5\n`,
    typed("float `1.5`", "a boolean", "default.usage_sink.0.create_table"),
  );
  await reject(
    `${BASE}[[usage_sink]]\nbuffer_capacity = 8\n`,
    'config: missing field `kind` for key "default.usage_sink.0"',
  );
});

test("required credential gateway_key namespace and price fields follow figment order", async () => {
  const reject = async (toml: string, message: string) => {
    await assert.rejects(
      () => loadConfig(toml, secrets),
      (error: unknown) => {
        assert.equal(error instanceof Error ? error.message : "", message);
        return true;
      },
    );
  };
  const typed = (found: string, expected: string, key: string) =>
    `config: invalid type: found ${found}, expected ${expected} for key "${key}"`;
  const missing = (field: string, key: string) => `config: missing field \`${field}\` for key "${key}"`;
  await reject(
    `${BASE}[[price]]\nprovider = "openai"\nmodel = "gpt"\ninput_microdollars_per_million = 1.5\n[[credential]]\nprovider = "openai"\n`,
    missing("namespace", "default.credential.0"),
  );
  await reject(
    `${BASE}[[credential]]\nnamespace = "platform"\nweight = 1\n`,
    missing("provider", "default.credential.0"),
  );
  await reject(`${BASE}[[credential]]\n`, missing("namespace", "default.credential.0"));
  await reject(
    BASE.replace('namespace = "platform"\n', "").replace('bind = "127.0.0.1:9"', "bind = 1"),
    missing("namespace", "default.gateway_key.0"),
  );
  await reject(
    BASE.replace('namespace = "platform"', 'namespace = ""'),
    "gateway_key `GW_KEY` references undefined namespace ``",
  );
  await reject(
    BASE.replace('id = "platform"\n', "").replace('bind = "127.0.0.1:9"', "bind = 1"),
    missing("id", "default.namespace.0"),
  );
  await reject(
    BASE.replace('bind = "127.0.0.1:9"', "bind = 1") + '[[price]]\nprovider = "openai"\nmodel = "gpt"\n',
    missing("input_microdollars_per_million", "default.price.0"),
  );
  await reject(
    `${BASE}[[provider]]\nid = "openai"\nkind = "openai"\nbase_url = "http://127.0.0.1:9"\n[[price]]\nprovider = "openai"\nmodel = "*"\ninput_microdollars_per_million = 1\n`,
    missing("output_microdollars_per_million", "default.price.0"),
  );
  await reject(
    `${BASE}[[price]]\nprovider = "openai"\nmodel = "gpt"\nreasoning_microdollars_per_million = 1.5\n`,
    typed("float `1.5`", "u64", "default.price.0"),
  );
  await reject(
    `${BASE}[[provider]]\nid = "openai"\nkind = "openai"\nbase_url = "http://127.0.0.1:9"\n[[price]]\nprovider = ""\nmodel = "gpt"\ninput_microdollars_per_million = 1\noutput_microdollars_per_million = 1\n`,
    "`[[price]]` requires a non-empty `provider` and `model` glob",
  );
  const priced = await loadConfig(
    `${BASE}[[provider]]\nid = "openai"\nkind = "openai"\nbase_url = "http://127.0.0.1:9"\n[[price]]\nprovider = "openai"\nmodel = "*"\ninput_microdollars_per_million = 0\noutput_microdollars_per_million = 0\n`,
    secrets,
  );
  assert.equal(priced.prices[0]?.inputMicrodollarsPerMillion, 0n);
  assert.equal(priced.prices[0]?.outputMicrodollarsPerMillion, 0n);
});

test("axond env overrides are figment values and cite the environment", async () => {
  const reject = async (extra: Record<string, string>, message: string, toml = BASE) => {
    await assert.rejects(
      () => loadConfig(toml, envSecretReader({ GW_KEY: "k", ...extra }, async () => "")),
      (error: unknown) => {
        assert.equal(error instanceof Error ? error.message : "", message);
        return true;
      },
    );
  };
  const envTyped = (found: string, expected: string, key: string) =>
    `config: invalid type: found ${found}, expected ${expected} for key "${key}" in \`AXOND_\` environment variable(s)`;
  await reject(
    { AXOND_FAILOVER__MAX_ATTEMPTS: "1.5" },
    envTyped("float `1.5`", "u32", "FAILOVER.MAX_ATTEMPTS"),
    `${BASE}[[price]]\nprovider = "openai"\nmodel = "gpt"\ninput_microdollars_per_million = 1.5\noutput_microdollars_per_million = 1\n`,
  );
  await reject(
    { AXOND_FAILOVER__MAX_ATTEMPTS: "0" },
    "failover.max_attempts must be at least 1",
  );
  await reject(
    { AXOND_FAILOVER__MAX_ATTEMPTS: "-1" },
    'config: invalid value signed int `-1`, expected u32 for key "FAILOVER.MAX_ATTEMPTS" in `AXOND_` environment variable(s)',
  );
  await reject(
    { AXOND_FAILOVER__MAX_ATTEMPTS: "4294967296" },
    'config: invalid value unsigned int `4294967296`, expected u32 for key "FAILOVER.MAX_ATTEMPTS" in `AXOND_` environment variable(s)',
  );
  await reject(
    { AXOND_FAILOVER__MAX_ATTEMPTS: '"3"' },
    envTyped('string "3"', "u32", "FAILOVER.MAX_ATTEMPTS"),
  );
  await reject({ AXOND_FAILOVER: "1.5" }, envTyped("float `1.5`", "struct Failover", "FAILOVER"));
  await reject(
    { AXOND_FAILOVER: "{max_attempts=1.5}" },
    envTyped("float `1.5`", "u32", "FAILOVER.MAX_ATTEMPTS"),
  );
  await reject(
    { AXOND_ADMISSION__MAX_REQUEST_BYTES: "1.5", AXOND_SHUTDOWN__NOPE: "1" },
    envTyped("float `1.5`", "usize", "ADMISSION.MAX_REQUEST_BYTES"),
  );
  await reject(
    { AXOND_FAILOVER__MAX_ATTEMPTS: "1.5" },
    'config: invalid type: found float `1.5`, expected usize for key "default.admission.max_request_bytes"',
    `${BASE}[admission]\nmax_request_bytes = 1.5\n`,
  );
  await reject(
    { AXOND_CREDENTIAL__0__WEIGHT: "1.5" },
    envTyped("map", "a sequence", "CREDENTIAL"),
  );
  await reject(
    { AXOND_CATALOG__SOURCE: "nope" },
    'config: unknown variant: found `nope`, expected `one of `none`, `models-dev`, `seed`` for key "CATALOG.SOURCE" in `AXOND_` environment variable(s)',
  );
  await reject(
    { AXOND_CREDENTIAL_POOL__STRATEGY: "nope" },
    'config: unknown variant: found `nope`, expected ``round-robin` or `weighted`` for key "CREDENTIAL_POOL.STRATEGY" in `AXOND_` environment variable(s)',
  );
  await reject(
    { AXOND_SHUTDOWN__NOPE: "1" },
    'config: unknown field: found `nope`, expected `one of `drain_grace_ms`, `deadline_ms`, `flush_timeout_ms`` for key "SHUTDOWN.NOPE" in `AXOND_` environment variable(s)',
  );
  await reject(
    { AXOND_SHUTDOWN__DRAIN_GRACE_MS: "1.5", AXOND_SHUTDOWN__NOPE: "1" },
    envTyped("float `1.5`", "u64", "SHUTDOWN.DRAIN_GRACE_MS"),
  );
  await reject(
    { AXOND_TRANSPORT__CONNECT_TIMEOUT_MS: "1.5" },
    'config: unknown field: found `nope`, expected `one of `drain_grace_ms`, `deadline_ms`, `flush_timeout_ms`` for key "default.shutdown.nope"',
    `${BASE}[shutdown]\nnope = 1\n`,
  );
  await reject({ AXOND_STORAGE__PATH: "1" }, envTyped("unsigned int `1`", "a string", "STORAGE.PATH"));
  await reject(
    { AXOND_STORAGE__CREATE_TABLE: "1.5" },
    envTyped("float `1.5`", "a boolean", "STORAGE.CREATE_TABLE"),
  );
  await reject(
    { AXOND_STORAGE__USAGE_INDEX__BUFFER_CAPACITY: "1.5" },
    envTyped("float `1.5`", "usize", "STORAGE.USAGE_INDEX.BUFFER_CAPACITY"),
  );
  await reject(
    { AXOND_DISCOVERY__REFRESH_INTERVAL_SECONDS: "1.5" },
    envTyped("float `1.5`", "u64", "DISCOVERY.REFRESH_INTERVAL_SECONDS"),
  );
  await reject(
    { AXOND_DISCOVERY__REFRESH_INTERVAL_SECONDS: "0" },
    "discovery.refresh_interval_seconds must be at least 1",
  );
  await reject(
    { AXOND_BLOCKLIST__MODELS: "[1]" },
    envTyped("unsigned int `1`", "a string", "BLOCKLIST.MODELS.0"),
  );
  await reject({ AXOND_BLOCKLIST__MODELS: "gpt" }, envTyped('string "gpt"', "a sequence", "BLOCKLIST.MODELS"));
  await reject({ AXOND_SERVER: "1.5" }, envTyped("float `1.5`", "struct Server", "SERVER"));
  await reject({ AXOND_NAMESPACE: "[1]" }, envTyped("unsigned int `1`", "struct Namespace", "NAMESPACE.0"));
  await reject({ AXOND_NAMESPACE: "platform" }, envTyped('string "platform"', "a sequence", "NAMESPACE"));
  await reject(
    { AXOND_NAMESPACE: '[{id=1,default=true}]' },
    envTyped("unsigned int `1`", "a string", "NAMESPACE.0.ID"),
  );
  await reject(
    { AXOND_NAMESPACE: '[{id="platform",default=1.5}]' },
    envTyped("float `1.5`", "a boolean", "NAMESPACE.0.DEFAULT"),
  );
  await reject({ AXOND_GATEWAY_KEY: '["k"]' }, envTyped('string "k"', "struct GatewayKey", "GATEWAY_KEY.0"));
  await reject(
    { AXOND_GATEWAY_KEY: '[{env="GW_KEY",namespace=1}]' },
    envTyped("unsigned int `1`", "a string", "GATEWAY_KEY.0.NAMESPACE"),
  );
  await reject({ AXOND_FAILOVER: "[1.5]" }, envTyped("float `1.5`", "u32", "FAILOVER.0"));
  await reject({ AXOND_FAILOVER: "[0]" }, "failover.max_attempts must be at least 1");
  await reject({ AXOND_ADMISSION: "[1.5]" }, envTyped("float `1.5`", "usize", "ADMISSION.0"));
  await reject({ AXOND_ADMISSION: "[0,1.5]" }, envTyped("float `1.5`", "usize", "ADMISSION.1"));
  await reject(
    { AXOND_ADMISSION: "[1.5,1.5]" },
    envTyped("float `1.5`", "usize", "ADMISSION.0"),
  );
  await reject(
    { AXOND_FAILOVER: "[1.5]" },
    'config: invalid type: found float `1.5`, expected usize for key "default.admission.max_request_bytes"',
    `${BASE}[admission]\nmax_request_bytes = 1.5\n`,
  );
  await reject(
    { AXOND_CREDENTIAL: '[{namespace="platform",provider="openai",weight=1.5}]' },
    envTyped("float `1.5`", "u32", "CREDENTIAL.0.WEIGHT"),
  );
  await reject(
    { AXOND_CREDENTIAL: '[{provider="openai"}]' },
    'config: missing field `namespace` for key "CREDENTIAL.0" in `AXOND_` environment variable(s)',
  );
  await reject(
    { AXOND_PRICE: '[{provider=1,model="gpt",input_microdollars_per_million=1,output_microdollars_per_million=1}]' },
    envTyped("unsigned int `1`", "a string", "PRICE.0.PROVIDER"),
  );
  await reject(
    { AXOND_PRICE: '[{model="gpt",input_microdollars_per_million=1,output_microdollars_per_million=1}]' },
    'config: missing field `provider` for key "PRICE.0" in `AXOND_` environment variable(s)',
  );
  await reject(
    {
      AXOND_PRICE:
        '[{provider="openai",model="gpt",input_microdollars_per_million=1,output_microdollars_per_million=1,reasoning_microdollars_per_million=1.5}]',
    },
    envTyped("float `1.5`", "u64", "PRICE.0"),
  );
  await reject(
    { AXOND_PROVIDER: '[{id="openai",kind="nope",base_url="http://x"}]' },
    'config: unknown variant: found `nope`, expected `one of `openai`, `anthropic`, `openai-compatible`` for key "PROVIDER.0.KIND" in `AXOND_` environment variable(s)',
  );
  await reject(
    { AXOND_USAGE_SINK: '[{kind="redis"}]' },
    'config: unknown variant: found `redis`, expected `one of `stdout`, `postgres`, `otlp`` for key "USAGE_SINK.0.KIND" in `AXOND_` environment variable(s)',
  );
  await reject(
    { AXOND_USAGE_SINK: '[{kind="stdout",buffer_capacity=1.5}]' },
    envTyped("float `1.5`", "usize", "USAGE_SINK.0.BUFFER_CAPACITY"),
  );
  await reject({ AXOND_STORAGE: "[1.5]" }, envTyped("float `1.5`", "enum StorageBackend", "STORAGE.0"));
  await reject({ AXOND_SERVER: "[1.5]" }, envTyped("float `1.5`", "socket address", "SERVER.0"));
  await reject({ AXOND_BLOCKLIST: "[[1]]" }, envTyped("unsigned int `1`", "a string", "BLOCKLIST.0.0"));
  const raised = await loadConfig(BASE, envSecretReader({ GW_KEY: "k", AXOND_FAILOVER__MAX_ATTEMPTS: "+8" }, async () => ""));
  assert.equal(raised.transport.maxAttempts, 8);
  const replaced = await loadConfig(
    `${BASE}[failover]\nmax_attempts = 1.5\n`,
    envSecretReader({ GW_KEY: "k", AXOND_FAILOVER__MAX_ATTEMPTS: "3" }, async () => ""),
  );
  assert.equal(replaced.transport.maxAttempts, 3);
  const sequenced = await loadConfig(
    `${BASE}[failover]\nmax_attempts = 1.5\n`,
    envSecretReader({ GW_KEY: "k", AXOND_FAILOVER: "[4,2,3,4,5]" }, async () => ""),
  );
  assert.equal(sequenced.transport.maxAttempts, 4);
  const renamed = await loadConfig(
    BASE.replace('id = "platform"', 'id = "file-ns"'),
    envSecretReader({ GW_KEY: "k", AXOND_NAMESPACE: '[{id="platform",default=true}]' }, async () => ""),
  );
  assert.deepEqual(
    renamed.namespaces.map((namespace) => namespace.id),
    ["platform"],
  );
  const sink = await loadConfig(
    BASE,
    envSecretReader({ GW_KEY: "k", AXOND_USAGE_SINK: '[{kind="stdout"}]' }, async () => ""),
  );
  assert.equal(sink.usageSinks[0]?.kind, "stdout");
});

test("an earlier figment key finishes before a later sequence fill", async () => {
  const tail = `
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
  const reject = async (toml: string, message: string, extra: Record<string, string> = {}) => {
    await assert.rejects(
      () => loadConfig(toml, envSecretReader({ GW_KEY: "k", ...extra }, async () => "")),
      (error: unknown) => {
        assert.equal(error instanceof Error ? error.message : "", message);
        return true;
      },
    );
  };
  const typed = (found: string, expected: string, key: string) =>
    `config: invalid type: found ${found}, expected ${expected} for key "${key}"`;
  await reject(
    `failover = [1.5]\n[admission]\nmax_request_bytes = 1.5\n${tail}`,
    typed("float `1.5`", "usize", "default.admission.max_request_bytes"),
  );
  await reject(
    `namespace = "x"\n[admission]\nmax_request_bytes = 1.5\n[server]\nbind = "127.0.0.1:9"\n[storage]\nbackend = "sqlite"\npath = "/tmp/axond.sqlite"\n[[gateway_key]]\nenv = "GW_KEY"\nnamespace = "platform"\n`,
    typed("float `1.5`", "usize", "default.admission.max_request_bytes"),
  );
  await reject(
    `failover = [1.5]\n[blocklist]\nmodels = [1]\n${tail}`,
    typed("signed int `1`", "a string", "default.blocklist.models.0"),
  );
  await reject(
    `failover = [1.5]\n${tail}[[credential]]\nnamespace = "platform"\nprovider = "openai"\nenv = "OPENAI_KEY"\nweight = 1.5\n`,
    typed("float `1.5`", "u32", "default.credential.0.weight"),
  );
  await reject(
    `failover = [1.5]\n${tail}`,
    typed("float `1.5`", "usize", "ADMISSION.0") + " in `AXOND_` environment variable(s)",
    { AXOND_ADMISSION: "[1.5]" },
  );
  await reject(
    `failover = [1.5]\n${tail}`,
    typed("float `1.5`", "u32", "CREDENTIAL.0.WEIGHT") + " in `AXOND_` environment variable(s)",
    { AXOND_CREDENTIAL: '[{weight=1.5}]' },
  );
  await reject(
    `failover = [1.5]\n${tail}`,
    typed("unsigned int `1`", "a string", "BLOCKLIST.0.0") + " in `AXOND_` environment variable(s)",
    { AXOND_BLOCKLIST: "[[1]]" },
  );
  await reject(
    `failover = [1.5]\n${tail}`,
    typed("float `1.5`", "u32", "default.failover.0"),
    { AXOND_NAMESPACE: "[1]" },
  );
  await reject(
    `failover = [1.5]\n${tail}`,
    typed("float `1.5`", "u32", "default.failover.0"),
    { AXOND_NAMESPACE: "[[1]]" },
  );
  await reject(
    tail,
    typed("unsigned int `1`", "a string", "NAMESPACE.0.0") + " in `AXOND_` environment variable(s)",
    { AXOND_NAMESPACE: "[[1]]" },
  );
  await reject(
    tail,
    'config: unknown variant: found `nope`, expected `one of `openai`, `anthropic`, `openai-compatible`` for key "PROVIDER.0.1" in `AXOND_` environment variable(s)',
    { AXOND_PROVIDER: '[["openai", "nope"]]' },
  );
  await reject(
    tail,
    typed("sequence", "struct PriceRule", "PRICE.0") + " in `AXOND_` environment variable(s)",
    { AXOND_PRICE: "[[1]]" },
  );
  await reject(
    tail,
    'config: invalid length 2, expected struct GatewayKey with 3 elements for key "GATEWAY_KEY.0" in `AXOND_` environment variable(s)',
    { AXOND_GATEWAY_KEY: "[[]]" },
  );
});

test("a later AXOND_ value replaces an earlier one and strings use figment escapes", async () => {
  const envTyped = (found: string, expected: string, key: string) =>
    `config: invalid type: found ${found}, expected ${expected} for key "${key}" in \`AXOND_\` environment variable(s)`;
  const weight = '[{namespace="platform",provider="openai",weight=1.5}]';
  const arrayLater: Record<string, string> = {};
  arrayLater["AXOND_CREDENTIAL__0__WEIGHT"] = "1.5";
  arrayLater["AXOND_CREDENTIAL"] = weight;
  await assert.rejects(
    () => loadConfig(BASE, envSecretReader({ GW_KEY: "k", ...arrayLater }, async () => "")),
    (error: unknown) => {
      assert.equal(error instanceof Error ? error.message : "", envTyped("float `1.5`", "u32", "CREDENTIAL.0.WEIGHT"));
      return true;
    },
  );
  const mapLater: Record<string, string> = {};
  mapLater["AXOND_CREDENTIAL"] = weight;
  mapLater["AXOND_CREDENTIAL__0__WEIGHT"] = "1.5";
  await assert.rejects(
    () => loadConfig(BASE, envSecretReader({ GW_KEY: "k", ...mapLater }, async () => "")),
    (error: unknown) => {
      assert.equal(error instanceof Error ? error.message : "", envTyped("map", "a sequence", "CREDENTIAL"));
      return true;
    },
  );
  const leafLater: Record<string, string> = {};
  leafLater["AXOND_FAILOVER"] = "[4, 9]";
  leafLater["AXOND_FAILOVER__MAX_ATTEMPTS"] = "8";
  const replaced = await loadConfig(BASE, envSecretReader({ GW_KEY: "k", ...leafLater }, async () => ""));
  assert.equal(replaced.transport.maxAttempts, 8);
  assert.equal(replaced.transport.overallTimeoutMs, 30_000);
  const sequenceLater: Record<string, string> = {};
  sequenceLater["AXOND_FAILOVER__MAX_ATTEMPTS"] = "8";
  sequenceLater["AXOND_FAILOVER"] = "[4, 9]";
  const filled = await loadConfig(BASE, envSecretReader({ GW_KEY: "k", ...sequenceLater }, async () => ""));
  assert.equal(filled.transport.maxAttempts, 4);
  assert.equal(filled.transport.overallTimeoutMs, 9);
  const decoded = await loadConfig(
    BASE,
    envSecretReader({ GW_KEY: "k", AXOND_STORAGE__PATH: '"hi\\u0041"' }, async () => ""),
  );
  assert.equal(decoded.storage.path, "hiA");
  const rawEscape = await loadConfig(
    BASE,
    envSecretReader({ GW_KEY: "k", AXOND_STORAGE__PATH: '"hi\\q"' }, async () => ""),
  );
  assert.equal(rawEscape.storage.path, '"hi\\q"');
  await assert.rejects(
    () => loadConfig(BASE, envSecretReader({ GW_KEY: "k", AXOND_NAMESPACE: '[{id="a\\0",default=true}]' }, async () => "")),
    (error: unknown) => {
      assert.equal(
        error instanceof Error ? error.message : "",
        envTyped("string \"[{id=\\\"a\\\\0\\\",default=true}]\"", "a sequence", "NAMESPACE"),
      );
      return true;
    },
  );
});

test("a u64 overall timeout above 2^53 loads", async () => {
  const u64Max = "18446744073709551615";
  const wide = await loadConfig(
    BASE,
    envSecretReader({ GW_KEY: "k", AXOND_FAILOVER__OVERALL_TIMEOUT_MS: u64Max }, async () => ""),
  );
  assert.equal(wide.transport.overallTimeoutMs, BigInt(u64Max));
  await assert.rejects(
    () =>
      loadConfig(
        BASE,
        envSecretReader({ GW_KEY: "k", AXOND_FAILOVER__OVERALL_TIMEOUT_MS: "0" }, async () => ""),
      ),
    (error: unknown) => {
      assert.equal(error instanceof Error ? error.message : "", "failover.overall_timeout_ms must be at least 1");
      return true;
    },
  );
});

test("an admission ceiling above the semaphore limit names every digit", async () => {
  const permits = "2305843009213693951";
  const absurd = "2305843009213693952";
  const ceiling = (field: string, value: string) =>
    `admission.${field} (${value}) must not exceed ${permits}: a larger ceiling is not a bound this process can hold`;
  await assert.rejects(
    () =>
      loadConfig(
        BASE,
        envSecretReader({ GW_KEY: "k", AXOND_ADMISSION__MAX_IN_FLIGHT: absurd }, async () => ""),
      ),
    (error: unknown) => {
      assert.equal(error instanceof Error ? error.message : "", ceiling("max_in_flight", absurd));
      return true;
    },
  );
  await assert.rejects(
    () =>
      loadConfig(
        BASE,
        envSecretReader({ GW_KEY: "k", AXOND_ADMISSION__MAX_IN_FLIGHT_STREAMS: absurd }, async () => ""),
      ),
    (error: unknown) => {
      assert.equal(
        error instanceof Error ? error.message : "",
        `admission.max_in_flight_streams (${absurd}) must not exceed admission.max_in_flight (1024): a stream is an in-flight request`,
      );
      return true;
    },
  );
  const held = await loadConfig(
    BASE,
    envSecretReader({ GW_KEY: "k", AXOND_ADMISSION__MAX_IN_FLIGHT: permits }, async () => ""),
  );
  assert.equal(held.admission.maxInFlight, Number.MAX_SAFE_INTEGER);
  const wideBytes = await loadConfig(
    BASE,
    envSecretReader(
      { GW_KEY: "k", AXOND_ADMISSION__MAX_REQUEST_BYTES: "18446744073709551615" },
      async () => "",
    ),
  );
  assert.equal(wideBytes.maxRequestBytes, Number.MAX_SAFE_INTEGER);
});

test("a tenant ceiling that cannot isolate a tenant is refused before the semaphore bound", async () => {
  const exceed = (per: string, global: string) =>
    `admission.max_in_flight_per_tenant (${per}) must not exceed admission.max_in_flight (${global}): a per-tenant ceiling above the global one cannot isolate a tenant`;
  const tenants = "admission.max_tenants must be at least 1 when max_in_flight_per_tenant is set";
  const absurd = "2305843009213693952";
  await assert.rejects(
    () => loadConfig(`${BASE}\n[admission]\nmax_in_flight = 16\nmax_in_flight_per_tenant = 32\n`, secrets),
    (error: unknown) => {
      assert.equal(error instanceof Error ? error.message : "", exceed("32", "16"));
      return true;
    },
  );
  await assert.rejects(
    () =>
      loadConfig(
        `${BASE}\n[admission]\nmax_in_flight = 16\nmax_in_flight_per_tenant = 32\nmax_in_flight_streams = 64\n`,
        secrets,
      ),
    (error: unknown) => {
      assert.equal(error instanceof Error ? error.message : "", exceed("32", "16"));
      return true;
    },
  );
  await assert.rejects(
    () => loadConfig(`${BASE}\n[admission]\nmax_tenants = 0\n`, secrets),
    (error: unknown) => {
      assert.equal(error instanceof Error ? error.message : "", tenants);
      return true;
    },
  );
  await assert.rejects(
    () =>
      loadConfig(
        `${BASE}\n[admission]\nmax_in_flight = 16\nmax_in_flight_per_tenant = 4\nmax_tenants = 0\n`,
        secrets,
      ),
    (error: unknown) => {
      assert.equal(error instanceof Error ? error.message : "", tenants);
      return true;
    },
  );
  await assert.rejects(
    () => loadConfig(`${BASE}\n[admission]\nmax_in_flight = 257\nmax_tenants = 0\n`, secrets),
    (error: unknown) => {
      assert.equal(error instanceof Error ? error.message : "", tenants);
      return true;
    },
  );
  await assert.rejects(
    () =>
      loadConfig(`${BASE}\n[admission]\nmax_in_flight = 16\nmax_in_flight_per_tenant = ${absurd}\n`, secrets),
    (error: unknown) => {
      assert.equal(error instanceof Error ? error.message : "", exceed(absurd, "16"));
      return true;
    },
  );
  const lowered = await loadConfig(`${BASE}\n[admission]\nmax_in_flight = 16\n`, secrets);
  assert.equal(lowered.admission.maxInFlight, 16);
  const isolated = await loadConfig(
    `${BASE}\n[admission]\nmax_in_flight = 16\nmax_in_flight_per_tenant = 4\n`,
    secrets,
  );
  assert.equal(isolated.admission.maxInFlight, 16);
  const disabled = await loadConfig(
    `${BASE}\n[admission]\nmax_in_flight = 16\nmax_in_flight_per_tenant = 0\nmax_tenants = 0\n`,
    secrets,
  );
  assert.equal(disabled.admission.maxInFlight, 16);
  const turnedOff = await loadConfig(
    `${BASE}\n[admission]\nmax_in_flight = 16\nmax_tenants = 0\n`,
    secrets,
  );
  assert.equal(turnedOff.admission.maxInFlight, 16);
  const atGlobal = await loadConfig(
    `${BASE}\n[admission]\nmax_in_flight = 256\nmax_tenants = 0\n`,
    secrets,
  );
  assert.equal(atGlobal.admission.maxInFlight, 256);
  const env: Record<string, string> = {};
  env["AXOND_ADMISSION__MAX_IN_FLIGHT"] = "16";
  env["AXOND_ADMISSION__MAX_IN_FLIGHT_PER_TENANT"] = "32";
  await assert.rejects(
    () => loadConfig(BASE, envSecretReader({ GW_KEY: "k", ...env }, async () => "")),
    (error: unknown) => {
      assert.equal(error instanceof Error ? error.message : "", exceed("32", "16"));
      return true;
    },
  );
  await assert.rejects(
    () =>
      loadConfig(
        BASE,
        envSecretReader({ GW_KEY: "k", AXOND_ADMISSION__MAX_TENANTS: "0" }, async () => ""),
      ),
    (error: unknown) => {
      assert.equal(error instanceof Error ? error.message : "", tenants);
      return true;
    },
  );
});

test("an enabled usage journal is refused after earlier boot bounds", async () => {
  const notBuilt = '`[usage_journal] backend = "postgres"` is not built (ADR 0049)';
  await assert.rejects(
    () => loadConfig(`${BASE}\n[usage_journal]\nbackend = "postgres"\n`, secrets),
    (error: unknown) => {
      assert.equal(error instanceof Error ? error.message : "", notBuilt);
      return true;
    },
  );
  await assert.rejects(
    () =>
      loadConfig(
        `${BASE.replace('path = "/tmp/axond.sqlite"', 'path = ":memory:"')}\n[usage_journal]\nbackend = "postgres"\n`,
        secrets,
      ),
    (error: unknown) => {
      assert.equal(
        error instanceof Error ? error.message : "",
        "`[storage]` sqlite `:memory:` is not durable; use a file path",
      );
      return true;
    },
  );
  await assert.rejects(
    () =>
      loadConfig(`${BASE}\n[admission]\nmax_request_bytes = 0\n[usage_journal]\nbackend = "postgres"\n`, secrets),
    (error: unknown) => {
      assert.equal(error instanceof Error ? error.message : "", "admission.max_request_bytes must be at least 1");
      return true;
    },
  );
  await assert.rejects(
    () =>
      loadConfig(
        `${BASE}\n[[usage_sink]]\nkind = "postgres"\n[usage_journal]\nbackend = "postgres"\n`,
        secrets,
      ),
    (error: unknown) => {
      assert.equal(
        error instanceof Error ? error.message : "",
        "usage_sink `postgres`: `dsn_env` must name the env var holding the connection string",
      );
      return true;
    },
  );
});

test("an unknown usage sink kind and an enabled usage journal fail boot", async () => {
  await assert.rejects(
    () => loadConfig(`${BASE}\n[[usage_sink]]\nkind = "redis"\n`, secrets),
    (error: unknown) => {
      assert.equal(
        error instanceof Error ? error.message : "",
        'config: unknown variant: found `redis`, expected `one of `stdout`, `postgres`, `otlp`` for key "default.usage_sink.0.kind"',
      );
      return true;
    },
  );
  await assert.rejects(
    () => loadConfig(`${BASE}\n[usage_journal]\nbackend = "postgres"\ndsn_env = "JOURNAL_DSN"\n`, secrets),
    /not built \(ADR 0049\)/,
  );
});
