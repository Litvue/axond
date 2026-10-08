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
