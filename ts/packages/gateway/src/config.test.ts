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
    /config: number too large to fit in target type/,
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

test("an unknown usage sink kind and an enabled usage journal fail boot", async () => {
  await assert.rejects(
    () => loadConfig(`${BASE}\n[[usage_sink]]\nkind = "redis"\n`, secrets),
    /unknown variant `redis`, expected `stdout`, `postgres`, or `otlp`/,
  );
  await assert.rejects(
    () => loadConfig(`${BASE}\n[usage_journal]\nbackend = "postgres"\ndsn_env = "JOURNAL_DSN"\n`, secrets),
    /not built \(ADR 0049\)/,
  );
});
