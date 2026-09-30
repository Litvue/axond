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
    `${GRAPH}[[price]]\nprovider = "nope"\nmodel = "a*b"\n`,
    "`[[price]]` references undefined provider `nope`",
  );
  await reject(`${GRAPH}[[price]]\nmodel = "gpt"\n`, "`[[price]]` requires a non-empty `provider` and `model` glob");
  await reject(
    `${GRAPH}[[price]]\nprovider = "openai"\nmodel = "a*b"\n`,
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
  await reject(withBind("bind = 9223372036854775808"), "config: number too large to fit in target type");
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
