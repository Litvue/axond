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
  assert.equal((await loadConfig(BASE, envSecretReader({ GW_KEY: "k", axond_server__bind: "not-a-socket" }, async () => ""))).bind, "127.0.0.1:9");
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

test("review regressions preserve multiline numeric types, bind validity, price rates and timer ranges", async () => {
  const secrets = envSecretReader({ GW_KEY: "k", axond_server__bind: "127.0.0.1:81" }, async () => "");
  assert.equal((await loadConfig(BASE, secrets)).bind, "127.0.0.1:9");
  await assert.rejects(loadConfig('failover = [\n  1.0 # integral float\n]\n' + BASE, secrets), /expected u32/);
  await assert.rejects(loadConfig(BASE.replace('127.0.0.1:9', '[192.0.2.1::]:8080'), secrets), /socket address/);
  const cfg = await loadConfig(BASE + '\n[[provider]]\nid="p"\nkind="openai"\nbase_url="https://example.test"\n[transport]\nstream_idle_timeout_ms = 2147483648\n[[price]]\nprovider="p"\nmodel="*"\ninput_microdollars_per_million=1\noutput_microdollars_per_million=2\nreasoning_microdollars_per_million=3\ncache_read_microdollars_per_million=4\ncache_write_microdollars_per_million=5\n', secrets);
  assert.equal(cfg.transport.streamIdleTimeoutMs, 2147483647);
  assert.equal(cfg.prices[0]?.reasoningMicrodollarsPerMillion, 3n);
  assert.equal(cfg.prices[0]?.cacheReadMicrodollarsPerMillion, 4n);
  assert.equal(cfg.prices[0]?.cacheWriteMicrodollarsPerMillion, 5n);
});
