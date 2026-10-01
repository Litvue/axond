import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { chmod, mkdir, mkdtemp, rm, stat, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { figmentFileSource, locateConfigFile } from "./config-file.ts";

const BIN = new URL("../../../bin/axond", import.meta.url);
const STORAGE =
  'Error: failed to load config from `%s`: invalid config: `[storage]` is required (ADR 0063): set `backend = "sqlite"` with `path`, or `backend = "postgres"` with `dsn_env`\n';

test("config_file_matches_figment_and_the_rust_boot_error", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-cfg-"));
  const child = join(root, "nested", "deeper");
  await mkdir(child, { recursive: true });
  await writeFile(join(root, "axond.toml"), "marker = true\n");
  try {
    assert.equal(await locateConfigFile("axond.toml", child), join(root, "axond.toml"));
    assert.equal(await locateConfigFile(join(root, "axond.toml"), child), join(root, "axond.toml"));
    assert.equal(await locateConfigFile(join(root, "missing.toml"), child), null);
    assert.equal(await locateConfigFile(root, child), null);
    assert.equal(await locateConfigFile("absent-axond.toml", child), null);

    const missing = join(root, "no-such.toml");
    const missingRun = await run(missing, child);
    assert.equal(missingRun.code, 1);
    assert.equal(missingRun.stdout, "");
    assert.equal(missingRun.stderr, STORAGE.replace("%s", missing));

    const directoryRun = await run(root, child);
    assert.equal(directoryRun.code, 1);
    assert.equal(directoryRun.stderr, STORAGE.replace("%s", root));

    const bad = join(root, "bad.toml");
    await writeFile(bad, "not toml [[\n");
    const badRun = await run(bad, child);
    assert.equal(badRun.code, 1);
    assert.equal(badRun.stdout, "");
    assert.ok(badRun.stderr.startsWith(`Error: failed to load config from \`${bad}\`: config load: `), badRun.stderr);
    assert.equal(badRun.stderr.endsWith("\n"), true);

    const port = await freePort();
    await writeFile(
      join(root, "axond.toml"),
      `
[server]
bind = "127.0.0.1:${port}"
[storage]
backend = "sqlite"
path = "${join(root, "axond.sqlite")}"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_INBOUND_KEY"
namespace = "platform"
`,
    );
    const served = spawn(BIN.pathname, {
      cwd: child,
      env: { ...process.env, AXOND_CONFIG: "axond.toml", GW_INBOUND_KEY: "local-key" },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stderr = "";
    served.stderr?.on("data", (chunk: Buffer) => {
      stderr += chunk.toString();
    });
    try {
      await waitFor(`http://127.0.0.1:${port}/healthz`);
    } finally {
      served.kill("SIGTERM");
      await new Promise((resolve) => served.once("exit", resolve));
    }
    assert.equal(stderr, "", stderr);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("storage_boot_matches_the_rust_refusals", async () => {
  assert.equal(figmentFileSource("/tmp/storage-probe.toml", "/tmp/storage-probe"), "../storage-probe.toml");
  assert.equal(figmentFileSource("/tmp/storage-probe.toml", "/workspace"), "/tmp/storage-probe.toml");

  const root = await mkdtemp(join(tmpdir(), "axond-storage-"));
  try {
    const memory = join(root, "memory.toml");
    await writeFile(memory, '[storage]\nbackend = "sqlite"\npath = ":memory:"\n');
    const memoryRun = await run(memory, root);
    assert.equal(memoryRun.code, 1);
    assert.equal(memoryRun.stdout, "");
    assert.equal(
      memoryRun.stderr,
      "Error: failed to load config from `" +
        memory +
        "`: invalid config: `[storage]` sqlite `:memory:` is not durable; use a file path\n",
    );

    const bounds = join(root, "bounds.toml");
    await writeFile(bounds, '[storage]\npath = "/tmp/axond.sqlite"\n[storage.usage_index]\nbuffer_capacity = 0\n');
    const boundsRun = await run(bounds, root);
    assert.equal(boundsRun.code, 1);
    assert.equal(
      boundsRun.stderr,
      "Error: failed to load config from `" +
        bounds +
        "`: invalid config: `[storage.usage_index]` buffer_capacity must be at least 1\n",
    );

    const floated = join(root, "float.toml");
    await writeFile(
      floated,
      '[storage]\npath = "/tmp/axond.sqlite"\n[storage.usage_index]\nbuffer_capacity = 1.5\n',
    );
    const floatRun = await run(floated, root);
    assert.equal(floatRun.code, 1);
    assert.equal(floatRun.stdout, "");
    assert.equal(
      floatRun.stderr,
      "Error: failed to load config from `" +
        floated +
        '`: config load: invalid type: found float `1.5`, expected usize for key "default.storage.usage_index.buffer_capacity" in ' +
        figmentFileSource(floated, root) +
        " TOML file\n",
    );

    const postgres = join(root, "postgres.toml");
    await writeFile(
      postgres,
      `
[storage]
backend = "postgres"
dsn_env = "AXOND_STORAGE_BOOT_DSN"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_INBOUND_KEY"
namespace = "platform"
`,
    );
    const unset = await run(postgres, root, { GW_INBOUND_KEY: "k", AXOND_STORAGE_BOOT_DSN: "" });
    assert.equal(unset.code, 1);
    assert.equal(unset.stdout, "");
    assert.equal(
      unset.stderr,
      "Error: store: store unavailable: env `AXOND_STORAGE_BOOT_DSN` is unset or empty\n",
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("credential_graph_matches_the_rust_boot_refusals", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-graph-"));
  const db = join(root, "axond.sqlite");
  const base = `
[storage]
backend = "sqlite"
path = "${db}"
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
  const fresh = join(root, "fresh.sqlite");
  const structural = base.replaceAll(db, fresh);
  try {
    const missingNs = join(root, "missing-ns.toml");
    await writeFile(
      missingNs,
      structural.replace(
        'namespace = "platform"\nprovider = "openai"\nenv = "OPENAI_KEY"',
        'namespace = "ghost"\nprovider = "openai"\nenv = "OPENAI_KEY"',
      ),
    );
    const missingRun = await run(missingNs, root, { GW_KEY: "k", OPENAI_KEY: "sk" });
    assert.equal(missingRun.code, 1);
    assert.equal(missingRun.stdout, "");
    assert.equal(
      missingRun.stderr,
      "Error: failed to load config from `" +
        missingNs +
        "`: invalid config: credential references undefined namespace `ghost`\n",
    );
    await assert.rejects(() => stat(fresh));

    const priced = join(root, "price.toml");
    await writeFile(
      priced,
      structural.replace(
        'namespace = "platform"\nprovider = "openai"\nenv = "OPENAI_KEY"',
        'namespace = "ghost"\nprovider = "openai"\nenv = "OPENAI_KEY"',
      ) +
        '[[price]]\nprovider = "nope"\nmodel = "gpt"\ninput_microdollars_per_million = 1\noutput_microdollars_per_million = 1\n',
    );
    const priceRun = await run(priced, root, { GW_KEY: "k", OPENAI_KEY: "sk" });
    assert.equal(priceRun.code, 1);
    assert.equal(
      priceRun.stderr,
      "Error: failed to load config from `" +
        priced +
        "`: invalid config: `[[price]]` references undefined provider `nope`\n",
    );

    const failover = join(root, "failover.toml");
    await writeFile(
      failover,
      structural.replace(
        'namespace = "platform"\nprovider = "openai"\nenv = "OPENAI_KEY"',
        'namespace = "ghost"\nprovider = "openai"\nenv = "OPENAI_KEY"',
      ) + "[failover]\nfailure_threshold = 0\n",
    );
    const failoverRun = await run(failover, root, { GW_KEY: "k", OPENAI_KEY: "sk" });
    assert.equal(failoverRun.code, 1);
    assert.equal(
      failoverRun.stderr,
      "Error: failed to load config from `" +
        failover +
        "`: invalid config: failover.failure_threshold must be at least 1\n",
    );
    await assert.rejects(() => stat(fresh));

    const unset = join(root, "unset.toml");
    await writeFile(unset, base);
    const unsetRun = await run(unset, root, { GW_KEY: "k", OPENAI_KEY: "" });
    assert.equal(unsetRun.code, 1);
    assert.equal(unsetRun.stdout, "");
    assert.equal(
      unsetRun.stderr,
      "Error: config resolution failed: credential `primary` for namespace `platform` provider `openai` references env var `OPENAI_KEY`, which is unset or empty\n",
    );
    await stat(db);

    const keyPath = join(root, "gateway-key");
    const fileBase = base.replace(
      '[[gateway_key]]\nenv = "GW_KEY"\nnamespace = "platform"',
      `[[gateway_key]]\nfile = "${keyPath}"\nnamespace = "platform"`,
    );
    const missingKey = join(root, "missing-key.toml");
    await writeFile(missingKey, fileBase);
    const missingKeyRun = await run(missingKey, root, { OPENAI_KEY: "sk" });
    assert.equal(missingKeyRun.code, 1);
    assert.equal(missingKeyRun.stdout, "");
    assert.equal(
      missingKeyRun.stderr,
      "Error: config resolution failed: gateway_key for namespace `platform` file `" +
        keyPath +
        "` failed (entity not found): No such file or directory (os error 2)\n",
    );

    await writeFile(keyPath, "");
    await chmod(keyPath, 0o600);
    const emptyKey = join(root, "empty-key.toml");
    await writeFile(emptyKey, fileBase);
    const emptyRun = await run(emptyKey, root, { OPENAI_KEY: "sk" });
    assert.equal(emptyRun.code, 1);
    assert.equal(emptyRun.stdout, "");
    assert.equal(
      emptyRun.stderr,
      "Error: config resolution failed: gateway_key for namespace `platform` file `" + keyPath + "` is empty\n",
    );

    await writeFile(keyPath, Buffer.from([0xff, 0xfe]));
    await chmod(keyPath, 0o600);
    const badKey = join(root, "bad-key.toml");
    await writeFile(badKey, fileBase);
    const badRun = await run(badKey, root, { OPENAI_KEY: "sk" });
    assert.equal(badRun.code, 1);
    assert.equal(badRun.stdout, "");
    assert.equal(
      badRun.stderr,
      "Error: config resolution failed: gateway_key for namespace `platform` file `" +
        keyPath +
        "` is not valid UTF-8\n",
    );

    const keyDir = join(root, "key-dir");
    await mkdir(keyDir);
    const dirToml = join(root, "dir-key.toml");
    await writeFile(
      dirToml,
      base.replace(
        '[[gateway_key]]\nenv = "GW_KEY"\nnamespace = "platform"',
        `[[gateway_key]]\nfile = "${keyDir}"\nnamespace = "platform"`,
      ),
    );
    const dirRun = await run(dirToml, root, { OPENAI_KEY: "sk" });
    assert.equal(dirRun.code, 1);
    assert.equal(dirRun.stdout, "");
    assert.equal(
      dirRun.stderr,
      "Error: config resolution failed: gateway_key for namespace `platform` file `" +
        keyDir +
        "` failed (is a directory): Is a directory (os error 21)\n",
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("catalogue_boot_matches_the_rust_refusals", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-catalog-"));
  const db = join(root, "fresh.sqlite");
  const base = `
[storage]
backend = "sqlite"
path = "${db}"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
`;
  try {
    const http = join(root, "http.toml");
    await writeFile(
      http,
      `${base}[catalog]\nsource = "models-dev"\nsource_url = "http://models.dev/catalog.json"\n`,
    );
    const httpRun = await run(http, root, { GW_KEY: "k" });
    assert.equal(httpRun.code, 1);
    assert.equal(httpRun.stdout, "");
    assert.equal(
      httpRun.stderr,
      "Error: failed to load config from `" +
        http +
        "`: invalid config: catalog.source_url `http://models.dev/catalog.json` must be `https://`: imported metadata is read for pricing and enablement decisions, so a source that can be substituted in transit is refused rather than trusted\n",
    );
    await assert.rejects(() => stat(db));

    const postgres = join(root, "postgres.toml");
    await writeFile(
      postgres,
      `${base}[catalog]\nsource = "models-dev"\nstore = "postgres"\n`,
    );
    const postgresRun = await run(postgres, root, { GW_KEY: "k" });
    assert.equal(postgresRun.code, 1);
    assert.equal(postgresRun.stdout, "");
    assert.equal(
      postgresRun.stderr,
      "Error: failed to load config from `" +
        postgres +
        "`: invalid config: catalog `postgres`: `dsn_env` must name the env var holding the connection string\n",
    );
    await assert.rejects(() => stat(db));

    const discovery = join(root, "discovery.toml");
    await writeFile(
      discovery,
      `${base}[discovery]\nrefresh_interval_seconds = 0\n[catalog]\nsource = "models-dev"\nsource_url = "http://models.dev/catalog.json"\n`,
    );
    const discoveryRun = await run(discovery, root, { GW_KEY: "k" });
    assert.equal(discoveryRun.code, 1);
    assert.equal(discoveryRun.stdout, "");
    assert.equal(
      discoveryRun.stderr,
      "Error: failed to load config from `" +
        discovery +
        "`: invalid config: discovery.refresh_interval_seconds must be at least 1\n",
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("bind_boot_matches_the_rust_socket_addr", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-bind-"));
  const db = join(root, "fresh.sqlite");
  const body = `
[storage]
backend = "sqlite"
path = "${db}"
[budget]
enabled = true
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
`;
  try {
    const host = join(root, "host.toml");
    await writeFile(host, `[server]\nbind = "localhost:8080"\n${body}`);
    const hostRun = await run(host, root, { GW_KEY: "k" });
    assert.equal(hostRun.code, 1);
    assert.equal(hostRun.stdout, "");
    assert.equal(
      hostRun.stderr,
      "Error: failed to load config from `" +
        host +
        '`: config load: invalid socket address syntax for key "default.server.bind" in ' +
        figmentFileSource(host, root) +
        " TOML file\n",
    );
    await assert.rejects(() => stat(db));

    const integer = join(root, "integer.toml");
    await writeFile(integer, `[server]\nbind = 8080\n${body}`);
    const integerRun = await run(integer, root, { GW_KEY: "k" });
    assert.equal(integerRun.code, 1);
    assert.equal(integerRun.stdout, "");
    assert.equal(
      integerRun.stderr,
      "Error: failed to load config from `" +
        integer +
        '`: config load: invalid type: found signed int `8080`, expected socket address for key "default.server.bind" in ' +
        figmentFileSource(integer, root) +
        " TOML file\n",
    );
    await assert.rejects(() => stat(db));

    const fromEnv = join(root, "env.toml");
    await writeFile(fromEnv, `[server]\nbind = "127.0.0.1:9"\n${body}`);
    const envRun = await run(fromEnv, root, { GW_KEY: "k", AXOND_SERVER__BIND: "localhost:8080" });
    assert.equal(envRun.code, 1);
    assert.equal(envRun.stdout, "");
    assert.equal(
      envRun.stderr,
      "Error: failed to load config from `" +
        fromEnv +
        "`: config load: invalid socket address syntax for key \"SERVER.BIND\" in `AXOND_` environment variable(s)\n",
    );
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("bounds_boot_matches_the_rust_order", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-bounds-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "attempts.toml");
  await writeFile(
    config,
    `
[storage]
backend = "sqlite"
path = "${db}"
[[namespace]]
id = "platform"
default = true
[[credential]]
namespace = "ghost"
provider = "openai"
env = "OPENAI_KEY"
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
[failover]
max_attempts = 0
[catalog]
source = "models-dev"
source_url = "http://models.dev/catalog.json"
`,
  );
  try {
    const result = await run(config, root, { GW_KEY: "k" });
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.equal(
      result.stderr,
      "Error: failed to load config from `" +
        config +
        "`: invalid config: failover.max_attempts must be at least 1\n",
    );
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("typed_extract_beats_a_later_bound", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-extract-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "admission.toml");
  await writeFile(
    config,
    `
[storage]
backend = "sqlite"
path = "${db}"
[[namespace]]
id = "platform"
default = true
[[credential]]
namespace = "ghost"
provider = "openai"
env = "OPENAI_KEY"
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
[failover]
max_attempts = 0
[admission]
max_request_bytes = 1.5
[catalog]
source = "models-dev"
source_url = "http://models.dev/catalog.json"
`,
  );
  try {
    const result = await run(config, root, { GW_KEY: "k" });
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.equal(
      result.stderr,
      "Error: failed to load config from `" +
        config +
        '`: config load: invalid type: found float `1.5`, expected usize for key "default.admission.max_request_bytes" in ' +
        figmentFileSource(config, root) +
        " TOML file\n",
    );
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("gateway_key_missing_namespace_beats_a_bad_bind", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-gateway-key-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "gateway-key.toml");
  await writeFile(
    config,
    `
[server]
bind = 1
[storage]
backend = "sqlite"
path = "${db}"
[[namespace]]
id = "platform"
default = true
[[price]]
provider = "openai"
model = "gpt"
input_microdollars_per_million = 1.5
[[gateway_key]]
env = "GW_KEY"
`,
  );
  try {
    const result = await run(config, root, { GW_KEY: "k" });
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.equal(
      result.stderr,
      "Error: failed to load config from `" +
        config +
        '`: config load: missing field `namespace` for key "default.gateway_key.0" in ' +
        figmentFileSource(config, root) +
        " TOML file\n",
    );
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("blocklist_string_beats_a_catalog_boolean", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-blocklist-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "blocklist.toml");
  await writeFile(
    config,
    `
[storage]
backend = "sqlite"
path = "${db}"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
[blocklist]
models = [1]
[catalog]
create_table = 1.5
refresh_interval_seconds = 1.5
`,
  );
  try {
    const result = await run(config, root, { GW_KEY: "k" });
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.equal(
      result.stderr,
      "Error: failed to load config from `" +
        config +
        '`: config load: invalid type: found signed int `1`, expected a string for key "default.blocklist.models.0" in ' +
        figmentFileSource(config, root) +
        " TOML file\n",
    );
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("catalog_create_table_float_beats_a_later_refresh", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-catalog-order-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "catalog.toml");
  await writeFile(
    config,
    `
[storage]
backend = "sqlite"
path = "${db}"
[[namespace]]
id = "platform"
default = true
[[credential]]
namespace = "ghost"
provider = "openai"
env = "OPENAI_KEY"
weight = 1.5
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
[catalog]
create_table = 1.5
refresh_interval_seconds = 1.5
source = "nope"
`,
  );
  try {
    const result = await run(config, root, { GW_KEY: "k" });
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.equal(
      result.stderr,
      "Error: failed to load config from `" +
        config +
        '`: config load: invalid type: found float `1.5`, expected a boolean for key "default.catalog.create_table" in ' +
        figmentFileSource(config, root) +
        " TOML file\n",
    );
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("shutdown_unknown_field_beats_a_transport_float", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-transport-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "transport.toml");
  await writeFile(
    config,
    `
[storage]
backend = "sqlite"
path = "${db}"
[[namespace]]
id = "platform"
default = true
[[credential]]
namespace = "ghost"
provider = "openai"
env = "OPENAI_KEY"
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
[transport]
connect_timeout_ms = 1.5
[shutdown]
nope = 1
[admission]
max_request_bytes = 0
`,
  );
  try {
    const result = await run(config, root, { GW_KEY: "k" });
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.equal(
      result.stderr,
      "Error: failed to load config from `" +
        config +
        '`: config load: unknown field: found `nope`, expected `one of `drain_grace_ms`, `deadline_ms`, `flush_timeout_ms`` for key "default.shutdown.nope" in ' +
        figmentFileSource(config, root) +
        " TOML file\n",
    );
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("unknown_shutdown_field_is_refused_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-shutdown-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "shutdown.toml");
  await writeFile(
    config,
    `
[storage]
backend = "sqlite"
path = "${db}"
[[namespace]]
id = "platform"
default = true
[[credential]]
namespace = "ghost"
provider = "openai"
env = "OPENAI_KEY"
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
[shutdown]
nope = 1
[admission]
max_request_bytes = 0
[failover]
max_attempts = 0
`,
  );
  try {
    const result = await run(config, root, { GW_KEY: "k" });
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.equal(
      result.stderr,
      "Error: failed to load config from `" +
        config +
        "`: config load: unknown field: found `nope`, expected `one of `drain_grace_ms`, `deadline_ms`, `flush_timeout_ms`` for key \"default.shutdown.nope\" in " +
        figmentFileSource(config, root) +
        " TOML file\n",
    );
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("admission_zero_beats_a_transport_zero", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-admit-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "admission.toml");
  await writeFile(
    config,
    `
[storage]
backend = "sqlite"
path = "${db}"
[[namespace]]
id = "platform"
default = true
[[credential]]
namespace = "ghost"
provider = "openai"
env = "OPENAI_KEY"
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
[admission]
max_request_bytes = 0
[transport]
connect_timeout_ms = 0
max_response_bytes = 0
`,
  );
  try {
    const result = await run(config, root, { GW_KEY: "k" });
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.equal(
      result.stderr,
      "Error: failed to load config from `" +
        config +
        "`: invalid config: admission.max_request_bytes must be at least 1\n",
    );
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("sequence_struct_is_refused_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-seq-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "attempts.toml");
  await writeFile(
    config,
    `
failover = [0]
[storage]
backend = "sqlite"
path = "${db}"
[[namespace]]
id = "platform"
default = true
[[credential]]
namespace = "ghost"
provider = "openai"
env = "OPENAI_KEY"
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
[catalog]
source = "models-dev"
source_url = "http://models.dev/catalog.json"
`,
  );
  try {
    const result = await run(config, root, { GW_KEY: "k" });
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.equal(
      result.stderr,
      "Error: failed to load config from `" + config + "`: invalid config: failover.max_attempts must be at least 1\n",
    );
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("sequence_element_is_refused_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-seq-el-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "namespace.toml");
  await writeFile(
    config,
    `
namespace = [[1]]
[storage]
backend = "sqlite"
path = "${db}"
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
`,
  );
  try {
    const result = await run(config, root, { GW_KEY: "k" });
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.equal(
      result.stderr,
      "Error: failed to load config from `" +
        config +
        '`: config load: invalid type: found signed int `1`, expected a string for key "default.namespace.0.0" in namespace.toml TOML file\n',
    );
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("section_shape_is_refused_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-shape-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "admission.toml");
  await writeFile(
    config,
    `
admission = "x"
[storage]
backend = "sqlite"
path = "${db}"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
[failover]
max_attempts = 0
`,
  );
  try {
    const result = await run(config, root, { GW_KEY: "k" });
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.equal(
      result.stderr,
      "Error: failed to load config from `" +
        config +
        '`: config load: invalid type: found string "x", expected struct AdmissionConfigWire for key "default.admission" in ' +
        figmentFileSource(config, root) +
        " TOML file\n",
    );
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("env_failover_float_beats_a_price_float", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-env-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "failover.toml");
  await writeFile(
    config,
    `
[server]
bind = "127.0.0.1:9"
[storage]
backend = "sqlite"
path = "${db}"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
[[price]]
provider = "openai"
model = "gpt"
input_microdollars_per_million = 1.5
output_microdollars_per_million = 1
`,
  );
  try {
    const result = await run(config, root, { GW_KEY: "k", AXOND_FAILOVER__MAX_ATTEMPTS: "1.5" });
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.equal(
      result.stderr,
      "Error: failed to load config from `" +
        config +
        '`: config load: invalid type: found float `1.5`, expected u32 for key "FAILOVER.MAX_ATTEMPTS" in `AXOND_` environment variable(s)\n',
    );
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("env_namespace_sequence_int_is_refused_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-env-seq-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "namespace.toml");
  await writeFile(
    config,
    `
[server]
bind = "127.0.0.1:9"
[storage]
backend = "sqlite"
path = "${db}"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
[[price]]
provider = "openai"
model = "gpt"
input_microdollars_per_million = 1.5
output_microdollars_per_million = 1
`,
  );
  try {
    const result = await run(config, root, { GW_KEY: "k", AXOND_NAMESPACE: "[1]" });
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.equal(
      result.stderr,
      "Error: failed to load config from `" +
        config +
        '`: config load: invalid type: found unsigned int `1`, expected struct Namespace for key "NAMESPACE.0" in `AXOND_` environment variable(s)\n',
    );
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("later_credential_array_replaces_an_earlier_nested_weight", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-env-order-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "credential.toml");
  await writeFile(
    config,
    `
[server]
bind = "127.0.0.1:9"
[storage]
backend = "sqlite"
path = "${db}"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
`,
  );
  const extra: Record<string, string> = { GW_KEY: "k" };
  extra["AXOND_CREDENTIAL__0__WEIGHT"] = "1.5";
  extra["AXOND_CREDENTIAL"] = '[{namespace="platform",provider="openai",weight=1.5}]';
  try {
    const result = await run(config, root, extra);
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.equal(
      result.stderr,
      "Error: failed to load config from `" +
        config +
        '`: config load: invalid type: found float `1.5`, expected u32 for key "CREDENTIAL.0.WEIGHT" in `AXOND_` environment variable(s)\n',
    );
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("u64_overall_timeout_boots", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-u64-timeout-"));
  const port = await freePort();
  const config = join(root, "axond.toml");
  await writeFile(
    config,
    `
[server]
bind = "127.0.0.1:${port}"
[storage]
backend = "sqlite"
path = "${join(root, "axond.sqlite")}"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
`,
  );
  const served = spawn(BIN.pathname, {
    cwd: root,
    env: {
      ...process.env,
      AXOND_CONFIG: config,
      GW_KEY: "k",
      AXOND_FAILOVER__OVERALL_TIMEOUT_MS: "18446744073709551615",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let stderr = "";
  served.stderr?.on("data", (chunk: Buffer) => {
    stderr += chunk.toString();
  });
  try {
    await waitFor(`http://127.0.0.1:${port}/healthz`);
    const health = await fetch(`http://127.0.0.1:${port}/healthz`);
    assert.equal(health.status, 200);
    assert.equal(await health.text(), "ok");
  } finally {
    served.kill("SIGTERM");
    await new Promise((resolve) => served.once("exit", resolve));
    await rm(root, { recursive: true, force: true });
  }
  assert.equal(stderr, "", stderr);
});

test("usage_sink_batch_above_the_buffer_is_refused_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-usage-batch-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "usage.toml");
  await writeFile(
    config,
    `
[server]
bind = "127.0.0.1:9"
[storage]
backend = "sqlite"
path = "${db}"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
[[usage_sink]]
kind = "postgres"
dsn_env = "DSN"
buffer_capacity = 1
max_batch = 9223372036854775807
`,
  );
  try {
    const result = await run(config, root, { GW_KEY: "k" });
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.equal(
      result.stderr,
      "Error: failed to load config from `" +
        config +
        "`: invalid config: usage_sink `postgres`: max_batch (9223372036854775807) must not exceed buffer_capacity (1)\n",
    );
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("toml_integer_outside_i64_is_a_parse_error_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-toml-range-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "range.toml");
  const line = "failover = { overall_timeout_ms = 9223372036854775808 }";
  await writeFile(
    config,
    `${line}
[shutdown]
nope = 1
[server]
bind = "127.0.0.1:9"
[storage]
backend = "sqlite"
path = "${db}"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
`,
  );
  const column = line.indexOf("9223372036854775808");
  const pad = "  ";
  const diagram =
    "TOML parse error at line 1, column " +
    (column + 1) +
    "\n" +
    pad +
    "|\n" +
    "1 | " +
    line +
    "\n" +
    pad +
    "|" +
    " ".repeat(column + 1) +
    "^\n" +
    "number too large to fit in target type\n" +
    " in range.toml TOML file\n";
  try {
    const result = await run(config, root, { GW_KEY: "k" });
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.equal(
      result.stderr,
      "Error: failed to load config from `" + config + "`: config load: " + diagram,
    );
    assert.equal(result.stderr.includes("unknown field"), false);
    await assert.rejects(() => stat(db));

    await writeFile(config, `n = -9223372036854775809\n[storage]\nbackend = "sqlite"\npath = "${db}"\n`);
    const below = await run(config, root, { GW_KEY: "k" });
    assert.equal(below.code, 1);
    assert.equal(below.stdout, "");
    assert.match(below.stderr, /number too small to fit in target type\n in range\.toml TOML file\n$/);
    await assert.rejects(() => stat(db));

    await writeFile(config, `n = 0x8000000000000000\n[storage]\nbackend = "sqlite"\npath = "${db}"\n`);
    const hex = await run(config, root, { GW_KEY: "k" });
    assert.equal(hex.code, 1);
    assert.match(hex.stderr, /1 \| n = 0x8000000000000000\n {2}\| {5}\^\nnumber too large to fit in target type\n/);
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("trailing_comma_in_an_inline_table_is_a_parse_error_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-inline-comma-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "inline.toml");
  const line = "failover = { overall_timeout_ms = 8, }";
  await writeFile(
    config,
    `${line}
[shutdown]
nope = 1
[server]
bind = "127.0.0.1:9"
[storage]
backend = "sqlite"
path = "${db}"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
`,
  );
  const column = line.indexOf(",");
  const pad = "  ";
  const diagram =
    "TOML parse error at line 1, column " +
    (column + 1) +
    "\n" +
    pad +
    "|\n" +
    "1 | " +
    line +
    "\n" +
    pad +
    "|" +
    " ".repeat(column + 1) +
    "^\n" +
    "invalid inline table\nexpected `}`\n" +
    " in inline.toml TOML file\n";
  try {
    const result = await run(config, root, { GW_KEY: "k" });
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.equal(
      result.stderr,
      "Error: failed to load config from `" + config + "`: config load: " + diagram,
    );
    assert.equal(result.stderr.includes("unknown field"), false);
    await assert.rejects(() => stat(db));

    await writeFile(config, `failover = {\n  overall_timeout_ms = 8\n}\n[storage]\nbackend = "sqlite"\npath = "${db}"\n`);
    const opened = await run(config, root, { GW_KEY: "k" });
    assert.equal(opened.code, 1);
    assert.equal(opened.stdout, "");
    assert.match(opened.stderr, /invalid inline table\nexpected `}`\n in inline\.toml TOML file\n$/);
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("hex_escape_in_a_string_is_a_parse_error_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-hex-escape-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "escape.toml");
  const line = 'path = "data\\x41.db"';
  await writeFile(
    config,
    `${line}
[shutdown]
nope = 1
[server]
bind = "127.0.0.1:9"
[storage]
backend = "sqlite"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
`,
  );
  const column = line.indexOf("\\x") + 2;
  const pad = "  ";
  const diagram =
    "TOML parse error at line 1, column " +
    (column + 1) +
    "\n" +
    pad +
    "|\n" +
    "1 | " +
    line +
    "\n" +
    pad +
    "|" +
    " ".repeat(column + 1) +
    "^\n" +
    "invalid escape sequence\nexpected `b`, `f`, `n`, `r`, `t`, `u`, `U`, `\\`, `\"`\n" +
    " in escape.toml TOML file\n";
  try {
    const result = await run(config, root, { GW_KEY: "k" });
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.equal(
      result.stderr,
      "Error: failed to load config from `" + config + "`: config load: " + diagram,
    );
    assert.equal(result.stderr.includes("unknown field"), false);
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("unknown_escape_is_a_parse_error_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-unknown-escape-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "letter.toml");
  const line = 'path = "data\\a.db"';
  await writeFile(
    config,
    `${line}
[shutdown]
nope = 1
[server]
bind = "127.0.0.1:9"
[storage]
backend = "sqlite"
path = "${db}"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
`,
  );
  const column = line.indexOf("\\a") + 2;
  const pad = "  ";
  const diagram =
    "TOML parse error at line 1, column " +
    (column + 1) +
    "\n" +
    pad +
    "|\n" +
    "1 | " +
    line +
    "\n" +
    pad +
    "|" +
    " ".repeat(column + 1) +
    "^\n" +
    "invalid escape sequence\nexpected `b`, `f`, `n`, `r`, `t`, `u`, `U`, `\\`, `\"`\n" +
    " in letter.toml TOML file\n";
  try {
    const result = await run(config, root, { GW_KEY: "k" });
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.equal(
      result.stderr,
      "Error: failed to load config from `" + config + "`: config load: " + diagram,
    );
    assert.equal(result.stderr.includes("unknown field"), false);
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("short_unicode_escape_is_a_parse_error_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-unicode-escape-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "unicode.toml");
  const line = 'path = "data\\u41.db"';
  await writeFile(
    config,
    `${line}
[shutdown]
nope = 1
[server]
bind = "127.0.0.1:9"
[storage]
backend = "sqlite"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
`,
  );
  const column = line.indexOf("\\u") + 2;
  const pad = "  ";
  const diagram =
    "TOML parse error at line 1, column " +
    (column + 1) +
    "\n" +
    pad +
    "|\n" +
    "1 | " +
    line +
    "\n" +
    pad +
    "|" +
    " ".repeat(column + 1) +
    "^\n" +
    "invalid unicode 4-digit hex code\n" +
    " in unicode.toml TOML file\n";
  try {
    const result = await run(config, root, { GW_KEY: "k" });
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.equal(
      result.stderr,
      "Error: failed to load config from `" + config + "`: config load: " + diagram,
    );
    assert.equal(result.stderr.includes("unknown field"), false);
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("leading_zero_is_a_parse_error_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-leading-zero-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "zero.toml");
  const line = "n = 0922";
  await writeFile(
    config,
    `${line}
[shutdown]
nope = 1
[server]
bind = "127.0.0.1:9"
[storage]
backend = "sqlite"
path = "${db}"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
`,
  );
  const column = line.indexOf("0922") + 1;
  const pad = "  ";
  const diagram =
    "TOML parse error at line 1, column " +
    (column + 1) +
    "\n" +
    pad +
    "|\n" +
    "1 | " +
    line +
    "\n" +
    pad +
    "|" +
    " ".repeat(column + 1) +
    "^\n" +
    "expected newline, `#`\n" +
    " in zero.toml TOML file\n";
  try {
    const result = await run(config, root, { GW_KEY: "k" });
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.equal(
      result.stderr,
      "Error: failed to load config from `" + config + "`: config load: " + diagram,
    );
    assert.equal(result.stderr.includes("unknown field"), false);
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("float_past_the_finite_range_is_a_parse_error_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-float-range-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "float.toml");
  const line = "n = 1e309";
  await writeFile(
    config,
    `${line}
[shutdown]
nope = 1
[server]
bind = "127.0.0.1:9"
[storage]
backend = "sqlite"
path = "${db}"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
`,
  );
  const column = line.indexOf("1e309");
  const pad = "  ";
  const diagram =
    "TOML parse error at line 1, column " +
    (column + 1) +
    "\n" +
    pad +
    "|\n" +
    "1 | " +
    line +
    "\n" +
    pad +
    "|" +
    " ".repeat(column + 1) +
    "^\n" +
    "invalid floating-point number\n" +
    " in float.toml TOML file\n";
  try {
    const result = await run(config, root, { GW_KEY: "k" });
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.equal(
      result.stderr,
      "Error: failed to load config from `" + config + "`: config load: " + diagram,
    );
    assert.equal(result.stderr.includes("unknown field"), false);
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("impossible_calendar_day_is_a_parse_error_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-calendar-day-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "day.toml");
  const line = "n = 2024-02-30";
  await writeFile(
    config,
    `${line}
[shutdown]
nope = 1
[server]
bind = "127.0.0.1:9"
[storage]
backend = "sqlite"
path = "${db}"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
`,
  );
  const column = line.indexOf("2024-02-30") + 8;
  const pad = "  ";
  const diagram =
    "TOML parse error at line 1, column " +
    (column + 1) +
    "\n" +
    pad +
    "|\n" +
    "1 | " +
    line +
    "\n" +
    pad +
    "|" +
    " ".repeat(column + 1) +
    "^\n" +
    "invalid date-time\n" +
    "value is out of range\n" +
    " in day.toml TOML file\n";
  try {
    const result = await run(config, root, { GW_KEY: "k" });
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.equal(
      result.stderr,
      "Error: failed to load config from `" + config + "`: config load: " + diagram,
    );
    assert.equal(result.stderr.includes("unknown field"), false);
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("leap_second_reaches_extract_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-leap-second-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "leap.toml");
  await writeFile(
    config,
    `n = 2024-01-01T23:59:60Z
[shutdown]
nope = 1
[server]
bind = "127.0.0.1:9"
[storage]
backend = "sqlite"
path = "${db}"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
`,
  );
  try {
    const result = await run(config, root, { GW_KEY: "k" });
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.match(result.stderr, /unknown field/);
    assert.equal(result.stderr.includes("invalid date"), false);
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("impossible_clock_is_a_parse_error_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-clock-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "clock.toml");
  const line = "n = 23:60:00";
  await writeFile(
    config,
    `${line}
[shutdown]
nope = 1
[server]
bind = "127.0.0.1:9"
[storage]
backend = "sqlite"
path = "${db}"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
`,
  );
  const column = line.indexOf("60");
  const pad = "  ";
  const diagram =
    "TOML parse error at line 1, column " +
    (column + 1) +
    "\n" +
    pad +
    "|\n" +
    "1 | " +
    line +
    "\n" +
    pad +
    "|" +
    " ".repeat(column + 1) +
    "^\n" +
    "invalid time\n" +
    "value is out of range\n" +
    " in clock.toml TOML file\n";
  try {
    const result = await run(config, root, { GW_KEY: "k" });
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.equal(
      result.stderr,
      "Error: failed to load config from `" + config + "`: config load: " + diagram,
    );
    assert.equal(result.stderr.includes("unknown field"), false);
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("usage_journal_postgres_is_refused_after_an_earlier_bound", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-journal-order-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "journal.toml");
  await writeFile(
    config,
    `
[server]
bind = "127.0.0.1:9"
[storage]
backend = "sqlite"
path = "${db}"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
[admission]
max_request_bytes = 0
[usage_journal]
backend = "postgres"
`,
  );
  try {
    const result = await run(config, root, { GW_KEY: "k" });
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.equal(
      result.stderr,
      "Error: failed to load config from `" +
        config +
        "`: invalid config: admission.max_request_bytes must be at least 1\n",
    );
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("tenant_ceiling_above_the_global_one_is_refused_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-tenant-ceiling-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "admission.toml");
  const header = `
[server]
bind = "127.0.0.1:9"
[storage]
backend = "sqlite"
path = "${db}"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
[admission]
`;
  try {
    await writeFile(
      config,
      `${header}max_in_flight = 16\nmax_in_flight_per_tenant = 2305843009213693952\n`,
    );
    const above = await run(config, root, { GW_KEY: "k" });
    assert.equal(above.code, 1);
    assert.equal(above.stdout, "");
    assert.equal(
      above.stderr,
      "Error: failed to load config from `" +
        config +
        "`: invalid config: admission.max_in_flight_per_tenant (2305843009213693952) must not exceed admission.max_in_flight (16): a per-tenant ceiling above the global one cannot isolate a tenant\n",
    );
    await assert.rejects(() => stat(db));
    await writeFile(config, `${header}max_tenants = 0\n`);
    const tenants = await run(config, root, { GW_KEY: "k" });
    assert.equal(tenants.code, 1);
    assert.equal(tenants.stdout, "");
    assert.equal(
      tenants.stderr,
      "Error: failed to load config from `" +
        config +
        "`: invalid config: admission.max_tenants must be at least 1 when max_in_flight_per_tenant is set\n",
    );
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("admission_ceiling_above_the_semaphore_is_refused_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-admit-ceiling-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "admission.toml");
  await writeFile(
    config,
    `
[server]
bind = "127.0.0.1:9"
[storage]
backend = "sqlite"
path = "${db}"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
`,
  );
  try {
    const result = await run(config, root, {
      GW_KEY: "k",
      AXOND_ADMISSION__MAX_IN_FLIGHT: "2305843009213693952",
    });
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.equal(
      result.stderr,
      "Error: failed to load config from `" +
        config +
        "`: invalid config: admission.max_in_flight (2305843009213693952) must not exceed 2305843009213693951: a larger ceiling is not a bound this process can hold\n",
    );
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("admission_float_beats_a_later_failover_array", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-order-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "order.toml");
  await writeFile(
    config,
    `
failover = [1.5]
[admission]
max_request_bytes = 1.5
[server]
bind = "127.0.0.1:9"
[storage]
backend = "sqlite"
path = "${db}"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
`,
  );
  try {
    const result = await run(config, root, { GW_KEY: "k" });
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.equal(
      result.stderr,
      "Error: failed to load config from `" +
        config +
        '`: config load: invalid type: found float `1.5`, expected usize for key "default.admission.max_request_bytes" in ' +
        figmentFileSource(config, root) +
        " TOML file\n",
    );
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

function run(
  config: string,
  cwd: string,
  extra: Record<string, string> = {},
): Promise<{ code: number | null; stdout: string; stderr: string }> {
  return new Promise((resolve, reject) => {
    const child = spawn(BIN.pathname, {
      cwd,
      env: { ...process.env, ...extra, AXOND_CONFIG: config },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout?.on("data", (chunk: Buffer) => {
      stdout += chunk.toString();
    });
    child.stderr?.on("data", (chunk: Buffer) => {
      stderr += chunk.toString();
    });
    child.on("error", reject);
    child.on("close", (code) => resolve({ code, stdout, stderr }));
  });
}

function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      const port = typeof address === "object" && address ? address.port : 0;
      server.close(() => resolve(port));
    });
    server.on("error", reject);
  });
}

async function waitFor(url: string): Promise<void> {
  let last = "";
  for (let attempt = 0; attempt < 40; attempt += 1) {
    try {
      const response = await fetch(url);
      if (response.ok) return;
      last = `${response.status}`;
    } catch (error) {
      last = error instanceof Error ? error.message : "fetch failed";
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error(`timed out waiting for ${url}: ${last}`);
}
