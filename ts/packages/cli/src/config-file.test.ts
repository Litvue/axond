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
      ) + "[[price]]\nprovider = \"nope\"\nmodel = \"gpt\"\n",
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
