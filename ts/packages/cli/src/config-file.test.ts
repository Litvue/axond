import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
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
