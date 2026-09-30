import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { locateConfigFile } from "./config-file.ts";

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

function run(config: string, cwd: string): Promise<{ code: number | null; stdout: string; stderr: string }> {
  return new Promise((resolve, reject) => {
    const child = spawn(BIN.pathname, {
      cwd,
      env: { ...process.env, AXOND_CONFIG: config },
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
