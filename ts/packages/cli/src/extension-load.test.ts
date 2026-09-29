import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import type { AddressInfo } from "node:net";

const BIN = new URL("../../../bin/axond", import.meta.url);

test("extension_file_loads_from_a_directory_without_a_rebuild", async () => {
  const dir = await mkdtemp(join(tmpdir(), "axond-ext-"));
  const port = await freePort();
  try {
    await writeFile(
      join(dir, "probe.ts"),
      `export default {
  name: "probe",
  apiVersion: 1,
  stage: "pre-auth",
  async middleware() {
    return new Response("loaded");
  },
};
`,
    );
    await writeFile(
      join(dir, "axond.toml"),
      `
[server]
bind = "127.0.0.1:${port}"

[storage]
backend = "sqlite"
path = "${join(dir, "axond.sqlite")}"

[[namespace]]
id = "platform"
default = true

[[provider]]
id = "fake-openai"
kind = "openai"
base_url = "http://127.0.0.1:9"

[[credential]]
namespace = "platform"
provider = "fake-openai"
env = "GW_FAKE_OPENAI_KEY"

[[gateway_key]]
env = "GW_INBOUND_KEY"
namespace = "platform"

[[price]]
provider = "fake-openai"
model = "*"
input_microdollars_per_million = 1
output_microdollars_per_million = 1
`,
    );
    const child = spawn(BIN.pathname, {
      env: {
        ...process.env,
        AXOND_CONFIG: join(dir, "axond.toml"),
        AXOND_EXTENSIONS_DIR: dir,
        GW_INBOUND_KEY: "test-inbound-key",
        GW_FAKE_OPENAI_KEY: "upstream",
      },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stderr = "";
    child.stderr?.on("data", (chunk: Buffer) => {
      stderr += chunk.toString();
    });
    try {
      await waitFor(`http://127.0.0.1:${port}/healthz`);
      const response = await fetch(`http://127.0.0.1:${port}/ns/platform/v1/models`, {
        headers: { authorization: "Bearer test-inbound-key" },
      });
      assert.equal(response.status, 200);
      assert.equal(await response.text(), "loaded");
    } catch (error) {
      throw new Error(`${error instanceof Error ? error.message : "boot failed"}\n${stderr}`);
    } finally {
      child.kill("SIGTERM");
      await new Promise((done) => child.once("exit", done));
    }
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

function freePort(): Promise<number> {
  return new Promise((resolve) => {
    const probe = createServer();
    probe.listen(0, "127.0.0.1", () => {
      const { port } = probe.address() as AddressInfo;
      probe.close(() => resolve(port));
    });
  });
}

async function waitFor(url: string): Promise<void> {
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    try {
      const response = await fetch(url);
      if (response.ok) {
        await response.text();
        return;
      }
    } catch {
      // not listening yet
    }
    await new Promise((wake) => setTimeout(wake, 50));
  }
  throw new Error(`timed out waiting for ${url}`);
}
