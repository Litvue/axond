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

test("unsupported_extension_api_version_is_refused_when_loaded_from_disk", async () => {
  const dir = await mkdtemp(join(tmpdir(), "axond-ext-"));
  try {
    await writeFile(
      join(dir, "future.ts"),
      `export default {
  name: "future",
  apiVersion: 2,
  stage: "pre-auth",
  async middleware() {
    return new Response("nope");
  },
};
`,
    );
    await writeFile(
      join(dir, "axond.toml"),
      `
[server]
bind = "127.0.0.1:9"

[storage]
backend = "sqlite"
path = "${join(dir, "axond.sqlite")}"

[[namespace]]
id = "platform"
default = true

[[gateway_key]]
env = "GW_INBOUND_KEY"
namespace = "platform"
`,
    );
    const child = spawn(BIN.pathname, {
      env: {
        ...process.env,
        AXOND_CONFIG: join(dir, "axond.toml"),
        AXOND_EXTENSIONS_DIR: dir,
        GW_INBOUND_KEY: "test-inbound-key",
      },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stderr = "";
    child.stderr?.on("data", (chunk: Buffer) => {
      stderr += chunk.toString();
    });
    const code = await new Promise<number | null>((resolve) => {
      const timer = setTimeout(() => {
        child.kill("SIGTERM");
        resolve(null);
      }, 10_000);
      child.once("exit", (status) => {
        clearTimeout(timer);
        resolve(status);
      });
    });
    assert.equal(code, 1, stderr);
    assert.match(stderr, /future\.ts apiVersion 2 is not supported \(want 1\)/);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("sigterm fails readiness immediately and exits after the drain window", async () => {
  const dir = await mkdtemp(join(tmpdir(), "axond-drain-"));
  const port = await freePort();
  try {
    await writeFile(
      join(dir, "axond.toml"),
      `
[server]
bind = "127.0.0.1:${port}"

[storage]
backend = "sqlite"
path = "${join(dir, "axond.sqlite")}"

[shutdown]
drain_grace_ms = 800
deadline_ms = 1000
flush_timeout_ms = 1000

[[namespace]]
id = "platform"
default = true

[[gateway_key]]
env = "GW_INBOUND_KEY"
namespace = "platform"
`,
    );
    const child = spawn(BIN.pathname, {
      env: {
        ...process.env,
        AXOND_CONFIG: join(dir, "axond.toml"),
        GW_INBOUND_KEY: "test-inbound-key",
      },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stderr = "";
    child.stderr?.on("data", (chunk: Buffer) => {
      stderr += chunk.toString();
    });
    const exited = new Promise<number | null>((resolve) => {
      child.once("exit", (status) => resolve(status));
    });
    const base = `http://127.0.0.1:${port}`;
    try {
      await waitFor(`${base}/healthz`);
      const signaled = Date.now();
      child.kill("SIGTERM");
      let ready: Response | null = null;
      const pollUntil = Date.now() + 1_000;
      while (Date.now() < pollUntil) {
        ready = await fetch(`${base}/readyz`);
        if (ready.status === 503) {
          break;
        }
        await ready.body?.cancel();
        await new Promise((wake) => setTimeout(wake, 20));
      }
      assert.ok(ready);
      assert.equal(ready.status, 503);
      assert.equal(await ready.text(), "draining");
      const health = await fetch(`${base}/healthz`);
      assert.equal(health.status, 200);
      assert.equal(await health.text(), "ok");
      const admitted = await fetch(`${base}/api/v1/namespaces`, {
        headers: { authorization: "Bearer test-inbound-key" },
      });
      assert.equal(admitted.status, 200, stderr);
      const code = await Promise.race([
        exited,
        new Promise<null>((resolve) => setTimeout(() => resolve(null), 4_000)),
      ]);
      const elapsed = Date.now() - signaled;
      assert.equal(code, 0, stderr);
      assert.ok(elapsed >= 700, `exited after ${elapsed}ms, before drain_grace_ms`);
    } finally {
      if (child.exitCode === null) {
        child.kill("SIGKILL");
        await new Promise((done) => child.once("exit", done));
      }
    }
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("a second signal closes admission before the drain grace ends", async () => {
  const child = await bootShutdown({ drainGraceMs: 5_000, deadlineMs: 400, flushTimeoutMs: 400 });
  try {
    await waitFor(`${child.base}/healthz`);
    const signaled = Date.now();
    child.proc.kill("SIGTERM");
    let ready: Response | null = null;
    const readyUntil = Date.now() + 1_000;
    while (Date.now() < readyUntil) {
      ready = await fetch(`${child.base}/readyz`);
      if (ready.status === 503) {
        break;
      }
      await ready.body?.cancel();
      await new Promise((wake) => setTimeout(wake, 20));
    }
    assert.ok(ready);
    assert.equal(ready.status, 503);
    assert.equal(await ready.text(), "draining");
    const admitted = await fetch(`${child.base}/api/v1/namespaces`, {
      headers: { authorization: "Bearer test-inbound-key" },
    });
    assert.equal(admitted.status, 200);
    await admitted.text();
    child.proc.kill("SIGTERM");
    let closedAdmission = false;
    const closedUntil = Date.now() + 1_000;
    while (Date.now() < closedUntil) {
      try {
        const closed = await fetch(`${child.base}/api/v1/namespaces`, {
          headers: { authorization: "Bearer test-inbound-key" },
        });
        if (closed.status === 503) {
          assert.equal(closed.headers.get("retry-after"), "0");
          assert.equal((await closed.json()).error.type, "draining");
          closedAdmission = true;
          break;
        }
        await closed.body?.cancel();
      } catch {
        closedAdmission = true;
        break;
      }
      await new Promise((wake) => setTimeout(wake, 20));
    }
    assert.equal(closedAdmission, true);
    const code = await Promise.race([
      child.exited,
      new Promise<null>((resolve) => setTimeout(() => resolve(null), 3_000)),
    ]);
    assert.equal(code, 0, child.log.stderr);
    assert.ok(Date.now() - signaled < 2_500, "second signal waited out the 5000ms grace");
  } finally {
    await child.stop();
  }
});

test("drain_grace_ms of 0 closes admission on the first signal", async () => {
  const child = await bootShutdown({ drainGraceMs: 0, deadlineMs: 400, flushTimeoutMs: 400 });
  try {
    await waitFor(`${child.base}/healthz`);
    const signaled = Date.now();
    child.proc.kill("SIGTERM");
    const code = await Promise.race([
      child.exited,
      new Promise<null>((resolve) => setTimeout(() => resolve(null), 3_000)),
    ]);
    assert.equal(code, 0, child.log.stderr);
    assert.ok(Date.now() - signaled < 1_500);
  } finally {
    await child.stop();
  }
});

async function bootShutdown(shutdown: { drainGraceMs: number; deadlineMs: number; flushTimeoutMs: number }) {
  const dir = await mkdtemp(join(tmpdir(), "axond-shutdown-"));
  const port = await freePort();
  await writeFile(
    join(dir, "axond.toml"),
    `
[server]
bind = "127.0.0.1:${port}"

[storage]
backend = "sqlite"
path = "${join(dir, "axond.sqlite")}"

[shutdown]
drain_grace_ms = ${shutdown.drainGraceMs}
deadline_ms = ${shutdown.deadlineMs}
flush_timeout_ms = ${shutdown.flushTimeoutMs}

[[namespace]]
id = "platform"
default = true

[[gateway_key]]
env = "GW_INBOUND_KEY"
namespace = "platform"
`,
  );
  const proc = spawn(BIN.pathname, {
    env: {
      ...process.env,
      AXOND_CONFIG: join(dir, "axond.toml"),
      GW_INBOUND_KEY: "test-inbound-key",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  const log = { stderr: "" };
  proc.stderr?.on("data", (chunk: Buffer) => {
    log.stderr += chunk.toString();
  });
  const exited = new Promise<number | null>((resolve) => {
    proc.once("exit", (status) => resolve(status));
  });
  return {
    proc,
    log,
    exited,
    base: `http://127.0.0.1:${port}`,
    stop: async () => {
      if (proc.exitCode === null) {
        proc.kill("SIGKILL");
        await new Promise((done) => proc.once("exit", done));
      }
      await rm(dir, { recursive: true, force: true });
    },
  };
}

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
