import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { chmod, mkdtemp, rm, writeFile } from "node:fs/promises";
import { createServer as createHttpServer } from "node:http";
import { connect, createServer, type Socket } from "node:net";
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
    await child.stdoutEnded;
    const records = shutdownRecords(child.log.stdout);
    const requested = records.filter((record) => record.phase === "requested");
    assert.equal(requested.length, 1);
    assert.equal(requested[0]?.signal, "SIGTERM");
    assert.equal(requested[0]?.drain_grace_ms, 0);
    assert.equal(records.some((record) => record.phase === "admission_closed"), true);
    assert.equal(records.some((record) => record.phase === "second_signal"), false);
    assert.equal(records.some((record) => record.phase === "spend_unsettled"), false);
    const stopped = records.find((record) => record.phase === "stopped");
    assert.equal(stopped?.usage_flushed, true);
    assert.equal(JSON.stringify(records).includes("test-inbound-key"), false);
  } finally {
    await child.stop();
  }
});

test("shutdown_log_names_the_phase_and_omits_the_secret", async () => {
  const key = "sk-shutdown-sentinel";
  const child = await bootShutdown({ drainGraceMs: 8_000, deadlineMs: 700, flushTimeoutMs: 400 }, key);
  const port = Number(new URL(child.base).port);
  let socket: Socket | undefined;
  try {
    await waitFor(`${child.base}/healthz`);
    socket = await holdOpen(port);
    child.proc.kill("SIGTERM");
    const requested = await waitForShutdown(() => child.log.stdout, "requested", 2_000);
    assert.ok(requested, child.log.stderr + child.log.stdout);
    assert.equal(requested.signal, "SIGTERM");
    assert.equal(requested.drain_grace_ms, 8_000);
    assert.equal(requested.deadline_ms, 700);
    assert.equal(requested.in_flight, 0);
    child.proc.kill("SIGTERM");
    const second = await waitForShutdown(() => child.log.stdout, "second_signal", 2_000);
    assert.ok(second, child.log.stdout);
    assert.equal(second.signal, "SIGTERM");
    const closed = await waitForShutdown(() => child.log.stdout, "admission_closed", 1_000);
    assert.ok(closed, child.log.stdout);
    assert.equal(closed.deadline_ms, 700);
    assert.equal(closed.in_flight, 0);
    child.proc.kill("SIGINT");
    const ignored = await waitForShutdown(() => child.log.stdout, "signal_ignored", 2_000);
    assert.ok(ignored, child.log.stdout);
    assert.equal(ignored.signal, "SIGINT");
    const expired = await waitForShutdown(() => child.log.stdout, "deadline_expired", 3_000);
    assert.ok(expired, child.log.stdout);
    assert.equal(expired.deadline_ms, 700);
    assert.equal(expired.in_flight, 0);
    const code = await Promise.race([
      child.exited,
      new Promise<null>((resolve) => setTimeout(() => resolve(null), 3_000)),
    ]);
    assert.equal(code, 0, child.log.stderr);
    await child.stdoutEnded;
    const encoded = JSON.stringify(shutdownRecords(child.log.stdout));
    assert.equal(encoded.includes(key), false);
    assert.equal(encoded.includes("axond.sqlite"), false);
    assert.equal(encoded.includes("127.0.0.1"), false);
    assert.equal(encoded.includes(String(port)), false);
    assert.equal(shutdownRecords(child.log.stdout).some((record) => record.phase === "spend_unsettled"), false);
  } finally {
    socket?.destroy();
    await child.stop();
  }
});

test("key_material_log_names_the_path_and_omits_the_secret", async () => {
  const secret = "sk-file-sentinel";
  const dir = await mkdtemp(join(tmpdir(), "axond-key-"));
  const keyPath = join(dir, "gateway-key");
  await writeFile(keyPath, secret);
  await chmod(keyPath, 0o644);
  const child = await bootFileKey(dir, keyPath);
  try {
    await waitFor(`${child.base}/healthz`);
    const warned = await waitForKeyMaterial(() => child.log.stdout);
    assert.equal(warned.length, 1, child.log.stderr + child.log.stdout);
    assert.equal(warned[0]?.path, keyPath);
    const listed = await fetch(`${child.base}/api/v1/namespaces`, {
      headers: { authorization: `Bearer ${secret}` },
    });
    assert.equal(listed.status, 200);
    await listed.text();
    const request = await waitForRequestSubject(() => child.log.stdout, keyPath);
    assert.ok(request, child.log.stdout);
    assert.equal(child.log.stdout.includes(secret), false);
    assert.equal(child.log.stderr.includes(secret), false);
  } finally {
    await child.stop();
  }
  await chmod(keyPath, 0o600);
  const quiet = await bootFileKey(dir, keyPath);
  try {
    await waitFor(`${quiet.base}/healthz`);
    assert.equal(keyMaterialRecords(quiet.log.stdout).length, 0);
    const listed = await fetch(`${quiet.base}/api/v1/namespaces`, {
      headers: { authorization: `Bearer ${secret}` },
    });
    assert.equal(listed.status, 200);
    await listed.text();
    assert.equal(quiet.log.stdout.includes(secret), false);
  } finally {
    await quiet.stop();
    await rm(dir, { recursive: true, force: true });
  }
});

test("shutdown_spend_log_names_the_stages_and_omits_the_secret", async () => {
  const key = "sk-spend-sentinel";
  const prompt = "PROMPT_SENTINEL_spend";
  const dir = await mkdtemp(join(tmpdir(), "axond-spend-"));
  const port = await freePort();
  const upstream = createHttpServer((_req, res) => {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(
      JSON.stringify({
        id: "chatcmpl-test",
        object: "chat.completion",
        choices: [{ index: 0, message: { role: "assistant", content: "ok" }, finish_reason: "stop" }],
        usage: { prompt_tokens: 3, completion_tokens: 2 },
      }),
    );
  });
  await new Promise<void>((resolve) => {
    upstream.listen(0, "127.0.0.1", () => resolve());
  });
  const upstreamPort = (upstream.address() as AddressInfo).port;
  await writeFile(
    join(dir, "hold.ts"),
    `export default {
  name: "hold-settle",
  apiVersion: 1,
  stage: "pre-dispatch",
  async middleware(c, next) {
    c.get("axond").onSettle(() => new Promise((resolve) => setTimeout(resolve, 5000)));
    await next();
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

[shutdown]
drain_grace_ms = 0
deadline_ms = 300
flush_timeout_ms = 400

[[namespace]]
id = "platform"
default = true

[[provider]]
id = "fake-openai"
kind = "openai"
base_url = "http://127.0.0.1:${upstreamPort}"

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
      GW_INBOUND_KEY: key,
      GW_FAKE_OPENAI_KEY: "sk-upstream-sentinel",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  const log = { stderr: "", stdout: "" };
  child.stderr?.on("data", (chunk: Buffer) => {
    log.stderr += chunk.toString();
  });
  child.stdout?.on("data", (chunk: Buffer) => {
    log.stdout += chunk.toString();
  });
  const exited = new Promise<number | null>((resolve) => {
    child.once("exit", (status) => resolve(status));
  });
  const stdoutEnded = new Promise<void>((resolve) => {
    child.stdout?.on("end", () => resolve());
  });
  try {
    await waitFor(`http://127.0.0.1:${port}/healthz`);
    const budget = await fetch(`http://127.0.0.1:${port}/api/v1/namespaces/platform/budget`, {
      method: "PUT",
      headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
      body: JSON.stringify({ cadence: "monthly", limit_microdollars: 1_000_000_000, timezone: "UTC" }),
    });
    const budgetBody = await budget.text();
    assert.equal(budget.status, 200, budgetBody);
    const chat = await fetch(`http://127.0.0.1:${port}/ns/platform/v1/chat/completions`, {
      method: "POST",
      headers: { authorization: `Bearer ${key}`, "content-type": "application/json" },
      body: JSON.stringify({ model: "fake-openai/gpt-test", messages: [{ role: "user", content: prompt }] }),
    });
    const chatBody = await chat.text();
    assert.equal(chat.status, 200, chatBody);
    await new Promise((wake) => setTimeout(wake, 40));
    child.kill("SIGTERM");
    const line = await waitForShutdown(() => log.stdout, "spend_unsettled", 3_000);
    assert.ok(line, log.stderr + log.stdout);
    assert.equal(line.in_flight, 0);
    assert.equal(line.unsettled, 1);
    assert.equal(line.settlements_queued, 0);
    assert.equal(line.settlements_executing, 1);
    assert.equal(line.settlements_reserved, 0);
    assert.equal(line.settle_share_ms, 200);
    assert.ok((line.oldest_settlement_ms ?? 0) >= 40);
    const code = await Promise.race([
      exited,
      new Promise<null>((resolve) => setTimeout(() => resolve(null), 3_000)),
    ]);
    assert.equal(code, 0, log.stderr);
    await stdoutEnded;
    const encoded = JSON.stringify(shutdownRecords(log.stdout));
    assert.equal(encoded.includes(key), false);
    assert.equal(encoded.includes(prompt), false);
    assert.equal(encoded.includes("sk-upstream-sentinel"), false);
    assert.equal(encoded.includes("axond.sqlite"), false);
    assert.equal(encoded.includes("127.0.0.1"), false);
    assert.equal(encoded.includes(String(port)), false);
    assert.equal(encoded.includes(String(upstreamPort)), false);
  } finally {
    upstream.close();
    if (child.exitCode === null) {
      child.kill("SIGKILL");
      await new Promise((done) => child.once("exit", done));
    }
    await rm(dir, { recursive: true, force: true });
  }
});

async function bootShutdown(
  shutdown: { drainGraceMs: number; deadlineMs: number; flushTimeoutMs: number },
  gatewayKey = "test-inbound-key",
) {
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
      GW_INBOUND_KEY: gatewayKey,
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  const log = { stderr: "", stdout: "" };
  proc.stderr?.on("data", (chunk: Buffer) => {
    log.stderr += chunk.toString();
  });
  proc.stdout?.on("data", (chunk: Buffer) => {
    log.stdout += chunk.toString();
  });
  const exited = new Promise<number | null>((resolve) => {
    proc.once("exit", (status) => resolve(status));
  });
  const stdoutEnded = new Promise<void>((resolve) => {
    if (!proc.stdout) {
      resolve();
      return;
    }
    proc.stdout.on("end", () => resolve());
  });
  return {
    proc,
    log,
    exited,
    stdoutEnded,
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

async function bootFileKey(dir: string, keyPath: string) {
  const port = await freePort();
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

[[gateway_key]]
file = "${keyPath}"
namespace = "platform"
`,
  );
  const proc = spawn(BIN.pathname, {
    env: {
      ...process.env,
      AXOND_CONFIG: join(dir, "axond.toml"),
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  const log = { stderr: "", stdout: "" };
  proc.stderr?.on("data", (chunk: Buffer) => {
    log.stderr += chunk.toString();
  });
  proc.stdout?.on("data", (chunk: Buffer) => {
    log.stdout += chunk.toString();
  });
  return {
    proc,
    log,
    base: `http://127.0.0.1:${port}`,
    stop: async () => {
      if (proc.exitCode === null) {
        proc.kill("SIGKILL");
        await new Promise((done) => proc.once("exit", done));
      }
    },
  };
}

async function waitForKeyMaterial(read: () => string): Promise<Array<{ msg: string; path?: string }>> {
  const until = Date.now() + 2_000;
  while (Date.now() < until) {
    const found = keyMaterialRecords(read());
    if (found.length > 0) {
      return found;
    }
    await new Promise((wake) => setTimeout(wake, 20));
  }
  return keyMaterialRecords(read());
}

async function waitForRequestSubject(read: () => string, subject: string): Promise<boolean> {
  const until = Date.now() + 2_000;
  while (Date.now() < until) {
    const found = read().split("\n").some((line) => {
      try {
        const parsed = JSON.parse(line) as { msg?: string; subject?: string };
        return parsed.msg === "request" && parsed.subject === subject;
      } catch {
        return false;
      }
    });
    if (found) {
      return true;
    }
    await new Promise((wake) => setTimeout(wake, 20));
  }
  return false;
}

function keyMaterialRecords(text: string): Array<{ msg: string; path?: string }> {
  const records = [];
  for (const line of text.split("\n")) {
    if (!line.includes('"msg":"key_material"')) {
      continue;
    }
    const parsed = JSON.parse(line) as { msg: string; path?: string };
    if (parsed.msg === "key_material") {
      records.push(parsed);
    }
  }
  return records;
}

function shutdownRecords(text: string): Array<{
  msg: string;
  phase: string;
  signal?: string;
  drain_grace_ms?: number;
  deadline_ms?: number;
  in_flight?: number;
  unsettled?: number;
  settlements_queued?: number;
  settlements_executing?: number;
  settlements_reserved?: number;
  oldest_settlement_ms?: number;
  settle_share_ms?: number;
  usage_flushed?: boolean;
}> {
  const records = [];
  for (const line of text.split("\n")) {
    if (!line.includes('"msg":"shutdown"')) {
      continue;
    }
    const parsed = JSON.parse(line) as {
      msg: string;
      phase: string;
      signal?: string;
      drain_grace_ms?: number;
      deadline_ms?: number;
      in_flight?: number;
      unsettled?: number;
      settlements_queued?: number;
      settlements_executing?: number;
      settlements_reserved?: number;
      oldest_settlement_ms?: number;
      settle_share_ms?: number;
      usage_flushed?: boolean;
    };
    if (parsed.msg === "shutdown") {
      records.push(parsed);
    }
  }
  return records;
}

async function waitForShutdown(
  read: () => string,
  phase: string,
  boundMs: number,
): Promise<ReturnType<typeof shutdownRecords>[number] | undefined> {
  const until = Date.now() + boundMs;
  while (Date.now() < until) {
    const found = shutdownRecords(read()).find((record) => record.phase === phase);
    if (found) {
      return found;
    }
    await new Promise((wake) => setTimeout(wake, 20));
  }
  return undefined;
}

function holdOpen(port: number): Promise<Socket> {
  return new Promise((resolve, reject) => {
    const socket = connect({ port, host: "127.0.0.1" });
    const fail = (error: Error) => {
      socket.destroy();
      reject(error);
    };
    socket.once("error", fail);
    socket.once("connect", () => {
      socket.off("error", fail);
      socket.on("error", () => {
        // The deadline closes this socket.
      });
      socket.write("POST /api/v1/namespaces HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 64\r\n");
      resolve(socket);
    });
  });
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
