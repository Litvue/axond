/**
 * Boot a real axond binary against the fake upstream, once per test file.
 *
 * The binary is located by `AXOND_BIN` (the CI lane passes the path it built);
 * otherwise the debug build is used, so `cargo build -p axond && npm test` is
 * all a local run needs. The generated config is the same shape the Python lane
 * generates, so the two lanes differ only in which SDK drives the gateway.
 */

import { spawn, type ChildProcess } from "node:child_process";
import { existsSync } from "node:fs";
import { mkdtemp, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import type { AddressInfo } from "node:net";

import { CHAT, EMBEDDINGS, FakeUpstream, MESSAGES, RESPONSES } from "./fakeUpstream.js";

const REPO_ROOT = resolve(import.meta.dirname, "../../..");

export const GATEWAY_KEY = "test-inbound-key";
export const UPSTREAM_OPENAI_KEY = "test-upstream-openai-key";
export const UPSTREAM_ANTHROPIC_KEY = "test-upstream-anthropic-key";

const PRICE =
  "{ input_microdollars_per_million = 2500000, output_microdollars_per_million = 10000000 }";

const ALIASES: ReadonlyArray<readonly [alias: string, provider: string, target: string]> = [
  ["chat-golden", "fake-openai", CHAT],
  ["messages-golden", "fake-anthropic", MESSAGES],
  ["embeddings-golden", "fake-openai", EMBEDDINGS],
  ["responses-golden", "fake-openai", RESPONSES],
];

/** The alias catalogue `GET /v1/models` must report. */
export const ALIAS_NAMES: readonly string[] = ALIASES.map(([alias]) => alias);

/** A booted gateway and the upstream it forwards to. */
export interface Harness {
  /** The gateway's base URL, without the `/v1` prefix. */
  readonly baseUrl: string;
  readonly upstream: FakeUpstream;
  stop(): Promise<void>;
}

function binary(): string {
  const path = process.env["AXOND_BIN"] ?? join(REPO_ROOT, "target/debug/axond");
  if (!existsSync(path)) {
    throw new Error(`axond binary not found at ${path}; build it first`);
  }
  return path;
}

async function freePort(): Promise<number> {
  const probe = createServer();
  await new Promise<void>((ready) => probe.listen(0, "127.0.0.1", ready));
  const { port } = probe.address() as AddressInfo;
  await new Promise<void>((closed) => probe.close(() => closed()));
  return port;
}

function config(bind: string, upstream: string): string {
  const models = ALIASES.map(
    ([alias, provider, target]) =>
      `[[model]]\nname = "${alias}"\n` +
      `targets = [ { provider = "${provider}", model = "${target}", price = ${PRICE} } ]\n`,
  ).join("\n");
  return `
[server]
bind = "${bind}"

[[namespace]]
id = "platform"
default = true

[[provider]]
id = "fake-openai"
kind = "openai"
base_url = "${upstream}"

[[provider]]
id = "fake-anthropic"
kind = "anthropic"
base_url = "${upstream}"

[[credential]]
namespace = "platform"
provider = "fake-openai"
env = "GW_FAKE_OPENAI_KEY"

[[credential]]
namespace = "platform"
provider = "fake-anthropic"
env = "GW_FAKE_ANTHROPIC_KEY"

[[gateway_key]]
env = "GW_INBOUND_KEY"
namespace = "platform"

${models}`;
}

/** Boot the fake upstream and a real gateway pointed at it. */
export async function start(): Promise<Harness> {
  const upstream = await FakeUpstream.start();
  const bind = `127.0.0.1:${await freePort()}`;
  const directory = await mkdtemp(join(tmpdir(), "axond-compat-ts-"));
  const configPath = join(directory, "axond.toml");
  await writeFile(configPath, config(bind, upstream.baseUrl), "utf8");

  const environment = { ...process.env };
  delete environment["OTEL_EXPORTER_OTLP_ENDPOINT"];
  const child = spawn(binary(), {
    env: {
      ...environment,
      AXOND_CONFIG: configPath,
      GW_INBOUND_KEY: GATEWAY_KEY,
      GW_FAKE_OPENAI_KEY: UPSTREAM_OPENAI_KEY,
      GW_FAKE_ANTHROPIC_KEY: UPSTREAM_ANTHROPIC_KEY,
      RUST_LOG: "warn",
    },
    stdio: ["ignore", "ignore", "inherit"],
  });

  const baseUrl = `http://${bind}`;
  const harness: Harness = {
    baseUrl,
    upstream,
    async stop() {
      await terminate(child);
      await upstream.stop();
    },
  };
  try {
    await awaitReady(child, baseUrl);
  } catch (error) {
    await harness.stop();
    throw error;
  }
  return harness;
}

/** Whether the process is gone, however it went: a status or a signal. */
function dead(child: ChildProcess): boolean {
  return child.exitCode !== null || child.signalCode !== null;
}

async function awaitReady(child: ChildProcess, baseUrl: string): Promise<void> {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    if (dead(child)) {
      throw new Error(`axond exited with ${child.exitCode ?? child.signalCode}`);
    }
    try {
      const response = await fetch(`${baseUrl}/healthz`);
      if (response.ok) {
        await response.text();
        return;
      }
    } catch {
      // Not listening yet.
    }
    await new Promise((wake) => setTimeout(wake, 50));
  }
  throw new Error("axond did not become healthy");
}

async function terminate(child: ChildProcess): Promise<void> {
  if (dead(child)) {
    return;
  }
  const exited = new Promise<void>((done) => child.once("exit", () => done()));
  child.kill("SIGTERM");
  const timer = setTimeout(() => child.kill("SIGKILL"), 10_000);
  try {
    await exited;
  } finally {
    clearTimeout(timer);
  }
}
