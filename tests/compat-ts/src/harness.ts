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
export const NAMESPACE = "platform";
export const UNGRANTED_NAMESPACE = "tenant";
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

/** The alias catalogue `GET /ns/{ns}/v1/models` must report. */
export const ALIAS_NAMES: readonly string[] = ALIASES.map(([alias]) => alias);

/** How long a gateway has to answer `/healthz`, and each probe of it. */
const READY_TIMEOUT_MS = 30_000;
const PROBE_TIMEOUT_MS = 1_000;

/** A booted gateway and the upstream it forwards to. */
export interface Harness {
  /** The gateway's base URL, without the `/v1` prefix. */
  readonly baseUrl: string;
  readonly upstream: FakeUpstream;
  stop(): Promise<void>;
}

export interface StartOptions {
  /** Shortened by the harness's own tests, which assert on giving up. */
  readonly readyTimeoutMs?: number;
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

function config(bind: string, upstream: string, sqlite: string): string {
  const models = ALIASES.map(
    ([alias, provider, target]) =>
      `[[model]]\nname = "${alias}"\n` +
      `targets = [ { provider = "${provider}", model = "${target}", price = ${PRICE} } ]\n`,
  ).join("\n");
  return `
[server]
bind = "${bind}"

[storage]
backend = "sqlite"
path = "${sqlite}"

[[namespace]]
id = "${NAMESPACE}"
default = true

# A second store-backed namespace. ADR 0063 uses one deployment-wide static key.
[[namespace]]
id = "${UNGRANTED_NAMESPACE}"

[[provider]]
id = "fake-openai"
kind = "openai"
base_url = "${upstream}"

[[provider]]
id = "fake-anthropic"
kind = "anthropic"
base_url = "${upstream}"

[[credential]]
namespace = "${NAMESPACE}"
provider = "fake-openai"
env = "GW_FAKE_OPENAI_KEY"

[[credential]]
namespace = "${NAMESPACE}"
provider = "fake-anthropic"
env = "GW_FAKE_ANTHROPIC_KEY"

[[gateway_key]]
env = "GW_INBOUND_KEY"
namespace = "${NAMESPACE}"

${models}`;
}

/** Boot the fake upstream and a real gateway pointed at it. */
export async function start(options: StartOptions = {}): Promise<Harness> {
  // Before anything with a handle on the event loop: a missing binary must fail
  // the run, and a listening socket nobody closes keeps `node --test` alive.
  const program = binary();
  const upstream = await FakeUpstream.start();
  try {
    return await boot(program, upstream, options.readyTimeoutMs ?? READY_TIMEOUT_MS);
  } catch (error) {
    // The upstream holds a listening socket, and `node --test` does not exit
    // while one is open, so a failed boot must not leave it behind.
    await upstream.stop();
    throw error;
  }
}

async function boot(
  program: string,
  upstream: FakeUpstream,
  readyTimeoutMs: number,
): Promise<Harness> {
  const bind = `127.0.0.1:${await freePort()}`;
  const directory = await mkdtemp(join(tmpdir(), "axond-compat-ts-"));
  const configPath = join(directory, "axond.toml");
  const sqlite = join(directory, "axond.sqlite").replace(/\\/g, "/");
  await writeFile(configPath, config(bind, upstream.baseUrl, sqlite), "utf8");

  const environment = { ...process.env };
  delete environment["OTEL_EXPORTER_OTLP_ENDPOINT"];
  const child = spawn(program, {
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
  // `existsSync` passes for a directory or a file without the execute bit, and
  // the refusal to launch arrives as an event: unhandled, it throws out of the
  // event loop and takes the whole test file with it.
  let launch: Error | undefined;
  child.once("error", (error) => {
    launch = error;
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
    await awaitReady(child, baseUrl, readyTimeoutMs, () => launch);
  } catch (error) {
    // The upstream is the caller's to close, which is what keeps this path from
    // stopping it twice.
    await terminate(child);
    throw error;
  }
  return harness;
}

/** Whether the process is gone, however it went: a status or a signal. */
function dead(child: ChildProcess): boolean {
  return child.exitCode !== null || child.signalCode !== null;
}

async function awaitReady(
  child: ChildProcess,
  baseUrl: string,
  readyTimeoutMs: number,
  launchFailure: () => Error | undefined,
): Promise<void> {
  const deadline = Date.now() + readyTimeoutMs;
  while (Date.now() < deadline) {
    const failure = launchFailure();
    if (failure !== undefined) {
      throw new Error(`axond could not be launched: ${failure.message}`);
    }
    if (dead(child)) {
      throw new Error(`axond exited with ${child.exitCode ?? child.signalCode}`);
    }
    try {
      // Each probe is bounded: an accepted connection that is never answered
      // would otherwise outlast the deadline it is supposed to be checked against.
      const response = await fetch(`${baseUrl}/healthz`, {
        signal: AbortSignal.timeout(PROBE_TIMEOUT_MS),
      });
      if (response.ok) {
        await response.text();
        return;
      }
      await response.body?.cancel();
    } catch {
      // Not listening yet, or too slow to answer.
    }
    await new Promise((wake) => setTimeout(wake, 50));
  }
  throw new Error("axond did not become healthy");
}

async function terminate(child: ChildProcess): Promise<void> {
  // A process that never launched has no pid and emits `error` in place of
  // `exit`, so there is nothing to signal and nothing to wait for.
  if (child.pid === undefined || dead(child)) {
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
