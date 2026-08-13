/**
 * The harness's own failure paths, which must end the run rather than stall it.
 *
 * A gateway that dies from a signal leaves `exitCode` null, so a liveness check
 * that reads only `exitCode` sees a healthy process forever: readiness spins out
 * its whole deadline and shutdown then waits on an `exit` event that already
 * fired. A boot that throws before the fake upstream is closed leaves a
 * listening socket, and `node --test` does not exit while one is open. A gateway
 * that accepts the readiness probe and never answers it outlasts the deadline the
 * probe is checked against. Each turns a fast, legible failure into a CI job that
 * hangs until the workflow times out, which is exactly the report a broken
 * gateway must not produce.
 */

import assert from "node:assert/strict";
import { chmod, mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { start } from "./harness.js";

/** An executable "axond" that runs `body` instead of serving. */
async function stubBinary(body: string): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "axond-compat-ts-stub-"));
  const path = join(directory, "axond");
  await writeFile(path, `#!/usr/bin/env node\n${body}\n`, "utf8");
  await chmod(path, 0o755);
  return path;
}

/** An "axond" that kills itself the way a crashing process would. */
function suicidalBinary(): Promise<string> {
  return stubBinary("process.kill(process.pid, 'SIGKILL');");
}

/** An "axond" that accepts the probe's connection and never answers it. */
function muteBinary(): Promise<string> {
  return stubBinary(`
const { readFileSync } = require('node:fs');
const { createServer } = require('node:net');
const bind = /bind = "([^"]+)"/.exec(readFileSync(process.env.AXOND_CONFIG, 'utf8'))[1];
const [host, port] = bind.split(':');
createServer(() => {}).listen(Number(port), host);
`);
}

/** Run `body` with `AXOND_BIN` pointed elsewhere, then put it back. */
async function withBinary(path: string, body: () => Promise<void>): Promise<void> {
  const original = process.env["AXOND_BIN"];
  process.env["AXOND_BIN"] = path;
  try {
    await body();
  } finally {
    if (original === undefined) {
      delete process.env["AXOND_BIN"];
    } else {
      process.env["AXOND_BIN"] = original;
    }
  }
}

function listeningSockets(): number {
  return process.getActiveResourcesInfo().filter((resource) => resource === "TCPSERVERWRAP").length;
}

test("a missing gateway binary is reported without leaving the upstream listening", async () => {
  const before = listeningSockets();
  await withBinary(join(tmpdir(), "axond-does-not-exist"), async () => {
    await assert.rejects(start(), /axond binary not found/);
  });
  // A leaked listener would not fail this test; it would keep the whole test
  // file's process alive after the run, so it is asserted directly.
  assert.equal(listeningSockets(), before, "the fake upstream outlived a failed boot");
});

test(
  "a gateway that dies from a signal fails the run promptly instead of hanging",
  {
    skip: process.platform === "win32" ? "POSIX signals only" : false,
    // Bounded so the regression this covers — a wait that never returns — is
    // reported as a failed test rather than as a hung CI job.
    timeout: 20_000,
  },
  async () => {
    const before = listeningSockets();
    await withBinary(await suicidalBinary(), async () => {
      const started = Date.now();
      // Well inside the 30s readiness deadline: the crash is noticed, not waited
      // out. A hang here is the regression, reported as this test's timeout.
      await assert.rejects(start(), /axond exited with SIG/);
      assert.ok(Date.now() - started < 10_000, "the crash was not noticed promptly");
    });
    assert.equal(listeningSockets(), before, "the fake upstream outlived a crashed gateway");
  },
);

test(
  "a gateway that cannot be launched is reported rather than thrown out of the event loop",
  { skip: process.platform === "win32" ? "POSIX permissions only" : false, timeout: 20_000 },
  async () => {
    // Present, so the existence check passes, but not executable: the refusal
    // arrives as an `error` event, which is fatal to the test file if unhandled.
    const directory = await mkdtemp(join(tmpdir(), "axond-compat-ts-stub-"));
    const path = join(directory, "axond");
    await writeFile(path, "#!/usr/bin/env node\n", { encoding: "utf8", mode: 0o644 });

    const before = listeningSockets();
    await withBinary(path, async () => {
      await assert.rejects(start({ readyTimeoutMs: 3_000 }), /axond could not be launched/);
    });
    assert.equal(listeningSockets(), before, "the fake upstream outlived a failed launch");
  },
);

test(
  "a gateway that never answers the probe gives up on the readiness deadline",
  { timeout: 20_000 },
  async () => {
    const before = listeningSockets();
    await withBinary(await muteBinary(), async () => {
      const started = Date.now();
      // The probe is bounded, so the deadline governs; an unbounded probe would
      // sit on the accepted connection forever and never look at it again.
      await assert.rejects(start({ readyTimeoutMs: 3_000 }), /did not become healthy/);
      assert.ok(Date.now() - started < 10_000, "the deadline did not govern");
    });
    assert.equal(listeningSockets(), before, "the fake upstream outlived an unready gateway");
  },
);
