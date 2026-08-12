/**
 * The harness's own crash path.
 *
 * A gateway that dies from a signal leaves `exitCode` null, so a liveness check
 * that reads only `exitCode` sees a healthy process forever: readiness spins out
 * its whole deadline and shutdown then waits on an `exit` event that already
 * fired. That turns a fast failure into a CI job that hangs until the workflow
 * times out, which is exactly the report a broken gateway must not produce.
 */

import assert from "node:assert/strict";
import { chmod, mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { start } from "./harness.js";

/** An "axond" that kills itself the way a crashing process would. */
async function suicidalBinary(): Promise<string> {
  const directory = await mkdtemp(join(tmpdir(), "axond-compat-ts-crash-"));
  const path = join(directory, "axond");
  await writeFile(path, "#!/usr/bin/env node\nprocess.kill(process.pid, 'SIGKILL');\n", "utf8");
  await chmod(path, 0o755);
  return path;
}

test(
  "a gateway that dies from a signal fails the run promptly instead of hanging",
  {
    skip: process.platform === "win32" ? "POSIX signals only" : false,
    // Bounded so the regression this covers — a wait that never returns — is
    // reported as a failed test rather than as a hung CI job.
    timeout: 20_000,
  },
  async () => {
    const original = process.env["AXOND_BIN"];
    process.env["AXOND_BIN"] = await suicidalBinary();
    const started = Date.now();
    try {
      // Well inside the 30s readiness deadline, and `start` cleans up after
      // itself — a hang here would be the regression, reported as a timeout.
      await assert.rejects(start(), /axond exited with SIG/);
      assert.ok(Date.now() - started < 10_000, "the crash was not noticed promptly");
    } finally {
      if (original === undefined) {
        delete process.env["AXOND_BIN"];
      } else {
        process.env["AXOND_BIN"] = original;
      }
    }
  },
);
