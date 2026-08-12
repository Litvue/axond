/**
 * The lane's other half: what the SDKs' types say, held to as strictly as the
 * wire itself.
 *
 * `npm test` compiles against the vendors' shipped declarations with
 * `skipLibCheck` off, so a release whose types no longer describe the calls we
 * make — or whose own `.d.ts` no longer type-checks — fails the build instead of
 * passing as `any`. A compiler that is silently no longer enforcing that would
 * leave every test here green, so the negative cases in `../negative` are
 * compiled on purpose and required to fail.
 */

import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { join, resolve } from "node:path";
import { test } from "node:test";

const PROJECT = resolve(import.meta.dirname, "..");

interface Compilation {
  readonly status: number | null;
  readonly output: string;
}

function compile(project: string): Promise<Compilation> {
  const tsc = join(PROJECT, "node_modules/typescript/bin/tsc");
  const child = spawn(process.execPath, [tsc, "--project", project], {
    cwd: PROJECT,
    stdio: ["ignore", "pipe", "pipe"],
  });
  let output = "";
  child.stdout.setEncoding("utf8").on("data", (chunk: string) => (output += chunk));
  child.stderr.setEncoding("utf8").on("data", (chunk: string) => (output += chunk));
  return new Promise((done) => {
    child.once("close", (status) => done({ status, output }));
  });
}

test("the SDKs' declarations are enforced, not trusted", { timeout: 120_000 }, async () => {
  const { status, output } = await compile("negative/tsconfig.json");

  assert.notEqual(status, 0, `the negative cases compiled:\n${output}`);
  // An unresolvable type *inside* a declaration file: reported only while
  // `skipLibCheck` stays off, which is what keeps a broken SDK release visible.
  assert.match(output, /broken-declaration\.d\.ts.*error TS2304/s);
  // And a call the SDK's own definition rejects.
  assert.match(output, /cases\.ts.*error TS/s);
  assert.match(output, /messages/);
});
