import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { readFileSync } from "node:fs";
import test from "node:test";

import { cliArguments, helpText, parseArgv, versionText } from "./argv.ts";
import { AXOND_VERSION } from "./product-version.ts";

const BIN = new URL("../../../bin/axond", import.meta.url);
const WITHDRAWN = ["mint", "keygen", "revoke", "check", "migrate", "admin", "budget"] as const;

test("argv_matches_clap_before_config_load", async () => {
  const cargo = readFileSync(new URL("../../../../Cargo.toml", import.meta.url), "utf8");
  const version = /^version = "([^"]+)"/m.exec(cargo)?.[1];
  assert.equal(AXOND_VERSION, version);
  assert.equal(versionText(), `axond ${version}\n`);

  assert.deepEqual(parseArgv(["axond"]), { action: "serve" });
  assert.deepEqual(parseArgv(["axond", "--"]), { action: "serve" });
  assert.deepEqual(parseArgv(["/tmp/axond-bin", "--version"]), {
    action: "stdout",
    code: 0,
    text: `axond ${version}\n`,
  });
  assert.deepEqual(parseArgv(["./axond", "-V"]), {
    action: "stdout",
    code: 0,
    text: versionText(),
  });
  assert.deepEqual(parseArgv(["axond", "--version", "--help"]), {
    action: "stdout",
    code: 0,
    text: versionText(),
  });
  assert.deepEqual(parseArgv(["axond", "-Vh"]), {
    action: "stdout",
    code: 0,
    text: versionText(),
  });
  assert.deepEqual(parseArgv(["/tmp/axond-bin", "--help"]), {
    action: "stdout",
    code: 0,
    text: helpText("axond-bin"),
  });
  assert.equal(
    helpText("axond"),
    "A store-backed, self-hosted AI gateway\n\nUsage: axond\n\nOptions:\n  -h, --help     Print help\n  -V, --version  Print version\n",
  );
  assert.deepEqual(parseArgv(["./axond", "--help"]), {
    action: "stdout",
    code: 0,
    text: helpText("axond"),
  });
  assert.deepEqual(parseArgv(["foo/bar/axond", "-h"]), {
    action: "stdout",
    code: 0,
    text: helpText("axond"),
  });
  assert.deepEqual(parseArgv(["", "--help"]), {
    action: "stdout",
    code: 0,
    text: helpText("axond"),
  });
  assert.deepEqual(parseArgv(["axond", "--help", "--version"]), {
    action: "stdout",
    code: 0,
    text: helpText("axond"),
  });
  assert.deepEqual(parseArgv(["axond", "-hV"]), {
    action: "stdout",
    code: 0,
    text: helpText("axond"),
  });
  assert.deepEqual(parseArgv(["axond", "-hfoo"]), {
    action: "stdout",
    code: 0,
    text: helpText("axond"),
  });
  assert.deepEqual(parseArgv(["axond", "--help", "--nope"]), {
    action: "stdout",
    code: 0,
    text: helpText("axond"),
  });

  for (const command of WITHDRAWN) {
    assert.deepEqual(parseArgv(["axond", command]), {
      action: "stderr",
      code: 2,
      text: unexpected("axond", command),
    });
  }
  assert.deepEqual(parseArgv(["axond", "help"]), {
    action: "stderr",
    code: 2,
    text: unexpected("axond", "help"),
  });
  assert.deepEqual(parseArgv(["axond", "--nope"]), {
    action: "stderr",
    code: 2,
    text: unexpected("axond", "--nope"),
  });
  assert.deepEqual(parseArgv(["axond", "-v"]), {
    action: "stderr",
    code: 2,
    text: unexpected("axond", "-v"),
  });
  assert.deepEqual(parseArgv(["axond", "-xV"]), {
    action: "stderr",
    code: 2,
    text: unexpected("axond", "-x"),
  });
  assert.deepEqual(parseArgv(["axond", "-"]), {
    action: "stderr",
    code: 2,
    text: unexpected("axond", "-"),
  });
  assert.deepEqual(parseArgv(["axond", "--", "mint"]), {
    action: "stderr",
    code: 2,
    text: unexpected("axond", "mint"),
  });
  assert.deepEqual(parseArgv(["axond", "extra", "--version"]), {
    action: "stderr",
    code: 2,
    text: unexpected("axond", "extra"),
  });
  assert.deepEqual(parseArgv(["axond", "--version=1"]), {
    action: "stderr",
    code: 2,
    text: "error: unexpected value '1' for '--version' found; no more were expected\n\nUsage: axond --version\n\nFor more information, try '--help'.\n",
  });
  assert.deepEqual(parseArgv(["axond", "--help="]), {
    action: "stderr",
    code: 2,
    text: "error: unexpected value '' for '--help' found; no more were expected\n\nUsage: axond --help\n\nFor more information, try '--help'.\n",
  });

  assert.deepEqual(cliArguments(["/usr/bin/node", "/workspace/ts/packages/cli/src/main.ts", "--version"]), [
    "axond",
    "--version",
  ]);
  assert.deepEqual(cliArguments(["/home/ubuntu/.bun/bin/bun", "/workspace/ts/packages/cli/src/main.ts", "mint"]), [
    "axond",
    "mint",
  ]);
  assert.deepEqual(cliArguments(["/tmp/axond-ts/axond", "--version"]), ["/tmp/axond-ts/axond", "--version"]);
  assert.deepEqual(cliArguments(["/tmp/axond-ts/axond", "/some/main.ts"]), [
    "/tmp/axond-ts/axond",
    "/some/main.ts",
  ]);

  const missing = "/no/such/axond.toml";
  const versionRun = await run([BIN.pathname, "--version"], missing);
  assert.equal(versionRun.code, 0);
  assert.equal(versionRun.stdout, `axond ${version}\n`);
  assert.equal(versionRun.stderr, "");

  const helpRun = await run([BIN.pathname, "-h"], missing);
  assert.equal(helpRun.code, 0);
  assert.equal(helpRun.stdout, helpText("axond"));
  assert.equal(helpRun.stderr, "");

  const mintRun = await run([BIN.pathname, "mint"], missing);
  assert.equal(mintRun.code, 2);
  assert.equal(mintRun.stdout, "");
  assert.equal(mintRun.stderr, unexpected("axond", "mint"));
  assert.equal(mintRun.stderr.includes(missing), false);
});

function unexpected(bin: string, arg: string): string {
  return `error: unexpected argument '${arg}' found\n\nUsage: ${bin}\n\nFor more information, try '--help'.\n`;
}

function run(command: string[], config: string): Promise<{ code: number | null; stdout: string; stderr: string }> {
  return new Promise((resolve, reject) => {
    const child = spawn(command[0]!, command.slice(1), {
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
    child.on("close", (code) => {
      resolve({ code, stdout, stderr });
    });
  });
}
