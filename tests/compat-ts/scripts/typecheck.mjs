#!/usr/bin/env node
/**
 * Compile the lane and, when that fails, say which half failed.
 *
 * This lane makes two different claims: that `axond` preserves the provider wire,
 * and that it preserves it in the shapes the vendors' own types describe. Because
 * `skipLibCheck` is deliberately off, a pinned SDK release whose shipped `.d.ts`
 * does not type-check — an optional peer such as `zod` that nobody installed is
 * the usual cause — fails the build before a single request is made. That is a
 * fact about the SDK release, not about the gateway, and reading it off raw `tsc`
 * output takes knowing which paths are ours.
 *
 * Usage:
 *     node scripts/typecheck.mjs                        # build the lane
 *     node scripts/typecheck.mjs --project <tsconfig>   # check one project
 */

import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname, join, resolve } from "node:path";

const PROJECT = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const TSC = join(PROJECT, "node_modules/typescript/bin/tsc");
const DIAGNOSTIC = /^(?<file>[^(]+)\(\d+,\d+\): error TS\d+/;

/** Where a `tsc` diagnostic came from: a declaration file, or code we wrote. */
function classify(output) {
  const declarations = new Set();
  let sources = 0;
  for (const line of output.split("\n")) {
    const file = DIAGNOSTIC.exec(line)?.groups?.file;
    if (file === undefined) {
      continue;
    }
    if (file.endsWith(".d.ts")) {
      declarations.add(file);
    } else {
      sources += 1;
    }
  }
  return { declarations: [...declarations], sources };
}

function explain(output) {
  const { declarations, sources } = classify(output);
  if (declarations.length === 0) {
    if (sources > 0) {
      return [
        "compat-ts: a call this lane makes is rejected by the SDKs' own types.",
        "That is the wire claim failing at the type level: either the gateway's",
        "shape changed, or the pinned SDK describes the call differently now.",
      ];
    }
    return [];
  }
  return [
    "compat-ts: the failure is in shipped type declarations, not in axond's wire",
    "behaviour — no request was made. Declarations at fault:",
    ...declarations.map((file) => `  ${file}`),
    "",
    "A pinned SDK release whose own `.d.ts` does not type-check (commonly an",
    "optional peer dependency such as `zod` that this lane does not install) fails",
    "here. Satisfy the declaration — install the peer at an exact version — rather",
    "than turning on `skipLibCheck`, which would hide the next one. See",
    "tests/compat-ts/README.md.",
    ...(sources > 0
      ? ["", "There are also errors in this lane's own code; both must be read."]
      : []),
  ];
}

const argv = process.argv.slice(2);
const project = argv.indexOf("--project");
const args = project === -1 ? ["--build", "--force"] : ["--project", argv[project + 1]];

const child = spawn(process.execPath, [TSC, ...args], { cwd: PROJECT, stdio: ["ignore", "pipe", "pipe"] });
let output = "";
for (const stream of [child.stdout, child.stderr]) {
  stream.setEncoding("utf8").on("data", (chunk) => {
    output += chunk;
    process.stdout.write(chunk);
  });
}
child.once("error", (error) => {
  console.error(`compat-ts: could not run tsc: ${error.message}`);
  process.exit(1);
});
child.once("close", (status) => {
  if (status !== 0) {
    for (const line of explain(output)) {
      console.error(line);
    }
  }
  process.exit(status ?? 1);
});
