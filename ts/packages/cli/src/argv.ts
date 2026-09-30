import { basename } from "node:path";

import { AXOND_VERSION } from "./product-version.ts";

const ABOUT = "A store-backed, self-hosted AI gateway";

export type ArgvDecision =
  | { action: "serve" }
  | { action: "stdout"; text: string; code: 0 }
  | { action: "stderr"; text: string; code: 2 };

/** File name clap prints in `Usage:`, falling back to the command name. */
export function usageBin(argv0: string): string {
  const name = basename(argv0);
  return name.length > 0 ? name : "axond";
}

export function versionText(): string {
  return `axond ${AXOND_VERSION}\n`;
}

export function helpText(bin: string): string {
  return `${ABOUT}\n\nUsage: ${bin}\n\nOptions:\n  -h, --help     Print help\n  -V, --version  Print version\n`;
}

/**
 * Node and `bun file.ts` put the runtime in `argv[0]` and this file in
 * `argv[1]`. A compiled Bun binary keeps `argv[0]` as `bun` and `argv[1]` as
 * `/$bunfs/root/<entry>`; the file the operator invoked is `execPath`.
 */
export function cliArguments(argv: readonly string[], execPath = ""): readonly string[] {
  const invoked = basename(argv[0] ?? "");
  const script = argv[1];
  if (argv[0] === "bun" && script?.startsWith("/$bunfs/")) {
    return [usageBin(execPath), ...argv.slice(2)];
  }
  if (
    (invoked === "node" || invoked === "nodejs" || invoked === "bun") &&
    script !== undefined &&
    (script.endsWith("/main.ts") || script.endsWith("\\main.ts") || script === "main.ts")
  ) {
    return ["axond", ...argv.slice(2)];
  }
  return argv.length > 0 ? argv : ["axond"];
}

/** Clap 4.6.6 with `std`, `help`, `usage`, and `error-context`, and no other flags. */
export function parseArgv(argv: readonly string[]): ArgvDecision {
  const bin = usageBin(argv[0] ?? "");
  const args = argv.slice(1);
  let endOfFlags = false;
  for (let index = 0; index < args.length; index += 1) {
    const arg = args[index]!;
    if (!endOfFlags && arg === "--") {
      endOfFlags = true;
      continue;
    }
    if (!endOfFlags && arg.startsWith("--")) {
      if (arg === "--help") {
        return { action: "stdout", code: 0, text: helpText(bin) };
      }
      if (arg === "--version") {
        return { action: "stdout", code: 0, text: versionText() };
      }
      if (arg.startsWith("--help=")) {
        return tooManyValues(bin, "--help", arg.slice("--help=".length));
      }
      if (arg.startsWith("--version=")) {
        return tooManyValues(bin, "--version", arg.slice("--version=".length));
      }
      return unexpected(bin, arg);
    }
    if (!endOfFlags && arg.startsWith("-") && arg.length > 1) {
      for (const character of arg.slice(1)) {
        if (character === "h") {
          return { action: "stdout", code: 0, text: helpText(bin) };
        }
        if (character === "V") {
          return { action: "stdout", code: 0, text: versionText() };
        }
        return unexpected(bin, `-${character}`);
      }
    }
    return unexpected(bin, arg);
  }
  return { action: "serve" };
}

function unexpected(bin: string, arg: string): ArgvDecision {
  return {
    action: "stderr",
    code: 2,
    text: `error: unexpected argument '${arg}' found\n\nUsage: ${bin}\n\nFor more information, try '--help'.\n`,
  };
}

function tooManyValues(bin: string, flag: string, value: string): ArgvDecision {
  return {
    action: "stderr",
    code: 2,
    text: `error: unexpected value '${value}' for '${flag}' found; no more were expected\n\nUsage: ${bin} ${flag}\n\nFor more information, try '--help'.\n`,
  };
}
