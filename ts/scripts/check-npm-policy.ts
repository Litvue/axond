/**
 * Advisory and license gate for the TypeScript lockfile.
 *
 * `npm audit` covers the whole lockfile, including devDependencies. Licenses
 * are checked on the production graph (`npm ci --omit=dev`), which is what the
 * container installs. The allow list matches deny.toml, plus 0BSD for tslib.
 */
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { join } from "node:path";

const root = new URL("..", import.meta.url).pathname;

const ALLOW = new Set([
  "Apache-2.0",
  "Apache-2.0 WITH LLVM-exception",
  "MIT",
  "BSD-2-Clause",
  "BSD-3-Clause",
  "ISC",
  "Unicode-3.0",
  "Zlib",
  "CDLA-Permissive-2.0",
  "BSL-1.0",
  "0BSD",
]);

execFileSync("npm", ["audit", "--audit-level=low"], { cwd: root, stdio: "inherit" });

interface LsNode {
  version?: string;
  extraneous?: boolean;
  dependencies?: Record<string, LsNode>;
}

const tree = JSON.parse(execFileSync("npm", ["ls", "--omit=dev", "--all", "--json"], { cwd: root, encoding: "utf8" })) as LsNode & {
  name?: string;
};
const rows: { name: string; version: string; license: string }[] = [];

function walk(name: string, node: LsNode): void {
  if (!node.extraneous && node.version) {
    rows.push({ name, version: node.version, license: licenseOf(name) });
  }
  for (const [child, value] of Object.entries(node.dependencies ?? {})) {
    walk(child, value);
  }
}

if (tree.name) {
  walk(tree.name, tree);
}

const failures: string[] = [];
for (const row of rows) {
  if (!licenseAllowed(row.license)) {
    failures.push(`${row.name}@${row.version} license ${row.license}`);
  }
}
if (failures.length > 0) {
  process.stderr.write(`npm license check failed:\n${failures.join("\n")}\n`);
  process.exit(1);
}
process.stdout.write(`npm policy: audit clean, ${rows.length} production packages licensed\n`);

function licenseOf(name: string): string {
  const path = name === "axond-typescript" ? join(root, "package.json") : join(root, "node_modules", name, "package.json");
  const pkg = JSON.parse(readFileSync(path, "utf8")) as { license?: string | { type?: string } };
  if (typeof pkg.license === "string") {
    return pkg.license;
  }
  if (pkg.license && typeof pkg.license.type === "string") {
    return pkg.license.type;
  }
  return "MISSING";
}

function licenseAllowed(expression: string): boolean {
  const trimmed = expression.trim();
  if (trimmed.startsWith("(") && trimmed.endsWith(")") && wraps(trimmed)) {
    return licenseAllowed(trimmed.slice(1, -1));
  }
  const orParts = splitTop(trimmed, "OR");
  if (orParts.length > 1) {
    return orParts.some(licenseAllowed);
  }
  const andParts = splitTop(trimmed, "AND");
  if (andParts.length > 1) {
    return andParts.every(licenseAllowed);
  }
  return ALLOW.has(trimmed);
}

function splitTop(expression: string, operator: "OR" | "AND"): string[] {
  const parts: string[] = [];
  let depth = 0;
  let current = "";
  const tokens = expression.split(/\s+/);
  for (let index = 0; index < tokens.length; index += 1) {
    const token = tokens[index]!;
    if (token === "(") {
      depth += 1;
    }
    if (token === ")") {
      depth -= 1;
    }
    if (depth === 0 && token === operator) {
      parts.push(current.trim());
      current = "";
      continue;
    }
    current = current.length === 0 ? token : `${current} ${token}`;
  }
  const tail = current.trim();
  if (tail.length > 0) {
    parts.push(tail);
  }
  return parts;
}

function wraps(expression: string): boolean {
  let depth = 0;
  for (let index = 0; index < expression.length; index += 1) {
    const char = expression[index];
    if (char === "(") {
      depth += 1;
    } else if (char === ")") {
      depth -= 1;
      if (depth === 0) {
        return index === expression.length - 1;
      }
    }
  }
  return false;
}
