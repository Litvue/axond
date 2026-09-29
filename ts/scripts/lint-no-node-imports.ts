import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";

const roots = ["packages/sdk/src", "packages/gateway/src"];
const failures: string[] = [];

async function walk(directory: string): Promise<void> {
  const entries = await readdir(directory, { withFileTypes: true });
  for (const entry of entries) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) {
      await walk(path);
      continue;
    }
    if (!entry.name.endsWith(".ts") || entry.name.endsWith(".test.ts")) {
      continue;
    }
    const text = await readFile(path, "utf8");
    if (text.includes("node:")) {
      failures.push(path);
    }
  }
}

for (const root of roots) {
  await walk(new URL(`../${root}`, import.meta.url).pathname);
}

if (failures.length > 0) {
  process.stderr.write(`node: imports are not allowed in the core packages:\n${failures.join("\n")}\n`);
  process.exit(1);
}
