import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

import { METRIC_NAMES } from "../packages/gateway/src/metrics.ts";

const root = resolve(new URL("../..", import.meta.url).pathname);
const files = [
  "ops/observability/alerts/axond-alerts.yml",
  "ops/observability/dashboards/axond-fleet.json",
  "ops/observability/dashboards/axond-tenancy.json",
];
const catalogue = new Set(METRIC_NAMES.map((name) => name.replaceAll(".", "_")));
const patterns = [/\b(axond_[a-z0-9_]+)(?=\s*(?:\{|\[|\)|,|$))/gm];
const missing = new Set<string>();

for (const file of files) {
  const source = await readFile(resolve(root, file), "utf8");
  const expressions: string[] = [];
  if (file.endsWith(".json")) {
    const visit = (value: unknown): void => {
      if (value === null || typeof value !== "object") return;
      for (const [key, item] of Object.entries(value)) {
        if ((key === "expr" || key === "query") && typeof item === "string") expressions.push(item);
        else visit(item);
      }
    };
    visit(JSON.parse(source));
  } else {
    for (const line of source.split("\n")) {
      const match = /^\s*expr:\s*(.*)$/.exec(line);
      if (match) expressions.push(match[1]!.startsWith('"') ? JSON.parse(match[1]!) : match[1]!);
    }
  }
  const text = expressions.join("\n").replace(/\{[^}]*\}/g, "{}").replace(/\b(?:by|without|on|ignoring|group_left|group_right)\s*\([^)]*\)/g, "").replace(/(label_values\([^,]*),[^)]*/g, "$1");
  for (const pattern of patterns) {
    for (const match of text.matchAll(pattern)) {
      const raw = match[1]!;
      const base = raw.replace(/_(bucket|sum|count)$/, "");
      if (!catalogue.has(base) && !catalogue.has(raw)) {
        missing.add(`${file}: ${raw}`);
      }
    }
  }
}

if (missing.size > 0) {
  process.stderr.write(
    `alert or dashboard metrics missing from the TypeScript catalogue:\n${[...missing].sort().join("\n")}\n`,
  );
  process.exit(1);
}
process.stdout.write(`observability catalogue covers ${catalogue.size} names used by alerts and dashboards\n`);
