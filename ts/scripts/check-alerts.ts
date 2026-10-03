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
const patterns = [
  /(?:rate|increase|label_values)\((axond_[a-z0-9_]+)/g,
  /(?:max|min|sum|avg)\((axond_[a-z0-9_]+)/g,
  /(?:max|min|sum|avg) by \([^)]*\) \((axond_[a-z0-9_]+)/g,
];
const missing = new Set<string>();

for (const file of files) {
  const text = await readFile(resolve(root, file), "utf8");
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
