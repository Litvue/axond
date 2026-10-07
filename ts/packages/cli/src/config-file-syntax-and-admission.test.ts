import assert from "node:assert/strict";

import { spawn } from "node:child_process";

import { chmod, mkdir, mkdtemp, rm, stat, writeFile } from "node:fs/promises";

import { createServer } from "node:net";

import { tmpdir } from "node:os";

import { join } from "node:path";

import test from "node:test";


import { figmentFileSource, locateConfigFile } from "./config-file.ts";


const BIN = new URL("../../../bin/axond", import.meta.url);

const STORAGE =
  'Error: failed to load config from `%s`: invalid config: `[storage]` is required (ADR 0063): set `backend = "sqlite"` with `path`, or `backend = "postgres"` with `dsn_env`\n';


function run(
  config: string,
  cwd: string,
  extra: Record<string, string> = {},
): Promise<{ code: number | null; stdout: string; stderr: string }> {
  return new Promise((resolve, reject) => {
    const child = spawn(BIN.pathname, {
      cwd,
      env: { ...process.env, ...extra, AXOND_CONFIG: config },
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
    child.on("close", (code) => resolve({ code, stdout, stderr }));
  });
}


function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = createServer();
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      const port = typeof address === "object" && address ? address.port : 0;
      server.close(() => resolve(port));
    });
    server.on("error", reject);
  });
}


async function waitFor(url: string): Promise<void> {
  let last = "";
  for (let attempt = 0; attempt < 40; attempt += 1) {
    try {
      const response = await fetch(url);
      if (response.ok) return;
      last = `${response.status}`;
    } catch (error) {
      last = error instanceof Error ? error.message : "fetch failed";
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  throw new Error(`timed out waiting for ${url}: ${last}`);
}


test("bad_underscore_is_a_parse_error_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-underscore-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "under.toml");
  const line = "n = 1__2";
  await writeFile(
    config,
    `${line}
[shutdown]
nope = 1
[server]
bind = "127.0.0.1:9"
[storage]
backend = "sqlite"
path = "${db}"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
`,
  );
  const column = line.indexOf("__") + 1;
  const pad = "  ";
  const diagram =
    "TOML parse error at line 1, column " +
    (column + 1) +
    "\n" +
    pad +
    "|\n" +
    "1 | " +
    line +
    "\n" +
    pad +
    "|" +
    " ".repeat(column + 1) +
    "^\n" +
    "invalid integer\nexpected digit\n" +
    " in under.toml TOML file\n";
  try {
    const result = await run(config, root, { GW_KEY: "k" });
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.equal(
      result.stderr,
      "Error: failed to load config from `" + config + "`: config load: " + diagram,
    );
    assert.equal(result.stderr.includes("unknown field"), false);
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});


test("short_unicode_escape_is_a_parse_error_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-unicode-escape-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "unicode.toml");
  const line = 'path = "data\\u41.db"';
  await writeFile(
    config,
    `${line}
[shutdown]
nope = 1
[server]
bind = "127.0.0.1:9"
[storage]
backend = "sqlite"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
`,
  );
  const column = line.indexOf("\\u") + 2;
  const pad = "  ";
  const diagram =
    "TOML parse error at line 1, column " +
    (column + 1) +
    "\n" +
    pad +
    "|\n" +
    "1 | " +
    line +
    "\n" +
    pad +
    "|" +
    " ".repeat(column + 1) +
    "^\n" +
    "invalid unicode 4-digit hex code\n" +
    " in unicode.toml TOML file\n";
  try {
    const result = await run(config, root, { GW_KEY: "k" });
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.equal(
      result.stderr,
      "Error: failed to load config from `" + config + "`: config load: " + diagram,
    );
    assert.equal(result.stderr.includes("unknown field"), false);
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});


test("leading_zero_is_a_parse_error_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-leading-zero-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "zero.toml");
  const line = "n = 0922";
  await writeFile(
    config,
    `${line}
[shutdown]
nope = 1
[server]
bind = "127.0.0.1:9"
[storage]
backend = "sqlite"
path = "${db}"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
`,
  );
  const column = line.indexOf("0922") + 1;
  const pad = "  ";
  const diagram =
    "TOML parse error at line 1, column " +
    (column + 1) +
    "\n" +
    pad +
    "|\n" +
    "1 | " +
    line +
    "\n" +
    pad +
    "|" +
    " ".repeat(column + 1) +
    "^\n" +
    "expected newline, `#`\n" +
    " in zero.toml TOML file\n";
  try {
    const result = await run(config, root, { GW_KEY: "k" });
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.equal(
      result.stderr,
      "Error: failed to load config from `" + config + "`: config load: " + diagram,
    );
    assert.equal(result.stderr.includes("unknown field"), false);
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});


test("float_past_the_finite_range_is_a_parse_error_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-float-range-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "float.toml");
  const line = "n = 1e309";
  await writeFile(
    config,
    `${line}
[shutdown]
nope = 1
[server]
bind = "127.0.0.1:9"
[storage]
backend = "sqlite"
path = "${db}"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
`,
  );
  const column = line.indexOf("1e309");
  const pad = "  ";
  const diagram =
    "TOML parse error at line 1, column " +
    (column + 1) +
    "\n" +
    pad +
    "|\n" +
    "1 | " +
    line +
    "\n" +
    pad +
    "|" +
    " ".repeat(column + 1) +
    "^\n" +
    "invalid floating-point number\n" +
    " in float.toml TOML file\n";
  try {
    const result = await run(config, root, { GW_KEY: "k" });
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.equal(
      result.stderr,
      "Error: failed to load config from `" + config + "`: config load: " + diagram,
    );
    assert.equal(result.stderr.includes("unknown field"), false);
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});


test("impossible_calendar_day_is_a_parse_error_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-calendar-day-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "day.toml");
  const line = "n = 2024-02-30";
  await writeFile(
    config,
    `${line}
[shutdown]
nope = 1
[server]
bind = "127.0.0.1:9"
[storage]
backend = "sqlite"
path = "${db}"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
`,
  );
  const column = line.indexOf("2024-02-30") + 8;
  const pad = "  ";
  const diagram =
    "TOML parse error at line 1, column " +
    (column + 1) +
    "\n" +
    pad +
    "|\n" +
    "1 | " +
    line +
    "\n" +
    pad +
    "|" +
    " ".repeat(column + 1) +
    "^\n" +
    "invalid date-time\n" +
    "value is out of range\n" +
    " in day.toml TOML file\n";
  try {
    const result = await run(config, root, { GW_KEY: "k" });
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.equal(
      result.stderr,
      "Error: failed to load config from `" + config + "`: config load: " + diagram,
    );
    assert.equal(result.stderr.includes("unknown field"), false);
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});


test("leap_second_reaches_extract_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-leap-second-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "leap.toml");
  await writeFile(
    config,
    `n = 2024-01-01T23:59:60Z
[shutdown]
nope = 1
[server]
bind = "127.0.0.1:9"
[storage]
backend = "sqlite"
path = "${db}"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
`,
  );
  try {
    const result = await run(config, root, { GW_KEY: "k" });
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.match(result.stderr, /unknown field/);
    assert.equal(result.stderr.includes("invalid date"), false);
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});


test("impossible_clock_is_a_parse_error_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-clock-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "clock.toml");
  const line = "n = 23:60:00";
  await writeFile(
    config,
    `${line}
[shutdown]
nope = 1
[server]
bind = "127.0.0.1:9"
[storage]
backend = "sqlite"
path = "${db}"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
`,
  );
  const column = line.indexOf("60");
  const pad = "  ";
  const diagram =
    "TOML parse error at line 1, column " +
    (column + 1) +
    "\n" +
    pad +
    "|\n" +
    "1 | " +
    line +
    "\n" +
    pad +
    "|" +
    " ".repeat(column + 1) +
    "^\n" +
    "invalid time\n" +
    "value is out of range\n" +
    " in clock.toml TOML file\n";
  try {
    const result = await run(config, root, { GW_KEY: "k" });
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.equal(
      result.stderr,
      "Error: failed to load config from `" + config + "`: config load: " + diagram,
    );
    assert.equal(result.stderr.includes("unknown field"), false);
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});


test("usage_journal_postgres_is_refused_after_an_earlier_bound", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-journal-order-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "journal.toml");
  await writeFile(
    config,
    `
[server]
bind = "127.0.0.1:9"
[storage]
backend = "sqlite"
path = "${db}"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
[admission]
max_request_bytes = 0
[usage_journal]
backend = "postgres"
`,
  );
  try {
    const result = await run(config, root, { GW_KEY: "k" });
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.equal(
      result.stderr,
      "Error: failed to load config from `" +
        config +
        "`: invalid config: admission.max_request_bytes must be at least 1\n",
    );
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});


test("tenant_ceiling_above_the_global_one_is_refused_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-tenant-ceiling-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "admission.toml");
  const header = `
[server]
bind = "127.0.0.1:9"
[storage]
backend = "sqlite"
path = "${db}"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
[admission]
`;
  try {
    await writeFile(
      config,
      `${header}max_in_flight = 16\nmax_in_flight_per_tenant = 2305843009213693952\n`,
    );
    const above = await run(config, root, { GW_KEY: "k" });
    assert.equal(above.code, 1);
    assert.equal(above.stdout, "");
    assert.equal(
      above.stderr,
      "Error: failed to load config from `" +
        config +
        "`: invalid config: admission.max_in_flight_per_tenant (2305843009213693952) must not exceed admission.max_in_flight (16): a per-tenant ceiling above the global one cannot isolate a tenant\n",
    );
    await assert.rejects(() => stat(db));
    await writeFile(config, `${header}max_tenants = 0\n`);
    const tenants = await run(config, root, { GW_KEY: "k" });
    assert.equal(tenants.code, 1);
    assert.equal(tenants.stdout, "");
    assert.equal(
      tenants.stderr,
      "Error: failed to load config from `" +
        config +
        "`: invalid config: admission.max_tenants must be at least 1 when max_in_flight_per_tenant is set\n",
    );
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});


test("admission_ceiling_above_the_semaphore_is_refused_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-admit-ceiling-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "admission.toml");
  await writeFile(
    config,
    `
[server]
bind = "127.0.0.1:9"
[storage]
backend = "sqlite"
path = "${db}"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
`,
  );
  try {
    const result = await run(config, root, {
      GW_KEY: "k",
      AXOND_ADMISSION__MAX_IN_FLIGHT: "2305843009213693952",
    });
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.equal(
      result.stderr,
      "Error: failed to load config from `" +
        config +
        "`: invalid config: admission.max_in_flight (2305843009213693952) must not exceed 2305843009213693951: a larger ceiling is not a bound this process can hold\n",
    );
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});


test("admission_float_beats_a_later_failover_array", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-order-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "order.toml");
  await writeFile(
    config,
    `
failover = [1.5]
[admission]
max_request_bytes = 1.5
[server]
bind = "127.0.0.1:9"
[storage]
backend = "sqlite"
path = "${db}"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
`,
  );
  try {
    const result = await run(config, root, { GW_KEY: "k" });
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.equal(
      result.stderr,
      "Error: failed to load config from `" +
        config +
        '`: config load: invalid type: found float `1.5`, expected usize for key "default.admission.max_request_bytes" in ' +
        figmentFileSource(config, root) +
        " TOML file\n",
    );
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
