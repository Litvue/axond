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


test("later_credential_array_replaces_an_earlier_nested_weight", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-env-order-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "credential.toml");
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
  const extra: Record<string, string> = { GW_KEY: "k" };
  extra["AXOND_CREDENTIAL__0__WEIGHT"] = "1.5";
  extra["AXOND_CREDENTIAL"] = '[{namespace="platform",provider="openai",weight=1.5}]';
  try {
    const result = await run(config, root, extra);
    assert.equal(result.code, 1);
    assert.equal(result.stdout, "");
    assert.equal(
      result.stderr,
      "Error: failed to load config from `" +
        config +
        '`: config load: invalid type: found float `1.5`, expected u32 for key "CREDENTIAL.0.WEIGHT" in `AXOND_` environment variable(s)\n',
    );
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});


test("u64_overall_timeout_boots", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-u64-timeout-"));
  const port = await freePort();
  const config = join(root, "axond.toml");
  await writeFile(
    config,
    `
[server]
bind = "127.0.0.1:${port}"
[storage]
backend = "sqlite"
path = "${join(root, "axond.sqlite")}"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
`,
  );
  const served = spawn(BIN.pathname, {
    cwd: root,
    env: {
      ...process.env,
      AXOND_CONFIG: config,
      GW_KEY: "k",
      AXOND_FAILOVER__OVERALL_TIMEOUT_MS: "18446744073709551615",
    },
    stdio: ["ignore", "pipe", "pipe"],
  });
  let stderr = "";
  served.stderr?.on("data", (chunk: Buffer) => {
    stderr += chunk.toString();
  });
  try {
    await waitFor(`http://127.0.0.1:${port}/healthz`);
    const health = await fetch(`http://127.0.0.1:${port}/healthz`);
    assert.equal(health.status, 200);
    assert.equal(await health.text(), "ok");
  } finally {
    served.kill("SIGTERM");
    await new Promise((resolve) => served.once("exit", resolve));
    await rm(root, { recursive: true, force: true });
  }
  assert.equal(stderr, "", stderr);
});


test("usage_sink_batch_above_the_buffer_is_refused_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-usage-batch-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "usage.toml");
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
[[usage_sink]]
kind = "postgres"
dsn_env = "DSN"
buffer_capacity = 1
max_batch = 9223372036854775807
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
        "`: invalid config: usage_sink `postgres`: max_batch (9223372036854775807) must not exceed buffer_capacity (1)\n",
    );
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});


test("toml_integer_outside_i64_is_a_parse_error_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-toml-range-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "range.toml");
  const line = "failover = { overall_timeout_ms = 9223372036854775808 }";
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
  const column = line.indexOf("9223372036854775808");
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
    "number too large to fit in target type\n" +
    " in range.toml TOML file\n";
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

    await writeFile(config, `n = -9223372036854775809\n[storage]\nbackend = "sqlite"\npath = "${db}"\n`);
    const below = await run(config, root, { GW_KEY: "k" });
    assert.equal(below.code, 1);
    assert.equal(below.stdout, "");
    assert.match(below.stderr, /number too small to fit in target type\n in range\.toml TOML file\n$/);
    await assert.rejects(() => stat(db));

    await writeFile(config, `n = 0x8000000000000000\n[storage]\nbackend = "sqlite"\npath = "${db}"\n`);
    const hex = await run(config, root, { GW_KEY: "k" });
    assert.equal(hex.code, 1);
    assert.match(hex.stderr, /1 \| n = 0x8000000000000000\n {2}\| {5}\^\nnumber too large to fit in target type\n/);
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});


test("trailing_comma_in_an_inline_table_is_a_parse_error_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-inline-comma-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "inline.toml");
  const line = "failover = { overall_timeout_ms = 8, }";
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
  const column = line.indexOf(",");
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
    "invalid inline table\nexpected `}`\n" +
    " in inline.toml TOML file\n";
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

    await writeFile(config, `failover = {\n  overall_timeout_ms = 8\n}\n[storage]\nbackend = "sqlite"\npath = "${db}"\n`);
    const opened = await run(config, root, { GW_KEY: "k" });
    assert.equal(opened.code, 1);
    assert.equal(opened.stdout, "");
    assert.match(opened.stderr, /invalid inline table\nexpected `}`\n in inline\.toml TOML file\n$/);
    await assert.rejects(() => stat(db));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});


test("hex_escape_in_a_string_is_a_parse_error_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-hex-escape-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "escape.toml");
  const line = 'path = "data\\x41.db"';
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
  const column = line.indexOf("\\x") + 2;
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
    "invalid escape sequence\nexpected `b`, `f`, `n`, `r`, `t`, `u`, `U`, `\\`, `\"`\n" +
    " in escape.toml TOML file\n";
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


test("unknown_escape_is_a_parse_error_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-unknown-escape-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "letter.toml");
  const line = 'path = "data\\a.db"';
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
  const column = line.indexOf("\\a") + 2;
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
    "invalid escape sequence\nexpected `b`, `f`, `n`, `r`, `t`, `u`, `U`, `\\`, `\"`\n" +
    " in letter.toml TOML file\n";
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


test("control_character_is_a_parse_error_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-control-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "control.toml");
  const line = 'path = "data\u0001.db"';
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
  const column = line.indexOf("\u0001");
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
    "invalid basic string\n" +
    " in control.toml TOML file\n";
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


test("unicode_key_is_a_parse_error_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-unicode-key-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "cafe.toml");
  const line = "café = 1";
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
  const column = line.indexOf("é");
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
    "^^\n" +
    "expected `.`, `=`\n" +
    " in cafe.toml TOML file\n";
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


test("unclosed_inline_table_is_a_parse_error_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-unclosed-inline-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "open.toml");
  const line = "n = { a = 1, b = 2";
  await writeFile(config, line);
  const column = line.length;
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
    "invalid inline table\nexpected `}`\n" +
    " in open.toml TOML file\n";
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


test("capital_true_is_a_string_error_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-capital-true-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "true.toml");
  const line = "n = TRUE";
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
  const column = line.indexOf("T");
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
    "invalid string\nexpected `\"`, `'`\n" +
    " in true.toml TOML file\n";
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


test("unfinished_local_time_is_a_parse_error_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-unfinished-time-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "time.toml");
  const line = "n = 07:";
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
  const column = line.length;
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
    " in time.toml TOML file\n";
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


test("unfinished_date_is_a_parse_error_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-unfinished-date-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "date.toml");
  const line = "n = 1979-";
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
  const column = line.length;
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
    " in date.toml TOML file\n";
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


test("signed_date_is_an_integer_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-signed-date-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "plusdate.toml");
  const line = "n = +1979-05-27";
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
  const column = line.indexOf("-");
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
    " in plusdate.toml TOML file\n";
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


test("sign_without_a_digit_is_a_parse_error_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-sign-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "plus.toml");
  const line = "n = +";
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
  const column = line.length;
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
    "invalid integer\n" +
    " in plus.toml TOML file\n";
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


test("leading_dot_is_a_parse_error_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-leading-dot-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "dot.toml");
  const line = "n = .5";
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
  const column = line.indexOf(".");
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
    "invalid floating-point number\nexpected leading digit\n" +
    " in dot.toml TOML file\n";
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


test("duplicate_key_is_a_parse_error_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-duplicate-key-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "dup.toml");
  const line = "n = 2";
  await writeFile(
    config,
    `n = 1
${line}
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
  const column = 0;
  const pad = "  ";
  const diagram =
    "TOML parse error at line 2, column " +
    (column + 1) +
    "\n" +
    pad +
    "|\n" +
    "2 | " +
    line +
    "\n" +
    pad +
    "|" +
    " ".repeat(column + 1) +
    "^\n" +
    "duplicate key `n` in document root\n" +
    " in dup.toml TOML file\n";
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


test("empty_dotted_key_is_a_parse_error_before_the_store_opens", async () => {
  const root = await mkdtemp(join(tmpdir(), "axond-dotted-key-"));
  const db = join(root, "fresh.sqlite");
  const config = join(root, "dotted.toml");
  const line = "a..b = 1";
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
  const column = line.indexOf(".");
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
    "expected `.`, `=`\n" +
    " in dotted.toml TOML file\n";
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
