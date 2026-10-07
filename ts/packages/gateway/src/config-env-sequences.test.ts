import assert from "node:assert/strict";

import test from "node:test";


import { envSecretReader, loadConfig, usageBatchSize } from "./config.ts";


const BASE = `
[server]
bind = "127.0.0.1:9"
[storage]
backend = "sqlite"
path = "/tmp/axond.sqlite"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
`;


const secrets = envSecretReader({ GW_KEY: "k" }, async () => "");


const GRAPH = `
[storage]
backend = "sqlite"
path = "/tmp/axond.sqlite"
[[namespace]]
id = "platform"
default = true
[[provider]]
id = "openai"
kind = "openai"
base_url = "http://127.0.0.1:9"
[[credential]]
namespace = "platform"
provider = "openai"
env = "OPENAI_KEY"
id = "primary"
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
`;


test("an earlier figment key finishes before a later sequence fill", async () => {
  const tail = `
[server]
bind = "127.0.0.1:9"
[storage]
backend = "sqlite"
path = "/tmp/axond.sqlite"
[[namespace]]
id = "platform"
default = true
[[gateway_key]]
env = "GW_KEY"
namespace = "platform"
`;
  const reject = async (toml: string, message: string, extra: Record<string, string> = {}) => {
    await assert.rejects(
      () => loadConfig(toml, envSecretReader({ GW_KEY: "k", ...extra }, async () => "")),
      (error: unknown) => {
        assert.equal(error instanceof Error ? error.message : "", message);
        return true;
      },
    );
  };
  const typed = (found: string, expected: string, key: string) =>
    `config: invalid type: found ${found}, expected ${expected} for key "${key}"`;
  await reject(
    `failover = [1.5]\n[admission]\nmax_request_bytes = 1.5\n${tail}`,
    typed("float `1.5`", "usize", "default.admission.max_request_bytes"),
  );
  await reject(
    `namespace = "x"\n[admission]\nmax_request_bytes = 1.5\n[server]\nbind = "127.0.0.1:9"\n[storage]\nbackend = "sqlite"\npath = "/tmp/axond.sqlite"\n[[gateway_key]]\nenv = "GW_KEY"\nnamespace = "platform"\n`,
    typed("float `1.5`", "usize", "default.admission.max_request_bytes"),
  );
  await reject(
    `failover = [1.5]\n[blocklist]\nmodels = [1]\n${tail}`,
    typed("signed int `1`", "a string", "default.blocklist.models.0"),
  );
  await reject(
    `failover = [1.5]\n${tail}[[credential]]\nnamespace = "platform"\nprovider = "openai"\nenv = "OPENAI_KEY"\nweight = 1.5\n`,
    typed("float `1.5`", "u32", "default.credential.0.weight"),
  );
  await reject(
    `failover = [1.5]\n${tail}`,
    typed("float `1.5`", "usize", "ADMISSION.0") + " in `AXOND_` environment variable(s)",
    { AXOND_ADMISSION: "[1.5]" },
  );
  await reject(
    `failover = [1.5]\n${tail}`,
    typed("float `1.5`", "u32", "CREDENTIAL.0.WEIGHT") + " in `AXOND_` environment variable(s)",
    { AXOND_CREDENTIAL: '[{weight=1.5}]' },
  );
  await reject(
    `failover = [1.5]\n${tail}`,
    typed("unsigned int `1`", "a string", "BLOCKLIST.0.0") + " in `AXOND_` environment variable(s)",
    { AXOND_BLOCKLIST: "[[1]]" },
  );
  await reject(
    `failover = [1.5]\n${tail}`,
    typed("float `1.5`", "u32", "default.failover.0"),
    { AXOND_NAMESPACE: "[1]" },
  );
  await reject(
    `failover = [1.5]\n${tail}`,
    typed("float `1.5`", "u32", "default.failover.0"),
    { AXOND_NAMESPACE: "[[1]]" },
  );
  await reject(
    tail,
    typed("unsigned int `1`", "a string", "NAMESPACE.0.0") + " in `AXOND_` environment variable(s)",
    { AXOND_NAMESPACE: "[[1]]" },
  );
  await reject(
    tail,
    'config: unknown variant: found `nope`, expected `one of `openai`, `anthropic`, `openai-compatible`` for key "PROVIDER.0.1" in `AXOND_` environment variable(s)',
    { AXOND_PROVIDER: '[["openai", "nope"]]' },
  );
  await reject(
    tail,
    typed("sequence", "struct PriceRule", "PRICE.0") + " in `AXOND_` environment variable(s)",
    { AXOND_PRICE: "[[1]]" },
  );
  await reject(
    tail,
    'config: invalid length 2, expected struct GatewayKey with 3 elements for key "GATEWAY_KEY.0" in `AXOND_` environment variable(s)',
    { AXOND_GATEWAY_KEY: "[[]]" },
  );
});


test("a later AXOND_ value replaces an earlier one and strings use figment escapes", async () => {
  const envTyped = (found: string, expected: string, key: string) =>
    `config: invalid type: found ${found}, expected ${expected} for key "${key}" in \`AXOND_\` environment variable(s)`;
  const weight = '[{namespace="platform",provider="openai",weight=1.5}]';
  const arrayLater: Record<string, string> = {};
  arrayLater["AXOND_CREDENTIAL__0__WEIGHT"] = "1.5";
  arrayLater["AXOND_CREDENTIAL"] = weight;
  await assert.rejects(
    () => loadConfig(BASE, envSecretReader({ GW_KEY: "k", ...arrayLater }, async () => "")),
    (error: unknown) => {
      assert.equal(error instanceof Error ? error.message : "", envTyped("float `1.5`", "u32", "CREDENTIAL.0.WEIGHT"));
      return true;
    },
  );
  const mapLater: Record<string, string> = {};
  mapLater["AXOND_CREDENTIAL"] = weight;
  mapLater["AXOND_CREDENTIAL__0__WEIGHT"] = "1.5";
  await assert.rejects(
    () => loadConfig(BASE, envSecretReader({ GW_KEY: "k", ...mapLater }, async () => "")),
    (error: unknown) => {
      assert.equal(error instanceof Error ? error.message : "", envTyped("map", "a sequence", "CREDENTIAL"));
      return true;
    },
  );
  const leafLater: Record<string, string> = {};
  leafLater["AXOND_FAILOVER"] = "[4, 9]";
  leafLater["AXOND_FAILOVER__MAX_ATTEMPTS"] = "8";
  const replaced = await loadConfig(BASE, envSecretReader({ GW_KEY: "k", ...leafLater }, async () => ""));
  assert.equal(replaced.transport.maxAttempts, 8);
  assert.equal(replaced.transport.overallTimeoutMs, 30_000);
  const sequenceLater: Record<string, string> = {};
  sequenceLater["AXOND_FAILOVER__MAX_ATTEMPTS"] = "8";
  sequenceLater["AXOND_FAILOVER"] = "[4, 9]";
  const filled = await loadConfig(BASE, envSecretReader({ GW_KEY: "k", ...sequenceLater }, async () => ""));
  assert.equal(filled.transport.maxAttempts, 4);
  assert.equal(filled.transport.overallTimeoutMs, 9);
  const decoded = await loadConfig(
    BASE,
    envSecretReader({ GW_KEY: "k", AXOND_STORAGE__PATH: '"hi\\u0041"' }, async () => ""),
  );
  assert.equal(decoded.storage.path, "hiA");
  const rawEscape = await loadConfig(
    BASE,
    envSecretReader({ GW_KEY: "k", AXOND_STORAGE__PATH: '"hi\\q"' }, async () => ""),
  );
  assert.equal(rawEscape.storage.path, '"hi\\q"');
  await assert.rejects(
    () => loadConfig(BASE, envSecretReader({ GW_KEY: "k", AXOND_NAMESPACE: '[{id="a\\0",default=true}]' }, async () => "")),
    (error: unknown) => {
      assert.equal(
        error instanceof Error ? error.message : "",
        envTyped("string \"[{id=\\\"a\\\\0\\\",default=true}]\"", "a sequence", "NAMESPACE"),
      );
      return true;
    },
  );
});


test("a u64 overall timeout above 2^53 loads", async () => {
  const u64Max = "18446744073709551615";
  const wide = await loadConfig(
    BASE,
    envSecretReader({ GW_KEY: "k", AXOND_FAILOVER__OVERALL_TIMEOUT_MS: u64Max }, async () => ""),
  );
  assert.equal(wide.transport.overallTimeoutMs, BigInt(u64Max));
  await assert.rejects(
    () =>
      loadConfig(
        BASE,
        envSecretReader({ GW_KEY: "k", AXOND_FAILOVER__OVERALL_TIMEOUT_MS: "0" }, async () => ""),
      ),
    (error: unknown) => {
      assert.equal(error instanceof Error ? error.message : "", "failover.overall_timeout_ms must be at least 1");
      return true;
    },
  );
});


test("an admission ceiling above the semaphore limit names every digit", async () => {
  const permits = "2305843009213693951";
  const absurd = "2305843009213693952";
  const ceiling = (field: string, value: string) =>
    `admission.${field} (${value}) must not exceed ${permits}: a larger ceiling is not a bound this process can hold`;
  await assert.rejects(
    () =>
      loadConfig(
        BASE,
        envSecretReader({ GW_KEY: "k", AXOND_ADMISSION__MAX_IN_FLIGHT: absurd }, async () => ""),
      ),
    (error: unknown) => {
      assert.equal(error instanceof Error ? error.message : "", ceiling("max_in_flight", absurd));
      return true;
    },
  );
  await assert.rejects(
    () =>
      loadConfig(
        BASE,
        envSecretReader({ GW_KEY: "k", AXOND_ADMISSION__MAX_IN_FLIGHT_STREAMS: absurd }, async () => ""),
      ),
    (error: unknown) => {
      assert.equal(
        error instanceof Error ? error.message : "",
        `admission.max_in_flight_streams (${absurd}) must not exceed admission.max_in_flight (1024): a stream is an in-flight request`,
      );
      return true;
    },
  );
  const held = await loadConfig(
    BASE,
    envSecretReader({ GW_KEY: "k", AXOND_ADMISSION__MAX_IN_FLIGHT: permits }, async () => ""),
  );
  assert.equal(held.admission.maxInFlight, Number.MAX_SAFE_INTEGER);
  const wideBytes = await loadConfig(
    BASE,
    envSecretReader(
      { GW_KEY: "k", AXOND_ADMISSION__MAX_REQUEST_BYTES: "18446744073709551615" },
      async () => "",
    ),
  );
  assert.equal(wideBytes.maxRequestBytes, Number.MAX_SAFE_INTEGER);
});


test("a tenant ceiling that cannot isolate a tenant is refused before the semaphore bound", async () => {
  const exceed = (per: string, global: string) =>
    `admission.max_in_flight_per_tenant (${per}) must not exceed admission.max_in_flight (${global}): a per-tenant ceiling above the global one cannot isolate a tenant`;
  const tenants = "admission.max_tenants must be at least 1 when max_in_flight_per_tenant is set";
  const absurd = "2305843009213693952";
  await assert.rejects(
    () => loadConfig(`${BASE}\n[admission]\nmax_in_flight = 16\nmax_in_flight_per_tenant = 32\n`, secrets),
    (error: unknown) => {
      assert.equal(error instanceof Error ? error.message : "", exceed("32", "16"));
      return true;
    },
  );
  await assert.rejects(
    () =>
      loadConfig(
        `${BASE}\n[admission]\nmax_in_flight = 16\nmax_in_flight_per_tenant = 32\nmax_in_flight_streams = 64\n`,
        secrets,
      ),
    (error: unknown) => {
      assert.equal(error instanceof Error ? error.message : "", exceed("32", "16"));
      return true;
    },
  );
  await assert.rejects(
    () => loadConfig(`${BASE}\n[admission]\nmax_tenants = 0\n`, secrets),
    (error: unknown) => {
      assert.equal(error instanceof Error ? error.message : "", tenants);
      return true;
    },
  );
  await assert.rejects(
    () =>
      loadConfig(
        `${BASE}\n[admission]\nmax_in_flight = 16\nmax_in_flight_per_tenant = 4\nmax_tenants = 0\n`,
        secrets,
      ),
    (error: unknown) => {
      assert.equal(error instanceof Error ? error.message : "", tenants);
      return true;
    },
  );
  await assert.rejects(
    () => loadConfig(`${BASE}\n[admission]\nmax_in_flight = 257\nmax_tenants = 0\n`, secrets),
    (error: unknown) => {
      assert.equal(error instanceof Error ? error.message : "", tenants);
      return true;
    },
  );
  await assert.rejects(
    () =>
      loadConfig(`${BASE}\n[admission]\nmax_in_flight = 16\nmax_in_flight_per_tenant = ${absurd}\n`, secrets),
    (error: unknown) => {
      assert.equal(error instanceof Error ? error.message : "", exceed(absurd, "16"));
      return true;
    },
  );
  const lowered = await loadConfig(`${BASE}\n[admission]\nmax_in_flight = 16\n`, secrets);
  assert.equal(lowered.admission.maxInFlight, 16);
  const isolated = await loadConfig(
    `${BASE}\n[admission]\nmax_in_flight = 16\nmax_in_flight_per_tenant = 4\n`,
    secrets,
  );
  assert.equal(isolated.admission.maxInFlight, 16);
  const disabled = await loadConfig(
    `${BASE}\n[admission]\nmax_in_flight = 16\nmax_in_flight_per_tenant = 0\nmax_tenants = 0\n`,
    secrets,
  );
  assert.equal(disabled.admission.maxInFlight, 16);
  const turnedOff = await loadConfig(
    `${BASE}\n[admission]\nmax_in_flight = 16\nmax_tenants = 0\n`,
    secrets,
  );
  assert.equal(turnedOff.admission.maxInFlight, 16);
  const atGlobal = await loadConfig(
    `${BASE}\n[admission]\nmax_in_flight = 256\nmax_tenants = 0\n`,
    secrets,
  );
  assert.equal(atGlobal.admission.maxInFlight, 256);
  const env: Record<string, string> = {};
  env["AXOND_ADMISSION__MAX_IN_FLIGHT"] = "16";
  env["AXOND_ADMISSION__MAX_IN_FLIGHT_PER_TENANT"] = "32";
  await assert.rejects(
    () => loadConfig(BASE, envSecretReader({ GW_KEY: "k", ...env }, async () => "")),
    (error: unknown) => {
      assert.equal(error instanceof Error ? error.message : "", exceed("32", "16"));
      return true;
    },
  );
  await assert.rejects(
    () =>
      loadConfig(
        BASE,
        envSecretReader({ GW_KEY: "k", AXOND_ADMISSION__MAX_TENANTS: "0" }, async () => ""),
      ),
    (error: unknown) => {
      assert.equal(error instanceof Error ? error.message : "", tenants);
      return true;
    },
  );
});


test("an enabled usage journal is refused after earlier boot bounds", async () => {
  const notBuilt = '`[usage_journal] backend = "postgres"` is not built (ADR 0049)';
  await assert.rejects(
    () => loadConfig(`${BASE}\n[usage_journal]\nbackend = "postgres"\n`, secrets),
    (error: unknown) => {
      assert.equal(error instanceof Error ? error.message : "", notBuilt);
      return true;
    },
  );
  await assert.rejects(
    () =>
      loadConfig(
        `${BASE.replace('path = "/tmp/axond.sqlite"', 'path = ":memory:"')}\n[usage_journal]\nbackend = "postgres"\n`,
        secrets,
      ),
    (error: unknown) => {
      assert.equal(
        error instanceof Error ? error.message : "",
        "`[storage]` sqlite `:memory:` is not durable; use a file path",
      );
      return true;
    },
  );
  await assert.rejects(
    () =>
      loadConfig(`${BASE}\n[admission]\nmax_request_bytes = 0\n[usage_journal]\nbackend = "postgres"\n`, secrets),
    (error: unknown) => {
      assert.equal(error instanceof Error ? error.message : "", "admission.max_request_bytes must be at least 1");
      return true;
    },
  );
  await assert.rejects(
    () =>
      loadConfig(
        `${BASE}\n[[usage_sink]]\nkind = "postgres"\n[usage_journal]\nbackend = "postgres"\n`,
        secrets,
      ),
    (error: unknown) => {
      assert.equal(
        error instanceof Error ? error.message : "",
        "usage_sink `postgres`: `dsn_env` must name the env var holding the connection string",
      );
      return true;
    },
  );
});


test("an unknown usage sink kind and an enabled usage journal fail boot", async () => {
  await assert.rejects(
    () => loadConfig(`${BASE}\n[[usage_sink]]\nkind = "redis"\n`, secrets),
    (error: unknown) => {
      assert.equal(
        error instanceof Error ? error.message : "",
        'config: unknown variant: found `redis`, expected `one of `stdout`, `postgres`, `otlp`` for key "default.usage_sink.0.kind"',
      );
      return true;
    },
  );
  await assert.rejects(
    () => loadConfig(`${BASE}\n[usage_journal]\nbackend = "postgres"\ndsn_env = "JOURNAL_DSN"\n`, secrets),
    /not built \(ADR 0049\)/,
  );
});
