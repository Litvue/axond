import { createServer } from "node:http";
import { readFile, stat } from "node:fs/promises";
import { pathToFileURL } from "node:url";
import { Agent } from "undici";

import { getRequestListener } from "@hono/node-server";
import pg from "pg";

import {
  admissionFromOptions,
  createAdmission,
  createAxond,
  createMetrics,
  envSecretReader,
  loadConfig,
  resolveConfigSecrets,
  resolveTelemetry,
} from "../../gateway/src/index.ts";
import { cliArguments, parseArgv } from "./argv.ts";
import { figmentFileSource, locateConfigFile, wantsFigmentLocation } from "./config-file.ts";
import { createBackgroundDrain, remainingMs, settleShareMs as flushSettleShare } from "./shutdown-budget.ts";
import { openUsageDelivery } from "./usage-delivery.ts";
import type { AxondExtension, KeyMaterialLog, ShutdownLog } from "@axond/sdk";

import { discoverOnce, startDiscovery } from "./discovery.ts";
import { seedConfigNamespaces } from "./seed-namespaces.ts";
import { applyPostgresMigration, createPostgresStore, POSTGRES_SCHEMA } from "./postgres-store.ts";
import { openSqliteStore } from "./sqlite-store.ts";

const { Client } = pg;

async function main(): Promise<void> {
  const argv = parseArgv(cliArguments(process.argv, process.execPath));
  if (argv.action !== "serve") {
    const stream = argv.action === "stdout" ? process.stdout : process.stderr;
    stream.write(argv.text);
    process.exit(argv.code);
  }
  const configPath = process.env["AXOND_CONFIG"] ?? "axond.toml";
  const config = await loadOperatorConfig(configPath);
  const redactSecrets: string[] = [];
  const metrics = createMetrics(redactSecrets);
  const store =
    config.storage.backend === "sqlite"
      ? openSqliteStore(config.storage.path!, metrics)
      : await openPostgres(requirePostgresDsn(config.storage), metrics);
  await seedConfigNamespaces(store, config.namespaces);
  await resolveConfigSecrets(config, envSecretReader(process.env, readGatewayKeyFile));
  redactSecrets.push(config.gatewayKey);
  const extensions = await loadExtensionDir(config.extensionsDir ?? process.env["AXOND_EXTENSIONS_DIR"] ?? null);
  for (const extension of extensions) {
    for (const [index, sql] of (extension.migrations ?? []).entries()) {
      assertMigrationPrefix(extension.name, sql);
      const id = `${extension.name}:${index}`;
      if (config.storage.backend === "sqlite") {
        const { applyMigration } = await import("./sqlite-store.ts");
        applyMigration(config.storage.path!, id, sql);
      } else {
        await applyPostgresMigration(config.storage.dsn!, id, sql);
      }
    }
  }
  const telemetry = resolveTelemetry({
    endpoint: process.env["OTEL_EXPORTER_OTLP_ENDPOINT"],
    protocol: process.env["OTEL_EXPORTER_OTLP_PROTOCOL"],
    instanceId: process.env["AXOND_INSTANCE_ID"],
  });
  const background = createBackgroundDrain();
  const usageDelivery = await openUsageDelivery({
    sinks: config.usageSinks,
    env: process.env,
    telemetry,
    metrics,
    onBackground: background.track,
    onLog: (record) => {
      process.stdout.write(`${JSON.stringify(record)}\n`);
    },
  });
  let serving = true;
  let admitting = true;
  const admission = createAdmission(
    admissionFromOptions({
      maxInFlight: config.admission.maxInFlight,
      maxInFlightStreams: config.admission.maxInFlightStreams,
      queueCapacity: config.admission.queueCapacity,
      queueWaitMs: config.admission.queueWaitMs,
      maxPendingSettlements: config.admission.maxPendingSettlements,
      maxInFlightSettlements: config.admission.maxInFlightSettlements,
      settlementQueueWaitMs: config.admission.settlementQueueWaitMs,
      settlementTimeoutMs: config.admission.settlementTimeoutMs,
    }),
  );
  const app = createAxond({
    store,
    providers: config.providers,
    gatewayKey: config.gatewayKey,
    gatewayKeySubject: config.gatewayKeySubject,
    credentials: config.credentials,
    prices: config.prices,
    blocklist: config.blocklist,
    defaultNamespace: config.defaultNamespace,
    configNamespaces: config.namespaces.map((namespace) => namespace.id),
    transport: config.transport,
    maxRequestBytes: config.maxRequestBytes,
    maxPromptTokens: config.maxPromptTokens,
    maxOutputTokens: config.maxOutputTokens,
    maxStreamDurationMs: config.maxStreamDurationMs,
    maxStreamBytes: config.maxStreamBytes,
    admissionControl: admission,
    credentialPool: {
      strategy: config.credentialPool.strategy,
      failureThreshold: config.credentialPool.failureThreshold,
      cooldownMs: config.credentialPool.cooldownSeconds * 1000,
    },
    upstreamDispatcher: connectDispatcher(config.transport.connectTimeoutMs ?? 5_000),
    extensions,
    rawPath: (c) => c.req.header("x-axond-raw-path") ?? new URL(c.req.url).pathname,
    serving: () => serving,
    onStoreUnavailable: config.storage.onUnavailable,
    admitting: () => admitting,
    metrics,
    telemetry: telemetry ?? undefined,
    onBackground: background.track,
    onLog: (record) => {
      process.stdout.write(`${JSON.stringify(record)}\n`);
    },
    onUsage: (record) => {
      usageDelivery.write(record);
    },
  });
  const listener = getRequestListener(app.fetch);
  const server = createServer((req, res) => {
    const raw = (req.url ?? "/").split("?")[0] ?? "/";
    delete req.headers["x-axond-raw-path"];
    req.headers["x-axond-raw-path"] = raw;
    listener(req, res);
  });
  const [host, portText] = splitBind(config.bind);
  await new Promise<void>((resolve) => {
    server.listen(Number(portText), host, () => resolve());
  });
  const writeLog = (record: unknown) => {
    process.stdout.write(`${JSON.stringify(record)}\n`);
  };
  void discoverOnce({
    store,
    providers: config.providers,
    credentials: config.credentials,
    catalog: config.catalog,
    platformNamespace: config.defaultNamespace,
    metrics,
    onLog: writeLog,
  });
  const stopDiscovery = startDiscovery({
    store,
    providers: config.providers,
    credentials: config.credentials,
    catalog: config.catalog,
    platformNamespace: config.defaultNamespace,
    intervalSeconds: config.discoveryIntervalSeconds,
    metrics,
    onLog: writeLog,
  });
  let phase: "serving" | "draining" | "closing" = "serving";
  let exited = false;
  let deadlineTimer: ReturnType<typeof setTimeout> | undefined;
  const finish = async () => {
    if (exited) {
      return;
    }
    exited = true;
    if (deadlineTimer) {
      clearTimeout(deadlineTimer);
    }
    const started = Date.now();
    const deadline = started + config.shutdown.flushTimeoutMs;
    const settleShareMs = flushSettleShare(config.shutdown.flushTimeoutMs);
    const leftovers = await admission.awaitIdle(Math.min(settleShareMs, remainingMs(deadline, Date.now())));
    const inFlight = admission.inFlightRequests();
    if (leftovers.spawned > 0) {
      metrics.record("axond.shutdown.abandoned_settlements", leftovers.spawned);
    }
    if (leftovers.spawned > 0 || inFlight > 0) {
      const unsettled: ShutdownLog = {
        msg: "shutdown",
        phase: "spend_unsettled",
        in_flight: inFlight,
        unsettled: leftovers.spawned,
        settlements_queued: leftovers.queued,
        settlements_executing: leftovers.executing,
        settlements_reserved: leftovers.reserved,
        oldest_settlement_ms: leftovers.oldestAgeMs,
        settle_share_ms: settleShareMs,
      };
      writeLog(unsettled);
    }
    const usageFlushed = await usageDelivery.flush(remainingMs(deadline, Date.now()));
    const telemetryFlushed = await background.drain(remainingMs(deadline, Date.now()));
    if (!telemetryFlushed) {
      writeLog({ msg: "telemetry_flush", outcome: "timeout" });
    }
    writeLog({
      msg: "shutdown",
      phase: "stopped",
      usage_flushed: usageFlushed,
      telemetry_flushed: telemetryFlushed,
    });
    process.exit(0);
  };
  const closeAdmission = () => {
    if (phase === "closing") {
      return;
    }
    phase = "closing";
    admitting = false;
    metrics.set("axond.shutdown.phase", 2, { "axond.lifecycle_phase": "closing" });
    const admissionClosed: ShutdownLog = {
      msg: "shutdown",
      phase: "admission_closed",
      deadline_ms: config.shutdown.deadlineMs,
      in_flight: admission.inFlightRequests(),
    };
    writeLog(admissionClosed);
    deadlineTimer = setTimeout(() => {
      const stuck = admission.inFlightRequests();
      const expired: ShutdownLog = {
        msg: "shutdown",
        phase: "deadline_expired",
        deadline_ms: config.shutdown.deadlineMs,
        in_flight: stuck,
      };
      writeLog(expired);
      if (stuck > 0) {
        metrics.record("axond.shutdown.abandoned_requests", stuck);
      }
      server.closeAllConnections();
      void finish();
    }, config.shutdown.deadlineMs);
    deadlineTimer.unref();
    server.close(() => {
      void finish();
    });
    server.closeIdleConnections();
  };
  const shutdown = (signal: string) => {
    if (phase === "closing") {
      const ignored: ShutdownLog = {
        msg: "shutdown",
        phase: "signal_ignored",
        signal,
      };
      writeLog(ignored);
      return;
    }
    if (phase === "draining") {
      const second: ShutdownLog = {
        msg: "shutdown",
        phase: "second_signal",
        signal,
      };
      writeLog(second);
      closeAdmission();
      return;
    }
    phase = "draining";
    metrics.set("axond.shutdown.phase", 1, { "axond.lifecycle_phase": "draining" });
    const requested: ShutdownLog = {
      msg: "shutdown",
      phase: "requested",
      signal,
      drain_grace_ms: config.shutdown.drainGraceMs,
      deadline_ms: config.shutdown.deadlineMs,
      in_flight: admission.inFlightRequests(),
    };
    writeLog(requested);
    stopDiscovery();
    serving = false;
    if (config.shutdown.drainGraceMs === 0) {
      closeAdmission();
      return;
    }
    const grace = setTimeout(closeAdmission, config.shutdown.drainGraceMs);
    grace.unref();
  };
  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);
}

async function readGatewayKeyFile(path: string): Promise<string> {
  const bytes = await readFile(path);
  if (bytes.length === 0) {
    return "";
  }
  try {
    const info = await stat(path);
    if ((info.mode & 0o077) !== 0) {
      const line: KeyMaterialLog = { msg: "key_material", path };
      process.stdout.write(`${JSON.stringify(line)}\n`);
    }
  } catch {
    // Metadata is advisory. The bytes are already in hand.
  }
  try {
    return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  } catch {
    const invalid = new Error("gateway key file is not valid UTF-8");
    (invalid as { code: string }).code = "INVALID_UTF8";
    throw invalid;
  }
}

function splitBind(bind: string): [string, string] {
  const index = bind.lastIndexOf(":");
  return [bind.slice(0, index), bind.slice(index + 1)];
}

/** Node's fetch honors an undici agent. Bun's fetch ignores it, so the binary does not set one. */
function connectDispatcher(connectTimeoutMs: number): object | undefined {
  if (process.versions.bun) {
    return undefined;
  }
  return new Agent({
    connectTimeout: connectTimeoutMs,
    connect: { autoSelectFamily: false },
  });
}

function assertMigrationPrefix(name: string, sql: string): void {
  const prefix = `axond_ext_${name}_`;
  for (const match of sql.matchAll(/create\s+table\s+(?:if\s+not\s+exists\s+)?([a-zA-Z0-9_]+)/gi)) {
    const table = match[1]!;
    if (!table.startsWith(prefix)) {
      throw new Error(`extension ${name} migration creates \`${table}\` outside \`${prefix}\``);
    }
  }
}

function requirePostgresDsn(storage: { dsn?: string; dsnEnv?: string }): string {
  const dsn = storage.dsn;
  if (dsn === undefined || dsn.length === 0) {
    throw new Error(`store: store unavailable: env \`${storage.dsnEnv ?? ""}\` is unset or empty`);
  }
  return dsn;
}

async function openPostgres(dsn: string, metrics: Parameters<typeof createPostgresStore>[1]) {
  const setup = new Client({ connectionString: dsn });
  await setup.connect();
  await setup.query(POSTGRES_SCHEMA);
  await setup.end();
  return createPostgresStore(async () => {
    const client = new Client({ connectionString: dsn });
    await client.connect();
    return {
      client: {
        query: async (sql, params) => {
          const result = await client.query(sql, params ? [...params] : []);
          return { rows: result.rows as Record<string, unknown>[], rowCount: result.rowCount };
        },
      },
      release: () => client.end(),
    };
  }, metrics);
}

async function loadExtensionDir(dir: string | null): Promise<AxondExtension[]> {
  if (!dir) {
    return [];
  }
  const { readdir } = await import("node:fs/promises");
  const { join } = await import("node:path");
  const files = (await readdir(dir)).filter((file) => file.endsWith(".ts") || file.endsWith(".js") || file.endsWith(".mjs")).sort();
  const extensions: AxondExtension[] = [];
  for (const file of files) {
    const loaded = (await import(pathToFileURL(join(dir, file)).href)) as { default?: AxondExtension };
    const extension = loaded.default;
    if (!extension || extension.apiVersion !== 1) {
      throw new Error(`extension ${file} apiVersion ${String(extension?.apiVersion)} is not supported (want 1)`);
    }
    extensions.push(extension);
  }
  return extensions;
}

async function loadOperatorConfig(configPath: string) {
  let toml = "";
  let located: string | null = null;
  try {
    located = await locateConfigFile(configPath);
    if (located) {
      toml = await readFile(located, "utf8");
    }
  } catch (error) {
    const detail = error instanceof Error ? error.message : "unreadable";
    throw new Error(`failed to load config from \`${configPath}\`: config load: ${detail}`);
  }
  try {
    return await loadConfig(toml, envSecretReader(process.env, readGatewayKeyFile), {
      resolveSecrets: false,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : "invalid config";
    if (message.startsWith("config: ")) {
      let detail = message.slice("config: ".length);
      if (located && wantsFigmentLocation(detail)) {
        detail += ` in ${figmentFileSource(located)} TOML file`;
      }
      throw new Error(`failed to load config from \`${configPath}\`: config load: ${detail}`);
    }
    throw new Error(`failed to load config from \`${configPath}\`: invalid config: ${message}`);
  }
}

main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : "boot failed";
  process.stderr.write(`Error: ${message}\n`);
  process.exit(1);
});
