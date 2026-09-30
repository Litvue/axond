import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
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
  resolveTelemetry,
  usageEvent,
} from "../../gateway/src/index.ts";
import type { AxondExtension, ShutdownLog } from "@axond/sdk";

import { discoverOnce, startDiscovery } from "./discovery.ts";
import { createPostgresStore, POSTGRES_SCHEMA } from "./postgres-store.ts";
import { openSqliteStore } from "./sqlite-store.ts";

const { Client } = pg;

async function main(): Promise<void> {
  const configPath = process.env["AXOND_CONFIG"] ?? "axond.toml";
  const toml = await readFile(configPath, "utf8");
  const config = await loadConfig(
    toml,
    envSecretReader(process.env, async (path) => readFile(path, "utf8")),
  );
  const metrics = createMetrics(typeof config.gatewayKey === "string" ? [config.gatewayKey] : []);
  const store =
    config.storage.backend === "sqlite"
      ? openSqliteStore(config.storage.path!, metrics)
      : await openPostgres(config.storage.dsn!, metrics);
  for (const namespace of config.namespaces) {
    await store.putNamespace({
      id: namespace.id,
      attrs: {},
      blocklist: null,
      allowPlatformFallback: namespace.allowPlatformFallback,
      fromConfig: true,
    });
  }
  const extensions = await loadExtensionDir(config.extensionsDir ?? process.env["AXOND_EXTENSIONS_DIR"] ?? null);
  for (const extension of extensions) {
    for (const [index, sql] of (extension.migrations ?? []).entries()) {
      if (config.storage.backend === "sqlite") {
        const { applyMigration } = await import("./sqlite-store.ts");
        applyMigration(config.storage.path!, `${extension.name}:${index}`, sql);
      }
    }
  }
  const telemetry = resolveTelemetry({
    endpoint: process.env["OTEL_EXPORTER_OTLP_ENDPOINT"],
    protocol: process.env["OTEL_EXPORTER_OTLP_PROTOCOL"],
    instanceId: process.env["AXOND_INSTANCE_ID"],
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
    onLog: (record) => {
      process.stdout.write(`${JSON.stringify(record)}\n`);
    },
    onUsage: (record) => {
      process.stdout.write(`${JSON.stringify(usageEvent(record))}\n`);
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
    metrics,
    onLog: writeLog,
  });
  const stopDiscovery = startDiscovery({
    store,
    providers: config.providers,
    credentials: config.credentials,
    catalog: config.catalog,
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
    const leftovers = await admission.awaitIdle(Math.floor(config.shutdown.flushTimeoutMs / 2));
    if (leftovers.spawned > 0) {
      metrics.record("axond.shutdown.abandoned_settlements", leftovers.spawned);
    }
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

main().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : "boot failed";
  process.stderr.write(`${message}\n`);
  process.exit(1);
});
