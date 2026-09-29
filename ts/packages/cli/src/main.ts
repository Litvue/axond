import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import { pathToFileURL } from "node:url";

import { getRequestListener } from "@hono/node-server";
import pg from "pg";

import { createAxond, createMetrics, envSecretReader, loadConfig, resolveTelemetry } from "../../gateway/src/index.ts";
import type { AxondExtension } from "@axond/sdk";

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
  const store =
    config.storage.backend === "sqlite"
      ? openSqliteStore(config.storage.path!)
      : await openPostgres(config.storage.dsn!);
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
    extensions,
    rawPath: (c) => c.req.header("x-axond-raw-path") ?? new URL(c.req.url).pathname,
    serving: () => serving,
    onStoreUnavailable: config.storage.onUnavailable,
    admitting: () => admitting,
    metrics: createMetrics(typeof config.gatewayKey === "string" ? [config.gatewayKey] : []),
    telemetry: telemetry ?? undefined,
    onLog: (record) => {
      process.stdout.write(`${JSON.stringify(record)}\n`);
    },
    onUsage: (record) => {
      const line = {
        schema_version: record.schemaVersion,
        request_id: record.requestId,
        namespace: record.namespace,
        subject: record.subject,
        model: record.model,
        target_provider: record.targetProvider,
        target_model: record.targetModel,
        status: record.status,
        input_tokens: record.inputTokens.toString(),
        output_tokens: record.outputTokens.toString(),
        cost_microdollars: record.costMicrodollars?.toString() ?? null,
        catalog_version: record.catalogVersion,
        price_book: record.priceBook,
        price_book_checksum: record.priceBookChecksum,
        signer_kid: record.signerKid,
      };
      process.stdout.write(`${JSON.stringify(line)}\n`);
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
  void discoverOnce({
    store,
    providers: config.providers,
    credentials: config.credentials,
    catalog: config.catalog,
  });
  const stopDiscovery = startDiscovery({
    store,
    providers: config.providers,
    credentials: config.credentials,
    catalog: config.catalog,
    intervalSeconds: config.discoveryIntervalSeconds,
  });
  let phase: "serving" | "draining" | "closing" = "serving";
  const closeAdmission = () => {
    if (phase === "closing") {
      return;
    }
    phase = "closing";
    admitting = false;
    const deadline = setTimeout(() => {
      server.closeAllConnections();
      process.exit(0);
    }, config.shutdown.deadlineMs);
    deadline.unref();
    server.close(() => process.exit(0));
    server.closeIdleConnections();
  };
  const shutdown = () => {
    if (phase === "closing") {
      return;
    }
    if (phase === "draining") {
      closeAdmission();
      return;
    }
    phase = "draining";
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

async function openPostgres(dsn: string) {
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
  });
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
