import { afterEach } from "node:test";
import assert from "node:assert/strict";

import { createServer } from "node:http";

import { once } from "node:events";

import test from "node:test";

import { Hono } from "hono";


import { createAxond } from "./app.ts";

import { StoreFailure } from "./errors.ts";

import { forceChatIncludeUsage, rewriteTopLevelModel } from "./body.ts";

import { loadConfig, envSecretReader } from "./config.ts";

import { createMemoryStore } from "./memory-store.ts";

import { createMetrics } from "./metrics.ts";

import { scopeStore } from "./scoped-store.ts";

import { costMicrodollars } from "./pricing.ts";

import type { Store } from "@axond/sdk";


const ownedServers = new Set<import("node:http").Server>();
afterEach(async () => {
  const servers = [...ownedServers]; ownedServers.clear();
  await Promise.all(servers.map((server) => new Promise<void>((resolve) => {
    server.closeAllConnections(); server.close(() => resolve());
  })));
});

export const KEY = "test-inbound-key";


export async function gateway(
  metrics?: ReturnType<typeof createMetrics>,
  telemetry?: { endpoint: string; instanceId?: string },
  responseBody?: string,
  onLog?: (record: {
    msg: "request";
    status?: string;
    input_tokens?: string;
    [key: string]: unknown;
  }) => void,
) {
  const store = createMemoryStore();
  await store.putNamespace({
    id: "platform",
    attrs: {},
    blocklist: null,
    allowPlatformFallback: false,
    fromConfig: true,
  });
  await store.putNamespace({
    id: "tenant",
    attrs: {},
    blocklist: null,
    allowPlatformFallback: false,
    fromConfig: true,
  });
  await store.putBudget("platform", "compat", 1_000_000_000_000n);
  await store.putBudget("tenant", "compat", 1_000_000_000_000n);
  const upstream = await listenUpstream(responseBody);
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    configNamespaces: ["platform", "tenant"],
    providers: [
      { id: "fake-openai", kind: "openai", baseUrl: upstream.url },
      { id: "fake-anthropic", kind: "anthropic", baseUrl: upstream.url },
    ],
    credentials: [
      { namespace: "platform", provider: "fake-openai", secret: "upstream-openai", id: "openai" },
      { namespace: "platform", provider: "fake-anthropic", secret: "upstream-anthropic", id: "anthropic" },
    ],
    prices: [
      {
        provider: "fake-openai",
        model: "*",
        inputMicrodollarsPerMillion: 2_500_000n,
        outputMicrodollarsPerMillion: 10_000_000n,
      },
      {
        provider: "fake-anthropic",
        model: "*",
        inputMicrodollarsPerMillion: 2_500_000n,
        outputMicrodollarsPerMillion: 10_000_000n,
      },
    ],
    rawPath: (c) => c.req.header("x-axond-raw-path") ?? new URL(c.req.url).pathname,
    metrics,
    telemetry,
    onLog,
  });
  return { app, store, upstream };
}


export async function listenUpstream(responseBody?: string): Promise<{
  url: string;
  requests: { path: string; authorization: string; body: string; traceparent: string }[];
  close: () => void;
}> {
  const requests: { path: string; authorization: string; body: string; traceparent: string }[] = [];
  const server = createServer(async (req, res) => {
    const chunks: Buffer[] = [];
    for await (const chunk of req) {
      chunks.push(chunk as Buffer);
    }
    const body = Buffer.concat(chunks).toString("utf8");
    requests.push({
      path: req.url ?? "",
      authorization: req.headers.authorization ?? req.headers["x-api-key"]?.toString() ?? "",
      body,
      traceparent: req.headers.traceparent?.toString() ?? "",
    });
    const payload = {
      id: "chatcmpl-test",
      choices: [{ message: { role: "assistant", content: "The capital of France is Paris." } }],
      usage: { prompt_tokens: 12, completion_tokens: 7 },
    };
    res.writeHead(200, { "content-type": "application/json" });
    res.end(responseBody ?? JSON.stringify(payload));
  });
  ownedServers.add(server);
  server.listen(0, "127.0.0.1");
  await once(server, "listening");
  const address = server.address();
  if (!address || typeof address === "string") {
    throw new Error("no port");
  }
  return {
    url: `http://127.0.0.1:${address.port}`,
    requests,
    close: () => { ownedServers.delete(server); server.closeAllConnections(); server.close(); },
  };
}


