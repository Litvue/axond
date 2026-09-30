# TypeScript gateway

The supported operator artifact is still the Rust binary described in the
[README](../README.md). This page is the TypeScript port tracked by
[ADR 0066](./adr/0066-typescript-hono-extension-contract.md).

## Run the Node process

```bash
cd ts
npm ci
export AXOND_CONFIG=../axond.toml
export GW_INBOUND_KEY=...
./bin/axond
```

`axond.toml` is the same file the Rust binary reads. Withdrawn sections still
fail the boot. Point `AXOND_EXTENSIONS_DIR`, or `[extensions] dir`, at a
directory of `.ts`, `.js`, or `.mjs` files. Each file's default export is an
extension with `apiVersion: 1`. Restart the process to pick up a new file; do
not rebuild.

A compiled `bun build --compile` binary loads those files, but it does not
resolve packages from the extension's `node_modules`. Bundle an extension that
imports a package first (`bun build extension.ts --outfile extension.js`) and
point the directory at the bundle. The Node wrapper resolves packages from the
extension file. `ts/scripts/sign-artifact.sh` signs that binary with cosign
2.5.2 and verifies the signature. Pull requests use an ephemeral key that is
not uploaded to the transparency log. A `v*` tag signs keyless with GitHub
OIDC.

## Mount it in a Hono app

`createAxond` returns a Hono app. A host keeps its own routes and mounts the
gateway at `/`. A `pre-auth` extension may return a `Response` and stop the
pipeline. `npm run check:docs` runs this sample.

```ts
import { Hono } from "hono";
import { createAxond, createMemoryStore } from "@axond/gateway";

const gateway = createAxond({
  store: createMemoryStore(),
  gatewayKey: "local-key",
  extensions: [
    {
      name: "banner",
      apiVersion: 1,
      stage: "pre-auth",
      async middleware() {
        return new Response("extended");
      },
    },
  ],
});
const app = new Hono();
app.get("/host", (c) => c.text("host"));
app.route("/", gateway);

const health = await app.request("http://127.0.0.1/healthz");
if (health.status !== 200 || (await health.text()) !== "ok") {
  throw new Error(`health ${health.status}`);
}
const host = await app.request("http://127.0.0.1/host");
if (host.status !== 200 || (await host.text()) !== "host") {
  throw new Error(`host ${host.status}`);
}
const extended = await app.request("http://127.0.0.1/api/v1/namespaces");
if ((await extended.text()) !== "extended") {
  throw new Error("extension did not run");
}
```

## Prove it

```bash
ops/typescript-compat.sh
```

That runs the workspace tests, `tests/compat`, and `tests/compat-ts` with
`AXOND_BIN` pointed at `ts/bin/axond`.

## Extensions

| Package | Stage | What it does |
| --- | --- | --- |
| `@axond/rate-limit` | pre-dispatch | `isolate` counts in this process. `store` counts in `axond_ext_ratelimit_window`. Isolate mode can over-admit across replicas. |
| `@axond/redact` | pre-dispatch | Rewrites request JSON and SSE frames. Patterns reject nested quantifiers. |
| `@axond/tokens` | pre-auth and post-auth | Verifies `axt1.` HMAC tokens, checks revocation and epoch, and mints on `POST /api/v1/tokens` with the static key. |

Trusted extensions may `query` the process store. Untrusted ones are scoped to
the request namespace. Tables must be named `axond_ext_<name>_...`.

An extension records its own series on `c.var.axond.metrics`. `record` adds to
a counter and `set` stores a gauge. The name must start with `axond.ext.`.
A catalogue name is refused. An attribute value that contains a configured
secret is dropped, and so is a metric name that contains one. A new series
past the process ceiling of 200 is not stored. `npm run check:docs` runs the
sample below.

```ts
import { createAxond, createMemoryStore, createMetrics } from "@axond/gateway";

const metrics = createMetrics(["local-key"]);
const gateway = createAxond({
  store: createMemoryStore(),
  gatewayKey: "local-key",
  metrics,
  extensions: [
    {
      name: "counter",
      apiVersion: 1,
      stage: "pre-auth",
      async middleware(c) {
        c.var.axond.metrics.record("axond.ext.counter.hits", 1, { route: "list" });
        let refused = false;
        try {
          c.var.axond.metrics.record("axond.request.count", 1);
        } catch {
          refused = true;
        }
        if (!refused) {
          throw new Error("catalogue metric was accepted");
        }
        return new Response("counted");
      },
    },
  ],
});
const response = await gateway.request("http://127.0.0.1/api/v1/namespaces");
if ((await response.text()) !== "counted") {
  throw new Error("extension did not run");
}
const point = metrics.points.find((item) => item.name === "axond.ext.counter.hits");
if (!point || point.value !== 1 || point.attributes.route !== "list") {
  throw new Error("metric missing");
}
```

`transformSseEvents` from `@axond/sdk` buffers until an event delimiter
(`\n\n` or `\r\n\r\n`). The callback sees one complete event. Returning the
same object writes the original bytes, including comments and a CRLF
delimiter. Returning a new object re-encodes that frame. Returning `null`
drops it. An incomplete tail at the end of the stream is forwarded as it
arrived. `npm run check:docs` runs the sample below.

```ts
import { transformSseEvents } from "@axond/sdk";

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const parts = ["data: hel", "lo\n\ndata: next\n: keep\n\n"];
let index = 0;
const source = new ReadableStream<Uint8Array>({
  pull(controller) {
    if (index >= parts.length) {
      controller.close();
      return;
    }
    controller.enqueue(encoder.encode(parts[index]));
    index += 1;
  },
});
const seen: string[] = [];
const transformed = transformSseEvents(source, (event) => {
  seen.push(event.data);
  if (event.data === "hello") {
    return { ...event, data: "HELLO" };
  }
  return event;
});
const reader = transformed.getReader();
const received: Uint8Array[] = [];
for (;;) {
  const next = await reader.read();
  if (next.done) {
    break;
  }
  received.push(next.value);
}
const bytes = new Uint8Array(received.reduce((total, item) => total + item.length, 0));
let offset = 0;
for (const item of received) {
  bytes.set(item, offset);
  offset += item.length;
}
const text = decoder.decode(bytes);
if (text !== "data: HELLO\n\ndata: next\n: keep\n\n") {
  throw new Error(text);
}
if (seen.join(",") !== "hello,next") {
  throw new Error(seen.join(","));
}
```

## Container

`ts/Dockerfile` is the TypeScript image. The repository-root `Dockerfile` stays
the Rust release image. A local build (`axond-ts:local`, 235 MB) served
`GET /healthz` as `ok` and listed the configured `platform` namespace from
SQLite inside the container.

## Worker

`ts/packages/worker` is the Hyperdrive template. Set the Hyperdrive id, keep
caching disabled, and bundle the Worker with the extensions it imports. A
compiled Bun binary loads those same extensions from disk instead. A cron
trigger (`*/5 * * * *`) runs provider discovery and catalogue import off the
request path. A failed fetch marks the row stale and leaves the last payload. The process writes JSON `msg` `provider_discovery` with the provider id and a bounded reason, and that line omits the credential and the base URL.

`npm run test:workerd` boots that template under workerd. It reads
`CLOUDFLARE_HYPERDRIVE_LOCAL_CONNECTION_STRING_HYPERDRIVE` from
`AXOND_TEST_POSTGRES`, checks `GET /healthz`, and creates a namespace through
Postgres. That is Hyperdrive's local proxy against Postgres 16. PlanetScale
and a Cloudflare account are not part of this repository's test environment.

## Store

SQLite is the default. Postgres uses `dsn_env` and the schema in
`createPostgresStore`. Extension migrations apply once on either store:
a recorded id is skipped, and a failed statement names that id without the
driver text. The two extra namespace columns
`allow_platform_fallback` and `from_config` are applied by the TypeScript
process; they are not in the Rust `ops/postgres` scripts. Catalogue refusal
counts live in `axond_catalog_streak`. The Rust process does not read that
table.

Operators moving a process between the two binaries should follow the
[migration guide](./migration-typescript.md).
