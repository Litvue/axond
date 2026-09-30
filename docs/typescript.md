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

`./bin/axond --version` prints `axond` and the workspace version from
`Cargo.toml`. `--help` and `-h` print the same help text as the Rust binary.
Both finish before the config file is opened. Any other argument, including
`mint`, `keygen`, `revoke`, `check`, `migrate`, `admin`, and `budget`, exits 2
and does not boot.

`AXOND_CONFIG` defaults to `axond.toml`. A relative path is sought from the
working directory upward, the same walk Figment uses. A missing file or a
directory is an empty document: boot exits 1 with `Error: failed to load
config from \`…\`: invalid config: \`[storage]\` is required …`. A file that
is not TOML uses the `config load:` prefix. Other boot failures use the same
`Error:` line.

SQLite `:memory:` is refused, including with surrounding whitespace. A SQLite
file that sets `dsn_env`, or a Postgres file that sets `path`, fails boot with
the Rust sentence. `[storage.usage_index]` uses the same bounds: `buffer_capacity`
at least 1 and at most Tokio's permit limit, `max_batch` at least 1, at most
4096, and no greater than the buffer, and `flush_interval_ms` at most 24h.
`0` is a legal flush interval. A float, a negative, or a string in that table
is `config load:` with Figment's type sentence and the file label. An integer
above `u64` says `number too large to fit in target type`. A Postgres
`dsn_env` whose value is unset or empty is not an invalid config: boot exits 1
with `Error: store: store unavailable: env \`…\` is unset or empty`.

A credential or gateway key that names an unknown namespace or provider, a
duplicate credential id, a weight of 0, an empty `env`, or both `env` and
`file` on one gateway key is `invalid config` with the Rust sentence. That
check runs before the store opens. An unset credential env, or a gateway key
file that is missing, empty, a directory, or not UTF-8, is
`Error: config resolution failed: …` after the store is open. The file error
uses Rust's `entity not found`, `is a directory`, and `not valid UTF-8`
wording. A price row that names an unknown provider, and
`failover.failure_threshold = 0`, are reported before a bad credential.

An enabled `[catalog]` uses the Rust bounds. `source = "models-dev"` with no
`source_url` fetches `https://models.dev/catalog.json`. A plaintext URL, a URL
with embedded credentials, a hostless URL, and any document other than
`/catalog.json` fails boot with the Rust sentence, and the credential is not
echoed. `source = "seed"` rejects `source_url`. A zero refresh interval, a
timeout above the interval, and a retry ceiling above the interval fail boot.
`discovery.refresh_interval_seconds = 0` is reported before those catalogue
errors. A disabled catalogue ignores `source_url`.

`[server] bind` is a socket address. `localhost:8080`, a bare port, or any
other value that is not an IP and port fails boot with Figment's
`invalid socket address syntax` line, and the store file is not created. A
number, bool, array, or table uses Figment's type sentence. `AXOND_SERVER__BIND`
overrides the file and names the `AXOND_` environment, including when the
value is numeric. An omitted bind stays `0.0.0.0:8080`. `[::1]:8080` is a
valid bind.

`failover.max_attempts = 0` and `failover.overall_timeout_ms = 0` fail boot
with `must be at least 1` before a bad credential and before a catalogue URL.
A float is Figment's type sentence (`expected u32` or `expected u64`).
Admission, transport, and shutdown bounds are checked in that same pass.
`admission.max_request_bytes = 0` is reported before a zero
`transport.connect_timeout_ms` and before the catalogue. Transport zeros run
from `connect_timeout_ms` through `max_error_bytes`, so a zero connect timeout
is reported before a zero `max_response_bytes` and before an error body that
is larger than the response. A float,
string, or negative on an admission, catalogue, or discovery integer is
Figment's extract error, including a float on a disabled catalogue. That
extract error is reported before a zero failover bound. `server = "x"` is
`expected struct Server`, and `server = [1]` is a socket-address error at
`default.server.0`. An empty `server = []` keeps `0.0.0.0:8080`. A scalar in
place of a table is the same extract error: `admission = "x"` is
`expected struct AdmissionConfigWire` and is reported before a zero failover
bound. `[namespace]` (one pair of brackets) is `expected a sequence`. A table
written as an array fills fields in declaration order and drops extra
elements. `failover = [0]` is `failover.max_attempts must be at least 1`
before a ghost credential. `failover = [1.5]` is `expected u32` at
`default.failover.0`. `storage = []` still requires a SQLite path.

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
request path. The refresh walks the platform credential pool, or the lexicographically first tenant pool, and follows `has_more` for up to 20 pages. A failed fetch marks the row stale and leaves the last payload. The process writes JSON `msg` `provider_discovery` with the provider id and a bounded reason, and that line omits the credential and the base URL. A `3xx` from the provider or the catalogue is not followed, so the credential stays on the configured URL.

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
