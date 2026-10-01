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
is `config load:` with Figment's type sentence and the file label. A file
integer outside `i64` is the TOML parser diagram (`number too large to fit in
target type` or `number too small to fit in target type`) before that field
is checked. A Postgres
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
errors. A disabled catalogue ignores `source_url`. An enabled catalogue with
`store = "postgres"` and no `dsn_env` is
`` catalog `postgres`: `dsn_env` must name the env var holding the connection string ``
after the URL check, and the store file is not created. A plaintext URL is
still the `https://` sentence. `schema = "public.axond"` is
`` `catalog.schema` must be a single unqualified schema name: it names the search path, not a table ``.
A disabled catalogue does not check that store.

`[server] bind` is a socket address. `localhost:8080`, a bare port, or any
other value that is not an IP and port fails boot with Figment's
`invalid socket address syntax` line, and the store file is not created. A
number, bool, array, or table uses Figment's type sentence. `AXOND_SERVER__BIND`
overrides the file and names the `AXOND_` environment, including when the
value is numeric. An omitted bind stays `0.0.0.0:8080`. `[::1]:8080` is a
valid bind.

`AXOND_` overrides are Figment values, merged over the file. `AXOND_FAILOVER__MAX_ATTEMPTS=1.5` is `expected u32` at key `FAILOVER.MAX_ATTEMPTS` in the `AXOND_` environment, before a later price float, and the store file is not created. `=0` is `failover.max_attempts must be at least 1`. A file float on `admission.max_request_bytes` is still reported before that env float. `AXOND_CREDENTIAL__0__WEIGHT` is `expected a sequence` at `CREDENTIAL`. `AXOND_NAMESPACE=[1]` is `expected struct Namespace` at `NAMESPACE.0`. `AXOND_FAILOVER=[1.5]` is `expected u32` at `FAILOVER.0`, and `[0]` is `failover.max_attempts must be at least 1`. `AXOND_ADMISSION=[0,1.5]` is `expected usize` at `ADMISSION.1`. `AXOND_STORAGE=[1.5]` is `expected enum StorageBackend` at `STORAGE.0`. A price array cites `PRICE.0.PROVIDER` for a named field and `PRICE.0` for a flattened integer. `AXOND_SHUTDOWN__NOPE` is `unknown field` at `SHUTDOWN.NOPE`. Figment finishes each top-level key before the next, so `admission.max_request_bytes = 1.5` is reported before `failover = [1.5]`, and `AXOND_ADMISSION=[1.5]` is reported before that file array. `AXOND_NAMESPACE=[1]` is not, because failover is the earlier key. A later `AXOND_` value replaces an earlier one when the shapes differ. `AXOND_CREDENTIAL__0__WEIGHT` set before `AXOND_CREDENTIAL=[{weight=1.5}]` is `expected u32` at `CREDENTIAL.0.WEIGHT`; the nested map still wins when it is set second. `AXOND_FAILOVER=[4, 9]` then `AXOND_FAILOVER__MAX_ATTEMPTS=8` loads `max_attempts` 8 and the default overall timeout. A quoted env string decodes Figment escapes, so `AXOND_STORAGE__PATH="hi\u0041"` is the path `hiA`. An invalid escape (`\0`, `\q`) keeps the raw value. `AXOND_FAILOVER__OVERALL_TIMEOUT_MS=18446744073709551615` loads that u64. The only bound is `must be at least 1`. `AXOND_ADMISSION__MAX_IN_FLIGHT=2305843009213693952` is `admission.max_in_flight (2305843009213693952) must not exceed 2305843009213693951: a larger ceiling is not a bound this process can hold`, and the store file is not created. The same ceiling at `2305843009213693951` loads. A file integer outside `i64` is the TOML parser diagram before extract. `failover = { overall_timeout_ms = 9223372036854775808 }` is `number too large to fit in target type` before `[shutdown] nope`, and the store file is not created. An `AXOND_` value may still be a full `u64`. `AXOND_USAGE_SINK` with `max_batch=18446744073709551615` and `buffer_capacity=1` is `usage_sink `postgres`: max_batch (18446744073709551615) must not exceed buffer_capacity (1)`. A file `max_batch` of `9223372036854775807` with `buffer_capacity = 1` names that batch. A file `buffer_capacity` of `9223372036854775807` loads. A written `max_in_flight_per_tenant` above `max_in_flight` is `admission.max_in_flight_per_tenant (32) must not exceed admission.max_in_flight (16): a per-tenant ceiling above the global one cannot isolate a tenant`, and the store file is not created. `max_tenants = 0` while a tenant ceiling is set is `admission.max_tenants must be at least 1 when max_in_flight_per_tenant is set`. `max_in_flight = 16` alone turns the default tenant ceiling off and loads. A per-tenant integer above the semaphore limit is that isolation sentence, before the semaphore bound.

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
elements. `[storage] backend = "nope"` is Figment's `unknown variant` sentence for
`sqlite` or `postgres`, before a missing path. `on_unavailable = "nope"` is
the same sentence for `deny` or `allow`. `create_table = 1.5` is `expected a
boolean`. An unknown `[shutdown]` key is `unknown field` naming
`drain_grace_ms`, `deadline_ms`, and `flush_timeout_ms`. Figment walks table
keys in sorted order, so an admission float is reported before `[shutdown] nope`,
and `[shutdown] nope` is reported before a transport float and before
`admission.max_request_bytes = 0`. Inside `[shutdown]`, `aaa`
is reported before `drain_grace_ms = 1.5`, and `deadline_ms = 1.5` is reported
before `drain_grace_ms = 1.5`. Inside `[transport]`,
`buffered_body_timeout_ms = 1.5` is reported before `connect_timeout_ms = 1.5`.
A bad `server.bind` is reported before `[shutdown]` and `[storage]`.
`create_table = 1.5` is reported before `path = 1`. `path = 1` is `expected a
string`, and a bad `backend` is still reported first. `[catalog]
create_table = 1.5` is reported before `refresh_interval_seconds = 1.5`.
`bootstrap = "nope"` is reported before `source = "nope"`. `store = "nope"`
names `in-memory` or `postgres`. `source_url = 1` is `expected a string` when
`source = "none"`. A credential `namespace = 1` is reported before
`weight = 1.5`. A namespace `allow_platform_fallback = 1.5` is reported before
`id = 1`. An admission float is reported before that credential weight.
`[blocklist] models = [1]` is reported before `[catalog] create_table = 1.5`.
A price `input_microdollars_per_million = 1.5` is `expected u64` at
`default.price.0`. `[[provider]] base_url = 1` is reported before
`server.bind = 1`. `unpriced_models = "nope"` names `deny` or `allow`.
`[usage_journal] connect_timeout_ms = 1.5` is reported before
`create_schema = 1.5` when `backend = "none"`. `backend = "postgres"` is
`` `[usage_journal] backend = "postgres"` is not built (ADR 0049) `` after
storage, admission, credentials, and usage sinks pass. Sqlite `:memory:` with
that backend is `` `[storage]` sqlite `:memory:` is not durable; use a file path ``.
`admission.max_request_bytes = 0` with that backend is
`admission.max_request_bytes must be at least 1`, and the store file is not
created. `[[usage_sink]]
buffer_capacity = 1.5` is reported before `create_table = 1.5` on a stdout
sink. `[[gateway_key]]` without `namespace` is `missing field \`namespace\``
before `server.bind = 1`. A credential that names `provider` and omits
`namespace` is that sentence before a price float. A `[[price]]` row with
`provider` and `model` but no `input_microdollars_per_million` is
`missing field \`input_microdollars_per_million\`` at `default.price.0`.
`output_microdollars_per_million` is required next. `[[namespace]]` without
`id` is `missing field \`id\`` before a bad bind.
`failover = [0]` is `failover.max_attempts must be at least 1`
before a ghost credential. `failover = [1.5]` is `expected u32` at
`default.failover.0`. `storage = []` still requires a SQLite path.
`namespace = [[1]]` fills that element in declaration order and is
`expected a string` at `default.namespace.0.0`. `namespace = [[]]` is
`invalid length 0, expected struct Namespace with 3 elements`.
`gateway_key = [[]]` is `invalid length 2` because the two leading sources
are defaulted and `namespace` is not. `price = [[1]]` stays
`expected struct PriceRule`, because a flattened price cannot be filled from
a sequence. `AXOND_NAMESPACE=[[1]]` is the same string sentence at
`NAMESPACE.0.0` in the `AXOND_` environment. The store file is not created.
A TOML integer outside `i64` fails in the parser before any field is
deserialized. `number too large to fit in target type` and `number too small
to fit in target type` name the line and column, and they are reported before
`[shutdown] nope`. An `AXOND_` override is not a TOML integer and may still be
a full `u64`. An inline table stays on one line. A trailing comma is
`invalid inline table` and `` expected `}` `` with the caret on the comma,
before extract and before a later integer. A newline or a `#` comment inside
the braces is that diagram at the newline or the `#`. An array may still end
with a comma. A basic string `\x` with two hex digits, or `\e`, is
`invalid escape sequence` with the caret on the next character, before
extract and before a later integer. A short `\u` or `\U`, or a code point
that is a surrogate or above `U+10FFFF`, is `invalid unicode 4-digit hex code`
or `invalid unicode 8-digit hex code` at that same character. A complete hex
sequence that is out of range also says `value is out of range`. `\u0041` and
`\U00000041` still decode. A literal string keeps the backslash. A decimal
integer that is only `0` stops there. A following digit, `_`, or a signed
`+0x` is `expected newline, `#`` at that character, before extract and before
a later integer. A local time `07:32:00` and a four-digit year `0123-01-01`
still parse. Inside an array the same spot is `invalid array` and
`` expected `]` ``. Inside an inline table it is the inline closer. `0`,
`0.5`, `0e1`, and `0x10` still parse. A decimal float that overflows to
positive infinity is `invalid floating-point number` at the start of the
number, before extract and before a later integer. `-1e309`, `1e308`,
`1e-400`, and `inf` still parse. A calendar day that month does not have is
`invalid date-time` and `value is out of range` on the day, before extract
and before a later integer. `2024-02-29`, `1900-02-28`, and a quoted
`2024-02-30` still parse.

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
