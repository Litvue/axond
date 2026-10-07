import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { postgresClientOptions } from "./postgres-store.ts";


test("postgres clients use the connect and statement limits", async () => {
  const options = postgresClientOptions("postgres://axond:socket-secret@127.0.0.1:1/axond");
  assert.equal(options.connectionTimeoutMillis, 15_000);
  assert.equal(options.query_timeout, 60_000);
  assert.equal(options.connectionString.includes("socket-secret"), true);
  const main = await readFile(new URL("./main.ts", import.meta.url), "utf8");
  assert.match(main, /new Client\(postgresClientOptions\(dsn\)\)/);
  assert.match(main, /queryPgClient\(/);
  assert.match(main, /closePgClient\(/);
  const migration = await readFile(new URL("./postgres-store.ts", import.meta.url), "utf8");
  assert.match(migration, /new pg\.Client\(postgresClientOptions\(dsn\)\)/);
});
