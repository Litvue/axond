import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import pg from "pg";

import { createAxond } from "../../gateway/src/index.ts";
import { applyPostgresMigration, createPostgresStore, POSTGRES_SCHEMA } from "../../cli/src/postgres-store.ts";
import { applyMigration, openSqliteStore } from "../../cli/src/sqlite-store.ts";

import { heldTokenClaims, signToken, tokensExtension } from "./index.ts";

const KEY = "signing-key";

test("a minted token authorizes one namespace until it is revoked or its epoch moves", async () => {
  const dir = await mkdtemp(join(tmpdir(), "axond-tokens-"));
  const path = join(dir, "axond.sqlite");
  try {
    const store = openSqliteStore(path);
    const extensions = tokensExtension(KEY);
    for (const [index, sql] of extensions.flatMap((extension) => extension.migrations ?? []).entries()) {
      applyMigration(path, `tokens:${index}`, sql);
    }
    await store.putNamespace({
      id: "platform",
      attrs: {},
      blocklist: null,
      allowPlatformFallback: false,
      fromConfig: true,
    });
    const app = createAxond({
      store,
      gatewayKey: KEY,
      defaultNamespace: "platform",
      providers: [],
      extensions,
    });
    const token = await signToken(KEY, {
      sub: "ada",
      namespace: "platform",
      scope: ["models"],
      globs: ["*"],
      cap: null,
      exp: Math.floor(Date.now() / 1000) + 60,
      epoch: 1,
      jti: "jti-1",
    });
    const ok = await app.request("http://127.0.0.1/ns/platform/v1/models", {
      headers: { authorization: `Bearer ${token}` },
    });
    assert.equal(ok.status, 200);
    await store.query("INSERT INTO axond_ext_tokens_revocation (jti, namespace) VALUES (?, ?)", ["jti-1", "platform"]);
    const revoked = await app.request("http://127.0.0.1/ns/platform/v1/models", {
      headers: { authorization: `Bearer ${token}` },
    });
    assert.equal(revoked.status, 401);
    assert.equal((await revoked.json()).error.type, "token_revoked");

    const other = await signToken(KEY, {
      sub: "ada",
      namespace: "tenant",
      scope: ["models"],
      globs: ["*"],
      cap: null,
      exp: Math.floor(Date.now() / 1000) + 60,
      epoch: 1,
      jti: "jti-other",
    });
    const denied = await app.request("http://127.0.0.1/ns/platform/v1/models", {
      headers: { authorization: `Bearer ${other}` },
    });
    assert.equal(denied.status, 403);
    assert.equal((await denied.json()).error.type, "namespace_not_authorized");

    const narrow = await signToken(KEY, {
      sub: "ada",
      namespace: "platform",
      scope: ["chat"],
      globs: ["fake-openai/*"],
      cap: "1",
      exp: Math.floor(Date.now() / 1000) + 60,
      epoch: 1,
      jti: "jti-narrow",
    });
    const scope = await app.request("http://127.0.0.1/ns/platform/v1/models", {
      headers: { authorization: `Bearer ${narrow}` },
    });
    assert.equal(scope.status, 403);
    assert.equal((await scope.json()).error.type, "token_scope_insufficient");

    const expired = await signToken(KEY, {
      sub: "ada",
      namespace: "platform",
      scope: ["models"],
      globs: ["*"],
      cap: null,
      exp: Math.floor(Date.now() / 1000) - 10,
      epoch: 1,
      jti: "jti-expired",
    });
    const stale = await app.request("http://127.0.0.1/ns/platform/v1/models", {
      headers: { authorization: `Bearer ${expired}` },
    });
    assert.equal(stale.status, 401);
    assert.equal((await stale.json()).error.type, "token_expired");

    await store.query("INSERT INTO axond_ext_tokens_epoch (namespace, epoch) VALUES (?, ?)", ["platform", 4]);
    const oldEpoch = await signToken(KEY, {
      sub: "ada",
      namespace: "platform",
      scope: ["models"],
      globs: ["*"],
      cap: null,
      exp: Math.floor(Date.now() / 1000) + 60,
      epoch: 3,
      jti: "jti-epoch",
    });
    const epoch = await app.request("http://127.0.0.1/ns/platform/v1/models", {
      headers: { authorization: `Bearer ${oldEpoch}` },
    });
    assert.equal(epoch.status, 401);
    assert.equal((await epoch.json()).error.type, "token_expired");
    assert.equal(heldTokenClaims(), 0);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("a minted token revokes on postgres and drops its claims", { skip: !process.env["AXOND_TEST_POSTGRES"] }, async () => {
  const dsn = process.env["AXOND_TEST_POSTGRES"] ?? "";
  const extensions = tokensExtension(KEY);
  const setup = new pg.Client({ connectionString: dsn });
  await setup.connect();
  try {
    await setup.query(POSTGRES_SCHEMA);
    await setup.query("DROP TABLE IF EXISTS axond_ext_tokens_revocation");
    await setup.query("DROP TABLE IF EXISTS axond_ext_tokens_epoch");
    await setup.query("DELETE FROM axond_schema_migrations WHERE id IN ('tokens:0', 'tokens:1')");
    await setup.query("DELETE FROM axond_namespace WHERE id = 'tok-platform'");
  } finally {
    await setup.end();
  }
  const migrations = extensions.flatMap((extension) => extension.migrations ?? []);
  for (const [index, sql] of migrations.entries()) {
    await applyPostgresMigration(dsn, `tokens:${index}`, sql);
  }
  const store = createPostgresStore(async () => {
    const client = new pg.Client({ connectionString: dsn });
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
  try {
    await store.putNamespace({
      id: "tok-platform",
      attrs: {},
      blocklist: null,
      allowPlatformFallback: false,
      fromConfig: true,
    });
    const app = createAxond({
      store,
      gatewayKey: KEY,
      defaultNamespace: "tok-platform",
      providers: [],
      extensions,
    });
    const token = await signToken(KEY, {
      sub: "ada",
      namespace: "tok-platform",
      scope: ["models"],
      globs: ["*"],
      cap: null,
      exp: Math.floor(Date.now() / 1000) + 60,
      epoch: 1,
      jti: "jti-pg",
    });
    const ok = await app.request("http://127.0.0.1/ns/tok-platform/v1/models", {
      headers: { authorization: `Bearer ${token}` },
    });
    assert.equal(ok.status, 200);
    assert.equal(heldTokenClaims(), 0);
    await store.query("INSERT INTO axond_ext_tokens_revocation (jti, namespace) VALUES (?, ?)", ["jti-pg", "tok-platform"]);
    const revoked = await app.request("http://127.0.0.1/ns/tok-platform/v1/models", {
      headers: { authorization: `Bearer ${token}` },
    });
    assert.equal(revoked.status, 401);
    assert.equal((await revoked.json()).error.type, "token_revoked");
    assert.equal(heldTokenClaims(), 0);
  } finally {
    const cleanup = new pg.Client({ connectionString: dsn });
    await cleanup.connect();
    await cleanup.query("DROP TABLE IF EXISTS axond_ext_tokens_revocation");
    await cleanup.query("DROP TABLE IF EXISTS axond_ext_tokens_epoch");
    await cleanup.query("DELETE FROM axond_namespace WHERE id = 'tok-platform'");
    await cleanup.end();
  }
});


test("signed malformed claims are rejected and valid inference grants cannot administer", async () => {
  const { verifyToken } = await import("./index.ts");
  for (const invalid of [{}, { exp: "tomorrow" }, { cap: "NaN" }]) {
    const token = await signToken(KEY, invalid as any);
    assert.equal(await verifyToken(KEY, token), null);
  }
  const { createMemoryStore } = await import("../../gateway/src/memory-store.ts");
  const store = createMemoryStore();
  const app = createAxond({ store, gatewayKey: "admin-key", defaultNamespace: "platform", providers: [], extensions: tokensExtension(KEY) });
  const token = await signToken(KEY, { sub: "user", namespace: "platform", scope: ["models"], globs: ["*"], cap: null, exp: Math.floor(Date.now()/1000)+60, epoch: 1, jti: "review" });
  const response = await app.request("http://localhost/api/v1/namespaces", { headers: { authorization: `Bearer ${token}` } });
  assert.equal(response.status, 403);
  assert.equal(heldTokenClaims(), 0);
  const mint = await app.request("http://localhost/api/v1/tokens", { method: "POST", headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" }, body: "{}" });
  assert.equal(mint.status, 401);
  assert.throws(() => tokensExtension(KEY, { mintKey: KEY }), /must differ/);
});

test("concurrent requests with one client request ID keep distinct token authority", async () => {
  const { createMemoryStore } = await import("../../gateway/src/memory-store.ts");
  const store = createMemoryStore(); store.query = async () => ({ rows: [] });
  for (const id of ["one", "two"]) await store.putNamespace({ id, attrs: {}, blocklist: null, allowPlatformFallback: false, fromConfig: false });
  const [verify, revoke] = tokensExtension(KEY);
  let entered = 0; let release!: () => void;
  const barrier = new Promise<void>((resolve) => { release = resolve; });
  const app = createAxond({ store, gatewayKey: "admin", defaultNamespace: "one", providers: [], extensions: [verify!, {
    name: "review-barrier", apiVersion: 1, stage: "pre-auth", trusted: true,
    async middleware(_c, next) { if (++entered === 2) release(); await barrier; await next(); },
  }, revoke!] });
  const requests = await Promise.all(["one", "two"].map(async (namespace) => {
    const token = await signToken(KEY, { sub: namespace, namespace, scope: ["models"], globs: ["*"], cap: null, exp: Math.floor(Date.now()/1000)+60, epoch: 1, jti: namespace });
    return app.request(`http://localhost/ns/${namespace}/v1/models`, { headers: { authorization: `Bearer ${token}`, "x-request-id": "shared" } });
  }));
  assert.deepEqual(requests.map((r) => r.status), [200, 200]); assert.equal(heldTokenClaims(), 0);
});
