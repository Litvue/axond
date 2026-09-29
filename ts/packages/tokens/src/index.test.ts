import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { createAxond } from "../../gateway/src/index.ts";
import { applyMigration, openSqliteStore } from "../../cli/src/sqlite-store.ts";

import { signToken, tokensExtension } from "./index.ts";

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
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});
