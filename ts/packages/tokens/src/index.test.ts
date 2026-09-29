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
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});
