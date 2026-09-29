import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { applyMigration, openSqliteStore } from "./sqlite-store.ts";

test("sqlite settlement is exactly once per request_id and survives reopen", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axond-sqlite-"));
  const path = join(directory, "axond.sqlite");
  try {
    const store = openSqliteStore(path);
    await store.putNamespace({
      id: "platform",
      attrs: { workspace: "litvue" },
      blocklist: null,
      allowPlatformFallback: false,
      fromConfig: true,
    });
    await store.putBudget("platform", "compat", 1_000_000n);
    const input = {
      requestId: "same",
      namespace: "platform",
      period: "compat",
      model: "fake/gpt",
      status: "ok",
      cost: 1000n,
      incarnation: 1n,
    };
    const results = await Promise.all(Array.from({ length: 10 }, () => store.settle(input)));
    assert.equal(results.filter((result) => result.charged).length, 1);
    const distinct = await Promise.all(Array.from({ length: 10 }, (_, index) => store.settle({ ...input, requestId: `n-${index}` })));
    assert.equal(distinct.filter((result) => result.charged).length, 10);
    const budget = await store.getBudget("platform", "compat");
    assert.equal(budget?.spent, 11_000n);
    const reopened = openSqliteStore(path);
    const again = await reopened.getBudget("platform", "compat");
    assert.equal(again?.spent, 11_000n);
    applyMigration(path, "ext", "CREATE TABLE IF NOT EXISTS axond_ext_demo_note (id TEXT PRIMARY KEY)");
    applyMigration(path, "ext", "CREATE TABLE IF NOT EXISTS axond_ext_demo_note (id TEXT PRIMARY KEY)");
    const rows = await reopened.query("SELECT id FROM axond_schema_migrations WHERE id = ?", ["ext"]);
    assert.equal(rows.rows.length, 1);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("delete then recreate bumps incarnation so a late settle does not charge", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axond-sqlite-"));
  const path = join(directory, "axond.sqlite");
  try {
    const store = openSqliteStore(path);
    await store.putNamespace({ id: "temp", attrs: {}, blocklist: null, allowPlatformFallback: true, fromConfig: false });
    await store.putBudget("temp", "p", 5000n);
    const resolved = await store.resolveNamespace("temp", Date.now());
    assert.equal(resolved?.admitted, true);
    await store.deleteNamespace("temp");
    await store.putNamespace({ id: "temp", attrs: {}, blocklist: null, allowPlatformFallback: true, fromConfig: false });
    await store.putBudget("temp", "p", 5000n);
    const late = await store.settle({
      requestId: "late",
      namespace: "temp",
      period: "p",
      model: "m",
      status: "ok",
      cost: 10n,
      incarnation: resolved!.incarnation,
    });
    assert.equal(late.charged, false);
    const budget = await store.getBudget("temp", "p");
    assert.equal(budget?.spent, 0n);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
