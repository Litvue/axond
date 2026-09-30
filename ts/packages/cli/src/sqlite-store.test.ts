import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { GatewayFailure, StoreFailure } from "../../gateway/src/errors.ts";
import { createMetrics } from "../../gateway/src/metrics.ts";
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

test("sqlite usage summary groups in byte order and saturates at the signed cap", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axond-sqlite-"));
  const path = join(directory, "axond.sqlite");
  try {
    const store = openSqliteStore(path);
    await store.putNamespace({
      id: "wsp_usage",
      attrs: {},
      blocklist: null,
      allowPlatformFallback: true,
      fromConfig: false,
    });
    const settle = (requestId: string, model: string, status: string, cost: bigint | null, period = "p") =>
      store.settle({
        requestId,
        namespace: "wsp_usage",
        period,
        model,
        status,
        cost,
        incarnation: 1n,
      });
    const half = 9223372036854775807n / 2n + 1n;
    await settle("r1", "b/m", "ok", 15n);
    await settle("r2", "b/m", "upstream_error", 1n);
    await settle("r3", "a/m", "ok", null);
    await settle("r4", "a/m", "ok", 7n);
    await settle("r1", "b/m", "ok", 99n);
    await settle("r5", "😀", "ok", 1n);
    await settle("r6", "\uFFFF", "ok", 2n);
    await settle("r7", "c/m", "ok", half);
    await settle("r8", "c/m", "ok", half);
    await settle("r9", "d/m", "ok", 9223372036854775807n + 1n);
    const summary = await store.summarizeUsage("wsp_usage", "p");
    assert.deepEqual(
      summary.map((row) => [row.model, row.status, row.count, row.cost_microdollars]),
      [
        ["a/m", "ok", 2, 7],
        ["b/m", "ok", 1, 15],
        ["b/m", "upstream_error", 1, 1],
        ["c/m", "ok", 2, "9223372036854775807"],
        ["d/m", "ok", 1, "9223372036854775807"],
        ["\uFFFF", "ok", 1, 2],
        ["😀", "ok", 1, 1],
      ],
    );
    const stored = await store.query(
      "SELECT CAST(cost_microdollars AS TEXT) AS cost FROM axond_store_usage WHERE request_id = ?",
      ["r9"],
    );
    assert.equal(String(stored.rows[0]?.["cost"]), "9223372036854775807");
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("sqlite hides a driver error behind store failure", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axond-sqlite-"));
  const path = join(directory, "axond.sqlite");
  try {
    const store = openSqliteStore(path);
    await assert.rejects(() => store.query("NOT SQL"), (error: unknown) => {
      assert.ok(error instanceof StoreFailure);
      assert.equal(error.message.includes("syntax"), false);
      return true;
    });
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("sqlite provider models keep the last payload and reject a different fresh source", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axond-sqlite-"));
  const path = join(directory, "axond.sqlite");
  try {
    const store = openSqliteStore(path);
    await store.upsertProviderModels({
      provider: "openai",
      fetchedAt: "2026-09-29T00:00:00Z",
      stale: false,
      data: [{ id: "gpt-test" }],
      source: "https://api.openai.com/v1",
    });
    await store.upsertProviderModels({
      provider: "openai",
      fetchedAt: "2026-09-29T00:01:00Z",
      stale: false,
      data: [{ id: "other" }],
      source: "https://example.invalid/v1",
    });
    const kept = await store.getProviderModels("openai");
    assert.deepEqual(kept?.data, [{ id: "gpt-test" }]);
    await store.markProviderModelsStale("openai");
    const stale = await store.getProviderModels("openai");
    assert.equal(stale?.stale, true);
    assert.deepEqual(stale?.data, [{ id: "gpt-test" }]);
    await store.upsertProviderModels({
      provider: "openai",
      fetchedAt: "2026-09-29T00:02:00Z",
      stale: false,
      data: [{ id: "other" }],
      source: "https://example.invalid/v1",
    });
    const replaced = await store.getProviderModels("openai");
    assert.equal(replaced?.stale, false);
    assert.deepEqual(replaced?.data, [{ id: "other" }]);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("sqlite store operations record wait and query duration without the namespace", async () => {
  const directory = await mkdtemp(join(tmpdir(), "axond-sqlite-"));
  const path = join(directory, "axond.sqlite");
  try {
    const metrics = createMetrics();
    const store = openSqliteStore(path, metrics);
    await store.putNamespace({
      id: "sk-live-secret",
      attrs: {},
      blocklist: null,
      allowPlatformFallback: false,
      fromConfig: true,
    });
    await store.settle({
      requestId: "req-1",
      namespace: "sk-live-secret",
      period: "compat",
      model: "fake/gpt",
      status: "ok",
      cost: null,
      incarnation: 1n,
    });
    await assert.rejects(
      () => store.getBudget("missing", "compat"),
      (error: unknown) => error instanceof GatewayFailure,
    );
    const operations = metrics.points.filter((point) => point.name === "axond.store.operations");
    const write = operations.find((point) => point.attributes["axond.store.operation"] === "namespace_write");
    const charge = operations.find((point) => point.attributes["axond.store.operation"] === "budget_charge");
    const read = operations.find((point) => point.attributes["axond.store.operation"] === "budget_read");
    assert.equal(write?.value, 1);
    assert.equal(write?.attributes["axond.store.backend"], "sqlite");
    assert.equal(write?.attributes["axond.store.outcome"], "ok");
    assert.equal(charge?.value, 1);
    assert.equal(charge?.attributes["axond.store.outcome"], "ok");
    assert.equal(read?.value, 1);
    assert.equal(read?.attributes["axond.store.outcome"], "error");
    for (const operation of ["namespace_write", "budget_charge", "budget_read"]) {
      const wait = metrics.points.find(
        (point) => point.name === "axond.store.acquire_wait" && point.attributes["axond.store.operation"] === operation,
      );
      const duration = metrics.points.find(
        (point) => point.name === "axond.store.query_duration" && point.attributes["axond.store.operation"] === operation,
      );
      assert.ok(wait && wait.value >= 0);
      assert.ok(duration && duration.value >= 0);
    }
    assert.equal(
      metrics.points.some((point) => point.name === "axond.store.connections_opened"),
      false,
    );
    assert.equal(JSON.stringify(metrics.points).includes("sk-live-secret"), false);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
