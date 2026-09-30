import assert from "node:assert/strict";
import test from "node:test";

import { createBackgroundDrain, remainingMs, settleShareMs } from "./shutdown-budget.ts";

test("shutdown_flush_budget_is_one_deadline", async () => {
  assert.equal(settleShareMs(400), 200);
  assert.equal(settleShareMs(5), 2);
  const started = 1_000;
  const deadline = started + 400;
  assert.equal(remainingMs(deadline, started), 400);
  assert.equal(remainingMs(deadline, started + 200), 200);
  assert.equal(remainingMs(deadline, started + 500), 0);

  const drain = createBackgroundDrain();
  assert.equal(await drain.drain(0), true);
  let release = () => {};
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  drain.track(gate);
  assert.equal(await drain.drain(20), false);
  release();
  assert.equal(await drain.drain(1_000), true);

  const secret = "https://collector.example/v1/traces";
  const failing = createBackgroundDrain();
  failing.track(Promise.reject(new Error(secret)));
  assert.equal(await failing.drain(1_000), true);
});
