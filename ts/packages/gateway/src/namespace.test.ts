import assert from "node:assert/strict";
import test from "node:test";

import { createAxond } from "./app.ts";
import { createMemoryStore } from "./memory-store.ts";
import { monthlyPeriod, validateTimezone } from "./namespace.ts";

const KEY = "test-inbound-key";

test("monthly_period_matches_the_bundled_tzdb", async () => {
  const boundaries: readonly (readonly [string, string, string])[] = [
    ["2026-09-30T23:30:00Z", "UTC", "2026-09"],
    ["2026-09-30T23:30:00Z", "Pacific/Auckland", "2026-10"],
    ["2026-10-01T03:00:00Z", "UTC", "2026-10"],
    ["2026-10-01T03:00:00Z", "America/Los_Angeles", "2026-09"],
    ["2026-12-31T23:59:59Z", "UTC", "2026-12"],
    ["2027-01-01T00:00:00Z", "UTC", "2027-01"],
    ["2026-09-30T23:00:00Z", "Africa/Casablanca", "2026-09"],
    ["2026-09-30T23:30:00Z", "Africa/El_Aaiun", "2026-09"],
    ["2027-01-01T00:00:00Z", "America/Coyhaique", "2026-12"],
    ["2026-09-30T23:30:00Z", "Factory", "2026-09"],
    ["2026-09-30T23:30:00Z", "Etc/Unknown", "2026-09"],
  ];
  for (const [iso, zone, want] of boundaries) {
    assert.equal(monthlyPeriod(Date.parse(iso), zone), want, `${iso} ${zone}`);
  }
  assert.equal(
    monthlyPeriod(Date.parse("2026-03-08T07:00:00Z"), "america/new_york"),
    monthlyPeriod(Date.parse("2026-03-08T07:00:00Z"), "America/New_York"),
  );
  for (const zone of ["PST", "SystemV/EST5", "Mars/Olympus", "local"]) {
    assert.throws(() => validateTimezone(zone), { message: `unknown timezone \`${zone}\`` });
  }

  const instant = Date.parse("2026-09-30T23:00:00Z");
  const store = createMemoryStore();
  const app = createAxond({
    store,
    providers: [],
    gatewayKey: KEY,
    defaultNamespace: "platform",
    clock: () => instant,
  });
  const headers = { authorization: `Bearer ${KEY}`, "content-type": "application/json" };
  const created = await app.request("http://127.0.0.1/api/v1/namespaces", {
    method: "POST",
    headers,
    body: JSON.stringify({ id: "wsp_tz" }),
  });
  assert.equal(created.status, 201);
  const refused = await app.request("http://127.0.0.1/api/v1/namespaces/wsp_tz/budget", {
    method: "PUT",
    headers,
    body: JSON.stringify({ cadence: "monthly", limit_microdollars: 5, timezone: "PST" }),
  });
  assert.equal(refused.status, 400);
  assert.deepEqual(await refused.json(), {
    error: { type: "bad_request", message: "bad request: unknown timezone `PST`" },
  });
  const policy = await app.request("http://127.0.0.1/api/v1/namespaces/wsp_tz/budget", {
    method: "PUT",
    headers,
    body: JSON.stringify({ cadence: "monthly", limit_microdollars: 5, timezone: "Africa/Casablanca" }),
  });
  assert.equal(policy.status, 200);
  const body = await policy.json();
  assert.equal(body.period, "2026-09");
  assert.equal(body.timezone, "Africa/Casablanca");
  const coyhaique = await app.request("http://127.0.0.1/api/v1/namespaces/wsp_tz/budget", {
    method: "PUT",
    headers,
    body: JSON.stringify({ cadence: "monthly", limit_microdollars: 5, timezone: "America/Coyhaique" }),
  });
  assert.equal(coyhaique.status, 200);
  assert.equal((await coyhaique.json()).timezone, "America/Coyhaique");
});
