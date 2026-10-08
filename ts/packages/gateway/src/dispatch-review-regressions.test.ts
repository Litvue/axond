import assert from "node:assert/strict";
import test from "node:test";
import { planCredentialWalk, noteCredentialFailure, credentialState, type CredentialPool } from "./dispatch.ts";

test("cooled credentials all get a probe and fallback tenants share their owner's state", () => {
  const pools = new Map<string, CredentialPool>();
  const keys = ["a", "b"].map((id) => ({ id, namespace: "platform", provider: "p", secret: "fixture" }));
  const policy = { strategy: "round-robin" as const, failureThreshold: 1, cooldownMs: 100 };
  for (const key of keys) noteCredentialFailure(pools, key.namespace, key.provider, key.id, 0, 1);
  const walk = planCredentialWalk(keys, pools, "tenant", "p", "platform", false, 101, policy);
  assert.deepEqual(walk.attempts.map((key) => key.id).sort(), ["a", "b"]);
  assert.equal(credentialState(pools, keys[0]!, 102, 100), "parked");
  assert.equal(pools.has("tenant\0p"), false);
});
