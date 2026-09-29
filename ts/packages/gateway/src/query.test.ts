import assert from "node:assert/strict";
import test from "node:test";

import { createAxond } from "./app.ts";
import { GatewayFailure } from "./errors.ts";
import { createMemoryStore } from "./memory-store.ts";
import { parseCredentialQuery } from "./query.ts";

const KEY = "test-inbound-key";

test("parseCredentialQuery rejects a repeated namespaces key and bad encoding", () => {
  assert.equal(parseCredentialQuery(null), null);
  assert.equal(parseCredentialQuery(""), null);
  assert.equal(parseCredentialQuery("foo=bar"), null);
  assert.equal(parseCredentialQuery("&namespaces=all&"), "all");
  assert.equal(parseCredentialQuery("namespaces"), "");
  assert.equal(parseCredentialQuery("namespaces="), "");
  assert.equal(parseCredentialQuery("namespaces=%61ll"), "all");
  assert.equal(parseCredentialQuery("name%73paces=all"), "all");
  assert.equal(parseCredentialQuery("namespaces=a+ll"), "a ll");
  assert.equal(parseCredentialQuery("namespaces=all%00"), "all\u0000");
  assert.equal(parseCredentialQuery("namespaces=%C3%A9"), "é");
  assert.throws(() => parseCredentialQuery("namespaces=all&namespaces=beta"), (error: unknown) => {
    assert.ok(error instanceof GatewayFailure);
    assert.equal(error.type, "bad_request");
    assert.equal(error.status, 400);
    assert.equal(error.message, "duplicate query parameter `namespaces`");
    return true;
  });
  assert.throws(() => parseCredentialQuery("namespaces=all&namespaces=all"), (error: unknown) => {
    assert.equal(error instanceof GatewayFailure && error.message, "duplicate query parameter `namespaces`");
    return true;
  });
  for (const query of ["namespaces=%GG", "namespaces=%", "namespaces=%2", "%GG=1", "namespaces=%FF", "namespaces=%C3%28"]) {
    assert.throws(() => parseCredentialQuery(query), (error: unknown) => {
      assert.ok(error instanceof GatewayFailure);
      assert.equal(error.message, "invalid query string encoding");
      return true;
    });
  }
});

test("duplicate_namespaces_query_is_rejected", async () => {
  const store = createMemoryStore();
  await store.putNamespace({
    id: "platform",
    attrs: {},
    blocklist: null,
    allowPlatformFallback: false,
    fromConfig: true,
  });
  await store.putNamespace({
    id: "tenant",
    attrs: {},
    blocklist: null,
    allowPlatformFallback: false,
    fromConfig: false,
  });
  const app = createAxond({
    store,
    gatewayKey: KEY,
    defaultNamespace: "platform",
    credentials: [
      { namespace: "platform", provider: "fake-openai", secret: "sk-platform-secret", id: "platform-key" },
      { namespace: "tenant", provider: "fake-openai", secret: "sk-tenant-secret", id: "tenant-key" },
    ],
  });
  const headers = { authorization: `Bearer ${KEY}` };

  const own = await app.request("http://127.0.0.1/ns/platform/v1/credentials?foo=bar", { headers });
  assert.equal(own.status, 200);
  const ownBody = await own.json();
  assert.deepEqual(
    ownBody.data.map((row: { credential_id: string }) => row.credential_id),
    ["platform-key"],
  );

  const decoded = await app.request("http://127.0.0.1/ns/platform/v1/credentials?name%73paces=%61ll", { headers });
  assert.equal(decoded.status, 200);
  const decodedBody = await decoded.json();
  assert.deepEqual(
    decodedBody.data.map((row: { credential_id: string }) => row.credential_id).sort(),
    ["platform-key", "tenant-key"],
  );

  const repeated = await app.request(
    "http://127.0.0.1/ns/tenant/v1/credentials?namespaces=all&namespaces=beta",
    { headers },
  );
  assert.equal(repeated.status, 400);
  const repeatedBody = await repeated.json();
  assert.equal(repeatedBody.error.type, "bad_request");
  assert.equal(repeatedBody.error.message, "duplicate query parameter `namespaces`");
  assert.equal(JSON.stringify(repeatedBody).includes("sk-platform-secret"), false);
  assert.equal(JSON.stringify(repeatedBody).includes("sk-tenant-secret"), false);

  const encoded = await app.request("http://127.0.0.1/ns/platform/v1/credentials?foo=%GG&namespaces=all", { headers });
  assert.equal(encoded.status, 400);
  assert.equal((await encoded.json()).error.message, "invalid query string encoding");

  const empty = await app.request("http://127.0.0.1/ns/platform/v1/credentials?namespaces=", { headers });
  assert.equal(empty.status, 400);
  assert.equal((await empty.json()).error.message, "invalid `namespaces` value");

  const nul = await app.request("http://127.0.0.1/ns/platform/v1/credentials?namespaces=all%00", { headers });
  assert.equal(nul.status, 400);
  assert.equal((await nul.json()).error.message, "invalid `namespaces` value");
});
