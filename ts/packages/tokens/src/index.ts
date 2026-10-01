import { Hono } from "hono";

import type { AxondEnv, AxondExtension } from "@axond/sdk";

export interface TokenClaims {
  sub: string;
  namespace: string;
  scope: string[];
  globs: string[];
  cap: string | null;
  exp: number;
  epoch: number;
  jti: string;
}

/**
 * pre-auth verification of `axt1.` tokens, a static-key mint route, and a
 * post-auth revocation lookup. Tokens are HMAC-SHA256 over the JSON claims.
 */
const claimsByRequest = new Map<string, TokenClaims>();

/** How many verified tokens are still held after their request finished. */
export function heldTokenClaims(): number {
  return claimsByRequest.size;
}

export function tokensExtension(signingKey: string): AxondExtension[] {
  const routes = new Hono<AxondEnv>();
  routes.post("/api/v1/tokens", async (c) => {
    const authorization = c.req.header("authorization") ?? "";
    const presented = authorization.startsWith("Bearer ") ? authorization.slice("Bearer ".length) : "";
    if (presented.length === 0 || !timingSafe(presented, signingKey)) {
      return c.json({ error: { type: "unauthorized", message: "unauthorized" } }, 401);
    }
    const body = await c.req.json<Partial<TokenClaims>>();
    if (!body.sub || !body.namespace || !body.exp) {
      return c.json({ error: { type: "bad_request", message: "missing token claims" } }, 400);
    }
    const claims: TokenClaims = {
      sub: body.sub,
      namespace: body.namespace,
      scope: body.scope ?? ["chat", "messages", "embeddings", "responses", "models"],
      globs: body.globs ?? ["*"],
      cap: body.cap ?? null,
      exp: body.exp,
      epoch: body.epoch ?? 1,
      jti: body.jti ?? crypto.randomUUID(),
    };
    const token = await signToken(signingKey, claims);
    return c.json({ token, claims }, 201);
  });
  const migrations = [
    `CREATE TABLE IF NOT EXISTS axond_ext_tokens_revocation (
        jti TEXT PRIMARY KEY NOT NULL,
        namespace TEXT NOT NULL
      )`,
    `CREATE TABLE IF NOT EXISTS axond_ext_tokens_epoch (
        namespace TEXT PRIMARY KEY NOT NULL,
        epoch INTEGER NOT NULL
      )`,
  ];
  const verify: AxondExtension = {
    name: "tokens",
    apiVersion: 1,
    stage: "pre-auth",
    trusted: true,
    routes,
    migrations,
    async middleware(c, next) {
      const header = c.req.header("authorization") ?? "";
      const token = header.startsWith("Bearer ") ? header.slice("Bearer ".length) : (c.req.header("x-api-key") ?? "");
      if (!token.startsWith("axt1.")) {
        await next();
        return;
      }
      const claims = await verifyToken(signingKey, token);
      if (!claims) {
        return c.json({ error: { type: "unauthorized", message: "unauthorized" } }, 401);
      }
      if (claims.exp * 1000 <= Date.now()) {
        return c.json({ error: { type: "token_expired", message: "token expired" } }, 401);
      }
      const axond = c.get("axond");
      claimsByRequest.set(axond.requestId, claims);
      axond.authenticated = true;
      axond.subject = claims.sub;
      axond.aliasGlobs = claims.globs;
      if (claims.cap !== null) {
        axond.spendCapMicrodollars = BigInt(claims.cap);
      }
      try {
        await next();
      } finally {
        claimsByRequest.delete(axond.requestId);
      }
    },
  };
  const revoke: AxondExtension = {
    name: "tokens",
    apiVersion: 1,
    stage: "post-auth",
    trusted: true,
    async middleware(c, next) {
      const axond = c.get("axond");
      const claims = claimsByRequest.get(axond.requestId);
      if (!claims) {
        await next();
        return;
      }
      if (axond.namespace?.id !== claims.namespace) {
        return c.json(
          {
            error: {
              type: "namespace_not_authorized",
              message: "the authenticated grant does not authorize the selected namespace",
            },
          },
          403,
        );
      }
      if (!claims.scope.includes(axond.route)) {
        return c.json({ error: { type: "token_scope_insufficient", message: `token scope does not authorize \`${axond.route}\`` } }, 403);
      }
      const revoked = await axond.store.query(
        "SELECT jti FROM axond_ext_tokens_revocation WHERE jti = ? AND namespace = ?",
        [claims.jti, claims.namespace],
      );
      if (revoked.rows.length > 0) {
        return c.json({ error: { type: "token_revoked", message: "token revoked" } }, 401);
      }
      const epoch = await axond.store.query(
        "SELECT epoch FROM axond_ext_tokens_epoch WHERE namespace = ?",
        [claims.namespace],
      );
      const current = epoch.rows[0] ? Number(epoch.rows[0]["epoch"]) : claims.epoch;
      if (claims.epoch < current) {
        return c.json({ error: { type: "token_expired", message: "token epoch is stale" } }, 401);
      }
      await next();
    },
  };
  return [verify, revoke];
}

export async function signToken(key: string, claims: TokenClaims): Promise<string> {
  const payload = base64Url(new TextEncoder().encode(JSON.stringify(claims)));
  const mac = await hmac(key, payload);
  return `axt1.${payload}.${base64Url(mac)}`;
}

export async function verifyToken(key: string, token: string): Promise<TokenClaims | null> {
  const parts = token.split(".");
  if (parts.length !== 3 || parts[0] !== "axt1" || !parts[1] || !parts[2]) {
    return null;
  }
  const expected = base64Url(await hmac(key, parts[1]));
  if (!timingSafe(expected, parts[2])) {
    return null;
  }
  try {
    return JSON.parse(new TextDecoder().decode(base64UrlDecode(parts[1]))) as TokenClaims;
  } catch {
    return null;
  }
}

async function hmac(key: string, payload: string): Promise<Uint8Array> {
  const cryptoKey = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(key),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const signature = await crypto.subtle.sign("HMAC", cryptoKey, new TextEncoder().encode(payload));
  return new Uint8Array(signature);
}

function base64Url(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }
  return btoa(binary).replaceAll("+", "-").replaceAll("/", "_").replaceAll("=", "");
}

function base64UrlDecode(value: string): Uint8Array {
  const padded = value.replaceAll("-", "+").replaceAll("_", "/") + "===".slice((value.length + 3) % 4);
  const binary = atob(padded);
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) {
    bytes[index] = binary.charCodeAt(index);
  }
  return bytes;
}

function timingSafe(left: string, right: string): boolean {
  const a = new TextEncoder().encode(left);
  const b = new TextEncoder().encode(right);
  let diff = a.length ^ b.length;
  const length = Math.max(a.length, b.length);
  for (let index = 0; index < length; index += 1) {
    diff |= (a[index] ?? 0) ^ (b[index] ?? 0);
  }
  return diff === 0;
}
