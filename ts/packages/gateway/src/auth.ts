import { GatewayFailure } from "./errors.ts";

/** Compare two strings without returning early on the first differing byte. */
export function constantTimeEqual(left: string, right: string): boolean {
  const encoder = new TextEncoder();
  const a = encoder.encode(left);
  const b = encoder.encode(right);
  const length = Math.max(a.length, b.length);
  let diff = a.length ^ b.length;
  for (let index = 0; index < length; index += 1) {
    diff |= (a[index] ?? 0) ^ (b[index] ?? 0);
  }
  return diff === 0;
}

/**
 * Authorization: Bearer wins over x-api-key, matching the Rust gateway.
 * The raw secret is returned. Callers compare it in constant time.
 */
export function presentedCredential(headers: Headers): string | null {
  const authorization = headers.get("authorization");
  if (authorization !== null) {
    const bearer = authorization.startsWith("Bearer ") ? authorization.slice("Bearer ".length) : null;
    if (bearer !== null && bearer.length > 0) {
      return bearer;
    }
    return null;
  }
  const apiKey = headers.get("x-api-key");
  if (apiKey !== null && apiKey.length > 0) {
    return apiKey;
  }
  return null;
}

export function assertGatewayKey(presented: string | null, gatewayKey: string, alreadyAuthenticated: boolean): void {
  if (alreadyAuthenticated) {
    return;
  }
  if (presented === null || !constantTimeEqual(presented, gatewayKey)) {
    throw new GatewayFailure("unauthorized", 401, "unauthorized");
  }
}
