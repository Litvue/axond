import { GatewayFailure } from "./errors.ts";

const MAX_LEN = 128;

const EMPTY_ID = "a namespace identifier must not be empty";
const LONG_ID = "a namespace identifier is over the 128-byte limit";
const CHAR_ID =
  "a namespace identifier contains a character outside ASCII letters, digits, `.`, `-`, and `_`";
const BOUNDARY_ID = "a namespace identifier must start and end with an ASCII letter or digit";

function isAsciiAlphanumeric(byte: number): boolean {
  return (
    (byte >= 0x30 && byte <= 0x39) ||
    (byte >= 0x41 && byte <= 0x5a) ||
    (byte >= 0x61 && byte <= 0x7a)
  );
}

/**
 * Store-facing refusal for a namespace id, in the same order as
 * `NamespaceId::parse`. The text itself stays out of the message. `null`
 * means the id is canonical.
 */
export function namespaceIdMessage(input: string): string | null {
  if (input.length === 0) {
    return EMPTY_ID;
  }
  const bytes = new TextEncoder().encode(input);
  if (bytes.length > MAX_LEN) {
    return LONG_ID;
  }
  for (const byte of bytes) {
    if (!isAsciiAlphanumeric(byte) && byte !== 0x2d && byte !== 0x5f && byte !== 0x2e) {
      return CHAR_ID;
    }
  }
  const first = bytes[0]!;
  const last = bytes[bytes.length - 1]!;
  if (!isAsciiAlphanumeric(first) || !isAsciiAlphanumeric(last)) {
    return BOUNDARY_ID;
  }
  return null;
}

/**
 * Canonical namespace id: one URL segment of ASCII letters, digits, `.`, `-`,
 * and `_`, starting and ending with a letter or digit. The text is not decoded
 * and not case-folded. A refused id is never copied into the error.
 *
 * Inference paths collapse every refusal to `invalid_namespace`. Management
 * create uses {@link namespaceIdMessage} as `bad_request`.
 */
export function parseNamespaceId(input: string): string {
  if (namespaceIdMessage(input) !== null) {
    throw new GatewayFailure("invalid_namespace", 400, "namespace identifier is invalid");
  }
  return input;
}

export function namespaceFromCanonicalPath(path: string): string {
  // `/namespaces/{ns}` is the withdrawn ADR 0062 spelling. It is not a prefix
  // of `/ns/`, and it is not mounted.
  if (!path.startsWith("/ns/")) {
    throw new GatewayFailure("invalid_namespace", 400, "namespace identifier is invalid");
  }
  const rest = path.slice("/ns/".length);
  const slash = rest.indexOf("/");
  if (slash <= 0 || slash === rest.length - 1) {
    throw new GatewayFailure("invalid_namespace", 400, "namespace identifier is invalid");
  }
  return parseNamespaceId(rest.slice(0, slash));
}

export function validatePeriod(period: string): void {
  if (period.length === 0 || period.length > 128 || !/^[A-Za-z0-9._-]+$/.test(period)) {
    throw new GatewayFailure(
      "bad_request",
      400,
      "period must be 1–128 characters of [A-Za-z0-9._-]",
    );
  }
}

export function monthlyPeriod(nowMs: number, timeZone: string): string {
  validateTimezone(timeZone);
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone,
    year: "numeric",
    month: "2-digit",
  }).formatToParts(new Date(nowMs));
  const year = parts.find((part) => part.type === "year")?.value;
  const month = parts.find((part) => part.type === "month")?.value;
  if (!year || !month) {
    throw new GatewayFailure("bad_request", 400, `unknown timezone \`${timeZone}\``);
  }
  return `${year}-${month}`;
}

export function validateTimezone(timeZone: string): void {
  try {
    new Intl.DateTimeFormat("en-US", { timeZone }).format(0);
  } catch {
    throw new GatewayFailure("bad_request", 400, `unknown timezone \`${timeZone}\``);
  }
}
