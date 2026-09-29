import { GatewayFailure } from "./errors.ts";

const MAX_LEN = 128;

/**
 * Canonical namespace id: one URL segment of ASCII letters, digits, `.`, `-`,
 * and `_`, starting and ending with a letter or digit. The text is not decoded
 * and not case-folded. A refused id is never copied into the error.
 */
export function parseNamespaceId(input: string): string {
  if (
    input.length === 0 ||
    input.length > MAX_LEN ||
    !/^[\x00-\x7F]*$/.test(input) ||
    ![...input].every((char) => /[A-Za-z0-9._-]/.test(char)) ||
    !/^[A-Za-z0-9]/.test(input) ||
    !/[A-Za-z0-9]$/.test(input)
  ) {
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
