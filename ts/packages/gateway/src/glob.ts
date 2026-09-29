import { GatewayFailure } from "./errors.ts";

/** Exact id, `prefix*`, `*suffix`, or `*`. One star, at either end. */
export function validateGlob(pattern: string): void {
  const stars = [...pattern].filter((char) => char === "*").length;
  const star = pattern.indexOf("*");
  const valid = stars === 0 || pattern === "*" || (stars === 1 && (star === 0 || star === pattern.length - 1));
  if (!valid || pattern.length === 0) {
    throw new GatewayFailure(
      "bad_request",
      400,
      `blocklist glob \`${pattern}\` is invalid: use an exact id, \`prefix*\`, \`*suffix\`, or \`*\``,
    );
  }
}

export function globMatch(pattern: string, value: string): boolean {
  if (pattern === "*") {
    return true;
  }
  if (!pattern.includes("*")) {
    return pattern === value;
  }
  if (pattern.startsWith("*")) {
    return value.endsWith(pattern.slice(1));
  }
  if (pattern.endsWith("*")) {
    return value.startsWith(pattern.slice(0, -1));
  }
  return false;
}
