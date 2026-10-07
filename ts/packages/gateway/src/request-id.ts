/**
 * Monotonic `req_` + UUIDv7 mint, matching `Uuid7Generator` in
 * `crates/gateway/src/desired_state/ids.rs`.
 *
 * The 48-bit timestamp is Unix milliseconds. The 12-bit sequence advances when
 * the clock has not moved, carries into the next millisecond when it is
 * exhausted, and stays on the last millisecond when the clock steps backwards.
 * At the end of the 48-bit range the timestamp saturates.
 */

const MAX_MILLIS = (1n << 48n) - 1n;
const MAX_SEQUENCE = 0xfff;

export function createRequestIdGenerator(): (nowMs?: number) => string {
  let lastMillis = 0n;
  let lastSequence = 0;
  return (nowMs = Date.now()) => {
    const raw = Number.isFinite(nowMs) ? Math.floor(nowMs) : 0;
    let now = BigInt(Math.max(0, raw));
    if (now > MAX_MILLIS) {
      now = MAX_MILLIS;
    }
    let millis: bigint;
    let sequence: number;
    if (now > lastMillis) {
      millis = now;
      sequence = 0;
    } else if (lastSequence < MAX_SEQUENCE) {
      millis = lastMillis;
      sequence = lastSequence + 1;
    } else {
      millis = lastMillis + 1n;
      if (millis > MAX_MILLIS) {
        millis = MAX_MILLIS;
      }
      sequence = 0;
    }
    lastMillis = millis;
    lastSequence = sequence;
    const entropy = new Uint8Array(8);
    crypto.getRandomValues(entropy);
    const bytes = new Uint8Array(16);
    let rest = millis;
    for (let index = 5; index >= 0; index -= 1) {
      bytes[index] = Number(rest & 0xffn);
      rest >>= 8n;
    }
    bytes[6] = 0x70 | ((sequence >> 8) & 0x0f);
    bytes[7] = sequence & 0xff;
    bytes.set(entropy, 8);
    bytes[8] = (bytes[8]! & 0x3f) | 0x80;
    let hex = "";
    for (const byte of bytes) {
      hex += byte.toString(16).padStart(2, "0");
    }
    const text = `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
    return `req_${text}`;
  };
}

const mint = createRequestIdGenerator();

/** Mint the next request id for this process. */
export function mintRequestId(nowMs?: number): string {
  return mint(nowMs);
}
