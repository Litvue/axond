import assert from "node:assert/strict";
import test from "node:test";

import { createRequestIdGenerator } from "./request-id.ts";

const SHAPE = /^req_[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const MAX_MILLIS = (1n << 48n) - 1n;

function parts(id: string): { millis: bigint; sequence: number } {
  assert.match(id, SHAPE);
  assert.equal(id.length, 40);
  const hex = id.slice(4).replaceAll("-", "");
  const bytes = new Uint8Array(16);
  for (let index = 0; index < 16; index += 1) {
    bytes[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
  }
  let millis = 0n;
  for (let index = 0; index < 6; index += 1) {
    millis = (millis << 8n) | BigInt(bytes[index]!);
  }
  return { millis, sequence: ((bytes[6]! & 0x0f) << 8) | bytes[7]! };
}

test("a minted id is req_ plus a lowercase uuid7", () => {
  const id = createRequestIdGenerator()();
  const parsed = parts(id);
  assert.equal(parsed.sequence >= 0 && parsed.sequence <= 0xfff, true);
});

test("ids minted in one generator sort in mint order", () => {
  const mint = createRequestIdGenerator();
  const minted = Array.from({ length: 1000 }, () => mint());
  assert.equal(new Set(minted).size, minted.length);
  for (let index = 1; index < minted.length; index += 1) {
    assert.equal(minted[index - 1]! < minted[index]!, true);
  }
});

test("the same millisecond advances the 12-bit sequence", () => {
  const mint = createRequestIdGenerator();
  const stamp = 1_700_000_000_000;
  const first = parts(mint(stamp));
  const second = parts(mint(stamp));
  const steppedBack = parts(mint(stamp - 50));
  assert.equal(first.millis, BigInt(stamp));
  assert.equal(first.sequence, 0);
  assert.equal(second.millis, first.millis);
  assert.equal(second.sequence, 1);
  assert.equal(steppedBack.millis, first.millis);
  assert.equal(steppedBack.sequence, 2);
});

test("an exhausted sequence carries into the next millisecond", () => {
  const mint = createRequestIdGenerator();
  const stamp = 50;
  let last = "";
  for (let index = 0; index < 4096; index += 1) {
    last = mint(stamp);
  }
  const atCap = parts(last);
  const carriedId = mint(stamp);
  const carried = parts(carriedId);
  const later = parts(mint(stamp + 10));
  assert.equal(atCap.millis, BigInt(stamp));
  assert.equal(atCap.sequence, 4095);
  assert.equal(last < carriedId, true);
  assert.equal(carried.millis, BigInt(stamp + 1));
  assert.equal(carried.sequence, 0);
  assert.equal(later.millis, BigInt(stamp + 10));
  assert.equal(later.sequence, 0);
});

test("the timestamp saturates at the 48-bit maximum", () => {
  const mint = createRequestIdGenerator();
  const stamp = Number(MAX_MILLIS);
  const first = parts(mint(stamp));
  for (let index = 0; index < 4095; index += 1) {
    mint(stamp);
  }
  const saturated = parts(mint(stamp + 1));
  assert.equal(first.millis, MAX_MILLIS);
  assert.equal(first.sequence, 0);
  assert.equal(saturated.millis, MAX_MILLIS);
  assert.equal(saturated.sequence, 0);
});
