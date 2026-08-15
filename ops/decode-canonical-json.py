#!/usr/bin/env python3
"""Decode Axond's canonical v1 body encoding for qualification evidence.

This is deliberately a qualification helper, not a second production reader.
The gateway stores desired-state bodies as tagged, length-prefixed canonical
bytes, while the administrative state projection intentionally never returns
body content. The restore drill uses this narrow decoder to inspect a restored
price-book body without adding a body-leaking admin route.
"""

from __future__ import annotations

import json
import struct
import sys


MAGIC = b"axond.desired-state\0"
MAX_DEPTH = 32


class DecodeError(ValueError):
    """The input is not a canonical value this qualification helper can read."""


class Cursor:
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.offset = 0

    def take(self, count: int) -> bytes:
        end = self.offset + count
        if count < 0 or end > len(self.data):
            raise DecodeError("canonical bytes end mid-value")
        value = self.data[self.offset : end]
        self.offset = end
        return value

    def byte(self) -> int:
        return self.take(1)[0]

    def length(self) -> int:
        value = struct.unpack(">Q", self.take(8))[0]
        remaining = len(self.data) - self.offset
        if value > remaining:
            raise DecodeError("canonical length exceeds remaining bytes")
        return value

    def string(self) -> str:
        try:
            return self.take(self.length()).decode("utf-8")
        except UnicodeDecodeError as error:
            raise DecodeError("canonical string is not UTF-8") from error

    def value(self, depth: int = 0):
        if depth > MAX_DEPTH:
            raise DecodeError("canonical value is too deeply nested")
        tag = self.byte()
        if tag == 0x01:
            value = self.byte()
            if value not in (0, 1):
                raise DecodeError("canonical boolean is not 0 or 1")
            return bool(value)
        if tag == 0x02:
            return int.from_bytes(self.take(16), "big", signed=True)
        if tag == 0x03:
            return self.string()
        if tag == 0x04:
            return {"$bytes": self.take(self.length()).hex()}
        if tag in (0x05, 0x06):
            return [self.value(depth + 1) for _ in range(self.length())]
        if tag == 0x07:
            fields = {}
            previous_key = None
            for _ in range(self.length()):
                if self.byte() != 0x03:
                    raise DecodeError("canonical map key is not a string")
                key_bytes = self.take(self.length())
                try:
                    key = key_bytes.decode("utf-8")
                except UnicodeDecodeError as error:
                    raise DecodeError("canonical map key is not UTF-8") from error
                ordering = (len(key_bytes), key_bytes)
                if previous_key is not None and ordering <= previous_key:
                    raise DecodeError("canonical map keys are not strictly ordered")
                previous_key = ordering
                if key in fields:
                    raise DecodeError("canonical map key is duplicated")
                fields[key] = self.value(depth + 1)
            return fields
        raise DecodeError(f"unknown canonical value tag: {tag:#x}")


def decode(encoded_hex: str):
    try:
        data = bytes.fromhex(encoded_hex)
    except ValueError as error:
        raise DecodeError("body is not hexadecimal") from error
    if not data.startswith(MAGIC):
        raise DecodeError("canonical magic is missing")
    cursor = Cursor(data[len(MAGIC) :])
    if cursor.byte() != 1:
        raise DecodeError("unsupported canonical serializer version")
    value = cursor.value()
    if cursor.offset != len(cursor.data):
        raise DecodeError("canonical body has trailing bytes")
    return value


def main() -> int:
    try:
        value = decode(sys.stdin.buffer.read().decode("ascii").strip())
    except (DecodeError, UnicodeDecodeError) as error:
        print(f"decode-canonical-json: {error}", file=sys.stderr)
        return 1
    json.dump(value, sys.stdout, separators=(",", ":"), sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
