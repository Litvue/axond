#!/usr/bin/env python3
"""Verification scaffolding: committed wire fixtures plus a JSONL request log.

Not a production fake. Each POST is logged (path, model, status, key tail only)
so a proof can show the gateway forwarded a request without writing secret
material. Serves the same bytes as tests/compat/fake_upstream.py.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(REPO / "tests" / "compat"))

from fake_upstream import _Handler, fixture  # noqa: E402
from http.server import ThreadingHTTPServer


class LoggingHandler(_Handler):
    def do_POST(self):  # noqa: N802
        length = int(self.headers.get("content-length", "0"))
        raw = self.rfile.read(length) or b"{}"
        try:
            body = json.loads(raw)
        except json.JSONDecodeError:
            body = {}
        streamed = bool(body.get("stream"))
        path = self.path.split("?", 1)[0]
        status = 200
        try:
            if streamed and path in {
                "/chat/completions",
                "/messages",
                "/responses",
            }:
                from fake_upstream import _STREAMED

                payload = fixture(_STREAMED[path])
                self._stream(payload)
            else:
                from fake_upstream import _BUFFERED

                payload = fixture(_BUFFERED[(path, False)])
                self._buffered(payload)
        except KeyError:
            status = 404
            payload = b'{"error":{"type":"not_found","message":"no fixture"}}'
            self.send_response(404)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        auth = self.headers.get("authorization") or self.headers.get("x-api-key") or ""
        tail = auth[-4:] if auth else ""
        record = {
            "path": path,
            "model": body.get("model"),
            "stream": streamed,
            "status": status,
            "key_tail": tail,
        }
        log_path = getattr(self.server, "verify_log", None)
        if log_path:
            with Path(log_path).open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(record, separators=(",", ":")) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--log", required=True, help="JSONL path for each upstream POST")
    args = parser.parse_args()
    server = ThreadingHTTPServer(("127.0.0.1", args.port), LoggingHandler)
    server.verify_log = args.log
    Path(args.log).parent.mkdir(parents=True, exist_ok=True)
    Path(args.log).touch()
    try:
        server.serve_forever()
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
