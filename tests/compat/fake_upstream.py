"""A fake provider upstream serving the committed wire fixtures.

The Python twin of ``crates/gateway/tests/support/upstream.rs``: same fixtures,
same target-model vocabulary, so the SDK lane and the Rust lane qualify the
gateway against identical bytes (ADR 0014).
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"

CHAT = "fixture-chat"
EMBEDDINGS = "fixture-embeddings"
MESSAGES = "fixture-messages"

_BUFFERED = {
    ("/chat/completions", False): "openai/chat_completion.json",
    ("/embeddings", False): "openai/embeddings.json",
    ("/messages", False): "anthropic/message_thinking_tool_use.json",
}
_STREAMED = {
    "/chat/completions": "openai/chat_completion.sse",
    "/messages": "anthropic/message_thinking_tool_use.sse",
}


def fixture(name: str) -> bytes:
    return (FIXTURES / name).read_bytes()


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_args):  # noqa: D102 - quiet by default
        pass

    def do_POST(self):  # noqa: N802 - BaseHTTPRequestHandler's spelling
        length = int(self.headers.get("content-length", "0"))
        body = json.loads(self.rfile.read(length) or b"{}")
        self.server.requests.append(
            {
                "path": self.path,
                "model": body.get("model"),
                "authorization": self.headers.get("authorization"),
                "x-api-key": self.headers.get("x-api-key"),
                "anthropic-version": self.headers.get("anthropic-version"),
                "body": body,
            }
        )
        streamed = bool(body.get("stream"))
        if streamed and self.path in _STREAMED:
            self._stream(fixture(_STREAMED[self.path]))
        else:
            self._buffered(fixture(_BUFFERED[(self.path, False)]))

    def _buffered(self, payload: bytes) -> None:
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _stream(self, payload: bytes) -> None:
        """Write the recorded SSE bytes one event at a time, close-delimited."""
        self.send_response(200)
        self.send_header("content-type", "text/event-stream")
        self.send_header("connection", "close")
        self.end_headers()
        for event in payload.split(b"\n\n"):
            if not event:
                continue
            self.wfile.write(event + b"\n\n")
            self.wfile.flush()
        self.close_connection = True


class FakeUpstream:
    """A running fake upstream; use as a context manager."""

    def __init__(self) -> None:
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        self._server.requests = []
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def base_url(self) -> str:
        host, port = self._server.server_address[:2]
        return f"http://{host}:{port}"

    @property
    def requests(self) -> list[dict]:
        return self._server.requests

    def __enter__(self) -> "FakeUpstream":
        self._thread.start()
        return self

    def __exit__(self, *_exc) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)
