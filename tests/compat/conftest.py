"""Boot a real axond binary against the fake upstream, once per session.

The binary is located by ``AXOND_BIN`` (the CI lane passes the path it built);
otherwise the debug build is used, so ``cargo build && pytest tests/compat`` is
all a local run needs.
"""

from __future__ import annotations

import os
import socket
import subprocess
import time
from pathlib import Path

import pytest
import urllib.error
import urllib.request

from fake_upstream import CHAT, EMBEDDINGS, FakeUpstream, MESSAGES, RESPONSES

REPO_ROOT = Path(__file__).resolve().parents[2]
GATEWAY_KEY = "test-inbound-key"
UPSTREAM_OPENAI_KEY = "test-upstream-openai-key"
UPSTREAM_ANTHROPIC_KEY = "test-upstream-anthropic-key"

PRICE = (
    "{ input_microdollars_per_million = 2500000, "
    "output_microdollars_per_million = 10000000 }"
)


def _binary() -> Path:
    path = Path(os.environ.get("AXOND_BIN", REPO_ROOT / "target/debug/axond"))
    if not path.exists():
        raise RuntimeError(f"axond binary not found at {path}; build it first")
    return path


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _config(bind: str, upstream: str) -> str:
    models = "".join(
        f'[[model]]\nname = "{alias}"\n'
        f'targets = [ {{ provider = "{provider}", model = "{target}", price = {PRICE} }} ]\n\n'
        for alias, provider, target in [
            ("chat-golden", "fake-openai", CHAT),
            ("messages-golden", "fake-anthropic", MESSAGES),
            ("embeddings-golden", "fake-openai", EMBEDDINGS),
            ("responses-golden", "fake-openai", RESPONSES),
        ]
    )
    return f"""
[server]
bind = "{bind}"

[[namespace]]
id = "platform"
default = true

[[provider]]
id = "fake-openai"
kind = "openai"
base_url = "{upstream}"

[[provider]]
id = "fake-anthropic"
kind = "anthropic"
base_url = "{upstream}"

[[credential]]
namespace = "platform"
provider = "fake-openai"
env = "GW_FAKE_OPENAI_KEY"

[[credential]]
namespace = "platform"
provider = "fake-anthropic"
env = "GW_FAKE_ANTHROPIC_KEY"

[[gateway_key]]
env = "GW_INBOUND_KEY"
namespace = "platform"

{models}"""


@pytest.fixture(scope="session")
def upstream():
    with FakeUpstream() as server:
        yield server


@pytest.fixture(scope="session")
def gateway(upstream, tmp_path_factory):
    bind = f"127.0.0.1:{_free_port()}"
    config = tmp_path_factory.mktemp("axond") / "axond.toml"
    config.write_text(_config(bind, upstream.base_url))

    env = {
        **os.environ,
        "AXOND_CONFIG": str(config),
        "GW_INBOUND_KEY": GATEWAY_KEY,
        "GW_FAKE_OPENAI_KEY": UPSTREAM_OPENAI_KEY,
        "GW_FAKE_ANTHROPIC_KEY": UPSTREAM_ANTHROPIC_KEY,
        "RUST_LOG": "warn",
    }
    env.pop("OTEL_EXPORTER_OTLP_ENDPOINT", None)
    process = subprocess.Popen([str(_binary())], env=env)
    base_url = f"http://{bind}"
    try:
        _await_ready(process, base_url)
        yield base_url
    finally:
        process.terminate()
        process.wait(timeout=10)


def _await_ready(process: subprocess.Popen, base_url: str) -> None:
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"axond exited with {process.returncode}")
        try:
            with urllib.request.urlopen(f"{base_url}/healthz", timeout=1) as response:
                if response.status == 200:
                    return
        except (urllib.error.URLError, ConnectionError, TimeoutError):
            time.sleep(0.05)
    raise RuntimeError("axond did not become healthy")
