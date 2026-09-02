#!/usr/bin/env python3
"""Boot a release binary and prove it serves, on every supported target.

`ops/tier0-gate.sh` is the stronger Linux gate: it boots the musl binary inside
a kernel-enforced network namespace against a temp SQLite file, so an external
datastore or egress dependency shows up as a boot or serving failure. That is
not a no-datastore promise. That gate cannot run on macOS or Windows, and
the release matrix ships binaries for both. This runner is the portable subset —
the same serving assertions, expressed with nothing but the standard library:

* `/healthz` and `/readyz` answer, unauthenticated;
* `/ns/platform/v1/models` needs a gateway key and lists the configured alias;
* an unknown model is refused with the typed `unknown_model` error;
* one chat completion completes against a local fixture upstream.

Portability, not convenience, drives the implementation choices: ports are
claimed from the operating system rather than hard-coded (a fixed port is a flake
on shared runners), the fixture upstream runs in-process instead of as a second
child, temporary files live in a `TemporaryDirectory` deleted after the child
exits (Windows will not unlink a file the child still holds), and the child is
terminated then killed through `subprocess` alone, since there are no process
groups or signals to rely on off Unix.

Usage: ops/binary-smoke.py [axond-binary]
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tests" / "compat"))

from fake_upstream import CHAT, FakeUpstream  # noqa: E402

GATEWAY_KEY = "binary-smoke-inbound-key"
UPSTREAM_KEY = "binary-smoke-upstream-placeholder"
ALIAS = "smoke-chat"
BOOT_TIMEOUT_SECONDS = 60.0
REQUEST_TIMEOUT_SECONDS = 10.0
TERMINATE_TIMEOUT_SECONDS = 10.0

CONFIG_TEMPLATE = """# Written by ops/binary-smoke.py; ports are claimed at run time.
[server]
bind = "{bind}"

[storage]
backend = "sqlite"
path = "{sqlite}"

[[namespace]]
id = "platform"
default = true

[[provider]]
id = "fixture-openai"
kind = "openai"
base_url = "{upstream}"

[[credential]]
namespace = "platform"
provider = "fixture-openai"
env = "GW_SMOKE_UPSTREAM_KEY"

[[gateway_key]]
env = "GW_SMOKE_INBOUND_KEY"
namespace = "platform"

[[model]]
name = "{alias}"
targets = [
  {{ provider = "fixture-openai", model = "{target_model}", price = {{ input_microdollars_per_million = 2500000, output_microdollars_per_million = 10000000 }} }},
]
"""


class SmokeFailure(RuntimeError):
    """A serving assertion the shipped binary must satisfy, but did not."""


class Response:
    def __init__(self, status: int, body: str) -> None:
        self.status = status
        self.body = body


def request(
    url: str, *, key: str | None = None, payload: dict | None = None
) -> Response:
    """One HTTP exchange, treating an error status as a value, not an exception."""
    data = None if payload is None else json.dumps(payload).encode()
    headers = {} if key is None else {"Authorization": f"Bearer {key}"}
    if data is not None:
        headers["content-type"] = "application/json"
    http_request = urllib.request.Request(url, data=data, headers=headers)
    try:
        with urllib.request.urlopen(
            http_request, timeout=REQUEST_TIMEOUT_SECONDS
        ) as response:
            return Response(response.status, response.read().decode("utf-8", "replace"))
    except urllib.error.HTTPError as error:
        with error:
            return Response(error.code, error.read().decode("utf-8", "replace"))


def claim_port() -> int:
    """A port the operating system says is free, on every platform.

    `SO_REUSEADDR` is deliberately not set: on Windows it permits two live
    binds to the same port, which would hand out a port axond cannot claim.
    """
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def resolve_binary(argument: str | None) -> Path:
    if argument:
        candidates = [Path(argument)]
    else:
        env = os.environ.get("AXOND_BIN")
        candidates = (
            [Path(env)]
            if env
            else [ROOT / "target" / "release" / "axond", ROOT / "target" / "debug" / "axond"]
        )
    for candidate in candidates:
        for path in (candidate, candidate.with_suffix(".exe")):
            if path.is_file():
                return path.resolve()
    searched = ", ".join(str(candidate) for candidate in candidates)
    raise SmokeFailure(f"no axond binary found (looked for {searched})")


def await_ready(process: subprocess.Popen, base_url: str) -> None:
    deadline = time.monotonic() + BOOT_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise SmokeFailure(
                f"axond exited with status {process.returncode} before serving /healthz"
            )
        try:
            with urllib.request.urlopen(f"{base_url}/healthz", timeout=1) as response:
                if response.status == 200:
                    return
        except (urllib.error.URLError, OSError):
            time.sleep(0.05)
    raise SmokeFailure(
        f"axond did not serve /healthz within {BOOT_TIMEOUT_SECONDS:.0f}s"
    )


def probe(base_url: str, upstream: FakeUpstream) -> None:
    """The serving contract every published binary owes an operator."""
    health = request(f"{base_url}/healthz")
    if health.status != 200 or health.body.strip() != "ok":
        raise SmokeFailure(f"/healthz answered {health.status} {health.body.strip()!r}")

    ready = request(f"{base_url}/readyz")
    if ready.status != 200 or ready.body.strip() != "ready":
        raise SmokeFailure(f"/readyz answered {ready.status} {ready.body.strip()!r}")

    anonymous = request(f"{base_url}/ns/platform/v1/models")
    if anonymous.status != 401:
        raise SmokeFailure(
            f"unauthenticated /v1/models answered {anonymous.status} instead of 401"
        )

    models = request(f"{base_url}/ns/platform/v1/models", key=GATEWAY_KEY)
    if models.status != 200:
        raise SmokeFailure(
            f"authenticated /v1/models answered {models.status}: {models.body}"
        )
    served = {entry.get("id") for entry in json.loads(models.body).get("data", [])}
    if ALIAS not in served:
        raise SmokeFailure(f"/v1/models omitted the configured alias {ALIAS}: {served}")

    unknown = request(
        f"{base_url}/ns/platform/v1/chat/completions",
        key=GATEWAY_KEY,
        payload={
            "model": "does-not-exist",
            "messages": [{"role": "user", "content": "hello"}],
        },
    )
    if unknown.status != 404:
        raise SmokeFailure(
            f"unknown model answered {unknown.status} instead of 404: {unknown.body}"
        )
    error_type = json.loads(unknown.body).get("error", {}).get("type")
    if error_type != "unknown_model":
        raise SmokeFailure(
            f"unknown model was refused as {error_type!r}, not 'unknown_model'"
        )

    completion = request(
        f"{base_url}/ns/platform/v1/chat/completions",
        key=GATEWAY_KEY,
        payload={
            "model": ALIAS,
            "messages": [{"role": "user", "content": "What is the capital of France?"}],
        },
    )
    if completion.status != 200:
        raise SmokeFailure(
            f"fixture request answered {completion.status}: {completion.body}"
        )
    body = json.loads(completion.body)
    if body.get("object") != "chat.completion":
        raise SmokeFailure(f"fixture request was not chat-completion shaped: {body}")
    dispatched = [entry for entry in upstream.requests if entry["model"] == CHAT]
    if not dispatched:
        raise SmokeFailure(
            "the fixture upstream saw no request for "
            f"{CHAT}; the alias did not reach a target"
        )

    print(f"healthz: ok, readyz: ready, models: {ALIAS}")
    print("auth: unauthenticated /v1/models -> 401")
    print("errors: unknown model -> 404 unknown_model")
    print("serving: local fixture upstream -> 200 chat.completion")


def stop(process: subprocess.Popen) -> None:
    """Leave no child behind, with no signal or process-group assumptions."""
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=TERMINATE_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=TERMINATE_TIMEOUT_SECONDS)


def smoke(binary: Path) -> None:
    with FakeUpstream() as upstream, tempfile.TemporaryDirectory(
        prefix="axond-binary-smoke-"
    ) as workspace:
        directory = Path(workspace)
        port = claim_port()
        config = directory / "axond.toml"
        config.write_text(
            CONFIG_TEMPLATE.format(
                bind=f"127.0.0.1:{port}",
                sqlite=directory / "axond.sqlite",
                upstream=upstream.base_url,
                alias=ALIAS,
                target_model=CHAT,
            ),
            encoding="utf-8",
        )
        environment = {
            **os.environ,
            "AXOND_CONFIG": str(config),
            "GW_SMOKE_INBOUND_KEY": GATEWAY_KEY,
            "GW_SMOKE_UPSTREAM_KEY": UPSTREAM_KEY,
            "RUST_LOG": "warn",
        }
        # An exporter inherited from the environment would make the boot depend
        # on a collector this smoke does not run.
        for inherited in (
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            "OTEL_EXPORTER_OTLP_PROTOCOL",
        ):
            environment.pop(inherited, None)

        log_path = directory / "axond.log"
        # The child's own handle is closed before the directory is removed;
        # Windows refuses to unlink a file an open handle still refers to.
        with log_path.open("wb") as log:
            process = subprocess.Popen(
                [str(binary)], env=environment, stdout=log, stderr=subprocess.STDOUT
            )
            try:
                await_ready(process, f"http://127.0.0.1:{port}")
                probe(f"http://127.0.0.1:{port}", upstream)
            except BaseException:
                stop(process)
                sys.stderr.write(
                    f"--- axond log ---\n{log_path.read_text(encoding='utf-8', errors='replace')}"
                )
                raise
            stop(process)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "binary",
        nargs="?",
        help="axond binary to smoke; defaults to $AXOND_BIN, then a release "
        "build, then a debug build",
    )
    arguments = parser.parse_args()
    try:
        binary = resolve_binary(arguments.binary)
        print(f"smoking {binary}")
        smoke(binary)
    except SmokeFailure as failure:
        print(f"BINARY SMOKE FAILED: {failure}", file=sys.stderr)
        return 1
    print("binary smoke passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
