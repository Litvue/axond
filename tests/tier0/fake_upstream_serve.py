"""Serve the committed compatibility fixtures on a fixed loopback port."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "compat"))
from fake_upstream import serve_forever  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=18082)
    args = parser.parse_args()
    serve_forever(args.port)


if __name__ == "__main__":
    main()
