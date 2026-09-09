#!/usr/bin/env python3
"""Read Store rows from the launched SQLite file. Read-only proof helper."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sqlite", required=True)
    parser.add_argument("--namespaces", action="store_true")
    parser.add_argument("--budget", nargs=2, metavar=("NS", "PERIOD"))
    args = parser.parse_args()
    path = Path(args.sqlite)
    if not path.is_file():
        print(f"no sqlite file at {path}", file=sys.stderr)
        return 1
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 5000")
    if args.namespaces:
        rows = [dict(row) for row in conn.execute("SELECT id, attrs, blocklist FROM axond_namespace ORDER BY id")]
        json.dump(rows, sys.stdout, indent=2, default=str)
        print()
        return 0
    if args.budget:
        ns, period = args.budget
        rows = [
            dict(row)
            for row in conn.execute(
                "SELECT * FROM axond_store_budget WHERE namespace = ? AND period = ?",
                (ns, period),
            )
        ]
        json.dump(rows, sys.stdout, indent=2, default=str)
        print()
        return 0
    print("pass --namespaces or --budget NS PERIOD", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
