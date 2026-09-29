#!/usr/bin/env bash
# Conformance launcher for the TypeScript gateway.
# Boots nothing itself: the existing compat lanes spawn AXOND_BIN.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export AXOND_BIN="${AXOND_BIN:-$ROOT/ts/bin/axond}"
echo "AXOND_BIN=$AXOND_BIN"
(cd "$ROOT/ts" && npm test && npm run lint:web && npm run check:docs)
(cd "$ROOT" && python3 -m pytest tests/compat -q)
(cd "$ROOT/tests/compat-ts" && npm test)
python3 "$ROOT/ops/check-openapi.py" "$ROOT/ts/openapi.json"
