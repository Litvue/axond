#!/usr/bin/env python3
"""Validate the generated /api/v1 OpenAPI 3.1 document.

The spec is produced from the code (utoipa) and dumped by
`openapi_spec_is_31_and_covers_mounted_routes` when `AXOND_OPENAPI_OUT` is set.
This gate is what CI and `just openapi-smoke` use to refuse a document that
claims an unmounted route or is not 3.1.

Usage:
    ops/check-openapi.py PATH.json
    ops/check-openapi.py --self-test
"""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path


REQUIRED_METHODS = {
    "/api/v1/openapi.json": {"get"},
    "/api/v1/namespaces": {"get", "post"},
    "/api/v1/namespaces/{ns}": {"get", "put", "delete"},
    "/api/v1/namespaces/{ns}/budgets/{period}": {"get", "put"},
    "/api/v1/namespaces/{ns}/usage": {"get"},
    "/api/v1/providers/{id}/models": {"get"},
    "/api/v1/providers/models": {"get"},
}

HTTP_METHODS = frozenset({"get", "post", "put", "patch", "delete", "head", "options"})


def check(spec: dict) -> list[str]:
    failures: list[str] = []
    version = spec.get("openapi")
    if not isinstance(version, str) or not version.startswith("3.1"):
        failures.append(f"openapi version must start with 3.1, got {version!r}")

    paths = spec.get("paths")
    if not isinstance(paths, dict):
        failures.append("spec is missing paths")
        return failures

    for path, methods in REQUIRED_METHODS.items():
        item = paths.get(path)
        if not isinstance(item, dict):
            failures.append(f"missing path {path}")
            continue
        present = {method for method in item if method.lower() in HTTP_METHODS}
        required = {method.lower() for method in methods}
        for method in sorted(required - present):
            failures.append(f"missing {method.upper()} {path}")
        for method in sorted(present - required):
            failures.append(f"unexpected {method.upper()} {path}")

    for path in paths:
        if path not in REQUIRED_METHODS:
            failures.append(f"unexpected path {path}")

    usage = paths.get("/api/v1/namespaces/{ns}/usage", {}).get("get", {})
    params = usage.get("parameters") if isinstance(usage, dict) else None
    period = None
    if isinstance(params, list):
        period = next((p for p in params if p.get("name") == "period"), None)
    if not isinstance(period, dict):
        failures.append("GET .../usage is missing the period query parameter")
    else:
        if period.get("in") != "query":
            failures.append("period must be a query parameter")
        if period.get("required") is not True:
            failures.append("period query must be required")

    scheme = (
        spec.get("components", {}).get("securitySchemes", {}).get("gateway_key")
        if isinstance(spec.get("components"), dict)
        else None
    )
    if not isinstance(scheme, dict):
        failures.append("missing components.securitySchemes.gateway_key")
    else:
        if scheme.get("type") != "http" or scheme.get("scheme") != "bearer":
            failures.append(f"gateway_key must be HTTP bearer, got {scheme}")

    return failures


def self_test() -> int:
    good = {
        "openapi": "3.1.0",
        "paths": {
            path: {method: {} for method in methods}
            for path, methods in REQUIRED_METHODS.items()
        },
        "components": {
            "securitySchemes": {"gateway_key": {"type": "http", "scheme": "bearer"}}
        },
    }
    good["paths"]["/api/v1/namespaces/{ns}/usage"] = {
        "get": {
            "parameters": [{"name": "period", "in": "query", "required": True}]
        }
    }
    assert check(good) == [], check(good)

    bad = json.loads(json.dumps(good))
    bad["openapi"] = "3.0.3"
    bad["paths"]["/api/v1/providers"] = {"get": {}}
    del bad["components"]["securitySchemes"]["gateway_key"]
    found = check(bad)
    assert any("3.1" in f for f in found), found
    assert any("unexpected path" in f and "/api/v1/providers" in f for f in found), found
    assert any("gateway_key" in f for f in found), found

    missing_discovery = json.loads(json.dumps(good))
    del missing_discovery["paths"]["/api/v1/providers/models"]
    found = check(missing_discovery)
    assert any(
        "missing path" in f and "/api/v1/providers/models" in f for f in found
    ), found

    missing_delete = json.loads(json.dumps(good))
    del missing_delete["paths"]["/api/v1/namespaces/{ns}"]["delete"]
    found = check(missing_delete)
    assert any(
        "missing DELETE" in f and "/api/v1/namespaces/{ns}" in f for f in found
    ), found

    extra_path = json.loads(json.dumps(good))
    extra_path["paths"]["/api/v1/extra"] = {"get": {}}
    found = check(extra_path)
    assert any("unexpected path" in f and "/api/v1/extra" in f for f in found), found

    extra_method = json.loads(json.dumps(good))
    extra_method["paths"]["/api/v1/namespaces"]["patch"] = {}
    found = check(extra_method)
    assert any("unexpected PATCH" in f and "/api/v1/namespaces" in f for f in found), found

    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as handle:
        json.dump(good, handle)
        path = handle.name
    try:
        loaded = json.loads(Path(path).read_text(encoding="utf-8"))
        assert check(loaded) == []
    finally:
        Path(path).unlink(missing_ok=True)

    print("check-openapi: self-test passed")
    return 0


def main(argv: list[str]) -> int:
    if argv[1:] == ["--self-test"]:
        return self_test()
    if len(argv) != 2:
        print("usage: ops/check-openapi.py PATH.json", file=sys.stderr)
        return 2
    path = Path(argv[1])
    spec = json.loads(path.read_text(encoding="utf-8"))
    failures = check(spec)
    if failures:
        for failure in failures:
            print(f"{path}: {failure}", file=sys.stderr)
        return 1
    print(f"check-openapi: {path} is OpenAPI 3.1 and covers the mounted /api/v1 routes")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
