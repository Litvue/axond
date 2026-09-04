#!/usr/bin/env python3
"""Assemble a self-contained axond crate for crates.io.

The git workspace keeps `gateway-core` and `gateway-transport` as unpublished
members so `cargo test -p gateway-core` still works. crates.io does not accept
path dependencies, and those members are not published, so the registry tarball
cannot list them as Cargo deps.

This script copies both internals under `src/gateway_core/` and
`src/gateway_transport/`, rewrites `crate::` / `gateway_core::` paths so they
resolve as modules of `axond`, injects the module declarations, and writes a
standalone `Cargo.toml` whose third-party deps are the union of the three
crates. The published package then compiles without resolving the internals
from the registry.

Usage:
    ops/stage-axond-crate.py --out DIR
    ops/stage-axond-crate.py --self-test
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
INTERNAL = ("gateway-core", "gateway-transport")
INTERNAL_MOD = {
    "gateway-core": "gateway_core",
    "gateway-transport": "gateway_transport",
}
EXTERN_REWRITE = ("gateway_core", "gateway_transport")

INJECT = (
    '#[allow(dead_code, unused_imports)]\n'
    '#[path = "gateway_core/lib.rs"]\n'
    "mod gateway_core;\n"
    '#[allow(dead_code, unused_imports)]\n'
    '#[path = "gateway_transport/lib.rs"]\n'
    "mod gateway_transport;\n"
    "\n"
)


def rewrite_crate_paths(text: str, module: str) -> str:
    """Point `crate::` inside a bundled internal crate at its new module."""
    return text.replace("crate::", f"crate::{module}::")


def rewrite_extern_paths(text: str, crate_mod: str) -> str:
    """Point an extern-crate path at the bundled module, without doubling."""
    return re.sub(
        rf"(?<!crate::)\b{re.escape(crate_mod)}::",
        f"crate::{crate_mod}::",
        text,
    )


def rewrite_internal_source(text: str, package: str) -> str:
    module = INTERNAL_MOD[package]
    if package == "gateway-transport":
        text = rewrite_extern_paths(text, "gateway_core")
        text = re.sub(
            r"crate::(?!gateway_core::|gateway_transport::)",
            "crate::gateway_transport::",
            text,
        )
        return text
    return rewrite_crate_paths(text, module)


def rewrite_axond_source(text: str) -> str:
    for crate_mod in EXTERN_REWRITE:
        text = rewrite_extern_paths(text, crate_mod)
    return text


def inject_internal_mods(text: str) -> str:
    """Declare the bundled internals as the first crate items.

    Inner attributes and crate docs must stay first. Outer attributes on the
    original first `mod` must stay attached to that `mod`, not to the inject.
    """
    lines = text.splitlines(keepends=True)
    index = 0
    while index < len(lines):
        stripped = lines[index].strip()
        if (
            not stripped
            or stripped.startswith("//")
            or stripped.startswith("//!")
            or stripped.startswith("#![")
        ):
            index += 1
            continue
        break
    if index >= len(lines):
        raise SystemExit("no crate item to inject bundled internals before")
    return "".join(lines[:index]) + INJECT + "".join(lines[index:])


def cargo_metadata() -> dict:
    completed = subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--locked"],
        cwd=ROOT,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if completed.returncode != 0:
        raise SystemExit(f"cargo metadata failed:\n{completed.stderr.strip()}")
    return json.loads(completed.stdout)


def workspace_packages(metadata: dict) -> dict[str, dict]:
    members = set(metadata["workspace_members"])
    return {
        package["name"]: package
        for package in metadata["packages"]
        if package["id"] in members
    }


def pin_resolved_versions(metadata: dict, deps: dict[str, dict], package_names: list[str]) -> None:
    """Replace caret reqs with the exact versions this workspace already locked."""
    packages_by_id = {package["id"]: package for package in metadata["packages"]}
    nodes_by_id = {node["id"]: node for node in metadata["resolve"]["nodes"]}
    resolved: dict[str, str] = {}
    for package in workspace_packages(metadata).values():
        if package["name"] not in package_names:
            continue
        node = nodes_by_id[package["id"]]
        for dep in node.get("deps", []):
            kinds = dep.get("dep_kinds")
            if kinds is not None and all(
                kind.get("kind") not in (None, "normal") for kind in kinds
            ):
                continue
            pkg = packages_by_id[dep["pkg"]]
            if pkg["name"] in INTERNAL or not pkg.get("source"):
                continue
            previous = resolved.get(pkg["name"])
            if previous is not None and previous != pkg["version"]:
                raise SystemExit(
                    f"resolved {pkg['name']} at both {previous} and {pkg['version']}"
                )
            resolved[pkg["name"]] = pkg["version"]
    missing = sorted(set(deps) - set(resolved))
    if missing:
        raise SystemExit(f"no locked version for direct deps: {', '.join(missing)}")
    for name, dep in deps.items():
        dep["req"] = f"={resolved[name]}"


def merge_registry_deps(packages: list[dict]) -> dict[str, dict]:
    """Union normal registry deps; skip unpublished workspace path crates."""
    merged: dict[str, dict] = {}
    for package in packages:
        for dep in package["dependencies"]:
            if dep.get("kind") not in (None, "normal"):
                continue
            if dep.get("target") is not None:
                raise SystemExit(
                    f"{package['name']} has a target-specific dependency "
                    f"{dep['name']}; the stager does not emit [target] tables"
                )
            if dep.get("rename"):
                raise SystemExit(
                    f"{package['name']} renames {dep['name']} to {dep['rename']}; "
                    "the stager does not emit package/rename keys"
                )
            if dep["name"] in INTERNAL or dep.get("source") is None:
                continue
            features = set(dep.get("features") or [])
            current = merged.get(dep["name"])
            if current is None:
                merged[dep["name"]] = {
                    "req": dep["req"],
                    "features": features,
                    "uses_default_features": bool(dep["uses_default_features"]),
                    "optional": bool(dep["optional"]),
                }
                continue
            if current["req"] != dep["req"]:
                raise SystemExit(
                    f"dependency {dep['name']} has conflicting version "
                    f"requirements {current['req']!r} and {dep['req']!r}"
                )
            current["features"] |= features
            current["uses_default_features"] = (
                current["uses_default_features"] or bool(dep["uses_default_features"])
            )
            current["optional"] = current["optional"] and bool(dep["optional"])
    return merged


def toml_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def emit_string_array(values: list[str]) -> str:
    return "[" + ", ".join(toml_string(value) for value in values) + "]"


def emit_dependency(name: str, dep: dict) -> str:
    features = sorted(dep["features"])
    simple = (
        dep["uses_default_features"]
        and not features
        and not dep["optional"]
    )
    if simple:
        return f"{name} = {toml_string(dep['req'])}\n"
    parts = [f"version = {toml_string(dep['req'])}"]
    if not dep["uses_default_features"]:
        parts.append("default-features = false")
    if features:
        parts.append(f"features = {emit_string_array(features)}")
    if dep["optional"]:
        parts.append("optional = true")
    return f"{name} = {{ {', '.join(parts)} }}\n"


def emit_cargo_toml(axond: dict, deps: dict[str, dict]) -> str:
    lines = [
        "[package]\n",
        f"name = {toml_string(axond['name'])}\n",
        f"version = {toml_string(axond['version'])}\n",
        f"edition = {toml_string(axond['edition'])}\n",
        f"rust-version = {toml_string(axond['rust_version'])}\n",
        f"license = {toml_string(axond['license'])}\n",
        f"repository = {toml_string(axond['repository'])}\n",
        f"homepage = {toml_string(axond['homepage'])}\n",
        "readme = \"README.md\"\n",
        f"description = {toml_string(axond['description'])}\n",
        f"keywords = {emit_string_array(axond['keywords'])}\n",
        f"categories = {emit_string_array(axond['categories'])}\n",
        "include = [\"src/**/*.rs\", \"src/**/*.json\", \"sql/*.sql\"]\n",
        "\n",
        "[[bin]]\n",
        "name = \"axond\"\n",
        "path = \"src/main.rs\"\n",
        "\n",
        "[lib]\n",
        "name = \"axond_fuzz_seam\"\n",
        "path = \"src/fuzz_seam.rs\"\n",
        "test = false\n",
        "doctest = false\n",
        "\n",
        "[lints.rust]\n",
        "unexpected_cfgs = { level = \"warn\", check-cfg = ['cfg(fuzzing)'] }\n",
        "\n",
        "[dependencies]\n",
    ]
    for name in sorted(deps):
        lines.append(emit_dependency(name, deps[name]))
    text = "".join(lines)
    for forbidden in INTERNAL:
        if re.search(rf"^{re.escape(forbidden)}\s*=", text, re.MULTILINE):
            raise SystemExit(
                f"staged Cargo.toml still depends on unpublished {forbidden}"
            )
    return text


def copy_rust_tree(src: Path, dst: Path, rewrite) -> None:
    for path in src.rglob("*"):
        relative = path.relative_to(src)
        target = dst / relative
        if path.is_dir():
            target.mkdir(parents=True, exist_ok=True)
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        if path.suffix == ".rs":
            target.write_text(rewrite(path.read_text(encoding="utf-8")), encoding="utf-8")
        else:
            shutil.copy2(path, target)


def stage(out: Path) -> None:
    if out.exists():
        if any(out.iterdir()):
            raise SystemExit(f"{out} exists and is not empty")
    else:
        out.mkdir(parents=True)

    metadata = cargo_metadata()
    packages = workspace_packages(metadata)
    missing = [name for name in ("axond", *INTERNAL) if name not in packages]
    if missing:
        raise SystemExit(f"workspace missing packages: {', '.join(missing)}")
    axond = packages["axond"]
    internals = [packages[name] for name in INTERNAL]
    deps = merge_registry_deps([axond, *internals])
    pin_resolved_versions(metadata, deps, ["axond", *INTERNAL])

    (out / "Cargo.toml").write_text(emit_cargo_toml(axond, deps), encoding="utf-8")
    # Direct deps are `=workspace-resolved` so they cannot float. Transitives
    # resolve here; `cargo package --locked` needs this lock, and a rewritten
    # workspace lock is stale once axond no longer depends on path crates.
    completed = subprocess.run(
        ["cargo", "generate-lockfile", "--manifest-path", str(out / "Cargo.toml")],
        cwd=out,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if completed.returncode != 0:
        raise SystemExit(f"cargo generate-lockfile failed:\n{completed.stderr.strip()}")

    src_out = out / "src"
    copy_rust_tree(
        ROOT / "crates/gateway/src",
        src_out,
        rewrite_axond_source,
    )
    copy_rust_tree(
        ROOT / "crates/gateway-core/src",
        src_out / "gateway_core",
        lambda text: rewrite_internal_source(text, "gateway-core"),
    )
    copy_rust_tree(
        ROOT / "crates/gateway-transport/src",
        src_out / "gateway_transport",
        lambda text: rewrite_internal_source(text, "gateway-transport"),
    )
    for root_name in ("main.rs", "fuzz_seam.rs"):
        path = src_out / root_name
        path.write_text(inject_internal_mods(path.read_text(encoding="utf-8")), encoding="utf-8")

    shutil.copytree(ROOT / "crates/gateway/sql", out / "sql")
    shutil.copy2(ROOT / "README.md", out / "README.md")
    for license_name in ("LICENSE-APACHE", "LICENSE-MIT", "NOTICE"):
        source = ROOT / license_name
        if source.exists():
            shutil.copy2(source, out / license_name)

    print(f"staged self-contained axond crate at {out}")


def self_test() -> int:
    core = rewrite_internal_source(
        "use crate::ProviderError;\nfn f(event: crate::SseEvent) {}\nuse crate::{Foo, Bar};\n",
        "gateway-core",
    )
    assert (
        core
        == "use crate::gateway_core::ProviderError;\n"
        "fn f(event: crate::gateway_core::SseEvent) {}\n"
        "use crate::gateway_core::{Foo, Bar};\n"
    ), core

    transport = rewrite_internal_source(
        "use gateway_core::{ProviderAdapter, ProviderError};\n"
        "//! drives a [`gateway_core::ProviderAdapter`]\n"
        "use crate::AlreadyRoot;\n",
        "gateway-transport",
    )
    assert "use crate::gateway_core::{ProviderAdapter, ProviderError};" in transport, transport
    assert "[`crate::gateway_core::ProviderAdapter`]" in transport, transport
    assert "use crate::gateway_transport::AlreadyRoot;" in transport, transport
    assert "crate::gateway_transport::gateway_core" not in transport, transport

    doubled = rewrite_extern_paths("use crate::gateway_core::Foo;\n", "gateway_core")
    assert doubled == "use crate::gateway_core::Foo;\n", doubled

    axond = rewrite_axond_source(
        "use gateway_core::ModelPrice;\n"
        "use gateway_transport::{ByteStream, TransportError};\n"
        "let x = gateway_core::Usage { input_tokens: 1, output_tokens: 0, cached_tokens: None };\n"
        "/// [`CircuitBreaker`](gateway_core::CircuitBreaker)\n"
    )
    assert "use crate::gateway_core::ModelPrice;" in axond, axond
    assert "use crate::gateway_transport::{ByteStream, TransportError};" in axond, axond
    assert "crate::gateway_core::Usage" in axond, axond
    assert "(crate::gateway_core::CircuitBreaker)" in axond, axond

    injected = inject_internal_mods(
        "//! docs\n"
        "#![cfg(fuzzing)]\n"
        "\n"
        "#[allow(dead_code)]\n"
        "mod admin;\n"
        "mod config;\n"
    )
    assert injected.index("#![cfg(fuzzing)]") < injected.index("mod gateway_core;"), injected
    assert injected.index("mod gateway_core;") < injected.index("#[allow(dead_code)]\nmod admin;"), injected
    assert injected.index("mod gateway_transport;") < injected.index("mod admin;"), injected
    assert injected.index("#[allow(dead_code)]\nmod admin;") < injected.index("mod config;"), injected

    dep = emit_dependency(
        "reqwest",
        {
            "req": "^0.12",
            "features": {"json", "stream"},
            "uses_default_features": False,
            "optional": False,
        },
    )
    assert dep.startswith("reqwest = { version = \"^0.12\", default-features = false, features = ["), dep
    assert '"json"' in dep and '"stream"' in dep, dep

    simple = emit_dependency(
        "anyhow",
        {"req": "^1", "features": set(), "uses_default_features": True, "optional": False},
    )
    assert simple == 'anyhow = "^1"\n', simple

    merged = merge_registry_deps(
        [
            {
                "name": "axond",
                "dependencies": [
                    {
                        "name": "gateway-core",
                        "kind": None,
                        "source": None,
                        "req": "*",
                        "features": [],
                        "uses_default_features": True,
                        "optional": False,
                        "target": None,
                        "rename": None,
                    },
                    {
                        "name": "serde_json",
                        "kind": None,
                        "source": "registry+https://github.com/rust-lang/crates.io-index",
                        "req": "^1",
                        "features": ["raw_value"],
                        "uses_default_features": True,
                        "optional": False,
                        "target": None,
                        "rename": None,
                    },
                    {
                        "name": "http-body-util",
                        "kind": "dev",
                        "source": "registry+https://github.com/rust-lang/crates.io-index",
                        "req": "^0.1",
                        "features": [],
                        "uses_default_features": True,
                        "optional": False,
                        "target": None,
                        "rename": None,
                    },
                ],
            },
            {
                "name": "gateway-core",
                "dependencies": [
                    {
                        "name": "serde_json",
                        "kind": None,
                        "source": "registry+https://github.com/rust-lang/crates.io-index",
                        "req": "^1",
                        "features": [],
                        "uses_default_features": True,
                        "optional": False,
                        "target": None,
                        "rename": None,
                    },
                    {
                        "name": "regex",
                        "kind": None,
                        "source": "registry+https://github.com/rust-lang/crates.io-index",
                        "req": "^1",
                        "features": [],
                        "uses_default_features": True,
                        "optional": False,
                        "target": None,
                        "rename": None,
                    },
                ],
            },
        ]
    )
    assert set(merged) == {"serde_json", "regex"}, merged
    assert merged["serde_json"]["features"] == {"raw_value"}, merged
    assert "gateway-core" not in merged

    pin_meta = {
        "workspace_members": ["axond-id", "core-id"],
        "packages": [
            {"id": "axond-id", "name": "axond", "source": None},
            {"id": "core-id", "name": "gateway-core", "source": None},
            {
                "id": "anyhow-id",
                "name": "anyhow",
                "version": "1.0.104",
                "source": "registry+https://github.com/rust-lang/crates.io-index",
            },
            {
                "id": "regex-id",
                "name": "regex",
                "version": "1.13.1",
                "source": "registry+https://github.com/rust-lang/crates.io-index",
            },
        ],
        "resolve": {
            "nodes": [
                {
                    "id": "axond-id",
                    "deps": [
                        {
                            "name": "anyhow",
                            "pkg": "anyhow-id",
                            "dep_kinds": [{"kind": None}],
                        },
                        {
                            "name": "http_body_util",
                            "pkg": "anyhow-id",
                            "dep_kinds": [{"kind": "dev"}],
                        },
                    ],
                },
                {
                    "id": "core-id",
                    "deps": [
                        {"name": "regex", "pkg": "regex-id", "dep_kinds": [{"kind": None}]}
                    ],
                },
            ]
        },
    }
    pinned = {
        "anyhow": {
            "req": "^1",
            "features": set(),
            "uses_default_features": True,
            "optional": False,
        },
        "regex": {
            "req": "^1",
            "features": set(),
            "uses_default_features": True,
            "optional": False,
        },
    }
    pin_resolved_versions(pin_meta, pinned, ["axond", "gateway-core"])
    assert pinned["anyhow"]["req"] == "=1.0.104", pinned
    assert pinned["regex"]["req"] == "=1.13.1", pinned

    fake_axond = {
        "name": "axond",
        "version": "0.5.0",
        "edition": "2024",
        "rust_version": "1.97",
        "license": "Apache-2.0 OR MIT",
        "repository": "https://github.com/Litvue/axond",
        "homepage": "https://github.com/Litvue/axond",
        "description": "gateway",
        "keywords": ["llm"],
        "categories": ["command-line-utilities"],
    }
    manifest = emit_cargo_toml(
        fake_axond,
        {
            "anyhow": {
                "req": "^1",
                "features": set(),
                "uses_default_features": True,
                "optional": False,
            }
        },
    )
    assert "gateway-core" not in manifest
    assert "gateway-transport" not in manifest
    assert 'anyhow = "^1"' in manifest

    print(f"stage-axond-crate: self-test passed on Python {sys.version.split()[0]}")
    return 0


def main(argv: list[str]) -> int:
    if argv == ["--self-test"]:
        return self_test()
    if len(argv) == 2 and argv[0] == "--out":
        stage(Path(argv[1]).resolve())
        return 0
    raise SystemExit("Usage: ops/stage-axond-crate.py --out DIR | --self-test")


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
