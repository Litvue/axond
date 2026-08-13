#!/usr/bin/env python3
"""Fast, dependency-free documentation drift checks.

Usage:
    ops/check-docs.py              # every drift check against the committed tree
    ops/check-docs.py --self-test  # only the release-path gates' own regressions
"""

from __future__ import annotations

import re
import sys
import tempfile
from pathlib import Path
from urllib.parse import unquote


ROOT = Path(__file__).resolve().parent.parent


def markdown_files() -> list[Path]:
    files = [
        ROOT / "README.md",
        ROOT / "RELEASE.md",
        ROOT / "CONTRIBUTING.md",
        ROOT / "SECURITY.md",
    ]
    files.extend((ROOT / "docs").rglob("*.md"))
    files.extend((ROOT / "crates").rglob("README.md"))
    files.extend((ROOT / "tests").rglob("README.md"))
    files.append(ROOT / "fuzz/README.md")
    # Only committed documentation is ours to check; an installed dependency
    # tree (the TypeScript compatibility lane's `node_modules`) is not.
    return sorted(
        path for path in set(files) if "node_modules" not in path.relative_to(ROOT).parts
    )


def check_relative_links(files: list[Path]) -> list[str]:
    failures: list[str] = []
    link_pattern = re.compile(r"!?(?:\[[^]]*\])\(([^)]+)\)")
    for source in files:
        text = source.read_text(encoding="utf-8")
        for raw_target in link_pattern.findall(text):
            target = raw_target.strip().strip("<>")
            if not target or target.startswith(("#", "http://", "https://", "mailto:")):
                continue
            path_text = unquote(target.split("#", 1)[0].split("?", 1)[0])
            if not path_text:
                continue
            resolved = (source.parent / path_text).resolve()
            if not resolved.exists():
                failures.append(
                    f"{source.relative_to(ROOT)}: missing relative link target {target!r}"
                )
    return failures


def workspace_version() -> str:
    cargo = (ROOT / "Cargo.toml").read_text(encoding="utf-8")
    match = re.search(
        r"\[workspace\.package\]\s+version\s*=\s*\"([^\"]+)\"", cargo
    )
    if not match:
        raise RuntimeError("could not read workspace.package.version")
    return match.group(1)


def check_release_markers() -> list[str]:
    version = workspace_version()
    failures: list[str] = []
    candidates = [ROOT / "README.md", ROOT / "docker-compose.yml"]
    candidates.extend((ROOT / "docs").rglob("*"))
    candidates.extend((ROOT / "deploy").rglob("*"))
    found = 0
    for path in candidates:
        if not path.is_file():
            continue
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if "x-release-please-version" not in line:
                continue
            found += 1
            if version not in line:
                failures.append(
                    f"{path.relative_to(ROOT)}:{number}: release marker does not contain {version}"
                )
    if found < 7:
        failures.append(f"expected at least 7 release-managed documentation markers, found {found}")
    return failures


def check_stale_claims(files: list[Path]) -> list[str]:
    forbidden = {
        "open dev mode": "authentication has no open/keyless mode",
        "future Tier 1 `jti` denylist": "precise JTI revocation has shipped",
        "Precise per-token revocation remains future": "precise revocation has shipped",
        "the upload itself is untested": "crates.io publication has shipped",
        "no version has been published yet": "crates.io publication has shipped",
        "`POST /v1/responses`), `400 unsupported_wire`": "Responses is implemented",
        "the stateful surface is not implemented yet": "stateful mode parsing and bootstrap validation have shipped",
        "no stateful mode, `/admin/v1` route, or durable schema ships yet": "stateful bootstrap configuration ships; the control plane does not",
    }
    failures: list[str] = []
    for source in files + [
        ROOT / "axond.example.toml",
        ROOT / "axond.stateful.example.toml",
        ROOT / ".agents/skills/testing-axond/SKILL.md",
    ]:
        text = source.read_text(encoding="utf-8")
        for phrase, reason in forbidden.items():
            if phrase in text:
                failures.append(
                    f"{source.relative_to(ROOT)}: stale phrase {phrase!r} ({reason})"
                )
    return failures


def check_route_contract() -> list[str]:
    source = (ROOT / "crates/gateway/src/routes.rs").read_text(encoding="utf-8")
    documented = (ROOT / "docs/compatibility.md").read_text(encoding="utf-8")
    registered = set(re.findall(r'path:\s*"(/[^"]+)"', source))
    return [
        f"docs/compatibility.md: registered route {route!r} is not documented"
        for route in sorted(registered)
        if route not in documented
    ]


def check_operating_mode_contract() -> list[str]:
    """Every stateful bootstrap key the parser accepts is documented and shown.

    Bootstrap is the only surface a stateful operator can edit, so an
    undocumented key there is worse than an undocumented inference key: there is
    no `/admin/v1` alternative for it.
    """
    source = (ROOT / "crates/gateway/src/config.rs").read_text(encoding="utf-8")
    reference = (ROOT / "docs/configuration.md").read_text(encoding="utf-8")
    example = (ROOT / "axond.stateful.example.toml").read_text(encoding="utf-8")
    failures: list[str] = []
    sections = (
        ("[control_plane]", "ControlPlane"),
        ("[secret_store]", "SecretStore"),
        ("[[admin_breakglass]]", "AdminBreakglass"),
    )
    for section, struct in sections:
        body = re.search(rf"pub struct {struct} \{{(.*?)\n\}}", source, re.DOTALL)
        if body is None:
            failures.append(f"crates/gateway/src/config.rs: {struct} not found")
            continue
        for field in re.findall(r"\n    pub (\w+):", body.group(1)):
            if f"`{field}`" not in reference:
                failures.append(
                    f"docs/configuration.md: {section} key {field!r} is not documented"
                )
        for path, text in (("docs/configuration.md", reference), ("axond.stateful.example.toml", example)):
            if section not in text:
                failures.append(f"{path}: stateful bootstrap section {section} is missing")
    for mode in ("stateless", "stateful"):
        if f'mode = "{mode}"' not in reference:
            failures.append(f"docs/configuration.md: `mode = \"{mode}\"` is not documented")
    return failures


def check_msrv_documented() -> list[str]:
    """The published MSRV policy names the versions the manifests actually pin.

    `ops/msrv-gate.sh` keeps the manifests, the Dockerfile, and the pinned
    toolchain consistent with each other; this keeps the operator-facing
    statement of the policy consistent with them.
    """
    cargo = (ROOT / "Cargo.toml").read_text(encoding="utf-8")
    msrv = re.search(r"^rust-version\s*=\s*\"([^\"]+)\"", cargo, re.MULTILINE)
    if msrv is None:
        return ["Cargo.toml: [workspace.package] rust-version is not declared"]
    pinned = re.search(
        r"^channel\s*=\s*\"([^\"]+)\"",
        (ROOT / "rust-toolchain.toml").read_text(encoding="utf-8"),
        re.MULTILINE,
    )
    if pinned is None:
        return ["rust-toolchain.toml: no [toolchain] channel is pinned"]

    heading = "### The Rust version floor (MSRV)"
    document = (ROOT / "docs/compatibility.md").read_text(encoding="utf-8")
    if heading not in document:
        return [f"docs/compatibility.md: {heading!r} section is missing"]
    start = document.index(heading)
    end = document.find("\n### ", start + len(heading))
    section = document[start : end if end != -1 else len(document)]
    return [
        f"docs/compatibility.md: the MSRV section does not mention {value!r}"
        for value in (msrv.group(1), pinned.group(1))
        if value not in section
    ]


def workflow_job_targets(workflow: str, job: str) -> list[str]:
    text = (ROOT / ".github/workflows" / workflow).read_text(encoding="utf-8")
    return matrix_targets(text, job)


def matrix_targets(workflow_text: str, job: str) -> list[str]:
    block = re.search(
        rf"^  {re.escape(job)}:\n(.*?)(?=^  \S|\Z)",
        workflow_text,
        re.MULTILINE | re.DOTALL,
    )
    if block is None:
        return []
    return re.findall(r"^\s+target:\s*(\S+)\s*$", block.group(1), re.MULTILINE)


def smoke_matrix_failures(
    smoked: list[str], released: list[str], document: str
) -> list[str]:
    """The published, the booted, and the documented target sets are one set.

    A published target that is only compiled is a weaker promise than one that is
    booted and served, and the difference is invisible from the documentation
    unless it is checked. So the release `binaries` matrix, the `binary-smoke`
    matrix, and the platform table must name the same targets: adding a target to
    the release without a smoke lane fails here, exactly like smoking a target
    that is never published.
    """
    if not smoked:
        return ["ci.yml: the binary-smoke matrix declares no target"]
    if not released:
        return ["release-please.yml: the release-binaries matrix declares no target"]
    failures: list[str] = []
    for target in smoked:
        if target not in released:
            failures.append(
                f"ci.yml: binary-smoke covers {target!r}, which the release "
                "binaries matrix does not publish"
            )
    for target in released:
        if target not in smoked:
            failures.append(
                f"release-please.yml: released target {target!r} has no "
                "binary-smoke lane in ci.yml; a published binary that is never "
                "booted is not covered by the documented smoke matrix"
            )
    for target in sorted(set(smoked) | set(released)):
        if f"`{target}`" not in document:
            failures.append(
                f"docs/compatibility.md: release target {target!r} is not documented"
            )
    if "binary-smoke" not in document:
        failures.append(
            "docs/compatibility.md: the platform matrix does not name the "
            "`binary-smoke` lane that exercises it"
        )
    return failures


def check_smoke_matrix() -> list[str]:
    return smoke_matrix_failures(
        workflow_job_targets("ci.yml", "binary-smoke"),
        workflow_job_targets("release-please.yml", "release-binaries"),
        (ROOT / "docs/compatibility.md").read_text(encoding="utf-8"),
    )


def release_script_trigger_failures(workflow_text: str, page: str) -> list[str]:
    """Every script the release path runs is named by the trigger that owns it.

    Trigger 6 is what tells a contributor that touching the release path owes a
    review, and it can only do that if it names the scripts that path runs: a
    script the release workflow invokes but the page omits is load-bearing for
    what gets signed while looking like an ordinary file.
    """
    return [
        f"docs/security/threat-model-review.md: {script!r} runs in the release "
        "workflow but trigger 6 does not name it"
        for script in sorted(set(re.findall(r"ops/[\w.-]+\.(?:py|sh)", workflow_text)))
        if f"`{script}`" not in page
    ]


def check_release_script_triggers() -> list[str]:
    return release_script_trigger_failures(
        (ROOT / ".github/workflows/release-please.yml").read_text(encoding="utf-8"),
        (ROOT / "docs/security/threat-model-review.md").read_text(encoding="utf-8"),
    )


def self_test() -> int:
    """Prove the release-path gates fail when they should, not only pass.

    The gate's whole value is that it fails; a check that only ever passes on the
    committed tree would be indistinguishable from no check at all.
    """
    workflow = (
        "jobs:\n"
        "  binary-smoke:\n"
        "    strategy:\n"
        "      matrix:\n"
        "        include:\n"
        "          - os: ubuntu-latest\n"
        "            target: gnu\n"
        "          - os: macos-14\n"
        "            target: mac\n"
        "  other-job:\n"
        "    steps:\n"
        "      - run: cargo build --target not-a-matrix-entry\n"
    )
    assert matrix_targets(workflow, "binary-smoke") == ["gnu", "mac"]
    assert matrix_targets(workflow, "absent-job") == []

    document = "`gnu` and `mac` are released and the `binary-smoke` lane boots them"
    assert smoke_matrix_failures(["gnu", "mac"], ["gnu", "mac"], document) == []

    # A released target with no smoke lane: the finding this gate exists for.
    unsmoked = smoke_matrix_failures(["gnu"], ["gnu", "mac"], document)
    assert len(unsmoked) == 1, unsmoked
    assert "released target 'mac' has no binary-smoke lane" in unsmoked[0], unsmoked

    # And the converse, plus an undocumented target, still fail.
    unpublished = smoke_matrix_failures(["gnu", "mac"], ["gnu"], document)
    assert len(unpublished) == 1, unpublished
    assert "does not publish" in unpublished[0], unpublished
    undocumented = smoke_matrix_failures(["gnu"], ["gnu"], "`binary-smoke`")
    assert undocumented == [
        "docs/compatibility.md: release target 'gnu' is not documented"
    ], undocumented
    unnamed_lane = smoke_matrix_failures(["gnu"], ["gnu"], "`gnu`")
    assert len(unnamed_lane) == 1 and "binary-smoke` lane" in unnamed_lane[0]

    for empty in (
        smoke_matrix_failures([], ["gnu"], document),
        smoke_matrix_failures(["gnu"], [], document),
    ):
        assert len(empty) == 1 and "declares no target" in empty[0], empty

    named = release_script_trigger_failures(
        "run: ops/docker-smoke.sh\nrun: python ops/binary-smoke.py x\n",
        "trigger 6 fires on `ops/docker-smoke.sh` and `ops/binary-smoke.py`",
    )
    assert named == [], named
    omitted = release_script_trigger_failures(
        "run: python ops/binary-smoke.py release-bin/axond\n",
        "trigger 6 fires on `ops/docker-smoke.sh`",
    )
    assert len(omitted) == 1, omitted
    assert "'ops/binary-smoke.py' runs in the release workflow" in omitted[0], omitted

    with tempfile.TemporaryDirectory() as raw:
        records = Path(raw)
        (records / "0031-first.md").write_text("# 31. First\n", encoding="utf-8")
        assert check_adr_numbering(records) == []
        # The collision this gate exists for: two branches both took 0031.
        (records / "0031-second.md").write_text("# 31. Second\n", encoding="utf-8")
        collision = check_adr_numbering(records)
        assert len(collision) == 1, collision
        assert "ADR 0031 is already" in collision[0], collision
        # A renamed file whose heading kept the old number is just as ambiguous.
        (records / "0031-second.md").unlink()
        (records / "0032-renumbered.md").write_text("# 31. First\n", encoding="utf-8")
        heading = check_adr_numbering(records)
        assert len(heading) == 1 and "does not start with" in heading[0], heading
        # A malformed name or an empty record is reported, not raised: a gate that
        # tracebacks tells a contributor nothing about what to rename.
        (records / "0032-renumbered.md").unlink()
        (records / "003-short.md").write_text("# 3. Short\n", encoding="utf-8")
        short = check_adr_numbering(records)
        assert len(short) == 1 and "four-digit ADR number" in short[0], short
        (records / "003-short.md").unlink()
        (records / "0032-empty.md").write_text("", encoding="utf-8")
        empty_record = check_adr_numbering(records)
        assert len(empty_record) == 1, empty_record
        assert "does not start with" in empty_record[0], empty_record

    package = '[package]\nname = "axond"\n\n[[bin]]\nname = "axond"\n'
    distinct = f'{package}\n[lib]\nname = "axond_fuzz_seam"\n'
    assert documented_target_failures("m", distinct) == []
    assert documented_target_failures("m", package) == []
    # The clash, spelled out and — the easier one to reintroduce — defaulted to
    # the package name by leaving `[lib] name` off entirely.
    for clashing in (f'{package}\n[lib]\nname = "axond"\n', f"{package}\n[lib]\n"):
        shadowed = documented_target_failures("m", clashing)
        assert len(shadowed) == 1, shadowed
        assert "after a binary of the same crate" in shadowed[0], shadowed

    print("check-docs: release-path gate self-test passed")
    return 0


def check_front_door_size() -> list[str]:
    failures: list[str] = []
    for relative, limit in (("README.md", 260), ("docs/deployment.md", 220)):
        count = len((ROOT / relative).read_text(encoding="utf-8").splitlines())
        if count > limit:
            failures.append(f"{relative}: {count} lines exceeds front-door limit {limit}")
    return failures


def check_review_trigger_tests() -> list[str]:
    """Every test the threat-model trigger page names still exists.

    The page is only useful if its named tests are the *real* floor, and a
    documented test name rots silently: a rename in the crates leaves the page
    pointing at coverage nobody has. Every backticked lowercase identifier with
    three or more underscores in it is treated as a test function and must be
    declared somewhere under `crates/`. Shorter identifiers are configuration
    keys and field names (`allow_platform_fallback`, `credential_source`), which
    other checks and the configuration reference already cover.
    """
    page = ROOT / "docs/security/threat-model-review.md"
    if not page.is_file():
        return [
            "docs/security/threat-model-review.md: the trigger page is missing; "
            "the security, deployment, contributor, and release docs link to it"
        ]
    text = page.read_text(encoding="utf-8")
    declared = set()
    for source in (ROOT / "crates").rglob("*.rs"):
        declared.update(
            re.findall(r"\bfn\s+([a-z0-9_]+)", source.read_text(encoding="utf-8"))
        )
    named = {
        candidate
        for candidate in re.findall(r"`([a-z0-9_]+)`", text)
        if candidate.count("_") >= 3
    }
    failures = [
        f"docs/security/threat-model-review.md: named test {name!r} does not exist under crates/"
        for name in sorted(named - declared)
    ]
    if len(named) < 40:
        failures.append(
            "docs/security/threat-model-review.md: only "
            f"{len(named)} named tests remain; the triggers are meant to name the "
            "existing floor for each area"
        )
    return failures


def documented_target_failures(relative: str, text: str) -> list[str]:
    """The library-versus-binary name clash in one manifest, if it has one."""
    library = re.search(r"^\[lib\]$(.*?)(?=^\[|\Z)", text, re.M | re.S)
    if not library:
        return []
    named = re.search(r'^name = "([^"]+)"', library.group(1), re.M)
    if named:
        name = named.group(1)
    else:
        # An omitted `[lib] name` defaults to the package name, which for a
        # package whose binary is named after it *is* the clash. Resolve it the
        # way Cargo does rather than skipping the manifest.
        package = re.search(r"^\[package\]$(.*?)(?=^\[|\Z)", text, re.M | re.S)
        packaged = (
            re.search(r'^name = "([^"]+)"', package.group(1), re.M) if package else None
        )
        if not packaged:
            return [f"{relative}: neither `[package]` nor `[lib]` names the library"]
        name = packaged.group(1)
    binaries = [
        match.group(1)
        for match in re.finditer(
            r"^\[\[bin\]\]$.*?^name = \"([^\"]+)\"", text, re.M | re.S
        )
    ]
    if name not in binaries:
        return []
    return [
        f"{relative}: the library is named {name!r} after a binary of the same "
        "crate, so `cargo doc` documents only one of them and the other's modules "
        "are no longer rustdoc-linted"
    ]


def check_documented_targets() -> list[str]:
    """No crate names a library after one of its own binaries.

    `cargo doc` documents one target per crate name, so a library sharing a
    binary's name quietly takes its place: `target/doc/axond/` becomes the
    library's, every module under the binary stops being rustdoc-linted, and the
    `Documentation` lane keeps passing with `RUSTDOCFLAGS=-D warnings` covering
    nothing. Marking the library `doc = false` does not help — it removes the
    whole crate from the documentation instead. Only distinct names keep both.
    """
    failures: list[str] = []
    for manifest in sorted((ROOT / "crates").glob("*/Cargo.toml")):
        failures.extend(
            documented_target_failures(
                str(manifest.relative_to(ROOT)),
                manifest.read_text(encoding="utf-8"),
            )
        )
    return failures


def check_adr_numbering(directory: Path | None = None) -> list[str]:
    """One decision per ADR number, and the heading agrees with the filename.

    Two records sharing a number makes every "ADR 00NN" reference ambiguous, and
    the collision is easy to create: the number is chosen when the branch is cut,
    not when it merges.
    """
    failures: list[str] = []
    by_number: dict[str, Path] = {}
    for record in sorted((directory or ROOT / "docs/adr").glob("[0-9]*.md")):
        number = record.name[:4]
        if not number.isdigit():
            failures.append(
                f"docs/adr/{record.name}: the filename must start with a "
                "four-digit ADR number"
            )
            continue
        first = by_number.setdefault(number, record)
        if first is not record:
            failures.append(
                f"docs/adr/{record.name}: ADR {number} is already "
                f"{first.name}; renumber one of them"
            )
        lines = record.read_text(encoding="utf-8").splitlines()
        heading = lines[0] if lines else ""
        if not heading.startswith(f"# {int(number)}. "):
            failures.append(
                f"docs/adr/{record.name}: heading {heading!r} does not start with "
                f"'# {int(number)}. '"
            )
    return failures


def main(argv: list[str]) -> int:
    if argv == ["--self-test"]:
        return self_test()
    if argv:
        raise SystemExit(f"usage: check-docs.py [--self-test], not {' '.join(argv)}")
    # Every run proves the gate still fails when it should, so a refactor that
    # neuters it is caught by the same lane that relies on it.
    self_test()
    files = markdown_files()
    failures = []
    failures.extend(check_relative_links(files))
    failures.extend(check_release_markers())
    failures.extend(check_operating_mode_contract())
    failures.extend(check_stale_claims(files))
    failures.extend(check_route_contract())
    failures.extend(check_msrv_documented())
    failures.extend(check_smoke_matrix())
    failures.extend(check_release_script_triggers())
    failures.extend(check_review_trigger_tests())
    failures.extend(check_front_door_size())
    failures.extend(check_documented_targets())
    failures.extend(check_adr_numbering())
    if failures:
        for failure in failures:
            print(f"documentation check failed: {failure}", file=sys.stderr)
        return 1
    print(f"documentation checks passed ({len(files)} Markdown files)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
