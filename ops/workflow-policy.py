#!/usr/bin/env python3
"""Supply-chain policy for the GitHub Actions workflows.

A workflow step that resolves a tag or a branch at run time runs whatever the
upstream owner has pushed there. `actions/checkout@v7` is a moving pointer, and
the actions in this repository can read the release GitHub App token, the
crates.io token, and the release signing identity. So every step is pinned to a
full commit SHA, with the human-readable version kept in a trailing comment, and
this gate is what keeps a new mutable ref from being merged.

The gate is deliberately line-based and dependency-free: it must run on the same
`python3` floor as the other `ops/` checks, with no PyYAML. It reads the shell
under `ops/` as well, because that is where the release's `cosign verify` calls
live and an unrestricted one there weakens exactly what a workflow-level check
would be protecting.

Usage:
    ops/workflow-policy.py              # check the workflows in this repository
    ops/workflow-policy.py --self-test  # prove the gate rejects what it claims to
"""

from __future__ import annotations

import re
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent

USES = re.compile(r"^\s*(?:-\s*)?uses:\s*(?P<ref>[^\s#]+)\s*(?:#\s*(?P<comment>.*?))?\s*$")
PINNED = re.compile(r"^(?P<action>[^@]+)@(?P<sha>[0-9a-f]{40})$")
# A readable pin comment names the released version (`v1.2.3`, `v0.24.0`) or the
# upstream branch the SHA was taken from (`stable`, for actions that publish no
# usable release tag).
VERSION_COMMENT = re.compile(r"^(?:v\d+(?:\.\d+)*(?:-[0-9A-Za-z.]+)?|stable|master|main)$")
# `^https://github\.com/.../release-please\.yml@(refs/heads/main|refs/tags/…)$`:
# the signer identity has to stay anchored at both ends, or a modified copy of
# the release workflow running from another ref would satisfy it.
ANCHORED_IDENTITY = re.compile(r"^\s*SIGNER_IDENTITY:\s*\^.+\$\s*$")


def workflows(root: Path) -> list[Path]:
    directory = root / ".github" / "workflows"
    return sorted(p for p in directory.glob("*.y*ml") if p.is_file())


def verifier_scripts(root: Path) -> list[Path]:
    """The shell the workflows call, where the `cosign verify` invocations live.

    A workflow step that runs `ops/verify-image-evidence.sh` is as much part of
    the release's signature verification as an inline `run:` block, so the
    identity restriction has to be checked where the command actually is.
    """
    return sorted(p for p in (root / "ops").glob("*.sh") if p.is_file())


def check_pins(text: str, relative: str) -> tuple[list[str], list[tuple[str, str, str, str]]]:
    """Return the failures and every `(action, sha, comment, where)` pin found.

    Every occurrence is kept, not one per action: two steps in the same file
    disagreeing about an action's pin is the same problem as two files doing so.
    """
    failures: list[str] = []
    pins: list[tuple[str, str, str, str]] = []
    for number, line in enumerate(text.splitlines(), 1):
        match = USES.match(line)
        if match is None:
            continue
        ref = match.group("ref").strip("\"'")
        comment = (match.group("comment") or "").strip()
        where = f"{relative}:{number}"
        if ref.startswith("./"):
            continue  # An action committed to this repository moves with it.
        if ref.startswith("docker://"):
            failures.append(f"{where}: container action {ref!r} is not allowed")
            continue
        pinned = PINNED.match(ref)
        if pinned is None:
            action, _, requested = ref.partition("@")
            failures.append(
                f"{where}: {action or ref!r} is pinned to the mutable ref "
                f"{requested or '(none)'!r}; use the full 40-character commit SHA "
                "with the version in a trailing comment, e.g. "
                "`uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1`"
            )
            continue
        action, sha = pinned.group("action"), pinned.group("sha")
        if not comment:
            failures.append(
                f"{where}: {action}@{sha[:7]}… has no version comment; append "
                "`# v<version>` so the pin stays reviewable"
            )
        elif not VERSION_COMMENT.match(comment):
            failures.append(
                f"{where}: {action} pin comment {comment!r} does not name a version "
                "or upstream branch"
            )
        else:
            pins.append((action, sha, comment, where))
    return failures, pins


def check_permissions(text: str, relative: str) -> list[str]:
    failures: list[str] = []
    top_level = [line for line in text.splitlines() if line.startswith("permissions:")]
    if not top_level:
        failures.append(
            f"{relative}: no workflow-level `permissions:`; declare the least "
            "privilege the workflow needs (`contents: read` for most)"
        )
    for number, line in enumerate(text.splitlines(), 1):
        if re.match(r"^\s*permissions:\s*write-all\s*$", line):
            failures.append(f"{relative}:{number}: `permissions: write-all` is not allowed")
    return failures


def check_cosign_verify(text: str, relative: str) -> list[str]:
    """Every `cosign verify` restricts the certificate identity and its issuer.

    Without both flags the command accepts any Fulcio certificate, so it proves
    only that *someone* signed the artifact.
    """
    failures: list[str] = []
    lines = text.splitlines()
    for index, line in enumerate(lines):
        # A shell comment discussing the command is prose, not an invocation.
        if "cosign verify" not in line or line.lstrip().startswith("#"):
            continue
        # The flags live on the continuation lines of the same command.
        block = "\n".join(lines[index : index + 16])
        if "--certificate-identity-regexp" not in block or "--certificate-oidc-issuer" not in block:
            failures.append(
                f"{relative}:{index + 1}: `cosign verify` must pass "
                "--certificate-identity-regexp and --certificate-oidc-issuer"
            )
    return failures


def check_signer_identity(text: str, relative: str) -> list[str]:
    """Keyless verification stays bound to this workflow's own identity."""
    failures: list[str] = []
    lines = text.splitlines()
    # Every declaration is checked, not just the first: a job-level `env:` entry
    # shadows the workflow-level one, so an unanchored override widens what
    # `cosign verify` accepts even when an anchored line remains in the file.
    for number, line in enumerate(lines, 1):
        if "SIGNER_IDENTITY:" not in line or ANCHORED_IDENTITY.match(line):
            continue
        failures.append(
            f"{relative}:{number}: SIGNER_IDENTITY must be an anchored regular "
            "expression (`^…$`), so it cannot match a copy of the workflow on "
            "another ref"
        )
    return failures


def check(root: Path) -> list[str]:
    files = workflows(root)
    if not files:
        return [".github/workflows: no workflow files found"]
    failures: list[str] = []
    seen: dict[str, tuple[str, str, str]] = {}
    for path in files:
        relative = str(path.relative_to(root))
        text = path.read_text(encoding="utf-8")
        pin_failures, pins = check_pins(text, relative)
        failures.extend(pin_failures)
        failures.extend(check_permissions(text, relative))
        failures.extend(check_signer_identity(text, relative))
        failures.extend(check_cosign_verify(text, relative))
        for action, sha, comment, where in pins:
            first = seen.setdefault(action, (sha, comment, where))
            if first[0] != sha:
                failures.append(
                    f"{where}: {action} is pinned to {sha[:7]}… here but to "
                    f"{first[0][:7]}… at {first[2]}; one reviewed pin per action"
                )
            elif first[1] != comment:
                failures.append(
                    f"{where}: {action}@{sha[:7]}… is labelled {comment!r} here "
                    f"but {first[1]!r} at {first[2]}"
                )
    for path in verifier_scripts(root):
        failures.extend(
            check_cosign_verify(path.read_text(encoding="utf-8"), str(path.relative_to(root)))
        )
    return failures


SELF_TEST_PERMISSIONS = "permissions:\n  contents: read\n"
GOOD = SELF_TEST_PERMISSIONS + (
    "jobs:\n"
    "  build:\n"
    "    steps:\n"
    "      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1\n"
    "      - uses: ./.github/actions/local\n"
)


def self_test() -> list[str]:
    cases: list[tuple[str, str, str]] = [
        ("a clean workflow", GOOD, ""),
        (
            "a tag ref",
            SELF_TEST_PERMISSIONS + "jobs:\n  a:\n    steps:\n      - uses: actions/checkout@v7\n",
            "mutable ref",
        ),
        (
            "a branch ref",
            SELF_TEST_PERMISSIONS
            + "jobs:\n  a:\n    steps:\n      - uses: dtolnay/rust-toolchain@stable\n",
            "mutable ref",
        ),
        (
            "a short SHA",
            SELF_TEST_PERMISSIONS
            + "jobs:\n  a:\n    steps:\n      - uses: actions/checkout@3d3c42e # v7.0.1\n",
            "mutable ref",
        ),
        (
            "an unlabelled pin",
            SELF_TEST_PERMISSIONS
            + "jobs:\n  a:\n    steps:\n"
            "      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1\n",
            "no version comment",
        ),
        (
            "an uninformative pin comment",
            SELF_TEST_PERMISSIONS
            + "jobs:\n  a:\n    steps:\n"
            "      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # latest\n",
            "does not name a version",
        ),
        (
            "a container action",
            SELF_TEST_PERMISSIONS + "jobs:\n  a:\n    steps:\n      - uses: docker://alpine:3\n",
            "container action",
        ),
        (
            "a workflow with no permissions block",
            "jobs:\n  a:\n    steps:\n"
            "      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1\n",
            "least privilege",
        ),
        (
            "a job asking for write-all",
            SELF_TEST_PERMISSIONS + "jobs:\n  a:\n    permissions: write-all\n",
            "write-all",
        ),
        (
            "an unanchored signer identity",
            SELF_TEST_PERMISSIONS + "env:\n  SIGNER_IDENTITY: https://github.com/o/r\n",
            "anchored regular",
        ),
        (
            "an unanchored signer identity shadowing an anchored one",
            SELF_TEST_PERMISSIONS
            + "env:\n  SIGNER_IDENTITY: ^https://github\\.com/o/r@refs/heads/main$\n"
            "jobs:\n  a:\n    env:\n      SIGNER_IDENTITY: https://github.com/o/r\n",
            "anchored regular",
        ),
        (
            "one workflow disagreeing with itself about a pin",
            SELF_TEST_PERMISSIONS + "jobs:\n  a:\n    steps:\n"
            "      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1\n"
            "      - uses: actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0 # v7.0.0\n",
            "one reviewed pin per action",
        ),
        (
            "cosign verify without an identity restriction",
            SELF_TEST_PERMISSIONS
            + "jobs:\n  a:\n    steps:\n      - run: cosign verify ghcr.io/o/r@sha256:x\n",
            "--certificate-identity-regexp",
        ),
    ]
    problems: list[str] = []
    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        directory = root / ".github" / "workflows"
        directory.mkdir(parents=True)
        for description, body, expected in cases:
            (directory / "case.yml").write_text(body, encoding="utf-8")
            failures = check(root)
            if expected and not any(expected in failure for failure in failures):
                problems.append(
                    f"self-test: {description} was not rejected for {expected!r}: {failures}"
                )
            if not expected and failures:
                problems.append(f"self-test: {description} was rejected: {failures}")
        # Two workflows must not disagree about the same action's reviewed pin.
        (directory / "case.yml").write_text(GOOD, encoding="utf-8")
        (directory / "other.yml").write_text(
            SELF_TEST_PERMISSIONS
            + "jobs:\n  a:\n    steps:\n"
            "      - uses: actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0 # v7.0.0\n",
            encoding="utf-8",
        )
        if not any("one reviewed pin per action" in failure for failure in check(root)):
            problems.append("self-test: disagreeing pins for one action were not rejected")
        (directory / "other.yml").unlink()
        # The release verifies signatures from a shell script, not from a `run:`
        # block, so the identity restriction is checked there too.
        ops = root / "ops"
        ops.mkdir()
        script = ops / "verify-evidence.sh"
        script.write_text('cosign verify "$IMAGE" --certificate-identity-regexp "$ID" \\\n  --certificate-oidc-issuer https://token.actions.githubusercontent.com\n', encoding="utf-8")
        if check(root):
            problems.append(f"self-test: a restricted script verify was rejected: {check(root)}")
        # Prose about the command is not the command.
        script.write_text("# cosign verify runs below\n", encoding="utf-8")
        if check(root):
            problems.append(f"self-test: a comment was read as a verify: {check(root)}")
        script.write_text('cosign verify "$IMAGE"\n', encoding="utf-8")
        if not any(
            "--certificate-identity-regexp" in failure for failure in check(root)
        ):
            problems.append(
                "self-test: an unrestricted `cosign verify` in ops/ was not rejected"
            )
    return problems


def main(argv: list[str]) -> int:
    if argv[1:] == ["--self-test"]:
        problems = self_test()
        for problem in problems:
            print(problem, file=sys.stderr)
        if problems:
            return 1
        print("workflow policy self-test passed")
        return 0
    if argv[1:]:
        print(f"usage: {argv[0]} [--self-test]", file=sys.stderr)
        return 2

    failures = check(ROOT)
    for failure in failures:
        print(f"workflow policy failed: {failure}", file=sys.stderr)
    if failures:
        return 1
    print(
        f"workflow policy passed ({len(workflows(ROOT))} workflows, "
        f"{len(verifier_scripts(ROOT))} ops scripts)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
