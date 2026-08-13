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
# The one file allowed to verify against a key instead of a certificate
# identity. It signs a throwaway image in a local registry to prove the pinned
# cosign still writes the documented signature format; nothing it verifies is a
# release artifact. Naming the file keeps the exemption from being something any
# future script can grant itself by minting a key pair — widening it is a review
# of this list, which is the point.
KEY_VERIFY_FILES = frozenset({"ops/check-cosign-format.sh"})


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


def command_block(lines: list[str], index: int) -> str:
    """One shell command: the line, plus the continuations a trailing `\\` joins.

    A fixed lookahead reads past the end of the command, which is unsafe in both
    directions here: a restriction belonging to a *later* command would satisfy
    an unrestricted one, so an unrestricted keyless `cosign verify` sitting near
    a key-based one would pass the gate.
    """
    end = index
    while lines[end].rstrip().endswith("\\") and end + 1 < len(lines):
        end += 1
    return "\n".join(lines[index : end + 1])


def unquote(word: str) -> str:
    """A shell word without the quoting that does not change what it names."""
    return word.strip().strip("\"'")


def minted_public_keys(text: str) -> set[str]:
    """The public keys this file generates, as the words that would refer to them.

    A pair minted here is a signer this file named itself; a key handed in from
    outside is not, so the two are told apart by what the `--key` word is rather
    than by whether the file mints a pair somewhere. Both the generated path and
    any variable holding it count, because the script that mints a pair reads it
    back through a variable. Commented-out lines mint nothing.
    """
    code = "\n".join(
        line for line in text.splitlines() if not line.lstrip().startswith("#")
    )
    keys = {
        f"{unquote(match.group(1))}.pub"
        for match in re.finditer(
            r"cosign generate-key-pair[^\n]*--output-key-prefix[= ]+(\S+)", code
        )
    }
    # `pub="$work/canary.pub"` refers to the same file as the prefix does.
    for match in re.finditer(r"^\s*(\w+)=(\S+)\s*$", code, re.MULTILINE):
        if unquote(match.group(2)) in keys:
            keys |= {f"${match.group(1)}", f"${{{match.group(1)}}}"}
    return keys


def check_cosign_verify(text: str, relative: str) -> list[str]:
    """Every `cosign verify` restricts who the signature must come from.

    Keyless verification without both certificate flags accepts any Fulcio
    certificate, so it proves only that *someone* signed the artifact. `--key`
    can say the same thing — the signature must verify against one named public
    key — but only in `KEY_VERIFY_FILES`, and only for the public half of a pair
    that file mints itself. Anywhere else, and for any key arriving from the
    environment, the certificate flags are the only accepted restriction: a
    script must not be able to opt out of the release contract by generating a
    key pair next to the verify it wants excused.
    """
    failures: list[str] = []
    lines = text.splitlines()
    minted = minted_public_keys(text) if relative in KEY_VERIFY_FILES else set()
    for index, line in enumerate(lines):
        # A shell comment discussing the command is prose, not an invocation.
        if "cosign verify" not in line or line.lstrip().startswith("#"):
            continue
        block = command_block(lines, index)
        key = re.search(r"--key[= ]+(\S+)", block)
        if key:
            if unquote(key.group(1)) in minted:
                continue
            failures.append(
                f"{relative}:{index + 1}: `cosign verify --key {key.group(1)}` names a "
                "signer only in " + ", ".join(sorted(KEY_VERIFY_FILES)) + ", and only "
                "for a key generated in that file; otherwise pass "
                "--certificate-identity-regexp and --certificate-oidc-issuer"
            )
            continue
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
        # A comment discussing the setting configures nothing, and these workflows
        # explain the identity in prose next to it.
        if "SIGNER_IDENTITY:" not in line or line.lstrip().startswith("#"):
            continue
        if ANCHORED_IDENTITY.match(line):
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
            "a comment about the signer identity",
            SELF_TEST_PERMISSIONS
            + "env:\n  SIGNER_IDENTITY: ^https://github\\.com/o/r@refs/heads/main$\n"
            "  # SIGNER_IDENTITY: stays anchored so another ref cannot satisfy it\n",
            "",
        ),
        (
            "one workflow disagreeing with itself about a pin",
            SELF_TEST_PERMISSIONS + "jobs:\n  a:\n    steps:\n"
            "      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1\n"
            "      - uses: actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0 # v7.0.0\n",
            "one reviewed pin per action",
        ),
        (
            "a workflow minting its own key to skip the identity restriction",
            SELF_TEST_PERMISSIONS
            + "jobs:\n  a:\n    steps:\n"
            "      - run: cosign generate-key-pair --output-key-prefix k\n"
            "      - run: cosign verify --key k.pub ghcr.io/o/r@sha256:x\n",
            "names a signer only in",
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
        script.unlink()
        # The format canary verifies against a pair it mints itself, which names
        # a signer; the same command anywhere else, or against a key from the
        # environment, does not.
        minted = (
            'cosign generate-key-pair --output-key-prefix "$WORK/k"\n'
            'PUB="$WORK/k.pub"\n'
        )
        for name in sorted(KEY_VERIFY_FILES):
            canary = root / name
            canary.write_text(
                minted + 'cosign verify --key "$PUB" "$IMAGE" >/dev/null\n'
                'cosign verify --key "$PUB" "$INDEX" >/dev/null\n',
                encoding="utf-8",
            )
            if check(root):
                problems.append(
                    f"self-test: {name}'s self-minted verifies were rejected: {check(root)}"
                )
            # Minting a throwaway pair does not vouch for a key handed in from
            # elsewhere, even in the file allowed to use one.
            canary.write_text(
                minted + 'cosign verify --key "$RELEASE_KEY" "$IMAGE" >/dev/null\n',
                encoding="utf-8",
            )
            if not any("names a signer only in" in failure for failure in check(root)):
                problems.append(
                    "self-test: a foreign key was excused by an unrelated generated pair"
                )
            # A commented-out generator mints nothing.
            canary.write_text(
                '# cosign generate-key-pair --output-key-prefix "$WORK/k"\n'
                'cosign verify --key "$WORK/k.pub" "$IMAGE" >/dev/null\n',
                encoding="utf-8",
            )
            if not any("names a signer only in" in failure for failure in check(root)):
                problems.append(
                    "self-test: a commented-out generator was read as minting a key"
                )
            canary.write_text(
                minted + 'cosign verify "$IMAGE" >/dev/null\n'
                'cosign verify --key "$PUB" "$INDEX" >/dev/null\n',
                encoding="utf-8",
            )
            if not any(
                "--certificate-identity-regexp" in failure for failure in check(root)
            ):
                problems.append(
                    "self-test: an unrestricted `cosign verify` was excused by a "
                    "key-based one on the next line"
                )
            canary.unlink()
        # Another script cannot grant itself the exemption by minting a pair.
        script.write_text(
            minted + 'cosign verify --key "$PUB" "$IMAGE" >/dev/null\n',
            encoding="utf-8",
        )
        if not any("names a signer only in" in failure for failure in check(root)):
            problems.append(
                "self-test: a script outside KEY_VERIFY_FILES exempted itself by "
                "generating a key pair"
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
