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

# Kubernetes stateful overlay drills remain behind an explicit opt-in. Request-
# path qualification (capacity, endurance, provider/transport faults) is SQLite
# + `/ns/{ns}/v1` and does not use this input. Recovery, rollout, and
# stateful-endurance qualification jobs were retired with the tier matrix
# (ADR 0063 / #427).
LEGACY_POSTGRES_INPUT = "run_legacy_postgres_qualification"
LEGACY_POSTGRES_GUARD = (
    "${{ github.event_name == 'workflow_dispatch' && "
    "inputs.run_legacy_postgres_qualification == true }}"
)
LEGACY_POSTGRES_JOBS = {
    ".github/workflows/ci.yml": {
        "stateful-deploy-drill": LEGACY_POSTGRES_GUARD,
        "stateful-persistent-drill": LEGACY_POSTGRES_GUARD,
    },
}
UNSCHEDULED_LEGACY_WORKFLOWS = frozenset()
ACTIONS_ENDURANCE_WORKFLOW = ".github/workflows/endurance.yml"
ACTIONS_ENDURANCE_MAX_JOB_MINUTES = 15
ACTIONS_ENDURANCE_SMOKE_JOBS = {
    "endurance": "the_endurance_smoke_tier_qualifies_and_publishes_its_evidence",
}


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
    key — but it is read only where the certificate flags are absent, and then
    only in `KEY_VERIFY_FILES` and only for the public half of a pair that file
    mints itself. Anywhere else, and for any key arriving from the environment,
    the certificate flags are the only accepted restriction: a script must not
    be able to opt out of the release contract by generating a key pair next to
    the verify it wants excused.
    """
    failures: list[str] = []
    lines = text.splitlines()
    minted = minted_public_keys(text) if relative in KEY_VERIFY_FILES else set()
    for index, line in enumerate(lines):
        # A shell comment discussing the command is prose, not an invocation.
        if "cosign verify" not in line or line.lstrip().startswith("#"):
            continue
        block = command_block(lines, index)
        # The certificate flags are the rule, so a command carrying them passes
        # whatever else it names: `--key` is read only as an alternative for a
        # command that offers no certificate restriction at all.
        if (
            "--certificate-identity-regexp" in block
            and "--certificate-oidc-issuer" in block
        ):
            continue
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


def check_musl_installer(text: str, relative: str) -> list[str]:
    """Every Linux-musl lane uses the same bounded package-manager path."""
    expected_steps = {
        ".github/workflows/ci.yml": 2,
        ".github/workflows/release-please.yml": 1,
    }

    lines = text.splitlines()
    blocks: list[tuple[int, str]] = []
    for index, line in enumerate(lines):
        match = re.match(r"^(\s*)- name:\s*Install musl tools\s*$", line)
        if not match:
            continue
        indent = match.group(1)
        end = index + 1
        while end < len(lines) and not lines[end].startswith(f"{indent}- "):
            end += 1
        blocks.append((index + 1, "\n".join(lines[index:end])))

    failures: list[str] = []
    expected = expected_steps.get(relative)
    if expected is not None and len(blocks) != expected:
        failures.append(
            f"{relative}: expected {expected} bounded musl install step(s), found "
            f"{len(blocks)}"
        )
    for number, block in blocks:
        if not re.search(
            r"^\s*run:\s*bash ops/install-musl-tools\.sh\s*$", block, re.MULTILINE
        ):
            failures.append(
                f"{relative}:{number}: musl install must use the shared bounded "
                "ops/install-musl-tools.sh path"
            )
        timeout = re.search(r"^\s*timeout-minutes:\s*(\d+)\s*$", block, re.MULTILINE)
        if not timeout or int(timeout.group(1)) != 25:
            failures.append(
                f"{relative}:{number}: musl install needs the reviewed 25-minute "
                "outer timeout that its inner-budget self-test targets"
            )
        if re.search(r"^\s*continue-on-error:\s*true\s*$", block, re.MULTILINE):
            failures.append(
                f"{relative}:{number}: musl installation must remain fail-closed"
            )

    for number, line in enumerate(lines, 1):
        if re.search(r"apt-get[^\n]*musl-tools", line):
            failures.append(
                f"{relative}:{number}: inline musl-tools installation bypasses the "
                "shared timeout and retry policy"
            )
    return failures


def nested_block(lines: list[str], start: int, indent: int) -> list[str]:
    """Return the lines nested below one line at `indent` spaces."""
    end = start + 1
    while end < len(lines):
        line = lines[end]
        if line.strip() and len(line) - len(line.lstrip()) <= indent:
            break
        end += 1
    return lines[start + 1 : end]


def named_blocks(text: str, parent: str) -> dict[str, list[str]]:
    """Return two-space-keyed blocks below a top-level YAML mapping."""
    lines = text.splitlines()
    try:
        start = lines.index(f"{parent}:")
    except ValueError:
        return {}
    blocks: dict[str, list[str]] = {}
    for index in range(start + 1, len(lines)):
        match = re.match(r"^  ([A-Za-z0-9_-]+):\s*$", lines[index])
        if match:
            blocks[match.group(1)] = nested_block(lines, index, 2)
        elif lines[index].strip() and not lines[index].startswith(" "):
            break
    return blocks


def dispatch_block(text: str) -> list[str]:
    lines = text.splitlines()
    for index, line in enumerate(lines):
        if line == "  workflow_dispatch:":
            return nested_block(lines, index, 2)
    return []


def check_legacy_postgres_qualification(root: Path) -> list[str]:
    """Keep remaining PostgreSQL overlay drills manual, explicit, and visible."""
    failures: list[str] = []
    for relative, expected_jobs in LEGACY_POSTGRES_JOBS.items():
        path = root / relative
        if not path.is_file():
            failures.append(f"{relative}: legacy PostgreSQL workflow is missing")
            continue
        text = path.read_text(encoding="utf-8")
        dispatch = dispatch_block(text)
        try:
            input_start = dispatch.index(f"      {LEGACY_POSTGRES_INPUT}:")
        except ValueError:
            failures.append(
                f"{relative}: workflow_dispatch must declare the explicit boolean "
                f"{LEGACY_POSTGRES_INPUT!r} opt-in"
            )
        else:
            input_lines = nested_block(dispatch, input_start, 6)
            required = {line.strip() for line in input_lines}
            for setting in ("required: true", "default: false", "type: boolean"):
                if setting not in required:
                    failures.append(
                        f"{relative}: {LEGACY_POSTGRES_INPUT} must include `{setting}`"
                    )

        if relative in UNSCHEDULED_LEGACY_WORKFLOWS and re.search(
            r"(?m)^  schedule:\s*$", text
        ):
            failures.append(
                f"{relative}: legacy/long qualification must not have a schedule"
            )

        jobs = named_blocks(text, "jobs")
        for job, expected_guard in expected_jobs.items():
            block = jobs.get(job)
            if block is None:
                failures.append(f"{relative}: legacy PostgreSQL job {job!r} is missing")
                continue
            guards = [
                line.strip().removeprefix("if:").strip()
                for line in block
                if re.match(r"^    if:\s*", line)
            ]
            if guards != [expected_guard]:
                failures.append(
                    f"{relative}: legacy PostgreSQL job {job!r} must use the exact "
                    f"explicit-dispatch guard `{expected_guard}`; found {guards or 'none'}"
                )
    return failures


def check_blob_qualification_status(root: Path) -> list[str]:
    """A green software CI aggregate must not imply stateful-v2 qualification."""
    required = {
        "RELEASE.md": (
            "| Blob-backed flat-namespace stateful-v2 qualification | Pending |",
            "is a software-change gate, not a production-qualification gate",
        ),
        "docs/operations/qualification.md": (
            "run_legacy_postgres_qualification=true",
            "The blob-backed stateful-v2 gates remain **pending**",
        ),
    }
    failures: list[str] = []
    for relative, markers in required.items():
        path = root / relative
        if not path.is_file():
            failures.append(f"{relative}: qualification status document is missing")
            continue
        text = path.read_text(encoding="utf-8")
        for marker in markers:
            if marker not in text:
                failures.append(
                    f"{relative}: missing explicit blob qualification status {marker!r}"
                )
    return failures


def check_actions_endurance_budget(root: Path) -> list[str]:
    """GitHub Actions may exercise endurance smoke, never a configurable soak."""
    relative = ACTIONS_ENDURANCE_WORKFLOW
    path = root / relative
    if not path.is_file():
        return [f"{relative}: Actions endurance smoke workflow is missing"]
    text = path.read_text(encoding="utf-8")
    failures: list[str] = []

    for marker in (
        "_soak_tier_",
        "ENDURANCE_DURATION_MS",
        "43200000",
        "qualification-evidence.py",
        "promote-qualification.py",
        "qualification-record",
    ):
        if marker in text:
            failures.append(
                f"{relative}: Actions endurance workflow contains soak-only "
                f"marker {marker!r}"
            )

    dispatch = dispatch_block(text)
    for line in dispatch:
        match = re.match(r"^      ([A-Za-z0-9_-]+):\s*$", line)
        if match and re.search(
            r"(?:duration|hours?|minutes?|soak)", match.group(1), re.IGNORECASE
        ):
            failures.append(
                f"{relative}: Actions endurance must not expose duration-like "
                f"workflow_dispatch input {match.group(1)!r}"
            )

    jobs = named_blocks(text, "jobs")
    unexpected = sorted(set(jobs) - set(ACTIONS_ENDURANCE_SMOKE_JOBS))
    if unexpected:
        failures.append(
            f"{relative}: Actions endurance workflow has non-smoke jobs {unexpected}"
        )
    for job, smoke_test in ACTIONS_ENDURANCE_SMOKE_JOBS.items():
        block = jobs.get(job)
        if block is None:
            failures.append(f"{relative}: endurance smoke job {job!r} is missing")
            continue
        body = "\n".join(block)
        timeout = re.search(r"(?m)^    timeout-minutes:\s*(\d+)\s*$", body)
        if timeout is None:
            failures.append(
                f"{relative}: endurance smoke job {job!r} needs an explicit "
                f"{ACTIONS_ENDURANCE_MAX_JOB_MINUTES}-minute timeout"
            )
        elif int(timeout.group(1)) > ACTIONS_ENDURANCE_MAX_JOB_MINUTES:
            failures.append(
                f"{relative}: endurance smoke job {job!r} timeout exceeds "
                f"{ACTIONS_ENDURANCE_MAX_JOB_MINUTES} minutes"
            )
        if smoke_test not in body:
            failures.append(
                f"{relative}: endurance smoke job {job!r} does not invoke "
                f"{smoke_test}"
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
        failures.extend(check_musl_installer(text, relative))
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
    # Temporary self-test repositories exercise the generic supply-chain rules
    # without carrying Axond's release documents. A real checkout always has
    # RELEASE.md, which turns on the repository-specific transition contract.
    if (root / "RELEASE.md").is_file():
        failures.extend(check_legacy_postgres_qualification(root))
        failures.extend(check_blob_qualification_status(root))
        failures.extend(check_actions_endurance_budget(root))
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

    opt_in = (
        "  workflow_dispatch:\n"
        "    inputs:\n"
        f"      {LEGACY_POSTGRES_INPUT}:\n"
        "        description: legacy only\n"
        "        required: true\n"
        "        default: false\n"
        "        type: boolean\n"
    )

    def legacy_fixture(relative: str) -> str:
        jobs = LEGACY_POSTGRES_JOBS[relative]
        body = "name: fixture\non:\n"
        if relative.endswith("ci.yml"):
            body += "  pull_request:\n"
        body += opt_in + "permissions:\n  contents: read\njobs:\n"
        for job, guard in jobs.items():
            body += f"  {job}:\n    if: {guard}\n    runs-on: ubuntu-latest\n"
        return body

    with tempfile.TemporaryDirectory() as raw:
        root = Path(raw)
        workflow_dir = root / ".github" / "workflows"
        workflow_dir.mkdir(parents=True)
        fixtures = {
            relative: legacy_fixture(relative)
            for relative in LEGACY_POSTGRES_JOBS
        }

        def write_legacy_fixtures() -> None:
            for relative, body in fixtures.items():
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(body, encoding="utf-8")

        write_legacy_fixtures()
        failures = check_legacy_postgres_qualification(root)
        if failures:
            problems.append(
                f"self-test: explicit legacy PostgreSQL opt-ins were rejected: {failures}"
            )

        ci = root / ".github/workflows/ci.yml"
        ci.write_text(
            fixtures[".github/workflows/ci.yml"].replace(
                f"    if: {LEGACY_POSTGRES_GUARD}\n", "", 1
            ),
            encoding="utf-8",
        )
        failures = check_legacy_postgres_qualification(root)
        if not any("explicit-dispatch guard" in failure for failure in failures):
            problems.append(
                "self-test: an unguarded PR-triggered legacy PostgreSQL job was accepted"
            )

        write_legacy_fixtures()
        ci.write_text(
            fixtures[".github/workflows/ci.yml"].replace(
                "        default: false\n", "        default: true\n", 1
            ),
            encoding="utf-8",
        )
        failures = check_legacy_postgres_qualification(root)
        if not any("default: false" in failure for failure in failures):
            problems.append(
                "self-test: a default-on legacy PostgreSQL dispatch was accepted"
            )

        actions_smoke = (
            "name: Endurance smoke\n"
            "on:\n"
            "  workflow_dispatch:\n"
            "    inputs:\n"
            "      note:\n"
            "        type: string\n"
            "permissions:\n  contents: read\n"
            "jobs:\n"
            "  endurance:\n"
            "    timeout-minutes: 15\n"
            "    steps:\n"
            "      - run: cargo test the_endurance_smoke_tier_qualifies_and_publishes_its_evidence\n"
        )
        endurance = root / ".github/workflows/endurance.yml"
        endurance.parent.mkdir(parents=True, exist_ok=True)
        endurance.write_text(actions_smoke, encoding="utf-8")
        failures = check_actions_endurance_budget(root)
        if failures:
            problems.append(
                f"self-test: bounded Actions endurance smoke was rejected: {failures}"
            )

        endurance.write_text(
            actions_smoke.replace(
                "    inputs:\n",
                "    inputs:\n"
                "      duration_ms:\n"
                "        default: '43200000'\n"
                "        type: string\n",
                1,
            ),
            encoding="utf-8",
        )
        failures = check_actions_endurance_budget(root)
        if not any("duration-like" in failure for failure in failures):
            problems.append(
                "self-test: a twelve-hour Actions endurance input/default was accepted"
            )

        endurance.write_text(
            actions_smoke.replace("    timeout-minutes: 15\n", "    timeout-minutes: 16\n", 1),
            encoding="utf-8",
        )
        failures = check_actions_endurance_budget(root)
        if not any("timeout exceeds 15 minutes" in failure for failure in failures):
            problems.append(
                "self-test: an Actions endurance job over fifteen minutes was accepted"
            )

        endurance.write_text(
            actions_smoke.replace(
                "the_endurance_smoke_tier_qualifies_and_publishes_its_evidence",
                "the_endurance_soak_tier_qualifies_and_publishes_its_evidence",
                1,
            ),
            encoding="utf-8",
        )
        failures = check_actions_endurance_budget(root)
        if not any("does not invoke" in failure for failure in failures):
            problems.append(
                "self-test: an Actions endurance soak entry point was accepted"
            )

        (root / "RELEASE.md").write_text(
            "| Blob-backed flat-namespace stateful-v2 qualification | Pending | x |\n"
            "CI Success is a software-change gate, not a production-qualification gate\n",
            encoding="utf-8",
        )
        qualification = root / "docs/operations/qualification.md"
        qualification.parent.mkdir(parents=True)
        qualification.write_text(
            "run_legacy_postgres_qualification=true\n"
            "The blob-backed stateful-v2 gates remain **pending**\n",
            encoding="utf-8",
        )
        if check_blob_qualification_status(root):
            problems.append(
                "self-test: explicit pending blob qualification status was rejected"
            )
        qualification.write_text(
            "run_legacy_postgres_qualification=true\n", encoding="utf-8"
        )
        if not any(
            "missing explicit blob qualification status" in failure
            for failure in check_blob_qualification_status(root)
        ):
            problems.append(
                "self-test: a missing pending blob qualification status was accepted"
            )

    good_musl = (
        "jobs:\n"
        "  static:\n"
        "    steps:\n"
        "      - name: Install musl tools\n"
        "        timeout-minutes: 25\n"
        "        run: bash ops/install-musl-tools.sh\n"
        "  smoke:\n"
        "    steps:\n"
        "      - name: Install musl tools\n"
        "        if: matrix.musl\n"
        "        timeout-minutes: 25\n"
        "        run: bash ops/install-musl-tools.sh\n"
    )
    good_release_musl = (
        "jobs:\n"
        "  release:\n"
        "    steps:\n"
        "      - name: Install musl tools\n"
        "        if: matrix.musl\n"
        "        timeout-minutes: 25\n"
        "        run: bash ops/install-musl-tools.sh\n"
    )
    musl_cases = [
        ("shared bounded CI installers", ".github/workflows/ci.yml", good_musl, ""),
        (
            "a shared bounded release installer",
            ".github/workflows/release-please.yml",
            good_release_musl,
            "",
        ),
        (
            "an inline apt installer",
            ".github/workflows/ci.yml",
            good_musl.replace(
                "run: bash ops/install-musl-tools.sh",
                "run: sudo apt-get install -y musl-tools",
                1,
            ),
            "shared bounded",
        ),
        (
            "a missing outer timeout",
            ".github/workflows/ci.yml",
            good_musl.replace("        timeout-minutes: 25\n", "", 1),
            "reviewed 25-minute",
        ),
        (
            "an outer timeout shorter than the reviewed budget",
            ".github/workflows/ci.yml",
            good_musl.replace("        timeout-minutes: 25\n", "        timeout-minutes: 20\n", 1),
            "reviewed 25-minute",
        ),
        (
            "only one musl lane",
            ".github/workflows/ci.yml",
            good_musl.split("  smoke:\n", 1)[0],
            "found 1",
        ),
        (
            "a missing release musl lane",
            ".github/workflows/release-please.yml",
            "jobs:\n  release:\n    steps:\n",
            "found 0",
        ),
        (
            "an inline install in another workflow",
            ".github/workflows/other.yml",
            "jobs:\n  job:\n    steps:\n"
            "      - run: sudo apt-get install -y musl-tools\n",
            "inline musl-tools",
        ),
    ]
    for description, relative, body, expected in musl_cases:
        failures = check_musl_installer(body, relative)
        if expected and not any(expected in failure for failure in failures):
            problems.append(
                f"self-test: {description} was not rejected for {expected!r}: {failures}"
            )
        if not expected and failures:
            problems.append(f"self-test: {description} was rejected: {failures}")

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
            # A command that does restrict the certificate is restricted, whatever
            # else it names: `--key` is read only where there is no such flag.
            canary.write_text(
                'cosign verify --key "$RELEASE_KEY" "$IMAGE" \\\n'
                '  --certificate-identity-regexp "$ID" \\\n'
                "  --certificate-oidc-issuer https://token.actions.githubusercontent.com\n",
                encoding="utf-8",
            )
            if check(root):
                problems.append(
                    f"self-test: a certificate-restricted verify naming a key was "
                    f"rejected: {check(root)}"
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
