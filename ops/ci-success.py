#!/usr/bin/env python3
"""Keep the required CI Success check strict for both PRs and release refs."""

import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile


COMMON = {"changes", "fmt", "clippy", "tests", "fuzz-smoke", "sdk-compat", "docs", "workflow-policy"}
RELEASE = {
    "msrv", "api-compat", "build", "faults", "sdk-compat-ts", "openapi-smoke",
    "rustdoc", "publish-dry-run", "static-binary", "binary-smoke", "docker-smoke",
    "cosign-format", "rollout-drill", "quickstart-smoke",
}
LEGACY = {"stateful-deploy-drill", "stateful-persistent-drill"}


def expected_results(event, dependencies, legacy):
    expected = {job: "success" for job in COMMON}
    expected.update({job: "skipped" if event == "pull_request" else "success" for job in RELEASE})
    expected.update({job: "success" if event == "workflow_dispatch" and legacy == "true" else "skipped" for job in LEGACY})
    expected["dependency-policy"] = "success" if dependencies == "true" else "skipped"
    return expected


def failures(needs, event, dependencies, legacy):
    expected = expected_results(event, dependencies, legacy)
    errors = []
    if dependencies not in {"true", "false"} or (event != "pull_request" and dependencies != "true"):
        errors.append("dependency change detection must succeed; non-PR runs must check dependencies")
    if needs.keys() != expected.keys():
        errors.append(f"unexpected job set: missing={sorted(expected.keys() - needs.keys())}, extra={sorted(needs.keys() - expected.keys())}")
    for job, result in expected.items():
        actual = needs.get(job, {}).get("result", "missing")
        if actual != result:
            errors.append(f"{job}: expected {result}, got {actual}")
    return errors


def self_test():
    all_jobs = COMMON | RELEASE | LEGACY | {"dependency-policy"}
    for event, dependencies, legacy in [
        ("pull_request", "false", ""), ("pull_request", "true", ""),
        ("push", "true", ""), ("merge_group", "true", ""),
        ("workflow_dispatch", "true", "false"), ("workflow_dispatch", "true", "true"),
    ]:
        needs = {job: {"result": "success"} for job in all_jobs}
        for job in RELEASE:
            if event == "pull_request":
                needs[job]["result"] = "skipped"
        for job in LEGACY:
            if event != "workflow_dispatch" or legacy != "true":
                needs[job]["result"] = "skipped"
        if dependencies == "false":
            needs["dependency-policy"]["result"] = "skipped"
        assert not failures(needs, event, dependencies, legacy)
        for job in all_jobs:
            original = needs[job]["result"]
            for result in {"success", "failure", "cancelled", "skipped"} - {original}:
                needs[job]["result"] = result
                assert failures(needs, event, dependencies, legacy), (event, job, result)
            needs[job]["result"] = original
        assert failures(needs, event, "", legacy)
        assert failures({}, event, dependencies, legacy)

    workflow = (Path(__file__).resolve().parent.parent / ".github/workflows/ci.yml").read_text()
    jobs = set(re.findall(r"^  ([\w-]+):$", workflow.split("jobs:\n", 1)[1], re.M))
    assert jobs == all_jobs | {"CI-Success"}, jobs ^ all_jobs
    gate = workflow.split("  CI-Success:\n", 1)[1]
    assert set(re.findall(r"^      - ([\w-]+)$", gate, re.M)) == all_jobs
    for job in RELEASE:
        block = re.search(rf"^  {job}:\n(.*?)(?=^  [\w-]+:|\Z)", workflow, re.M | re.S).group(1)
        assert "if: ${{ github.event_name != 'pull_request' }}" in block, job
    tests = workflow.split("  tests:\n", 1)[1].split("\n  faults:", 1)[0]
    assert "cargo test --workspace --locked" in tests
    assert "--release" not in tests
    assert "tags: ['v*']" in workflow
    assert "if: ${{ always() }}" in gate
    # Exercise the workflow's actual change detector against Git history, so
    # a broken path filter cannot silently turn off cargo-deny on lock changes.
    changes = workflow.split("  changes:\n", 1)[1].split("\n  fmt:", 1)[0]
    shell = changes.split("        run: |\n", 1)[1]
    shell = "\n".join(line[10:] for line in shell.splitlines() if line.strip())
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        def git(*args):
            return subprocess.run(["git", *args], cwd=root, check=True, capture_output=True)
        git("init", "--quiet")
        git("config", "user.email", "ci-test@example.invalid")
        git("config", "user.name", "CI gate test")
        git("commit", "--allow-empty", "-qm", "base")
        for filename, expected in [
            ("README.md", "false"), ("crates/gateway/src/routes.rs", "false"),
            ("Cargo.lock", "true"), ("fuzz/Cargo.lock", "true"),
            ("deny.toml", "true"), ("crates/gateway/Cargo.toml", "true"),
        ]:
            path = root / filename
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("changed\n")
            git("add", filename)
            git("commit", "-qm", "change")
            output = root / "output"
            output.write_text("")
            env = dict(os.environ, EVENT_NAME="pull_request", GITHUB_OUTPUT=str(output))
            subprocess.run(["bash", "-e", "-c", shell], cwd=root, env=env, check=True)
            assert output.read_text().strip() == f"dependencies={expected}", filename
    print("CI gate self-test passed: PR, main/tag, merge queue, manual, and failure/skip cases")


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        self_test()
    else:
        errors = failures(json.loads(os.environ["CI_NEEDS"]), os.environ["CI_EVENT"],
                          os.environ["CI_DEPENDENCIES"], os.environ.get("CI_LEGACY", ""))
        for error in errors:
            print(f"::error::{error}")
        sys.exit(bool(errors))
