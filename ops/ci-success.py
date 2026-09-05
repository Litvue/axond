#!/usr/bin/env python3
"""Keep the required CI Success check strict for both PRs and release refs."""

import json
import os
from pathlib import Path
import re
import sys


COMMON = {"changes", "fmt", "clippy", "tests", "fuzz-smoke", "sdk-compat", "docs", "workflow-policy"}
RELEASE = {
    "msrv", "api-compat", "build", "faults", "sdk-compat-ts", "openapi-smoke",
    "rustdoc", "publish-dry-run", "static-binary", "binary-smoke", "docker-smoke",
    "cosign-format", "rollout-drill", "quickstart-smoke",
}
LEGACY = {"stateful-deploy-drill", "stateful-persistent-drill"}


def expected_results(event, dependencies, legacy, rust):
    expected = {job: "success" if rust == "true" or job in {"changes", "workflow-policy"} else "skipped" for job in COMMON}
    expected.update({job: "skipped" if rust == "false" or event == "pull_request" else "success" for job in RELEASE})
    expected.update({job: "success" if event == "workflow_dispatch" and legacy == "true" else "skipped" for job in LEGACY})
    expected["dependency-policy"] = "success" if dependencies == "true" else "skipped"
    return expected


def failures(needs, event, dependencies, legacy, rust):
    expected = expected_results(event, dependencies, legacy, rust)
    errors = []
    if rust not in {"true", "false"}:
        errors.append("Rust change detection must succeed")
    if event == "workflow_dispatch" and rust != "true":
        errors.append("manual qualification must run Rust checks")
    if dependencies not in {"true", "false"} or (rust == "true" and event != "pull_request" and dependencies != "true") or (rust == "false" and dependencies != "false"):
        errors.append("dependency change detection must agree with the selected suite")
    if needs.keys() != expected.keys():
        errors.append(f"unexpected job set: missing={sorted(expected.keys() - needs.keys())}, extra={sorted(needs.keys() - expected.keys())}")
    for job, result in expected.items():
        actual = needs.get(job, {}).get("result", "missing")
        if actual != result:
            errors.append(f"{job}: expected {result}, got {actual}")
    return errors


def self_test():
    all_jobs = COMMON | RELEASE | LEGACY | {"dependency-policy"}
    cases = [
        ("pull_request", "false", "", "true"), ("pull_request", "true", "", "true"),
        ("push", "true", "", "true"), ("merge_group", "true", "", "true"),
        ("workflow_dispatch", "true", "false", "true"), ("workflow_dispatch", "true", "true", "true"),
        ("pull_request", "false", "", "false"), ("push", "false", "", "false"),
        ("merge_group", "false", "", "false"),
    ]
    for event, dependencies, legacy, rust in cases:
        needs = {job: {"result": result} for job, result in expected_results(event, dependencies, legacy, rust).items()}
        assert not failures(needs, event, dependencies, legacy, rust)
        for job in all_jobs:
            original = needs[job]["result"]
            for result in {"success", "failure", "cancelled", "skipped"} - {original}:
                needs[job]["result"] = result
                assert failures(needs, event, dependencies, legacy, rust), (event, job, result)
            needs[job]["result"] = original
        assert failures(needs, event, "", legacy, rust)
        assert failures(needs, event, dependencies, legacy, "")
        assert failures(needs, event, "true", legacy, "false")
        assert failures({}, event, dependencies, legacy, rust)

    workflow = (Path(__file__).resolve().parent.parent / ".github/workflows/ci.yml").read_text()
    jobs = set(re.findall(r"^  ([\w-]+):$", workflow.split("jobs:\n", 1)[1], re.M))
    assert jobs == all_jobs | {"CI-Success"}, jobs ^ all_jobs
    gate = workflow.split("  CI-Success:\n", 1)[1]
    assert set(re.findall(r"^      - ([\w-]+)$", gate, re.M)) == all_jobs
    for job in RELEASE:
        block = re.search(rf"^  {job}:\n(.*?)(?=^  [\w-]+:|\Z)", workflow, re.M | re.S).group(1)
        assert "if: ${{ needs.changes.outputs.rust == 'true' && github.event_name != 'pull_request' }}" in block, job
        assert "    needs: changes" in block, job
    tests = workflow.split("  tests:\n", 1)[1].split("\n  faults:", 1)[0]
    assert "cargo test --workspace --locked" in tests
    assert "--release" not in tests
    assert "tags: ['v*']" in workflow
    assert "if: ${{ always() }}" in gate
    for job in COMMON - {"changes", "workflow-policy"}:
        block = re.search(rf"^  {job}:\n(.*?)(?=^  [\w-]+:|\Z)", workflow, re.M | re.S).group(1)
        assert "if: ${{ needs.changes.outputs.rust == 'true' }}" in block, job
        assert "    needs: changes" in block, job
    assert "CI_RUST: ${{ needs.changes.outputs.rust }}" in gate
    print("CI gate self-test passed: website-only, Rust, main/tag, merge queue, manual, failure/skip cases")


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        self_test()
    else:
        errors = failures(json.loads(os.environ["CI_NEEDS"]), os.environ["CI_EVENT"],
                          os.environ["CI_DEPENDENCIES"], os.environ.get("CI_LEGACY", ""), os.environ.get("CI_RUST", ""))
        for error in errors:
            print(f"::error::{error}")
        sys.exit(bool(errors))
