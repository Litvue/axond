#!/usr/bin/env python3
"""Keep the required CI Success check strict for both PRs and release refs."""

import json
import os
from pathlib import Path
import re
import sys


COMMON = {"changes", "typescript", "docs", "workflow-policy"}
RELEASE = {"openapi-smoke", "cosign-format", "quickstart-smoke"}


def expected_results(event, typescript="true"):
    expected = {job: "success" if typescript == "true" or job == "changes" else "skipped" for job in COMMON}
    expected.update({job: "skipped" if typescript == "false" or event == "pull_request" else "success" for job in RELEASE})
    return expected


def failures(needs, event, typescript="true"):
    expected = expected_results(event, typescript)
    errors = []
    if typescript not in {"true", "false"}:
        errors.append("TypeScript change detection must succeed")
    if event == "workflow_dispatch" and typescript != "true":
        errors.append("manual qualification must run TypeScript checks")
    if needs.keys() != expected.keys():
        errors.append(f"unexpected job set: missing={sorted(expected.keys() - needs.keys())}, extra={sorted(needs.keys() - expected.keys())}")
    for job, result in expected.items():
        actual = needs.get(job, {}).get("result", "missing")
        if actual != result:
            errors.append(f"{job}: expected {result}, got {actual}")
    return errors


def self_test():
    all_jobs = COMMON | RELEASE
    for event in ("pull_request", "push", "merge_group", "workflow_dispatch"):
        for selected in ("true", "false"):
            if event == "workflow_dispatch" and selected == "false":
                continue
            needs = {job: {"result": result} for job, result in expected_results(event, selected).items()}
            assert not failures(needs, event, selected)
            for job in all_jobs:
                original = needs[job]["result"]
                for result in {"success", "failure", "cancelled", "skipped"} - {original}:
                    needs[job]["result"] = result
                    assert failures(needs, event, selected), (event, job, result)
                needs[job]["result"] = original
            assert failures(needs, event, "")
            assert failures({}, event, selected)
    assert failures({}, "workflow_dispatch", "false")

    workflow = (Path(__file__).resolve().parent.parent / ".github/workflows/ci.yml").read_text()
    jobs = set(re.findall(r"^  ([\w-]+):$", workflow.split("jobs:\n", 1)[1], re.M))
    assert jobs == all_jobs | {"CI-Success"}, jobs ^ all_jobs
    gate = workflow.split("  CI-Success:\n", 1)[1]
    assert set(re.findall(r"^      - ([\w-]+)$", gate, re.M)) == all_jobs
    for job in RELEASE:
        block = re.search(rf"^  {job}:\n(.*?)(?=^  [\w-]+:|\Z)", workflow, re.M | re.S).group(1)
        assert "if: ${{ needs.changes.outputs.typescript == 'true' && github.event_name != 'pull_request' }}" in block, job
        assert "    needs: changes" in block, job
    assert "tags: ['v*']" in workflow
    assert "if: ${{ always() }}" in gate
    for job in COMMON - {"changes"}:
        block = re.search(rf"^  {job}:\n(.*?)(?=^  [\w-]+:|\Z)", workflow, re.M | re.S).group(1)
        assert "if: ${{ needs.changes.outputs.typescript == 'true' }}" in block, job
        assert "    needs: changes" in block, job
    print("CI gate self-test passed: website-only, TypeScript, main/tag, merge queue, manual, failure/skip cases")


if __name__ == "__main__":
    if sys.argv[1:] == ["--self-test"]:
        self_test()
    else:
        errors = failures(json.loads(os.environ["CI_NEEDS"]), os.environ["CI_EVENT"],
                          os.environ.get("CI_TYPESCRIPT", ""))
        for error in errors:
            print(f"::error::{error}")
        sys.exit(bool(errors))
