#!/usr/bin/env python3
"""Refuse a second open pull request for the same change.

Two agents that start from the same issue will pick the same Conventional
Commit title and the same `Closes #N` line. Searching GitHub before `gh pr
create` is a race: both searches can miss. This gate keeps the oldest open PR
and fails every newer sibling, so the duplicate cannot merge.

Usage:
    ops/check-duplicate-prs.py            # live check when GITHUB_* is set
    ops/check-duplicate-prs.py --self-test
"""

from __future__ import annotations

import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Iterable

CLOSES = re.compile(
    r"(?:closes|fixes|resolves)(?:\s+[A-Za-z0-9_.-]+/[\w.-]+)?\s*#(\d+)",
    re.IGNORECASE,
)
ISSUE_URL = re.compile(
    r"(?:closes|fixes|resolves)\s+https://github\.com/[^/\s]+/[^/\s]+/issues/(\d+)",
    re.IGNORECASE,
)


def normalize_title(title: str) -> str:
    return " ".join(title.split())


def closed_issues(body: str | None) -> frozenset[int]:
    text = body or ""
    found = {int(n) for n in CLOSES.findall(text)}
    found.update(int(n) for n in ISSUE_URL.findall(text))
    return frozenset(found)


def conflicts(
    current: dict,
    others: Iterable[dict],
) -> list[str]:
    """Return reasons the current PR is a newer duplicate of an older open PR."""
    current_number = int(current["number"])
    title = normalize_title(current.get("title") or "")
    issues = closed_issues(current.get("body"))
    reasons: list[str] = []
    for other in others:
        other_number = int(other["number"])
        if other_number >= current_number:
            continue
        other_title = normalize_title(other.get("title") or "")
        other_issues = closed_issues(other.get("body"))
        if title and title == other_title:
            reasons.append(
                f"#{current_number} repeats the title of older open #{other_number}: {title!r}"
            )
        shared = issues & other_issues
        for issue in sorted(shared):
            reasons.append(
                f"#{current_number} closes #{issue}, which older open #{other_number} already closes"
            )
    return reasons


def _parse_next_link(header: str | None) -> str | None:
    if not header:
        return None
    for part in header.split(","):
        if 'rel="next"' not in part:
            continue
        start = part.find("<")
        end = part.find(">", start)
        if start != -1 and end != -1:
            return part[start + 1 : end]
    return None


def list_open_pulls(repo: str, token: str) -> list[dict]:
    owner, name = repo.split("/", 1)
    url: str | None = (
        f"https://api.github.com/repos/{urllib.parse.quote(owner)}/"
        f"{urllib.parse.quote(name)}/pulls?state=open&per_page=100"
    )
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "axond-check-duplicate-prs",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    pulls: list[dict] = []
    while url:
        request = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                payload = json.loads(response.read().decode())
                if not isinstance(payload, list):
                    raise RuntimeError(f"GitHub pulls response was not a list: {payload!r}")
                pulls.extend(payload)
                url = _parse_next_link(response.headers.get("Link"))
        except urllib.error.HTTPError as error:
            raise RuntimeError(f"GitHub pulls list failed: {error.code} {error.reason}") from error
    return pulls


def live_check() -> int:
    event = os.environ.get("GITHUB_EVENT_NAME", "")
    if event not in {"pull_request", "pull_request_target"}:
        return 0
    repo = os.environ.get("GITHUB_REPOSITORY", "")
    number = os.environ.get("GITHUB_PR_NUMBER") or os.environ.get("PR_NUMBER")
    title = os.environ.get("GITHUB_PR_TITLE", "")
    body = os.environ.get("GITHUB_PR_BODY", "")
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN") or ""
    if not repo or not number:
        print(
            "check-duplicate-prs: pull_request event is missing GITHUB_REPOSITORY or GITHUB_PR_NUMBER",
            file=sys.stderr,
        )
        return 1
    current = {"number": int(number), "title": title, "body": body}
    others = list_open_pulls(repo, token)
    problems = conflicts(current, others)
    if not problems:
        return 0
    for reason in problems:
        print(f"error: {reason}", file=sys.stderr)
    print(
        "Close the newer PR and push onto the older branch. Do not open a sibling.",
        file=sys.stderr,
    )
    return 1


def self_test() -> int:
    older = {
        "number": 570,
        "title": "fix(service): tolerate null other-kind vector algorithm params",
        "body": "Closes #567\n",
    }
    newer = {
        "number": 571,
        "title": "fix(service): tolerate null other-kind vector algorithm params",
        "body": "Closes #567\n",
    }
    title_hit = conflicts(newer, [older, newer])
    assert any("repeats the title" in reason for reason in title_hit), title_hit
    assert any("closes #567" in reason for reason in title_hit), title_hit
    # The older PR must stay mergeable while the sibling is open.
    assert conflicts(older, [older, newer]) == []
    different = {
        "number": 572,
        "title": "perf(store): retain Postgres sessions across bursts",
        "body": "Closes #465\n",
    }
    assert conflicts(different, [older, newer]) == []
    # Fixes #N is the same closer as Closes #N.
    alias = {
        "number": 573,
        "title": "fix(service): a different subject",
        "body": "Fixes #567\n",
    }
    aliased = conflicts(alias, [older])
    assert any("closes #567" in reason for reason in aliased), aliased
    url_close = {
        "number": 574,
        "title": "another spelling",
        "body": "Resolves https://github.com/Litvue/custodian/issues/567\n",
    }
    assert any("closes #567" in reason for reason in conflicts(url_close, [older]))
    print("ok")
    return 0


def main(argv: list[str]) -> int:
    if argv == ["--self-test"]:
        return self_test()
    if argv:
        print(f"usage: {sys.argv[0]} [--self-test]", file=sys.stderr)
        return 2
    return live_check()


if __name__ == "__main__":
    try:
        raise SystemExit(main(sys.argv[1:]))
    except AssertionError:
        raise
    except Exception as error:  # noqa: BLE001 — operator-facing gate
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1) from error
