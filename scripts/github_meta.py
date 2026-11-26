#!/usr/bin/env python3
"""Batched GitHub repository metadata, shared by sync_github_metadata.py and verify_catalog.py.

One GraphQL query asks for up to BATCH repositories at once, so the whole catalog costs a handful of
requests instead of one REST call per entry. A repository GitHub cannot find comes back as None.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from typing import Any

GITHUB_REPO_RE = re.compile(r"^https?://github\.com/([^/]+)/([^/#?]+?)(?:\.git)?/?$")
GRAPHQL_URL = "https://api.github.com/graphql"
BATCH = 100  # GraphQL node limit per query is far above this; 100 keeps each response small

FIELDS = "nameWithOwner stargazerCount pushedAt updatedAt isArchived isPrivate licenseInfo { spdxId key }"


def parse_repo(url: str) -> tuple[str, str] | None:
    """(owner, repo) for a plain repository URL, None for anything else (subpaths, blogs, docs)."""
    m = GITHUB_REPO_RE.match(url.strip())
    return (m.group(1), m.group(2)) if m else None


def get_token() -> str | None:
    if os.getenv("GITHUB_TOKEN"):
        return os.getenv("GITHUB_TOKEN")
    try:
        return subprocess.check_output(["gh", "auth", "token"], text=True, stderr=subprocess.DEVNULL).strip() or None
    except Exception:
        return None


def _query(repos: list[tuple[str, str]], token: str) -> dict[str, Any]:
    parts = [
        f'r{i}: repository(owner: {json.dumps(o)}, name: {json.dumps(n)}) {{ {FIELDS} }}' for i, (o, n) in enumerate(repos)
    ]
    body = json.dumps({"query": "query {" + " ".join(parts) + "}"}).encode()
    req = urllib.request.Request(
        GRAPHQL_URL,
        data=body,
        method="POST",
        headers={"Authorization": f"Bearer {token}", "User-Agent": "awesome-agent-harness-sync"},
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_many(repos: list[tuple[str, str]], token: str | None = None) -> dict[tuple[str, str], dict[str, Any] | None]:
    """Metadata for every (owner, repo), keyed by the pair as given. None means GitHub could not find it
    (deleted, private, or renamed beyond a redirect). Raises if there is no token: GraphQL requires one."""
    token = token or get_token()
    if not token:
        raise RuntimeError("GraphQL needs a token: set GITHUB_TOKEN or run `gh auth login`.")
    unique = list(dict.fromkeys(repos))
    chunks = [unique[i : i + BATCH] for i in range(0, len(unique), BATCH)]

    def run(chunk: list[tuple[str, str]]) -> dict[tuple[str, str], dict[str, Any] | None]:
        for attempt in range(3):  # transient 502s happen on large GraphQL responses
            try:
                payload = _query(chunk, token)
                break
            except urllib.error.HTTPError as e:
                if e.code < 500 or attempt == 2:
                    raise
                time.sleep(2**attempt)
        other = [e for e in payload.get("errors", []) if e.get("type") != "NOT_FOUND"]
        if other:
            raise RuntimeError(f"GraphQL errors: {other[:3]}")
        data = payload.get("data") or {}
        return {key: data.get(f"r{i}") for i, key in enumerate(chunk)}  # NOT_FOUND leaves the alias null

    out: dict[tuple[str, str], dict[str, Any] | None] = {}
    # A few batches in parallel: GitHub asks for no more than ~4 concurrent requests per token (secondary limits).
    with ThreadPoolExecutor(max_workers=4) as pool:
        for part in pool.map(run, chunks):
            out.update(part)
    return out
