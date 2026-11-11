#!/usr/bin/env python3
"""Check an environment end to end. Reads RASK_API_KEY and RASK_BASE_URL, prints one line per call, and no secrets.

    RASK_API_KEY=... RASK_BASE_URL=https://.../v3 uv run python examples/smoke.py

Creates one folder and deletes it again, so it refuses to run without RASK_BASE_URL: the default is production.
Exits 1 if any call failed, 2 if it could not start.
"""

from __future__ import annotations

import os
import sys
import uuid
from collections.abc import Callable
from typing import Any

from rask_sdk import Rask, RaskError

failed = 0


def run(name: str, call: Callable[[], Any]) -> Any:
    """Make one call and print its outcome."""
    global failed
    try:
        result = call()
    except RaskError as error:
        failed += 1
        print(f"{name} FAILED status={error.status} code={error.code} request_id={error.request_id}")
        return None
    http = result.http
    print(f"{name} ok status={http.status} request_id={http.request_id}")
    return result


def main() -> int:
    if not os.environ.get("RASK_BASE_URL", "").strip():
        print("Cannot start: set RASK_BASE_URL. This program writes, and the default base URL is production.")
        return 2
    try:
        client = Rask()
    except RaskError as error:
        print(f"Cannot start: {error}")
        return 2
    with client:
        run("getMe", client.account.get_me)
        run("listLanguages", client.catalogs.list_languages)
        run("listVoices(ru-ru)", lambda: client.voices.list_voices(target_language="ru-ru"))
        first = run("listProjects(page 1)", lambda: client.projects.list_projects(limit=2))
        if first is not None and first.next_cursor:
            run("listProjects(page 2)", first.next_page)
        elif first is not None:
            print("listProjects(page 2) skipped: there is only one page")
        run("getBalance", client.account.get_balance)
        run("getPlan", client.account.get_plan)
        folder = run(
            "createFolder", lambda: client.projects.create_folder(name=f"sdk-smoke-python-{uuid.uuid4().hex[:8]}")
        )
        if folder is not None:
            run("getFolder", lambda: client.projects.get_folder(folder.id))
            run("deleteFolder", lambda: client.projects.delete_folder(folder.id))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
