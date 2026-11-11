"""The owner's hard requirement, over real HTTP: every operation and helper stays under a custom base URL.

A harness with its own prefix (/rask/v3, the shape of https://gw.example.com/rask/v3), a base URL with a trailing slash,
all 82 operations through the public methods, and then the recording decides.
"""

from __future__ import annotations

import asyncio
import io
import subprocess
import sys
from collections.abc import Iterator
from typing import Any

import pytest

from rask_sdk import AsyncRask, Rask, RaskError
from rask_sdk._generated.operations import OPERATIONS

from .harness import REPO, Harness, find_harness
from .runner import kwargs_for, method_of, sweep_call

PREFIX = "/rask/v3"


@pytest.fixture(scope="module")
def harness() -> Iterator[Harness]:
    directory = find_harness()
    if directory is None:
        pytest.skip("rask-sdk-acceptance not found")
    instance = Harness(directory, prefix=PREFIX)
    yield instance
    instance.stop()


def _api_requests(harness: Harness) -> list[dict[str, Any]]:
    return [r for r in harness.get("/requests")["requests"] if r["listener"] == "api"]


def _assert_all_under_prefix(harness: Harness, expected_operations: set[str]) -> None:
    records = harness.get("/requests")["requests"]
    assert {record.get("operationId") for record in records if record["listener"] == "api"} >= expected_operations
    for record in records:
        if record["listener"] != "api":
            assert "authorization" not in record["headers"], f"The key reached the other host: {record['url']}"
            continue
        assert record["path"].startswith(f"{PREFIX}/"), record["path"]
        assert "//" not in record["path"], record["path"]
        assert record["headers"]["authorization"] == f"Bearer {harness.api_key}"
        assert record["violations"] == [], (record["operationId"], record["violations"])


def _sync_every_operation(client: Rask, operations: list[dict[str, Any]]) -> None:
    for operation in operations:
        sweep_call(client, operation)


async def _async_every_operation(client: AsyncRask, operations: list[dict[str, Any]]) -> None:
    for operation in operations:
        await sweep_call(client, operation)


def test_every_operation_sync(harness: Harness) -> None:
    harness.post("/reset")
    operations = harness.get("/operations")["operations"]
    assert len(operations) == 82
    with Rask(api_key=harness.api_key, base_url=f"{harness.base_url}/") as client:
        assert client.base_url == harness.base_url
        _sync_every_operation(client, operations)
    assert len(_api_requests(harness)) == 82
    _assert_all_under_prefix(harness, set(OPERATIONS))


def test_every_operation_async(harness: Harness) -> None:
    harness.post("/reset")
    operations = harness.get("/operations")["operations"]

    async def drive() -> None:
        async with AsyncRask(api_key=harness.api_key, base_url=f"{harness.base_url}/") as client:
            await _async_every_operation(client, operations)

    asyncio.run(drive())
    assert len(_api_requests(harness)) == 82
    _assert_all_under_prefix(harness, set(OPERATIONS))


def _script_helpers(harness: Harness) -> None:
    harness.post("/reset")
    harness.post("/jobs", {"id": "job-1", "type": "dub", "states": ["running", "succeeded"], "retry_after": 1})
    harness.post("/pages", {"operationId": "listProjects", "pages": 2, "page_size": 1})


def test_helpers_sync(harness: Harness) -> None:
    _script_helpers(harness)
    with Rask(api_key=harness.api_key, base_url=harness.base_url) as client:
        client.jobs.wait("job-1")
        assert len(list(client.projects.list_projects(limit=1))) == 2
        client.uploads.upload_file(io.BytesIO(b"x" * 1024), filename="a.mp4", content_type="video/mp4")
    _assert_all_under_prefix(harness, {"getJob", "listProjects", "createUpload", "completeUpload"})
    puts = [r for r in harness.get("/requests")["requests"] if r["operationId"] == "__storage_put"]
    assert len(puts) == 1
    assert "authorization" not in puts[0]["headers"]
    assert puts[0]["listener"] == "other"


def test_helpers_async(harness: Harness) -> None:
    _script_helpers(harness)

    async def drive() -> None:
        async with AsyncRask(api_key=harness.api_key, base_url=harness.base_url) as client:
            await client.jobs.wait("job-1")
            assert len([item async for item in await client.projects.list_projects(limit=1)]) == 2
            await client.uploads.upload_file(io.BytesIO(b"x" * 1024), filename="a.mp4", content_type="video/mp4")

    asyncio.run(drive())
    _assert_all_under_prefix(harness, {"getJob", "listProjects", "createUpload", "completeUpload"})
    puts = [r for r in harness.get("/requests")["requests"] if r["operationId"] == "__storage_put"]
    assert len(puts) == 1
    assert "authorization" not in puts[0]["headers"]


def test_env_var_reaches_every_helper(harness: Harness, monkeypatch: pytest.MonkeyPatch) -> None:
    _script_helpers(harness)
    monkeypatch.setenv("RASK_API_KEY", harness.api_key)
    monkeypatch.setenv("RASK_BASE_URL", f"{harness.base_url}/")
    with Rask() as client:
        assert client.base_url == harness.base_url
        client.jobs.wait("job-1")
        client.uploads.upload_file(io.BytesIO(b"x" * 1024), filename="a.mp4", content_type="video/mp4")
    _assert_all_under_prefix(harness, {"getJob", "createUpload", "completeUpload"})


def test_redirected_get_reaches_the_other_host_without_the_key(harness: Harness) -> None:
    harness.post("/reset")
    harness.post("/script", {"operationId": "getMe", "steps": [{"redirect": {"to": "other", "status": 307}}]})
    with Rask(api_key=harness.api_key, base_url=harness.base_url) as client:
        client.account.get_me()
    records = harness.get("/requests")["requests"]
    other = [record for record in records if record["listener"] == "other"]
    assert len(other) == 1, "The redirect was not followed, so this test proves nothing."
    assert "authorization" not in other[0]["headers"]
    assert "x-rask-client" not in other[0]["headers"]
    assert records[0]["headers"]["authorization"] == f"Bearer {harness.api_key}"


def test_redirected_post_is_refused_and_nothing_reaches_the_other_host(harness: Harness) -> None:
    harness.post("/reset")
    harness.post("/script", {"operationId": "createFolder", "steps": [{"redirect": {"to": "other", "status": 307}}]})
    with Rask(api_key=harness.api_key, base_url=harness.base_url) as client, pytest.raises(RaskError) as caught:
        client.projects.create_folder(name="x")
    assert caught.value.status == 307
    assert caught.value.headers["location"].startswith("http://localhost")
    assert [r for r in harness.get("/requests")["requests"] if r["listener"] == "other"] == []


def test_smoke_program(harness: Harness) -> None:
    """examples/smoke.py against the mock: the same program the lead runs on the test stand."""
    harness.post("/reset")
    harness.post("/pages", {"operationId": "listProjects", "pages": 2, "page_size": 2})
    done = subprocess.run(
        [sys.executable, str(REPO / "examples" / "smoke.py")],
        env={"RASK_API_KEY": harness.api_key, "RASK_BASE_URL": harness.base_url},
        cwd=REPO,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert done.returncode == 0, done.stdout + done.stderr
    lines = done.stdout.strip().splitlines()
    names = [line.split(" ")[0] for line in lines]
    assert names == [
        "getMe",
        "listLanguages",
        "listVoices(ru-ru)",
        "listProjects(page",
        "listProjects(page",
        "getBalance",
        "getPlan",
        "createFolder",
        "getFolder",
        "deleteFolder",
    ]
    assert all(" ok status=" in line for line in lines), lines
    assert harness.api_key not in done.stdout + done.stderr
    assert "status=204" in lines[-1]


def test_kwargs_for_covers_query_names() -> None:
    # `from` is a Python keyword, so the method takes from_ and the wire still says from.
    _, kwargs = kwargs_for("getUsage", {"query": {"from": "2026-01-01T00:00:00Z"}})
    assert kwargs == {"from_": "2026-01-01T00:00:00Z"}
    assert callable(method_of(Rask(api_key="x", base_url="http://127.0.0.1:1/v3"), "getUsage"))
