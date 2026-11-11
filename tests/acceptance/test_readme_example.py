"""The worked example in the README, run as written against the mock."""

from __future__ import annotations

import re
from collections.abc import Iterator
from pathlib import Path

import pytest

from .harness import REPO, Harness, find_harness


@pytest.fixture(scope="module")
def harness() -> Iterator[Harness]:
    directory = find_harness()
    if directory is None:
        pytest.skip("rask-sdk-acceptance not found")
    instance = Harness(directory)
    yield instance
    instance.stop()


def test_worked_example(harness: Harness, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    readme = (REPO / "README.md").read_text(encoding="utf-8")
    found = re.search(r"<!-- worked example -->\s*```python\n(.*?)```", readme, re.S)
    assert found, "The README lost its worked example."
    harness.post("/reset")
    harness.post(
        "/jobs",
        {
            "id": "job-import",
            "type": "media_import",
            "states": ["running", "succeeded"],
            "result": {
                "type": "media_import",
                "media_id": "med-1",
                "kind": "video",
                "duration_seconds": 1.0,
                "size_bytes": 2048,
            },
        },
    )
    harness.post("/script", {"operationId": "completeUpload", "steps": [{"job": "job-import"}]})
    harness.post("/jobs", {"id": "job-project", "type": "project_processing", "states": ["succeeded"]})
    harness.post("/script", {"operationId": "createProject", "steps": [{"job": "job-project"}]})
    # The download goes to the harness's other host: the contract's example URL would leave this machine.
    download_url = f"{harness.other_url}/download/talk.es.mp4?X-Amz-Signature=abc"
    harness.post(
        "/script",
        {
            "operationId": "listProjectOutputs",
            "steps": [
                {
                    "body": {
                        "object": "list",
                        "has_more": False,
                        "next_cursor": None,
                        "data": [
                            {
                                "object": "output",
                                "kind": "dubbed_video",
                                "format": "mp4",
                                "language": "es-es",
                                "available": True,
                                "unavailable_reason": None,
                                "outdated": False,
                                "version": None,
                                "url": download_url,
                                "expires_at": "2026-11-03T10:00:00Z",
                                "subtitles_request": None,
                                "filename": "talk.es.mp4",
                            }
                        ],
                    }
                }
            ],
        },
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("RASK_API_KEY", harness.api_key)
    monkeypatch.setenv("RASK_BASE_URL", harness.base_url)
    (tmp_path / "talk.mp4").write_bytes(b"\0" * 2048)
    exec(compile(found.group(1), "README.md", "exec"), {"__name__": "readme"})  # noqa: S102
    assert (tmp_path / "talk.es.mp4").exists()
    records = harness.get("/requests")["requests"]
    order = [record["operationId"] for record in records if record["listener"] == "api"]
    for name in ("createUpload", "completeUpload", "estimateProject", "createProject", "getJob", "listProjectOutputs"):
        assert name in order, name
    assert all(record["violations"] == [] for record in records), [r["violations"] for r in records]
    other = [record for record in records if record["listener"] == "other"]
    assert len(other) == 2, "The PUT and the download."
    assert all("authorization" not in record["headers"] for record in other)
