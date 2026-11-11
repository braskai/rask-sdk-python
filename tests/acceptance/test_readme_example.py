"""The worked example in the README, run as written against the mock."""

from __future__ import annotations

import http.server
import re
import threading
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
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


def worked_example() -> Any:
    readme = (REPO / "README.md").read_text(encoding="utf-8")
    found = re.search(r"<!-- worked example -->\s*```python\n(.*?)```", readme, re.S)
    assert found, "The README lost its worked example."
    return compile(found.group(1), "README.md", "exec")


def prepare(harness: Harness, download_url: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Script the mock for the example, with the dubbed video at download_url, and put the example's file in place."""
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


def test_worked_example(harness: Harness, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # The download goes to the harness's other host: the contract's example URL would leave this machine.
    prepare(harness, f"{harness.other_url}/download/talk.es.mp4?X-Amz-Signature=abc", tmp_path, monkeypatch)
    exec(worked_example(), {"__name__": "readme"})  # noqa: S102
    assert (tmp_path / "talk.es.mp4").exists()
    records = harness.get("/requests")["requests"]
    order = [record["operationId"] for record in records if record["listener"] == "api"]
    for name in ("createUpload", "completeUpload", "estimateProject", "createProject", "getJob", "listProjectOutputs"):
        assert name in order, name
    assert all(record["violations"] == [] for record in records), [r["violations"] for r in records]
    other = [record for record in records if record["listener"] == "other"]
    assert len(other) == 2, "The PUT and the download."
    assert all("authorization" not in record["headers"] for record in other)


class ExpiredLink(http.server.BaseHTTPRequestHandler):
    """What storage answers once a presigned link has expired."""

    def do_GET(self) -> None:
        body = b"<Error><Code>AccessDenied</Code><Message>Request has expired</Message></Error>"
        self.send_response(403)
        self.send_header("Content-Type", "application/xml")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        pass


def test_worked_example_does_not_save_an_error_as_the_video(
    harness: Harness, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    storage = http.server.HTTPServer(("127.0.0.1", 0), ExpiredLink)
    threading.Thread(target=storage.serve_forever, daemon=True).start()
    try:
        url = f"http://127.0.0.1:{storage.server_address[1]}/download/talk.es.mp4?X-Amz-Signature=abc"
        prepare(harness, url, tmp_path, monkeypatch)
        with pytest.raises(httpx.HTTPStatusError, match="403"):
            exec(worked_example(), {"__name__": "readme"})  # noqa: S102
    finally:
        storage.shutdown()
        storage.server_close()
    assert not (tmp_path / "talk.es.mp4").exists()
