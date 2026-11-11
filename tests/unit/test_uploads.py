"""upload_file inputs the harness does not reach: unseekable streams, iterables, bad sizes."""

from __future__ import annotations

import asyncio
import io
import json
import threading
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest

from rask_sdk import AsyncRask, Rask, RaskConfigError, UploadError, models

KEY = "raskai_test_key_unit"
BASE = "https://gw.example.com/rask/v3"
PUT_URL = "https://s3.example.com/bucket/k?X-Amz-Signature=sig%2Fwith%2Fslash&X-Amz-Expires=3600"
DATA = bytes(range(256)) * 16


def upload_body(size: int, method: str = "single") -> dict[str, Any]:
    return {
        "object": "upload",
        "id": "upl-1",
        "status": "pending",
        "filename": "f.mp4",
        "content_type": "video/mp4",
        "size_bytes": size,
        "purpose": "project_source",
        "transfer": {
            "method": method,
            "url": PUT_URL,
            "headers": {"Content-Type": "video/mp4", "Content-Length": str(size)},
            "expires_at": "2026-11-03T10:00:00Z",
        },
        "media_id": None,
        "import_job_id": None,
        "expires_at": "2026-11-04T10:00:00Z",
        "created_at": "2026-11-03T09:00:00Z",
    }


class Backend:
    def __init__(self, put_status: int = 200, method: str = "single") -> None:
        self.put_status = put_status
        self.method = method
        self.calls: list[httpx.Request] = []
        self.created: dict[str, Any] = {}
        self.put_bytes = b""

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(request)
        if request.method == "PUT":
            self.put_bytes = request.read()
            return httpx.Response(self.put_status)
        if request.url.path.endswith("/uploads"):
            self.created = json.loads(request.content)
            return httpx.Response(201, json=upload_body(self.created["size_bytes"], self.method))
        return httpx.Response(202, json={"object": "job", "id": "job-1", "status": "queued"})


def client_for(backend: Backend) -> Rask:
    return Rask(api_key=KEY, base_url=BASE, http_client=httpx.Client(transport=httpx.MockTransport(backend)))


def test_path_gives_name_and_type(tmp_path: Path) -> None:
    path = tmp_path / "talk.mp4"
    path.write_bytes(DATA)
    backend = Backend()
    progress: list[tuple[int, int]] = []
    job = client_for(backend).uploads.upload_file(path, on_progress=lambda sent, total: progress.append((sent, total)))
    assert isinstance(job, models.Job)
    assert backend.created == {"filename": "talk.mp4", "content_type": "video/mp4", "size_bytes": len(DATA)}
    assert backend.put_bytes == DATA
    assert progress[-1] == (len(DATA), len(DATA))


def test_put_keeps_the_url_and_sends_signed_headers_only(tmp_path: Path) -> None:
    path = tmp_path / "talk.mp4"
    path.write_bytes(DATA)
    backend = Backend()
    client_for(backend).uploads.upload_file(path)
    put = next(request for request in backend.calls if request.method == "PUT")
    assert str(put.url) == PUT_URL
    assert "authorization" not in put.headers
    assert "x-rask-client" not in put.headers
    assert "idempotency-key" not in put.headers
    assert put.headers["content-type"] == "video/mp4"
    assert put.headers["content-length"] == str(len(DATA))
    assert "transfer-encoding" not in put.headers


def test_a_server_supplied_authorization_never_goes_out(tmp_path: Path) -> None:
    path = tmp_path / "a.mp4"
    path.write_bytes(DATA)
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.method == "PUT":
            request.read()
            return httpx.Response(200)
        if request.url.path.endswith("/uploads"):
            body = upload_body(len(DATA))
            body["transfer"]["headers"]["Authorization"] = "Bearer injected"
            return httpx.Response(201, json=body)
        return httpx.Response(202, json={"object": "job", "id": "j"})

    Rask(
        api_key=KEY, base_url=BASE, http_client=httpx.Client(transport=httpx.MockTransport(handler))
    ).uploads.upload_file(path)
    assert "authorization" not in next(request for request in seen if request.method == "PUT").headers


def test_unseekable_stream_is_spooled() -> None:
    class Pipe(io.RawIOBase):
        def __init__(self) -> None:
            self.inner = io.BytesIO(DATA)

        def readable(self) -> bool:
            return True

        def seekable(self) -> bool:
            return False

        def read(self, size: int = -1) -> bytes:
            return self.inner.read(size)

    backend = Backend()
    client_for(backend).uploads.upload_file(Pipe(), filename="p.mp4", content_type="video/mp4")  # type: ignore[arg-type]
    assert backend.created["size_bytes"] == len(DATA)
    assert backend.put_bytes == DATA


def test_stream_uploads_from_its_current_position() -> None:
    stream = io.BytesIO(b"skip" + DATA)
    stream.seek(4)
    backend = Backend()
    client_for(backend).uploads.upload_file(stream, filename="s.mp4", content_type="video/mp4")
    assert backend.created["size_bytes"] == len(DATA)
    assert backend.put_bytes == DATA


def test_iterable_of_bytes() -> None:
    def chunks() -> Iterator[bytes]:
        yield DATA[:100]
        yield DATA[100:]

    backend = Backend()
    client_for(backend).uploads.upload_file(chunks(), filename="i.mp4", content_type="video/mp4")
    assert backend.put_bytes == DATA


def test_async_iterable_and_path(tmp_path: Path) -> None:
    async def chunks() -> AsyncIterator[bytes]:
        yield DATA[:100]
        yield DATA[100:]

    backend = Backend()
    path = tmp_path / "a.mp4"
    path.write_bytes(DATA)

    async def drive() -> None:
        async with httpx.AsyncClient(transport=httpx.MockTransport(backend)) as http:
            client = AsyncRask(api_key=KEY, base_url=BASE, http_client=http)
            await client.uploads.upload_file(chunks(), filename="i.mp4", content_type="video/mp4")
            first = backend.put_bytes
            assert first == DATA
            await client.uploads.upload_file(path)
            assert backend.put_bytes == DATA

    asyncio.run(drive())


def test_size_mismatch_is_refused_before_anything_is_sent() -> None:
    backend = Backend()
    with pytest.raises(RaskConfigError, match="size_bytes"):
        client_for(backend).uploads.upload_file(
            io.BytesIO(DATA), filename="s.mp4", content_type="video/mp4", size_bytes=1
        )
    assert backend.calls == []


def test_empty_file_is_refused() -> None:
    with pytest.raises(RaskConfigError, match="empty"):
        client_for(Backend()).uploads.upload_file(io.BytesIO(b""), filename="s.mp4", content_type="video/mp4")


def test_stream_needs_a_name_and_a_guessable_type() -> None:
    with pytest.raises(RaskConfigError, match="filename"):
        client_for(Backend()).uploads.upload_file(io.BytesIO(DATA))
    with pytest.raises(RaskConfigError, match="content_type"):
        client_for(Backend()).uploads.upload_file(io.BytesIO(DATA), filename="noextension")


def test_opened_file_is_closed_after_a_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "a.mp4"
    path.write_bytes(DATA)
    backend = Backend(put_status=400)
    opened: list[Any] = []
    original = Path.open

    def spy(self: Path, *args: Any, **kwargs: Any) -> Any:
        handle = original(self, *args, **kwargs)
        if self == path:
            opened.append(handle)
        return handle

    monkeypatch.setattr(Path, "open", spy)
    with pytest.raises(UploadError) as caught:
        client_for(backend).uploads.upload_file(path)
    assert caught.value.status == 400
    assert caught.value.upload_id == "upl-1"
    assert opened[0].closed
    assert sum(request.method == "PUT" for request in backend.calls) == 1, "A refused signature is not worth repeating."


def test_a_stream_of_the_caller_stays_open() -> None:
    stream = io.BytesIO(DATA)
    client_for(Backend()).uploads.upload_file(stream, filename="s.mp4", content_type="video/mp4")
    assert not stream.closed


def test_text_stream_is_refused() -> None:
    with pytest.raises(RaskConfigError, match="binary"):
        client_for(Backend()).uploads.upload_file(io.StringIO("text"), filename="s.mp4", content_type="video/mp4")  # type: ignore[arg-type]


def test_on_progress_runs_on_the_event_loop_thread(tmp_path: Path) -> None:
    path = tmp_path / "a.mp4"
    path.write_bytes(DATA)
    threads: set[int] = set()

    async def drive() -> int:
        async with httpx.AsyncClient(transport=httpx.MockTransport(Backend())) as http:
            client = AsyncRask(api_key=KEY, base_url=BASE, http_client=http)
            await client.uploads.upload_file(path, on_progress=lambda sent, total: threads.add(threading.get_ident()))
        return threading.get_ident()

    loop_thread = asyncio.run(drive())
    assert threads == {loop_thread}


def test_unknown_transfer_method_says_to_upgrade() -> None:
    backend = Backend(method="multipart")
    with pytest.raises(UploadError, match="multipart"):
        client_for(backend).uploads.upload_file(io.BytesIO(DATA), filename="s.mp4", content_type="video/mp4")
    assert all(request.method != "PUT" for request in backend.calls)
