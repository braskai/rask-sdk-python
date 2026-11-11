"""What uploads.upload_file reads from: a path or a stream, always restartable for the whole-file retries (S7)."""

from __future__ import annotations

import io
import mimetypes
import os
import tempfile
from collections.abc import AsyncIterable, Callable, Iterable, Iterator
from pathlib import Path
from typing import IO, Any

from .errors import RaskConfigError, UploadError

CHUNK_BYTES = 1024 * 1024
_SPOOL_IN_MEMORY_BYTES = 8 * 1024 * 1024

ProgressCallback = Callable[[int, int], None]


class UploadSource:
    def __init__(self, stream: IO[bytes], start: int, size: int, filename: str, content_type: str, owned: bool) -> None:
        self._stream = stream
        self._start = start
        self._owned = owned
        self.size = size
        self.filename = filename
        self.content_type = content_type

    def chunks(self) -> Iterator[bytes]:
        """The whole file from its start. Each call starts over."""
        self._stream.seek(self._start)
        sent = 0
        while sent < self.size:
            chunk = self._stream.read(min(CHUNK_BYTES, self.size - sent))
            if not chunk:
                raise UploadError(f"The source ended after {sent} bytes; {self.size} were declared.")
            sent += len(chunk)
            yield chunk

    def close(self) -> None:
        if self._owned:
            self._stream.close()


def _spool(chunks: Iterable[bytes]) -> Any:
    spool = tempfile.SpooledTemporaryFile(max_size=_SPOOL_IN_MEMORY_BYTES)  # noqa: SIM115
    try:
        for chunk in chunks:
            spool.write(chunk)
        spool.seek(0)
    except BaseException:
        # Nobody else holds the copy yet. Left to the garbage collector, it keeps its descriptor and its disk space
        # for as long as the error is held.
        spool.close()
        raise
    return spool


async def spool_async(chunks: AsyncIterable[bytes]) -> Any:
    spool = tempfile.SpooledTemporaryFile(max_size=_SPOOL_IN_MEMORY_BYTES)  # noqa: SIM115
    try:
        async for chunk in chunks:
            spool.write(chunk)
        spool.seek(0)
    except BaseException:
        spool.close()
        raise
    return spool


def _rewinds(stream: Any) -> bool:
    """Whether a stream can be read again from where it stands.

    One may say it seeks and then fail to: a wrapper can answer seekable() without asking what it wraps. Nothing has
    been read from it when the seek fails here, so it can still be copied like any other stream that does not seek.
    """
    seekable = getattr(stream, "seekable", None)
    if seekable is None or not seekable():
        return False
    try:
        stream.seek(stream.tell())
    except OSError:
        return False
    return True


def _guess_content_type(filename: str, content_type: str | None) -> str:
    if content_type:
        return content_type
    guessed, _ = mimetypes.guess_type(filename)
    if not guessed:
        raise RaskConfigError(f"Cannot tell the content type of {filename!r}: pass content_type=...")
    return guessed


def open_source(
    source: str | os.PathLike[str] | IO[bytes] | Iterable[bytes],
    *,
    filename: str | None,
    content_type: str | None,
    size_bytes: int | None,
    owned: bool = False,
) -> UploadSource:
    """Open a path, take a seekable stream, or spool anything else: the size must be known and the file sent again."""
    stream: Any
    if isinstance(source, str | os.PathLike):
        path = Path(source)
        filename = filename or path.name
        stream, owned = path.open("rb"), True
    elif isinstance(source, io.TextIOBase):
        raise RaskConfigError("upload_file needs a binary stream: open the file with 'rb'.")
    elif hasattr(source, "read"):
        stream = source
        name = getattr(source, "name", None)
        if not filename and not owned and isinstance(name, str):
            filename = os.path.basename(name)
        if not _rewinds(stream):
            stream, owned = _spool(iter(lambda: source.read(CHUNK_BYTES), b"")), True
    elif isinstance(source, Iterable) and not isinstance(source, bytes | bytearray):
        stream, owned = _spool(source), True
    else:
        raise RaskConfigError("upload_file takes a path or a binary stream.")

    try:
        if not filename:
            raise RaskConfigError("A stream has no file name: pass filename=...")
        start = stream.tell()
        stream.seek(0, os.SEEK_END)
        size = stream.tell() - start
        stream.seek(start)
        if size_bytes is not None and size_bytes != size:
            raise RaskConfigError(f"size_bytes is {size_bytes} but the source holds {size} bytes.")
        if size <= 0:
            raise RaskConfigError(f"{filename!r} is empty.")
        return UploadSource(stream, start, size, filename, _guess_content_type(filename, content_type), owned)
    except BaseException:
        if owned:
            stream.close()
        raise
