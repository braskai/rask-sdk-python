"""The hand-written helpers on client.jobs and client.uploads."""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncIterable, Iterable
from typing import IO

from ._flow import DEFAULT_WAIT_TIMEOUT_SECONDS, upload_flow, wait_flow
from ._generated import models as _m
from ._resource import AsyncResource, SyncResource
from ._source import ProgressCallback, open_source, spool_async

UploadInput = str | os.PathLike[str] | IO[bytes] | Iterable[bytes]


class SyncJobsHelpers(SyncResource):
    def wait(self, job_id: str, *, timeout: float = DEFAULT_WAIT_TIMEOUT_SECONDS) -> _m.Job:
        """Wait for a job to end, and return it when it succeeded.

        Args:
            job_id: The job.
            timeout: Seconds to wait for. The default, 4 h, covers the longest jobs.

        Raises:
            JobFailedError: The job failed; the error carries the job's problem.
            JobCanceledError: The job was canceled.
            WaitTimeoutError: The job did not finish in time. It keeps running.
        """
        result: _m.Job = self._client._run(wait_flow(self._client._config, job_id, timeout=timeout))
        return result


class AsyncJobsHelpers(AsyncResource):
    async def wait(self, job_id: str, *, timeout: float = DEFAULT_WAIT_TIMEOUT_SECONDS) -> _m.Job:
        """Wait for a job to end, and return it when it succeeded.

        Args:
            job_id: The job.
            timeout: Seconds to wait for. The default, 4 h, covers the longest jobs.

        Raises:
            JobFailedError: The job failed; the error carries the job's problem.
            JobCanceledError: The job was canceled.
            WaitTimeoutError: The job did not finish in time. It keeps running.
        """
        result: _m.Job = await self._client._run(wait_flow(self._client._config, job_id, timeout=timeout))
        return result


class SyncUploadsHelpers(SyncResource):
    def upload_file(
        self,
        source: UploadInput,
        *,
        filename: str | None = None,
        content_type: str | None = None,
        size_bytes: int | None = None,
        purpose: _m.MediaPurposeKnown | None = None,
        on_progress: ProgressCallback | None = None,
    ) -> _m.Job:
        """Upload a file: createUpload, one PUT without the API key, completeUpload.

        Returns the `media_import` job. Wait for it, then use `job.result.media_id`.

        Args:
            source: A path, a binary stream, or an iterable of bytes (spooled to a temporary file first).
            filename: Defaults to the file's name.
            content_type: Defaults to a guess from the file name.
            size_bytes: Optional check against what the source holds.
            purpose: `project_source` by default.
            on_progress: Called with (bytes sent, total bytes). A retried PUT starts from zero again.

        Raises:
            UploadError: The PUT failed for good. The upload was not completed.
        """
        opened = open_source(source, filename=filename, content_type=content_type, size_bytes=size_bytes)
        try:
            flow = upload_flow(self._client._config, opened, purpose=purpose, on_progress=on_progress)
            result: _m.Job = self._client._run(flow)
            return result
        finally:
            opened.close()


class AsyncUploadsHelpers(AsyncResource):
    async def upload_file(
        self,
        source: UploadInput | AsyncIterable[bytes],
        *,
        filename: str | None = None,
        content_type: str | None = None,
        size_bytes: int | None = None,
        purpose: _m.MediaPurposeKnown | None = None,
        on_progress: ProgressCallback | None = None,
    ) -> _m.Job:
        """Upload a file: createUpload, one PUT without the API key, completeUpload.

        Returns the `media_import` job. Wait for it, then use `job.result.media_id`.

        Args:
            source: A path, a binary stream, or an (async) iterable of bytes (spooled to a temporary file first).
            filename: Defaults to the file's name.
            content_type: Defaults to a guess from the file name.
            size_bytes: Optional check against what the source holds.
            purpose: `project_source` by default.
            on_progress: Called with (bytes sent, total bytes). A retried PUT starts from zero again.

        Raises:
            UploadError: The PUT failed for good. The upload was not completed.
        """
        readable: UploadInput
        spooled = isinstance(source, AsyncIterable)
        if isinstance(source, AsyncIterable):
            readable = await spool_async(source)
        else:
            readable = source
        opened = await asyncio.to_thread(
            open_source,
            readable,
            filename=filename,
            content_type=content_type,
            size_bytes=size_bytes,
            owned=spooled,
        )
        try:
            flow = upload_flow(self._client._config, opened, purpose=purpose, on_progress=on_progress)
            result: _m.Job = await self._client._run(flow)
            return result
        finally:
            await asyncio.to_thread(opened.close)
