"""Typed errors (S9). Everything the SDK raises on purpose is a RaskError."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import httpx


class RaskError(Exception):
    """Base of every SDK error. API errors carry the Problem, connection errors have status None.

    Every Problem member can be read as an attribute (`err.current_etag`, `err.required_minutes`, ...), including
    members this SDK version does not know. An unknown `code` is kept as is.
    """

    def __init__(
        self,
        message: str | None = None,
        *,
        status: int | None = None,
        problem: Any = None,
        headers: Mapping[str, str] | None = None,
        body: str | None = None,
        idempotency_key: str | None = None,
    ) -> None:
        self.status = status
        self.problem = problem
        self.headers: httpx.Headers = httpx.Headers(headers or {})
        self.body = body
        self.idempotency_key = idempotency_key
        self.request_id: str | None = self.headers.get("x-request-id")
        self.retry_after: float | None = parse_retry_after(self.headers.get("retry-after"))
        self.code: str | None = self._member("code")
        self.type: str | None = self._member("type")
        self.title: str | None = self._member("title")
        self.detail: str | None = self._member("detail")
        self.instance: str | None = self._member("instance")
        self.retryable: bool | None = self._member("retryable")
        self.retry: str | None = self._member("retry")
        self.recommendation: str | None = self._member("recommendation")
        self.hint: str | None = self._member("hint")
        self.errors: list[Any] = self._member("errors") or []
        super().__init__(message or self._default_message())

    def _member(self, name: str) -> Any:
        return getattr(self.problem, name, None) if self.problem is not None else None

    def _default_message(self) -> str:
        parts = [f"HTTP {self.status}" if self.status is not None else type(self).__name__]
        if self.code:
            parts.append(self.code)
        text = " ".join(parts)
        if self.detail:
            text += f": {self.detail}"
        if self.request_id:
            text += f" (request_id={self.request_id})"
        return text

    def __getattr__(self, name: str) -> Any:
        # Members of the Problem that are not attributes of their own: balance members, current_etag, policy, ...
        if name.startswith("__"):
            raise AttributeError(name)
        problem = self.__dict__.get("problem")
        if problem is not None and hasattr(problem, name):
            return getattr(problem, name)
        raise AttributeError(f"{type(self).__name__} has no attribute {name!r}")


class RaskConfigError(RaskError, ValueError):
    """The client or a call is misconfigured. Raised before anything is sent."""


class RaskConnectionError(RaskError):
    """The request never produced a response: the connection failed, and retrying did not help."""


class RaskTimeoutError(RaskConnectionError):
    """The request timed out, and retrying did not help."""


class RaskDecodeError(RaskError):
    """A successful status with a body that is not JSON."""


class BadRequestError(RaskError):
    """400."""


class AuthenticationError(RaskError):
    """401."""


class InsufficientBalanceError(RaskError):
    """402. `credit_type`, `required_minutes`, `balance_minutes`, `required_projects`, `balance_projects`."""


class PermissionDeniedError(RaskError):
    """403."""


class NotFoundError(RaskError):
    """404."""


class ConflictError(RaskError):
    """409: a state conflict, `cost_changed`, an exhausted `idempotency_key_in_use`."""


class GoneError(RaskError):
    """410."""


class PreconditionFailedError(RaskError):
    """412: the ETag is stale; `current_etag` holds the current one. Never retried automatically."""


class ContentTooLargeError(RaskError):
    """413."""


class UnsupportedMediaTypeError(RaskError):
    """415."""


class UnprocessableEntityError(RaskError):
    """422: `errors` lists each field."""


class PreconditionRequiredError(RaskError):
    """428: If-Match is missing. Never retried automatically."""


class RateLimitError(RaskError):
    """429: `retry_after`, `policy` and `window_seconds` say when and why."""


class ServerError(RaskError):
    """5xx."""


class InternalServerError(ServerError):
    """500. Not retried."""


class BadGatewayError(ServerError):
    """502, after retries."""


class ServiceUnavailableError(ServerError):
    """503, after retries."""


class GatewayTimeoutError(ServerError):
    """504, after retries."""


class JobFailedError(RaskError):
    """jobs.wait: the job failed. `job` is the job, and the attributes are its `error`."""

    def __init__(self, job: Any, **kwargs: Any) -> None:
        self.job = job
        problem = getattr(job, "error", None)
        text = f"Job {getattr(job, 'id', '?')} failed"
        if problem is not None:
            text += f": {getattr(problem, 'code', None)}: {getattr(problem, 'detail', None)}"
        super().__init__(text, status=getattr(problem, "status", None), problem=problem, **kwargs)


class JobCanceledError(RaskError):
    """jobs.wait: the job was canceled. `job` is the job."""

    def __init__(self, job: Any, **kwargs: Any) -> None:
        self.job = job
        problem = getattr(job, "error", None)
        text = f"Job {getattr(job, 'id', '?')} was canceled."
        super().__init__(text, status=getattr(problem, "status", None), problem=problem, **kwargs)


class WaitTimeoutError(RaskError):
    """jobs.wait: the job did not finish in time. The job keeps running; `job` is its last seen state, or None."""

    def __init__(self, message: str, *, job_id: str, job: Any = None) -> None:
        self.job_id = job_id
        self.job = job
        super().__init__(message)


class UploadError(RaskError):
    """uploads.upload_file: the PUT failed for good and the upload was not completed. `upload_id` is still open."""

    def __init__(self, message: str, *, upload_id: str | None = None, status: int | None = None) -> None:
        self.upload_id = upload_id
        super().__init__(message, status=status)


class RaskDeprecationWarning(FutureWarning):
    """The server sent a Deprecation header. Shown by default: the people who run the code should see it."""

    def __init__(self, operation_id: str, deprecation: str, sunset: str | None, link: str | None) -> None:
        self.operation_id = operation_id
        self.deprecation = deprecation
        self.sunset = sunset
        self.link = link
        text = f"The Rask API has deprecated {operation_id} (Deprecation: {deprecation})."
        if sunset:
            text += f" Sunset: {sunset}."
        if link:
            text += f" See {link}."
        super().__init__(text)


_BY_STATUS: dict[int, type[RaskError]] = {
    400: BadRequestError,
    401: AuthenticationError,
    402: InsufficientBalanceError,
    403: PermissionDeniedError,
    404: NotFoundError,
    409: ConflictError,
    410: GoneError,
    412: PreconditionFailedError,
    413: ContentTooLargeError,
    415: UnsupportedMediaTypeError,
    422: UnprocessableEntityError,
    428: PreconditionRequiredError,
    429: RateLimitError,
    500: InternalServerError,
    502: BadGatewayError,
    503: ServiceUnavailableError,
    504: GatewayTimeoutError,
}


def class_for_status(status: int) -> type[RaskError]:
    found = _BY_STATUS.get(status)
    if found is not None:
        return found
    return ServerError if status >= 500 else RaskError


def parse_retry_after(value: str | None) -> float | None:
    """Retry-After in seconds or as an HTTP date. None when absent or unreadable."""
    if value is None:
        return None
    value = value.strip()
    try:
        seconds = float(value)
    except ValueError:
        from datetime import UTC, datetime
        from email.utils import parsedate_to_datetime

        try:
            when = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
        return max(0.0, (when - datetime.now(UTC)).total_seconds())
    return seconds if seconds >= 0 else None


__all__ = [
    "AuthenticationError",
    "BadGatewayError",
    "BadRequestError",
    "ConflictError",
    "ContentTooLargeError",
    "GatewayTimeoutError",
    "GoneError",
    "InsufficientBalanceError",
    "InternalServerError",
    "JobCanceledError",
    "JobFailedError",
    "NotFoundError",
    "PermissionDeniedError",
    "PreconditionFailedError",
    "PreconditionRequiredError",
    "RaskConfigError",
    "RaskConnectionError",
    "RaskDecodeError",
    "RaskDeprecationWarning",
    "RaskError",
    "RaskTimeoutError",
    "RateLimitError",
    "ServerError",
    "ServiceUnavailableError",
    "UnprocessableEntityError",
    "UnsupportedMediaTypeError",
    "UploadError",
    "WaitTimeoutError",
]
