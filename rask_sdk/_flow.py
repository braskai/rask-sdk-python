"""What every call does, written once for the sync and the async client.

A flow is a generator that yields effects (send this request, sleep, PUT this file) and is resumed with their results.
The clients only run the effects, so retries, redirects, the job waiter and the upload helper cannot differ.
"""

from __future__ import annotations

import json
import math
import random
import time
import uuid
from collections.abc import Generator, Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Any
from urllib.parse import quote

import httpx

from ._config import CLIENT_HEADER, USER_AGENT, ClientConfig, redact_headers, redact_url
from ._generated import models as _m
from ._generated import operations as _ops
from ._models import HttpInfo, NoContent, NotModified, RaskModel
from ._operation import Operation, RequestOptions
from ._source import UploadSource
from .errors import (
    JobCanceledError,
    JobFailedError,
    RaskConfigError,
    RaskConnectionError,
    RaskDecodeError,
    RaskError,
    RaskTimeoutError,
    UploadError,
    WaitTimeoutError,
    class_for_status,
    parse_retry_after,
)

RETRY_STATUSES = frozenset({429, 502, 503, 504})
RETRY_BUDGET_SECONDS = 120.0
BACKOFF_FIRST_SECONDS = 0.5
BACKOFF_MAX_SECONDS = 30.0
BACKOFF_FACTOR = 1.5
KEY_IN_USE_RETRIES = 3
MAX_REDIRECTS = 3
MIN_ATTEMPT_TIMEOUT_SECONDS = 1.0

DEFAULT_WAIT_TIMEOUT_SECONDS = 14400.0
WAIT_SECONDS = 20
DEFAULT_POLL_SECONDS = 1.0

PUT_RETRIES = 3
MAX_RESIGNS = 3

_NOT_RETRYABLE = (httpx.UnsupportedProtocol, httpx.LocalProtocolError, httpx.TooManyRedirects)


@dataclass(slots=True)
class Send:
    request: httpx.Request


@dataclass(slots=True)
class Sleep:
    seconds: float


@dataclass(slots=True)
class PutFile:
    url: str
    headers: httpx.Headers
    source: UploadSource
    timeout: float
    on_progress: Any


Flow = Generator[Send | Sleep | PutFile, Any, Any]


@dataclass(slots=True)
class RawResult:
    status: int
    headers: httpx.Headers
    content: bytes
    idempotency_key: str | None


def backoff_seconds(attempt: int) -> float:
    """Exponential from 0.5 s to 30 s, factor 1.5, with jitter."""
    return min(BACKOFF_FIRST_SECONDS * BACKOFF_FACTOR**attempt, BACKOFF_MAX_SECONDS) * random.uniform(0.75, 1.25)


def to_jsonable(value: Any) -> Any:
    """Request data as JSON values. None means "not set" at any depth: requests never carry a null (S12)."""
    if isinstance(value, RaskModel):
        return value.to_dict()
    if isinstance(value, Mapping):
        return {key: to_jsonable(item) for key, item in value.items() if item is not None}
    if isinstance(value, str | bytes):
        return value
    if isinstance(value, Sequence):
        return [to_jsonable(item) for item in value if item is not None]
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    return value


def _query_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Enum):
        return str(value.value)
    if isinstance(value, Sequence) and not isinstance(value, str | bytes):
        return ",".join(_query_value(item) for item in value)  # arrays are style=form, explode=false
    return str(value)


def _encode_query(query: Mapping[str, Any]) -> str:
    pairs = (
        f"{quote(name, safe='')}={quote(_query_value(value), safe=',')}"
        for name, value in query.items()
        if value is not None
    )
    return "&".join(pairs)


def _path_value(op: Operation, name: str, value: Any) -> str:
    text = str(value)
    # An empty or dot segment would silently address another operation.
    if text in ("", ".", ".."):
        raise RaskConfigError(f"{op.method_name}: {name} cannot be {text!r}.")
    return quote(text, safe="")


def _build_url(config: ClientConfig, op: Operation, path: Mapping[str, Any] | None, query: Mapping[str, Any]) -> str:
    rendered = op.path
    for name in op.path_params:
        rendered = rendered.replace("{" + name + "}", _path_value(op, name, (path or {})[name]))
    # String concatenation, never URL resolution: resolving a path that starts with "/" would drop the base path.
    url = config.base_url + rendered
    encoded = _encode_query(query)
    return f"{url}?{encoded}" if encoded else url


def _encode_body(op: Operation, body: Any, extra_body: Mapping[str, Any] | None) -> bytes | None:
    if op.body_mode == "root":
        payload = to_jsonable(body)
    else:
        payload = to_jsonable(body or {})
    if extra_body:
        payload = {**(payload or {}), **to_jsonable(extra_body)}
    if not payload and not op.body_required:
        return None
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _follow_redirects(
    config: ClientConfig,
    op: Operation,
    url: str,
    trusted_headers: httpx.Headers,
    content: bytes | None,
    timeout: float,
) -> Flow:
    """One attempt, redirects included. The key goes to URLs under base_url and nowhere else."""
    public_headers = httpx.Headers({"Accept": trusted_headers["Accept"], "User-Agent": USER_AGENT})
    method = op.http_method
    extensions = {"timeout": httpx.Timeout(timeout).as_dict()}
    for hop in range(MAX_REDIRECTS + 1):
        trusted = config.is_under_base(url)
        headers = trusted_headers if trusted else public_headers
        request = httpx.Request(method, url, headers=headers, content=content, extensions=extensions)
        config.log("> %s %s %s", method, redact_url(url), redact_headers(headers))
        started = time.monotonic()
        response: httpx.Response = yield Send(request)
        config.log(
            "< %s %s %s (%.0f ms, request id %s)",
            response.status_code,
            method,
            redact_url(url),
            (time.monotonic() - started) * 1000,
            response.headers.get("x-request-id"),
        )
        location = response.headers.get("location")
        status = response.status_code
        if status not in (301, 302, 303, 307, 308) or not location or hop == MAX_REDIRECTS:
            return response
        target = str(httpx.URL(url).join(location))
        if httpx.URL(target).scheme not in ("http", "https"):
            return response
        safe_method = method in ("GET", "HEAD")
        # 301, 302 and 303 turn a write into a GET, which would quietly answer something else. A write that leaves the
        # API is not followed at all: without the key it cannot succeed, and it would hand the body to another host.
        if not safe_method and (status not in (307, 308) or not config.is_under_base(target)):
            return response
        config.log(
            "  redirect to %s%s", redact_url(target), "" if config.is_under_base(target) else " (without the key)"
        )
        url = target
    raise AssertionError("unreachable")  # pragma: no cover


def _connection_error(
    config: ClientConfig, op: Operation, url: str, exc: Exception, key: str | None
) -> RaskConnectionError:
    kind = RaskTimeoutError if isinstance(exc, httpx.TimeoutException) else RaskConnectionError
    what = "timed out" if kind is RaskTimeoutError else f"failed ({type(exc).__name__})"
    error = kind(f"{op.id} {what}: {redact_url(url)}", idempotency_key=key)
    error.__cause__ = exc
    return error


def error_from_response(response: httpx.Response, key: str | None) -> RaskError:
    problem = None
    try:
        data = json.loads(response.content)
    except ValueError:
        data = None
    if isinstance(data, Mapping):
        problem = _m.Problem.model_validate(data)
    return class_for_status(response.status_code)(
        status=response.status_code,
        problem=problem,
        headers=response.headers,
        body=None if problem is not None else response.text,
        idempotency_key=key,
    )


def request_flow(
    config: ClientConfig,
    op: Operation,
    *,
    path: Mapping[str, Any] | None = None,
    query: Mapping[str, Any] | None = None,
    body: Any = None,
    if_match: str | None = None,
    if_none_match: str | None = None,
    idempotency_key: str | None = None,
    options: RequestOptions | None = None,
) -> Flow:
    """One logical call: the Idempotency-Key, the retries (S3, S4), the typed error. Returns a RawResult."""
    options = options or {}
    if op.if_match == "required" and if_match is None:
        raise RaskConfigError(f"{op.method_name} requires if_match: the ETag you read, quotes included.")
    key = (idempotency_key or str(uuid.uuid4())) if op.idempotency != "none" else None

    url = _build_url(config, op, path, {**(query or {}), **(options.get("extra_query") or {})})
    content = _encode_body(op, body, options.get("extra_body"))
    headers = httpx.Headers({"Accept": "application/json", "Accept-Encoding": "gzip, deflate"})
    headers["User-Agent"] = USER_AGENT
    headers["X-Rask-Client"] = CLIENT_HEADER
    headers.update(config.default_headers)
    if key is not None:
        headers["Idempotency-Key"] = key
    if if_match is not None:
        headers["If-Match"] = if_match
    if if_none_match is not None:
        headers["If-None-Match"] = if_none_match
    if content is not None:
        headers["Content-Type"] = "application/json"
    headers.update(options.get("extra_headers") or {})
    headers["Authorization"] = f"Bearer {config.api_key.reveal()}"

    timeout = float(options.get("timeout", config.timeout))
    started = time.monotonic()
    attempt = 0
    key_in_use_retries = 0
    while True:
        # The first attempt gets the whole timeout; a retry may not run past the budget.
        remaining = RETRY_BUDGET_SECONDS - (time.monotonic() - started)
        attempt_timeout = timeout if attempt == 0 else max(min(timeout, remaining), MIN_ATTEMPT_TIMEOUT_SECONDS)
        retry_after: float | None = None
        failure: RaskError
        try:
            response = yield from _follow_redirects(config, op, url, headers, content, attempt_timeout)
        except httpx.RequestError as exc:
            failure = _connection_error(config, op, url, exc, key)
            retry = not isinstance(exc, _NOT_RETRYABLE)
        else:
            config.warn_deprecated(op.id, response.headers)
            status = response.status_code
            if 200 <= status < 300 or (status == 304 and if_none_match is not None):
                return RawResult(status, response.headers, response.content, key)
            failure = error_from_response(response, key)
            retry_after = failure.retry_after
            if status == 409 and failure.code == "idempotency_key_in_use":
                key_in_use_retries += 1
                retry = key_in_use_retries <= KEY_IN_USE_RETRIES
            else:
                # 500, 409 state conflicts, 412 and 428 are answers, not hiccups: a repeat would not change them.
                retry = status in RETRY_STATUSES
        if not retry or (config.max_retries is not None and attempt >= config.max_retries):
            raise failure
        delay = backoff_seconds(attempt)
        if retry_after is not None:
            delay = max(delay, retry_after)
        # A wait that cannot be honoured inside the budget ends the retries now rather than after a useless sleep.
        if time.monotonic() - started + delay > RETRY_BUDGET_SECONDS:
            raise failure
        attempt += 1
        config.log("  retry %s of %s in %.2f s: %s", attempt, op.id, delay, failure)
        yield Sleep(delay)


def decode_result(raw: RawResult, model: type[Any]) -> Any:
    """A RawResult as the operation's model, carrying its status and headers."""
    info = HttpInfo(raw.status, raw.headers, raw.content, raw.idempotency_key)
    result: RaskModel
    if raw.status == 304:
        result = NotModified()
    elif model is NoContent or not raw.content:
        if model is not NoContent:
            raise RaskDecodeError(
                f"HTTP {raw.status} with an empty body, expected {model.__name__}.",
                status=raw.status,
                headers=raw.headers,
                idempotency_key=raw.idempotency_key,
            )
        result = NoContent()
    else:
        try:
            result = model.model_validate(json.loads(raw.content))
        except ValueError as exc:  # both a JSON decode error and a body that is not even an object
            raise RaskDecodeError(
                f"HTTP {raw.status} with a body that is not a {model.__name__}: {exc}",
                status=raw.status,
                headers=raw.headers,
                body=raw.content.decode("utf-8", "replace"),
                idempotency_key=raw.idempotency_key,
            ) from exc
    result._http = info
    return result


def call_flow(config: ClientConfig, op: Operation, model: type[Any], **kwargs: Any) -> Flow:
    raw = yield from request_flow(config, op, **kwargs)
    return decode_result(raw, model)


def _is_retryable_failure(error: RaskError) -> bool:
    return isinstance(error, RaskConnectionError) or error.status in RETRY_STATUSES


def wait_flow(config: ClientConfig, job_id: str, *, timeout: float) -> Flow:
    """S6: loop wait_seconds=20 until the job ends. Leaves the job running on a timeout."""
    deadline = time.monotonic() + timeout
    # A shorter client timeout must not cut every long-poll short.
    longest_poll = min(WAIT_SECONDS, max(1, int(config.timeout) - 5))
    etag: str | None = None
    job: Any = None
    outage = 0
    last_failure: RaskError | None = None

    def timed_out(cause: RaskError | None = None) -> WaitTimeoutError:
        error = WaitTimeoutError(f"Job {job_id} did not finish in {timeout:g} s.", job_id=job_id, job=job)
        error.__cause__ = cause
        return error

    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise timed_out(last_failure)
        try:
            result = yield from call_flow(
                config,
                _ops.getJob,
                _m.Job,
                path={"job_id": job_id},
                query={"wait_seconds": min(longest_poll, max(1, math.ceil(remaining)))},
                if_none_match=etag,
            )
        except RaskError as exc:
            # request_flow has already spent its 120 s on this. An outage is no reason to give up a 4 h wait.
            if not _is_retryable_failure(exc):
                raise
            delay = backoff_seconds(outage)
            outage += 1
            last_failure = exc
            if time.monotonic() + delay >= deadline:
                raise timed_out(exc) from exc
            yield Sleep(delay)
            continue
        outage = 0
        last_failure = None
        if not isinstance(result, NotModified):
            job = result
            etag = result.http.etag
            status = job.status
            if status == "succeeded":
                return job
            if status == "failed":
                raise JobFailedError(job, headers=result.http.headers)
            if status == "canceled":
                raise JobCanceledError(job, headers=result.http.headers)
            # Anything else, a status this version has never seen included, is still running (S12).
        hint = parse_retry_after(result.http.headers.get("retry-after"))
        delay = DEFAULT_POLL_SECONDS if hint is None else hint
        if time.monotonic() + delay >= deadline:
            raise timed_out()
        yield Sleep(delay)


def _transfer_of(upload: Any) -> Any:
    transfer = upload.transfer
    if transfer is None:
        raise UploadError(
            f"Upload {upload.id} has no transfer URL; its status is {upload.status}.", upload_id=upload.id
        )
    if not isinstance(transfer, _m.SingleUploadTransfer):
        method = getattr(transfer, "method", None)
        raise UploadError(
            f"This SDK version can only do single PUT uploads; the server offered {method!r}. Upgrade rask-sdk.",
            upload_id=upload.id,
        )
    return transfer


def _put_is_expired(response: httpx.Response) -> bool:
    # S3 answers 403 AccessDenied "Request has expired", or 400 ExpiredToken when the signing credentials ran out.
    return response.status_code in (400, 403) and b"expired" in response.content.lower()


def upload_flow(
    config: ClientConfig,
    source: UploadSource,
    *,
    purpose: str | None = None,
    on_progress: Any = None,
) -> Flow:
    """S7: create, one PUT without the key (whole-file retries, re-signing), complete. Returns the import job."""
    upload = yield from call_flow(
        config,
        _ops.createUpload,
        _m.Upload,
        body={
            "filename": source.filename,
            "content_type": source.content_type,
            "size_bytes": source.size,
            "purpose": purpose,
        },
    )
    failures = 0
    resigns = 0
    while True:
        transfer = _transfer_of(upload)
        # Exactly the listed headers, the signed length included. The key is not among them and is never added.
        headers = httpx.Headers(dict(transfer.headers))
        headers.pop("authorization", None)
        headers["Content-Length"] = headers.get("content-length", str(source.size))
        headers["User-Agent"] = USER_AGENT
        status: int | None = None
        reason: str
        try:
            config.log("> PUT %s (%s bytes)", redact_url(transfer.url), source.size)
            put: httpx.Response = yield PutFile(transfer.url, headers, source, config.timeout, on_progress)
            config.log("< %s PUT %s", put.status_code, redact_url(transfer.url))
        except httpx.RequestError as exc:
            reason = f"{type(exc).__name__}"
        else:
            if 200 <= put.status_code < 300:
                break
            if _put_is_expired(put) and resigns < MAX_RESIGNS:
                resigns += 1
                upload = yield from call_flow(config, _ops.getUpload, _m.Upload, path={"upload_id": upload.id})
                continue
            status = put.status_code
            reason = f"HTTP {status}"
            if status < 500 and status not in (408, 429):
                raise UploadError(f"The storage refused the upload: {reason}.", upload_id=upload.id, status=status)
        failures += 1
        if failures > PUT_RETRIES:
            raise UploadError(
                f"The file was not uploaded after {failures} attempts: {reason}.", upload_id=upload.id, status=status
            )
        config.log("  retry %s of the whole file: %s", failures, reason)
        yield Sleep(backoff_seconds(failures - 1))
    return (yield from call_flow(config, _ops.completeUpload, _m.Job, path={"upload_id": upload.id}))
