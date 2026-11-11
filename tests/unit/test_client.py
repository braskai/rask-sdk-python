"""Keys, retries, logs and the like: what a small fake transport can settle."""

from __future__ import annotations

import asyncio
import datetime
import logging
import pprint
import subprocess
import sys
import warnings
from collections.abc import Iterator
from typing import Any

import httpx
import pytest

import rask_sdk._flow as flow_module
from rask_sdk import (
    AsyncRask,
    JobFailedError,
    NoContent,
    NotModified,
    Rask,
    RaskConfigError,
    RaskConnectionError,
    RaskDecodeError,
    RaskDeprecationWarning,
    RaskError,
    RaskTimeoutError,
    WaitTimeoutError,
)
from rask_sdk._config import USER_AGENT, redact_headers, redact_url
from rask_sdk._flow import backoff_seconds
from rask_sdk.errors import ConflictError, NotFoundError, ServerError, class_for_status, parse_retry_after

KEY = "raskai_test_key_unit"
BASE = "https://api.example.com/v3"


@pytest.fixture(autouse=True)
def no_rask_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    for name in ("RASK_API_KEY", "RASK_BASE_URL", "RASK_LOG"):
        monkeypatch.delenv(name, raising=False)
    yield


@pytest.fixture
def no_sleep(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    slept: list[float] = []
    monkeypatch.setattr("rask_sdk._client.time.sleep", slept.append)
    return slept


def client_with(handler: Any, **kwargs: Any) -> Rask:
    return Rask(api_key=KEY, base_url=BASE, http_client=httpx.Client(transport=httpx.MockTransport(handler)), **kwargs)


def test_key_missing_names_the_variable() -> None:
    with pytest.raises(RaskConfigError, match="RASK_API_KEY"):
        Rask(base_url=BASE)


def test_blank_key_counts_as_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("RASK_API_KEY", "  ")
    with pytest.raises(RaskConfigError, match="RASK_API_KEY"):
        Rask(base_url=BASE)


def test_key_never_shows() -> None:
    client = Rask(api_key=KEY, base_url=BASE)
    for text in (repr(client), str(client), pprint.pformat(vars(client)), repr(client._config)):
        assert KEY not in text


def test_user_agent_and_client_header() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={})

    client_with(handler).account.get_me()
    assert seen[0].headers["user-agent"] == USER_AGENT
    assert USER_AGENT.startswith("rask-sdk-python/3.0.0a0 python/3.")
    assert seen[0].headers["x-rask-client"] == "rask-sdk-python/3.0.0a0"
    assert "idempotency-key" not in seen[0].headers


def test_no_body_and_no_content_type_when_there_is_nothing_to_send() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(202, json={"object": "job", "id": "j"})

    client = client_with(handler)
    client.uploads.complete_upload("u")
    client.dubbing.estimate_dub("p")
    for request in seen:
        assert request.content == b""
        assert "content-type" not in request.headers


def test_query_values_are_encoded_as_the_contract_says() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"data": [], "has_more": False, "next_cursor": None})

    client = client_with(handler)
    when = datetime.datetime(2026, 1, 2, 3, 4, 5, tzinfo=datetime.UTC)
    client.projects.list_projects(ids=["a", "b"], created_after=when, name_contains="x y&z")
    client.usage.get_usage(from_="2026-01-01T00:00:00Z", group_by=["project"])
    client.voices.list_voices(target_language="ru-ru", free=True)
    assert seen[0].url.params["ids"] == "a,b"
    assert b"ids=a,b" in seen[0].url.query
    assert seen[0].url.params["created_after"] == "2026-01-02T03:04:05+00:00"
    assert seen[0].url.params["name_contains"] == "x y&z"
    assert seen[1].url.params["from"] == "2026-01-01T00:00:00Z"
    assert seen[2].url.params["free"] == "true"


def test_extra_options_reach_the_wire() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(201, json={"object": "folder", "id": "f"})

    client_with(handler).projects.create_folder(
        name="n", extra_headers={"X-Extra": "1"}, extra_query={"beta": "1"}, extra_body={"new_field": "v"}, timeout=5
    )
    assert seen[0].headers["x-extra"] == "1"
    assert seen[0].url.params["beta"] == "1"
    assert b'"new_field":"v"' in seen[0].content
    assert seen[0].extensions["timeout"]["read"] == 5


def test_max_retries_zero_turns_retries_off() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503)

    with pytest.raises(ServerError):
        client_with(handler, max_retries=0).account.get_me()
    assert calls == 1


def test_budget_ends_retries(no_sleep: list[float], monkeypatch: pytest.MonkeyPatch) -> None:
    now = [0.0]
    monkeypatch.setattr(flow_module.time, "monotonic", lambda: now[0])

    def advancing(seconds: float) -> None:
        no_sleep.append(seconds)
        now[0] += seconds

    monkeypatch.setattr("rask_sdk._client.time.sleep", advancing)
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        now[0] += 1
        return httpx.Response(503, headers={"Retry-After": "1"})

    with pytest.raises(ServerError):
        client_with(handler).account.get_me()
    assert now[0] <= 121
    assert calls > 3
    assert max(no_sleep) <= 30 * 1.25


def test_backoff_stays_in_its_range() -> None:
    for attempt, nominal in ((0, 0.5), (1, 0.75), (5, 0.5 * 1.5**5), (50, 30.0)):
        for _ in range(50):
            assert nominal * 0.75 <= backoff_seconds(attempt) <= nominal * 1.25


@pytest.mark.parametrize(
    ("exception", "kind"),
    [(httpx.ConnectError("x"), RaskConnectionError), (httpx.ReadTimeout("x"), RaskTimeoutError)],
)
def test_transport_failures_end_in_a_typed_error(exception: Exception, kind: type[RaskError]) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise exception

    with pytest.raises(kind) as caught:
        client_with(handler, max_retries=1).projects.create_folder(name="n")
    assert caught.value.idempotency_key
    assert caught.value.status is None
    assert caught.value.__cause__ is exception


def test_unsupported_protocol_is_not_retried() -> None:
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise httpx.UnsupportedProtocol("x")

    with pytest.raises(RaskConnectionError):
        client_with(handler).account.get_me()
    assert calls == 1


def test_retry_after_forms() -> None:
    assert parse_retry_after("5") == 5
    assert parse_retry_after(None) is None
    assert parse_retry_after("soon") is None
    assert parse_retry_after("-1") is None
    assert parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT") == 0
    future = datetime.datetime.now(datetime.UTC) + datetime.timedelta(seconds=100)
    assert 90 < (parse_retry_after(future.strftime("%a, %d %b %Y %H:%M:%S GMT")) or 0) <= 100


def test_error_classes_by_status() -> None:
    assert class_for_status(404) is NotFoundError
    assert issubclass(class_for_status(599), ServerError)
    assert class_for_status(418) is RaskError


def test_unknown_problem_members_are_attributes() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            409,
            json={"status": 409, "code": "brand_new", "brand_new_member": 7, "detail": "Nope."},
            headers={"X-Request-Id": "req-1"},
        )

    with pytest.raises(ConflictError) as caught:
        client_with(handler).projects.get_project("p")
    error = caught.value
    assert error.code == "brand_new"
    assert error.brand_new_member == 7
    assert error.request_id == "req-1"
    assert "brand_new" in str(error) and "req-1" in str(error)
    with pytest.raises(AttributeError):
        _ = error.not_there


def test_html_on_success_is_a_decode_error_with_the_body() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="<html>captive portal</html>")

    with pytest.raises(RaskDecodeError) as caught:
        client_with(handler).account.get_me()
    assert caught.value.status == 200
    assert "captive" in (caught.value.body or "")


def test_delete_returns_status_and_headers() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(204, headers={"X-Request-Id": "r"})

    result = client_with(handler).projects.delete_folder("f")
    assert isinstance(result, NoContent)
    assert result.http is not None
    assert result.http.status == 204
    assert result.http.request_id == "r"


def test_if_none_match_answers_not_modified() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(304, headers={"ETag": '"e"'})

    result = client_with(handler).catalogs.list_languages(if_none_match='"e"')
    assert isinstance(result, NotModified)
    assert result.http is not None
    assert result.http.status == 304
    assert seen[0].headers["if-none-match"] == '"e"'


def test_304_without_asking_is_an_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(304)

    with pytest.raises(RaskError):
        client_with(handler).catalogs.list_languages()


def test_wait_outlives_an_outage_longer_than_the_retry_budget(
    no_sleep: list[float], monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(flow_module, "RETRY_BUDGET_SECONDS", 0.0)
    answers: Iterator[httpx.Response] = iter(
        [
            httpx.Response(503),
            httpx.Response(504),
            httpx.Response(200, json={"object": "job", "id": "j", "status": "succeeded"}),
        ]
    )
    job = client_with(lambda request: next(answers)).jobs.wait("j")
    assert job.status == "succeeded"
    assert len(no_sleep) == 2


def test_wait_does_not_survive_a_real_error(no_sleep: list[float]) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"code": "job_not_found", "status": 404})

    with pytest.raises(NotFoundError):
        client_with(handler).jobs.wait("j")


def test_wait_timeout_keeps_the_last_job() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"object": "job", "id": "j", "status": "running"}, headers={"Retry-After": "5"})

    with pytest.raises(WaitTimeoutError) as caught:
        client_with(handler).jobs.wait("j", timeout=1)
    assert caught.value.job_id == "j"
    assert caught.value.job.status == "running"


def test_wait_failed_with_no_error_body_still_raises() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"object": "job", "id": "j", "status": "failed", "error": None})

    with pytest.raises(JobFailedError) as caught:
        client_with(handler).jobs.wait("j")
    assert caught.value.job.id == "j"
    assert caught.value.code is None


def test_wait_shortens_the_long_poll_under_a_short_client_timeout() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"object": "job", "id": "j", "status": "succeeded"})

    client = Rask(
        api_key=KEY, base_url=BASE, timeout=10, http_client=httpx.Client(transport=httpx.MockTransport(handler))
    )
    client.jobs.wait("j")
    assert seen[0].url.params["wait_seconds"] == "5"


def test_deprecation_warning_is_visible_by_default_and_once_per_operation() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={}, headers={"Deprecation": "@1", "Sunset": "soon", "Link": "<g>"})

    assert issubclass(RaskDeprecationWarning, FutureWarning)
    client = client_with(handler)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        client.account.get_me()
        client.account.get_me()
    assert len(caught) == 1
    message = caught[0].message
    assert isinstance(message, RaskDeprecationWarning)
    assert message.operation_id == "getMe"
    assert "soon" in str(message)


def test_redaction() -> None:
    url = "https://s3/x?a=1&X-Amz-Signature=abc&X-Amz-Credential=cred&b=2&x-amz-security-token=t"
    redacted = redact_url(url)
    assert "abc" not in redacted and "cred" not in redacted and "=t" not in redacted
    assert "a=1" in redacted and "b=2" in redacted
    safe = redact_headers(
        httpx.Headers(
            {
                "Authorization": f"Bearer {KEY}",
                "Idempotency-Key": "k",
                "X-Gateway-Key": "g",
                "Cookie": "c",
                "Accept": "application/json",
            }
        )
    )
    assert "bearer ***" in str(safe).lower() or "Bearer ***" in str(safe)
    assert KEY not in str(safe)
    assert safe["idempotency-key"] == "k"
    assert safe["x-gateway-key"] == "***"
    assert safe["cookie"] == "***"
    assert safe["accept"] == "application/json"


def test_httpx_info_log_does_not_leak_a_signature() -> None:
    records: list[str] = []

    class Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record.getMessage())

    capture = Capture()
    logger = logging.getLogger("httpx")
    previous = logger.level
    logger.addHandler(capture)
    logger.setLevel(logging.INFO)
    try:
        with httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(200))) as http:
            http.get("https://s3.example.com/x?X-Amz-Signature=topsecret")
    finally:
        logger.removeHandler(capture)
        logger.setLevel(previous)
    assert records
    assert "topsecret" not in "\n".join(records)


def test_debug_log_reaches_stderr_with_no_logging_configured() -> None:
    code = """
import httpx
from rask_sdk import Rask
client = Rask(api_key="raskai_test_key_unit", base_url="https://api.example.com/v3",
              http_client=httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(200, json={}))))
client.account.get_me()
"""
    done = subprocess.run(
        [sys.executable, "-c", code],
        env={"RASK_LOG": "debug", "PATH": ""},
        capture_output=True,
        text=True,
        check=True,
    )
    assert "/v3/me" in done.stderr
    assert "raskai_test_key_unit" not in done.stderr
    quiet = subprocess.run([sys.executable, "-c", code], env={"PATH": ""}, capture_output=True, text=True, check=True)
    assert quiet.stderr == ""


def test_async_client_context_manager_closes_its_own_http() -> None:
    async def drive() -> AsyncRask:
        async with AsyncRask(api_key=KEY, base_url=BASE) as client:
            return client

    client = asyncio.run(drive())
    assert client._http.is_closed


def test_sync_client_leaves_an_injected_http_open() -> None:
    http = httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(200, json={})))
    with Rask(api_key=KEY, base_url=BASE, http_client=http):
        pass
    assert not http.is_closed
    http.close()
