"""A check that passes whatever the SDK does tests nothing: break the SDK on purpose, and the scenario must fail."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from typing import Any
from urllib.parse import urljoin

import httpx
import pytest

import rask_sdk._client as client_module
import rask_sdk._flow as flow_module
import rask_sdk.errors as errors_module
from rask_sdk._config import ClientConfig

from .harness import Harness, find_harness
from .runner import run_scenario


@pytest.fixture(scope="module")
def harness() -> Iterator[Harness]:
    directory = find_harness()
    if directory is None:
        pytest.skip("rask-sdk-acceptance not found")
    instance = Harness(directory)
    yield instance
    instance.stop()


def key_on_every_host(patch: pytest.MonkeyPatch) -> None:
    patch.setattr(ClientConfig, "is_under_base", lambda self, url: True)


def key_on_the_put(patch: pytest.MonkeyPatch) -> None:
    original = client_module._put_request

    def with_key(effect: Any, content: Any) -> httpx.Request:
        effect.headers["Authorization"] = "Bearer raskai_test_key_leak"
        return original(effect, content)

    patch.setattr(client_module, "_put_request", with_key)


def fixed_idempotency_key(patch: pytest.MonkeyPatch) -> None:
    patch.setattr(flow_module.uuid, "uuid4", lambda: "11111111-1111-4111-8111-111111111111")


class _Stripping:
    """The json module, except that a decoded body loses whatever the SDK was generated without knowing."""

    def __getattr__(self, name: str) -> Any:
        return getattr(json, name)

    def loads(self, text: Any) -> Any:
        def strip(value: Any) -> Any:
            if isinstance(value, dict):
                return {k: strip(v) for k, v in value.items() if not k.startswith("s12_")}
            if isinstance(value, list):
                return [strip(v) for v in value]
            return value

        return strip(json.loads(text))


def unknown_fields_dropped(patch: pytest.MonkeyPatch) -> None:
    patch.setattr(flow_module, "json", _Stripping())


def base_url_resolved_as_a_url(patch: pytest.MonkeyPatch) -> None:
    original = flow_module._build_url

    def resolved(config: Any, op: Any, path: Any, query: Any) -> str:
        url = original(config, op, path, query)
        return urljoin(config.base_url + "/", url[len(config.base_url) :])

    patch.setattr(flow_module, "_build_url", resolved)


def retries_500(patch: pytest.MonkeyPatch) -> None:
    patch.setattr(flow_module, "RETRY_STATUSES", frozenset({429, 500, 502, 503, 504}))
    patch.setattr(flow_module, "RETRY_BUDGET_SECONDS", 2.0)


def no_long_poll(patch: pytest.MonkeyPatch) -> None:
    patch.setattr(flow_module, "WAIT_SECONDS", 5)


def empty_string_becomes_null(patch: pytest.MonkeyPatch) -> None:
    original = flow_module.to_jsonable
    patch.setattr(
        flow_module,
        "to_jsonable",
        lambda value: (
            {k: v for k, v in original(value).items() if v != ""} if isinstance(value, dict) else original(value)
        ),
    )


def key_in_the_debug_log(patch: pytest.MonkeyPatch) -> None:
    patch.setattr(flow_module, "redact_headers", dict)


def signature_in_the_debug_log(patch: pytest.MonkeyPatch) -> None:
    patch.setattr(flow_module, "redact_url", lambda url: url)


def warns_every_call(patch: pytest.MonkeyPatch) -> None:
    original = ClientConfig.warn_deprecated

    def again(self: ClientConfig, operation_id: str, headers: httpx.Headers) -> None:
        self._warned.clear()
        original(self, operation_id, headers)

    patch.setattr(ClientConfig, "warn_deprecated", again)


def ignores_retry_after(patch: pytest.MonkeyPatch) -> None:
    patch.setattr(errors_module, "parse_retry_after", lambda value: None)


def retries_key_in_use_forever(patch: pytest.MonkeyPatch) -> None:
    patch.setattr(flow_module, "KEY_IN_USE_RETRIES", 5)


def put_retries_too_few(patch: pytest.MonkeyPatch) -> None:
    patch.setattr(flow_module, "PUT_RETRIES", 0)


def no_resign(patch: pytest.MonkeyPatch) -> None:
    patch.setattr(flow_module, "MAX_RESIGNS", 0)


FAULTS: list[tuple[Callable[[pytest.MonkeyPatch], None], str]] = [
    (key_on_every_host, "BU5.no-key-on-redirect-get"),
    (key_on_the_put, "BU6.presigned-put-no-authorization"),
    (base_url_resolved_as_a_url, "BU1.path-prefix"),
    (fixed_idempotency_key, "S4.key-fresh-per-call"),
    (retries_500, "S3.no-retry-500"),
    (ignores_retry_after, "S3.retry-429-honours-retry-after"),
    (retries_key_in_use_forever, "S3.key-in-use-gives-up"),
    (no_long_poll, "S6.wait-succeeds"),
    (put_retries_too_few, "S7.put-retried-whole-file"),
    (no_resign, "S7.expired-url-in-flight"),
    (unknown_fields_dropped, "S12.job-unknown-field"),
    (empty_string_becomes_null, "S12.clear-with-empty-string"),
    (warns_every_call, "S14.warn-once-per-operation"),
    (key_in_the_debug_log, "S16.debug-log-redacts"),
    (signature_in_the_debug_log, "S16.debug-log-redacts"),
]


@pytest.mark.parametrize(("fault", "scenario_id"), FAULTS, ids=[fault.__name__ for fault, _ in FAULTS])
def test_fault_is_caught(
    harness: Harness, fault: Callable[[pytest.MonkeyPatch], None], scenario_id: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    with monkeypatch.context() as patch:
        fault(patch)
        result = run_scenario(harness, scenario_id, "sync", patch)
    assert not result["verify"]["pass"], f"{fault.__name__} went unnoticed by {scenario_id}"
