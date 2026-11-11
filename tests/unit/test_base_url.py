"""The base URL rules without a harness: where a request goes, and when the key goes with it."""

from __future__ import annotations

import asyncio
import inspect
import subprocess
import sys
from pathlib import Path
from typing import Any

import httpx
import pytest

from rask_sdk import AsyncRask, Rask, RaskConfigError, RaskError
from rask_sdk._generated.operations import OPERATIONS

KEY = "raskai_test_key_unit"
REPO = Path(__file__).resolve().parents[2]


def test_a_key_read_with_its_newline_is_trimmed() -> None:
    sent: list[httpx.Request] = []

    def answer(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        return httpx.Response(200, json={})

    http = httpx.Client(transport=httpx.MockTransport(answer))
    Rask(api_key=f"{KEY}\n", base_url="https://gw.example.com/v3", http_client=http).account.get_me()
    assert sent[0].headers["authorization"] == f"Bearer {KEY}"


@pytest.mark.parametrize("key", ["raskai_\ntopsecret", "raskai_\rtopsecret", "raskai_ topsecret", "raskai_ж_topsecret"])
def test_a_bad_key_fails_without_repeating_it(key: str) -> None:
    with pytest.raises(RaskConfigError) as caught:
        Rask(api_key=key, base_url="https://gw.example.com/v3")
    assert "API key" in str(caught.value)
    assert "topsecret" not in str(caught.value)


@pytest.mark.parametrize("base_url", [None, "", "  "])
def test_smoke_program_refuses_to_run_without_a_base_url(base_url: str | None) -> None:
    """It writes a folder, and the default base URL is production. The dead proxy keeps a regression off the network."""
    env = {"RASK_API_KEY": KEY, "HTTPS_PROXY": "http://127.0.0.1:9", "PATH": ""}
    if base_url is not None:
        env["RASK_BASE_URL"] = base_url
    done = subprocess.run(
        [sys.executable, str(REPO / "examples" / "smoke.py")],
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert done.returncode == 2, done.stdout + done.stderr
    assert "RASK_BASE_URL" in done.stdout
    assert "getMe" not in done.stdout


def fake_arguments(method: Any) -> tuple[list[Any], dict[str, Any]]:
    """Dummy values for the required parameters, read from the signature."""
    args: list[Any] = []
    kwargs: dict[str, Any] = {}
    for name, parameter in inspect.signature(method).parameters.items():
        if parameter.kind is parameter.VAR_KEYWORD or parameter.default is not parameter.empty:
            continue
        annotation = str(parameter.annotation)
        value: Any = 1 if annotation == "int" else "x"
        if annotation.startswith("Sequence"):
            value = ["x"]
        if annotation.startswith("_p.") or annotation.startswith("Mapping"):
            value = {}
        if parameter.kind is parameter.POSITIONAL_OR_KEYWORD:
            args.append(value)
        else:
            kwargs[name] = '"etag"' if name == "if_match" else value
    return args, kwargs


def recording_transport(seen: list[httpx.Request]) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"data": [], "has_more": False, "next_cursor": None})

    return httpx.MockTransport(handler)


@pytest.mark.parametrize("base_url", ["https://gw.example.com/rask/v3", "https://gw.example.com/rask/v3/"])
def test_every_operation_keeps_the_prefix_sync(base_url: str) -> None:
    seen: list[httpx.Request] = []
    with httpx.Client(transport=recording_transport(seen)) as http:
        client = Rask(api_key=KEY, base_url=base_url, http_client=http)
        for op in OPERATIONS.values():
            method = getattr(getattr(client, op.resource), op.method_name)
            args, kwargs = fake_arguments(method)
            method(*args, **kwargs)
    assert len(seen) == 82
    for request in seen:
        assert str(request.url).startswith("https://gw.example.com/rask/v3/")
        assert "//" not in request.url.path
        assert request.headers["authorization"] == f"Bearer {KEY}"


def test_every_operation_keeps_the_prefix_async() -> None:
    seen: list[httpx.Request] = []

    async def drive() -> None:
        async with httpx.AsyncClient(transport=recording_transport(seen)) as http:
            client = AsyncRask(api_key=KEY, base_url="https://gw.example.com/rask/v3/", http_client=http)
            for op in OPERATIONS.values():
                method = getattr(getattr(client, op.resource), op.method_name)
                args, kwargs = fake_arguments(method)
                await method(*args, **kwargs)

    asyncio.run(drive())
    assert len(seen) == 82
    assert all(str(request.url).startswith("https://gw.example.com/rask/v3/") for request in seen)


def test_default_and_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("RASK_BASE_URL", raising=False)
    assert Rask(api_key=KEY).base_url == "https://api.rask.ai/v3"
    monkeypatch.setenv("RASK_BASE_URL", "")
    assert Rask(api_key=KEY).base_url == "https://api.rask.ai/v3"
    monkeypatch.setenv("RASK_BASE_URL", "https://env.example.com/v3/")
    assert Rask(api_key=KEY).base_url == "https://env.example.com/v3"
    assert Rask(api_key=KEY, base_url="https://arg.example.com/v3").base_url == "https://arg.example.com/v3"


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "api.rask.ai/v3",
        "ftp://api.rask.ai/v3",
        "https://user:secret@api.rask.ai/v3",
        "https://api.rask.ai/v3?x=1",
        "https://api.rask.ai/v3#frag",
    ],
)
def test_bad_base_url_is_refused(bad: str) -> None:
    with pytest.raises(RaskConfigError):
        Rask(api_key=KEY, base_url=bad)


def _redirecting(location: str, status: int = 307) -> tuple[httpx.MockTransport, list[httpx.Request]]:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if len(seen) == 1:
            return httpx.Response(status, headers={"Location": location})
        return httpx.Response(200, json={"object": "me"})

    return httpx.MockTransport(handler), seen


BASE = "https://gw.example.com/rask/v3"


@pytest.mark.parametrize(
    ("location", "keeps_key"),
    [
        ("https://gw.example.com/rask/v3/me2", True),
        ("/rask/v3/me2", True),
        ("me2", True),
        ("https://other.example.com/rask/v3/me", False),  # another host
        ("https://gw.example.com:8443/rask/v3/me", False),  # another port
        ("http://gw.example.com/rask/v3/me", False),  # a downgrade
        ("https://gw.example.com/other/me", False),  # the same host, outside the API
        ("/v3/me", False),  # what Location says under a custom prefix: DESIGN 4.2
        ("https://gw.example.com/rask/v3/../../other", False),
        ("https://gw.example.com.evil.test/rask/v3/me", False),
        ("https://gw.example.com/rask/v3x/me", False),
    ],
)
def test_get_redirect_key_policy(location: str, keeps_key: bool) -> None:
    transport, seen = _redirecting(location)
    with httpx.Client(transport=transport) as http:
        Rask(api_key=KEY, base_url=BASE, http_client=http, default_headers={"X-Gateway": "g"}).account.get_me()
    assert len(seen) == 2
    assert seen[0].headers["authorization"] == f"Bearer {KEY}"
    assert ("authorization" in seen[1].headers) is keeps_key
    assert ("x-gateway" in seen[1].headers) is keeps_key


def test_redirect_key_policy_async() -> None:
    transport, seen = _redirecting("https://other.example.com/rask/v3/me")

    async def drive() -> None:
        async with httpx.AsyncClient(transport=transport) as http:
            await AsyncRask(api_key=KEY, base_url=BASE, http_client=http).account.get_me()

    asyncio.run(drive())
    assert "authorization" not in seen[1].headers


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
def test_write_redirected_off_the_api_is_not_followed(status: int) -> None:
    transport, seen = _redirecting("https://other.example.com/x", status)
    with httpx.Client(transport=transport) as http, pytest.raises(RaskError) as caught:
        Rask(api_key=KEY, base_url=BASE, http_client=http).projects.create_folder(name="n")
    assert len(seen) == 1
    assert caught.value.status == status


@pytest.mark.parametrize(("status", "follows"), [(301, False), (302, False), (303, False), (307, True), (308, True)])
def test_write_redirected_inside_the_api(status: int, follows: bool) -> None:
    transport, seen = _redirecting(f"{BASE}/folders2", status)
    with httpx.Client(transport=transport) as http:
        client = Rask(api_key=KEY, base_url=BASE, http_client=http)
        if follows:
            client.projects.create_folder(name="n")
            assert seen[1].method == "POST"
            assert seen[1].content == seen[0].content
            assert seen[1].headers["authorization"] == f"Bearer {KEY}"
            assert seen[1].headers["idempotency-key"] == seen[0].headers["idempotency-key"]
        else:
            with pytest.raises(RaskError):
                client.projects.create_folder(name="n")
            assert len(seen) == 1


def test_a_redirect_loop_ends() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(302, headers={"Location": f"{BASE}/me"})

    with httpx.Client(transport=httpx.MockTransport(handler)) as http, pytest.raises(RaskError) as caught:
        Rask(api_key=KEY, base_url=BASE, http_client=http).account.get_me()
    assert caught.value.status == 302
    assert len(seen) == 4


def test_a_client_that_follows_redirects_itself_cannot_override_the_policy() -> None:
    transport, seen = _redirecting("https://other.example.com/x")
    with httpx.Client(transport=transport, follow_redirects=True) as http:
        Rask(api_key=KEY, base_url=BASE, http_client=http).account.get_me()
    assert "authorization" not in seen[1].headers


def test_an_injected_client_with_its_own_auth_header_does_not_change_what_we_send() -> None:
    seen: list[httpx.Request] = []
    transport = recording_transport(seen)
    with httpx.Client(transport=transport, headers={"Authorization": "Bearer other"}) as http:
        Rask(api_key=KEY, base_url=BASE, http_client=http).account.get_me()
    assert seen[0].headers["authorization"] == f"Bearer {KEY}"


@pytest.mark.parametrize("bad", ["", ".", ".."])
def test_path_values_cannot_address_another_operation(bad: str) -> None:
    seen: list[httpx.Request] = []
    with httpx.Client(transport=recording_transport(seen)) as http:
        client = Rask(api_key=KEY, base_url=BASE, http_client=http)
        with pytest.raises(RaskConfigError):
            client.projects.delete_project(bad)
    assert seen == []


def test_path_values_are_encoded() -> None:
    seen: list[httpx.Request] = []
    with httpx.Client(transport=recording_transport(seen)) as http:
        Rask(api_key=KEY, base_url=BASE, http_client=http).projects.get_project("a/b c?d")
    assert seen[0].url.raw_path == b"/rask/v3/projects/a%2Fb%20c%3Fd"
