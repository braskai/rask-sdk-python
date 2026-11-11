"""The API v2 client against a fake network: the token endpoint and the API are both answered here."""

from __future__ import annotations

import asyncio
import functools
import json
import uuid
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

import httpx
import pydantic
import pytest
from authlib.integrations.httpx_client import AsyncOAuth2Client

from rask_sdk.v2 import RaskClientException, RaskSDKClient, schemas
from rask_sdk.v2.clients import rask_client

T = TypeVar("T")

TOKEN_HOST = "rask-prod.auth.us-east-2.amazoncognito.com"
PROJECT_ID = uuid.UUID("00000000-0000-4000-8000-000000000001")
SEGMENT_ID = uuid.UUID("00000000-0000-4000-8000-000000000002")
PROJECT = {"id": str(PROJECT_ID), "name": "Talk", "source_type": "local"}
CREDITS = {name: {"total": 10, "used": 1} for name in ("minutes", "video", "lipsync_free_minutes")}


class Network:
    """Issues tokens, and records the API requests and gives them all one answer."""

    def __init__(self) -> None:
        self.token_status = 200
        self.token_requests = 0
        self.requests: list[httpx.Request] = []
        self.status = 200
        self.body: Any = CREDITS

    async def handle(self, request: httpx.Request) -> httpx.Response:
        # A real network gives the other calls a turn while this one waits for its answer.
        await asyncio.sleep(0)
        if request.url.host == TOKEN_HOST:
            self.token_requests += 1
            if self.token_status != 200:
                return httpx.Response(self.token_status, json={"error": "invalid_client"})
            return httpx.Response(200, json={"access_token": "token", "token_type": "Bearer", "expires_in": 3600})
        self.requests.append(request)
        # Written out here, because httpx sends no body at all for json=None, and null is one of the cases.
        return httpx.Response(self.status, content=json.dumps(self.body), headers={"Content-Type": "application/json"})


@pytest.fixture
def network(monkeypatch: pytest.MonkeyPatch) -> Network:
    network = Network()
    transport = httpx.MockTransport(network.handle)
    monkeypatch.setattr(rask_client, "AsyncOAuth2Client", functools.partial(AsyncOAuth2Client, transport=transport))
    return network


def run(call: Callable[[RaskSDKClient], Awaitable[T]]) -> T:
    async def drive() -> T:
        return await call(RaskSDKClient("id", "secret"))

    return asyncio.run(drive())


def test_patch_project_goes_to_the_api(network: Network) -> None:
    network.body = PROJECT
    project = run(lambda client: client.patch_project(PROJECT_ID, schemas.ProjectPatch(name="Talk")))
    assert project.id == PROJECT_ID
    [request] = network.requests
    assert (request.method, str(request.url)) == ("PATCH", f"https://api.rask.ai/v2/projects/{PROJECT_ID}")
    assert request.headers["authorization"] == "Bearer token"


@pytest.mark.parametrize("body", [[], ["down"], "Bad Gateway", None, 502])
def test_an_error_whose_json_is_not_an_object_keeps_its_status(network: Network, body: Any) -> None:
    network.status, network.body = 502, body
    with pytest.raises(RaskClientException) as caught:
        run(lambda client: client.get_credits())
    assert caught.value.status == 502
    assert str(caught.value) == "502: Unknown error occurred."


def test_an_error_with_a_detail_still_carries_it(network: Network) -> None:
    network.status, network.body = 404, {"detail": "Project not found."}
    with pytest.raises(RaskClientException) as caught:
        run(lambda client: client.get_project(PROJECT_ID))
    assert str(caught.value) == "404: Project not found."


def test_a_burst_of_first_calls_asks_for_one_token(network: Network) -> None:
    async def burst(client: RaskSDKClient) -> list[schemas.CreditsGet]:
        return await asyncio.gather(*(client.get_credits() for _ in range(5)))

    assert len(run(burst)) == 5
    assert network.token_requests == 1
    assert len(network.requests) == 5


def test_every_call_of_a_burst_fails_when_the_token_is_refused(network: Network) -> None:
    network.token_status = 400

    async def burst(client: RaskSDKClient) -> list[Any]:
        return await asyncio.gather(*(client.get_credits() for _ in range(3)), return_exceptions=True)

    failures = run(burst)
    assert [type(failure) for failure in failures] == [RaskClientException] * 3
    assert {failure.status for failure in failures} == {401}
    assert network.requests == []


def test_the_client_closes_its_connections(network: Network) -> None:
    async def drive() -> list[bool]:
        async with RaskSDKClient("id", "secret") as managed:
            inside = managed._client.is_closed
        closed_by_hand = RaskSDKClient("id", "secret")
        await closed_by_hand.aclose()
        return [inside, managed._client.is_closed, closed_by_hand._client.is_closed]

    assert asyncio.run(drive()) == [False, True, True]


def test_every_schema_is_exported() -> None:
    models = {
        name
        for name, value in vars(schemas).items()
        if isinstance(value, type) and issubclass(value, pydantic.BaseModel)
    }
    assert sorted(models - set(schemas.__all__)) == []


def test_a_patch_of_the_speaker_alone_is_cleaned_like_one_with_timestamps() -> None:
    alone = schemas.SegmentPatch(id=SEGMENT_ID, speaker="SPEAKER_01 ")
    timed = schemas.SegmentPatch(id=SEGMENT_ID, speaker="SPEAKER_01 ", start="00:00:01,000", end="00:00:02,000")
    assert alone.speaker == timed.speaker == "SPEAKER_01"
