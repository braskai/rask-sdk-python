"""The two clients. They only run the effects of a flow; what a call does lives in _flow.py."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Iterator, Mapping
from types import TracebackType
from typing import Any, Self

import httpx

from ._config import DEFAULT_TIMEOUT, ClientConfig
from ._flow import Flow, PutFile, Send, Sleep
from ._generated.resources import AsyncResources, SyncResources


class _BaseClient:
    _config: ClientConfig

    @property
    def base_url(self) -> str:
        """The base URL every operation and helper uses, without a trailing slash."""
        return self._config.base_url

    @property
    def timeout(self) -> float:
        """The default timeout per request, in seconds."""
        return self._config.timeout

    def __repr__(self) -> str:
        return f"{type(self).__name__}(base_url={self.base_url!r}, timeout={self.timeout!r})"


def _sync_chunks(effect: PutFile) -> Iterator[bytes]:
    sent = 0
    for chunk in effect.source.chunks():
        yield chunk
        sent += len(chunk)
        if effect.on_progress is not None:
            effect.on_progress(sent, effect.source.size)


async def _async_chunks(effect: PutFile) -> AsyncIterator[bytes]:
    # Disk reads go to a thread; on_progress stays on the caller's event loop.
    chunks = effect.source.chunks()
    sent = 0
    while (chunk := await asyncio.to_thread(next, chunks, None)) is not None:
        yield chunk
        sent += len(chunk)
        if effect.on_progress is not None:
            effect.on_progress(sent, effect.source.size)


def _put_request(effect: PutFile, content: Any) -> httpx.Request:
    extensions = {"timeout": httpx.Timeout(effect.timeout).as_dict()}
    return httpx.Request("PUT", effect.url, headers=effect.headers, content=content, extensions=extensions)


class Rask(_BaseClient, SyncResources):
    """The Rask API v3 client.

    Args:
        api_key: The API key. Defaults to the RASK_API_KEY environment variable.
        base_url: Defaults to the RASK_BASE_URL environment variable, then to https://api.rask.ai/v3. A path prefix
            and a trailing slash are fine, as in https://gw.example.com/rask/v3/.
        timeout: Seconds per request. The default is 60 (a 25 s server budget plus a 20 s long-poll).
        max_retries: Limits the retries of one call. By default only the 120 s budget does. 0 turns retries off.
        default_headers: Extra headers on every API request, for a gateway. Never sent to any other host.
        http_client: An httpx.Client of your own, for proxies or mTLS. You close it.
    """

    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str | None = None,
        timeout: float = DEFAULT_TIMEOUT,
        max_retries: int | None = None,
        default_headers: Mapping[str, str] | None = None,
        http_client: httpx.Client | None = None,
    ) -> None:
        self._config = ClientConfig.resolve(api_key, base_url, timeout, max_retries, default_headers)
        self._owns_http = http_client is None
        self._http = http_client if http_client is not None else httpx.Client()
        self._init_resources()

    def _run(self, flow: Flow) -> Any:
        try:
            effect = next(flow)
            while True:
                try:
                    if isinstance(effect, Send):
                        value: Any = self._http.send(effect.request, follow_redirects=False)
                    elif isinstance(effect, Sleep):
                        time.sleep(effect.seconds)
                        value = None
                    elif isinstance(effect, PutFile):
                        request = _put_request(effect, _sync_chunks(effect))
                        value = self._http.send(request, follow_redirects=False)
                    else:  # pragma: no cover
                        raise TypeError(f"Unknown effect {effect!r}")
                except httpx.HTTPError as exc:
                    effect = flow.throw(exc)
                    continue
                effect = flow.send(value)
        except StopIteration as stop:
            return stop.value

    def close(self) -> None:
        """Close the connections, unless the http_client was yours."""
        if self._owns_http:
            self._http.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, kind: type[BaseException] | None, value: BaseException | None, tb: TracebackType | None) -> None:
        self.close()


class AsyncRask(_BaseClient, AsyncResources):
    """The Rask API v3 client for asyncio. Same arguments and behaviour as Rask; http_client is an httpx.AsyncClient."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str | None = None,
        timeout: float = DEFAULT_TIMEOUT,
        max_retries: int | None = None,
        default_headers: Mapping[str, str] | None = None,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._config = ClientConfig.resolve(api_key, base_url, timeout, max_retries, default_headers)
        self._owns_http = http_client is None
        self._http = http_client if http_client is not None else httpx.AsyncClient()
        self._init_resources()

    async def _run(self, flow: Flow) -> Any:
        try:
            effect = next(flow)
            while True:
                try:
                    if isinstance(effect, Send):
                        value: Any = await self._http.send(effect.request, follow_redirects=False)
                    elif isinstance(effect, Sleep):
                        await asyncio.sleep(effect.seconds)
                        value = None
                    elif isinstance(effect, PutFile):
                        request = _put_request(effect, _async_chunks(effect))
                        value = await self._http.send(request, follow_redirects=False)
                    else:  # pragma: no cover
                        raise TypeError(f"Unknown effect {effect!r}")
                except httpx.HTTPError as exc:
                    effect = flow.throw(exc)
                    continue
                effect = flow.send(value)
        except StopIteration as stop:
            return stop.value

    async def close(self) -> None:
        """Close the connections, unless the http_client was yours."""
        if self._owns_http:
            await self._http.aclose()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self, kind: type[BaseException] | None, value: BaseException | None, tb: TracebackType | None
    ) -> None:
        await self.close()
