"""The base classes of the generated resources."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from ._flow import call_flow
from ._operation import Operation, RequestOptions
from ._pagination import AsyncPage, Page

if TYPE_CHECKING:
    from ._client import AsyncRask, Rask

R = TypeVar("R")


class SyncResource:
    def __init__(self, client: Any) -> None:
        self._client: Rask = client

    def _call(self, op: Operation, model: type[R], **kwargs: Any) -> R:
        result: R = self._client._run(call_flow(self._client._config, op, model, **kwargs))
        return result

    def _list(
        self,
        op: Operation,
        model: type[Any],
        *,
        path: Mapping[str, Any] | None = None,
        query: Mapping[str, Any],
        options: RequestOptions | None = None,
    ) -> Page[Any]:
        listing: Any = self._call(op, model, path=path, query=query, options=options)

        def fetch(cursor: str) -> Page[Any]:
            return self._list(op, model, path=path, query={**query, "cursor": cursor}, options=options)

        return Page(listing, fetch)


class AsyncResource:
    def __init__(self, client: Any) -> None:
        self._client: AsyncRask = client

    async def _call(self, op: Operation, model: type[R], **kwargs: Any) -> R:
        result: R = await self._client._run(call_flow(self._client._config, op, model, **kwargs))
        return result

    async def _list(
        self,
        op: Operation,
        model: type[Any],
        *,
        path: Mapping[str, Any] | None = None,
        query: Mapping[str, Any],
        options: RequestOptions | None = None,
    ) -> AsyncPage[Any]:
        listing: Any = await self._call(op, model, path=path, query=query, options=options)

        async def fetch(cursor: str) -> AsyncPage[Any]:
            return await self._list(op, model, path=path, query={**query, "cursor": cursor}, options=options)

        return AsyncPage(listing, fetch)
