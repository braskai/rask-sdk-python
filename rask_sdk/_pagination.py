"""Pages over next_cursor (S5). Iterate a page to walk every item from it on; use `data` for just this page."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from typing import Any, Generic, TypeVar

from ._models import HttpInfo
from .errors import RaskError

T = TypeVar("T")


class _BasePage(Generic[T]):
    def __init__(self, listing: Any) -> None:
        self._listing = listing

    @property
    def data(self) -> list[T]:
        """The items of this page."""
        data: list[T] = self._listing.data
        return data

    @property
    def has_more(self) -> bool:
        return bool(self._listing.has_more)

    @property
    def next_cursor(self) -> str | None:
        cursor: str | None = self._listing.next_cursor
        return cursor

    @property
    def http(self) -> HttpInfo | None:
        http: HttpInfo | None = self._listing.http
        return http

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = self._listing.to_dict()
        return result

    def _refuse_repeat(self, given: set[str]) -> None:
        """Raise if this page's next_cursor was already given on the walk: following it again would never end."""
        cursor = self.next_cursor
        if not cursor:
            return
        if cursor in given:
            http = self.http
            raise RaskError(
                "The list gave a cursor it had already given, so walking it would never end.",
                status=http.status if http is not None else None,
                headers=http.headers if http is not None else None,
            )
        given.add(cursor)

    def __getattr__(self, name: str) -> Any:
        # Anything else on the list object, unknown fields included.
        if name.startswith("__") or name == "_listing":
            raise AttributeError(name)
        return getattr(self._listing, name)

    def __repr__(self) -> str:
        return f"{type(self).__name__}(items={len(self.data)}, has_more={self.has_more})"


class Page(_BasePage[T]):
    """One page of a list. `for item in page` keeps fetching until next_cursor is null.

    A cursor the list has already given on the walk raises RaskError, after the items of its page.
    """

    def __init__(self, listing: Any, fetch: Callable[[str], Page[T]]) -> None:
        super().__init__(listing)
        self._fetch = fetch

    def next_page(self) -> Page[T] | None:
        """Fetch the next page, or return None on the last one."""
        cursor = self.next_cursor
        return self._fetch(cursor) if cursor else None

    def iter_pages(self) -> Iterator[Page[T]]:
        """This page and each one after it."""
        page: Page[T] | None = self
        # Every cursor given on this walk, not only the last: a cycle such as A, B, A never repeats one in a row.
        given: set[str] = set()
        while page is not None:
            yield page
            page._refuse_repeat(given)
            page = page.next_page()

    def __iter__(self) -> Iterator[T]:
        for page in self.iter_pages():
            yield from page.data


class AsyncPage(_BasePage[T]):
    """One page of a list. `async for item in page` keeps fetching until next_cursor is null.

    A cursor the list has already given on the walk raises RaskError, after the items of its page.
    """

    def __init__(self, listing: Any, fetch: Callable[[str], Awaitable[AsyncPage[T]]]) -> None:
        super().__init__(listing)
        self._fetch = fetch

    async def next_page(self) -> AsyncPage[T] | None:
        """Fetch the next page, or return None on the last one."""
        cursor = self.next_cursor
        return await self._fetch(cursor) if cursor else None

    async def iter_pages(self) -> AsyncIterator[AsyncPage[T]]:
        """This page and each one after it."""
        page: AsyncPage[T] | None = self
        # Every cursor given on this walk, not only the last: a cycle such as A, B, A never repeats one in a row.
        given: set[str] = set()
        while page is not None:
            yield page
            page._refuse_repeat(given)
            page = await page.next_page()

    async def __aiter__(self) -> AsyncIterator[T]:
        async for page in self.iter_pages():
            for item in page.data:
                yield item
