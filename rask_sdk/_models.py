"""The base of every response model, and how a model stays usable whatever the server sends (S12)."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable, Mapping
from typing import Any, Self

import httpx
from pydantic import BaseModel, ConfigDict, PrivateAttr, TypeAdapter, ValidationError, model_validator
from pydantic.functional_validators import ModelWrapValidatorHandler

logger = logging.getLogger("rask_sdk")

UNKNOWN_TAG = "__unknown__"

_field_adapters: dict[tuple[type[Any], str], TypeAdapter[Any]] = {}


class HttpInfo:
    """Status and headers of the response a result came from, with no second call (S11)."""

    __slots__ = ("content", "headers", "idempotency_key", "status")

    def __init__(self, status: int, headers: httpx.Headers, content: bytes, idempotency_key: str | None) -> None:
        self.status = status
        self.headers = headers
        self.content = content
        self.idempotency_key = idempotency_key

    def _header(self, name: str) -> str | None:
        value: str | None = self.headers.get(name)
        return value

    @property
    def request_id(self) -> str | None:
        return self._header("x-request-id")

    @property
    def etag(self) -> str | None:
        return self._header("etag")

    @property
    def location(self) -> str | None:
        return self._header("location")

    @property
    def idempotent_replayed(self) -> bool:
        return (self._header("idempotent-replayed") or "").lower() == "true"

    def json(self) -> Any:
        """The body exactly as the server sent it."""
        return json.loads(self.content)

    def __repr__(self) -> str:
        return f"HttpInfo(status={self.status}, request_id={self.request_id!r})"


class RaskModel(BaseModel):
    """A response object. Unknown fields stay readable as attributes and in `model_extra`.

    `http` holds the status and headers on the object a call returned, and is None on the objects nested in it.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True, defer_build=True, protected_namespaces=())

    _http: HttpInfo | None = PrivateAttr(default=None)

    @property
    def http(self) -> HttpInfo | None:
        return self._http

    def to_dict(self) -> dict[str, Any]:
        """Plain JSON-compatible data with the wire names, unknown fields included."""
        return self.model_dump(mode="json", by_alias=True, exclude_unset=True, warnings=False)

    @model_validator(mode="wrap")
    @classmethod
    def _never_fatal(cls, value: Any, handler: ModelWrapValidatorHandler[Self]) -> Self:
        # Open values are already typed open. This catches what is left, like a type or a missing required field the
        # contract does not allow: keep the object and everything readable in it, rather than losing the whole response.
        try:
            return handler(value)
        except ValidationError as exc:
            if not isinstance(value, Mapping):
                raise
            problems = exc.errors(include_url=False, include_input=False, include_context=False)[:3]
            logger.debug("%s did not match the contract, keeping it as sent: %s", cls.__name__, problems)
            constructed: Self = _construct_leniently(cls, value)
            return constructed


def _construct_leniently(cls: type[Any], raw: Mapping[str, Any]) -> Any:
    values: dict[str, Any] = {}
    fields_set: set[str] = set()
    known: set[str] = set()
    for name, field in cls.model_fields.items():
        wire = field.alias or name
        known.add(wire)
        if wire not in raw:
            values[name] = None
            continue
        fields_set.add(name)
        adapter = _field_adapters.get((cls, name))
        if adapter is None:
            adapter = TypeAdapter(field.rebuild_annotation())
            _field_adapters[(cls, name)] = adapter
        try:
            values[name] = adapter.validate_python(raw[wire])
        except ValidationError:
            values[name] = raw[wire]
    for name, item in raw.items():
        if name not in known:
            values[name] = item
            fields_set.add(name)
    return cls.model_construct(_fields_set=fields_set, **values)


class UnknownVariant(RaskModel):
    """A union variant this SDK version does not know. All its fields are attributes, as the server sent them."""


class NoContent(RaskModel):
    """The result of an operation that answers 204."""


class NotModified(RaskModel):
    """The answer to `if_none_match` when nothing changed: `http.status` is 304 and there is no body."""


def discriminate(prop: str, mapping: dict[str, str]) -> Callable[[Any], str]:
    """Pick a union variant by its discriminator, falling back on UnknownVariant."""

    def pick(value: Any) -> str:
        tag = value.get(prop) if isinstance(value, Mapping) else getattr(value, prop, None)
        return mapping.get(tag, UNKNOWN_TAG) if isinstance(tag, str) else UNKNOWN_TAG

    return pick
