"""What the generated code passes to the runtime."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, TypedDict


@dataclass(frozen=True, slots=True)
class Operation:
    """One operation of the contract, reduced to what a request needs."""

    id: str
    resource: str
    method_name: str
    http_method: str
    path: str
    path_params: tuple[str, ...]
    query_params: tuple[tuple[str, str], ...]  # (wire name, keyword name)
    body_mode: str  # none, flat (fields are keywords) or root (a union, passed as body=)
    body_required: bool
    body_params: tuple[tuple[str, str], ...]
    idempotency: str  # none, optional or required on the wire; the SDK fills it in either way
    if_match: str  # none, optional or required
    if_none_match: bool
    response: str | None
    success: tuple[int, ...]
    paginated: bool
    deprecated: bool


class RequestOptions(TypedDict, total=False):
    """Per-call options every operation takes."""

    timeout: float
    """Seconds, for this call only. The default is the client's."""
    extra_headers: Mapping[str, str]
    """Extra request headers, sent to the API only."""
    extra_query: Mapping[str, Any]
    """Query parameters this SDK version does not know about."""
    extra_body: Mapping[str, Any]
    """Body fields this SDK version does not know about."""
