"""Client configuration: the API key, the base URL, and who may see the key."""

from __future__ import annotations

import logging
import os
import re
import sys
import warnings
from collections.abc import Mapping
from dataclasses import dataclass, field

import httpx

from ._version import __version__
from .errors import RaskConfigError, RaskDeprecationWarning

DEFAULT_BASE_URL = "https://api.rask.ai/v3"
DEFAULT_TIMEOUT = 60.0
USER_AGENT = f"rask-sdk-python/{__version__} python/{'.'.join(str(part) for part in sys.version_info[:3])}"
CLIENT_HEADER = f"rask-sdk-python/{__version__}"

logger = logging.getLogger("rask_sdk")

_SENSITIVE_QUERY = re.compile(
    r"(?i)([?&](?:x-amz-signature|x-amz-credential|x-amz-security-token|x-goog-signature|x-goog-credential"
    r"|signature|sig|awsaccesskeyid|access_token|token|api_key|key)=)[^&#\s]*"
)
_SENSITIVE_HEADER = re.compile(r"(?i)authorization|cookie|token|secret|signature|credential|password|key")
_PUBLIC_HEADERS = {"idempotency-key"}
_VALID_API_KEY = re.compile(r"[\x21-\x7e]+")


def redact_url(url: str) -> str:
    """A URL safe to log: presigned signatures and credentials are gone."""
    return _SENSITIVE_QUERY.sub(r"\1***", url)


class _RedactHttpxUrls(logging.Filter):
    """httpx logs every URL at INFO, presigned signatures included. Whoever turns that on should not leak them."""

    def filter(self, record: logging.LogRecord) -> bool:
        if isinstance(record.args, tuple):
            record.args = tuple(redact_url(str(arg)) if isinstance(arg, httpx.URL) else arg for arg in record.args)
        return True


logging.getLogger("httpx").addFilter(_RedactHttpxUrls())


def redact_headers(headers: httpx.Headers) -> dict[str, str]:
    """Headers safe to log."""
    safe: dict[str, str] = {}
    for name, value in headers.items():
        lowered = name.lower()
        if lowered == "authorization":
            safe[name] = f"{value.split(' ', 1)[0]} ***"
        elif _SENSITIVE_HEADER.search(lowered) and lowered not in _PUBLIC_HEADERS:
            safe[name] = "***"
        else:
            safe[name] = value
    return safe


class _Secret:
    """Holds the key so that no repr, str or dump of the client shows it."""

    __slots__ = ("_value",)

    def __init__(self, value: str) -> None:
        self._value = value

    def reveal(self) -> str:
        return self._value

    def __repr__(self) -> str:
        return "***"

    def __reduce__(self) -> str:
        raise TypeError("The API key cannot be pickled.")


def _normalize_base_url(value: str) -> tuple[str, httpx.URL]:
    text = str(value).strip().rstrip("/")
    try:
        parsed = httpx.URL(text)
    except httpx.InvalidURL as exc:
        raise RaskConfigError(f"base_url is not a valid URL: {exc}") from exc
    if parsed.scheme not in ("http", "https") or not parsed.host:
        raise RaskConfigError(f"base_url must be an absolute http(s) URL, got {text!r}.")
    if parsed.userinfo:
        raise RaskConfigError("base_url must not contain a user name or password.")
    if parsed.query or parsed.fragment:
        raise RaskConfigError("base_url must not contain a query string or a fragment.")
    return text, parsed


def _is_debug_requested() -> bool:
    return os.environ.get("RASK_LOG", "").strip().lower() == "debug"


@dataclass(slots=True)
class ClientConfig:
    """Everything a request needs from the client, for the sync and the async client alike."""

    api_key: _Secret
    base_url: str
    timeout: float
    max_retries: int | None
    default_headers: Mapping[str, str] = field(repr=False)  # a gateway's credentials may be in here
    debug: bool
    _base: httpx.URL
    _warned: set[str] = field(default_factory=set)

    @classmethod
    def resolve(
        cls,
        api_key: str | None,
        base_url: str | None,
        timeout: float,
        max_retries: int | None,
        default_headers: Mapping[str, str] | None,
    ) -> ClientConfig:
        key = api_key if api_key is not None else os.environ.get("RASK_API_KEY")
        if not key or not key.strip():
            raise RaskConfigError("No API key: pass api_key=... or set the RASK_API_KEY environment variable.")
        key = key.strip()
        # The HTTP stack quotes an invalid header value in its exceptions, so a bad key must stop here.
        if not _VALID_API_KEY.fullmatch(key):
            raise RaskConfigError("The API key holds spaces or control characters: check how it is read.")
        if base_url is not None:
            chosen = base_url
        else:
            chosen = os.environ.get("RASK_BASE_URL", "").strip() or DEFAULT_BASE_URL
        text, parsed = _normalize_base_url(chosen)
        if timeout <= 0:
            raise RaskConfigError(f"timeout must be positive, got {timeout}.")
        debug = _is_debug_requested()
        if debug and not logger.hasHandlers():
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter("%(asctime)s rask_sdk %(message)s"))
            logger.addHandler(handler)
        return cls(
            api_key=_Secret(key),
            base_url=text,
            timeout=float(timeout),
            max_retries=max_retries,
            default_headers=dict(default_headers or {}),
            debug=debug,
            _base=parsed,
        )

    def is_under_base(self, url: str) -> bool:
        """Whether a URL belongs to the API: same scheme, host and port, under the base path. Only those get the key."""
        target = httpx.URL(url)
        base = self._base
        if (target.scheme, target.host, target.port) != (base.scheme, base.host, base.port):
            return False
        segments = target.path.split("/")
        if ".." in segments:
            return False
        prefix = base.path.rstrip("/")
        return target.path == prefix or target.path.startswith(prefix + "/")

    def log(self, message: str, *args: object) -> None:
        """Debug traffic log. Per client under RASK_LOG=debug, or through the application's logging configuration."""
        if self.debug or logger.isEnabledFor(logging.DEBUG):
            logger.handle(logger.makeRecord(logger.name, logging.DEBUG, __file__, 0, message, args, None))

    def warn_deprecated(self, operation_id: str, headers: httpx.Headers) -> None:
        """S14: one warning per operation, on the first Deprecation header."""
        deprecation = headers.get("deprecation")
        if deprecation is None or operation_id in self._warned:
            return
        self._warned.add(operation_id)
        warning = RaskDeprecationWarning(operation_id, deprecation, headers.get("sunset"), headers.get("link"))
        warnings.warn(warning, stacklevel=2)
