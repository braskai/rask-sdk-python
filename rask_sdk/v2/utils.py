import functools

from authlib.integrations.base_client import MissingRequestTokenError  # type: ignore[import-untyped]
from authlib.integrations.base_client import MissingTokenError
from authlib.integrations.base_client import TokenExpiredError


def retry_on_auth_error():
    """Authenticate and retry once on missing or expired token."""

    def decorator(func):
        @functools.wraps(func)
        async def wrapper(instance, *args, **kwargs):
            try:
                return await func(instance, *args, **kwargs)
            except (MissingTokenError, MissingRequestTokenError, TokenExpiredError):
                await instance.authenticate()
                return await func(instance, *args, **kwargs)

        return wrapper

    return decorator
