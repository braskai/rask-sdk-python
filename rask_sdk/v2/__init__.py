"""The API v2 client (Cognito client credentials), kept unchanged beside the v3 SDK for migration.

Requires the ``v2`` extra: ``pip install "rask-sdk[v2]"``. New code should use ``from rask_sdk import Rask``.
"""

try:
    import authlib  # noqa: F401
except ImportError as exc:  # pragma: no cover
    raise ImportError('rask_sdk.v2 needs Authlib: pip install "rask-sdk[v2]"') from exc

from rask_sdk.v2 import enums, schemas  # noqa: E402
from rask_sdk.v2.clients import RaskSDKClient  # noqa: E402
from rask_sdk.v2.exceptions import RaskClientException  # noqa: E402

__all__ = ["RaskClientException", "RaskSDKClient", "enums", "schemas"]
