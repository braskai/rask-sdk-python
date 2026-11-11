"""The generated surface against the contract: all 82 operations, named as promised, and no drift."""

from __future__ import annotations

import inspect
import json
import re
import subprocess
import sys
from pathlib import Path

import httpx
import pytest

from rask_sdk import AsyncRask, Rask
from rask_sdk._generated.operations import OPERATIONS
from tests.acceptance.markers import check_deprecation_marker

REPO = Path(__file__).resolve().parents[2]
CONTRACT = json.loads((REPO / "contract" / "openapi.json").read_text(encoding="utf-8"))
METHODS = ("get", "post", "put", "patch", "delete")


def snake(name: str) -> str:
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name).lower()


CONTRACT_OPS = {
    item[method]["operationId"]: (method, path, item[method])
    for path, item in CONTRACT["paths"].items()
    for method in METHODS
    if method in item
}


def test_committed_code_matches_the_contract() -> None:
    done = subprocess.run(
        [sys.executable, str(REPO / "tools" / "generate.py"), "--check"], capture_output=True, text=True, check=False
    )
    assert done.returncode == 0, done.stderr


def test_all_operations_are_generated() -> None:
    assert len(CONTRACT_OPS) == 82
    assert set(OPERATIONS) == set(CONTRACT_OPS)


@pytest.mark.parametrize("cls", [Rask, AsyncRask])
@pytest.mark.parametrize("operation_id", sorted(CONTRACT_OPS))
def test_method_is_named_after_the_operation_under_its_tag(cls: type, operation_id: str) -> None:
    client = cls(api_key="k", base_url="http://127.0.0.1:1/v3")
    method, path, raw = CONTRACT_OPS[operation_id]
    resource = snake(raw["tags"][0].replace(" ", "_"))
    bound = getattr(getattr(client, resource), snake(operation_id))
    op = OPERATIONS[operation_id]
    assert (op.http_method, op.path) == (method.upper(), path)
    assert inspect.iscoroutinefunction(bound) is (cls is AsyncRask)
    parameters = inspect.signature(bound).parameters
    assert "options" in parameters
    headers = {
        parameter.get("name") or parameter["$ref"].split("/")[-1]: parameter for parameter in raw.get("parameters", [])
    }
    takes_key = any("Idempotency" in name for name in headers)
    assert ("idempotency_key" in parameters) is takes_key
    if op.if_match == "required":
        assert parameters["if_match"].default is inspect.Parameter.empty
        assert parameters["if_match"].kind is inspect.Parameter.KEYWORD_ONLY
    if takes_key:
        assert parameters["idempotency_key"].default is None


def test_missing_if_match_fails_at_the_call() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={})

    client = Rask(
        api_key="k", base_url="https://x.test/v3", http_client=httpx.Client(transport=httpx.MockTransport(handler))
    )
    with pytest.raises(TypeError, match="if_match"):
        client.segments.update_segment("p", "s", translation_text="x")  # type: ignore[call-arg]
    with pytest.raises(ValueError, match="if_match"):
        client.segments.update_segment("p", "s", translation_text="x", if_match=None)  # type: ignore[arg-type]
    assert seen == []


def test_generating_a_deprecated_operation_adds_the_marker() -> None:
    assert check_deprecation_marker() == []


def test_import_does_not_need_authlib() -> None:
    code = "import sys; sys.modules['authlib'] = None; import rask_sdk; print('rask_sdk.v2' in sys.modules)"
    done = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert done.stdout.strip() == "False"


def test_v2_client_stays_reachable() -> None:
    from rask_sdk.v2 import RaskClientException, RaskSDKClient, enums, schemas

    assert RaskSDKClient("id", "secret")._base_url == "https://api.rask.ai"
    assert issubclass(RaskClientException, Exception)
    assert enums.MediaKind
    assert schemas.ProjectCreate


def test_v2_says_how_to_get_authlib() -> None:
    code = "import sys; sys.modules['authlib'] = None; import rask_sdk.v2"
    done = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=False)
    assert done.returncode != 0
    assert "rask-sdk[v2]" in done.stderr


def test_readme_code_compiles() -> None:
    readme = (REPO / "README.md").read_text(encoding="utf-8")
    blocks = re.findall(r"```python\n(.*?)```", readme, re.S)
    assert blocks
    for index, block in enumerate(blocks):
        compile(block, f"README.md block {index}", "exec")
