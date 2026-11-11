"""S14: generate from a copy of the contract with one operation deprecated, and check the Python marker."""

from __future__ import annotations

import importlib
import importlib.util
import json
import sys
import tempfile
import warnings
from pathlib import Path
from typing import Any

import httpx

from rask_sdk import Rask

REPO = Path(__file__).resolve().parents[2]


def _generator() -> Any:
    spec = importlib.util.spec_from_file_location("rask_generate", REPO / "tools" / "generate.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["rask_generate"] = module
    spec.loader.exec_module(module)
    return module


def _generate_copy(out: Path) -> None:
    generator = _generator()
    contract = json.loads((REPO / "contract" / "openapi.json").read_text(encoding="utf-8"))
    contract["paths"]["/me"]["get"]["deprecated"] = True
    copy = out.parent / "openapi.json"
    copy.write_text(json.dumps(contract), encoding="utf-8")
    generator.main(["--contract", str(copy), "--out", str(out)])


def _import_as_sdk_package(out: Path) -> Any:
    """The generated copy as a sibling of rask_sdk._generated, so its relative imports find the real runtime."""
    name = "rask_sdk._generated_deprecated_probe"
    spec = importlib.util.spec_from_file_location(name, out / "__init__.py", submodule_search_locations=[str(out)])
    assert spec and spec.loader
    package = importlib.util.module_from_spec(spec)
    sys.modules[name] = package
    spec.loader.exec_module(package)
    return importlib.import_module(f"{name}.resources")


def check_deprecation_marker() -> list[str]:
    """The marker is on the sync and the async method, and calling the generated method warns."""
    with tempfile.TemporaryDirectory() as directory:
        out = Path(directory) / "generated"
        _generate_copy(out)
        resources = _import_as_sdk_package(out)
        client = Rask(
            api_key="k",
            base_url="https://x.test/v3",
            http_client=httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(200, json={}))),
        )
        failures = []
        for cls in (resources.Account, resources.AsyncAccount):
            if "getMe" not in getattr(cls.get_me, "__deprecated__", ""):
                failures.append(f"{cls.__name__}.get_me has no deprecation marker")
            if getattr(cls.get_balance, "__deprecated__", None) is not None:
                failures.append(f"{cls.__name__}.get_balance is marked but should not be")
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            resources.Account(client).get_me()
        if not any(issubclass(w.category, DeprecationWarning) and "getMe" in str(w.message) for w in caught):
            failures.append("calling the deprecated method did not warn")
        client.close()
    return failures
