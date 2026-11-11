from __future__ import annotations

import os
from collections.abc import Iterator

import pytest

from .harness import Harness, find_harness


def _start() -> Harness:
    directory = find_harness()
    if directory is None:
        if os.environ.get("RASK_SDK_ACCEPTANCE_REQUIRED"):
            pytest.fail("rask-sdk-acceptance not found: set RASK_SDK_ACCEPTANCE_DIR.")
        pytest.skip("rask-sdk-acceptance not found: set RASK_SDK_ACCEPTANCE_DIR.")
    return Harness(directory)


@pytest.fixture(scope="session")
def sync_harness() -> Iterator[Harness]:
    harness = _start()
    yield harness
    harness.stop()


@pytest.fixture(scope="session")
def async_harness() -> Iterator[Harness]:
    harness = _start()
    yield harness
    harness.stop()
