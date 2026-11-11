"""S1-S17 and BU1-BU7 from the shared scenario list, for both clients, over real HTTP on a prefixed base URL."""

from __future__ import annotations

import json
import os
from typing import Any

import pytest

from .harness import LANG, REPO, Harness, find_harness
from .runner import failure_text, run_scenario

RUN_SLOW = bool(os.environ.get("RUN_SLOW"))


def _scenario_list() -> list[dict[str, Any]]:
    directory = find_harness()
    if directory is None:
        return []
    return json.loads((directory / "scenarios" / "scenarios.json").read_text(encoding="utf-8"))["scenarios"]


def _applies(scenario: dict[str, Any]) -> bool:
    if scenario.get("status") == "later":
        return False
    return LANG in scenario.get("languages", [LANG])


SCENARIOS = [scenario for scenario in _scenario_list() if _applies(scenario)]
IDS = [scenario["id"] for scenario in SCENARIOS]
SLOW = {scenario["id"] for scenario in SCENARIOS if scenario.get("slow")}


def _check(harness: Harness, scenario_id: str, kind: str, monkeypatch: pytest.MonkeyPatch) -> None:
    if scenario_id in SLOW and not RUN_SLOW:
        pytest.skip("slow: set RUN_SLOW=1")
    result = run_scenario(harness, scenario_id, kind, monkeypatch)
    assert result["verify"]["pass"], failure_text(result)


@pytest.mark.parametrize("scenario_id", IDS)
def test_sync(sync_harness: Harness, scenario_id: str, monkeypatch: pytest.MonkeyPatch) -> None:
    _check(sync_harness, scenario_id, "sync", monkeypatch)


@pytest.mark.parametrize("scenario_id", IDS)
def test_async(async_harness: Harness, scenario_id: str, monkeypatch: pytest.MonkeyPatch) -> None:
    _check(async_harness, scenario_id, "async", monkeypatch)


@pytest.mark.parametrize("fixture", ["sync_harness", "async_harness"])
def test_report_is_complete(fixture: str, request: pytest.FixtureRequest) -> None:
    """Runs after the scenarios above: the harness's own report says nothing was left out or failed."""
    harness: Harness = request.getfixturevalue(fixture)
    report = harness.get(f"/report?lang={LANG}" + ("&slow=true" if RUN_SLOW else ""))
    assert report["failed"] == [], report["failed"]
    assert report["not_run"] == [], report["not_run"]
    assert report["complete"] is True, report


def test_contract_is_the_harness_contract() -> None:
    directory = find_harness()
    if directory is None:
        pytest.skip("rask-sdk-acceptance not found")
    ours = (REPO / "contract" / "openapi.json").read_bytes()
    theirs = (directory / "contract" / "openapi.json").read_bytes()
    assert ours == theirs, "contract/openapi.json differs from the harness's copy"
