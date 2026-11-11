"""One scenario: start it, run its calls through the public SDK, check expect_sdk, ask the harness to verify the wire.

This is the loop in the harness README, ported from test/reference/runner.mjs. Calls go through the generated methods
(`client.projects.create_folder(...)`), never through a lower layer, and results are read from the typed objects.
"""

from __future__ import annotations

import asyncio
import inspect
import io
import json
import logging
import pprint
import re
import tempfile
import warnings
from pathlib import Path
from typing import Any

import httpx

from rask_sdk import AsyncRask, Rask, RaskDeprecationWarning, RaskError, RaskModel
from rask_sdk._generated.operations import OPERATIONS
from rask_sdk._pagination import _BasePage

from .harness import LANG, Harness

ENV_NAMES = ("RASK_API_KEY", "RASK_BASE_URL", "RASK_LOG")


class LogCapture(logging.Handler):
    def __init__(self) -> None:
        super().__init__(logging.NOTSET)
        self.lines: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.lines.append(record.getMessage())


def kwargs_for(op_id: str, call: dict[str, Any]) -> tuple[list[Any], dict[str, Any]]:
    """A scenario's path/query/body as arguments of the generated method."""
    op = OPERATIONS[op_id]
    path = call.get("path") or {}
    args = [path[name] for name in op.path_params if name in path]
    kwargs: dict[str, Any] = {}
    query_names = dict(op.query_params)
    for wire, value in (call.get("query") or {}).items():
        kwargs[query_names[wire]] = value
    body = call.get("body")
    if body is not None:
        if op.body_mode == "root":
            kwargs["body"] = body
        else:
            body_names = dict(op.body_params)
            for wire, value in body.items():
                kwargs[body_names[wire]] = value
    if call.get("if_match") is not None:
        kwargs["if_match"] = call["if_match"]
    if call.get("idempotency_key") is not None:
        kwargs["idempotency_key"] = call["idempotency_key"]
    return args, kwargs


def method_of(client: Any, op_id: str) -> Any:
    op = OPERATIONS[op_id]
    return getattr(getattr(client, op.resource), op.method_name)


def content_bytes(content: dict[str, Any]) -> bytes:
    assert content["fill"] == "index_mod_256"
    return bytes(index % 256 for index in range(content["size_bytes"]))


def upload_kwargs(call: dict[str, Any], tmp: Path, progress: list[int]) -> tuple[Any, dict[str, Any]]:
    data = content_bytes(call["content"])
    kwargs: dict[str, Any] = {"filename": call["filename"], "content_type": call["content_type"]}
    kwargs["on_progress"] = lambda sent, total: progress.append(sent)
    if call["source"] == "path":
        path = tmp / call["filename"]
        path.write_bytes(data)
        return path, kwargs
    return io.BytesIO(data), kwargs


def run_call_sync(
    client: Rask, call: dict[str, Any], previous: list[dict[str, Any]], operations: list[Any], tmp: Path
) -> Any:
    helper = call.get("helper")
    if not helper:
        args, kwargs = kwargs_for(call["op"], call)
        return method_of(client, call["op"])(*args, **kwargs)
    if helper == "jobs.wait":
        job_id = call.get("job_id") or previous[call["job_id_from_call"]]["result"].id
        kwargs = {"timeout": call["timeout_seconds"]} if call.get("timeout_seconds") else {}
        return client.jobs.wait(job_id, **kwargs)
    if helper == "uploads.upload_file":
        progress: list[int] = []
        source, kwargs = upload_kwargs(call, tmp, progress)
        job = client.uploads.upload_file(source, **kwargs)
        return UploadOutcome(job, progress)
    if helper == "iterate":
        args, kwargs = kwargs_for(call["op"], call)
        return ItemsOutcome(list(method_of(client, call["op"])(*args, **kwargs)))
    if helper == "first_page":
        args, kwargs = kwargs_for(call["op"], call)
        return method_of(client, call["op"])(*args, **kwargs)
    if helper == "next_page":
        return previous[call["after_call"]]["result"].next_page()
    if helper == "sweep_operations":
        results = {}
        for operation in operations:
            if operation["has_json_response"]:
                results[operation["operationId"]] = sweep_call(client, operation)
        return SweepOutcome(results)
    raise AssertionError(f"Unknown helper {helper}")


def sweep_call(client: Any, operation: dict[str, Any]) -> Any:
    sample = operation["sample"]
    call = {
        "path": sample.get("path"),
        "query": sample.get("query"),
        "body": sample.get("body"),
        "if_match": (sample.get("headers") or {}).get("If-Match"),
    }
    args, kwargs = kwargs_for(operation["operationId"], call)
    return method_of(client, operation["operationId"])(*args, **kwargs)


async def run_call_async(
    client: AsyncRask, call: dict[str, Any], previous: list[dict[str, Any]], operations: list[Any], tmp: Path
) -> Any:
    helper = call.get("helper")
    if not helper:
        args, kwargs = kwargs_for(call["op"], call)
        return await method_of(client, call["op"])(*args, **kwargs)
    if helper == "jobs.wait":
        job_id = call.get("job_id") or previous[call["job_id_from_call"]]["result"].id
        kwargs = {"timeout": call["timeout_seconds"]} if call.get("timeout_seconds") else {}
        return await client.jobs.wait(job_id, **kwargs)
    if helper == "uploads.upload_file":
        progress: list[int] = []
        source, kwargs = upload_kwargs(call, tmp, progress)
        job = await client.uploads.upload_file(source, **kwargs)
        return UploadOutcome(job, progress)
    if helper == "iterate":
        args, kwargs = kwargs_for(call["op"], call)
        page = await method_of(client, call["op"])(*args, **kwargs)
        return ItemsOutcome([item async for item in page])
    if helper == "first_page":
        args, kwargs = kwargs_for(call["op"], call)
        return await method_of(client, call["op"])(*args, **kwargs)
    if helper == "next_page":
        return await previous[call["after_call"]]["result"].next_page()
    if helper == "sweep_operations":
        results = {}
        for operation in operations:
            if operation["has_json_response"]:
                results[operation["operationId"]] = await sweep_call(client, operation)
        return SweepOutcome(results)
    raise AssertionError(f"Unknown helper {helper}")


class UploadOutcome:
    def __init__(self, job: Any, progress: list[int]) -> None:
        self.job = job
        self.progress = progress


class ItemsOutcome:
    def __init__(self, items: list[Any]) -> None:
        self.items = items


class SweepOutcome:
    def __init__(self, results: dict[str, Any]) -> None:
        self.results = results


def plain(value: Any) -> Any:
    """What the SDK holds, as JSON values, for comparing with what the server sent."""
    if isinstance(value, RaskModel):
        return value.to_dict()
    if isinstance(value, _BasePage):
        return value.to_dict()
    if isinstance(value, list):
        return [plain(item) for item in value]
    if isinstance(value, dict):
        return {key: plain(item) for key, item in value.items()}
    return value


MISSING = object()


def read_pointer(value: Any, pointer: str) -> Any:
    """A JSON Pointer, walked over the typed objects: attributes, unknown fields, dicts and lists."""
    node = value
    for raw in pointer.lstrip("/").split("/"):
        part = raw.replace("~1", "/").replace("~0", "~")
        if isinstance(node, RaskModel | _BasePage):
            # The attribute of a wire name: the alias if the generator gave it one, else the name itself.
            model = node._listing if isinstance(node, _BasePage) else node
            attribute = next((n for n, f in type(model).model_fields.items() if f.alias == part), part)
            node = getattr(model, attribute, MISSING)
        elif isinstance(node, dict):
            node = node.get(part, MISSING)
        elif isinstance(node, list):
            node = node[int(part)] if part.isdigit() and int(part) < len(node) else MISSING
        else:
            return MISSING
        if node is MISSING:
            return MISSING
    return node


def http_of(source: Any) -> Any:
    if isinstance(source, RaskError):
        return source
    return source.http if source is not None else None


def check_raised(expect: dict[str, Any], outcome: dict[str, Any], records: list[Any]) -> list[str]:
    error = outcome.get("error")
    if error is None:
        return ["expected an error, the call returned"]
    failures = []
    if expect.get("error_class"):
        names = [cls.__name__ for cls in type(error).__mro__]
        if expect["error_class"] not in names:
            failures.append(f"expected {expect['error_class']}, got {names}")
    if not isinstance(error, RaskError):
        failures.append(f"expected a RaskError, got {error!r}")
        return failures
    if "status" in expect and error.status != expect["status"]:
        failures.append(f"expected status {expect['status']}, got {error.status}")
    if "code" in expect and error.code != expect["code"]:
        failures.append(f"expected code {expect['code']}, got {error.code}")
    for name, value in (expect.get("fields") or {}).items():
        actual = plain(getattr(error, name, None))
        if actual != value:
            failures.append(f"expected {name} {value!r}, got {actual!r}")
    for name in expect.get("present") or []:
        if getattr(error, name, None) is None:
            failures.append(f"expected {name} on the error")
    if expect.get("request_id_matches_wire"):
        sent = [(record.get("response") or {}).get("headers", {}).get("X-Request-Id") for record in records]
        if error.request_id not in sent:
            failures.append(f"request_id {error.request_id} is not one the server sent")
    return failures


def check_header(expect: dict[str, Any], outcome: dict[str, Any], records: list[Any]) -> list[str]:
    source = outcome.get("result") if outcome.get("result") is not None else outcome.get("error")
    holder = http_of(source)
    if holder is None:
        return [f"nothing holds the headers: {source!r}"]
    name = expect["name"]
    value = holder.headers.get(name)
    if value is None:
        return [f"header {name} is not exposed"]
    if "equals" in expect and value != expect["equals"]:
        return [f"header {name}: expected {expect['equals']}, got {value}"]
    if "matches" in expect and not re.search(expect["matches"], value):
        return [f"header {name}: {value} does not match {expect['matches']}"]
    if "equals_field" in expect and value != read_pointer(source, expect["equals_field"]):
        return [f"header {name} differs from {expect['equals_field']}"]
    if expect.get("matches_wire"):
        sent = [(record.get("response") or {}).get("headers", {}).get(name) for record in records]
        if value not in sent:
            return [f"header {name} {value} is not what the server sent"]
    return []


def check_injected(outcome: dict[str, Any], records: list[Any]) -> list[str]:
    failures = []
    result = outcome.get("result")
    results = result.results if isinstance(result, SweepOutcome) else None
    injected = 0
    for record in records:
        injections = (record.get("response") or {}).get("injections") or []
        if not injections:
            continue
        body = results.get(record["operationId"]) if results is not None else result
        for injection in injections:
            injected += 1
            pointer = injection["pointer"]
            actual = body if pointer == "/" else read_pointer(body, pointer)
            if actual is MISSING or plain(actual) != injection["value"]:
                shown = "(missing)" if actual is MISSING else repr(plain(actual))[:200]
                failures.append(f"{record['operationId']} lost the {injection['kind']} injected at {pointer}: {shown}")
    if not injected:
        failures.append("nothing was injected")
    return failures


def check_logs(expect: dict[str, Any], lines: list[str], records: list[Any]) -> list[str]:
    logs = "\n".join(lines)
    failures = []
    if expect.get("empty") and logs:
        failures.append(f"expected no logs, got {logs[:200]}")
    for text in expect.get("must_contain") or []:
        if text not in logs:
            failures.append(f"logs do not contain {text}")
    for text in expect.get("must_not_contain") or []:
        if text in logs:
            failures.append("logs contain a secret")
    for item in expect.get("must_not_contain_wire") or []:
        for record in records:
            if record.get("operationId") == item["operationId"]:
                value = record["raw_query_params"][item["query"]]
                if value in logs:
                    failures.append(f"logs contain {item['query']}")
    return failures


def config_value(client: Any, field: str) -> Any:
    if field == "base_url":
        return client.base_url
    if field == "timeout_seconds":
        return client.timeout
    if field == "jobs.wait.timeout_seconds":
        return inspect.signature(client.jobs.wait).parameters["timeout"].default
    raise AssertionError(f"Unknown config field {field}")


def check_manual(scenario: dict[str, Any]) -> list[str]:
    if scenario["id"] == "S14.language-markers":
        from .markers import check_deprecation_marker

        return check_deprecation_marker()
    raise AssertionError(f"No check for manual scenario {scenario['id']}")


def check_expectation(
    expect: dict[str, Any], run: dict[str, Any], records: list[Any], scenario: dict[str, Any]
) -> list[str]:
    client, construct_error, calls = run["client"], run["construct_error"], run["calls"]
    index = expect.get("call", len(calls) - 1)
    outcome = calls[index] if calls and 0 <= index < len(calls) else {}
    kind = expect["kind"]
    if kind == "returned":
        return (
            []
            if outcome and "error" not in outcome
            else [f"call {expect.get('call')} raised: {outcome.get('error')!r}"]
        )
    if kind == "returned_or_raised":
        return []
    if kind == "raised":
        return check_raised(expect, outcome, records)
    if kind == "raised_before_network":
        error = construct_error or (calls[0].get("error") if calls else None)
        if error is None:
            return ["expected an error before any request"]
        return (
            []
            if expect["message_contains"] in str(error)
            else [f"{error} does not mention {expect['message_contains']}"]
        )
    if kind == "config":
        if client is None:
            return [f"no client: {construct_error}"]
        value = config_value(client, expect["field"])
        return [] if value == expect["equals"] else [f"config {expect['field']} is {value!r}"]
    if kind == "repr_redacts":
        if client is None:
            return [f"no client: {construct_error}"]
        texts = [repr(client), str(client), pprint.pformat(vars(client)), repr(client._config)]
        return ["the key shows in the client representation"] if any(expect["secret"] in text for text in texts) else []
    if kind == "field":
        result = outcome.get("result")
        if result is None:
            return [f"call raised: {outcome.get('error')!r}"]
        if "raw_status" in expect and result.http.status != expect["raw_status"]:
            return [f"status is {result.http.status}"]
        if "path" not in expect:
            return []
        value = read_pointer(result, expect["path"])
        if expect.get("present"):
            return [f"{expect['path']} is missing"] if value is MISSING else []
        return [] if plain(value) == expect["equals"] else [f"{expect['path']} is {value!r}"]
    if kind == "header_exposed":
        return check_header(expect, outcome, records)
    if kind == "idempotency_key_exposed":
        source = outcome.get("result") if outcome.get("result") is not None else outcome.get("error")
        key = (
            source.idempotency_key
            if isinstance(source, RaskError)
            else (source.http.idempotency_key if source else None)
        )
        if not key:
            return ["the key is not exposed"]
        if "equals" in expect and key != expect["equals"]:
            return [f"key is {key}"]
        if expect.get("matches_wire") and key not in [r["headers"].get("idempotency-key") for r in records]:
            return [f"key {key} was never sent"]
        return []
    if kind == "items":
        result = outcome.get("result")
        if not isinstance(result, ItemsOutcome):
            return [f"no items: {outcome.get('error')!r}"]
        if "count" in expect and len(result.items) != expect["count"]:
            return [f"saw {len(result.items)} items, expected {expect['count']}"]
        if "ids" in expect and [item.id for item in result.items] != expect["ids"]:
            return [f"saw ids {[item.id for item in result.items]}"]
        return []
    if kind == "pages":
        page = outcome.get("result")
        if page is None:
            return [f"no page: {outcome.get('error')!r}"]
        if len(page.data) != expect["item_count"]:
            return [f"page has {len(page.data)} items"]
        return [] if page.has_more == expect["has_more"] else [f"has_more is {page.has_more}"]
    if kind == "warnings":
        seen = [w.message.operation_id for w in run["warnings"] if isinstance(w.message, RaskDeprecationWarning)]
        failures = [] if len(seen) == expect["count"] else [f"saw {len(seen)} warnings, expected {expect['count']}"]
        for name, count in (expect.get("per_operation") or {}).items():
            if seen.count(name) != count:
                failures.append(f"{name}: saw {seen.count(name)} warnings, expected {count}")
        return failures
    if kind == "progress":
        result = outcome.get("result")
        if not isinstance(result, UploadOutcome) or not result.progress or result.progress[-1] != expect["final_bytes"]:
            return ["on_progress did not reach the file size"]
        return []
    if kind == "logs":
        return check_logs(expect, run["logs"].lines, records)
    if kind == "injected_values_preserved":
        return check_injected(outcome, records)
    if kind == "manual":
        return check_manual(scenario)
    raise AssertionError(f"Unknown expectation {kind}")


def assert_local(client: Any) -> None:
    """These tests must never leave the machine: the default base URL is production, and this key must not go there."""
    host = httpx.URL(client.base_url).host
    assert host in ("127.0.0.1", "localhost", "::1"), f"Refusing to run a scenario against {client.base_url}"


def build_client(scenario: dict[str, Any], kind: str, monkeypatch: Any) -> Any:
    for name in ENV_NAMES:
        monkeypatch.delenv(name, raising=False)
    for name, value in (scenario.get("client") or {}).get("env", {}).items():
        monkeypatch.setenv(name, value)
    args = dict((scenario.get("client") or {}).get("args", {}))
    if "timeout_seconds" in args:
        args["timeout"] = args.pop("timeout_seconds")
    return (AsyncRask if kind == "async" else Rask)(**args)


def run_scenario(harness: Harness, scenario_id: str, kind: str, monkeypatch: Any) -> dict[str, Any]:
    scenario = harness.post(f"/scenarios/{scenario_id}/start", {"lang": LANG})
    operations = harness.get("/operations")["operations"]
    run: dict[str, Any] = {"client": None, "construct_error": None, "calls": [], "warnings": []}
    capture = LogCapture()
    run["logs"] = capture
    logger = logging.getLogger("rask_sdk")
    logger.addHandler(capture)
    try:
        with warnings.catch_warnings(record=True) as caught, tempfile.TemporaryDirectory() as directory:
            warnings.simplefilter("always")
            tmp = Path(directory)
            try:
                run["client"] = build_client(scenario, kind, monkeypatch)
            except RaskError as exc:
                run["construct_error"] = exc
            client = run["client"]
            if client is not None:
                if scenario.get("calls"):
                    assert_local(client)
                if kind == "async":

                    async def drive() -> None:
                        for call in scenario.get("calls") or []:
                            try:
                                result = await run_call_async(client, call, run["calls"], operations, tmp)
                                run["calls"].append({"result": result})
                            except Exception as exc:
                                run["calls"].append({"error": exc})
                        await client.close()

                    asyncio.run(drive())
                else:
                    for call in scenario.get("calls") or []:
                        try:
                            result = run_call_sync(client, call, run["calls"], operations, tmp)
                            run["calls"].append({"result": result})
                        except Exception as exc:
                            run["calls"].append({"error": exc})
                    client.close()
            run["warnings"] = list(caught)
    finally:
        logger.removeHandler(capture)
    records = harness.get("/requests")["requests"]
    sdk_failures: list[str] = []
    for expect in scenario.get("expect_sdk") or []:
        sdk_failures += check_expectation(expect, run, records, scenario)
    verify = harness.post(
        f"/scenarios/{scenario_id}/verify",
        {"lang": LANG, "sdk_passed": not sdk_failures, "sdk_detail": "; ".join(sdk_failures)},
    )
    return {"verify": verify, "sdk_failures": sdk_failures, "scenario": scenario}


def failure_text(result: dict[str, Any]) -> str:
    return json.dumps(result["verify"].get("failures"), indent=1, ensure_ascii=False)[:3000]
