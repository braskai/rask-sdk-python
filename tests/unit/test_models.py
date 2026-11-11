"""S12 at the model level: open values stay readable, and nothing the server sends is fatal."""

from __future__ import annotations

import datetime
from typing import Any

import httpx
import pytest

from rask_sdk import Rask, UnknownVariant, models

MINIMAL_JOB: dict[str, Any] = {
    "object": "job",
    "id": "job-1",
    "workspace_id": "w",
    "type": "dub",
    "status": "running",
    "progress": 10,
    "stage": None,
    "queue_position": None,
    "estimated_finish_at": None,
    "overdue": False,
    "project_id": "p",
    "media_id": None,
    "parent_job_id": None,
    "channel": "api",
    "params": {"type": "file_restore"},
    "details": None,
    "stages": None,
    "cost": {"credit_type": "minutes", "minutes": 3},
    "result": None,
    "error": None,
    "created_at": "2026-11-03T09:00:00Z",
    "started_at": None,
    "finished_at": None,
    "links": {"self": "/v3/jobs/job-1"},
}


def test_known_variant_is_typed() -> None:
    job = models.Job.model_validate(MINIMAL_JOB)
    assert isinstance(job.cost, models.MinutesCost)
    assert job.created_at == datetime.datetime(2026, 11, 3, 9, tzinfo=datetime.UTC)


def test_several_discriminator_values_can_share_a_variant() -> None:
    job = models.Job.model_validate({**MINIMAL_JOB, "cost": {"credit_type": "lipsync_free_minutes", "minutes": 1}})
    assert isinstance(job.cost, models.MinutesCost)


def test_unknown_enum_value_is_kept() -> None:
    job = models.Job.model_validate({**MINIMAL_JOB, "status": "brand_new", "type": "brand_new_type"})
    assert job.status == "brand_new"
    assert job.type == "brand_new_type"


def test_unknown_union_variant_is_kept_required_and_nullable() -> None:
    variant = {"type": "brand_new", "payload": {"a": 1}}
    job = models.Job.model_validate(
        {**MINIMAL_JOB, "params": variant, "cost": {"credit_type": "brand_new", "amount": 2}}
    )
    assert isinstance(job.params, UnknownVariant)
    assert job.params.type == "brand_new"
    assert job.params.payload == {"a": 1}
    assert isinstance(job.cost, UnknownVariant)
    assert job.cost.amount == 2
    dumped = job.to_dict()
    assert dumped["params"] == variant
    assert dumped["cost"] == {"credit_type": "brand_new", "amount": 2}


def test_unknown_fields_are_kept_at_any_depth() -> None:
    job = models.Job.model_validate(
        {
            **MINIMAL_JOB,
            "brand_new": 1,
            "cost": {"credit_type": "minutes", "minutes": 3, "nested_new": {"x": 1}},
        }
    )
    assert job.brand_new == 1
    assert job.model_extra == {"brand_new": 1}
    assert isinstance(job.cost, models.MinutesCost)
    assert job.cost.nested_new == {"x": 1}
    assert job.to_dict()["cost"]["nested_new"] == {"x": 1}


def test_a_wrong_type_keeps_the_rest_of_the_object_typed() -> None:
    job = models.Job.model_validate({**MINIMAL_JOB, "progress": "not a number", "overdue": {"odd": True}})
    assert job.progress == "not a number"
    assert job.overdue == {"odd": True}
    assert isinstance(job.cost, models.MinutesCost)
    assert job.created_at.year == 2026


def test_a_missing_required_field_reads_as_none_and_stays_out_of_the_dump() -> None:
    raw = {key: value for key, value in MINIMAL_JOB.items() if key != "workspace_id"}
    job = models.Job.model_validate(raw)
    assert job.workspace_id is None
    assert "workspace_id" not in job.to_dict()


def test_a_broken_nested_object_does_not_hurt_the_parent() -> None:
    job = models.Job.model_validate({**MINIMAL_JOB, "links": {"self": 12}})
    assert job.links.self == 12
    assert job.id == "job-1"


def test_an_unknown_string_where_an_object_belongs_is_kept() -> None:
    job = models.Job.model_validate({**MINIMAL_JOB, "cost": "free now"})
    assert job.cost == "free now"
    assert job.to_dict()["cost"] == "free now"
    job = models.Job.model_validate({**MINIMAL_JOB, "links": "gone"})
    assert job.links == "gone"
    assert job.to_dict()["links"] == "gone"


def test_http_info_is_on_the_top_object_only() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=MINIMAL_JOB, headers={"ETag": '"j"', "Idempotent-Replayed": "true"})

    job = Rask(
        api_key="k", base_url="https://x.test/v3", http_client=httpx.Client(transport=httpx.MockTransport(handler))
    ).jobs.get_job("job-1")
    assert job.http is not None
    assert job.http.status == 200
    assert job.http.etag == '"j"'
    assert job.http.headers["etag"] == '"j"'
    assert job.http.headers["ETag"] == '"j"'
    assert job.http.idempotent_replayed is True
    assert job.http.json()["id"] == "job-1"
    assert job.links.http is None


@pytest.mark.parametrize("name", ["JobStatus", "JobStatusKnown", "Cost", "Job", "UploadPutHeaders", "UnknownVariant"])
def test_public_names(name: str) -> None:
    assert name in models.__all__
    assert hasattr(models, name)
