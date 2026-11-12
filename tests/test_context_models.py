"""Context models enforce the analysis artifact validation contracts."""

from copy import deepcopy

import jsonschema
from pydantic import ValidationError
import pytest

from rca.models.context import CorrelationTimeline, GitHubContext, JobContext


_JOB = {"job_id": "101", "time_window": {}}
_CORRELATION = {
    "job_id": "101",
    "correlation": {"method": "none", "confidence": "none"},
    "timeline_events": [],
}
_GITHUB = {"job_id": "101", "job_metadata": {}, "github_fetches": []}


@pytest.mark.parametrize(
    ("model", "data"),
    [
        (JobContext, _JOB),
        (CorrelationTimeline, _CORRELATION),
        (GitHubContext, _GITHUB),
    ],
)
def test_context_models_preserve_required_fields_omission_and_extra_properties(model, data):
    data = {**deepcopy(data), "extra_context": {"anything": None}}
    assert model.model_validate(data).model_dump(exclude_unset=True) == data
    missing = {key: value for key, value in data.items() if key != "job_id"}
    with pytest.raises(ValidationError):
        model.model_validate(missing)


@pytest.mark.parametrize(
    ("model", "data", "field"),
    [
        (JobContext, _JOB, "job_name"),
        (JobContext, _JOB, "failed_tasks"),
        (CorrelationTimeline, _CORRELATION, "time_range"),
        (GitHubContext, _GITHUB, "job_metadata"),
    ],
)
def test_context_models_reject_explicit_null_for_nonnullable_fields(model, data, field):
    with pytest.raises(ValidationError):
        model.model_validate({**data, field: None})


def test_correlation_nullable_fields_and_open_nested_objects_round_trip():
    data = deepcopy(_CORRELATION)
    data["correlation"].update(
        identifiers={"guid": None, "namespace": None, "other": "value"},
        time_overlap={"splunk_first_log": None, "splunk_last_log": None},
    )
    assert CorrelationTimeline.model_validate(data).model_dump(exclude_unset=True) == data


@pytest.mark.parametrize("total_events", [True, "2", 1.5])
def test_job_context_rejects_non_json_integers(total_events):
    with pytest.raises(ValidationError):
        JobContext.model_validate({**_JOB, "total_events": total_events})


def test_job_context_accepts_integral_floats_and_numbers_without_coercing_strings():
    data = {**_JOB, "total_events": 2.0, "time_window": {"duration_seconds": 5}}
    model = JobContext.model_validate(data)
    assert model.total_events == 2
    assert model.time_window.duration_seconds == 5
    with pytest.raises(ValidationError):
        JobContext.model_validate({**_JOB, "time_window": {"duration_seconds": "5"}})
    with pytest.raises(ValidationError):
        JobContext.model_validate({**_JOB, "time_window": {"duration_seconds": True}})


@pytest.mark.parametrize("timestamp", ["2026-10-28", "tomorrow", "2026-99-99T00:00:00Z"])
def test_job_context_date_time_validation_matches_the_existing_format_checker(timestamp):
    data = {**_JOB, "parsed_at": timestamp}
    # jsonschema only enables its RFC3339 check when its optional format
    # dependency is installed. Keep validation consistent in either setup.
    if jsonschema.Draft7Validator.FORMAT_CHECKER.conforms(timestamp, "date-time"):
        assert JobContext.model_validate(data).parsed_at == timestamp
    else:
        with pytest.raises(ValidationError):
            JobContext.model_validate(data)


def test_job_context_preserves_rfc3339_timestamp_strings():
    timestamp = "2026-10-28t14:25:00z"
    data = {**_JOB, "parsed_at": timestamp}
    assert JobContext.model_validate(data).model_dump(exclude_unset=True) == data


def _github_with_result(result, *, field="fetched_configs"):
    return {
        **_GITHUB,
        "github_fetches": [
            {
                "task_path": "roles/example/tasks/main.yml:12",
                "location": {},
                "fetched_configs": {},
                "fetched_workload": {},
                field: {"example": result},
            }
        ],
    }


@pytest.mark.parametrize("field", ["fetched_configs", "fetched_workload"])
@pytest.mark.parametrize(
    "result",
    [
        {"path": "file.yml", "content": "data", "sha": "abc", "size": 4},
        {
            "path": "file.yml", "content": "data", "sha": "abc", "size": 4,
            "paths_tried": [{"path": "file.yml", "status": "success"}],
        },
        {"error": "not_found", "path": "file.yml", "extra": "preserved"},
        {
            "error": "all_paths_failed",
            "paths_tried": [{"path": "file.yml", "status": "404"}],
            "mcp_verified": False,
        },
    ],
)
def test_github_fetch_alternatives_round_trip(field, result):
    data = _github_with_result(result, field=field)
    assert GitHubContext.model_validate(data).model_dump(exclude_unset=True) == data


@pytest.mark.parametrize(
    "result",
    [
        {},
        {"error": "not_found"},
        {"error": "all_paths_failed", "paths_tried": [{"path": "file.yml", "status": "success"}]},
        {"path": "file.yml", "content": "data", "sha": "abc", "size": "4"},
        {"path": "file.yml", "content": "data", "sha": "abc", "size": 4, "error": "not_found"},
        {
            "path": "file.yml", "content": "data", "sha": "abc", "size": 4,
            "error": "all_paths_failed", "paths_tried": [],
        },
    ],
)
def test_github_fetch_models_match_oneof_rejections(result):
    data = _github_with_result(result)
    assert not jsonschema.Draft202012Validator(GitHubContext.model_json_schema()).is_valid(data)
    with pytest.raises(ValidationError):
        GitHubContext.model_validate(data)
